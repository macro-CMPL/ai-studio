"""把 M1 StageSpec 编译成 CompiledPipelineSpec —— 展开与 M4 成本/副作用的**单一定义源**。

编译器负责:
- 按 (artifact_type, logical_slot) 唯一解析 producer;缺失或歧义则拒绝。
- 保留多输入 Requirement 与 executor/control/cost/effect 元数据。
- 判定 stage 模式(ROOT_SINGLETON / FANOUT / PER_PARTITION)。
- 绑定 partition selector(id + version)与 propagation。
- 规范排序,计算包含 compiler_version 的稳定 spec_id。
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from studio.domain.enums import (
    AcceptanceStatus,
    ArtifactType,
    CardinalityKind,
    ControlRole,
    CostMode,
    ExecutorKind,
    PartitioningKind,
    PropagationMode,
    ToolEffectLevel,
)
from studio.domain.stages import Requirement, StageSpec
from studio.serialization import digest

COMPILER_VERSION = "1"


class StageMode(StrEnum):
    ROOT_SINGLETON = "root_singleton"
    FANOUT = "fanout"
    PER_PARTITION = "per_partition"


class CompilationError(Exception):
    """StageSpec 无法被编译成合法 pipeline。"""


class CompiledRequirement(BaseModel):
    model_config = ConfigDict(frozen=True)
    requirement_key: str
    artifact_type: ArtifactType
    logical_slot: str
    producer_stage: str
    cardinality_kind: CardinalityKind
    partition_by: str | None
    propagation_mode: PropagationMode
    acceptance_filter: AcceptanceStatus
    partition_selector_id: str | None
    partition_selector_version: str | None


class CompiledStage(BaseModel):
    model_config = ConfigDict(frozen=True)
    stage_id: str
    output_key: str
    logical_slot: str
    mode: StageMode
    executor_kind: ExecutorKind
    control_role: ControlRole
    cost_mode: CostMode
    allowed_tool_effects: tuple[ToolEffectLevel, ...]
    requirements: tuple[CompiledRequirement, ...]
    driver_stage: str | None
    upstream_stage: str | None
    driver_requirement: CompiledRequirement | None
    upstream_requirement: CompiledRequirement | None


class CompiledPipelineSpec(BaseModel):
    model_config = ConfigDict(frozen=True)
    compiler_version: str
    stages: tuple[CompiledStage, ...]

    @property
    def spec_id(self) -> str:
        return digest(
            {
                "compiler_version": self.compiler_version,
                "stages": [s.model_dump(mode="json") for s in self.stages],
            }
        )

    def by_stage(self, stage_id: str) -> CompiledStage | None:
        return next((s for s in self.stages if s.stage_id == stage_id), None)

    def by_output(self, output_key: str) -> CompiledStage | None:
        return next((s for s in self.stages if s.output_key == output_key), None)

    def root_stages(self) -> tuple[CompiledStage, ...]:
        return tuple(s for s in self.stages if s.mode is StageMode.ROOT_SINGLETON)

    def fanout_driven_by(self, output_key: str) -> tuple[CompiledStage, ...]:
        producer = self.by_output(output_key)
        if producer is None:
            return ()
        return tuple(
            s
            for s in self.stages
            if s.mode is StageMode.FANOUT and s.driver_stage == producer.stage_id
        )

    def per_partition_fed_by(self, output_key: str) -> tuple[CompiledStage, ...]:
        producer = self.by_output(output_key)
        if producer is None:
            return ()
        return tuple(
            s
            for s in self.stages
            if s.mode is StageMode.PER_PARTITION
            and s.upstream_stage == producer.stage_id
        )


def _requirement_key(req: Requirement) -> str:
    return f"{req.artifact_type.value}:{req.logical_slot}"


def compile_from(
    stages: tuple[StageSpec, ...], compiler_version: str = COMPILER_VERSION
) -> CompiledPipelineSpec:
    if not stages:
        raise CompilationError("pipeline 至少需要一个 stage")
    stage_ids = [s.stage_id for s in stages]
    if len(set(stage_ids)) != len(stage_ids):
        raise CompilationError("stage_id 必须唯一")

    # (artifact_type, logical_slot) -> [stage_id],用于解析 producer
    producers: dict[tuple[ArtifactType, str], list[str]] = {}
    for s in stages:
        for out in s.produces:
            producers.setdefault((out.artifact_type, out.logical_slot), []).append(
                s.stage_id
            )

    compiled: list[CompiledStage] = []
    for s in stages:
        if len(s.produces) != 1:
            raise CompilationError(f"{s.stage_id}: M3 要求恰好一个 OutputSpec")
        out = s.produces[0]

        creqs: list[CompiledRequirement] = []
        for req in s.requires:
            owners = producers.get((req.artifact_type, req.logical_slot), [])
            if not owners:
                raise CompilationError(
                    f"{s.stage_id}: 找不到 {req.artifact_type}/{req.logical_slot} 的 producer"
                )
            if len(owners) > 1:
                raise CompilationError(
                    f"{s.stage_id}: {req.artifact_type}/{req.logical_slot} 的 producer 歧义"
                )
            creqs.append(
                CompiledRequirement(
                    requirement_key=_requirement_key(req),
                    artifact_type=req.artifact_type,
                    logical_slot=req.logical_slot,
                    producer_stage=owners[0],
                    cardinality_kind=req.cardinality.kind,
                    partition_by=req.cardinality.partition_by,
                    propagation_mode=req.propagation_mode,
                    acceptance_filter=req.acceptance_filter,
                    partition_selector_id=req.partition_selector_id,
                    partition_selector_version=req.partition_selector_version,
                )
            )
        creqs.sort(key=lambda r: r.requirement_key)

        driver_req = next(
            (r for r in creqs if r.cardinality_kind is CardinalityKind.DYNAMIC_PARTITION_BY),
            None,
        )
        if not creqs:
            mode = StageMode.ROOT_SINGLETON
            driver_stage = upstream_stage = None
            upstream_req = None
        elif driver_req is not None:
            mode = StageMode.FANOUT
            driver_stage = driver_req.producer_stage
            upstream_stage = None
            upstream_req = None
        elif out.partitioning.kind is PartitioningKind.INHERIT_PARTITION:
            mode = StageMode.PER_PARTITION
            upstream_req = creqs[0]
            upstream_stage = upstream_req.producer_stage
            driver_stage = None
        else:
            raise CompilationError(f"{s.stage_id}: 无法判定 stage 模式")

        compiled.append(
            CompiledStage(
                stage_id=s.stage_id,
                output_key=out.logical_slot,
                logical_slot=out.logical_slot,
                mode=mode,
                executor_kind=s.executor_kind,
                control_role=s.control_role,
                cost_mode=s.cost_mode,
                allowed_tool_effects=tuple(
                    sorted(s.allowed_tool_effects, key=lambda e: e.value)
                ),
                requirements=tuple(creqs),
                driver_stage=driver_stage,
                upstream_stage=upstream_stage,
                driver_requirement=driver_req,
                upstream_requirement=upstream_req,
            )
        )

    # output_key 唯一性
    outs = [c.output_key for c in compiled]
    if len(set(outs)) != len(outs):
        raise CompilationError("output_key(logical_slot)必须唯一")

    compiled.sort(key=lambda c: c.stage_id)
    return CompiledPipelineSpec(
        compiler_version=compiler_version, stages=tuple(compiled)
    )
