"""把 M1 StageSpec 编译成 CompiledPipelineSpec —— 展开与 M4 成本/副作用的**单一定义源**。

编译器负责:
- 按 (artifact_type, logical_slot) 唯一解析 producer;缺失或歧义则拒绝。
- 依赖图拓扑校验(拒绝自环与间接环)。
- 判定 stage 模式,并对分区契约做交叉校验:
  ROOT_SINGLETON 输出必须 SINGLETON;
  FANOUT 恰好一个动态 driver,输出必须 DYNAMIC_FROM 且 from_key 匹配 partition_by;
  PER_PARTITION 输出必须 INHERIT_PARTITION,且恰好一个 PARTITION_PRESERVING 输入作为分区源;
  非动态 Requirement 不得残留 selector 字段。
- 保留完整输出契约与多输入 Requirement、executor/control/cost/effect 元数据。
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
    output_artifact_type: ArtifactType
    output_schema_version: int
    output_partitioning_kind: PartitioningKind
    output_from_key: str | None
    requirements: tuple[CompiledRequirement, ...]
    partition_source: CompiledRequirement | None
    driver_stage: str | None
    upstream_stage: str | None


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

    def consumers_of(
        self, output_key: str
    ) -> tuple[tuple[CompiledStage, CompiledRequirement], ...]:
        """所有以 output_key 为输入的 (stage, requirement)。"""
        out: list[tuple[CompiledStage, CompiledRequirement]] = []
        for s in self.stages:
            for r in s.requirements:
                if r.logical_slot == output_key:
                    out.append((s, r))
        return tuple(out)


def _requirement_key(req: Requirement) -> str:
    return f"{req.artifact_type.value}:{req.logical_slot}"


def _resolve_producers(stages: tuple[StageSpec, ...]) -> dict[tuple[ArtifactType, str], str]:
    index: dict[tuple[ArtifactType, str], list[str]] = {}
    for s in stages:
        for out in s.produces:
            index.setdefault((out.artifact_type, out.logical_slot), []).append(s.stage_id)
    resolved: dict[tuple[ArtifactType, str], str] = {}
    for key, owners in index.items():
        if len(owners) > 1:
            raise CompilationError(f"{key} 的 producer 歧义:{owners}")
        resolved[key] = owners[0]
    return resolved


def _assert_acyclic(edges: dict[str, set[str]]) -> None:
    WHITE, GRAY, BLACK = 0, 1, 2
    color = dict.fromkeys(edges, WHITE)

    def visit(node: str) -> None:
        color[node] = GRAY
        for nxt in sorted(edges.get(node, set())):
            if color[nxt] == GRAY:
                raise CompilationError("pipeline 依赖图存在环")
            if color[nxt] == WHITE:
                visit(nxt)
        color[node] = BLACK

    for node in sorted(edges):
        if color[node] == WHITE:
            visit(node)


def compile_from(
    stages: tuple[StageSpec, ...], compiler_version: str = COMPILER_VERSION
) -> CompiledPipelineSpec:
    if not stages:
        raise CompilationError("pipeline 至少需要一个 stage")
    stage_ids = [s.stage_id for s in stages]
    if len(set(stage_ids)) != len(stage_ids):
        raise CompilationError("stage_id 必须唯一")

    producers = _resolve_producers(stages)
    edges: dict[str, set[str]] = {s.stage_id: set() for s in stages}

    compiled: list[CompiledStage] = []
    for s in stages:
        if len(s.produces) != 1:
            raise CompilationError(f"{s.stage_id}: M3 要求恰好一个 OutputSpec")
        out = s.produces[0]

        creqs: list[CompiledRequirement] = []
        for req in s.requires:
            owner = producers.get((req.artifact_type, req.logical_slot))
            if owner is None:
                raise CompilationError(
                    f"{s.stage_id}: 找不到 {req.artifact_type}/{req.logical_slot} 的 producer"
                )
            if req.cardinality.kind is not CardinalityKind.DYNAMIC_PARTITION_BY and (
                req.partition_selector_id or req.partition_selector_version
            ):
                raise CompilationError(
                    f"{s.stage_id}: 非动态 Requirement 不得携带 selector"
                )
            edges[owner].add(s.stage_id)
            creqs.append(
                CompiledRequirement(
                    requirement_key=_requirement_key(req),
                    artifact_type=req.artifact_type,
                    logical_slot=req.logical_slot,
                    producer_stage=owner,
                    cardinality_kind=req.cardinality.kind,
                    partition_by=req.cardinality.partition_by,
                    propagation_mode=req.propagation_mode,
                    acceptance_filter=req.acceptance_filter,
                    partition_selector_id=req.partition_selector_id,
                    partition_selector_version=req.partition_selector_version,
                )
            )
        creqs.sort(key=lambda r: r.requirement_key)

        mode, partition_source, driver_stage, upstream_stage = _classify(
            s.stage_id, creqs, out.partitioning.kind, out.partitioning.from_key
        )

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
                output_artifact_type=out.artifact_type,
                output_schema_version=out.schema_version,
                output_partitioning_kind=out.partitioning.kind,
                output_from_key=out.partitioning.from_key,
                requirements=tuple(creqs),
                partition_source=partition_source,
                driver_stage=driver_stage,
                upstream_stage=upstream_stage,
            )
        )

    outs = [c.output_key for c in compiled]
    if len(set(outs)) != len(outs):
        raise CompilationError("output_key(logical_slot)必须唯一")

    _assert_acyclic(edges)
    compiled.sort(key=lambda c: c.stage_id)
    return CompiledPipelineSpec(compiler_version=compiler_version, stages=tuple(compiled))


def _classify(
    stage_id: str,
    creqs: list[CompiledRequirement],
    out_kind: PartitioningKind,
    out_from_key: str | None,
) -> tuple[StageMode, CompiledRequirement | None, str | None, str | None]:
    dyn = [r for r in creqs if r.cardinality_kind is CardinalityKind.DYNAMIC_PARTITION_BY]
  
    preserving = [
        r for r in creqs if r.propagation_mode is PropagationMode.PARTITION_PRESERVING
    ]

    if not creqs:
        if out_kind is not PartitioningKind.SINGLETON:
            raise CompilationError(f"{stage_id}: ROOT 输出必须是 SINGLETON")
        return StageMode.ROOT_SINGLETON, None, None, None

    if dyn:
        if len(dyn) != 1:
            raise CompilationError(f"{stage_id}: FANOUT 只能有一个动态 driver")
        driver = dyn[0]
        if out_kind is not PartitioningKind.DYNAMIC_FROM or out_from_key != driver.partition_by:
            raise CompilationError(
                f"{stage_id}: FANOUT 输出必须 DYNAMIC_FROM 且 from_key 匹配 driver.partition_by"
            )
        return StageMode.FANOUT, driver, driver.producer_stage, None

    if out_kind is not PartitioningKind.INHERIT_PARTITION:
        raise CompilationError(
            f"{stage_id}: 消费型 stage 输出必须 INHERIT_PARTITION(PER_PARTITION)"
        )
    if len(preserving) != 1:
        raise CompilationError(
            f"{stage_id}: PER_PARTITION 需要恰好一个 PARTITION_PRESERVING 输入作为分区源"
        )
    source = preserving[0]
    return StageMode.PER_PARTITION, source, None, source.producer_stage
