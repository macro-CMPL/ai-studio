"""不可变 PipelineSpec:把展开策略从 PM 分支里抽出,编译成带 digest 的静态模板。

ExpansionPM 依据注入的 spec 决定 bootstrap / fan-out / 逐分区展开,不再硬编码 stage。
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from studio.domain.enums import PropagationMode
from studio.serialization import digest


class StageMode(StrEnum):
    ROOT_SINGLETON = "root_singleton"  # 无输入,pipeline 初始化时创建
    FANOUT = "fanout"  # driver_stage 的产物列出分区,据此扇出
    PER_PARTITION = "per_partition"  # 上游某分区产物接受后,1:1 创建本阶段


class StageDef(BaseModel):
    model_config = ConfigDict(frozen=True)

    stage_id: str
    output_key: str
    logical_slot: str
    mode: StageMode
    driver_stage: str | None = None
    upstream_stage: str | None = None
    requirement_key: str | None = None
    propagation_mode: PropagationMode | None = None


class PipelineSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    stages: tuple[StageDef, ...]

    @property
    def spec_id(self) -> str:
        return digest([s.model_dump(mode="json") for s in self.stages])

    def root_stages(self) -> tuple[StageDef, ...]:
        return tuple(s for s in self.stages if s.mode is StageMode.ROOT_SINGLETON)

    def fanout_driven_by(self, producing_output_key: str) -> tuple[StageDef, ...]:
        producing = self._stage_by_output(producing_output_key)
        if producing is None:
            return ()
        return tuple(
            s
            for s in self.stages
            if s.mode is StageMode.FANOUT and s.driver_stage == producing.stage_id
        )

    def per_partition_fed_by(self, producing_output_key: str) -> tuple[StageDef, ...]:
        producing = self._stage_by_output(producing_output_key)
        if producing is None:
            return ()
        return tuple(
            s
            for s in self.stages
            if s.mode is StageMode.PER_PARTITION
            and s.upstream_stage == producing.stage_id
        )

    def by_stage(self, stage_id: str) -> StageDef | None:
        return next((s for s in self.stages if s.stage_id == stage_id), None)

    def _stage_by_output(self, output_key: str) -> StageDef | None:
        return next((s for s in self.stages if s.output_key == output_key), None)


def golden_pipeline() -> PipelineSpec:
    """M3 Golden 流水线:storyboard -> (fanout) plan -> (1:1) image。"""
    return PipelineSpec(
        stages=(
            StageDef(
                stage_id="storyboard",
                output_key="storyboard",
                logical_slot="storyboard",
                mode=StageMode.ROOT_SINGLETON,
            ),
            StageDef(
                stage_id="plan",
                output_key="plan",
                logical_slot="plan",
                mode=StageMode.FANOUT,
                driver_stage="storyboard",
                requirement_key="storyboard",
                propagation_mode=PropagationMode.AGGREGATE,
            ),
            StageDef(
                stage_id="image",
                output_key="image",
                logical_slot="image",
                mode=StageMode.PER_PARTITION,
                upstream_stage="plan",
                requirement_key="plan",
                propagation_mode=PropagationMode.PARTITION_PRESERVING,
            ),
        )
    )
