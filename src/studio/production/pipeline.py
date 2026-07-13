"""不可变 PipelineSpec:把展开策略从 PM 分支里抽出,编译成带 digest 的静态模板。

ExpansionPM 依据注入的 spec + selector registry 决定 bootstrap / fan-out / 逐分区展开。
分区提取用可配置的 partition_selector(不再硬编码 StoryboardPayload)。
"""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator

from studio.domain.artifacts import StoryboardPayload
from studio.domain.enums import PropagationMode
from studio.serialization import digest

PartitionSelector = Callable[[Any], tuple[str, ...]]


class StageMode(StrEnum):
    ROOT_SINGLETON = "root_singleton"
    FANOUT = "fanout"
    PER_PARTITION = "per_partition"


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
    partition_selector_id: str | None = None
    partition_selector_version: str | None = None


class PipelineSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    stages: tuple[StageDef, ...]

    @model_validator(mode="after")
    def _validate(self) -> PipelineSpec:
        ids = [s.stage_id for s in self.stages]
        if len(set(ids)) != len(ids):
            raise ValueError("stage_id 必须唯一")
        outputs = [s.output_key for s in self.stages]
        if len(set(outputs)) != len(outputs):
            raise ValueError("output_key 必须唯一")
        known = set(ids)
        for s in self.stages:
            if s.mode is StageMode.FANOUT:
                if s.driver_stage not in known:
                    raise ValueError(f"{s.stage_id}: driver_stage 不存在")
                if not s.partition_selector_id:
                    raise ValueError(f"{s.stage_id}: FANOUT 必须声明 partition_selector_id")
            if s.mode is StageMode.PER_PARTITION:
                if s.upstream_stage not in known:
                    raise ValueError(f"{s.stage_id}: upstream_stage 不存在")
                if not s.requirement_key or s.propagation_mode is None:
                    raise ValueError(f"{s.stage_id}: PER_PARTITION 需要 requirement/propagation")
        self._assert_acyclic()
        return self

    def _assert_acyclic(self) -> None:
        deps: dict[str, str | None] = {}
        for s in self.stages:
            deps[s.stage_id] = (
                s.driver_stage if s.mode is StageMode.FANOUT else s.upstream_stage
            )
        for start in deps:
            seen: set[str] = set()
            node: str | None = start
            while node is not None:
                if node in seen:
                    raise ValueError("PipelineSpec 存在环")
                seen.add(node)
                node = deps.get(node)

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
                partition_selector_id="storyboard_shots",
                partition_selector_version="1",
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


def _storyboard_shots(payload: Any) -> tuple[str, ...]:
    if isinstance(payload, StoryboardPayload):
        return tuple(sorted(s.shot_id for s in payload.shots))
    return ()


def golden_selectors() -> dict[str, PartitionSelector]:
    return {"storyboard_shots": _storyboard_shots}
