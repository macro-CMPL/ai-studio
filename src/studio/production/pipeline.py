"""Golden 流水线:以 M1 StageSpec 定义,再编译成 CompiledPipelineSpec(单一定义源)。

分区提取用可配置、带 version 的 partition selector,按 (id, version) 索引。
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from studio.domain.artifacts import StoryboardPayload
from studio.domain.enums import (
    ArtifactType,
    CardinalityKind,
    ControlRole,
    CostMode,
    ExecutorKind,
    PartitioningKind,
    PropagationMode,
    ToolEffectLevel,
)
from studio.domain.stages import (
    Cardinality,
    OutputSpec,
    Partitioning,
    Requirement,
    StageSpec,
)

from .compile import CompiledPipelineSpec, compile_from

PartitionSelector = Callable[[Any], tuple[str, ...]]


def golden_stagespecs() -> tuple[StageSpec, ...]:
    return (
        StageSpec(
            stage_id="storyboard",
            executor_kind=ExecutorKind.AGENT,
            control_role=ControlRole.PRODUCER,
            allowed_tool_effects=frozenset({ToolEffectLevel.PURE}),
            cost_mode=CostMode.FREE,
            requires=(),
            produces=(
                OutputSpec(
                    artifact_type=ArtifactType.STORYBOARD,
                    logical_slot="storyboard",
                    schema_version=1,
                    partitioning=Partitioning(kind=PartitioningKind.SINGLETON),
                ),
            ),
        ),
        StageSpec(
            stage_id="plan",
            executor_kind=ExecutorKind.AGENT,
            control_role=ControlRole.PRODUCER,
            allowed_tool_effects=frozenset({ToolEffectLevel.PURE}),
            cost_mode=CostMode.FREE,
            requires=(
                Requirement(
                    artifact_type=ArtifactType.STORYBOARD,
                    logical_slot="storyboard",
                    cardinality=Cardinality(
                        kind=CardinalityKind.DYNAMIC_PARTITION_BY, partition_by="shot_id"
                    ),
                    propagation_mode=PropagationMode.AGGREGATE,
                    partition_selector_id="storyboard_shots",
                    partition_selector_version="1",
                ),
            ),
            produces=(
                OutputSpec(
                    artifact_type=ArtifactType.IMAGE_PLAN,
                    logical_slot="plan",
                    schema_version=1,
                    partitioning=Partitioning(
                        kind=PartitioningKind.DYNAMIC_FROM, from_key="shot_id"
                    ),
                ),
            ),
        ),
        StageSpec(
            stage_id="image",
            executor_kind=ExecutorKind.PROVIDER,
            control_role=ControlRole.PRODUCER,
            allowed_tool_effects=frozenset({ToolEffectLevel.COSTED}),
            cost_mode=CostMode.METERED,
            requires=(
                Requirement(
                    artifact_type=ArtifactType.IMAGE_PLAN,
                    logical_slot="plan",
                    cardinality=Cardinality(kind=CardinalityKind.STATIC),
                    propagation_mode=PropagationMode.PARTITION_PRESERVING,
                ),
            ),
            produces=(
                OutputSpec(
                    artifact_type=ArtifactType.IMAGE,
                    logical_slot="image",
                    schema_version=1,
                    partitioning=Partitioning(kind=PartitioningKind.INHERIT_PARTITION),
                ),
            ),
        ),
    )


def golden_compiled() -> CompiledPipelineSpec:
    return compile_from(golden_stagespecs())


def _storyboard_shots(payload: Any) -> tuple[str, ...]:
    if isinstance(payload, StoryboardPayload):
        return tuple(sorted(s.shot_id for s in payload.shots))
    return ()


def golden_selectors() -> dict[tuple[str, str], PartitionSelector]:
    return {("storyboard_shots", "1"): _storyboard_shots}
