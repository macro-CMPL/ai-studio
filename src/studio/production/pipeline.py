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


def _storyboard_stage() -> StageSpec:
    return StageSpec(
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
    )


def _plan_stage(gated: bool = False) -> StageSpec:
    return StageSpec(
        stage_id="plan",
        executor_kind=ExecutorKind.AGENT,
        control_role=ControlRole.PRODUCER,
        allowed_tool_effects=frozenset({ToolEffectLevel.PURE}),
        cost_mode=CostMode.FREE,
        gated=gated,
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
    )


def _image_stage(
    executor_kind: ExecutorKind,
    cost_mode: CostMode,
    tool_effect: ToolEffectLevel,
    gated: bool = False,
) -> StageSpec:
    return StageSpec(
        stage_id="image",
        executor_kind=executor_kind,
        control_role=ControlRole.PRODUCER,
        allowed_tool_effects=frozenset({tool_effect}),
        cost_mode=cost_mode,
        gated=gated,
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
    )


def golden_stagespecs() -> tuple[StageSpec, ...]:
    """M4 Golden:image 是付费 PROVIDER stage(异步)。"""
    return (
        _storyboard_stage(),
        _plan_stage(),
        _image_stage(ExecutorKind.PROVIDER, CostMode.METERED, ToolEffectLevel.COSTED),
    )


def m3_stagespecs() -> tuple[StageSpec, ...]:
    """M3 编排测试:image 是本地 TRANSFORM leaf(可同步产出),不涉及付费 provider。"""
    return (
        _storyboard_stage(),
        _plan_stage(),
        _image_stage(ExecutorKind.TRANSFORM, CostMode.FREE, ToolEffectLevel.PURE),
    )


# --------------------------------------------------------------------------- #
# M5 完整视频流程:在 M4 基础上加三层质检(提示词/结果/阶段)与交付。
# 质检与交付阶段由 M5 进程管理器显式调度(externally_scheduled),不参与自动展开;
# plan / image 为门控产物(gated),提议后须经闸门决策接受。
# --------------------------------------------------------------------------- #


def _qc_stage(stage_id: str, partitioning: PartitioningKind) -> StageSpec:
    """质检评价阶段:确定性 TRANSFORM 评价器,产出 QC_REPORT(外部调度)。"""
    return StageSpec(
        stage_id=stage_id,
        executor_kind=ExecutorKind.TRANSFORM,
        control_role=ControlRole.EVALUATOR,
        allowed_tool_effects=frozenset({ToolEffectLevel.PURE}),
        cost_mode=CostMode.FREE,
        externally_scheduled=True,
        requires=(),
        produces=(
            OutputSpec(
                artifact_type=ArtifactType.QC_REPORT,
                logical_slot=stage_id,
                schema_version=1,
                partitioning=Partitioning(kind=partitioning),
            ),
        ),
    )


def _delivery_stage() -> StageSpec:
    """交付阶段:确定性 TRANSFORM,聚合已接受图像产出交付包(外部调度)。"""
    return StageSpec(
        stage_id="delivery",
        executor_kind=ExecutorKind.TRANSFORM,
        control_role=ControlRole.PRODUCER,
        allowed_tool_effects=frozenset({ToolEffectLevel.PURE}),
        cost_mode=CostMode.FREE,
        externally_scheduled=True,
        requires=(),
        produces=(
            OutputSpec(
                artifact_type=ArtifactType.DELIVERY,
                logical_slot="delivery",
                schema_version=1,
                partitioning=Partitioning(kind=PartitioningKind.SINGLETON),
            ),
        ),
    )


def golden_m5_stagespecs() -> tuple[StageSpec, ...]:
    """M5 完整流程:storyboard -> plan(gated) -> image(PROVIDER, gated)
    + prompt_qc / result_qc / stage_qc + delivery(均外部调度)。"""
    return (
        _storyboard_stage(),
        _plan_stage(gated=True),
        _image_stage(
            ExecutorKind.PROVIDER, CostMode.METERED, ToolEffectLevel.COSTED, gated=True
        ),
        _qc_stage("prompt_qc", PartitioningKind.INHERIT_PARTITION),
        _qc_stage("result_qc", PartitioningKind.INHERIT_PARTITION),
        _qc_stage("stage_qc", PartitioningKind.SINGLETON),
        _delivery_stage(),
    )


def golden_m5_compiled() -> CompiledPipelineSpec:
    return compile_from(golden_m5_stagespecs())


def golden_compiled() -> CompiledPipelineSpec:
    return compile_from(golden_stagespecs())


def m3_compiled() -> CompiledPipelineSpec:
    return compile_from(m3_stagespecs())


def _storyboard_shots(payload: Any) -> tuple[str, ...]:
    if isinstance(payload, StoryboardPayload):
        return tuple(sorted(s.shot_id for s in payload.shots))
    return ()


def golden_selectors() -> dict[tuple[str, str], PartitionSelector]:
    return {("storyboard_shots", "1"): _storyboard_shots}
