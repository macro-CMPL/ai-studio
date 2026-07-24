"""Step 8:M5 完整视频流程编译 —— 质检/交付为外部调度,不参与自动展开;plan/image 门控。"""

from __future__ import annotations

from studio.domain.enums import (
    ArtifactType,
    CardinalityKind,
    ControlRole,
    ExecutorKind,
    PartitioningKind,
    PropagationMode,
)
from studio.domain.stages import Cardinality, Requirement
from studio.production.compile import CompilationError, compile_from
from studio.production.pipeline import _qc_stage, golden_m5_compiled
from studio.production.quality import golden_m5_quality_config


def test_m5_pipeline_compiles() -> None:
    spec = golden_m5_compiled()
    ids = {s.stage_id for s in spec.stages}
    assert ids == {
        "storyboard", "plan", "image", "prompt_qc", "result_qc", "stage_qc", "delivery"
    }


def test_qc_and_delivery_are_externally_scheduled() -> None:
    spec = golden_m5_compiled()
    for sid in ("prompt_qc", "result_qc", "stage_qc", "delivery"):
        stage = spec.by_stage(sid)
        assert stage is not None and stage.externally_scheduled is True
    for sid in ("storyboard", "plan", "image"):
        stage = spec.by_stage(sid)
        assert stage is not None and stage.externally_scheduled is False


def test_externally_scheduled_excluded_from_bootstrap() -> None:
    spec = golden_m5_compiled()
    # 仅 storyboard 是可 bootstrap 的 ROOT(质检/交付虽 singleton 但外部调度,不参与)
    roots = {s.stage_id for s in spec.root_stages()}
    assert roots == {"storyboard"}


def test_image_consumers_exclude_qc_and_delivery() -> None:
    spec = golden_m5_compiled()
    # image 的自动装配消费者里不得出现 result_qc / stage_qc / delivery
    consumers = {s.stage_id for s, _ in spec.consumers_of("image")}
    assert "result_qc" not in consumers
    assert "stage_qc" not in consumers
    assert "delivery" not in consumers


def test_plan_and_image_are_gated() -> None:
    spec = golden_m5_compiled()
    assert spec.by_stage("plan").gated is True  # type: ignore[union-attr]
    assert spec.by_stage("image").gated is True  # type: ignore[union-attr]
    assert spec.by_stage("storyboard").gated is False  # type: ignore[union-attr]


def test_externally_scheduled_with_requires_rejected() -> None:
    # 外部调度阶段声明 requires 应在编译期被拒
    bad = _qc_stage("prompt_qc", PartitioningKind.INHERIT_PARTITION).model_copy(
        update={
            "requires": (
                Requirement(
                    artifact_type=ArtifactType.IMAGE_PLAN,
                    logical_slot="plan",
                    cardinality=Cardinality(kind=CardinalityKind.STATIC),
                    propagation_mode=PropagationMode.PARTITION_PRESERVING,
                ),
            )
        }
    )
    try:
        compile_from((bad,))
    except CompilationError as e:
        assert "外部调度" in str(e)
    else:
        raise AssertionError("应拒绝声明 requires 的外部调度阶段")


def test_quality_config_layers_and_limits() -> None:
    config = golden_m5_quality_config()
    assert config.is_gated("plan") and config.is_gated("image")
    assert not config.is_gated("storyboard")
    assert config.rework_limit("image") == 2
    assert config.layer_by_subject("plan") is not None
    assert config.layer_by_qc_stage("stage_qc") is not None


def test_qc_stage_metadata() -> None:
    spec = golden_m5_compiled()
    prompt = spec.by_stage("prompt_qc")
    assert prompt is not None
    assert prompt.executor_kind is ExecutorKind.TRANSFORM
    assert prompt.control_role is ControlRole.EVALUATOR
    assert prompt.output_artifact_type is ArtifactType.QC_REPORT
