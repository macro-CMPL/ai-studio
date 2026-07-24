"""Step 7(其一):质量评价调度管理器 —— 提示词/结果层质检任务调度 + 幂等/隔离。"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from studio.domain import ids as domain_ids
from studio.domain.enums import ArtifactType
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.production import identity
from studio.production.gate import GatePolicy
from studio.production.payloads import (
    ArtifactVersionProposedEvt,
    CreateTaskAttemptCmd,
)
from studio.production.qc_scheduling_pm import QCEvaluationSchedulingProcessManager
from studio.production.quality import QCLayer, QCLayerSpec, QualityConfig

_TS = datetime(2026, 1, 1, tzinfo=UTC)
_P = "proj-1"


def _config() -> QualityConfig:
    layers = (
        QCLayerSpec(
            layer=QCLayer.PROMPT, qc_stage_id="prompt_qc",
            subject_output_key="plan", subject_artifact_type=ArtifactType.IMAGE_PLAN,
        ),
        QCLayerSpec(
            layer=QCLayer.RESULT, qc_stage_id="result_qc",
            subject_output_key="image", subject_artifact_type=ArtifactType.IMAGE,
        ),
        QCLayerSpec(
            layer=QCLayer.STAGE, qc_stage_id="stage_qc",
            subject_output_key="image", subject_artifact_type=ArtifactType.IMAGE,
        ),
    )
    policies = {
        QCLayer.PROMPT: GatePolicy(policy_id="prompt", policy_version="1"),
        QCLayer.RESULT: GatePolicy(policy_id="result", policy_version="1"),
        QCLayer.STAGE: GatePolicy(policy_id="stage", policy_version="1"),
    }
    return QualityConfig(
        layers=layers,
        gated_output_keys=frozenset({"plan", "image"}),
        policies=policies,
        rework_limits={"image": 2},
    )


def _proposed(
    output_key: str, partition: str | None, revision: int, *, candidate: str
) -> ArtifactVersionProposedEvt:
    series = domain_ids.series_id(_P, output_key, partition)
    from studio.domain.artifacts import ArtifactRef

    ref = ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, revision),
        series_id=series, revision=revision, digest="a" * 64,
    )
    return ArtifactVersionProposedEvt(
        project_id=_P, series_id=series, revision=revision, artifact_ref=ref,
        candidate_id=candidate, produced_by_attempt="att", output_key=output_key,
        partition_key=partition,
    )


def _run(pm: Any, payloads: list[MessagePayload]) -> list[Any]:
    state = pm.initial_state()
    commands: list[Any] = []
    for pos, payload in enumerate(payloads):
        env: EventEnvelope[MessagePayload] = EventEnvelope(
            event_id=f"evt-{pos}", schema_version=1, stream_id="s", sequence=pos,
            global_position=pos, correlation_id="c", causation_id="x",
            recorded_at=_TS, payload=payload,
        )
        reaction = pm.react(state, env)
        state = reaction.state
        commands.extend(reaction.commands)
    return commands


# --------------------------------------------------------------------------- #


def test_plan_proposed_schedules_prompt_qc() -> None:
    cmds = _run(
        QCEvaluationSchedulingProcessManager(_config()),
        [_proposed("plan", "shot_01", 1, candidate="c-plan")],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 1
    cmd = creates[0]
    assert cmd.stage_id == "prompt_qc"
    assert cmd.output_key == "prompt_qc"
    assert cmd.partition_key == "shot_01"
    # 输入绑定精确指向被检查的 plan 版本
    assert len(cmd.exact_refs) == 1
    assert cmd.exact_refs[0].logical_slot == "plan"
    assert cmd.exact_refs[0].requirement_key == "image_plan:plan"
    # attempt_id / series_id 与派生一致
    series = domain_ids.series_id(_P, "prompt_qc", "shot_01")
    assert cmd.series_id == series
    tk = identity.task_key(_P, "prompt_qc", "shot_01")
    assert cmd.attempt_id == identity.attempt_id(
        tk, identity.input_binding_digest(cmd.exact_refs), 0
    )


def test_image_proposed_schedules_result_qc() -> None:
    cmds = _run(
        QCEvaluationSchedulingProcessManager(_config()),
        [_proposed("image", "shot_01", 1, candidate="c-img")],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 1
    assert creates[0].stage_id == "result_qc"
    assert creates[0].exact_refs[0].requirement_key == "image:image"


def test_same_version_scheduled_once() -> None:
    ev = _proposed("image", "shot_01", 1, candidate="c-img")
    cmds = _run(QCEvaluationSchedulingProcessManager(_config()), [ev, ev])
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 1  # 幂等:同一 subject 版本只调度一次


def test_rework_new_version_schedules_again() -> None:
    # 返工产生新图像版本(revision=2, 新 artifact_id) -> 再次调度结果质检
    cmds = _run(
        QCEvaluationSchedulingProcessManager(_config()),
        [
            _proposed("image", "shot_02", 1, candidate="c-v1"),
            _proposed("image", "shot_02", 2, candidate="c-v2"),
        ],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 2
    assert {c.exact_refs[0].revision for c in creates} == {1, 2}


def test_non_subject_output_key_ignored() -> None:
    # storyboard / qc_report 自身不是被检查主体 -> 不调度
    cmds = _run(
        QCEvaluationSchedulingProcessManager(_config()),
        [
            _proposed("storyboard", None, 1, candidate="c-sb"),
            _proposed("prompt_qc", "shot_01", 1, candidate="c-report"),
        ],
    )
    assert [c for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)] == []


def test_result_layer_scheduled_regardless_of_layer_order() -> None:
    # 即便阶段层(subject 亦为 image)排在结果层之前,image 提议仍应调度结果质检。
    layers = (
        QCLayerSpec(
            layer=QCLayer.STAGE, qc_stage_id="stage_qc",
            subject_output_key="image", subject_artifact_type=ArtifactType.IMAGE,
        ),
        QCLayerSpec(
            layer=QCLayer.RESULT, qc_stage_id="result_qc",
            subject_output_key="image", subject_artifact_type=ArtifactType.IMAGE,
        ),
    )
    config = QualityConfig(
        layers=layers,
        gated_output_keys=frozenset({"image"}),
        policies={
            QCLayer.STAGE: GatePolicy(policy_id="stage", policy_version="1"),
            QCLayer.RESULT: GatePolicy(policy_id="result", policy_version="1"),
        },
        rework_limits={},
    )
    cmds = _run(
        QCEvaluationSchedulingProcessManager(config),
        [_proposed("image", "shot_01", 1, candidate="c-img")],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 1
    assert creates[0].stage_id == "result_qc"  # 结果层,而非阶段层
