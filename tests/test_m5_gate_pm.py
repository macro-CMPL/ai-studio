"""Step 7(其二):闸门决策管理器 —— 报告接受→跑策略→记决策→应用结局(接受/拒绝/撤销)。

两跳:
  报告被接受 -> DecideGateCmd(第一跳)
  GateDecided 回流 -> 接受 / 拒绝 / 撤销(第二跳)
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from studio.domain import ids as domain_ids
from studio.domain.artifacts import ArtifactRef, QCFinding, QCReportPayload
from studio.domain.enums import ArtifactType, GateVerdict, Severity
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.production.gate import DecideGateCmd, GateDecidedEvt, GatePolicy
from studio.production.gate_pm import GateDecisionProcessManager
from studio.production.payloads import (
    AcceptArtifactVersionCmd,
    ArtifactCandidateProducedEvt,
    ArtifactVersionAcceptedEvt,
    ArtifactVersionProposedEvt,
    RejectArtifactVersionCmd,
    RevokeArtifactAcceptanceCmd,
)
from studio.production.quality import QCLayer, QCLayerSpec, QualityConfig
from studio.serialization import digest

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
        layers=layers, gated_output_keys=frozenset({"plan", "image"}),
        policies=policies, rework_limits={"image": 2},
    )


def _ref(output_key: str, partition: str | None, revision: int) -> ArtifactRef:
    series = domain_ids.series_id(_P, output_key, partition)
    return ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, revision),
        series_id=series, revision=revision, digest="a" * 64,
    )


def _proposed(
    output_key: str, partition: str | None, revision: int, candidate: str
) -> ArtifactVersionProposedEvt:
    ref = _ref(output_key, partition, revision)
    return ArtifactVersionProposedEvt(
        project_id=_P, series_id=ref.series_id, revision=revision, artifact_ref=ref,
        candidate_id=candidate, produced_by_attempt="att", output_key=output_key,
        partition_key=partition,
    )


def _accepted(
    output_key: str, partition: str | None, revision: int, candidate: str
) -> ArtifactVersionAcceptedEvt:
    ref = _ref(output_key, partition, revision)
    return ArtifactVersionAcceptedEvt(
        project_id=_P, series_id=ref.series_id, revision=revision, artifact_ref=ref,
        previous_current_ref=None, candidate_id=candidate, produced_by_attempt="att",
        output_key=output_key, partition_key=partition,
    )


def _report_payload(
    subject: ArtifactRef, partition: str | None, *, passed: bool,
    rework_scope: tuple[str, ...] = (), rule_id: str = "r1",
) -> QCReportPayload:
    findings: tuple[QCFinding, ...] = ()
    if not passed:
        findings = (
            QCFinding(
                rule_id=rule_id, severity=Severity.ERROR, description="d",
                suggested_action="fix", target_partition=partition,
            ),
        )
    return QCReportPayload(
        subject_refs=(subject,), target_partition=partition, evaluator="ev",
        evaluator_version="1", criteria_version="1", passed=passed,
        findings=findings, rework_scope=rework_scope, feedback="fb",
    )


def _report_candidate(
    qc_stage_id: str, partition: str | None, candidate: str, payload: QCReportPayload
) -> ArtifactCandidateProducedEvt:
    series = domain_ids.series_id(_P, qc_stage_id, partition)
    return ArtifactCandidateProducedEvt(
        candidate_id=candidate, attempt_id="qc-att", project_id=_P, series_id=series,
        output_key=qc_stage_id, partition_key=partition,
        digest=digest(payload),
        payload=payload,
    )


def _report_accepted(
    qc_stage_id: str, partition: str | None, revision: int, candidate: str
) -> ArtifactVersionAcceptedEvt:
    return _accepted(qc_stage_id, partition, revision, candidate)


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
# 提示词层:PASS -> 接受
# --------------------------------------------------------------------------- #


def test_prompt_pass_accepts_plan() -> None:
    plan_ref = _ref("plan", "shot_01", 1)
    report = _report_payload(plan_ref, "shot_01", passed=True)
    cmds = _run(
        GateDecisionProcessManager(_config()),
        [
            _proposed("plan", "shot_01", 1, "c-plan"),
            _report_candidate("prompt_qc", "shot_01", "c-rep", report),
            _proposed("prompt_qc", "shot_01", 1, "c-rep"),
            _report_accepted("prompt_qc", "shot_01", 1, "c-rep"),
        ],
    )
    # 第一跳:DecideGate(PASS)
    decides = [c.payload for c in cmds if isinstance(c.payload, DecideGateCmd)]
    assert len(decides) == 1 and decides[0].verdict is GateVerdict.PASS
    # 第二跳需要 GateDecided 回流
    gate_decided = GateDecidedEvt(
        report_ref=decides[0].report_ref, target_ref=decides[0].target_ref,
        target_partition=decides[0].target_partition, verdict=GateVerdict.PASS,
        rework_scope=(), feedback="fb", policy_id="prompt", policy_version="1",
    )
    cmds2 = _run(
        GateDecisionProcessManager(_config()),
        [
            _proposed("plan", "shot_01", 1, "c-plan"),
            _report_candidate("prompt_qc", "shot_01", "c-rep", report),
            _proposed("prompt_qc", "shot_01", 1, "c-rep"),
            _report_accepted("prompt_qc", "shot_01", 1, "c-rep"),
            gate_decided,
        ],
    )
    accepts = [c.payload for c in cmds2 if isinstance(c.payload, AcceptArtifactVersionCmd)]
    assert len(accepts) == 1
    assert accepts[0].candidate_id == "c-plan"
    assert accepts[0].series_id == plan_ref.series_id


# --------------------------------------------------------------------------- #
# 结果层:REWORK -> 拒绝
# --------------------------------------------------------------------------- #


def test_result_rework_rejects_image() -> None:
    img_ref = _ref("image", "shot_02", 1)
    report = _report_payload(
        img_ref, "shot_02", passed=False, rework_scope=("shot_02",)
    )
    decided = GateDecidedEvt(
        report_ref=_ref("result_qc", "shot_02", 1), target_ref=img_ref,
        target_partition="shot_02", verdict=GateVerdict.REWORK,
        rework_scope=("shot_02",), feedback="bad", policy_id="result",
        policy_version="1",
    )
    cmds = _run(
        GateDecisionProcessManager(_config()),
        [
            _proposed("image", "shot_02", 1, "c-img"),
            _report_candidate("result_qc", "shot_02", "c-rep", report),
            _proposed("result_qc", "shot_02", 1, "c-rep"),
            _report_accepted("result_qc", "shot_02", 1, "c-rep"),
            decided,
        ],
    )
    rejects = [c.payload for c in cmds if isinstance(c.payload, RejectArtifactVersionCmd)]
    assert len(rejects) == 1
    assert rejects[0].candidate_id == "c-img"


# --------------------------------------------------------------------------- #
# 阶段层:REWORK 精确撤销 shot_02,shot_01 不受影响
# --------------------------------------------------------------------------- #


def test_stage_rework_revokes_only_scoped_partition() -> None:
    img1 = _ref("image", "shot_01", 1)
    img2 = _ref("image", "shot_02", 1)
    # 阶段报告主体锚点取 shot_02(仅审计),范围 = [shot_02]
    report = _report_payload(
        img2, None, passed=False, rework_scope=("shot_02",), rule_id="cross_shot"
    )
    decided = GateDecidedEvt(
        report_ref=_ref("stage_qc", None, 1), target_ref=img2,
        target_partition=None, verdict=GateVerdict.REWORK,
        rework_scope=("shot_02",), feedback="inconsistent", policy_id="stage",
        policy_version="1",
    )
    cmds = _run(
        GateDecisionProcessManager(_config()),
        [
            # 两镜头图像已接受(结果层通过后)
            _accepted("image", "shot_01", 1, "c-img1"),
            _accepted("image", "shot_02", 1, "c-img2"),
            _report_candidate("stage_qc", None, "c-srep", report),
            _proposed("stage_qc", None, 1, "c-srep"),
            _report_accepted("stage_qc", None, 1, "c-srep"),
            decided,
        ],
    )
    revokes = [
        c.payload for c in cmds if isinstance(c.payload, RevokeArtifactAcceptanceCmd)
    ]
    assert len(revokes) == 1  # 仅 shot_02 被撤销
    assert revokes[0].artifact_ref == img2
    assert revokes[0].series_id == img2.series_id
    # shot_01 绝不在撤销集合里
    assert all(r.artifact_ref != img1 for r in revokes)


def test_stage_pass_no_revoke() -> None:
    img2 = _ref("image", "shot_02", 2)
    report = _report_payload(img2, None, passed=True)
    decided = GateDecidedEvt(
        report_ref=_ref("stage_qc", None, 2), target_ref=img2, target_partition=None,
        verdict=GateVerdict.PASS, rework_scope=(), feedback="ok",
        policy_id="stage", policy_version="1",
    )
    cmds = _run(
        GateDecisionProcessManager(_config()),
        [
            _accepted("image", "shot_02", 2, "c-img2"),
            _report_candidate("stage_qc", None, "c-srep2", report),
            _proposed("stage_qc", None, 2, "c-srep2"),
            _report_accepted("stage_qc", None, 2, "c-srep2"),
            decided,
        ],
    )
    assert [c for c in cmds if isinstance(c.payload, RevokeArtifactAcceptanceCmd)] == []


# --------------------------------------------------------------------------- #
# 幂等
# --------------------------------------------------------------------------- #


def test_decide_idempotent_on_report_replay() -> None:
    plan_ref = _ref("plan", "shot_01", 1)
    report = _report_payload(plan_ref, "shot_01", passed=True)
    accepted = _report_accepted("prompt_qc", "shot_01", 1, "c-rep")
    cmds = _run(
        GateDecisionProcessManager(_config()),
        [
            _proposed("plan", "shot_01", 1, "c-plan"),
            _report_candidate("prompt_qc", "shot_01", "c-rep", report),
            _proposed("prompt_qc", "shot_01", 1, "c-rep"),
            accepted,
            accepted,  # 重放
        ],
    )
    decides = [c.payload for c in cmds if isinstance(c.payload, DecideGateCmd)]
    assert len(decides) == 1  # 幂等:只决策一次


def test_apply_idempotent_on_gate_decided_replay() -> None:
    plan_ref = _ref("plan", "shot_01", 1)
    report = _report_payload(plan_ref, "shot_01", passed=True)
    decided = GateDecidedEvt(
        report_ref=_ref("prompt_qc", "shot_01", 1), target_ref=plan_ref,
        target_partition="shot_01", verdict=GateVerdict.PASS, rework_scope=(),
        feedback="fb", policy_id="prompt", policy_version="1",
    )
    cmds = _run(
        GateDecisionProcessManager(_config()),
        [
            _proposed("plan", "shot_01", 1, "c-plan"),
            _report_candidate("prompt_qc", "shot_01", "c-rep", report),
            _proposed("prompt_qc", "shot_01", 1, "c-rep"),
            _report_accepted("prompt_qc", "shot_01", 1, "c-rep"),
            decided,
            decided,  # 重放
        ],
    )
    accepts = [c.payload for c in cmds if isinstance(c.payload, AcceptArtifactVersionCmd)]
    assert len(accepts) == 1  # 幂等:只接受一次
