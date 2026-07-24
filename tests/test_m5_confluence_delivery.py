"""Step 7(其四):阶段汇合管理器 + 交付管理器。

阶段汇合:全部预期分区就绪 -> 阶段质检;撤销后暂停;返工新图像 -> 新一轮阶段质检。
交付:阶段质检 PASS -> 创建交付任务(一次);REWORK/BLOCK 不交付。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from studio.domain import ids as domain_ids
from studio.domain.artifacts import ArtifactRef
from studio.domain.enums import GateVerdict
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.production import identity
from studio.production.delivery_pm import DeliveryProcessManager
from studio.production.gate import GateDecidedEvt
from studio.production.payloads import (
    ArtifactAcceptanceRevokedEvt,
    ArtifactVersionAcceptedEvt,
    ArtifactVersionProposedEvt,
    CreateTaskAttemptCmd,
    StageExpandedEvt,
)
from studio.production.stage_confluence_pm import StageConfluenceProcessManager

_TS = datetime(2026, 1, 1, tzinfo=UTC)
_P = "proj-1"


def _image_ref(partition: str, revision: int) -> ArtifactRef:
    series = domain_ids.series_id(_P, "image", partition)
    return ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, revision), series_id=series,
        revision=revision, digest="a" * 64,
    )


def _plan_ref_for_expand() -> ArtifactRef:
    series = domain_ids.series_id(_P, "storyboard", None)
    return ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, 1), series_id=series,
        revision=1, digest="d" * 64,
    )


def _expanded(partitions: tuple[str, ...]) -> StageExpandedEvt:
    task_keys = tuple(sorted(identity.task_key(_P, "plan", p) for p in partitions))
    return StageExpandedEvt(
        project_id=_P, stage_id="plan", driver_ref=_plan_ref_for_expand(),
        partitions=tuple(sorted(partitions)), task_keys=task_keys,
    )


def _img_accepted(partition: str, revision: int) -> ArtifactVersionAcceptedEvt:
    ref = _image_ref(partition, revision)
    return ArtifactVersionAcceptedEvt(
        project_id=_P, series_id=ref.series_id, revision=revision, artifact_ref=ref,
        previous_current_ref=None, candidate_id=f"c-{partition}-{revision}",
        produced_by_attempt="att", output_key="image", partition_key=partition,
    )


def _img_revoked(partition: str, revision: int) -> ArtifactAcceptanceRevokedEvt:
    ref = _image_ref(partition, revision)
    return ArtifactAcceptanceRevokedEvt(
        project_id=_P, series_id=ref.series_id, revision=revision, artifact_ref=ref,
        report_ref="stage-report", reason="inconsistent", new_current_ref=None,
    )


def _stage_report_proposed(revision: int) -> ArtifactVersionProposedEvt:
    series = domain_ids.series_id(_P, "stage_qc", None)
    ref = ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, revision), series_id=series,
        revision=revision, digest="e" * 64,
    )
    return ArtifactVersionProposedEvt(
        project_id=_P, series_id=series, revision=revision, artifact_ref=ref,
        candidate_id=f"c-srep-{revision}", produced_by_attempt="qc-att",
        output_key="stage_qc", partition_key=None,
    )


def _stage_decided(revision: int, verdict: GateVerdict) -> GateDecidedEvt:
    series = domain_ids.series_id(_P, "stage_qc", None)
    report_ref = ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, revision), series_id=series,
        revision=revision, digest="e" * 64,
    )
    return GateDecidedEvt(
        report_ref=report_ref, target_ref=_image_ref("shot_02", 1),
        target_partition=None, verdict=verdict,
        rework_scope=() if verdict is GateVerdict.PASS else ("shot_02",),
        feedback="fb", policy_id="stage", policy_version="1",
    )


def _confluence() -> StageConfluenceProcessManager:
    return StageConfluenceProcessManager(
        expected_from_stage="plan", subject_output_key="image",
        subject_requirement_key="image:image", stage_qc_stage_id="stage_qc",
    )


def _delivery() -> DeliveryProcessManager:
    return DeliveryProcessManager(
        subject_output_key="image", subject_requirement_key="image:image",
        stage_qc_stage_id="stage_qc", delivery_stage_id="delivery",
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


# --- 阶段汇合 -------------------------------------------------------------- #


def test_confluence_waits_for_all_partitions() -> None:
    cmds = _run(
        _confluence(),
        [
            _expanded(("shot_01", "shot_02")),
            _img_accepted("shot_01", 1),  # 仅一个分区就绪
        ],
    )
    assert [c for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)] == []


def test_confluence_triggers_when_all_ready() -> None:
    cmds = _run(
        _confluence(),
        [
            _expanded(("shot_01", "shot_02")),
            _img_accepted("shot_01", 1),
            _img_accepted("shot_02", 1),
        ],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 1
    cmd = creates[0]
    assert cmd.stage_id == "stage_qc"
    assert cmd.partition_key is None
    # 聚合两镜头图像
    assert {b.partition_key for b in cmd.exact_refs} == {"shot_01", "shot_02"}


def test_confluence_same_image_set_not_retriggered() -> None:
    ev = _img_accepted("shot_02", 1)
    cmds = _run(
        _confluence(),
        [
            _expanded(("shot_01", "shot_02")),
            _img_accepted("shot_01", 1),
            ev, ev,  # 同图像集重复接受(重放)
        ],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 1


def test_confluence_rework_triggers_new_round() -> None:
    # 第一轮就绪 -> 触发;撤销 shot_02 v1 -> 暂停;shot_02 v2 接受 -> 第二轮触发
    cmds = _run(
        _confluence(),
        [
            _expanded(("shot_01", "shot_02")),
            _img_accepted("shot_01", 1),
            _img_accepted("shot_02", 1),  # 第一轮
            _img_revoked("shot_02", 1),
            _img_accepted("shot_02", 2),  # 第二轮(新图像集)
        ],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 2
    # 两轮阶段质检任务身份不同(绑定不同)
    assert creates[0].attempt_id != creates[1].attempt_id
    # 第二轮绑定含 shot_02 revision 2
    r2 = next(b for b in creates[1].exact_refs if b.partition_key == "shot_02")
    assert r2.revision == 2


def test_confluence_revoke_pauses_trigger() -> None:
    # 撤销后未补新版本 -> 不应触发新一轮
    cmds = _run(
        _confluence(),
        [
            _expanded(("shot_01", "shot_02")),
            _img_accepted("shot_01", 1),
            _img_accepted("shot_02", 1),
            _img_revoked("shot_02", 1),
        ],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 1  # 仅第一轮


# --- 交付 ------------------------------------------------------------------ #


def test_delivery_on_stage_pass() -> None:
    cmds = _run(
        _delivery(),
        [
            _img_accepted("shot_01", 1),
            _img_accepted("shot_02", 2),
            _stage_report_proposed(2),
            _stage_decided(2, GateVerdict.PASS),
        ],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 1
    cmd = creates[0]
    assert cmd.stage_id == "delivery"
    assert {b.partition_key for b in cmd.exact_refs} == {"shot_01", "shot_02"}


def test_delivery_not_on_rework() -> None:
    cmds = _run(
        _delivery(),
        [
            _img_accepted("shot_01", 1),
            _img_accepted("shot_02", 1),
            _stage_report_proposed(1),
            _stage_decided(1, GateVerdict.REWORK),
        ],
    )
    assert [c for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)] == []


def test_delivery_happens_once() -> None:
    passed = _stage_decided(2, GateVerdict.PASS)
    cmds = _run(
        _delivery(),
        [
            _img_accepted("shot_01", 1),
            _img_accepted("shot_02", 2),
            _stage_report_proposed(2),
            passed, passed,  # 重复
        ],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 1


def test_delivery_ignores_unknown_report() -> None:
    # 未追踪到阶段报告提议 -> 不交付
    cmds = _run(
        _delivery(),
        [
            _img_accepted("shot_01", 1),
            _img_accepted("shot_02", 2),
            _stage_decided(2, GateVerdict.PASS),  # 无对应 proposed
        ],
    )
    assert [c for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)] == []
