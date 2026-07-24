"""Step 7(其三):返工管理器 —— 相同输入强制重做(代数+1)+ 返工上限升级 + 幂等。"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from studio.domain import ids as domain_ids
from studio.domain.artifacts import ArtifactRef
from studio.domain.enums import ArtifactType, GateVerdict, PropagationMode
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.production import identity
from studio.production.gate import GateDecidedEvt, GatePolicy
from studio.production.payloads import (
    ArtifactVersionProposedEvt,
    CreateTaskAttemptCmd,
    EscalateAwaitHumanCmd,
    TaskAttemptCreatedEvt,
    TaskInputsBoundEvt,
)
from studio.production.quality import QCLayer, QCLayerSpec, QualityConfig
from studio.production.rework_pm import ReworkProcessManager
from studio.production.values import BindingItem

_TS = datetime(2026, 1, 1, tzinfo=UTC)
_P = "proj-1"


def _config(image_limit: int = 2) -> QualityConfig:
    layers = (
        QCLayerSpec(
            layer=QCLayer.RESULT, qc_stage_id="result_qc",
            subject_output_key="image", subject_artifact_type=ArtifactType.IMAGE,
        ),
        QCLayerSpec(
            layer=QCLayer.STAGE, qc_stage_id="stage_qc",
            subject_output_key="image", subject_artifact_type=ArtifactType.IMAGE,
        ),
    )
    return QualityConfig(
        layers=layers, gated_output_keys=frozenset({"image"}),
        policies={
            QCLayer.RESULT: GatePolicy(policy_id="result", policy_version="1"),
            QCLayer.STAGE: GatePolicy(policy_id="stage", policy_version="1"),
        },
        rework_limits={"image": image_limit},
    )


def _plan_binding(partition: str) -> BindingItem:
    series = domain_ids.series_id(_P, "plan", partition)
    ref = ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, 1), series_id=series,
        revision=1, digest="b" * 64,
    )
    return BindingItem.from_ref(
        requirement_key="image_plan:plan", logical_slot="plan",
        partition_key=partition, ref=ref,
        propagation_mode=PropagationMode.PARTITION_PRESERVING,
    )


def _image_attempt_id(partition: str, generation: int) -> str:
    tk = identity.task_key(_P, "image", partition)
    return identity.attempt_id(
        tk, identity.input_binding_digest((_plan_binding(partition),)), generation
    )


def _image_ref(partition: str, revision: int) -> ArtifactRef:
    series = domain_ids.series_id(_P, "image", partition)
    return ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, revision), series_id=series,
        revision=revision, digest="a" * 64,
    )


def _created(partition: str, generation: int) -> TaskAttemptCreatedEvt:
    return TaskAttemptCreatedEvt(
        attempt_id=_image_attempt_id(partition, generation), project_id=_P,
        stage_id="image", partition_key=partition, output_key="image",
        series_id=domain_ids.series_id(_P, "image", partition),
        execution_generation=generation,
    )


def _bound(partition: str, generation: int) -> TaskInputsBoundEvt:
    return TaskInputsBoundEvt(
        attempt_id=_image_attempt_id(partition, generation),
        exact_refs=(_plan_binding(partition),),
    )


def _proposed(partition: str, generation: int, revision: int) -> ArtifactVersionProposedEvt:
    ref = _image_ref(partition, revision)
    return ArtifactVersionProposedEvt(
        project_id=_P, series_id=ref.series_id, revision=revision, artifact_ref=ref,
        candidate_id=f"c-{partition}-r{revision}",
        produced_by_attempt=_image_attempt_id(partition, generation),
        output_key="image", partition_key=partition,
    )


def _stage_rework(scope: tuple[str, ...], anchor_ref: ArtifactRef) -> GateDecidedEvt:
    return GateDecidedEvt(
        report_ref=_ref_stage_report(1), target_ref=anchor_ref, target_partition=None,
        verdict=GateVerdict.REWORK, rework_scope=scope, feedback="cross-shot",
        policy_id="stage", policy_version="1",
    )


def _ref_stage_report(revision: int) -> ArtifactRef:
    series = domain_ids.series_id(_P, "stage_qc", None)
    return ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, revision), series_id=series,
        revision=revision, digest="c" * 64,
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


def test_stage_rework_creates_same_input_next_generation() -> None:
    cmds = _run(
        ReworkProcessManager(_config()),
        [
            _created("shot_01", 0), _bound("shot_01", 0), _proposed("shot_01", 0, 1),
            _created("shot_02", 0), _bound("shot_02", 0), _proposed("shot_02", 0, 1),
            _stage_rework(("shot_02",), _image_ref("shot_02", 1)),
        ],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 1  # 仅 shot_02 重做
    cmd = creates[0]
    assert cmd.partition_key == "shot_02"
    assert cmd.execution_generation == 1  # 代数 +1
    # 相同输入:绑定与原始一致
    assert cmd.exact_refs == (_plan_binding("shot_02"),)
    # 新 attempt_id(代数变化)
    assert cmd.attempt_id == _image_attempt_id("shot_02", 1)
    assert cmd.attempt_id != _image_attempt_id("shot_02", 0)
    # 返工血缘
    assert cmd.rework_of_attempt == _image_attempt_id("shot_02", 0)
    assert cmd.rework_report_ref == _ref_stage_report(1).artifact_id


def test_new_generation_yields_new_operation_id() -> None:
    op0 = identity.operation_id(_image_attempt_id("shot_02", 0), "shot_02:image:v0")
    op1 = identity.operation_id(_image_attempt_id("shot_02", 1), "shot_02:image:v0")
    assert op0 != op1  # 允许合理二次扣费


def test_rework_scope_isolates_partition() -> None:
    # 范围仅 shot_02:shot_01 不得被重做
    cmds = _run(
        ReworkProcessManager(_config()),
        [
            _created("shot_01", 0), _bound("shot_01", 0), _proposed("shot_01", 0, 1),
            _created("shot_02", 0), _bound("shot_02", 0), _proposed("shot_02", 0, 1),
            _stage_rework(("shot_02",), _image_ref("shot_02", 1)),
        ],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert {c.partition_key for c in creates} == {"shot_02"}


def test_duplicate_gate_decided_is_idempotent() -> None:
    ev = _stage_rework(("shot_02",), _image_ref("shot_02", 1))
    cmds = _run(
        ReworkProcessManager(_config()),
        [
            _created("shot_02", 0), _bound("shot_02", 0), _proposed("shot_02", 0, 1),
            ev, ev,  # 重复投递
        ],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 1  # 幂等:不重复创建


def test_rework_limit_escalates_to_await_human() -> None:
    # 上限=1:gen0 已存在,gen1 一次重做 OK,再对 gen1 返工则 next_gen=2 > 1 -> 升级
    cmds = _run(
        ReworkProcessManager(_config(image_limit=1)),
        [
            _created("shot_02", 0), _bound("shot_02", 0), _proposed("shot_02", 0, 1),
            _created("shot_02", 1), _bound("shot_02", 1), _proposed("shot_02", 1, 2),
            _stage_rework(("shot_02",), _image_ref("shot_02", 2)),
        ],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    escalations = [
        c.payload for c in cmds if isinstance(c.payload, EscalateAwaitHumanCmd)
    ]
    assert creates == []  # 不再自动重做
    assert len(escalations) == 1
    assert escalations[0].stage_id == "image"
    assert escalations[0].partition_key == "shot_02"
    assert escalations[0].generation == 2


def test_within_limit_still_reworks() -> None:
    # 上限=2:gen1 存在,对其返工 next_gen=2 <= 2 -> 仍重做
    cmds = _run(
        ReworkProcessManager(_config(image_limit=2)),
        [
            _created("shot_02", 0), _bound("shot_02", 0), _proposed("shot_02", 0, 1),
            _created("shot_02", 1), _bound("shot_02", 1), _proposed("shot_02", 1, 2),
            _stage_rework(("shot_02",), _image_ref("shot_02", 2)),
        ],
    )
    creates = [c.payload for c in cmds if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 1
    assert creates[0].execution_generation == 2


def test_non_rework_verdict_ignored() -> None:
    passed = GateDecidedEvt(
        report_ref=_ref_stage_report(1), target_ref=_image_ref("shot_02", 1),
        target_partition=None, verdict=GateVerdict.PASS, rework_scope=(),
        feedback="ok", policy_id="stage", policy_version="1",
    )
    cmds = _run(
        ReworkProcessManager(_config()),
        [
            _created("shot_02", 0), _bound("shot_02", 0), _proposed("shot_02", 0, 1),
            passed,
        ],
    )
    assert cmds == []
