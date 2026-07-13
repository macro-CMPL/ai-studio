"""M3 hardening 对抗测试:串项目、身份不变式、可重复展开、Lineage 双向、多输入累计。"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from kernel_helpers import FakeClock
from production_helpers import build_production_stack, init_command
from studio.application.routing_worker import RoutingCommandWorker
from studio.domain import ids as domain_ids
from studio.domain.artifacts import ArtifactRef, ImagePayload
from studio.domain.enums import (
    AcceptanceMode,
    CurrencyStatus,
    DependencyStatus,
    PropagationMode,
)
from studio.infrastructure.memory._state import MemoryDatabase
from studio.infrastructure.memory.unit_of_work import (
    MemoryCommandBus,
    MemoryUnitOfWorkFactory,
)
from studio.kernel.decisions import Accepted
from studio.kernel.envelopes import CommandEnvelope, EventEnvelope
from studio.kernel.errors import ContractViolation, IdempotencyConflict
from studio.production import identity
from studio.production.attempt import TaskAttemptDecider
from studio.production.dispatch import canonical_target
from studio.production.payloads import (
    ArtifactMarkedStaleEvt,
    ArtifactVersionAcceptedEvt,
    CreateTaskAttemptCmd,
    ExpandStageCmd,
    InitializePipelineCmd,
    MarkArtifactStaleCmd,
    ProductionEvent,
    ProposeArtifactVersionCmd,
    TaskAttemptCreatedEvt,
    TaskInputsBoundEvt,
)
from studio.production.process_managers import (
    LineageProcessManager,
    RecomputeProcessManager,
)
from studio.production.project import ProjectDecider
from studio.production.projections import ArtifactLifecycleView
from studio.production.series import ArtifactSeriesDecider
from studio.production.values import BindingItem

_TS = datetime(2026, 1, 1, tzinfo=UTC)
_FRESH_CURRENT = (CurrencyStatus.CURRENT, DependencyStatus.FRESH)


def _ref(series_id: str, revision: int, digest_hex: str = "a" * 64) -> ArtifactRef:
    return ArtifactRef(
        artifact_id=domain_ids.artifact_id(series_id, revision),
        series_id=series_id,
        revision=revision,
        digest=digest_hex,
    )


def _binding(ref: ArtifactRef, key: str) -> BindingItem:
    return BindingItem.from_ref(
        requirement_key=key,
        logical_slot=key,
        partition_key=None,
        ref=ref,
        propagation_mode=PropagationMode.AGGREGATE,
    )


def _envelope(pos: int, payload: ProductionEvent) -> EventEnvelope[ProductionEvent]:
    return EventEnvelope(
        event_id=f"evt-{pos}",
        schema_version=1,
        stream_id="s",
        sequence=pos,
        global_position=pos,
        correlation_id="c",
        causation_id="x",
        recorded_at=_TS,
        payload=payload,
    )


def _run_pm(pm: Any, payloads: list[ProductionEvent]) -> list[Any]:
    state = pm.initial_state()
    commands: list[Any] = []
    for pos, payload in enumerate(payloads):
        reaction = pm.react(state, _envelope(pos, payload))
        state = reaction.state
        commands.extend(reaction.commands)
    return commands


# --- Blocker 1:ExpansionPM 不串项目 --- #


def test_expansion_does_not_leak_across_projects() -> None:
    s = build_production_stack()
    s.bus.publish(init_command("proj-A", command_id="init-A"))
    s.bus.publish(init_command("proj-B", command_id="init-B"))
    s.driver.run_until_quiescent()

    view = ArtifactLifecycleView.build(s.db.state.events)
    for project in ("proj-A", "proj-B"):
        for shot in ("shot_01", "shot_02"):
            series = domain_ids.series_id(project, "image", shot)
            cur = view.current_ref(series)
            assert cur is not None, f"{project}/{shot} 缺失(发生串项目)"
            assert (view.currency(cur.artifact_id), view.dependency(cur.artifact_id)) == (
                _FRESH_CURRENT
            )


# --- Blocker 2:身份不变式 --- #


def test_router_rejects_target_payload_mismatch() -> None:
    worker = RoutingCommandWorker(
        deciders={},
        resolve_kind=identity.stream_kind,
        canonical_target=canonical_target,
        bus=MemoryCommandBus(),
        uow_factory=MemoryUnitOfWorkFactory(MemoryDatabase()),
        clock=FakeClock(),
    )
    forged: CommandEnvelope[Any] = CommandEnvelope(
        command_id="c1",
        schema_version=1,
        target=identity.project_stream("proj-A"),
        command_key="init",
        correlation_id="x",
        causation_id=None,
        issued_at=_TS,
        payload=InitializePipelineCmd(project_id="proj-B", pipeline_spec_id="spec"),
    )
    worker._bus.publish(forged)  # type: ignore[attr-defined]
    with pytest.raises(ContractViolation):
        worker.tick()


def test_attempt_decider_rejects_forged_identity() -> None:
    decider = TaskAttemptDecider(
        {"image": lambda stage, refs, part: ImagePayload(
            shot_id=part or "", prompt="p", blob_ref="b"
        )}
    )
    project, stage, partition = "p", "image", "shot_01"
    series = domain_ids.series_id(project, "image", partition)
    tk = identity.task_key(project, stage, partition)
    aid = identity.attempt_id(tk, identity.input_binding_digest(()), 0)
    valid = CreateTaskAttemptCmd(
        attempt_id=aid,
        project_id=project,
        stage_id=stage,
        partition_key=partition,
        output_key="image",
        series_id=series,
        exact_refs=(),
    )
    assert isinstance(decider.decide(decider.initial_state(), valid), Accepted)

    forged_attempt = valid.model_copy(update={"attempt_id": "forged"})
    assert decider.decide(decider.initial_state(), forged_attempt).code == "forged_attempt"  # type: ignore[union-attr]
    forged_series = valid.model_copy(update={"series_id": "wrong"})
    assert decider.decide(decider.initial_state(), forged_series).code == "forged_series"  # type: ignore[union-attr]


def test_series_decider_digest_and_candidate_conflict() -> None:
    decider = ArtifactSeriesDecider()
    series = domain_ids.series_id("p", "image", "shot_01")
    payload1 = ImagePayload(shot_id="shot_01", prompt="a", blob_ref="b")
    from studio.serialization import digest

    good = ProposeArtifactVersionCmd(
        project_id="p",
        series_id=series,
        candidate_id="cand-1",
        output_key="image",
        partition_key="shot_01",
        digest=digest(payload1),
        payload=payload1,
        acceptance_mode=AcceptanceMode.AUTO,
        produced_by_attempt="att-1",
    )
    # digest 不一致 -> Rejected
    bad_digest = good.model_copy(update={"digest": "b" * 64})
    assert decider.decide(decider.initial_state(), bad_digest).code == "digest_mismatch"  # type: ignore[union-attr]

    # 折叠首次 propose 的事件
    state = decider.initial_state()
    decision = decider.decide(state, good)
    assert isinstance(decision, Accepted)
    for pe in decision.events:
        state = decider.evolve(state, pe.payload)

    # 同 candidate_id 异内容 -> IdempotencyConflict
    payload2 = ImagePayload(shot_id="shot_01", prompt="CHANGED", blob_ref="b")
    conflict = good.model_copy(
        update={"payload": payload2, "digest": digest(payload2)}
    )
    with pytest.raises(IdempotencyConflict):
        decider.decide(state, conflict)


# --- Blocker 3:同一 Stage 可用新 driver 再次展开 --- #


def test_stage_can_reexpand_with_new_driver() -> None:
    decider = ProjectDecider()
    state = decider.initial_state()
    driver_v1 = _ref(domain_ids.series_id("p", "storyboard", None), 1)
    driver_v2 = _ref(domain_ids.series_id("p", "storyboard", None), 2, "b" * 64)

    def expand(driver: ArtifactRef) -> ExpandStageCmd:
        return ExpandStageCmd(
            project_id="p",
            stage_id="plan",
            driver_ref=driver,
            partitions=("shot_01",),
            task_keys=("tk1",),
        )

    d1 = decider.decide(state, expand(driver_v1))
    assert isinstance(d1, Accepted)
    for pe in d1.events:
        state = decider.evolve(state, pe.payload)

    # 同 driver 再展开 -> 拒绝
    assert decider.decide(state, expand(driver_v1)).code == "already_expanded"  # type: ignore[union-attr]
    # 新 driver -> 允许
    assert isinstance(decider.decide(state, expand(driver_v2)), Accepted)


# --- Blocker 4:Lineage 处理"旧输入执行中、上游先更新" --- #


def test_lineage_marks_late_downstream_stale() -> None:
    a_v1 = _ref("series-A", 1)
    a_v2 = _ref("series-A", 2, "b" * 64)
    t_v1 = _ref("series-T", 1, "c" * 64)
    binding_a1 = _binding(a_v1, "plan")

    events: list[ProductionEvent] = [
        TaskAttemptCreatedEvt(
            attempt_id="att-consumer",
            project_id="p",
            stage_id="image",
            partition_key="shot_01",
            output_key="image",
            series_id="series-T",
        ),
        TaskInputsBoundEvt(attempt_id="att-consumer", exact_refs=(binding_a1,)),
        ArtifactVersionAcceptedEvt(
            project_id="p", series_id="series-A", revision=1, artifact_ref=a_v1,
            previous_current_ref=None, candidate_id="ca1", produced_by_attempt="att-A1",
            output_key="plan", partition_key="shot_01",
        ),
        ArtifactVersionAcceptedEvt(
            project_id="p", series_id="series-A", revision=2, artifact_ref=a_v2,
            previous_current_ref=a_v1, candidate_id="ca2", produced_by_attempt="att-A2",
            output_key="plan", partition_key="shot_01",
        ),
        ArtifactVersionAcceptedEvt(
            project_id="p", series_id="series-T", revision=1, artifact_ref=t_v1,
            previous_current_ref=None, candidate_id="ct1",
            produced_by_attempt="att-consumer", output_key="image", partition_key="shot_01",
        ),
    ]
    commands = _run_pm(LineageProcessManager(), events)
    stale = [c.payload for c in commands if isinstance(c.payload, MarkArtifactStaleCmd)]
    assert len(stale) == 1
    assert stale[0].target_ref.artifact_id == t_v1.artifact_id
    assert stale[0].invalidated_input_ref.artifact_id == a_v1.artifact_id
    assert stale[0].replacement_ref.artifact_id == a_v2.artifact_id


# --- Blocker 5:多输入失效累计 --- #


def test_recompute_accumulates_multi_input_replacements() -> None:
    a_v1 = _ref("series-A", 1)
    a_v2 = _ref("series-A", 2, "b" * 64)
    b_v1 = _ref("series-B", 1, "c" * 64)
    b_v2 = _ref("series-B", 2, "d" * 64)
    t_v1 = _ref("series-T", 1, "e" * 64)

    events: list[ProductionEvent] = [
        TaskAttemptCreatedEvt(
            attempt_id="att-T", project_id="p", stage_id="stitch", partition_key=None,
            output_key="stitch", series_id="series-T",
        ),
        TaskInputsBoundEvt(
            attempt_id="att-T",
            exact_refs=(_binding(a_v1, "a"), _binding(b_v1, "b")),
        ),
        ArtifactVersionAcceptedEvt(
            project_id="p", series_id="series-T", revision=1, artifact_ref=t_v1,
            previous_current_ref=None, candidate_id="ct1", produced_by_attempt="att-T",
            output_key="stitch", partition_key=None,
        ),
        ArtifactMarkedStaleEvt(
            target_ref=t_v1, invalidated_input_ref=a_v1, replacement_ref=a_v2,
            root_cause_event_id="e1", scope=PropagationMode.AGGREGATE,
            task_key="tk", partition_key=None,
        ),
        ArtifactMarkedStaleEvt(
            target_ref=t_v1, invalidated_input_ref=b_v1, replacement_ref=b_v2,
            root_cause_event_id="e2", scope=PropagationMode.AGGREGATE,
            task_key="tk", partition_key=None,
        ),
    ]
    commands = _run_pm(RecomputeProcessManager(), events)
    creates = [c.payload for c in commands if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 2
    last_ids = {b.artifact_id for b in creates[-1].exact_refs}
    assert last_ids == {a_v2.artifact_id, b_v2.artifact_id}  # 两个替换累计


# --- 投影次要问题:未知 artifact --- #


def test_projection_unknown_artifact_raises() -> None:
    view = ArtifactLifecycleView.build([])
    with pytest.raises(LookupError):
        view.currency("nope")
