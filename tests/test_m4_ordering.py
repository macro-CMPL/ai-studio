"""M4:异步乱序保护 — status_revision(causal event global_position)。

证明:
1. status_revision < last_status_revision → Accepted(()) (幂等忽略过时命令)
2. status_revision > last_status_revision → 允许跳过中间状态(新命令先到)
3. 无 status_revision(None) → 向后兼容
4. 3→2→1 顺序最终收敛到事实序列 3 对应状态
5. UNKNOWN 先于 INITIATED 到达,不被永久拒绝
6. Result 先于 WaitingProvider 到达,最终保持 SUCCEEDED
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from studio.domain import ids as domain_ids
from studio.domain.artifacts import (
    ArtifactRef,
    ImagePayload,
    ImagePlanPayload,
    OperationParam,
    PlannedOperation,
)
from studio.domain.enums import PropagationMode, TaskAttemptStatus
from studio.kernel.decisions import Accepted
from studio.production import identity
from studio.production.attempt import TaskAttemptDecider
from studio.production.attempt_payloads import (
    MarkFailedCmd,
    MarkWaitingProviderCmd,
    MarkWaitingReconciliationCmd,
    RecordExecutionSpecCmd,
    RecordProviderResultCmd,
)
from studio.production.execution_spec import ProviderExecutionSpec
from studio.production.payloads import CreateTaskAttemptCmd
from studio.production.pipeline import golden_compiled
from studio.production.values import BindingItem
from studio.serialization import digest

_P = "p"
_PART = "shot_01"
_S = TaskAttemptStatus


def _apply(decider: Any, state: Any, cmd: Any) -> tuple[Any, Any]:
    dec = decider.decide(state, cmd)
    if isinstance(dec, Accepted):
        for pe in dec.events:
            state = decider.evolve(state, pe.payload)
    return state, dec


def _plan_payload() -> ImagePlanPayload:
    return ImagePlanPayload(
        operations=(
            PlannedOperation(
                logical_operation_key="shot_01:image:v0", op_type="gen",
                params=(OperationParam(key="shot", value=_PART),),
            ),
        )
    )


def _plan_binding() -> BindingItem:
    series = domain_ids.series_id(_P, "plan", _PART)
    ref = ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, 1), series_id=series,
        revision=1, digest=digest(_plan_payload()),
    )
    return BindingItem.from_ref(
        requirement_key="image_plan:plan", logical_slot="plan", partition_key=_PART,
        ref=ref, propagation_mode=PropagationMode.PARTITION_PRESERVING,
    )


def _image_attempt_id(binding: BindingItem) -> str:
    tk = identity.task_key(_P, "image", _PART)
    return identity.attempt_id(tk, identity.input_binding_digest((binding,)), 0)


def _spec(aid: str) -> ProviderExecutionSpec:
    return ProviderExecutionSpec.from_plan(
        attempt_id=aid, plan_ref=_plan_binding().to_ref(),
        plan_payload=_plan_payload(), provider_id="fake", provider_version="1",
        estimated_cost=Decimal(10), currency="CNY", pricing_version="1", request_ref="req",
    )


def _at_waiting_budget() -> tuple[TaskAttemptDecider, Any, str]:
    d = TaskAttemptDecider(golden_compiled(), {})
    binding = _plan_binding()
    aid = _image_attempt_id(binding)
    cmd = CreateTaskAttemptCmd(
        attempt_id=aid, project_id=_P, stage_id="image", partition_key=_PART,
        output_key="image", series_id=domain_ids.series_id(_P, "image", _PART),
        exact_refs=(binding,),
    )
    s, _ = _apply(d, d.initial_state(), cmd)
    s, _ = _apply(d, s, RecordExecutionSpecCmd(attempt_id=aid, spec=_spec(aid)))
    # Now at WAITING_BUDGET, last_status_revision set from spec event
    return d, s, aid


# --------------------------------------------------------------------------- #


def test_stale_command_idempotent_ignored() -> None:
    """revision=5 已设置;收到 revision=3 -> 幂等忽略。"""
    d, s, aid = _at_waiting_budget()
    # First move to WAITING_PROVIDER at revision=10
    s, _ = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=10))
    assert s.status is _S.WAITING_PROVIDER and s.last_status_revision == 10
    # Stale command: revision=5 < 10 -> ignored
    s2, dec = _apply(d, s, MarkWaitingReconciliationCmd(attempt_id=aid, status_revision=5))
    assert isinstance(dec, Accepted) and dec.events == ()
    assert s2.status is _S.WAITING_PROVIDER  # 未回退


def test_newer_command_skips_intermediate_states() -> None:
    """revision=20 到达时当前 revision=1;允许跳过中间状态直接迁移。"""
    d, s, aid = _at_waiting_budget()
    # revision=1(from spec evolve); 收到 revision=20 的 MarkFailed -> 直接迁移
    s2, dec = _apply(d, s, MarkFailedCmd(attempt_id=aid, reason="abort", status_revision=20))
    assert isinstance(dec, Accepted) and len(dec.events) == 1
    assert s2.status is _S.FAILED and s2.last_status_revision == 20


def test_no_revision_backward_compatible() -> None:
    """status_revision=None -> 不做乱序校验。"""
    d, s, aid = _at_waiting_budget()
    s2, dec = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=None))
    assert isinstance(dec, Accepted) and len(dec.events) == 1
    assert s2.status is _S.WAITING_PROVIDER


def test_order_3_2_1_converges_to_state_3() -> None:
    """事实顺序 INITIATED(1)→UNKNOWN(2)→SUBMITTED(3),命令 3→2→1 到达。

    最终状态应该是 SUBMITTED 对应的 WAITING_PROVIDER(revision=3)。
    """
    d, s, aid = _at_waiting_budget()
    # SUBMITTED arrives first (revision=30)
    s, dec = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=30))
    assert s.status is _S.WAITING_PROVIDER and s.last_status_revision == 30
    # UNKNOWN arrives second (revision=20) — stale, ignored
    s, dec = _apply(d, s, MarkWaitingReconciliationCmd(attempt_id=aid, status_revision=20))
    assert isinstance(dec, Accepted) and dec.events == ()
    assert s.status is _S.WAITING_PROVIDER  # 未回退
    # INITIATED arrives third (revision=10) — stale, ignored
    s, dec = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=10))
    assert isinstance(dec, Accepted) and dec.events == ()
    assert s.status is _S.WAITING_PROVIDER


def test_unknown_before_initiated_not_rejected() -> None:
    """UNKNOWN(revision=20) 先于 INITIATED(revision=10) 到达。

    UNKNOWN 对应 WAITING_RECONCILIATION 应直接被接受(跳过 WAITING_PROVIDER)。
    INITIATED 后到应被忽略。
    """
    d, s, aid = _at_waiting_budget()
    # UNKNOWN arrives first: WAITING_BUDGET -> WAITING_RECONCILIATION
    s, dec = _apply(d, s, MarkWaitingReconciliationCmd(attempt_id=aid, status_revision=20))
    assert isinstance(dec, Accepted) and len(dec.events) == 1
    assert s.status is _S.WAITING_RECONCILIATION
    # INITIATED arrives later (revision=10) -> stale, ignored
    s, dec = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=10))
    assert isinstance(dec, Accepted) and dec.events == ()
    assert s.status is _S.WAITING_RECONCILIATION


def test_result_before_waiting_provider_succeeds() -> None:
    """RecordProviderResult(revision=30) 先于 WaitingProvider(revision=10) 到达。

    Result 从 WAITING_BUDGET 被接受(status_revision 允许跳过中间状态);
    后到的 WaitingProvider 被忽略,最终保持 SUCCEEDED。
    last_status_revision 必须为 30(不是 1)。
    """
    d, s, aid = _at_waiting_budget()
    spec = _spec(aid)
    payload = ImagePayload(shot_id=_PART, prompt="p", blob_ref="blob://final")
    # Result arrives first (revision=30)
    s, dec = _apply(d, s, RecordProviderResultCmd(
        attempt_id=aid, operation_id=spec.operation_id, blob_ref="blob://final",
        payload=payload, status_revision=30,
    ))
    assert isinstance(dec, Accepted) and len(dec.events) == 1
    assert s.status is _S.SUCCEEDED
    assert s.last_status_revision == 30  # revision 持久化
    # WaitingProvider arrives later (revision=10) -> stale -> ignored
    s2, dec2 = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=10))
    assert isinstance(dec2, Accepted) and dec2.events == ()
    assert s2.status is _S.SUCCEEDED


def test_result_without_revision_from_waiting_budget_accepted() -> None:
    """RecordProviderResult(None revision) 从 WAITING_BUDGET 也被接受(跳过中间)。"""
    d, s, aid = _at_waiting_budget()
    spec = _spec(aid)
    payload = ImagePayload(shot_id=_PART, prompt="p", blob_ref="blob://final")
    s, dec = _apply(d, s, RecordProviderResultCmd(
        attempt_id=aid, operation_id=spec.operation_id, blob_ref="blob://final",
        payload=payload, status_revision=None,
    ))
    assert isinstance(dec, Accepted) and len(dec.events) == 1
    assert s.status is _S.SUCCEEDED


def test_same_revision_same_target_idempotent() -> None:
    """同 revision + 同 fingerprint → 幂等。"""
    d, s, aid = _at_waiting_budget()
    s, _ = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=10))
    assert s.last_status_revision == 10
    # 同 revision + 同 target -> 幂等
    s2, dec = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=10))
    assert isinstance(dec, Accepted) and dec.events == ()
    assert s2.status is _S.WAITING_PROVIDER


def test_same_revision_different_target_conflicts() -> None:
    """同 revision + 不同 target → IdempotencyConflict。"""
    from studio.kernel.errors import IdempotencyConflict as IC

    d, s, aid = _at_waiting_budget()
    s, _ = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=10))
    assert s.last_status_revision == 10
    # 同 revision=10 + 不同 target(RECONCILIATION) -> 冲突
    import pytest

    with pytest.raises(IC):
        d.decide(s, MarkWaitingReconciliationCmd(attempt_id=aid, status_revision=10))


def test_result_pm_emits_status_revision() -> None:
    """真实 ResultPM 发出 RecordProviderResultCmd 和 MarkFailedCmd 携带 status_revision。"""
    from datetime import UTC, datetime

    from studio.kernel.envelopes import EventEnvelope, MessagePayload
    from studio.production.attempt_payloads import ProviderExecutionSpecRecordedEvt
    from studio.production.budget import BudgetSettlementCompletedEvt
    from studio.production.payloads import TaskAttemptCreatedEvt
    from studio.production.pipeline import golden_compiled
    from studio.production.provider_op import ProviderOperationSucceededEvt, ProviderResultRef
    from studio.production.result_mapper import default_result_mappers
    from studio.production.result_pm import ProviderResultProcessManager

    _TS = datetime(2026, 1, 1, tzinfo=UTC)
    pm = ProviderResultProcessManager(golden_compiled(), default_result_mappers())
    spec = _spec(_image_attempt_id(_plan_binding()))
    op = spec.operation_id

    events: list[MessagePayload] = [
        TaskAttemptCreatedEvt(
            attempt_id=spec.attempt_id, project_id=_P, stage_id="image",
            partition_key=_PART, output_key="image",
            series_id=domain_ids.series_id(_P, "image", _PART),
        ),
        ProviderExecutionSpecRecordedEvt(attempt_id=spec.attempt_id, spec=spec),
        ProviderOperationSucceededEvt(
            operation_id=op,
            result_ref=ProviderResultRef(blob_ref="blob://x", digest="a" * 64),
            cost_actual=Decimal(10), cost_currency="CNY", provider_event_id="pe",
        ),
        BudgetSettlementCompletedEvt(
            operation_id=op, outcome="captured", captured_amount=Decimal(10),
            currency="CNY", quote_digest=spec.quote_digest(),
        ),
    ]
    state = pm.initial_state()
    result_cmds = []
    for pos, payload in enumerate(events):
        env: EventEnvelope[MessagePayload] = EventEnvelope(
            event_id=f"evt-{pos}", schema_version=1,
            stream_id=f"budget:{_P}" if pos == 3 else "s",
            sequence=pos, global_position=pos * 10,
            correlation_id="c", causation_id="x", recorded_at=_TS, payload=payload,
        )
        reaction = pm.react(state, env)
        state = reaction.state
        result_cmds.extend(reaction.commands)

    record = [c for c in result_cmds if isinstance(c.payload, RecordProviderResultCmd)]
    assert len(record) == 1
    # global_position of settlement event = 30
    assert record[0].payload.status_revision == 30


def test_result_first_persists_revision_on_replay() -> None:
    """Result(revision=30) → SUCCEEDED 后,last_status_revision==30 且重放不丢。"""
    d, s, aid = _at_waiting_budget()
    spec = _spec(aid)
    payload = ImagePayload(shot_id=_PART, prompt="p", blob_ref="blob://final")
    s, _ = _apply(d, s, RecordProviderResultCmd(
        attempt_id=aid, operation_id=spec.operation_id, blob_ref="blob://final",
        payload=payload, status_revision=30,
    ))
    assert s.status is _S.SUCCEEDED and s.last_status_revision == 30
    # 模拟重放:重新 evolve 该事件,revision 仍为 30
    from studio.production.payloads import ArtifactCandidateProducedEvt

    evt = ArtifactCandidateProducedEvt(
        candidate_id="c", attempt_id=aid, project_id=_P,
        series_id=domain_ids.series_id(_P, "image", _PART),
        output_key="image", partition_key=_PART, digest=digest(payload),
        payload=payload, status_revision=30,
    )
    fresh = d.initial_state()
    # 构造到 WAITING_BUDGET 再 evolve candidate
    cmd = CreateTaskAttemptCmd(
        attempt_id=aid, project_id=_P, stage_id="image", partition_key=_PART,
        output_key="image", series_id=domain_ids.series_id(_P, "image", _PART),
        exact_refs=(_plan_binding(),),
    )
    fresh, _ = _apply(d, fresh, cmd)
    fresh, _ = _apply(d, fresh, RecordExecutionSpecCmd(attempt_id=aid, spec=spec))
    replayed = d.evolve(fresh, evt)
    assert replayed.status is _S.SUCCEEDED
    assert replayed.last_status_revision == 30


def test_waiting_provider_then_result_same_revision_conflicts() -> None:
    """WaitingProvider(30) 后 Result(30) → IdempotencyConflict。

    同 global_position 只能对应一个权威事实。
    """
    import pytest

    from studio.kernel.errors import IdempotencyConflict as IC

    d, s, aid = _at_waiting_budget()
    spec = _spec(aid)
    # WaitingProvider at revision=30
    s, _ = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=30))
    assert s.status is _S.WAITING_PROVIDER and s.last_status_revision == 30
    # Result at same revision=30 -> conflict (different fingerprint)
    payload = ImagePayload(shot_id=_PART, prompt="p", blob_ref="blob://final")
    with pytest.raises(IC):
        d.decide(s, RecordProviderResultCmd(
            attempt_id=aid, operation_id=spec.operation_id, blob_ref="blob://final",
            payload=payload, status_revision=30,
        ))
