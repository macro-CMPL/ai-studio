"""M4 步骤3:ReconciliationClockDecider + OrphanReconciler(纯)。"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from studio.domain import ids as domain_ids
from studio.domain.artifacts import (
    ArtifactRef,
    ImagePlanPayload,
    OperationParam,
    PlannedOperation,
)
from studio.domain.enums import PropagationMode
from studio.kernel.decisions import Accepted, Rejected
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.kernel.errors import ContractViolation
from studio.production import identity
from studio.production.attempt_payloads import ProviderExecutionSpecRecordedEvt
from studio.production.budget import BudgetReservedEvt, BudgetSettlementCompletedEvt
from studio.production.execution_spec import ProviderExecutionSpec
from studio.production.payloads import TaskAttemptCreatedEvt
from studio.production.provider_op import (
    AbortBeforeSubmissionCmd,
    InitiateProviderOpCmd,
    ProviderOperationAbortedEvt,
    ProviderOperationInitiatedEvt,
    SubmissionAttemptClaimedEvt,
)
from studio.production.reconcile import (
    EmitReconciliationTickCmd,
    OrphanReconciliationProcessManager,
    ReconcilePolicy,
    ReconciliationClockDecider,
    ReconciliationTickEvt,
)
from studio.production.values import BindingItem
from studio.serialization import digest

_P = "p"
_PART = "shot_01"
_T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
_TS = datetime(2026, 1, 1, tzinfo=UTC)


def _plan_payload() -> ImagePlanPayload:
    return ImagePlanPayload(
        operations=(
            PlannedOperation(
                logical_operation_key="shot_01:image:v0", op_type="gen",
                params=(OperationParam(key="shot", value=_PART),),
            ),
        )
    )


def _plan_ref() -> ArtifactRef:
    series = domain_ids.series_id(_P, "plan", _PART)
    return ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, 1), series_id=series,
        revision=1, digest=digest(_plan_payload()),
    )


def _plan_binding() -> BindingItem:
    return BindingItem.from_ref(
        requirement_key="image_plan:plan", logical_slot="plan", partition_key=_PART,
        ref=_plan_ref(), propagation_mode=PropagationMode.PARTITION_PRESERVING,
    )


def _image_attempt_id() -> str:
    tk = identity.task_key(_P, "image", _PART)
    return identity.attempt_id(tk, identity.input_binding_digest((_plan_binding(),)), 0)


def _spec(aid: str) -> ProviderExecutionSpec:
    return ProviderExecutionSpec.from_plan(
        attempt_id=aid, plan_ref=_plan_ref(), plan_payload=_plan_payload(),
        provider_id="fake", provider_version="1", estimated_cost=Decimal("12.5"),
        currency="CNY", pricing_version="1", request_ref="req",
    )


def _created() -> TaskAttemptCreatedEvt:
    return TaskAttemptCreatedEvt(
        attempt_id=_image_attempt_id(), project_id=_P, stage_id="image",
        partition_key=_PART, output_key="image",
        series_id=domain_ids.series_id(_P, "image", _PART),
    )


def _reserved(spec: ProviderExecutionSpec) -> BudgetReservedEvt:
    return BudgetReservedEvt(
        operation_id=spec.operation_id, amount=spec.estimated_cost, currency="CNY",
        quote_digest=spec.quote_digest(),
    )


def _tick(as_of: datetime, seq: int) -> ReconciliationTickEvt:
    return ReconciliationTickEvt(
        scope="global", as_of=as_of, policy_version="1", sequence=seq
    )


_BUDGET_EVENTS = (BudgetReservedEvt, BudgetSettlementCompletedEvt)


def _origin_stream(payload: MessagePayload) -> str:
    """预算事件默认来自本项目 budget 流;其余事件流 ID 与 owner 校验无关。"""
    if isinstance(payload, _BUDGET_EVENTS):
        return identity.budget_stream(_P)
    return "s"


def _run(
    pm: Any, payloads: Sequence[MessagePayload | tuple[MessagePayload, str]]
) -> list[Any]:
    """喂事件流。元素可为 payload(自动推导来源流)或 (payload, stream_id) 元组。"""
    state = pm.initial_state()
    commands: list[Any] = []
    for pos, item in enumerate(payloads):
        if isinstance(item, tuple):
            payload, stream_id = item
        else:
            payload, stream_id = item, _origin_stream(item)
        env: EventEnvelope[MessagePayload] = EventEnvelope(
            event_id=f"evt-{pos}", schema_version=1, stream_id=stream_id, sequence=pos,
            global_position=pos, correlation_id="c", causation_id="x",
            recorded_at=_TS, payload=payload,
        )
        reaction = pm.react(state, env)
        state = reaction.state
        commands.extend(reaction.commands)
    return commands


def _of(cmds: list[Any], cls: type) -> list[Any]:
    return [c for c in cmds if isinstance(c.payload, cls)]


def _pm() -> OrphanReconciliationProcessManager:
    return OrphanReconciliationProcessManager(
        ReconcilePolicy(version="1", recycle_after=timedelta(minutes=10))
    )


# --- 时钟 --- #


def test_clock_emits_monotonic_ticks() -> None:
    d = ReconciliationClockDecider()
    s = d.initial_state()
    dec = d.decide(s, EmitReconciliationTickCmd(scope="g", as_of=_T0, policy_version="1"))
    assert isinstance(dec, Accepted)
    evt = dec.events[0].payload
    assert isinstance(evt, ReconciliationTickEvt) and evt.sequence == 1
    s = d.evolve(s, evt)
    stale = d.decide(s, EmitReconciliationTickCmd(scope="g", as_of=_T0, policy_version="1"))
    assert isinstance(stale, Rejected) and stale.code == "stale_tick"
    later = d.decide(
        s,
        EmitReconciliationTickCmd(
            scope="g", as_of=_T0 + timedelta(minutes=1), policy_version="1"
        ),
    )
    assert isinstance(later, Accepted)
    assert later.events[0].payload.sequence == 2  # type: ignore[union-attr]


# --- Reconciler --- #


def test_reconciler_reinitiates_lost_initiate() -> None:
    spec = _spec(_image_attempt_id())
    cmds = _run(_pm(), [
        _created(),
        ProviderExecutionSpecRecordedEvt(attempt_id=spec.attempt_id, spec=spec),
        _reserved(spec),
        _tick(_T0, 1),
    ])
    reinit = _of(cmds, InitiateProviderOpCmd)
    assert len(reinit) == 1
    assert reinit[0].target == f"provider-op:{spec.operation_id}"
    assert not _of(cmds, AbortBeforeSubmissionCmd)


def test_reconciler_no_auto_abort_when_claimed() -> None:
    spec = _spec(_image_attempt_id())
    op = spec.operation_id
    cmds = _run(_pm(), [
        _created(),
        ProviderExecutionSpecRecordedEvt(attempt_id=spec.attempt_id, spec=spec),
        _reserved(spec),
        ProviderOperationInitiatedEvt(operation_id=op, spec=spec),
        SubmissionAttemptClaimedEvt(operation_id=op),
        _tick(_T0 + timedelta(hours=1), 1),
    ])
    assert not _of(cmds, AbortBeforeSubmissionCmd)  # CLAIMED 后不自动 abort
    assert not _of(cmds, InitiateProviderOpCmd)


def test_reconciler_recycles_none_and_releases_two_rounds() -> None:
    spec = _spec(_image_attempt_id())
    op = spec.operation_id
    base: list[MessagePayload] = [
        _created(),
        ProviderExecutionSpecRecordedEvt(attempt_id=spec.attempt_id, spec=spec),
        _reserved(spec),
        _tick(_T0, 1),  # elapsed 0 -> reinitiate,first_seen=_T0
        _tick(_T0 + timedelta(minutes=15), 2),  # elapsed 15 >= 10 -> abort
    ]
    from studio.production.budget import ReleaseBudgetCmd

    cmds = _run(_pm(), base)
    assert len(_of(cmds, InitiateProviderOpCmd)) == 1
    assert len(_of(cmds, AbortBeforeSubmissionCmd)) == 1
    assert not _of(cmds, ReleaseBudgetCmd)  # 尚未观察到 Aborted,不释放

    # 第二轮:观察到 ProviderOperationAborted 后才释放
    with_abort = base + [
        ProviderOperationAbortedEvt(operation_id=op, reason="reconcile_recycle"),
        _tick(_T0 + timedelta(minutes=20), 3),
    ]
    cmds2 = _run(_pm(), with_abort)
    assert len(_of(cmds2, ReleaseBudgetCmd)) == 1
    assert len(_of(cmds2, AbortBeforeSubmissionCmd)) == 1  # 不重复 abort(abort_requested)


def test_reconciler_recycles_stuck_initiated() -> None:
    spec = _spec(_image_attempt_id())
    op = spec.operation_id
    cmds = _run(_pm(), [
        _created(),
        ProviderExecutionSpecRecordedEvt(attempt_id=spec.attempt_id, spec=spec),
        _reserved(spec),
        ProviderOperationInitiatedEvt(operation_id=op, spec=spec),
        _tick(_T0, 1),  # INITIATED,elapsed 0 -> 等待(无命令)
        _tick(_T0 + timedelta(minutes=15), 2),  # elapsed 15 -> abort
    ])
    assert not _of(cmds, InitiateProviderOpCmd)  # 已 INITIATED,不重发
    assert len(_of(cmds, AbortBeforeSubmissionCmd)) == 1


def test_reconciler_ignores_foreign_scope() -> None:
    spec = _spec(_image_attempt_id())
    cmds = _run(_pm(), [  # _pm() scope 默认 "global"
        _created(),
        ProviderExecutionSpecRecordedEvt(attempt_id=spec.attempt_id, spec=spec),
        _reserved(spec),
        ReconciliationTickEvt(  # 外部 scope,即便早已超阈值也不评估
            scope="other", as_of=_T0 + timedelta(hours=5), policy_version="1", sequence=1
        ),
    ])
    assert not _of(cmds, InitiateProviderOpCmd)
    assert not _of(cmds, AbortBeforeSubmissionCmd)


def test_reconciler_no_release_for_unreserved_or_untracked_abort() -> None:
    from studio.production.budget import ReleaseBudgetCmd

    spec = _spec(_image_attempt_id())
    op = spec.operation_id
    # 已跟踪但未 reserved -> 外部墓碑不触发对不存在预留的 Release
    unreserved = _run(_pm(), [
        _created(),
        ProviderExecutionSpecRecordedEvt(attempt_id=spec.attempt_id, spec=spec),
        ProviderOperationAbortedEvt(operation_id=op, reason="external"),
    ])
    assert not _of(unreserved, ReleaseBudgetCmd)
    # 完全未跟踪的 op 墓碑同样不释放
    untracked = _run(_pm(), [ProviderOperationAbortedEvt(operation_id="ghost", reason="x")])
    assert not _of(untracked, ReleaseBudgetCmd)


def test_reconciler_ignores_released_and_wrong_policy() -> None:
    spec = _spec(_image_attempt_id())
    op = spec.operation_id
    cmds = _run(_pm(), [
        _created(),
        ProviderExecutionSpecRecordedEvt(attempt_id=spec.attempt_id, spec=spec),
        _reserved(spec),
        ProviderOperationAbortedEvt(operation_id=op, reason="recycle"),
        BudgetSettlementCompletedEvt(
            operation_id=op, outcome="released", captured_amount=Decimal(0),
            currency="CNY", quote_digest=spec.quote_digest(),
        ),
        _tick(_T0 + timedelta(hours=2), 1),  # 已释放 -> 不再动作
        ReconciliationTickEvt(  # 正确 scope、错误 policy_version -> 被忽略
            scope="global", as_of=_T0 + timedelta(hours=3), policy_version="9", sequence=2
        ),
    ])
    from studio.production.budget import ReleaseBudgetCmd

    assert len(_of(cmds, ReleaseBudgetCmd)) == 1  # 仅 Aborted 触发的一次
    assert not _of(cmds, AbortBeforeSubmissionCmd)


# --- 对抗:跨项目预算 owner --- #


def test_reconciler_foreign_reserve_rejected() -> None:
    """内容一致但来自 budget:{other} 的预留 -> ContractViolation,绝不确认(不释放)。"""
    spec = _spec(_image_attempt_id())
    with pytest.raises(ContractViolation):
        _run(_pm(), [
            _created(),
            ProviderExecutionSpecRecordedEvt(attempt_id=spec.attempt_id, spec=spec),
            (_reserved(spec), identity.budget_stream("other")),  # 跨项目 budget 流
        ])


def test_reconciler_foreign_release_rejected() -> None:
    """释放墓碑来自 budget:{other} -> ContractViolation,绝不据此抑制正确释放。"""
    spec = _spec(_image_attempt_id())
    op = spec.operation_id
    with pytest.raises(ContractViolation):
        _run(_pm(), [
            _created(),
            ProviderExecutionSpecRecordedEvt(attempt_id=spec.attempt_id, spec=spec),
            (
                BudgetSettlementCompletedEvt(
                    operation_id=op, outcome="released", captured_amount=Decimal(0),
                    currency="CNY", quote_digest=spec.quote_digest(),
                ),
                identity.budget_stream("other"),
            ),
        ])
