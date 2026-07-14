"""M4 步骤3:ProviderSchedulingPM(saga)+ ProviderResultPM(结算屏障)。"""

from __future__ import annotations

from datetime import UTC, datetime
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
from studio.domain.enums import ArtifactType, PropagationMode
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.kernel.errors import ContractViolation
from studio.production.attempt_payloads import (
    MarkBlockedCmd,
    MarkFailedCmd,
    MarkWaitingProviderCmd,
    MarkWaitingReconciliationCmd,
    ProviderExecutionSpecRecordedEvt,
    RecordProviderResultCmd,
)
from studio.production.budget import (
    BudgetReservationDeclinedEvt,
    BudgetReservedEvt,
    BudgetSettlementCompletedEvt,
    ReleaseBudgetCmd,
    ReserveBudgetCmd,
    SettleBudgetCmd,
)
from studio.production.execution_spec import ProviderExecutionSpec
from studio.production.payloads import TaskAttemptCreatedEvt
from studio.production.pipeline import golden_compiled
from studio.production.provider_op import (
    InitiateProviderOpCmd,
    ProviderOperationAbortedEvt,
    ProviderOperationFailedEvt,
    ProviderOperationInitiatedEvt,
    ProviderOperationSubmissionUnknownEvt,
    ProviderOperationSubmittedEvt,
    ProviderOperationSucceededEvt,
    ProviderResultRef,
)
from studio.production.result_mapper import default_result_mappers
from studio.production.result_pm import ProviderResultProcessManager
from studio.production.scheduling_pm import ProviderSchedulingProcessManager
from studio.production.values import BindingItem
from studio.serialization import digest

_P = "p"
_PART = "shot_01"
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
    from studio.production import identity

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


def _of(cmds: list[Any], cls: type) -> list[Any]:
    return [c for c in cmds if isinstance(c.payload, cls)]


# --- ProviderSchedulingPM --- #


def test_scheduling_reserve_initiate_waiting() -> None:
    aid = _image_attempt_id()
    spec = _spec(aid)
    op = spec.operation_id
    cmds = _run(
        ProviderSchedulingProcessManager(),
        [
            _created(),
            ProviderExecutionSpecRecordedEvt(attempt_id=aid, spec=spec),
            BudgetReservedEvt(
                operation_id=op, amount=spec.estimated_cost, currency="CNY",
                quote_digest=spec.quote_digest(),
            ),
            ProviderOperationInitiatedEvt(operation_id=op, spec=spec),
        ],
    )
    reserve = _of(cmds, ReserveBudgetCmd)
    assert len(reserve) == 1
    assert reserve[0].target == f"budget:{_P}"
    assert reserve[0].payload.amount == spec.estimated_cost
    assert reserve[0].payload.quote_digest == spec.quote_digest()
    initiate = _of(cmds, InitiateProviderOpCmd)
    assert len(initiate) == 1 and initiate[0].target == f"provider-op:{op}"
    waiting = _of(cmds, MarkWaitingProviderCmd)
    assert len(waiting) == 1 and waiting[0].payload.attempt_id == aid


def test_scheduling_no_waiting_before_initiated() -> None:
    aid = _image_attempt_id()
    spec = _spec(aid)
    op = spec.operation_id
    cmds = _run(
        ProviderSchedulingProcessManager(),
        [
            _created(),
            ProviderExecutionSpecRecordedEvt(attempt_id=aid, spec=spec),
            BudgetReservedEvt(
                operation_id=op, amount=spec.estimated_cost, currency="CNY",
                quote_digest=spec.quote_digest(),
            ),
        ],
    )
    assert not _of(cmds, MarkWaitingProviderCmd)  # 未 Initiated 不标 WAITING_PROVIDER
    assert len(_of(cmds, InitiateProviderOpCmd)) == 1


def test_scheduling_forged_reservation_raises() -> None:
    aid = _image_attempt_id()
    spec = _spec(aid)
    op = spec.operation_id
    # 预留金额与 spec 不符 -> ContractViolation,绝不 Initiate
    with pytest.raises(ContractViolation):
        _run(
            ProviderSchedulingProcessManager(),
            [
                _created(),
                ProviderExecutionSpecRecordedEvt(attempt_id=aid, spec=spec),
                BudgetReservedEvt(
                    operation_id=op, amount=Decimal("0.01"), currency="CNY",
                    quote_digest=spec.quote_digest(),
                ),
            ],
        )


def test_scheduling_initiated_without_reservation_no_waiting() -> None:
    aid = _image_attempt_id()
    spec = _spec(aid)
    op = spec.operation_id
    cmds = _run(
        ProviderSchedulingProcessManager(),
        [
            _created(),
            ProviderExecutionSpecRecordedEvt(attempt_id=aid, spec=spec),
            ProviderOperationInitiatedEvt(operation_id=op, spec=spec),  # 无已确认预留
        ],
    )
    assert not _of(cmds, MarkWaitingProviderCmd)  # 预留未确认不推进


def test_scheduling_unknown_and_submitted_transitions() -> None:
    aid = _image_attempt_id()
    spec = _spec(aid)
    op = spec.operation_id
    cmds = _run(
        ProviderSchedulingProcessManager(),
        [
            _created(),
            ProviderExecutionSpecRecordedEvt(attempt_id=aid, spec=spec),
            BudgetReservedEvt(
                operation_id=op, amount=spec.estimated_cost, currency="CNY",
                quote_digest=spec.quote_digest(),
            ),
            ProviderOperationSubmissionUnknownEvt(operation_id=op, reason="timeout"),
            ProviderOperationSubmittedEvt(
                operation_id=op, job_id="j", provider_event_id="pe"
            ),
        ],
    )
    assert len(_of(cmds, MarkWaitingReconciliationCmd)) == 1
    assert len(_of(cmds, MarkWaitingProviderCmd)) == 1  # submitted 恢复


def test_scheduling_declined_mismatch_ignored() -> None:
    aid = _image_attempt_id()
    spec = _spec(aid)
    op = spec.operation_id
    cmds = _run(
        ProviderSchedulingProcessManager(),
        [
            _created(),
            ProviderExecutionSpecRecordedEvt(attempt_id=aid, spec=spec),
            BudgetReservationDeclinedEvt(
                operation_id=op, amount=spec.estimated_cost, available=Decimal(0),
                currency="CNY", quote_digest="f" * 64,  # quote 不符
            ),
        ],
    )
    assert not _of(cmds, MarkBlockedCmd)  # 错配 decline 不阻塞


def test_scheduling_declined_blocks() -> None:
    aid = _image_attempt_id()
    spec = _spec(aid)
    op = spec.operation_id
    cmds = _run(
        ProviderSchedulingProcessManager(),
        [
            _created(),
            ProviderExecutionSpecRecordedEvt(attempt_id=aid, spec=spec),
            BudgetReservationDeclinedEvt(
                operation_id=op, amount=spec.estimated_cost, available=Decimal(0),
                currency="CNY", quote_digest=spec.quote_digest(),
            ),
        ],
    )
    blocked = _of(cmds, MarkBlockedCmd)
    assert len(blocked) == 1 and blocked[0].payload.attempt_id == aid
    assert not _of(cmds, InitiateProviderOpCmd)


# --- ProviderResultPM(结算屏障) --- #


def _result_pm() -> ProviderResultProcessManager:
    return ProviderResultProcessManager(golden_compiled(), default_result_mappers())


def _prelude(spec: ProviderExecutionSpec) -> list[MessagePayload]:
    return [_created(), ProviderExecutionSpecRecordedEvt(attempt_id=spec.attempt_id, spec=spec)]


def test_result_success_barrier_publishes_after_settlement() -> None:
    spec = _spec(_image_attempt_id())
    op = spec.operation_id
    events = _prelude(spec) + [
        ProviderOperationSucceededEvt(
            operation_id=op,
            result_ref=ProviderResultRef(blob_ref="blob://final", digest="a" * 64),
            cost_actual=Decimal("12.5"), cost_currency="CNY", provider_event_id="pe-1",
        ),
        BudgetSettlementCompletedEvt(
            operation_id=op, outcome="captured", captured_amount=Decimal("12.5"),
            currency="CNY", quote_digest=spec.quote_digest(),
        ),
    ]
    cmds = _run(_result_pm(), events)
    assert len(_of(cmds, SettleBudgetCmd)) == 1
    records = _of(cmds, RecordProviderResultCmd)
    assert len(records) == 1
    assert records[0].payload.blob_ref == "blob://final"
    assert records[0].payload.payload.kind is ArtifactType.IMAGE


def test_result_no_publish_before_settlement() -> None:
    spec = _spec(_image_attempt_id())
    op = spec.operation_id
    events = _prelude(spec) + [
        ProviderOperationSucceededEvt(
            operation_id=op,
            result_ref=ProviderResultRef(blob_ref="blob://final", digest="a" * 64),
            cost_actual=Decimal("12.5"), cost_currency="CNY", provider_event_id="pe-1",
        ),
    ]
    cmds = _run(_result_pm(), events)
    assert len(_of(cmds, SettleBudgetCmd)) == 1
    assert not _of(cmds, RecordProviderResultCmd)  # 屏障未过,不发布


def test_result_barrier_rejects_amount_mismatch() -> None:
    spec = _spec(_image_attempt_id())
    op = spec.operation_id
    events = _prelude(spec) + [
        ProviderOperationSucceededEvt(
            operation_id=op,
            result_ref=ProviderResultRef(blob_ref="blob://final", digest="a" * 64),
            cost_actual=Decimal("12.5"), cost_currency="CNY", provider_event_id="pe-1",
        ),
        BudgetSettlementCompletedEvt(
            operation_id=op, outcome="captured", captured_amount=Decimal("99"),
            currency="CNY", quote_digest=spec.quote_digest(),
        ),
    ]
    with pytest.raises(ContractViolation):
        _run(_result_pm(), events)


def test_result_charged_failure_settles_then_marks_failed() -> None:
    spec = _spec(_image_attempt_id())
    op = spec.operation_id
    events = _prelude(spec) + [
        ProviderOperationFailedEvt(
            operation_id=op, charged=True, cost_actual=Decimal("5"),
            cost_currency="CNY", provider_event_id="pe-1",
        ),
        BudgetSettlementCompletedEvt(
            operation_id=op, outcome="captured", captured_amount=Decimal("5"),
            currency="CNY", quote_digest=spec.quote_digest(),
        ),
    ]
    cmds = _run(_result_pm(), events)
    assert len(_of(cmds, SettleBudgetCmd)) == 1
    assert len(_of(cmds, MarkFailedCmd)) == 1
    assert not _of(cmds, RecordProviderResultCmd)


def test_result_uncharged_failure_releases_then_marks_failed() -> None:
    spec = _spec(_image_attempt_id())
    op = spec.operation_id
    events = _prelude(spec) + [
        ProviderOperationFailedEvt(
            operation_id=op, charged=False, cost_actual=Decimal(0),
            cost_currency="CNY", provider_event_id="pe-1",
        ),
        BudgetSettlementCompletedEvt(
            operation_id=op, outcome="released", captured_amount=Decimal(0),
            currency="CNY", quote_digest=spec.quote_digest(),
        ),
    ]
    cmds = _run(_result_pm(), events)
    assert len(_of(cmds, ReleaseBudgetCmd)) == 1
    assert len(_of(cmds, MarkFailedCmd)) == 1


def test_result_aborted_waits_reconciler_release() -> None:
    spec = _spec(_image_attempt_id())
    op = spec.operation_id
    # Aborted 本身不发预算命令;Reconciler 释放后的 SettlementCompleted(released) 才 MarkFailed
    after_abort = _run(_result_pm(), _prelude(spec) + [
        ProviderOperationAbortedEvt(operation_id=op, reason="recycle"),
    ])
    assert not _of(after_abort, ReleaseBudgetCmd)
    assert not _of(after_abort, SettleBudgetCmd)
    assert not _of(after_abort, MarkFailedCmd)

    full = _run(_result_pm(), _prelude(spec) + [
        ProviderOperationAbortedEvt(operation_id=op, reason="recycle"),
        BudgetSettlementCompletedEvt(
            operation_id=op, outcome="released", captured_amount=Decimal(0),
            currency="CNY", quote_digest=spec.quote_digest(),
        ),
    ])
    assert not _of(full, ReleaseBudgetCmd)  # release 来自 Reconciler,不来自 ResultPM
    assert len(_of(full, MarkFailedCmd)) == 1


def test_result_completed_guard_no_republish() -> None:
    spec = _spec(_image_attempt_id())
    op = spec.operation_id
    settle = BudgetSettlementCompletedEvt(
        operation_id=op, outcome="captured", captured_amount=Decimal("12.5"),
        currency="CNY", quote_digest=spec.quote_digest(),
    )
    events = _prelude(spec) + [
        ProviderOperationSucceededEvt(
            operation_id=op,
            result_ref=ProviderResultRef(blob_ref="blob://final", digest="a" * 64),
            cost_actual=Decimal("12.5"), cost_currency="CNY", provider_event_id="pe-1",
        ),
        settle,
        settle,  # 二次投递
    ]
    cmds = _run(_result_pm(), events)
    assert len(_of(cmds, RecordProviderResultCmd)) == 1  # 不重复发布
