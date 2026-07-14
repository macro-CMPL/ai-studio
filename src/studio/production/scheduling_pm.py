"""ProviderSchedulingPM(纯 saga):ExecutionSpec -> 预留 -> 发起 -> 标记等待/对账/阻塞。

严格避免过早标记 WAITING_PROVIDER 造成的墓碑撕裂,并核对预留内容:
  ProviderExecutionSpecRecorded -> ReserveBudget
  BudgetReserved(amount/currency/quote 须与 spec 一致,否则 ContractViolation)-> Initiate
  ProviderOperationInitiated(spec 一致且预留已确认)-> MarkWaitingProvider
  ProviderOperationSubmissionUnknown -> MarkWaitingReconciliation
  ProviderOperationSubmitted         -> MarkWaitingProvider(UNKNOWN 恢复)
  BudgetReservationDeclined(与 spec 一致才阻塞)-> MarkBlocked
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.kernel.errors import ContractViolation
from studio.kernel.process_manager import ProposedCommand, Reaction

from . import identity
from .attempt_payloads import (
    MarkBlockedCmd,
    MarkWaitingProviderCmd,
    MarkWaitingReconciliationCmd,
    ProviderExecutionSpecRecordedEvt,
)
from .budget import BudgetReservationDeclinedEvt, BudgetReservedEvt, ReserveBudgetCmd
from .execution_spec import ProviderExecutionSpec
from .payloads import TaskAttemptCreatedEvt
from .provider_op import (
    InitiateProviderOpCmd,
    ProviderOperationInitiatedEvt,
    ProviderOperationSubmissionUnknownEvt,
    ProviderOperationSubmittedEvt,
)

SchedulingCommand = (
    ReserveBudgetCmd
    | InitiateProviderOpCmd
    | MarkWaitingProviderCmd
    | MarkWaitingReconciliationCmd
    | MarkBlockedCmd
)


def _reservation_matches(
    spec: ProviderExecutionSpec, amount: object, currency: str, quote_digest: str
) -> bool:
    return (
        amount == spec.estimated_cost
        and currency == spec.currency
        and quote_digest == spec.quote_digest()
    )


class SchedulingState(BaseModel):
    model_config = ConfigDict(frozen=True)
    project_by_attempt: tuple[tuple[str, str], ...] = ()
    specs: tuple[ProviderExecutionSpec, ...] = ()
    confirmed: tuple[str, ...] = ()  # 预留内容与 spec 一致的 operation_id

    def project_of(self, attempt_id: str) -> str | None:
        return next((p for (a, p) in self.project_by_attempt if a == attempt_id), None)

    def spec_of(self, operation_id: str) -> ProviderExecutionSpec | None:
        return next((s for s in self.specs if s.operation_id == operation_id), None)

    def is_confirmed(self, operation_id: str) -> bool:
        return operation_id in self.confirmed


class ProviderSchedulingProcessManager:
    pm_id = "provider-scheduling-pm"

    def initial_state(self) -> SchedulingState:
        return SchedulingState()

    def react(
        self, state: SchedulingState, event: EventEnvelope[MessagePayload]
    ) -> Reaction[SchedulingState, SchedulingCommand]:
        payload = event.payload
        if isinstance(payload, TaskAttemptCreatedEvt):
            return Reaction(
                state=state.model_copy(
                    update={
                        "project_by_attempt": (
                            *state.project_by_attempt,
                            (payload.attempt_id, payload.project_id),
                        )
                    }
                ),
                commands=(),
            )
        if isinstance(payload, ProviderExecutionSpecRecordedEvt):
            return self._on_spec_recorded(state, payload)
        if isinstance(payload, BudgetReservedEvt):
            return self._on_reserved(state, payload, event.stream_id)
        if isinstance(payload, ProviderOperationInitiatedEvt):
            return self._on_initiated(state, payload)
        if isinstance(payload, ProviderOperationSubmittedEvt):
            return self._mark(state, payload.operation_id, provider=True)
        if isinstance(payload, ProviderOperationSubmissionUnknownEvt):
            return self._mark(state, payload.operation_id, provider=False)
        if isinstance(payload, BudgetReservationDeclinedEvt):
            return self._on_declined(state, payload, event.stream_id)
        return Reaction(state=state, commands=())

    def _on_spec_recorded(
        self, state: SchedulingState, payload: ProviderExecutionSpecRecordedEvt
    ) -> Reaction[SchedulingState, SchedulingCommand]:
        spec = payload.spec
        new_state = state.model_copy(update={"specs": (*state.specs, spec)})
        project = state.project_of(spec.attempt_id)
        if project is None:
            return Reaction(state=new_state, commands=())
        cmd = ReserveBudgetCmd(
            project_id=project, operation_id=spec.operation_id,
            amount=spec.estimated_cost, currency=spec.currency,
            quote_digest=spec.quote_digest(),
        )
        return Reaction(
            state=new_state,
            commands=(
                ProposedCommand(
                    reaction_name="reserve", command_key=f"reserve:{spec.operation_id}",
                    target=identity.budget_stream(project), payload=cmd,
                ),
            ),
        )

    def _on_reserved(
        self, state: SchedulingState, payload: BudgetReservedEvt, stream_id: str
    ) -> Reaction[SchedulingState, SchedulingCommand]:
        spec = state.spec_of(payload.operation_id)
        if spec is None:
            return Reaction(state=state, commands=())  # 未追踪(不发起)
        project = state.project_of(spec.attempt_id)
        if project is None:
            return Reaction(state=state, commands=())  # owner 未知,不发起
        # owner 屏障:预留必须来自本项目 budget 流(先于内容指纹,拒绝跨项目串账)。
        identity.require_budget_owner(
            stream_id=stream_id, project_id=project, operation_id=payload.operation_id
        )
        if not _reservation_matches(
            spec, payload.amount, payload.currency, payload.quote_digest
        ):
            raise ContractViolation(
                f"预留与 spec 不一致:operation={payload.operation_id}"
            )
        new_state = state.model_copy(
            update={"confirmed": (*state.confirmed, payload.operation_id)}
        )
        return Reaction(
            state=new_state,
            commands=(
                ProposedCommand(
                    reaction_name="initiate",
                    command_key=f"initiate:{payload.operation_id}",
                    target=identity.provider_op_stream(payload.operation_id),
                    payload=InitiateProviderOpCmd(
                        operation_id=payload.operation_id, spec=spec
                    ),
                ),
            ),
        )

    def _on_initiated(
        self, state: SchedulingState, payload: ProviderOperationInitiatedEvt
    ) -> Reaction[SchedulingState, SchedulingCommand]:
        spec = state.spec_of(payload.operation_id)
        # 只有 spec 完全一致且预留已确认才推进 attempt。
        if (
            spec is None
            or payload.spec != spec
            or not state.is_confirmed(payload.operation_id)
        ):
            return Reaction(state=state, commands=())
        return Reaction(
            state=state,
            commands=(
                ProposedCommand(
                    reaction_name="waiting-provider",
                    command_key=f"waiting:{payload.operation_id}",
                    target=identity.attempt_stream(spec.attempt_id),
                    payload=MarkWaitingProviderCmd(attempt_id=spec.attempt_id),
                ),
            ),
        )

    def _mark(
        self, state: SchedulingState, operation_id: str, *, provider: bool
    ) -> Reaction[SchedulingState, SchedulingCommand]:
        """provider=True -> WAITING_PROVIDER(Submitted);False -> WAITING_RECONCILIATION。"""
        spec = state.spec_of(operation_id)
        if spec is None or not state.is_confirmed(operation_id):
            return Reaction(state=state, commands=())
        if provider:
            cmd: SchedulingCommand = MarkWaitingProviderCmd(attempt_id=spec.attempt_id)
            name, key = "resubmitted", f"resubmitted:{operation_id}"
        else:
            cmd = MarkWaitingReconciliationCmd(attempt_id=spec.attempt_id)
            name, key = "waiting-reconciliation", f"reconcile:{operation_id}"
        return Reaction(
            state=state,
            commands=(
                ProposedCommand(
                    reaction_name=name, command_key=key,
                    target=identity.attempt_stream(spec.attempt_id), payload=cmd,
                ),
            ),
        )

    def _on_declined(
        self, state: SchedulingState, payload: BudgetReservationDeclinedEvt, stream_id: str
    ) -> Reaction[SchedulingState, SchedulingCommand]:
        spec = state.spec_of(payload.operation_id)
        if spec is None:
            return Reaction(state=state, commands=())
        project = state.project_of(spec.attempt_id)
        if project is None:
            return Reaction(state=state, commands=())  # owner 未知,不阻塞
        # owner 屏障:decline 也必须来自本项目 budget 流,否则跨项目误阻塞。
        identity.require_budget_owner(
            stream_id=stream_id, project_id=project, operation_id=payload.operation_id
        )
        # 与 spec 不一致的 decline 视为错配,忽略以免错误阻塞同 operation。
        if not _reservation_matches(
            spec, payload.amount, payload.currency, payload.quote_digest
        ):
            return Reaction(state=state, commands=())
        return Reaction(
            state=state,
            commands=(
                ProposedCommand(
                    reaction_name="blocked", command_key=f"blocked:{payload.operation_id}",
                    target=identity.attempt_stream(spec.attempt_id),
                    payload=MarkBlockedCmd(
                        attempt_id=spec.attempt_id, reason="budget_declined"
                    ),
                ),
            ),
        )
