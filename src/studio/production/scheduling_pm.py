"""ProviderSchedulingPM(纯 saga):ExecutionSpec -> 预留 -> 发起 -> 标记等待/阻塞。

严格避免过早标记 WAITING_PROVIDER 造成的墓碑撕裂:
  ProviderExecutionSpecRecorded -> ReserveBudget
  BudgetReserved                -> InitiateProviderOp
  ProviderOperationInitiated    -> MarkWaitingProvider   (而非在 BudgetReserved 时)
  BudgetReservationDeclined     -> MarkBlocked
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.kernel.process_manager import ProposedCommand, Reaction

from . import identity
from .attempt_payloads import (
    MarkBlockedCmd,
    MarkWaitingProviderCmd,
    ProviderExecutionSpecRecordedEvt,
)
from .budget import BudgetReservationDeclinedEvt, BudgetReservedEvt, ReserveBudgetCmd
from .execution_spec import ProviderExecutionSpec
from .payloads import TaskAttemptCreatedEvt
from .provider_op import InitiateProviderOpCmd, ProviderOperationInitiatedEvt

SchedulingCommand = (
    ReserveBudgetCmd | InitiateProviderOpCmd | MarkWaitingProviderCmd | MarkBlockedCmd
)


class SchedulingState(BaseModel):
    model_config = ConfigDict(frozen=True)
    project_by_attempt: tuple[tuple[str, str], ...] = ()
    specs: tuple[ProviderExecutionSpec, ...] = ()

    def project_of(self, attempt_id: str) -> str | None:
        return next((p for (a, p) in self.project_by_attempt if a == attempt_id), None)

    def spec_of(self, operation_id: str) -> ProviderExecutionSpec | None:
        return next((s for s in self.specs if s.operation_id == operation_id), None)


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
            return self._on_reserved(state, payload)
        if isinstance(payload, ProviderOperationInitiatedEvt):
            return self._on_initiated(state, payload)
        if isinstance(payload, BudgetReservationDeclinedEvt):
            return self._on_declined(state, payload)
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
            project_id=project,
            operation_id=spec.operation_id,
            amount=spec.estimated_cost,
            currency=spec.currency,
            quote_digest=spec.quote_digest(),
        )
        return Reaction(
            state=new_state,
            commands=(
                ProposedCommand(
                    reaction_name="reserve",
                    command_key=f"reserve:{spec.operation_id}",
                    target=identity.budget_stream(project),
                    payload=cmd,
                ),
            ),
        )

    def _on_reserved(
        self, state: SchedulingState, payload: BudgetReservedEvt
    ) -> Reaction[SchedulingState, SchedulingCommand]:
        spec = state.spec_of(payload.operation_id)
        if spec is None:
            return Reaction(state=state, commands=())
        return Reaction(
            state=state,
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
        attempt_id = payload.spec.attempt_id
        return Reaction(
            state=state,
            commands=(
                ProposedCommand(
                    reaction_name="waiting-provider",
                    command_key=f"waiting:{payload.operation_id}",
                    target=identity.attempt_stream(attempt_id),
                    payload=MarkWaitingProviderCmd(attempt_id=attempt_id),
                ),
            ),
        )

    def _on_declined(
        self, state: SchedulingState, payload: BudgetReservationDeclinedEvt
    ) -> Reaction[SchedulingState, SchedulingCommand]:
        spec = state.spec_of(payload.operation_id)
        if spec is None:
            return Reaction(state=state, commands=())
        return Reaction(
            state=state,
            commands=(
                ProposedCommand(
                    reaction_name="blocked",
                    command_key=f"blocked:{payload.operation_id}",
                    target=identity.attempt_stream(spec.attempt_id),
                    payload=MarkBlockedCmd(
                        attempt_id=spec.attempt_id, reason="budget_declined"
                    ),
                ),
            ),
        )
