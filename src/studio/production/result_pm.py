"""ProviderResultPM(纯,严格结算屏障):provider 终态 -> 结算 -> (屏障后)发布结果。

pending 持久化在 PM 状态(M2 保证 state+Inbox+Outbox+checkpoint 同事务,崩溃不丢)。
只消费 BudgetSettlementCompleted(不直接消费 BudgetCaptured),核对
operation_id/quote_digest/currency/outcome/captured_amount 与 pending 一致后才发布。
  Succeeded          -> pending(success) + SettleBudget
  Failed(charged)    -> pending(fail)    + SettleBudget
  Failed(uncharged)  -> pending(fail)    + ReleaseBudget
  Aborted            -> pending(fail)    (由 Reconciler 驱动 ReleaseBudget)
  SettlementCompleted-> RecordProviderResult(success)/MarkFailed(fail),并记 completed 防重发。
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict

from studio.domain._base import Sha256Hex
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.kernel.errors import ContractViolation
from studio.kernel.process_manager import ProposedCommand, Reaction

from . import identity
from .attempt_payloads import (
    MarkFailedCmd,
    ProviderExecutionSpecRecordedEvt,
    RecordProviderResultCmd,
)
from .budget import BudgetSettlementCompletedEvt, ReleaseBudgetCmd, SettleBudgetCmd
from .compile import CompiledPipelineSpec
from .execution_spec import ProviderExecutionSpec
from .payloads import TaskAttemptCreatedEvt
from .provider_op import (
    ProviderOperationAbortedEvt,
    ProviderOperationFailedEvt,
    ProviderOperationSucceededEvt,
    ProviderResultRef,
)
from .result_mapper import ResultMapperRegistry

ResultCommand = (
    SettleBudgetCmd | ReleaseBudgetCmd | RecordProviderResultCmd | MarkFailedCmd
)


class ResultAttemptMeta(BaseModel):
    model_config = ConfigDict(frozen=True)
    attempt_id: str
    project_id: str
    stage_id: str


class PendingResult(BaseModel):
    model_config = ConfigDict(frozen=True)
    operation_id: str
    attempt_id: str
    kind: Literal["success", "fail"]
    result_ref: ProviderResultRef | None
    expected_outcome: Literal["captured", "released"]
    expected_captured: Decimal
    currency: str
    quote_digest: Sha256Hex
    reason: str


class ResultPMState(BaseModel):
    model_config = ConfigDict(frozen=True)
    metas: tuple[ResultAttemptMeta, ...] = ()
    specs: tuple[ProviderExecutionSpec, ...] = ()
    pending: tuple[PendingResult, ...] = ()
    completed: tuple[str, ...] = ()

    def meta_of(self, attempt_id: str) -> ResultAttemptMeta | None:
        return next((m for m in self.metas if m.attempt_id == attempt_id), None)

    def spec_of(self, operation_id: str) -> ProviderExecutionSpec | None:
        return next((s for s in self.specs if s.operation_id == operation_id), None)

    def pending_of(self, operation_id: str) -> PendingResult | None:
        return next((p for p in self.pending if p.operation_id == operation_id), None)


class ProviderResultProcessManager:
    pm_id = "provider-result-pm"

    def __init__(
        self, spec: CompiledPipelineSpec, mappers: ResultMapperRegistry
    ) -> None:
        self._spec = spec
        self._mappers = mappers

    def initial_state(self) -> ResultPMState:
        return ResultPMState()

    def react(
        self, state: ResultPMState, event: EventEnvelope[MessagePayload]
    ) -> Reaction[ResultPMState, ResultCommand]:
        payload = event.payload
        if isinstance(payload, TaskAttemptCreatedEvt):
            meta = ResultAttemptMeta(
                attempt_id=payload.attempt_id, project_id=payload.project_id,
                stage_id=payload.stage_id,
            )
            return Reaction(
                state=state.model_copy(update={"metas": (*state.metas, meta)}),
                commands=(),
            )
        if isinstance(payload, ProviderExecutionSpecRecordedEvt):
            return Reaction(
                state=state.model_copy(update={"specs": (*state.specs, payload.spec)}),
                commands=(),
            )
        if isinstance(payload, ProviderOperationSucceededEvt):
            return self._on_succeeded(state, payload)
        if isinstance(payload, ProviderOperationFailedEvt):
            return self._on_failed(state, payload)
        if isinstance(payload, ProviderOperationAbortedEvt):
            return self._on_aborted(state, payload)
        if isinstance(payload, BudgetSettlementCompletedEvt):
            return self._on_settled(state, payload)
        return Reaction(state=state, commands=())

    def _guarded(self, state: ResultPMState, operation_id: str) -> bool:
        """已缓存 pending 或已完成 -> 不重复处理。"""
        return (
            state.pending_of(operation_id) is not None
            or operation_id in state.completed
        )

    def _on_succeeded(
        self, state: ResultPMState, evt: ProviderOperationSucceededEvt
    ) -> Reaction[ResultPMState, ResultCommand]:
        if self._guarded(state, evt.operation_id):
            return Reaction(state=state, commands=())
        spec = state.spec_of(evt.operation_id)
        if spec is None:
            return Reaction(state=state, commands=())
        meta = state.meta_of(spec.attempt_id)
        if meta is None:
            return Reaction(state=state, commands=())
        pending = PendingResult(
            operation_id=evt.operation_id, attempt_id=spec.attempt_id, kind="success",
            result_ref=evt.result_ref, expected_outcome="captured",
            expected_captured=evt.cost_actual, currency=evt.cost_currency,
            quote_digest=spec.quote_digest(), reason="",
        )
        cmd = SettleBudgetCmd(
            project_id=meta.project_id, operation_id=evt.operation_id,
            actual=evt.cost_actual, currency=evt.cost_currency,
            quote_digest=spec.quote_digest(),
        )
        return Reaction(
            state=state.model_copy(update={"pending": (*state.pending, pending)}),
            commands=(
                ProposedCommand(
                    reaction_name="settle", command_key=f"settle:{evt.operation_id}",
                    target=identity.budget_stream(meta.project_id), payload=cmd,
                ),
            ),
        )

    def _on_failed(
        self, state: ResultPMState, evt: ProviderOperationFailedEvt
    ) -> Reaction[ResultPMState, ResultCommand]:
        if self._guarded(state, evt.operation_id):
            return Reaction(state=state, commands=())
        spec = state.spec_of(evt.operation_id)
        if spec is None:
            return Reaction(state=state, commands=())
        meta = state.meta_of(spec.attempt_id)
        if meta is None:
            return Reaction(state=state, commands=())
        if evt.charged:
            pending = PendingResult(
                operation_id=evt.operation_id, attempt_id=spec.attempt_id, kind="fail",
                result_ref=None, expected_outcome="captured",
                expected_captured=evt.cost_actual, currency=evt.cost_currency,
                quote_digest=spec.quote_digest(), reason="provider_failed_charged",
            )
            cmd: ResultCommand = SettleBudgetCmd(
                project_id=meta.project_id, operation_id=evt.operation_id,
                actual=evt.cost_actual, currency=evt.cost_currency,
                quote_digest=spec.quote_digest(),
            )
        else:
            pending = PendingResult(
                operation_id=evt.operation_id, attempt_id=spec.attempt_id, kind="fail",
                result_ref=None, expected_outcome="released",
                expected_captured=Decimal(0), currency=spec.currency,
                quote_digest=spec.quote_digest(), reason="provider_failed",
            )
            cmd = ReleaseBudgetCmd(
                project_id=meta.project_id, operation_id=evt.operation_id,
                quote_digest=spec.quote_digest(),
            )
        return Reaction(
            state=state.model_copy(update={"pending": (*state.pending, pending)}),
            commands=(
                ProposedCommand(
                    reaction_name="settle-fail",
                    command_key=f"settle-fail:{evt.operation_id}",
                    target=identity.budget_stream(meta.project_id), payload=cmd,
                ),
            ),
        )

    def _on_aborted(
        self, state: ResultPMState, evt: ProviderOperationAbortedEvt
    ) -> Reaction[ResultPMState, ResultCommand]:
        # 缓存失败 pending;由 Reconciler 驱动 ReleaseBudget,不在此发预算命令。
        if self._guarded(state, evt.operation_id):
            return Reaction(state=state, commands=())
        spec = state.spec_of(evt.operation_id)
        if spec is None:
            return Reaction(state=state, commands=())
        pending = PendingResult(
            operation_id=evt.operation_id, attempt_id=spec.attempt_id, kind="fail",
            result_ref=None, expected_outcome="released", expected_captured=Decimal(0),
            currency=spec.currency, quote_digest=spec.quote_digest(), reason="aborted",
        )
        return Reaction(
            state=state.model_copy(update={"pending": (*state.pending, pending)}),
            commands=(),
        )

    def _on_settled(
        self, state: ResultPMState, evt: BudgetSettlementCompletedEvt
    ) -> Reaction[ResultPMState, ResultCommand]:
        pending = state.pending_of(evt.operation_id)
        if pending is None or evt.operation_id in state.completed:
            return Reaction(state=state, commands=())
        # 严格屏障:不能仅凭 operation_id 就发布。
        if (
            evt.quote_digest != pending.quote_digest
            or evt.currency != pending.currency
            or evt.outcome != pending.expected_outcome
            or evt.captured_amount != pending.expected_captured
        ):
            raise ContractViolation(
                f"结算屏障与 pending 不一致:operation={evt.operation_id}"
            )
        meta = state.meta_of(pending.attempt_id)
        assert meta is not None
        completed = state.model_copy(
            update={"completed": (*state.completed, evt.operation_id)}
        )
        if pending.kind == "success":
            spec = state.spec_of(evt.operation_id)
            assert spec is not None and pending.result_ref is not None
            stage = self._spec.by_stage(meta.stage_id)
            assert stage is not None
            result_payload = self._mappers.build(spec, pending.result_ref, stage)
            record = RecordProviderResultCmd(
                attempt_id=pending.attempt_id, operation_id=evt.operation_id,
                blob_ref=pending.result_ref.blob_ref, payload=result_payload,
            )
            return Reaction(
                state=completed,
                commands=(
                    ProposedCommand(
                        reaction_name="record-result",
                        command_key=f"result:{evt.operation_id}",
                        target=identity.attempt_stream(pending.attempt_id),
                        payload=record,
                    ),
                ),
            )
        return Reaction(
            state=completed,
            commands=(
                ProposedCommand(
                    reaction_name="mark-failed",
                    command_key=f"failed:{evt.operation_id}",
                    target=identity.attempt_stream(pending.attempt_id),
                    payload=MarkFailedCmd(
                        attempt_id=pending.attempt_id, reason=pending.reason
                    ),
                ),
            ),
        )
