"""BudgetDecider(budget:{project} 流):强预算不变式的串行化点(不分片)。

- reserve/settle 均以 operation_id 幂等;同 op 异金额/币种/quote_digest -> IdempotencyConflict。
- SettleBudget 原子产出 CAPTURE (+RELEASE 差额) (+OVERRUN) + BudgetSettlementCompleted(始终最后)。
- 已发生的 CAPTURE 永不因余额不足被拒;实际突破预留才 OVERRUN + 阻止后续 reserve。
- 预留不足只 BudgetReservationDeclined(阻塞本 Attempt),不把预算聚合永久标脏。
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict

from studio.domain._base import Currency, NonBlank, NonNegativeMoney
from studio.domain.enums import LedgerDirection
from studio.kernel.decisions import Accepted, ProposedEvent, Rejected
from studio.kernel.envelopes import MessagePayload
from studio.kernel.errors import IdempotencyConflict

# --------------------------------------------------------------------------- #
# Payloads
# --------------------------------------------------------------------------- #


class InitializeBudgetCmd(MessagePayload):
    type: Literal["initialize_budget"] = "initialize_budget"
    project_id: str
    total: NonNegativeMoney
    currency: Currency


class ReserveBudgetCmd(MessagePayload):
    type: Literal["reserve_budget"] = "reserve_budget"
    project_id: str
    operation_id: str
    amount: NonNegativeMoney
    currency: Currency
    quote_digest: str


class SettleBudgetCmd(MessagePayload):
    type: Literal["settle_budget"] = "settle_budget"
    project_id: str
    operation_id: str
    actual: NonNegativeMoney
    currency: Currency
    quote_digest: str


class ReleaseBudgetCmd(MessagePayload):
    type: Literal["release_budget"] = "release_budget"
    project_id: str
    operation_id: str
    quote_digest: str


class AdjustBudgetCmd(MessagePayload):
    type: Literal["adjust_budget"] = "adjust_budget"
    project_id: str
    entry_id: str
    direction: LedgerDirection
    amount: NonNegativeMoney
    currency: Currency
    reason: NonBlank
    authority_ref: NonBlank


BudgetCommand = (
    InitializeBudgetCmd
    | ReserveBudgetCmd
    | SettleBudgetCmd
    | ReleaseBudgetCmd
    | AdjustBudgetCmd
)


class BudgetInitializedEvt(MessagePayload):
    type: Literal["budget_initialized"] = "budget_initialized"
    project_id: str
    total: NonNegativeMoney
    currency: Currency


class BudgetReservedEvt(MessagePayload):
    type: Literal["budget_reserved"] = "budget_reserved"
    operation_id: str
    amount: NonNegativeMoney
    currency: Currency
    quote_digest: str


class BudgetReservationDeclinedEvt(MessagePayload):
    type: Literal["budget_reservation_declined"] = "budget_reservation_declined"
    operation_id: str
    amount: NonNegativeMoney
    available: Decimal
    currency: Currency


class BudgetCapturedEvt(MessagePayload):
    type: Literal["budget_captured"] = "budget_captured"
    operation_id: str
    amount: NonNegativeMoney
    currency: Currency


class BudgetReleasedEvt(MessagePayload):
    type: Literal["budget_released"] = "budget_released"
    operation_id: str
    amount: NonNegativeMoney
    currency: Currency


class BudgetOverrunRecordedEvt(MessagePayload):
    type: Literal["budget_overrun_recorded"] = "budget_overrun_recorded"
    operation_id: str
    overrun_amount: NonNegativeMoney
    currency: Currency


class BudgetSettlementCompletedEvt(MessagePayload):
    type: Literal["budget_settlement_completed"] = "budget_settlement_completed"
    operation_id: str
    outcome: Literal["captured", "released"]
    captured_amount: NonNegativeMoney
    quote_digest: str


class BudgetAdjustedEvt(MessagePayload):
    type: Literal["budget_adjusted"] = "budget_adjusted"
    entry_id: str
    direction: LedgerDirection
    amount: NonNegativeMoney
    currency: Currency
    reason: str
    authority_ref: str


BudgetEvent = (
    BudgetInitializedEvt
    | BudgetReservedEvt
    | BudgetReservationDeclinedEvt
    | BudgetCapturedEvt
    | BudgetReleasedEvt
    | BudgetOverrunRecordedEvt
    | BudgetSettlementCompletedEvt
    | BudgetAdjustedEvt
)


# --------------------------------------------------------------------------- #
# State
# --------------------------------------------------------------------------- #


class Reservation(BaseModel):
    model_config = ConfigDict(frozen=True)
    operation_id: str
    amount: Decimal
    quote_digest: str


class Settlement(BaseModel):
    model_config = ConfigDict(frozen=True)
    operation_id: str
    captured_amount: Decimal
    quote_digest: str
    outcome: Literal["captured", "released"]


class BudgetState(BaseModel):
    model_config = ConfigDict(frozen=True)
    initialized: bool = False
    currency: str | None = None
    total: Decimal = Decimal(0)
    adj_credit: Decimal = Decimal(0)
    adj_debit: Decimal = Decimal(0)
    captured_total: Decimal = Decimal(0)
    reservations: tuple[Reservation, ...] = ()
    settlements: tuple[Settlement, ...] = ()
    adjustments: tuple[str, ...] = ()
    overrun: bool = False

    @property
    def reserved_total(self) -> Decimal:
        return sum((r.amount for r in self.reservations), Decimal(0))

    @property
    def available(self) -> Decimal:
        return (
            self.total
            + self.adj_credit
            - self.adj_debit
            - self.captured_total
            - self.reserved_total
        )

    def reservation(self, op: str) -> Reservation | None:
        return next((r for r in self.reservations if r.operation_id == op), None)

    def settlement(self, op: str) -> Settlement | None:
        return next((s for s in self.settlements if s.operation_id == op), None)


# --------------------------------------------------------------------------- #
# Decider
# --------------------------------------------------------------------------- #


class BudgetDecider:
    def initial_state(self) -> BudgetState:
        return BudgetState()

    def decide(
        self, state: BudgetState, command: BudgetCommand
    ) -> Accepted[BudgetEvent] | Rejected:
        if isinstance(command, InitializeBudgetCmd):
            return self._initialize(state, command)
        if isinstance(command, ReserveBudgetCmd):
            return self._reserve(state, command)
        if isinstance(command, SettleBudgetCmd):
            return self._settle(state, command)
        if isinstance(command, ReleaseBudgetCmd):
            return self._release(state, command)
        if isinstance(command, AdjustBudgetCmd):
            return self._adjust(state, command)
        return Rejected("unexpected_command", "budget 流不处理该命令")

    def _initialize(
        self, state: BudgetState, cmd: InitializeBudgetCmd
    ) -> Accepted[BudgetEvent] | Rejected:
        if state.initialized:
            return Rejected("already_initialized", "预算已初始化")
        return Accepted(
            (
                ProposedEvent(
                    "initialized",
                    BudgetInitializedEvt(
                        project_id=cmd.project_id, total=cmd.total, currency=cmd.currency
                    ),
                ),
            )
        )

    def _check_currency(self, state: BudgetState, currency: str) -> Rejected | None:
        if not state.initialized:
            return Rejected("not_initialized", "预算未初始化")
        if currency != state.currency:
            return Rejected("currency_mismatch", "币种与预算不一致")
        return None

    def _reserve(
        self, state: BudgetState, cmd: ReserveBudgetCmd
    ) -> Accepted[BudgetEvent] | Rejected:
        if (bad := self._check_currency(state, cmd.currency)) is not None:
            return bad
        if state.settlement(cmd.operation_id) is not None:
            return Rejected("already_settled", "该 operation 已结算")
        prior = state.reservation(cmd.operation_id)
        if prior is not None:
            if prior.amount == cmd.amount and prior.quote_digest == cmd.quote_digest:
                return Accepted(())  # 幂等
            raise IdempotencyConflict(cmd.operation_id, "reserve 金额/quote 不一致")
        if state.overrun or state.available < cmd.amount:
            return Accepted(
                (
                    ProposedEvent(
                        f"declined:{cmd.operation_id}",
                        BudgetReservationDeclinedEvt(
                            operation_id=cmd.operation_id,
                            amount=cmd.amount,
                            available=state.available,
                            currency=cmd.currency,
                        ),
                    ),
                )
            )
        return Accepted(
            (
                ProposedEvent(
                    f"reserved:{cmd.operation_id}",
                    BudgetReservedEvt(
                        operation_id=cmd.operation_id,
                        amount=cmd.amount,
                        currency=cmd.currency,
                        quote_digest=cmd.quote_digest,
                    ),
                ),
            )
        )

    def _settle(
        self, state: BudgetState, cmd: SettleBudgetCmd
    ) -> Accepted[BudgetEvent] | Rejected:
        if (bad := self._check_currency(state, cmd.currency)) is not None:
            return bad
        done = state.settlement(cmd.operation_id)
        if done is not None:
            if done.captured_amount == cmd.actual and done.quote_digest == cmd.quote_digest:
                return Accepted(())  # 幂等
            raise IdempotencyConflict(cmd.operation_id, "settle 金额/quote 不一致")
        reservation = state.reservation(cmd.operation_id)
        if reservation is None:
            return Rejected("unknown_reservation", "无对应预留")
        if reservation.quote_digest != cmd.quote_digest:
            raise IdempotencyConflict(cmd.operation_id, "settle quote 与 reserve 不一致")

        events: list[ProposedEvent[BudgetEvent]] = [
            ProposedEvent(
                f"captured:{cmd.operation_id}",
                BudgetCapturedEvt(
                    operation_id=cmd.operation_id, amount=cmd.actual, currency=cmd.currency
                ),
            )
        ]
        if cmd.actual < reservation.amount:
            events.append(
                ProposedEvent(
                    f"released:{cmd.operation_id}",
                    BudgetReleasedEvt(
                        operation_id=cmd.operation_id,
                        amount=reservation.amount - cmd.actual,
                        currency=cmd.currency,
                    ),
                )
            )
        elif cmd.actual > reservation.amount:
            events.append(
                ProposedEvent(
                    f"overrun:{cmd.operation_id}",
                    BudgetOverrunRecordedEvt(
                        operation_id=cmd.operation_id,
                        overrun_amount=cmd.actual - reservation.amount,
                        currency=cmd.currency,
                    ),
                )
            )
        events.append(
            ProposedEvent(
                f"completed:{cmd.operation_id}",
                BudgetSettlementCompletedEvt(
                    operation_id=cmd.operation_id,
                    outcome="captured",
                    captured_amount=cmd.actual,
                    quote_digest=cmd.quote_digest,
                ),
            )
        )
        return Accepted(tuple(events))

    def _release(
        self, state: BudgetState, cmd: ReleaseBudgetCmd
    ) -> Accepted[BudgetEvent] | Rejected:
        if not state.initialized:
            return Rejected("not_initialized", "预算未初始化")
        done = state.settlement(cmd.operation_id)
        if done is not None:
            if done.outcome == "released" and done.quote_digest == cmd.quote_digest:
                return Accepted(())
            raise IdempotencyConflict(cmd.operation_id, "release 与既有结算冲突")
        reservation = state.reservation(cmd.operation_id)
        if reservation is None:
            return Rejected("unknown_reservation", "无对应预留")
        assert state.currency is not None
        return Accepted(
            (
                ProposedEvent(
                    f"released:{cmd.operation_id}",
                    BudgetReleasedEvt(
                        operation_id=cmd.operation_id,
                        amount=reservation.amount,
                        currency=state.currency,
                    ),
                ),
                ProposedEvent(
                    f"completed:{cmd.operation_id}",
                    BudgetSettlementCompletedEvt(
                        operation_id=cmd.operation_id,
                        outcome="released",
                        captured_amount=Decimal(0),
                        quote_digest=cmd.quote_digest,
                    ),
                ),
            )
        )

    def _adjust(
        self, state: BudgetState, cmd: AdjustBudgetCmd
    ) -> Accepted[BudgetEvent] | Rejected:
        if (bad := self._check_currency(state, cmd.currency)) is not None:
            return bad
        if cmd.entry_id in state.adjustments:
            return Accepted(())  # 幂等
        return Accepted(
            (
                ProposedEvent(
                    f"adjusted:{cmd.entry_id}",
                    BudgetAdjustedEvt(
                        entry_id=cmd.entry_id,
                        direction=cmd.direction,
                        amount=cmd.amount,
                        currency=cmd.currency,
                        reason=cmd.reason,
                        authority_ref=cmd.authority_ref,
                    ),
                ),
            )
        )

    def evolve(self, state: BudgetState, event: BudgetEvent) -> BudgetState:
        if isinstance(event, BudgetInitializedEvt):
            return state.model_copy(
                update={"initialized": True, "currency": event.currency, "total": event.total}
            )
        if isinstance(event, BudgetReservedEvt):
            return state.model_copy(
                update={
                    "reservations": (
                        *state.reservations,
                        Reservation(
                            operation_id=event.operation_id,
                            amount=event.amount,
                            quote_digest=event.quote_digest,
                        ),
                    )
                }
            )
        if isinstance(event, BudgetCapturedEvt):
            return state.model_copy(
                update={"captured_total": state.captured_total + event.amount}
            )
        if isinstance(event, BudgetOverrunRecordedEvt):
            return state.model_copy(update={"overrun": True})
        if isinstance(event, BudgetSettlementCompletedEvt):
            remaining = tuple(
                r for r in state.reservations if r.operation_id != event.operation_id
            )
            return state.model_copy(
                update={
                    "reservations": remaining,
                    "settlements": (
                        *state.settlements,
                        Settlement(
                            operation_id=event.operation_id,
                            captured_amount=event.captured_amount,
                            quote_digest=event.quote_digest,
                            outcome=event.outcome,
                        ),
                    ),
                }
            )
        if isinstance(event, BudgetAdjustedEvt):
            if event.direction is LedgerDirection.CREDIT:
                return state.model_copy(
                    update={
                        "adj_credit": state.adj_credit + event.amount,
                        "adjustments": (*state.adjustments, event.entry_id),
                    }
                )
            return state.model_copy(
                update={
                    "adj_debit": state.adj_debit + event.amount,
                    "adjustments": (*state.adjustments, event.entry_id),
                }
            )
        # BudgetReservationDeclinedEvt / BudgetReleasedEvt:无状态迁移(信息事实)
        return state
