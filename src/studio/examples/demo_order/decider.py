"""DemoOrder Decider:纯 initial_state / decide / evolve。"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from studio.kernel.decisions import Accepted, ProposedEvent, Rejected

from .payloads import (
    AcceptOrderCmd,
    AdvanceStageCmd,
    DeliverCmd,
    DemoCommand,
    DemoEvent,
    OrderAcceptedEvt,
    OrderDeliveredEvt,
    StageAdvancedEvt,
)


class DemoOrderState(BaseModel):
    model_config = ConfigDict(frozen=True)

    accepted: bool = False
    stages: tuple[str, ...] = ()
    delivered: bool = False


class DemoOrderDecider:
    def initial_state(self) -> DemoOrderState:
        return DemoOrderState()

    def decide(
        self, state: DemoOrderState, command: DemoCommand
    ) -> Accepted[DemoEvent] | Rejected:
        if isinstance(command, AcceptOrderCmd):
            if state.accepted:
                return Rejected("already_accepted", "订单已接受")
            return Accepted(
                (
                    ProposedEvent(
                        "order-accepted", OrderAcceptedEvt(order_ref=command.order_ref)
                    ),
                )
            )
        if isinstance(command, AdvanceStageCmd):
            if not state.accepted:
                return Rejected("not_accepted", "订单尚未接受")
            if command.stage in state.stages:
                return Rejected("stage_done", f"阶段 {command.stage} 已完成")
            return Accepted(
                (
                    ProposedEvent(
                        f"stage-advanced:{command.stage}",
                        StageAdvancedEvt(stage=command.stage),
                    ),
                )
            )
        if isinstance(command, DeliverCmd):
            if state.delivered:
                return Rejected("already_delivered", "订单已交付")
            return Accepted((ProposedEvent("order-delivered", OrderDeliveredEvt()),))
        raise AssertionError("unreachable command variant")

    def evolve(self, state: DemoOrderState, event: DemoEvent) -> DemoOrderState:
        if isinstance(event, OrderAcceptedEvt):
            return state.model_copy(update={"accepted": True})
        if isinstance(event, StageAdvancedEvt):
            return state.model_copy(update={"stages": (*state.stages, event.stage)})
        if isinstance(event, OrderDeliveredEvt):
            return state.model_copy(update={"delivered": True})
        raise AssertionError("unreachable event variant")
