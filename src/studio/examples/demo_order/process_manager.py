"""DemoOrder ProcessManager:纯 react,驱动 接受->推进script->交付。

无关事件返回 unchanged state + 空 commands(仍由 Pump 推进 checkpoint)。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from studio.kernel.envelopes import EventEnvelope
from studio.kernel.process_manager import ProposedCommand, Reaction

from .payloads import (
    AdvanceStageCmd,
    DeliverCmd,
    DemoCommand,
    DemoEvent,
    OrderAcceptedEvt,
    StageAdvancedEvt,
)


class DemoOrderPMState(BaseModel):
    model_config = ConfigDict(frozen=True)

    handled: tuple[str, ...] = ()


class DemoOrderProcessManager:
    def __init__(self, pm_id: str = "demo-order-pm") -> None:
        self._pm_id = pm_id

    @property
    def pm_id(self) -> str:
        return self._pm_id

    def initial_state(self) -> DemoOrderPMState:
        return DemoOrderPMState()

    def react(
        self, state: DemoOrderPMState, event: EventEnvelope[DemoEvent]
    ) -> Reaction[DemoOrderPMState, DemoCommand]:
        payload = event.payload
        target = event.stream_id
        next_state = state.model_copy(update={"handled": (*state.handled, payload.type)})

        if isinstance(payload, OrderAcceptedEvt):
            return Reaction(
                state=next_state,
                commands=(
                    ProposedCommand(
                        reaction_name="on-accepted",
                        command_key="advance:script",
                        target=target,
                        payload=AdvanceStageCmd(stage="script"),
                    ),
                ),
            )
        if isinstance(payload, StageAdvancedEvt) and payload.stage == "script":
            return Reaction(
                state=next_state,
                commands=(
                    ProposedCommand(
                        reaction_name="on-script",
                        command_key="deliver",
                        target=target,
                        payload=DeliverCmd(),
                    ),
                ),
            )
        # 其它事件(含 OrderDelivered、其它 stage):不产命令,仅推进状态/checkpoint。
        return Reaction(state=next_state, commands=())
