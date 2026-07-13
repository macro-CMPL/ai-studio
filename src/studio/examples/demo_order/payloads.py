"""DemoOrder 的命令/事件 payload 判别联合。"""

from __future__ import annotations

from typing import Literal

from studio.kernel.envelopes import MessagePayload


# --- Commands ---
class AcceptOrderCmd(MessagePayload):
    type: Literal["accept_order"] = "accept_order"
    order_ref: str


class AdvanceStageCmd(MessagePayload):
    type: Literal["advance_stage"] = "advance_stage"
    stage: str


class DeliverCmd(MessagePayload):
    type: Literal["deliver"] = "deliver"


DemoCommand = AcceptOrderCmd | AdvanceStageCmd | DeliverCmd


# --- Events ---
class OrderAcceptedEvt(MessagePayload):
    type: Literal["order_accepted"] = "order_accepted"
    order_ref: str


class StageAdvancedEvt(MessagePayload):
    type: Literal["stage_advanced"] = "stage_advanced"
    stage: str


class OrderDeliveredEvt(MessagePayload):
    type: Literal["order_delivered"] = "order_delivered"


DemoEvent = OrderAcceptedEvt | StageAdvancedEvt | OrderDeliveredEvt
