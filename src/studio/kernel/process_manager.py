"""ProcessManager 契约:纯函数 react。

react(pm_state, event) -> Reaction(new_state, commands)。
即使不关心该事件,也必须返回(可能是 unchanged state + 空 commands),
由 Event Pump 事务性推进 Inbox/checkpoint,避免在无关事件上死循环。
react 不铸造 command_id(由 Event Pump 派生),保持纯函数。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

from .envelopes import EventEnvelope, MessagePayload

TPMState = TypeVar("TPMState")
TEvt = TypeVar("TEvt", bound=MessagePayload)
TCmd = TypeVar("TCmd", bound=MessagePayload)


@dataclass(frozen=True)
class ProposedCommand(Generic[TCmd]):
    reaction_name: str
    command_key: str
    target: str
    payload: TCmd


@dataclass(frozen=True)
class Reaction(Generic[TPMState, TCmd]):
    state: TPMState
    commands: tuple[ProposedCommand[TCmd], ...]


class ProcessManager(Protocol[TPMState, TEvt, TCmd]):
    @property
    def pm_id(self) -> str: ...

    def initial_state(self) -> TPMState: ...

    def react(
        self, state: TPMState, event: EventEnvelope[TEvt]
    ) -> Reaction[TPMState, TCmd]: ...
