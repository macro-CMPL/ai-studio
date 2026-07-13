"""Decider 契约:纯函数 initial_state / decide / evolve。

decide 返回 Decision = Accepted[事实列表] | Rejected(理由)。
只有 evolve 参与重放。decide/evolve 不得触碰 Clock/IdFactory/IO。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

from .envelopes import MessagePayload

TState = TypeVar("TState")
TEvt = TypeVar("TEvt", bound=MessagePayload)
# command 在 decide 中仅作直接入参(纯逆变位置)。
TCmd = TypeVar("TCmd", bound=MessagePayload, contravariant=True)


@dataclass(frozen=True)
class ProposedEvent(Generic[TEvt]):
    """decide 产出的领域事实体。event_key 在同一 Decision 内唯一。"""

    event_key: str
    payload: TEvt


@dataclass(frozen=True)
class Accepted(Generic[TEvt]):
    events: tuple[ProposedEvent[TEvt], ...]


@dataclass(frozen=True)
class Rejected:
    code: str
    message: str


# Decision = Accepted[TEvt] | Rejected(运行时以 isinstance 判别)。


class Decider(Protocol[TState, TCmd, TEvt]):
    def initial_state(self) -> TState: ...

    def decide(self, state: TState, command: TCmd) -> Accepted[TEvt] | Rejected: ...

    def evolve(self, state: TState, event: TEvt) -> TState: ...
