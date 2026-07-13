"""内核 Ports(协议)。infrastructure 提供实现,application 依赖这些抽象。"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import Any, Protocol

from .envelopes import CommandEnvelope, EventEnvelope, MessagePayload
from .outcomes import CommandOutcome, Versioned


@dataclass(frozen=True)
class NewEvent:
    """待追加的事件:sequence/global_position 由 EventStore 分配。"""

    event_id: str
    schema_version: int
    correlation_id: str
    causation_id: str
    recorded_at: datetime
    payload: MessagePayload


class EventStore(Protocol):
    def current_version(self, stream_id: str) -> int: ...

    def read_stream(self, stream_id: str) -> list[EventEnvelope[Any]]: ...

    def load_stream(self, stream_id: str) -> Versioned[list[EventEnvelope[Any]]]:
        """原子读取:一次返回 (version, events),避免 state/version 撕裂。"""
        ...

    def read_all(self, after_global_position: int) -> list[EventEnvelope[Any]]: ...

    def append(
        self, stream_id: str, expected_version: int, events: Sequence[NewEvent]
    ) -> list[str]:
        """缓冲追加,返回事件 ID(权威值)。校验在 commit 时执行。"""
        ...


class Inbox(Protocol):
    def is_processed(self, consumer_id: str, event_id: str) -> bool: ...

    def mark_processed(self, consumer_id: str, event_id: str) -> None: ...


class Outbox(Protocol):
    def enqueue(self, message: CommandEnvelope[Any]) -> None: ...

    def next_unsent(self) -> CommandEnvelope[Any] | None: ...

    def mark_sent(self, command_id: str) -> None: ...


class ProcessManagerStore(Protocol):
    def load(self, pm_id: str) -> Versioned[Any] | None: ...

    def save(self, pm_id: str, expected_version: int, state: Any) -> None: ...

    def checkpoint(self, pm_id: str) -> int: ...

    def set_checkpoint(self, pm_id: str, global_position: int) -> None: ...


class ProcessedCommandStore(Protocol):
    def get(self, consumer_id: str, command_id: str) -> CommandOutcome | None: ...

    def put(self, outcome: CommandOutcome) -> None: ...


class UnitOfWork(Protocol):
    event_store: EventStore
    inbox: Inbox
    outbox: Outbox
    process_managers: ProcessManagerStore
    processed_commands: ProcessedCommandStore

    def commit(self) -> None: ...

    def __enter__(self) -> UnitOfWork: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...


class UnitOfWorkFactory(Protocol):
    def __call__(self) -> UnitOfWork: ...


class CommandBus(Protocol):
    """命令投递通道(事务外的外部系统)。at-least-once。"""

    def publish(self, message: CommandEnvelope[Any]) -> None: ...

    def peek(self) -> CommandEnvelope[Any] | None: ...

    def ack(self, command_id: str) -> None: ...


class Clock(Protocol):
    def now(self) -> datetime: ...


class IdFactory(Protocol):
    def new_id(self) -> str: ...
