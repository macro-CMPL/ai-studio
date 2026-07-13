"""内存数据库共享状态 + 每事务写缓冲。

读:直接看已提交的 DbState。
写:缓冲到 Buffers,commit 时"先全量校验、再全量应用",实现原子性与真正的乐观并发。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from studio.kernel.envelopes import CommandEnvelope, EventEnvelope
from studio.kernel.outcomes import CommandOutcome
from studio.kernel.ports import NewEvent


@dataclass
class OutboxRow:
    message: CommandEnvelope[Any]
    sent: bool = False


@dataclass
class DbState:
    events: list[EventEnvelope[Any]] = field(default_factory=list)
    stream_versions: dict[str, int] = field(default_factory=dict)
    event_digests: dict[str, str] = field(default_factory=dict)
    global_counter: int = 0
    inbox: set[tuple[str, str]] = field(default_factory=set)
    outbox: list[OutboxRow] = field(default_factory=list)
    pm_states: dict[str, tuple[int, Any]] = field(default_factory=dict)
    pm_checkpoints: dict[str, int] = field(default_factory=dict)
    processed: dict[tuple[str, str], CommandOutcome] = field(default_factory=dict)


@dataclass
class Buffers:
    append_intents: list[tuple[str, int, list[NewEvent]]] = field(default_factory=list)
    inbox_adds: list[tuple[str, str]] = field(default_factory=list)
    outbox_adds: list[CommandEnvelope[Any]] = field(default_factory=list)
    outbox_sent: list[str] = field(default_factory=list)
    pm_saves: list[tuple[str, int, Any]] = field(default_factory=list)
    checkpoint_sets: list[tuple[str, int]] = field(default_factory=list)
    processed_puts: list[CommandOutcome] = field(default_factory=list)


class MemoryDatabase:
    """已提交状态的持有者。"""

    def __init__(self) -> None:
        self.state = DbState()
