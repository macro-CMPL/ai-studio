"""内存 UnitOfWork:缓冲写,commit 时"先全量校验、再全量应用"。

- 读:已提交状态。写:缓冲。
- commit:校验所有 append 的 expected_version 与幂等,校验所有 pm_save 的版本;
  任一失败则抛异常且不改动任何状态(原子);全部通过则一次性应用。
- 真正的乐观并发:基于快照 expected_version,提交时对活动状态复核。
CommandBus 为事务外通道(at-least-once)。
"""

from __future__ import annotations

from types import TracebackType
from typing import Any

from studio.kernel.envelopes import CommandEnvelope, EventEnvelope
from studio.kernel.errors import ConcurrencyConflict, IdempotencyConflict
from studio.kernel.ports import NewEvent
from studio.serialization import digest

from ._state import Buffers, DbState, MemoryDatabase, OutboxRow
from .event_store import MemoryEventStore
from .inbox import MemoryInbox
from .outbox import MemoryOutbox
from .process_manager_store import MemoryProcessManagerStore
from .processed_commands import MemoryProcessedCommandStore


class MemoryUnitOfWork:
    def __init__(self, db: MemoryDatabase) -> None:
        self._db = db
        self._buffers = Buffers()
        self._committed = False

    def __enter__(self) -> MemoryUnitOfWork:
        s = self._db.state
        b = self._buffers
        self.event_store = MemoryEventStore(s, b)
        self.inbox = MemoryInbox(s, b)
        self.outbox = MemoryOutbox(s, b)
        self.process_managers = MemoryProcessManagerStore(s, b)
        self.processed_commands = MemoryProcessedCommandStore(s, b)
        return self

    # -- commit：校验-全部，再应用-全部 -------------------------------------- #

    def _is_idempotent_noop(self, s: DbState, events: list[NewEvent]) -> bool:
        existing = [ne for ne in events if ne.event_id in s.event_digests]
        for ne in existing:
            if s.event_digests[ne.event_id] != digest(ne.payload):
                raise IdempotencyConflict(ne.event_id)
        if existing and len(existing) != len(events):
            raise IdempotencyConflict(existing[0].event_id)
        return bool(existing) and len(existing) == len(events)

    def commit(self) -> None:
        s = self._db.state
        b = self._buffers

        # 1) 校验 appends
        noop_flags: list[bool] = []
        for stream, expected, events in b.append_intents:
            noop = self._is_idempotent_noop(s, events)
            noop_flags.append(noop)
            if noop:
                continue
            current = s.stream_versions.get(stream, 0)
            if expected != current:
                raise ConcurrencyConflict(stream, expected, current)

        # 2) 校验 pm_saves
        for pm_id, expected, _state in b.pm_saves:
            entry = s.pm_states.get(pm_id)
            current_v = entry[0] if entry is not None else 0
            if expected != current_v:
                raise ConcurrencyConflict(f"pm:{pm_id}", expected, current_v)

        # --- 全部校验通过,开始应用 --- #

        for (stream, _expected, events), noop in zip(
            b.append_intents, noop_flags, strict=True
        ):
            if noop:
                continue
            seq = s.stream_versions.get(stream, 0)
            for ne in events:
                env: EventEnvelope[Any] = EventEnvelope(
                    event_id=ne.event_id,
                    schema_version=ne.schema_version,
                    stream_id=stream,
                    sequence=seq,
                    global_position=s.global_counter,
                    correlation_id=ne.correlation_id,
                    causation_id=ne.causation_id,
                    recorded_at=ne.recorded_at,
                    payload=ne.payload,
                )
                s.events.append(env)
                s.event_digests[ne.event_id] = digest(ne.payload)
                s.global_counter += 1
                seq += 1
            s.stream_versions[stream] = seq

        for pair in b.inbox_adds:
            s.inbox.add(pair)

        for message in b.outbox_adds:
            s.outbox.append(OutboxRow(message=message, sent=False))

        for command_id in b.outbox_sent:
            for row in s.outbox:
                if row.message.command_id == command_id:
                    row.sent = True
                    break

        for pm_id, _expected, new_state in b.pm_saves:
            entry = s.pm_states.get(pm_id)
            current_v = entry[0] if entry is not None else 0
            s.pm_states[pm_id] = (current_v + 1, new_state)

        for pm_id, position in b.checkpoint_sets:
            s.pm_checkpoints[pm_id] = position

        for outcome in b.processed_puts:
            s.processed[(outcome.consumer_id, outcome.command_id)] = outcome

        self._committed = True

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # 未 commit 则缓冲随对象丢弃,已提交状态不变。
        self._buffers = Buffers()


class MemoryUnitOfWorkFactory:
    def __init__(self, db: MemoryDatabase) -> None:
        self._db = db

    def __call__(self) -> MemoryUnitOfWork:
        return MemoryUnitOfWork(self._db)


class MemoryCommandBus:
    """事务外命令通道。peek/ack 支持"处理成功后再移除",模拟重投窗口。"""

    def __init__(self) -> None:
        self._queue: list[CommandEnvelope[Any]] = []

    def publish(self, message: CommandEnvelope[Any]) -> None:
        self._queue.append(message)

    def peek(self) -> CommandEnvelope[Any] | None:
        return self._queue[0] if self._queue else None

    def ack(self, command_id: str) -> None:
        for i, msg in enumerate(self._queue):
            if msg.command_id == command_id:
                del self._queue[i]
                return

    def __len__(self) -> int:
        return len(self._queue)
