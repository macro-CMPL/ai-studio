"""内存 UnitOfWork:缓冲写,commit 时"先全量校验、再全量应用"。

校验覆盖(含事务内缓冲,不只已提交状态):
- 事件:批内 event_id 不得重复;event_id 全局唯一(同 id 异 stream/payload => IdempotencyConflict);
  全部已存在且一致 => 幂等空操作;新事件按 expected_version 做乐观并发。
- pm_save:expected_version 复核(乐观并发)。
- checkpoint:单调不回退。
- processed:同 (consumer,command_id) 异内容 => IdempotencyConflict。
- outbox:同 command_id 异指纹 => IdempotencyConflict;同指纹幂等去重。
任一校验失败则抛异常且不改动任何状态(原子);全部通过才一次性应用。
"""

from __future__ import annotations

from types import TracebackType
from typing import Any

from studio.kernel.envelopes import CommandEnvelope, EventEnvelope
from studio.kernel.errors import ConcurrencyConflict, IdempotencyConflict
from studio.kernel.fingerprints import command_fingerprint
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

    def commit(self) -> None:
        if self._committed:
            raise RuntimeError("UnitOfWork 已提交,禁止重复 commit")
        s = self._db.state
        b = self._buffers

        planned = self._validate(s, b)
        self._apply(s, b, planned)
        self._committed = True

    # -- 校验:返回需实际写入的 (stream, events) 与 outbox 列表 ----------------- #

    def _validate(
        self, s: DbState, b: Buffers
    ) -> tuple[list[tuple[str, list[Any]]], list[CommandEnvelope[Any]]]:
        tx_event_ids: dict[str, tuple[str, str]] = {}
        running_versions: dict[str, int] = {}
        planned_appends: list[tuple[str, list[Any]]] = []

        for stream, expected, events in b.append_intents:
            batch_ids = [ne.event_id for ne in events]
            if len(set(batch_ids)) != len(batch_ids):
                raise IdempotencyConflict(batch_ids[0], "批内 event_id 重复")

            existing = 0
            for ne in events:
                d = digest(ne.payload)
                prior = s.event_digests.get(ne.event_id) or tx_event_ids.get(
                    ne.event_id
                )
                if prior is not None:
                    if prior != (stream, d):
                        raise IdempotencyConflict(
                            ne.event_id, "event_id 复用于不同 stream/payload"
                        )
                    existing += 1
            if existing == len(events):
                continue  # 幂等空操作
            if existing != 0:
                raise IdempotencyConflict(events[0].event_id, "部分重叠")

            base = running_versions.get(stream, s.stream_versions.get(stream, 0))
            if expected != base:
                raise ConcurrencyConflict(f"stream:{stream}", expected, base)
            running_versions[stream] = base + len(events)
            for ne in events:
                tx_event_ids[ne.event_id] = (stream, digest(ne.payload))
            planned_appends.append((stream, events))

        for pm_id, expected, _state in b.pm_saves:
            entry = s.pm_states.get(pm_id)
            current_v = entry[0] if entry is not None else 0
            if expected != current_v:
                raise ConcurrencyConflict(f"pm:{pm_id}", expected, current_v)

        for pm_id, position in b.checkpoint_sets:
            current_cp = s.pm_checkpoints.get(pm_id, -1)
            if position < current_cp:
                raise ConcurrencyConflict(f"checkpoint:{pm_id}", position, current_cp)

        for outcome in b.processed_puts:
            key = (outcome.consumer_id, outcome.command_id)
            prior_o = s.processed.get(key)
            if prior_o is not None and prior_o != outcome:
                raise IdempotencyConflict(outcome.command_id, "processed 结果被覆盖")

        planned_outbox: list[CommandEnvelope[Any]] = []
        seen_outbox: dict[str, str] = {
            row.message.command_id: command_fingerprint(
                row.message.target, row.message.command_key, row.message.payload
            )
            for row in s.outbox
        }
        for msg in b.outbox_adds:
            fp = command_fingerprint(msg.target, msg.command_key, msg.payload)
            prior_fp = seen_outbox.get(msg.command_id)
            if prior_fp is not None:
                if prior_fp != fp:
                    raise IdempotencyConflict(msg.command_id, "outbox command_id 复用")
                continue  # 幂等去重
            seen_outbox[msg.command_id] = fp
            planned_outbox.append(msg)

        return planned_appends, planned_outbox

    # -- 应用:全部校验通过后一次性写入 -------------------------------------- #

    def _apply(
        self,
        s: DbState,
        b: Buffers,
        planned: tuple[list[tuple[str, list[Any]]], list[CommandEnvelope[Any]]],
    ) -> None:
        planned_appends, planned_outbox = planned

        for stream, events in planned_appends:
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
                s.event_digests[ne.event_id] = (stream, digest(ne.payload))
                s.global_counter += 1
                seq += 1
            s.stream_versions[stream] = seq

        for pair in b.inbox_adds:
            s.inbox.add(pair)

        for message in planned_outbox:
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

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # 未 commit(异常或校验失败)则缓冲随对象丢弃,已提交状态不变。
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
