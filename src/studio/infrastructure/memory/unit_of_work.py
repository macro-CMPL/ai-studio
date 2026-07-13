"""内存 UnitOfWork:缓冲写,commit 在锁内"候选副本上校验并应用,再原子换入"。

commit 语义:
1. 取 MemoryDatabase.lock(临界区),使"读取当前态→校验→应用→换入"整体原子。
2. candidate = deepcopy(当前已提交态);所有写入按顺序在 candidate 上"校验后应用",
   因此事务内的投影状态(pm 版本 / checkpoint / processed / outbox / events)都会被后续校验看到。
3. 全部成功后一次性 `db.state = candidate`;任一步抛异常则 candidate 丢弃,已提交态不变(原子)。

这同时解决:
- 并发同 expected_version 仅一成(锁 + 提交时对最新态复核);
- 应用阶段异常不产生部分提交(仅换入完整 candidate);
- 事务内约束(checkpoint 单调 / processed 覆盖 / pm 版本)基于 candidate 投影;
- ProcessedCommand 按业务字段比较(忽略 processed_at),并发重投同结果为幂等而非冲突。
"""

from __future__ import annotations

import copy
from types import TracebackType
from typing import Any

from studio.kernel.envelopes import CommandEnvelope, EventEnvelope
from studio.kernel.errors import ConcurrencyConflict, IdempotencyConflict
from studio.kernel.fingerprints import command_fingerprint
from studio.kernel.outcomes import CommandOutcome
from studio.serialization import digest

from ._state import Buffers, DbState, MemoryDatabase, OutboxRow
from .event_store import MemoryEventStore
from .inbox import MemoryInbox
from .outbox import MemoryOutbox
from .process_manager_store import MemoryProcessManagerStore
from .processed_commands import MemoryProcessedCommandStore


def _same_business(a: CommandOutcome, b: CommandOutcome) -> bool:
    """ProcessedCommand 业务等价判断:忽略 processed_at(记录时间)。"""
    return (
        a.command_fingerprint == b.command_fingerprint
        and a.outcome_type == b.outcome_type
        and a.event_ids == b.event_ids
        and a.rejection_code == b.rejection_code
        and a.rejection_message == b.rejection_message
    )


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
        with self._db.lock:
            candidate = copy.deepcopy(self._db.state)
            self._apply_on(candidate, self._buffers)
            self._db.state = candidate  # 原子换入
        self._committed = True

    # -- 在 candidate 上逐项"校验后应用";异常则 candidate 被丢弃 ------------ #

    def _apply_on(self, c: DbState, b: Buffers) -> None:
        self._apply_events(c, b)

        for pair in b.inbox_adds:
            c.inbox.add(pair)

        self._apply_outbox_adds(c, b)

        for command_id in b.outbox_sent:
            for row in c.outbox:
                if row.message.command_id == command_id:
                    row.sent = True
                    break

        for pm_id, expected, new_state in b.pm_saves:
            entry = c.pm_states.get(pm_id)
            current_v = entry[0] if entry is not None else 0
            if expected != current_v:
                raise ConcurrencyConflict(f"pm:{pm_id}", expected, current_v)
            c.pm_states[pm_id] = (current_v + 1, new_state)

        for pm_id, position in b.checkpoint_sets:
            current_cp = c.pm_checkpoints.get(pm_id, -1)
            if position < current_cp:  # 单调(含事务内投影)
                raise ConcurrencyConflict(f"checkpoint:{pm_id}", position, current_cp)
            c.pm_checkpoints[pm_id] = position

        for outcome in b.processed_puts:
            key = (outcome.consumer_id, outcome.command_id)
            prior = c.processed.get(key)
            if prior is not None:
                if not _same_business(prior, outcome):
                    raise IdempotencyConflict(outcome.command_id, "processed 结果冲突")
                continue  # 业务等价 -> 幂等,保留首个
            c.processed[key] = outcome

    def _apply_events(self, c: DbState, b: Buffers) -> None:
        for stream, expected, events in b.append_intents:
            batch_ids = [ne.event_id for ne in events]
            if len(set(batch_ids)) != len(batch_ids):
                raise IdempotencyConflict(batch_ids[0], "批内 event_id 重复")

            existing = 0
            for ne in events:
                d = digest(ne.payload)
                prior = c.event_digests.get(ne.event_id)
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

            base = c.stream_versions.get(stream, 0)
            if expected != base:
                raise ConcurrencyConflict(f"stream:{stream}", expected, base)

            # 先构造全部 Envelope(校验 schema 等),再落地,避免部分写入。
            built: list[EventEnvelope[Any]] = []
            seq = base
            for ne in events:
                built.append(
                    EventEnvelope(
                        event_id=ne.event_id,
                        schema_version=ne.schema_version,
                        stream_id=stream,
                        sequence=seq,
                        global_position=c.global_counter + (seq - base),
                        correlation_id=ne.correlation_id,
                        causation_id=ne.causation_id,
                        recorded_at=ne.recorded_at,
                        payload=ne.payload,
                    )
                )
                seq += 1
            for env, ne in zip(built, events, strict=True):
                c.events.append(env)
                c.event_digests[ne.event_id] = (stream, digest(ne.payload))
                c.global_counter += 1
            c.stream_versions[stream] = seq

    def _apply_outbox_adds(self, c: DbState, b: Buffers) -> None:
        fingerprints = {
            row.message.command_id: command_fingerprint(
                row.message.target, row.message.command_key, row.message.payload
            )
            for row in c.outbox
        }
        for msg in b.outbox_adds:
            fp = command_fingerprint(msg.target, msg.command_key, msg.payload)
            prior_fp = fingerprints.get(msg.command_id)
            if prior_fp is not None:
                if prior_fp != fp:
                    raise IdempotencyConflict(msg.command_id, "outbox command_id 复用")
                continue  # 幂等去重
            fingerprints[msg.command_id] = fp
            c.outbox.append(OutboxRow(message=msg, sent=False))

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
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
