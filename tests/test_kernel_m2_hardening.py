"""M2 hardening 验收:针对 4 个 blocker 的对抗测试 + 契约缺口。"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kernel_helpers import CONSUMER_ID, accept_command, build_stack
from studio.examples.demo_order.payloads import AcceptOrderCmd, OrderAcceptedEvt
from studio.infrastructure.memory._state import MemoryDatabase
from studio.infrastructure.memory.unit_of_work import (
    MemoryUnitOfWork,
    MemoryUnitOfWorkFactory,
)
from studio.kernel.envelopes import CommandEnvelope
from studio.kernel.errors import ConcurrencyConflict, IdempotencyConflict
from studio.kernel.outcomes import CommandOutcome, OutcomeType
from studio.kernel.ports import NewEvent

_TS = datetime(2026, 1, 1, tzinfo=UTC)


def _new_event(event_id: str, order_ref: str) -> NewEvent:
    return NewEvent(
        event_id=event_id,
        schema_version=1,
        correlation_id="c",
        causation_id="x",
        recorded_at=_TS,
        payload=OrderAcceptedEvt(order_ref=order_ref),
    )


def _command(
    command_id: str, *, target: str = "t", key: str = "k", order_ref: str = "a"
) -> CommandEnvelope[AcceptOrderCmd]:
    return CommandEnvelope(
        command_id=command_id,
        schema_version=1,
        target=target,
        command_key=key,
        correlation_id="c",
        causation_id=None,
        issued_at=_TS,
        payload=AcceptOrderCmd(order_ref=order_ref),
    )


def _outcome(command_id: str, otype: OutcomeType, fp: str) -> CommandOutcome:
    return CommandOutcome(
        consumer_id="w",
        command_id=command_id,
        command_fingerprint=fp,
        outcome_type=otype,
        event_ids=(),
        rejection_code=None,
        rejection_message=None,
        processed_at=_TS,
    )


# --------------------------------------------------------------------------- #
# Blocker 1:commit 时的并发冲突不得逃出 Driver;不 ack、不留结果,可重试
# --------------------------------------------------------------------------- #


class _ConflictInjectingFactory:
    """在 worker 首次带 append 的 commit 前,插入一个竞争提交推进版本。"""

    def __init__(self, db: MemoryDatabase) -> None:
        self._db = db
        self.armed = True

    def __call__(self) -> MemoryUnitOfWork:
        parent = self

        class _U(MemoryUnitOfWork):
            def commit(self_inner) -> None:  # noqa: N805
                if parent.armed and self_inner._buffers.append_intents:
                    parent.armed = False
                    with MemoryUnitOfWork(parent._db) as other:
                        other.event_store.append(
                            "order-1", 0, [_new_event("competitor", "other")]
                        )
                        other.commit()
                super().commit()

        return _U(self._db)


def test_commit_conflict_does_not_escape_and_retries() -> None:
    s = build_stack()
    s.worker._uow = _ConflictInjectingFactory(s.db)  # type: ignore[assignment]
    s.bus.publish(accept_command("order-1", "ref", command_id="cmd-root"))

    # 首次 tick:commit 时遭遇并发冲突,但不得抛出、不得 ack、不得留结果。
    assert s.worker.tick() is True
    assert len(s.bus) == 1  # 未 ack,留待重试
    assert (CONSUMER_ID, "cmd-root") not in s.db.state.processed
    order_events = [e for e in s.db.state.events if e.stream_id == "order-1"]
    assert len(order_events) == 1  # 只有竞争者事件

    # 第二次 tick:基于新状态重决策(已被竞争者接受 -> 拒绝),提交并 ack。
    assert s.worker.tick() is True
    outcome = s.db.state.processed[(CONSUMER_ID, "cmd-root")]
    assert outcome.outcome_type == OutcomeType.REJECTED
    assert len(s.bus) == 0


# --------------------------------------------------------------------------- #
# Blocker 2:一致性快照(load_stream 原子返回 version+events)
# --------------------------------------------------------------------------- #


def test_load_stream_is_atomic_snapshot() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    with factory() as u:
        u.event_store.append("s", 0, [_new_event("E1", "a"), _new_event("E2", "b")])
        u.commit()
    with factory() as u:
        snap = u.event_store.load_stream("s")
    assert snap.version == 2
    assert [e.event_id for e in snap.value] == ["E1", "E2"]


# --------------------------------------------------------------------------- #
# Blocker 3:唯一约束 / 幂等冲突完整
# --------------------------------------------------------------------------- #


def test_same_batch_duplicate_event_ids_rejected() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    with factory() as u:
        u.event_store.append("s", 0, [_new_event("dup", "a"), _new_event("dup", "b")])
        with pytest.raises(IdempotencyConflict):
            u.commit()
    assert db.state.events == []


def test_cross_stream_same_event_id_rejected() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    with factory() as u:
        u.event_store.append("s1", 0, [_new_event("E", "a")])
        u.commit()
    with factory() as u:
        u.event_store.append("s2", 0, [_new_event("E", "a")])  # 同 id 异 stream
        with pytest.raises(IdempotencyConflict):
            u.commit()


def test_processed_command_overwrite_rejected() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    with factory() as u:
        u.processed_commands.put(_outcome("c", OutcomeType.ACCEPTED, "fp1"))
        u.commit()
    with factory() as u:
        u.processed_commands.put(_outcome("c", OutcomeType.REJECTED, "fp2"))
        with pytest.raises(IdempotencyConflict):
            u.commit()
    assert db.state.processed[("w", "c")].outcome_type == OutcomeType.ACCEPTED


def test_outbox_same_command_id_different_payload_rejected() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    with factory() as u:
        u.outbox.enqueue(_command("X", order_ref="a"))
        u.outbox.enqueue(_command("X", order_ref="b"))  # 同 id 异指纹
        with pytest.raises(IdempotencyConflict):
            u.commit()


def test_outbox_same_command_id_same_payload_deduped() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    with factory() as u:
        u.outbox.enqueue(_command("X", order_ref="a"))
        u.outbox.enqueue(_command("X", order_ref="a"))  # 同 id 同指纹 -> 去重
        u.commit()
    assert len(db.state.outbox) == 1


def test_worker_same_command_id_different_content_conflicts() -> None:
    s = build_stack()
    s.bus.publish(accept_command("order-1", "refA", command_id="c1"))
    assert s.worker.tick() is True
    s.bus.publish(accept_command("order-1", "refB", command_id="c1"))  # 同 id 异内容
    with pytest.raises(IdempotencyConflict):
        s.worker.tick()


# --------------------------------------------------------------------------- #
# Blocker 4:checkpoint 单调不回退
# --------------------------------------------------------------------------- #


def test_checkpoint_cannot_regress() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    with factory() as u:
        u.process_managers.set_checkpoint("pm", 10)
        u.commit()
    with factory() as u:
        u.process_managers.set_checkpoint("pm", 3)  # 回退
        with pytest.raises(ConcurrencyConflict):
            u.commit()
    assert db.state.pm_checkpoints["pm"] == 10


# --------------------------------------------------------------------------- #
# 契约缺口:双重 commit / 校验失败全量回滚
# --------------------------------------------------------------------------- #


def test_double_commit_is_rejected() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    with factory() as u:
        u.event_store.append("s", 0, [_new_event("E1", "a")])
        u.commit()
        with pytest.raises(RuntimeError):
            u.commit()


def test_validation_failure_rolls_back_everything() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    with factory() as u:
        u.event_store.append("s", 0, [_new_event("E1", "a")])  # 有效
        u.event_store.append("s", 5, [_new_event("E2", "b")])  # 错误 expected
        u.inbox.mark_processed("pm", "E1")
        with pytest.raises(ConcurrencyConflict):
            u.commit()
    assert db.state.events == []
    assert db.state.inbox == set()
