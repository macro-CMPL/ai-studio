"""M2 事务第二层边界:锁内原子 commit、应用异常无部分提交、事务内投影、语义去重。"""

from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from studio.examples.demo_order.payloads import OrderAcceptedEvt
from studio.infrastructure.memory._state import MemoryDatabase
from studio.infrastructure.memory.unit_of_work import MemoryUnitOfWorkFactory
from studio.kernel.errors import ConcurrencyConflict, IdempotencyConflict
from studio.kernel.outcomes import CommandOutcome, OutcomeType
from studio.kernel.ports import NewEvent

_TS = datetime(2026, 1, 1, tzinfo=UTC)


def _new_event(event_id: str, order_ref: str, schema_version: int = 1) -> NewEvent:
    return NewEvent(
        event_id=event_id,
        schema_version=schema_version,
        correlation_id="c",
        causation_id="x",
        recorded_at=_TS,
        payload=OrderAcceptedEvt(order_ref=order_ref),
    )


def _outcome(fp: str, otype: OutcomeType, at: datetime) -> CommandOutcome:
    return CommandOutcome(
        consumer_id="w",
        command_id="c",
        command_fingerprint=fp,
        outcome_type=otype,
        event_ids=("e1",) if otype is OutcomeType.ACCEPTED else (),
        rejection_code=None if otype is OutcomeType.ACCEPTED else "code",
        rejection_message=None if otype is OutcomeType.ACCEPTED else "msg",
        processed_at=at,
    )


# Blocker 1:锁内原子 commit —— 并发同 expected_version 仅一成 ----------------- #


def test_concurrent_same_expected_version_only_one_wins() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    results: dict[str, str] = {}
    barrier = threading.Barrier(2)

    def worker(name: str, event_id: str) -> None:
        with factory() as u:
            u.event_store.append("s", 0, [_new_event(event_id, name)])
            barrier.wait()  # 两个事务都完成缓冲后再竞争 commit
            try:
                u.commit()
                results[name] = "ok"
            except ConcurrencyConflict:
                results[name] = "conflict"

    t1 = threading.Thread(target=worker, args=("A", "EA"))
    t2 = threading.Thread(target=worker, args=("B", "EB"))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert sorted(results.values()) == ["conflict", "ok"]
    assert db.state.stream_versions["s"] == 1
    assert len(db.state.events) == 1


# Blocker 2:应用阶段异常不产生部分提交 -------------------------------------- #


def test_apply_phase_exception_leaves_no_partial_commit() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    with factory() as u:
        u.event_store.append(
            "s",
            0,
            [_new_event("valid", "a"), _new_event("bad", "b", schema_version=0)],
        )
        with pytest.raises(ValidationError):
            u.commit()
    assert db.state.events == []
    assert "s" not in db.state.stream_versions
    assert db.state.global_counter == 0


# Blocker 3:事务内投影(checkpoint / processed) ----------------------------- #


def test_within_tx_checkpoint_regression_rejected() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    with factory() as u:
        u.process_managers.set_checkpoint("pm", 10)
        u.process_managers.set_checkpoint("pm", 3)  # 同一事务内回退
        with pytest.raises(ConcurrencyConflict):
            u.commit()
    assert "pm" not in db.state.pm_checkpoints


def test_within_tx_processed_conflict_rejected() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    with factory() as u:
        u.processed_commands.put(_outcome("fp1", OutcomeType.ACCEPTED, _TS))
        u.processed_commands.put(_outcome("fp2", OutcomeType.REJECTED, _TS))
        with pytest.raises(IdempotencyConflict):
            u.commit()
    assert ("w", "c") not in db.state.processed


# Blocker 4:并发重投同结果、仅 processed_at 不同 -> 幂等而非冲突 -------------- #


def test_same_outcome_different_processed_at_is_idempotent() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    o1 = _outcome("fp", OutcomeType.ACCEPTED, _TS)
    o2 = _outcome("fp", OutcomeType.ACCEPTED, _TS + timedelta(seconds=5))

    with factory() as u:
        u.processed_commands.put(o1)
        u.commit()
    with factory() as u:
        u.processed_commands.put(o2)
        u.commit()  # 不得抛:业务等价

    assert db.state.processed[("w", "c")].processed_at == _TS  # 首个保留
