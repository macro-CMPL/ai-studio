"""Milestone 2 验收:内核事务原子性、幂等/去重、崩溃恢复、重放纯度、公平性。"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

import pytest

from kernel_helpers import (
    CONSUMER_ID,
    FakeClock,
    accept_command,
    build_stack,
    stream_events,
    stream_payload_types,
)
from studio.application.command_worker import CommandWorker
from studio.application.event_pump import EventPump
from studio.application.outbox_relay import OutboxRelay
from studio.examples.demo_order.decider import DemoOrderDecider, DemoOrderState
from studio.examples.demo_order.payloads import (
    DemoCommand,
    DemoEvent,
    OrderAcceptedEvt,
)
from studio.examples.demo_order.process_manager import DemoOrderProcessManager
from studio.infrastructure.memory._state import MemoryDatabase
from studio.infrastructure.memory.unit_of_work import (
    MemoryCommandBus,
    MemoryUnitOfWork,
    MemoryUnitOfWorkFactory,
)
from studio.kernel import identifiers as kids
from studio.kernel.decisions import Accepted
from studio.kernel.envelopes import CommandEnvelope, EventEnvelope, MessagePayload
from studio.kernel.errors import ConcurrencyConflict, IdempotencyConflict
from studio.kernel.ports import NewEvent
from studio.serialization import digest

_TS = datetime(2026, 1, 1, tzinfo=UTC)
_PM_ID = "demo-order-pm"


def _new_event(event_id: str, order_ref: str) -> NewEvent:
    return NewEvent(
        event_id=event_id,
        schema_version=1,
        correlation_id="corr",
        causation_id="cause",
        recorded_at=_TS,
        payload=OrderAcceptedEvt(order_ref=order_ref),
    )


# --------------------------------------------------------------------------- #
# 完整工作流 / 公平性 / 无死循环
# --------------------------------------------------------------------------- #


def test_full_workflow_reaches_quiescence_and_sequence() -> None:
    s = build_stack()
    s.bus.publish(accept_command("order-1", "ref-1"))
    s.driver.run_until_quiescent()

    assert stream_payload_types(s.db, "order-1") == [
        "order_accepted",
        "stage_advanced",
        "order_delivered",
    ]
    assert len(s.bus) == 0
    # 无关/终态事件不造成死循环:pump 已消费到底。
    assert s.pump.tick() is False
    last_pos = max(e.global_position for e in s.db.state.events)
    assert s.db.state.pm_checkpoints[_PM_ID] == last_pos


def test_final_aggregate_state() -> None:
    s = build_stack()
    s.bus.publish(accept_command("order-1", "ref-1"))
    s.driver.run_until_quiescent()

    state = s.decider.initial_state()
    for env in stream_events(s.db, "order-1"):
        state = s.decider.evolve(state, env.payload)
    assert state == DemoOrderState(accepted=True, stages=("script",), delivered=True)


# --------------------------------------------------------------------------- #
# 重放纯度 / 场景复现确定性
# --------------------------------------------------------------------------- #


def test_history_replay_only_uses_evolve() -> None:
    s = build_stack()
    s.bus.publish(accept_command("order-1", "ref-1"))
    s.driver.run_until_quiescent()

    events = stream_events(s.db, "order-1")
    replay = s.decider.initial_state()
    for env in events:  # 只喂 evolve,不触碰任何 Port/Clock
        replay = s.decider.evolve(replay, env.payload)
    assert digest(replay) == digest(
        DemoOrderState(accepted=True, stages=("script",), delivered=True)
    )


def test_scenario_reproduction_is_deterministic() -> None:
    def run() -> list[EventEnvelope[DemoEvent]]:
        s = build_stack()
        s.bus.publish(accept_command("order-1", "ref-1"))
        s.driver.run_until_quiescent()
        return stream_events(s.db, "order-1")

    a = run()
    b = run()
    assert [e.event_id for e in a] == [e.event_id for e in b]
    assert [e.payload for e in a] == [e.payload for e in b]


# --------------------------------------------------------------------------- #
# 命令去重 / 拒绝重投 / 空 Decision
# --------------------------------------------------------------------------- #


def test_command_dedup_same_id_one_effect() -> None:
    s = build_stack()
    s.bus.publish(accept_command("order-1", "ref", command_id="c1"))
    s.bus.publish(accept_command("order-1", "ref", command_id="c1"))
    assert s.worker.tick() is True
    assert s.worker.tick() is True

    accepted = [e for e in s.db.state.events if e.payload.type == "order_accepted"]
    assert len(accepted) == 1
    assert len(s.bus) == 0


def test_rejection_redelivery_returns_same_outcome() -> None:
    s = build_stack()
    s.bus.publish(accept_command("order-1", "ref", command_id="c1"))
    assert s.worker.tick() is True  # accepted

    s.bus.publish(accept_command("order-1", "ref2", command_id="c2"))
    assert s.worker.tick() is True  # rejected: already accepted

    outcome = s.db.state.processed[(CONSUMER_ID, "c2")]
    assert outcome.outcome_type == "rejected"
    assert outcome.rejection_code == "already_accepted"

    before = len(s.db.state.events)
    s.bus.publish(accept_command("order-1", "ref2", command_id="c2"))
    assert s.worker.tick() is True
    assert len(s.db.state.events) == before  # 不重新 decide,不新增事件


class _PingCmd(MessagePayload):
    type: Literal["ping"] = "ping"


class _NoopDecider:
    def initial_state(self) -> int:
        return 0

    def decide(self, state: int, command: _PingCmd) -> Accepted[MessagePayload]:
        return Accepted(())

    def evolve(self, state: int, event: MessagePayload) -> int:
        return state


def test_empty_decision_still_marks_processed() -> None:
    db = MemoryDatabase()
    bus = MemoryCommandBus()
    factory = MemoryUnitOfWorkFactory(db)
    worker: CommandWorker[int, _PingCmd, MessagePayload] = CommandWorker(
        decider=_NoopDecider(),
        bus=bus,
        uow_factory=factory,
        clock=FakeClock(),
        consumer_id="noop",
    )
    bus.publish(
        CommandEnvelope(
            command_id="p1",
            schema_version=1,
            target="t",
            command_key="ping",
            correlation_id="c",
            causation_id=None,
            issued_at=_TS,
            payload=_PingCmd(),
        )
    )
    assert worker.tick() is True
    assert db.state.events == []
    outcome = db.state.processed[("noop", "p1")]
    assert outcome.outcome_type == "accepted"
    assert outcome.event_ids == ()


# --------------------------------------------------------------------------- #
# 乐观并发 / 幂等冲突
# --------------------------------------------------------------------------- #


def test_optimistic_concurrency_one_wins() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)

    with factory() as u1:
        u1.event_store.append("stream", 0, [_new_event("EA", "A")])
        u1.commit()

    with factory() as u2:
        u2.event_store.append("stream", 0, [_new_event("EB", "B")])  # expected 0, now 1
        with pytest.raises(ConcurrencyConflict):
            u2.commit()

    assert db.state.stream_versions["stream"] == 1
    assert [e.event_id for e in db.state.events] == ["EA"]


def test_idempotency_conflict_on_same_id_different_payload() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)

    with factory() as u:
        u.event_store.append("stream", 0, [_new_event("E1", "a")])
        u.commit()

    with factory() as u:
        u.event_store.append("stream", 1, [_new_event("E1", "b")])  # 同 id 异内容
        with pytest.raises(IdempotencyConflict):
            u.commit()


def test_atomic_rollback_on_uncommitted_uow() -> None:
    db = MemoryDatabase()
    factory = MemoryUnitOfWorkFactory(db)
    with factory() as u:
        u.event_store.append("stream", 0, [_new_event("E1", "a")])
        u.inbox.mark_processed("pm", "E1")
        # 故意不 commit
    assert db.state.events == []
    assert db.state.inbox == set()


# --------------------------------------------------------------------------- #
# 崩溃恢复
# --------------------------------------------------------------------------- #


def test_event_committed_pm_not_consumed_recovers_via_checkpoint() -> None:
    s = build_stack()
    s.bus.publish(accept_command("order-1", "ref"))
    assert s.worker.tick() is True  # OrderAccepted committed;pump 尚未运行

    # 模拟重启:全新 pump,靠 checkpoint 恢复
    pump2: EventPump[object, DemoEvent, DemoCommand] = EventPump(
        process_manager=DemoOrderProcessManager(),
        uow_factory=s.uow_factory,
        clock=FakeClock(),
    )
    assert pump2.tick() is True
    unsent = [r for r in s.db.state.outbox if not r.sent]
    assert len(unsent) == 1
    assert unsent[0].message.payload.type == "advance_stage"


def test_pm_wrote_outbox_relay_not_sent_command_not_lost() -> None:
    s = build_stack()
    s.bus.publish(accept_command("order-1", "ref"))
    assert s.worker.tick() is True
    assert s.pump.tick() is True  # advance:script 写入 outbox(已提交)

    relay2 = OutboxRelay(uow_factory=s.uow_factory, bus=s.bus)
    assert relay2.tick() is True
    assert len(s.bus) == 1
    peeked = s.bus.peek()
    assert peeked is not None
    assert peeked.payload.type == "advance_stage"


class _FaultyMarkSentFactory:
    """publish 之后、标记 sent 之前崩溃(mark_sent 的 commit 抛错一次)。"""

    def __init__(self, db: MemoryDatabase) -> None:
        self._db = db
        self.armed = True

    def __call__(self) -> MemoryUnitOfWork:
        parent = self

        class _U(MemoryUnitOfWork):
            def commit(self_inner) -> None:  # noqa: N805
                if parent.armed and self_inner._buffers.outbox_sent:
                    parent.armed = False
                    raise RuntimeError("crash before mark_sent")
                super().commit()

        return _U(self._db)


def test_effectively_once_when_relay_republishes() -> None:
    db = MemoryDatabase()
    bus = MemoryCommandBus()
    clock = FakeClock()
    normal = MemoryUnitOfWorkFactory(db)
    worker: CommandWorker[DemoOrderState, DemoCommand, DemoEvent] = CommandWorker(
        decider=DemoOrderDecider(),
        bus=bus,
        uow_factory=normal,
        clock=clock,
        consumer_id=CONSUMER_ID,
    )
    pump: EventPump[object, DemoEvent, DemoCommand] = EventPump(
        process_manager=DemoOrderProcessManager(), uow_factory=normal, clock=clock
    )
    bus.publish(accept_command("order-1", "ref"))
    assert worker.tick() is True  # OrderAccepted
    assert pump.tick() is True  # advance:script -> outbox

    faulty = _FaultyMarkSentFactory(db)
    relay_faulty = OutboxRelay(uow_factory=faulty, bus=bus)
    with pytest.raises(RuntimeError):
        relay_faulty.tick()  # publish 成功,mark_sent 崩溃
    assert len(bus) == 1
    assert any(not r.sent for r in db.state.outbox)

    relay_ok = OutboxRelay(uow_factory=normal, bus=bus)
    assert relay_ok.tick() is True  # 重投
    assert len(bus) == 2  # 同一命令出现两次

    # Worker 靠 command_id 去重 -> 只有一个 stage_advanced
    assert worker.tick() is True
    assert worker.tick() is True
    advanced = [e for e in db.state.events if e.payload.type == "stage_advanced"]
    assert len(advanced) == 1


# --------------------------------------------------------------------------- #
# correlation / causation 链 / 序列化
# --------------------------------------------------------------------------- #


def test_correlation_and_causation_chain() -> None:
    s = build_stack()
    s.bus.publish(
        accept_command("order-1", "ref", command_id="cmd-root", correlation_id="corr-1")
    )
    s.driver.run_until_quiescent()

    events = stream_events(s.db, "order-1")
    assert all(e.correlation_id == "corr-1" for e in events)

    order_accepted = events[0]
    assert order_accepted.causation_id == "cmd-root"

    advance_cmd_id = kids.command_id(
        _PM_ID, order_accepted.event_id, "on-accepted", "advance:script"
    )
    stage_advanced = events[1]
    assert stage_advanced.causation_id == advance_cmd_id


def test_envelope_serialization_round_trip() -> None:
    cmd: CommandEnvelope[DemoCommand] = accept_command("order-1", "ref")
    raw = cmd.model_dump(mode="json")
    back = CommandEnvelope[DemoCommand].model_validate(raw)
    assert back == cmd

    s = build_stack()
    s.bus.publish(accept_command("order-1", "ref"))
    s.driver.run_until_quiescent()
    event = stream_events(s.db, "order-1")[0]
    raw_e = event.model_dump(mode="json")
    back_e = EventEnvelope[DemoEvent].model_validate(raw_e)
    assert back_e == event
