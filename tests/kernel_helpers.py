"""M2 测试辅助:确定性 Clock/IdFactory 与 DemoOrder 全栈装配。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from studio.application.command_worker import CommandWorker
from studio.application.driver import Driver
from studio.application.event_pump import EventPump
from studio.application.outbox_relay import OutboxRelay
from studio.examples.demo_order.decider import DemoOrderDecider, DemoOrderState
from studio.examples.demo_order.payloads import AcceptOrderCmd, DemoCommand, DemoEvent
from studio.examples.demo_order.process_manager import (
    DemoOrderPMState,
    DemoOrderProcessManager,
)
from studio.infrastructure.memory._state import MemoryDatabase
from studio.infrastructure.memory.unit_of_work import (
    MemoryCommandBus,
    MemoryUnitOfWorkFactory,
)
from studio.kernel.envelopes import CommandEnvelope, EventEnvelope

CONSUMER_ID = "order-worker"
_ISSUED_AT = datetime(2026, 1, 1, tzinfo=UTC)


class FakeClock:
    """确定性单调时钟:每次 now() 递增 1 秒。"""

    def __init__(self, start: datetime | None = None) -> None:
        self._t = start or datetime(2026, 1, 1, tzinfo=UTC)

    def now(self) -> datetime:
        t = self._t
        self._t += timedelta(seconds=1)
        return t


class FakeIdFactory:
    def __init__(self) -> None:
        self._n = 0

    def new_id(self) -> str:
        self._n += 1
        return f"id-{self._n}"


@dataclass
class DemoStack:
    db: MemoryDatabase
    bus: MemoryCommandBus
    clock: FakeClock
    decider: DemoOrderDecider
    pm: DemoOrderProcessManager
    uow_factory: MemoryUnitOfWorkFactory
    worker: CommandWorker[DemoOrderState, DemoCommand, DemoEvent]
    pump: EventPump[DemoOrderPMState, DemoEvent, DemoCommand]
    relay: OutboxRelay
    driver: Driver


def build_stack() -> DemoStack:
    db = MemoryDatabase()
    bus = MemoryCommandBus()
    clock = FakeClock()
    factory = MemoryUnitOfWorkFactory(db)
    decider = DemoOrderDecider()
    pm = DemoOrderProcessManager()
    worker: CommandWorker[DemoOrderState, DemoCommand, DemoEvent] = CommandWorker(
        decider=decider,
        bus=bus,
        uow_factory=factory,
        clock=clock,
        consumer_id=CONSUMER_ID,
    )
    pump: EventPump[DemoOrderPMState, DemoEvent, DemoCommand] = EventPump(
        process_manager=pm, uow_factory=factory, clock=clock
    )
    relay = OutboxRelay(uow_factory=factory, bus=bus)
    driver = Driver(worker=worker, pumps=[pump], relay=relay)
    return DemoStack(
        db=db,
        bus=bus,
        clock=clock,
        decider=decider,
        pm=pm,
        uow_factory=factory,
        worker=worker,
        pump=pump,
        relay=relay,
        driver=driver,
    )


def accept_command(
    order_id: str,
    order_ref: str,
    *,
    command_id: str = "cmd-root",
    correlation_id: str | None = None,
) -> CommandEnvelope[DemoCommand]:
    return CommandEnvelope(
        command_id=command_id,
        schema_version=1,
        target=order_id,
        command_key="accept",
        correlation_id=correlation_id or order_id,
        causation_id=None,
        issued_at=_ISSUED_AT,
        payload=AcceptOrderCmd(order_ref=order_ref),
    )


def stream_payload_types(db: MemoryDatabase, stream_id: str) -> list[str]:
    events = [e for e in db.state.events if e.stream_id == stream_id]
    events.sort(key=lambda e: e.global_position)
    return [e.payload.type for e in events]


def stream_events(db: MemoryDatabase, stream_id: str) -> list[EventEnvelope[DemoEvent]]:
    events = [e for e in db.state.events if e.stream_id == stream_id]
    events.sort(key=lambda e: e.sequence)
    return events
