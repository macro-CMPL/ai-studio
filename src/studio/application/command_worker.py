"""Command Worker:消费一条命令 -> decide -> 追加事件 + 记录处理结果(同事务)。

- 已处理命令(去重命中)返回同一 Outcome,不重新 decide。
- Rejected 也持久化,重投返回同一 Rejection。
- 空 Decision(ACCEPTED 无事件)仍标记 processed。
- 乐观并发冲突:不标记 processed、不 ack,留待下一 tick 重载状态重试。
每次 tick 只处理一条消息(公平)。
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from studio.kernel import identifiers as kids
from studio.kernel.decisions import Accepted, Decider, Rejected
from studio.kernel.envelopes import MessagePayload
from studio.kernel.errors import ConcurrencyConflict
from studio.kernel.outcomes import CommandOutcome, OutcomeType
from studio.kernel.ports import Clock, CommandBus, NewEvent, UnitOfWorkFactory

TState = TypeVar("TState")
TCmd = TypeVar("TCmd", bound=MessagePayload)
TEvt = TypeVar("TEvt", bound=MessagePayload)


class CommandWorker(Generic[TState, TCmd, TEvt]):
    def __init__(
        self,
        *,
        decider: Decider[TState, TCmd, TEvt],
        bus: CommandBus,
        uow_factory: UnitOfWorkFactory,
        clock: Clock,
        consumer_id: str,
        schema_version: int = 1,
    ) -> None:
        self._decider = decider
        self._bus = bus
        self._uow = uow_factory
        self._clock = clock
        self._consumer_id = consumer_id
        self._schema_version = schema_version

    def _load_state(self, uow: Any, stream_id: str) -> TState:
        state = self._decider.initial_state()
        for env in uow.event_store.read_stream(stream_id):
            state = self._decider.evolve(state, env.payload)
        return state

    def tick(self) -> bool:
        message = self._bus.peek()
        if message is None:
            return False

        with self._uow() as uow:
            existing = uow.processed_commands.get(
                self._consumer_id, message.command_id
            )
            if existing is not None:
                # 去重命中:已有结果,直接 ack,不重新 decide。
                self._bus.ack(message.command_id)
                return True

            state = self._load_state(uow, message.target)
            version = uow.event_store.current_version(message.target)
            decision = self._decider.decide(state, message.payload)

            if isinstance(decision, Rejected):
                uow.processed_commands.put(
                    CommandOutcome(
                        consumer_id=self._consumer_id,
                        command_id=message.command_id,
                        outcome_type=OutcomeType.REJECTED,
                        event_ids=(),
                        rejection_code=decision.code,
                        rejection_message=decision.message,
                    )
                )
                uow.commit()
                self._bus.ack(message.command_id)
                return True

            assert isinstance(decision, Accepted)
            recorded_at = self._clock.now()
            new_events = [
                NewEvent(
                    event_id=kids.event_id(message.command_id, pe.event_key),
                    schema_version=self._schema_version,
                    correlation_id=message.correlation_id,
                    causation_id=message.command_id,
                    recorded_at=recorded_at,
                    payload=pe.payload,
                )
                for pe in decision.events
            ]
            try:
                stored = uow.event_store.append(message.target, version, new_events)
            except ConcurrencyConflict:
                # 不 commit、不 ack:下一 tick 重载状态重试。
                return True

            uow.processed_commands.put(
                CommandOutcome(
                    consumer_id=self._consumer_id,
                    command_id=message.command_id,
                    outcome_type=OutcomeType.ACCEPTED,
                    event_ids=tuple(e.event_id for e in stored),
                    rejection_code=None,
                    rejection_message=None,
                )
            )
            uow.commit()

        self._bus.ack(message.command_id)
        return True
