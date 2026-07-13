"""Command Worker:消费一条命令 -> decide -> 追加事件 + 记录处理结果(同事务)。

- 已处理命令(去重命中)返回同一 Outcome,不重新 decide;同 id 异内容抛 IdempotencyConflict。
- Rejected 也持久化,重投返回同一 Rejection。
- 空 Decision(ACCEPTED 无事件)仍标记 processed。
- 并发冲突(在 commit 时抛出)被纳入处理边界:不 ack、不留结果,下一 tick 重载状态重试。
- 状态与版本用一次原子 load_stream 读取(避免 stale-state/fresh-version 撕裂)。
每次 tick 只处理一条消息(公平)。
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from studio.kernel import identifiers as kids
from studio.kernel.decisions import Accepted, Decider, Rejected
from studio.kernel.envelopes import MessagePayload
from studio.kernel.errors import ConcurrencyConflict, ContractViolation, IdempotencyConflict
from studio.kernel.fingerprints import command_fingerprint
from studio.kernel.outcomes import CommandOutcome, OutcomeType
from studio.kernel.ports import Clock, CommandBus, NewEvent, UnitOfWork, UnitOfWorkFactory

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

    def _fold(self, events: list[Any]) -> TState:
        state = self._decider.initial_state()
        for env in events:
            state = self._decider.evolve(state, env.payload)
        return state

    def tick(self) -> bool:
        message = self._bus.peek()
        if message is None:
            return False

        fingerprint = command_fingerprint(
            message.target, message.command_key, message.payload
        )

        with self._uow() as uow:
            existing = uow.processed_commands.get(
                self._consumer_id, message.command_id
            )
            if existing is not None:
                if existing.command_fingerprint != fingerprint:
                    raise IdempotencyConflict(
                        message.command_id, "同 command_id 复用于不同内容"
                    )
                self._bus.ack(message.command_id)
                return True

            snapshot = uow.event_store.load_stream(message.target)
            state = self._fold(snapshot.value)
            version = snapshot.version
            decision = self._decider.decide(state, message.payload)

            if isinstance(decision, Rejected):
                self._record(uow, message, fingerprint, OutcomeType.REJECTED, (), decision)
            else:
                assert isinstance(decision, Accepted)
                keys = [pe.event_key for pe in decision.events]
                if len(set(keys)) != len(keys):
                    raise ContractViolation("同一 Decision 内 event_key 重复")
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
                uow.event_store.append(message.target, version, new_events)
                event_ids = tuple(ne.event_id for ne in new_events)
                self._record(
                    uow, message, fingerprint, OutcomeType.ACCEPTED, event_ids, None
                )

            try:
                uow.commit()
            except ConcurrencyConflict:
                # 冲突在 commit 抛出:不 ack、不留结果,下一 tick 重载状态重试。
                return True

        self._bus.ack(message.command_id)
        return True

    def _record(
        self,
        uow: UnitOfWork,
        message: Any,
        fingerprint: str,
        outcome_type: OutcomeType,
        event_ids: tuple[str, ...],
        rejection: Rejected | None,
    ) -> None:
        uow.processed_commands.put(
            CommandOutcome(
                consumer_id=self._consumer_id,
                command_id=message.command_id,
                command_fingerprint=fingerprint,
                outcome_type=outcome_type,
                event_ids=event_ids,
                rejection_code=rejection.code if rejection else None,
                rejection_message=rejection.message if rejection else None,
                processed_at=self._clock.now(),
            )
        )
