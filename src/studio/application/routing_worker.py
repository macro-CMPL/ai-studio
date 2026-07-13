"""RoutingCommandWorker:按目标流类别把命令路由到对应 Decider。

复用 M2 CommandWorker 的所有一致性保证(去重、指纹、快照、冲突纳入边界),
仅在每条消息上按 target 的流前缀解析 decider 与 consumer_id。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from studio.kernel import identifiers as kids
from studio.kernel.decisions import Accepted, Rejected
from studio.kernel.errors import ConcurrencyConflict, ContractViolation, IdempotencyConflict
from studio.kernel.fingerprints import command_fingerprint
from studio.kernel.outcomes import CommandOutcome, OutcomeType
from studio.kernel.ports import Clock, CommandBus, NewEvent, UnitOfWork, UnitOfWorkFactory


class RoutingCommandWorker:
    def __init__(
        self,
        *,
        deciders: Mapping[str, Any],
        resolve_kind: Callable[[str], str],
        bus: CommandBus,
        uow_factory: UnitOfWorkFactory,
        clock: Clock,
        schema_version: int = 1,
    ) -> None:
        self._deciders = deciders
        self._resolve_kind = resolve_kind
        self._bus = bus
        self._uow = uow_factory
        self._clock = clock
        self._schema_version = schema_version

    def _fold(self, decider: Any, events: list[Any]) -> Any:
        state = decider.initial_state()
        for env in events:
            state = decider.evolve(state, env.payload)
        return state

    def tick(self) -> bool:
        message = self._bus.peek()
        if message is None:
            return False

        kind = self._resolve_kind(message.target)
        decider = self._deciders[kind]
        consumer_id = f"worker:{kind}"
        fingerprint = command_fingerprint(
            message.target, message.command_key, message.payload
        )

        with self._uow() as uow:
            existing = uow.processed_commands.get(consumer_id, message.command_id)
            if existing is not None:
                if existing.command_fingerprint != fingerprint:
                    raise IdempotencyConflict(
                        message.command_id, "同 command_id 复用于不同内容"
                    )
                self._bus.ack(message.command_id)
                return True

            snapshot = uow.event_store.load_stream(message.target)
            state = self._fold(decider, snapshot.value)
            version = snapshot.version
            decision = decider.decide(state, message.payload)

            if isinstance(decision, Rejected):
                self._put(uow, consumer_id, message, fingerprint, OutcomeType.REJECTED,
                          (), decision)
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
                self._put(uow, consumer_id, message, fingerprint, OutcomeType.ACCEPTED,
                          event_ids, None)

            try:
                uow.commit()
            except ConcurrencyConflict:
                return True

        self._bus.ack(message.command_id)
        return True

    def _put(
        self,
        uow: UnitOfWork,
        consumer_id: str,
        message: Any,
        fingerprint: str,
        outcome_type: OutcomeType,
        event_ids: tuple[str, ...],
        rejection: Rejected | None,
    ) -> None:
        uow.processed_commands.put(
            CommandOutcome(
                consumer_id=consumer_id,
                command_id=message.command_id,
                command_fingerprint=fingerprint,
                outcome_type=outcome_type,
                event_ids=event_ids,
                rejection_code=rejection.code if rejection else None,
                rejection_message=rejection.message if rejection else None,
                processed_at=self._clock.now(),
            )
        )
