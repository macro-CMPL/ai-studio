"""Event Pump:从 EventStore 按 checkpoint 拉取事件,react,事务性推进。

崩溃恢复靠 read_all(after=checkpoint) + 每 PM 的 last_global_position,
而非"新事件直接喂 PM"。无关事件也推进 checkpoint,避免死循环。
每次 tick 处理一条事件(公平)。绑定单个 ProcessManager。
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from studio.kernel import identifiers as kids
from studio.kernel.envelopes import CommandEnvelope, MessagePayload
from studio.kernel.errors import ContractViolation
from studio.kernel.ports import Clock, UnitOfWorkFactory
from studio.kernel.process_manager import ProcessManager

TPMState = TypeVar("TPMState")
TEvt = TypeVar("TEvt", bound=MessagePayload)
TCmd = TypeVar("TCmd", bound=MessagePayload)


class EventPump(Generic[TPMState, TEvt, TCmd]):
    def __init__(
        self,
        *,
        process_manager: ProcessManager[TPMState, TEvt, TCmd],
        uow_factory: UnitOfWorkFactory,
        clock: Clock,
        schema_version: int = 1,
    ) -> None:
        self._pm = process_manager
        self._uow = uow_factory
        self._clock = clock
        self._schema_version = schema_version

    @property
    def pm_id(self) -> str:
        return self._pm.pm_id

    def tick(self) -> bool:
        pm_id = self._pm.pm_id
        with self._uow() as uow:
            checkpoint = uow.process_managers.checkpoint(pm_id)
            events = uow.event_store.read_all(checkpoint)
            if not events:
                return False
            event = events[0]  # 最早的未处理事件(有限批 = 1)

            # 幂等双保险:即使已在 Inbox,也要推进 checkpoint 避免卡住。
            if uow.inbox.is_processed(pm_id, event.event_id):
                uow.process_managers.set_checkpoint(pm_id, event.global_position)
                uow.commit()
                return True

            loaded = uow.process_managers.load(pm_id)
            if loaded is None:
                pm_state = self._pm.initial_state()
                pm_version = 0
            else:
                pm_state = loaded.value
                pm_version = loaded.version

            reaction = self._pm.react(pm_state, event)

            seen_command_ids: set[str] = set()
            for pc in reaction.commands:
                cid = kids.command_id(
                    pm_id, event.event_id, pc.reaction_name, pc.command_key
                )
                if cid in seen_command_ids:
                    raise ContractViolation(
                        f"同一 Reaction 产生重复 command_id={cid}"
                    )
                seen_command_ids.add(cid)
                command: CommandEnvelope[Any] = CommandEnvelope(
                    command_id=cid,
                    schema_version=self._schema_version,
                    target=pc.target,
                    command_key=pc.command_key,
                    correlation_id=event.correlation_id,
                    causation_id=event.event_id,
                    issued_at=self._clock.now(),
                    payload=pc.payload,
                )
                uow.outbox.enqueue(command)

            uow.inbox.mark_processed(pm_id, event.event_id)
            uow.process_managers.save(pm_id, pm_version, reaction.state)
            uow.process_managers.set_checkpoint(pm_id, event.global_position)
            uow.commit()
            return True
