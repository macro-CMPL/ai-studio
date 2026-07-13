"""Outbox Relay:读最早未发送 -> publish 到 CommandBus -> 标记 sent。

publish 在事务外(外部系统)。若 publish 成功、标记 sent 前崩溃,
下一 tick 会重投同一命令;Command Worker 靠 command_id 去重 => effectively-once。
每次 tick 处理一条(公平)。
"""

from __future__ import annotations

from studio.kernel.ports import CommandBus, UnitOfWorkFactory


class OutboxRelay:
    def __init__(
        self,
        *,
        uow_factory: UnitOfWorkFactory,
        bus: CommandBus,
    ) -> None:
        self._uow = uow_factory
        self._bus = bus

    def tick(self) -> bool:
        with self._uow() as uow:
            message = uow.outbox.next_unsent()
        if message is None:
            return False

        # publish 在事务外:这里崩溃 -> 未发送,下一 tick 重来。
        self._bus.publish(message)

        # publish 之后、标记 sent 之前崩溃 -> 重投(Worker 去重)。
        with self._uow() as uow:
            uow.outbox.mark_sent(message.command_id)
            uow.commit()
        return True
