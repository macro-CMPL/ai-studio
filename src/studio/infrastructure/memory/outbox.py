"""内存 Outbox:读已提交,写缓冲(enqueue / mark_sent)。"""

from __future__ import annotations

from typing import Any

from studio.kernel.envelopes import CommandEnvelope

from ._state import Buffers, DbState


class MemoryOutbox:
    def __init__(self, state: DbState, buffers: Buffers) -> None:
        self._s = state
        self._b = buffers

    def enqueue(self, message: CommandEnvelope[Any]) -> None:
        self._b.outbox_adds.append(message)

    def next_unsent(self) -> CommandEnvelope[Any] | None:
        for row in self._s.outbox:
            if not row.sent:
                return row.message
        return None

    def mark_sent(self, command_id: str) -> None:
        self._b.outbox_sent.append(command_id)
