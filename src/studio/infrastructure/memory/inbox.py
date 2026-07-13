"""内存 Inbox:读已提交,写缓冲。"""

from __future__ import annotations

from ._state import Buffers, DbState


class MemoryInbox:
    def __init__(self, state: DbState, buffers: Buffers) -> None:
        self._s = state
        self._b = buffers

    def is_processed(self, consumer_id: str, event_id: str) -> bool:
        return (consumer_id, event_id) in self._s.inbox

    def mark_processed(self, consumer_id: str, event_id: str) -> None:
        self._b.inbox_adds.append((consumer_id, event_id))
