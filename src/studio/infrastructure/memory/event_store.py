"""内存 EventStore:读已提交状态;append 缓冲写(校验/分配在 commit 时)。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from studio.kernel.envelopes import EventEnvelope
from studio.kernel.outcomes import Versioned
from studio.kernel.ports import NewEvent

from ._state import Buffers, DbState


class MemoryEventStore:
    def __init__(self, state: DbState, buffers: Buffers) -> None:
        self._s = state
        self._b = buffers

    def current_version(self, stream_id: str) -> int:
        return self._s.stream_versions.get(stream_id, 0)

    def read_stream(self, stream_id: str) -> list[EventEnvelope[Any]]:
        events = [e for e in self._s.events if e.stream_id == stream_id]
        return sorted(events, key=lambda e: e.sequence)

    def load_stream(self, stream_id: str) -> Versioned[list[EventEnvelope[Any]]]:
        # 原子快照:version 与 events 同一次读取,不可撕裂。
        events = self.read_stream(stream_id)
        return Versioned(version=len(events), value=events)

    def read_all(self, after_global_position: int) -> list[EventEnvelope[Any]]:
        events = [
            e for e in self._s.events if e.global_position > after_global_position
        ]
        return sorted(events, key=lambda e: e.global_position)

    def append(
        self, stream_id: str, expected_version: int, events: Sequence[NewEvent]
    ) -> list[str]:
        if not events:
            return []
        self._b.append_intents.append((stream_id, expected_version, list(events)))
        return [ne.event_id for ne in events]
