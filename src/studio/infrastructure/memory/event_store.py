"""内存 EventStore:读已提交状态;append 缓冲写(校验/分配在 commit 时)。"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from studio.kernel.envelopes import EventEnvelope
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

    def read_all(self, after_global_position: int) -> list[EventEnvelope[Any]]:
        events = [
            e for e in self._s.events if e.global_position > after_global_position
        ]
        return sorted(events, key=lambda e: e.global_position)

    def append(
        self, stream_id: str, expected_version: int, events: Sequence[NewEvent]
    ) -> list[EventEnvelope[Any]]:
        if not events:
            return []
        self._b.append_intents.append((stream_id, expected_version, list(events)))
        # 临时信封:event_id 权威;sequence/global_position 为占位(commit 时定稿)。
        base_seq = self._s.stream_versions.get(stream_id, 0)
        base_pos = self._s.global_counter
        provisional: list[EventEnvelope[Any]] = []
        for i, ne in enumerate(events):
            provisional.append(
                EventEnvelope(
                    event_id=ne.event_id,
                    schema_version=ne.schema_version,
                    stream_id=stream_id,
                    sequence=base_seq + i,
                    global_position=base_pos + i,
                    correlation_id=ne.correlation_id,
                    causation_id=ne.causation_id,
                    recorded_at=ne.recorded_at,
                    payload=ne.payload,
                )
            )
        return provisional
