"""内存 ProcessedCommandStore:读已提交,写缓冲。"""

from __future__ import annotations

from studio.kernel.outcomes import CommandOutcome

from ._state import Buffers, DbState


class MemoryProcessedCommandStore:
    def __init__(self, state: DbState, buffers: Buffers) -> None:
        self._s = state
        self._b = buffers

    def get(self, consumer_id: str, command_id: str) -> CommandOutcome | None:
        return self._s.processed.get((consumer_id, command_id))

    def put(self, outcome: CommandOutcome) -> None:
        self._b.processed_puts.append(outcome)
