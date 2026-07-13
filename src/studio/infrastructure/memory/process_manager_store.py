"""内存 ProcessManagerStore:读已提交,写缓冲(save / set_checkpoint)。"""

from __future__ import annotations

from typing import Any

from studio.kernel.outcomes import Versioned

from ._state import Buffers, DbState


class MemoryProcessManagerStore:
    def __init__(self, state: DbState, buffers: Buffers) -> None:
        self._s = state
        self._b = buffers

    def load(self, pm_id: str) -> Versioned[Any] | None:
        entry = self._s.pm_states.get(pm_id)
        if entry is None:
            return None
        version, value = entry
        return Versioned(version=version, value=value)

    def save(self, pm_id: str, expected_version: int, state: Any) -> None:
        self._b.pm_saves.append((pm_id, expected_version, state))

    def checkpoint(self, pm_id: str) -> int:
        return self._s.pm_checkpoints.get(pm_id, -1)

    def set_checkpoint(self, pm_id: str, global_position: int) -> None:
        self._b.checkpoint_sets.append((pm_id, global_position))
