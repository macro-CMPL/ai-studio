"""同步 Driver:每轮显式调用全部阶段(禁止短路 or 造成饥饿)。

每个 tick 处理有限批(一条),Relay 本轮产出的命令下一轮由 Worker 处理,
从而在阶段之间保留崩溃边界。PM 遍历顺序按 pm_id 排序,保证确定性。
三个阶段互不直接调用。
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol


class SupportsTick(Protocol):
    def tick(self) -> bool: ...


class SupportsPumpTick(Protocol):
    @property
    def pm_id(self) -> str: ...

    def tick(self) -> bool: ...


class Driver:
    def __init__(
        self,
        *,
        worker: SupportsTick,
        pumps: Sequence[SupportsPumpTick],
        relay: SupportsTick,
        activity: Sequence[SupportsTick] = (),
    ) -> None:
        self._worker = worker
        # 按 pm_id 排序,保证确定性遍历。
        self._pumps = sorted(pumps, key=lambda p: p.pm_id)
        self._relay = relay
        # Activity worker(s):唯一做外部 I/O 的 tick,放在 relay 之后,
        # 其产出的命令下一轮由 worker 处理(阶段间保留崩溃边界)。
        self._activity = tuple(activity)

    def tick_round(self) -> bool:
        worker_progress = self._worker.tick()
        pump_results = [pump.tick() for pump in self._pumps]
        relay_progress = self._relay.tick()
        activity_results = [a.tick() for a in self._activity]
        return (
            worker_progress
            or any(pump_results)
            or relay_progress
            or any(activity_results)
        )

    def run_until_quiescent(self, max_rounds: int = 10_000) -> int:
        rounds = 0
        while self.tick_round():
            rounds += 1
            if rounds > max_rounds:
                raise RuntimeError("未在 max_rounds 内静默,可能存在活锁")
        return rounds
