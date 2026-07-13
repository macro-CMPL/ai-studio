"""内核异常。"""

from __future__ import annotations


class KernelError(Exception):
    """内核层错误基类。"""


class ConcurrencyConflict(KernelError):
    """append 时 expected_version 与流的当前版本不符。可重试(重载状态后)。"""

    def __init__(self, stream_id: str, expected: int, actual: int) -> None:
        super().__init__(
            f"并发冲突 stream={stream_id} expected={expected} actual={actual}"
        )
        self.stream_id = stream_id
        self.expected = expected
        self.actual = actual


class IdempotencyConflict(KernelError):
    """同一 event_id 出现但 payload digest 不同 —— 正确性错误,必须报警。

    通常意味着同一 Command 在不同代码版本下产生了不同事实。
    """

    def __init__(self, event_id: str) -> None:
        super().__init__(f"幂等冲突:event_id={event_id} 对应不同的 payload")
        self.event_id = event_id
