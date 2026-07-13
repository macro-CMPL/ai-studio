"""内核异常。"""

from __future__ import annotations


class KernelError(Exception):
    """内核层错误基类。"""


class ConcurrencyConflict(KernelError):
    """乐观并发冲突:expected_version 与当前版本不符。可重试(重载状态后)。"""

    def __init__(self, resource: str, expected: int, actual: int) -> None:
        super().__init__(
            f"并发冲突 resource={resource} expected={expected} actual={actual}"
        )
        self.resource = resource
        self.expected = expected
        self.actual = actual


class IdempotencyConflict(KernelError):
    """同一身份(event_id / command_id)对应不同内容 —— 正确性错误,必须报警。"""

    def __init__(self, identifier: str, detail: str = "") -> None:
        message = f"幂等冲突:{identifier}"
        if detail:
            message = f"{message} ({detail})"
        super().__init__(message)
        self.identifier = identifier
        self.detail = detail


class ContractViolation(KernelError):
    """Decider/ProcessManager 违反内核契约(如同一 Decision 内 event_key 重复)。"""
