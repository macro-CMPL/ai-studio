"""命令处理结果与版本化状态载体。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Generic, TypeVar

T = TypeVar("T")


class OutcomeType(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


@dataclass(frozen=True)
class CommandOutcome:
    """ProcessedCommand 的持久化结果:重投时返回同一结果,不重新 decide。

    command_fingerprint 用于识别"同 command_id、异内容"的调用方错误。
    """

    consumer_id: str
    command_id: str
    command_fingerprint: str
    outcome_type: OutcomeType
    event_ids: tuple[str, ...]
    rejection_code: str | None
    rejection_message: str | None
    processed_at: datetime


@dataclass(frozen=True)
class Versioned(Generic[T]):
    """带乐观并发版本的状态。version 为已应用的事件/写入数。"""

    version: int
    value: T
