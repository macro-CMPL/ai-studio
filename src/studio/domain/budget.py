"""预算:追加式不可变账本。余额为派生量,不存字段。

条目类型:RESERVE / CAPTURE / RELEASE / ADJUSTMENT。
- 成功扣费:CAPTURE(actual)
- 失败但被扣费:仍 CAPTURE(actual)
- 未扣费失败:RELEASE
- SUBMISSION_UNKNOWN:保持 reservation,不 RELEASE
- 实际 < 预留:CAPTURE 后 RELEASE 余额
- 实际 > 预留:记录实际并将订单转 OVER_BUDGET/BLOCKED
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from .enums import LedgerEntryType, LedgerSubjectType


class LedgerEntry(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    entry_id: str
    budget_id: str
    entry_type: LedgerEntryType
    amount: Decimal
    currency: str
    subject_type: LedgerSubjectType
    subject_id: str
    reservation_id: str | None
    created_at: datetime
