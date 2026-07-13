"""预算:追加式不可变账本。余额为派生量,不存字段。

条目类型:RESERVE / CAPTURE / RELEASE / ADJUSTMENT。
- 成功扣费:CAPTURE(actual)
- 失败但被扣费:仍 CAPTURE(actual)
- 未扣费失败:RELEASE
- SUBMISSION_UNKNOWN:保持 reservation,不 RELEASE
- 实际 < 预留:CAPTURE 后 RELEASE 余额
- 实际 > 预留:记录实际并将订单转 OVER_BUDGET/BLOCKED

金额恒非负;方向由 entry_type 表达。
"""

from __future__ import annotations

from typing import Annotated

from pydantic import StringConstraints

from ._base import FrozenModel, NonNegativeMoney, UtcDatetime
from .enums import LedgerEntryType, LedgerSubjectType

_NonEmpty = Annotated[str, StringConstraints(min_length=1)]


class LedgerEntry(FrozenModel):
    entry_id: str
    budget_id: str
    entry_type: LedgerEntryType
    amount: NonNegativeMoney
    currency: _NonEmpty
    subject_type: LedgerSubjectType
    subject_id: _NonEmpty
    reservation_id: str | None
    created_at: UtcDatetime
