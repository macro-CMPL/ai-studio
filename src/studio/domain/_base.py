"""领域层共享基元:深度不可变基类、强类型时间与金额。"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated

from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field


class FrozenModel(BaseModel):
    """不可变、禁止多余字段。所有领域值对象的基类。"""

    model_config = ConfigDict(frozen=True, extra="forbid")


def _to_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


# 拒绝 naive datetime(AwareDatetime),并统一归一化到 UTC(可重放/可比较)。
UtcDatetime = Annotated[AwareDatetime, AfterValidator(_to_utc)]

# 非负金额。方向由 LedgerEntry.entry_type 表达,金额本身恒非负。
NonNegativeMoney = Annotated[Decimal, Field(ge=0)]
