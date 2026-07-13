"""领域层共享基元:深度不可变基类、强类型时间、金额与格式化字符串。"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
)


class FrozenModel(BaseModel):
    """不可变、禁止多余字段。所有领域值对象的基类。"""

    model_config = ConfigDict(frozen=True, extra="forbid")


def _to_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


# 拒绝 naive datetime(AwareDatetime),并统一归一化到 UTC(可重放/可比较)。
UtcDatetime = Annotated[AwareDatetime, AfterValidator(_to_utc)]

# 非负金额。方向由 LedgerEntry.entry_type 表达,金额本身恒非负。
NonNegativeMoney = Annotated[Decimal, Field(ge=0)]

# 小写十六进制 SHA-256 摘要(内容寻址身份)。
Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

# ISO-4217 风格货币代码:三个大写字母。
Currency = Annotated[str, StringConstraints(pattern=r"^[A-Z]{3}$")]

# 非空且去除首尾空白后仍非空的字符串。
NonBlank = Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]
