"""ProviderExecutionSpec:副作用发生前作为事件数据落库的不可变执行规格 + 报价。

它把 operation 的稳定身份(operation_id 由 attempt_id + logical_operation_key 派生)、
请求内容指纹、以及带 pricing_version 的报价一并固定,供 Budget/ProviderOperation 使用。
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from studio.domain._base import Currency, NonNegativeMoney, Sha256Hex

from . import identity


class ProviderExecutionSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    attempt_id: str
    logical_operation_key: str
    provider_id: str
    provider_version: str
    request_ref: str
    request_digest: Sha256Hex
    estimated_cost: NonNegativeMoney
    currency: Currency
    pricing_version: str

    @property
    def operation_id(self) -> str:
        return identity.operation_id(self.attempt_id, self.logical_operation_key)

    def quote_digest(self) -> str:
        """报价指纹:同 operation 的 reserve/settle 必须一致,否则 IdempotencyConflict。"""
        from studio.serialization import digest

        return digest(
            {
                "operation_id": self.operation_id,
                "provider_id": self.provider_id,
                "provider_version": self.provider_version,
                "request_digest": self.request_digest,
                "estimated_cost": str(Decimal(self.estimated_cost)),
                "currency": self.currency,
                "pricing_version": self.pricing_version,
            }
        )
