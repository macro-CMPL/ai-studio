"""ProviderExecutionSpec:副作用发生前作为事件数据落库的不可变执行规格 + 报价。

携带 Plan 契约证据(plan_ref + 唯一 operation),使 AttemptDecider 能验证:
plan_ref 属于该 Attempt 的输入、Plan 恰好一个 operation、logical_operation_key 来自该 operation、
request_digest 与 operation 内容一致、operation_id 由 attempt+key 派生。
"""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, model_validator

from studio.domain._base import Currency, NonNegativeMoney, Sha256Hex
from studio.domain.artifacts import ArtifactRef, ImagePlanPayload, PlannedOperation
from studio.serialization import digest

from . import identity
from .values import BindingItem


def canon_money(value: Decimal) -> str:
    """金额规范化:10 与 10.0 产生相同串(避免报价指纹分裂)。"""
    return format(Decimal(value).normalize(), "f")


def _request_digest(operation: PlannedOperation) -> str:
    return digest(operation.model_dump(mode="json"))


class ProviderExecutionSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    attempt_id: str
    logical_operation_key: str
    provider_id: str
    provider_version: str
    plan_ref: ArtifactRef
    operation: PlannedOperation
    request_ref: str
    request_digest: Sha256Hex
    estimated_cost: NonNegativeMoney
    currency: Currency
    pricing_version: str

    @model_validator(mode="after")
    def _check(self) -> ProviderExecutionSpec:
        if self.logical_operation_key != self.operation.logical_operation_key:
            raise ValueError("logical_operation_key 必须来自 operation")
        if self.request_digest != _request_digest(self.operation):
            raise ValueError("request_digest 与 operation 内容不一致")
        return self

    @property
    def operation_id(self) -> str:
        return identity.operation_id(self.attempt_id, self.logical_operation_key)

    def quote_digest(self) -> str:
        return digest(
            {
                "operation_id": self.operation_id,
                "provider_id": self.provider_id,
                "provider_version": self.provider_version,
                "request_digest": self.request_digest,
                "estimated_cost": canon_money(self.estimated_cost),
                "currency": self.currency,
                "pricing_version": self.pricing_version,
            }
        )

    def verify_membership(self, exact_refs: Iterable[BindingItem]) -> None:
        """校验 plan_ref 确实是该 Attempt 的输入之一。"""
        if not any(b.artifact_id == self.plan_ref.artifact_id for b in exact_refs):
            raise ValueError("plan_ref 不属于该 Attempt 的 exact_refs")

    @classmethod
    def from_plan(
        cls,
        *,
        attempt_id: str,
        plan_ref: ArtifactRef,
        plan_payload: ImagePlanPayload,
        provider_id: str,
        provider_version: str,
        estimated_cost: Decimal,
        currency: str,
        pricing_version: str,
        request_ref: str,
    ) -> ProviderExecutionSpec:
        # Plan 必须恰好一个 operation(不默取第一项)
        if len(plan_payload.operations) != 1:
            raise ValueError("Plan 必须恰好一个 operation")
        op = plan_payload.operations[0]
        return cls(
            attempt_id=attempt_id,
            logical_operation_key=op.logical_operation_key,
            provider_id=provider_id,
            provider_version=provider_version,
            plan_ref=plan_ref,
            operation=op,
            request_ref=request_ref,
            request_digest=_request_digest(op),
            estimated_cost=estimated_cost,
            currency=currency,
            pricing_version=pricing_version,
        )
