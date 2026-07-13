"""执行期实体:TaskAttempt 与 ProviderOperation。

TaskAttempt 是重放与幂等的锚点:
- 技术重试沿用同一 attempt_id => operation_id 不变 => 不重复扣费
- 业务返工创建新 attempt_id => operation_id 变化 => 允许重新生成
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import PositiveInt, model_validator

from . import ids
from ._base import FrozenModel, NonNegativeMoney, UtcDatetime
from .artifacts import ArtifactRef
from .enums import ProviderOpStatus, TaskAttemptStatus


class TaskAttempt(FrozenModel):
    attempt_id: str
    stage_id: str
    partition_key: str | None
    attempt_no: PositiveInt
    consumed_refs: tuple[ArtifactRef, ...]
    plan_ref: ArtifactRef | None
    status: TaskAttemptStatus
    created_at: UtcDatetime


class ProviderOperation(FrozenModel):
    """外部副作用单元。operation_id 即幂等键,不可伪造。"""

    operation_id: str
    attempt_id: str
    logical_operation_key: str
    status: ProviderOpStatus
    job_id: str | None
    cost_estimate: NonNegativeMoney
    cost_actual: NonNegativeMoney | None

    @model_validator(mode="after")
    def _enforce_invariants(self) -> ProviderOperation:
        expected = ids.operation_id(self.attempt_id, self.logical_operation_key)
        if self.operation_id != expected:
            raise ValueError("operation_id 与 (attempt_id, logical_operation_key) 不一致")
        # 已提交/成功的操作必须持有外部 job_id,否则无法对账/查询。
        if self.status in (ProviderOpStatus.SUBMITTED, ProviderOpStatus.SUCCEEDED) and (
            self.job_id is None
        ):
            raise ValueError(f"状态 {self.status} 要求存在 job_id")
        # 尚未提交的操作不应预先持有 job_id(生命周期矛盾)。
        if self.status is ProviderOpStatus.INITIATED and self.job_id is not None:
            raise ValueError("INITIATED 状态不应存在 job_id")
        return self

    @classmethod
    def create(
        cls,
        *,
        attempt_id: str,
        logical_operation_key: str,
        status: ProviderOpStatus,
        cost_estimate: Decimal,
        job_id: str | None = None,
        cost_actual: Decimal | None = None,
    ) -> ProviderOperation:
        """唯一推荐的构造入口:operation_id 由 attempt + 逻辑键派生。"""
        return cls(
            operation_id=ids.operation_id(attempt_id, logical_operation_key),
            attempt_id=attempt_id,
            logical_operation_key=logical_operation_key,
            status=status,
            job_id=job_id,
            cost_estimate=cost_estimate,
            cost_actual=cost_actual,
        )
