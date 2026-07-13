"""执行期实体:TaskAttempt 与 ProviderOperation。

TaskAttempt 是重放与幂等的锚点:
- 技术重试沿用同一 attempt_id => operation_id 不变 => 不重复扣费
- 业务返工创建新 attempt_id => operation_id 变化 => 允许重新生成
"""

from __future__ import annotations

from pydantic import PositiveInt, model_validator

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
    """外部副作用单元。operation_id 即幂等键。"""

    operation_id: str
    attempt_id: str
    logical_operation_key: str
    status: ProviderOpStatus
    job_id: str | None
    cost_estimate: NonNegativeMoney
    cost_actual: NonNegativeMoney | None

    @model_validator(mode="after")
    def _check_job_id(self) -> ProviderOperation:
        # 已提交/成功的操作必须持有外部 job_id,否则无法对账/查询。
        if self.status in (ProviderOpStatus.SUBMITTED, ProviderOpStatus.SUCCEEDED) and (
            self.job_id is None
        ):
            raise ValueError(f"状态 {self.status} 要求存在 job_id")
        return self
