"""执行期实体:TaskAttempt 与 ProviderOperation。

TaskAttempt 是重放与幂等的锚点:
- 技术重试沿用同一 attempt_id => operation_id 不变 => 不重复扣费
- 业务返工创建新 attempt_id => operation_id 变化 => 允许重新生成
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict

from .artifacts import ArtifactRef
from .enums import ProviderOpStatus, TaskAttemptStatus


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class TaskAttempt(_Frozen):
    attempt_id: str
    stage_id: str
    partition_key: str | None
    attempt_no: int
    consumed_refs: tuple[ArtifactRef, ...]
    plan_ref: ArtifactRef | None
    status: TaskAttemptStatus
    created_at: datetime


class ProviderOperation(_Frozen):
    """外部副作用单元。operation_id 即幂等键。"""

    operation_id: str
    attempt_id: str
    logical_operation_key: str
    status: ProviderOpStatus
    job_id: str | None
    cost_estimate: str
    cost_actual: str | None
