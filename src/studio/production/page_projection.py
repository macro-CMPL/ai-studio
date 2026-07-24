"""工作室页面状态投影:由事件折叠出逐工位(stage)的中文状态视图(只读模型)。

第五里程碑只实现后端投影,不实现真实前端。每个工位提供:
工位编号 / 显示名称 / 状态 / 当前任务数 / 已完成数 / 受阻数 / 当前分区 / 返工次数 /
当前产物版本 / 累计成本 / 最近更新时间。

状态(统一中文):待命 / 工作中 / 质检中 / 已完成 / 受阻 / 等待人工 / 返工中。
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict

from studio.domain.enums import ControlRole
from studio.kernel.envelopes import EventEnvelope

from .attempt_payloads import AttemptBlockedEvt, ProviderExecutionSpecRecordedEvt
from .budget import BudgetSettlementCompletedEvt
from .compile import CompiledPipelineSpec
from .payloads import (
    ArtifactAcceptanceRevokedEvt,
    ArtifactVersionAcceptedEvt,
    ArtifactVersionProposedEvt,
    ProjectAwaitingHumanEvt,
    TaskAttemptCreatedEvt,
)

_DISPLAY_NAMES: dict[str, str] = {
    "storyboard": "分镜工位",
    "plan": "提示词工位",
    "image": "出图工位",
    "prompt_qc": "提示词质检工位",
    "result_qc": "结果质检工位",
    "stage_qc": "阶段质检工位",
    "delivery": "交付工位",
}


class StationStatus(StrEnum):
    IDLE = "待命"
    WORKING = "工作中"
    QC_RUNNING = "质检中"
    DONE = "已完成"
    BLOCKED = "受阻"
    AWAITING_HUMAN = "等待人工"
    REWORKING = "返工中"


class StationView(BaseModel):
    model_config = ConfigDict(frozen=True)

    stage_id: str
    display_name: str
    status: StationStatus
    task_count: int
    completed_count: int
    blocked_count: int
    current_partition: str | None
    rework_count: int
    current_artifact_version: int | None
    accumulated_cost: Decimal
    last_updated: datetime | None


class _StageAccumulator:
    def __init__(self, stage_id: str, is_evaluator: bool) -> None:
        self.stage_id = stage_id
        self.is_evaluator = is_evaluator
        self.attempt_ids: set[str] = set()
        self.partitions: set[str | None] = set()
        self.max_generation: dict[str | None, int] = {}
        self.current_ref: dict[str | None, Any] = {}  # partition -> ArtifactRef|None
        self.revoked_partitions: set[str | None] = set()
        self.blocked_attempts: set[str] = set()
        self.awaiting_human = False
        self.cost = Decimal(0)
        self.last_updated: datetime | None = None

    def touch(self, when: datetime) -> None:
        if self.last_updated is None or when > self.last_updated:
            self.last_updated = when


class StudioPageProjection:
    """按 stage(工位)聚合的页面投影。用 build(spec, events) 从事件日志折叠。"""

    def __init__(self, spec: CompiledPipelineSpec) -> None:
        self._spec = spec
        self._acc: dict[str, _StageAccumulator] = {
            s.stage_id: _StageAccumulator(
                s.stage_id, s.control_role is ControlRole.EVALUATOR
            )
            for s in spec.stages
        }
        self._attempt_stage: dict[str, str] = {}
        self._op_attempt: dict[str, str] = {}  # operation_id -> attempt_id
        # artifact_id -> (output_key, partition_key)(从提议事件建立,撤销时反查)
        self._artifact_index: dict[str, tuple[str, str | None]] = {}

    @classmethod
    def build(
        cls, spec: CompiledPipelineSpec, events: list[EventEnvelope[Any]]
    ) -> StudioPageProjection:
        view = cls(spec)
        for env in sorted(events, key=lambda e: e.global_position):
            view._apply(env)
        return view

    def _apply(self, env: EventEnvelope[Any]) -> None:
        payload = env.payload
        when = env.recorded_at
        if isinstance(payload, TaskAttemptCreatedEvt):
            acc = self._acc.get(payload.stage_id)
            if acc is None:
                return
            acc.attempt_ids.add(payload.attempt_id)
            acc.partitions.add(payload.partition_key)
            acc.max_generation[payload.partition_key] = max(
                acc.max_generation.get(payload.partition_key, 0),
                payload.execution_generation,
            )
            self._attempt_stage[payload.attempt_id] = payload.stage_id
            acc.touch(when)
        elif isinstance(payload, ArtifactVersionProposedEvt):
            self._artifact_index[payload.artifact_ref.artifact_id] = (
                payload.output_key,
                payload.partition_key,
            )
        elif isinstance(payload, ArtifactVersionAcceptedEvt):
            acc = self._acc.get(payload.output_key)
            if acc is not None:
                acc.current_ref[payload.partition_key] = payload.artifact_ref
                acc.revoked_partitions.discard(payload.partition_key)
                acc.touch(when)
        elif isinstance(payload, ArtifactAcceptanceRevokedEvt):
            indexed = self._artifact_index.get(payload.artifact_ref.artifact_id)
            if indexed is not None:
                output_key, partition = indexed
                acc = self._acc.get(output_key)
                if acc is not None:
                    acc.current_ref[partition] = payload.new_current_ref
                    if payload.new_current_ref is None:
                        acc.revoked_partitions.add(partition)
                    acc.touch(when)
        elif isinstance(payload, AttemptBlockedEvt):
            stage = self._attempt_stage.get(payload.attempt_id)
            if stage is not None:
                self._acc[stage].blocked_attempts.add(payload.attempt_id)
                self._acc[stage].touch(when)
        elif isinstance(payload, ProjectAwaitingHumanEvt):
            acc = self._acc.get(payload.stage_id)
            if acc is not None:
                acc.awaiting_human = True
                acc.touch(when)
        elif isinstance(payload, ProviderExecutionSpecRecordedEvt):
            self._op_attempt[payload.spec.operation_id] = payload.spec.attempt_id
        elif isinstance(payload, BudgetSettlementCompletedEvt):
            if payload.outcome != "captured":
                return
            attempt = self._op_attempt.get(payload.operation_id)
            stage = self._attempt_stage.get(attempt) if attempt else None
            if stage is not None:
                self._acc[stage].cost += payload.captured_amount
                self._acc[stage].touch(when)

    def station(self, stage_id: str) -> StationView:
        acc = self._acc[stage_id]
        completed = [p for p, ref in acc.current_ref.items() if ref is not None]
        reworking = [
            p
            for p in acc.revoked_partitions
            if acc.current_ref.get(p) is None
        ]
        status = self._status(acc, completed, reworking)
        # 当前分区:返工中优先显示返工分区,否则显示最近完成分区
        current_partition = _pick_partition(reworking or completed)
        current_version = None
        if current_partition is not None:
            ref = acc.current_ref.get(current_partition)
            current_version = ref.revision if ref is not None else None
        rework_count = sum(g for g in acc.max_generation.values())
        return StationView(
            stage_id=stage_id,
            display_name=_DISPLAY_NAMES.get(stage_id, stage_id),
            status=status,
            task_count=len(acc.attempt_ids),
            completed_count=len(completed),
            blocked_count=len(acc.blocked_attempts),
            current_partition=current_partition,
            rework_count=rework_count,
            current_artifact_version=current_version,
            accumulated_cost=acc.cost,
            last_updated=acc.last_updated,
        )

    def stations(self) -> tuple[StationView, ...]:
        return tuple(
            self.station(s.stage_id)
            for s in sorted(self._spec.stages, key=lambda x: x.stage_id)
        )

    def _status(
        self,
        acc: _StageAccumulator,
        completed: list[str | None],
        reworking: list[str | None],
    ) -> StationStatus:
        if not acc.attempt_ids:
            return StationStatus.IDLE
        if acc.awaiting_human:
            return StationStatus.AWAITING_HUMAN
        if acc.blocked_attempts:
            return StationStatus.BLOCKED
        if reworking:
            return StationStatus.REWORKING
        # 全部有过 attempt 的分区都已完成 -> 已完成
        if completed and len(completed) == len(acc.partitions):
            return StationStatus.DONE
        return StationStatus.QC_RUNNING if acc.is_evaluator else StationStatus.WORKING


def _pick_partition(partitions: list[str | None]) -> str | None:
    named = sorted(p for p in partitions if p is not None)
    return named[0] if named else None
