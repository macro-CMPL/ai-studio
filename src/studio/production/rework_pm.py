"""返工管理器:消费闸门 REWORK 决策 -> 相同输入强制重做(代数+1)或返工上限升级。

分工(锁定):
- 只处理"相同输入强制重做":输入引用不变,execution_generation 递增 ->
  新 attempt_id -> 新 operation_id -> 允许合理二次扣费。
- "输入变化返工"(上游产物被新版本取代)由 M3 的 Lineage/Recompute 血缘链负责,不在此。
- 返工上限:每阶段(按被重做产物的 stage_id)最大返工次数;下一代数超过上限则不再
  自动重做,改发 EscalateAwaitHumanCmd(项目进入等待人工,预算保持正确,不再付费)。

触发源:GateDecidedEvt(verdict=REWORK)。
- 阶段层决策:rework_scope 给出需重做的分区(可多个)。
- 结果层决策:target_partition 给出单一分区。
定位重做目标:target_ref.artifact_id -> 产出它的 attempt -> 该 attempt 的 output_key,
再对每个返工分区取该 output_key 下代数最高的 attempt 作为重做基线。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from studio.domain.enums import GateVerdict
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.kernel.process_manager import ProposedCommand, Reaction

from . import identity
from .gate import GateDecidedEvt
from .payloads import (
    ArtifactVersionProposedEvt,
    CreateTaskAttemptCmd,
    EscalateAwaitHumanCmd,
    TaskAttemptCreatedEvt,
    TaskInputsBoundEvt,
)
from .quality import QualityConfig
from .values import BindingItem

ReworkCommand = CreateTaskAttemptCmd | EscalateAwaitHumanCmd


class AttemptMeta(BaseModel):
    model_config = ConfigDict(frozen=True)
    attempt_id: str
    project_id: str
    stage_id: str
    output_key: str
    partition_key: str | None
    series_id: str
    generation: int
    bindings: tuple[BindingItem, ...] = ()


class ReworkState(BaseModel):
    model_config = ConfigDict(frozen=True)
    attempts: tuple[AttemptMeta, ...] = ()
    artifact_to_attempt: tuple[tuple[str, str], ...] = ()  # artifact_id -> attempt_id
    handled: tuple[str, ...] = ()  # 已处理的 (report_id, partition) 键(幂等)

    def attempt(self, attempt_id: str) -> AttemptMeta | None:
        return next((a for a in self.attempts if a.attempt_id == attempt_id), None)

    def attempt_of_artifact(self, artifact_id: str) -> AttemptMeta | None:
        aid = next(
            (a for (art, a) in self.artifact_to_attempt if art == artifact_id), None
        )
        return self.attempt(aid) if aid is not None else None

    def latest_for(self, output_key: str, partition: str | None) -> AttemptMeta | None:
        matches = [
            a
            for a in self.attempts
            if a.output_key == output_key and a.partition_key == partition
        ]
        if not matches:
            return None
        return max(matches, key=lambda a: a.generation)

    def with_attempt(self, updated: AttemptMeta) -> tuple[AttemptMeta, ...]:
        others = tuple(a for a in self.attempts if a.attempt_id != updated.attempt_id)
        return (*others, updated)


class ReworkProcessManager:
    pm_id = "rework-pm"

    def __init__(self, config: QualityConfig) -> None:
        self._config = config

    def initial_state(self) -> ReworkState:
        return ReworkState()

    def react(
        self, state: ReworkState, event: EventEnvelope[MessagePayload]
    ) -> Reaction[ReworkState, ReworkCommand]:
        payload = event.payload
        if isinstance(payload, TaskAttemptCreatedEvt):
            meta = AttemptMeta(
                attempt_id=payload.attempt_id, project_id=payload.project_id,
                stage_id=payload.stage_id, output_key=payload.output_key,
                partition_key=payload.partition_key, series_id=payload.series_id,
                generation=payload.execution_generation,
            )
            return Reaction(
                state=state.model_copy(
                    update={"attempts": state.with_attempt(meta)}
                ),
                commands=(),
            )
        if isinstance(payload, TaskInputsBoundEvt):
            bound = state.attempt(payload.attempt_id)
            if bound is None:
                return Reaction(state=state, commands=())
            updated = bound.model_copy(update={"bindings": payload.exact_refs})
            return Reaction(
                state=state.model_copy(
                    update={"attempts": state.with_attempt(updated)}
                ),
                commands=(),
            )
        if isinstance(payload, ArtifactVersionProposedEvt):
            mapping = (payload.artifact_ref.artifact_id, payload.produced_by_attempt)
            return Reaction(
                state=state.model_copy(
                    update={
                        "artifact_to_attempt": (*state.artifact_to_attempt, mapping)
                    }
                ),
                commands=(),
            )
        if isinstance(payload, GateDecidedEvt):
            return self._on_gate_decided(state, payload)
        return Reaction(state=state, commands=())

    def _on_gate_decided(
        self, state: ReworkState, payload: GateDecidedEvt
    ) -> Reaction[ReworkState, ReworkCommand]:
        if payload.verdict is not GateVerdict.REWORK:
            return Reaction(state=state, commands=())
        anchor = state.attempt_of_artifact(payload.target_ref.artifact_id)
        if anchor is None:
            return Reaction(state=state, commands=())  # 目标未追踪(不应发生)
        output_key = anchor.output_key
        partitions = payload.rework_scope or (payload.target_partition,)
        report_id = payload.report_ref.artifact_id
        limit = self._config.rework_limit(anchor.stage_id)

        commands: list[ProposedCommand[ReworkCommand]] = []
        new_state = state
        for partition in partitions:
            key = f"{report_id}:{partition}"
            if key in new_state.handled:
                continue  # 幂等:同报告同分区只重做一次
            base = new_state.latest_for(output_key, partition)
            if base is None:
                continue
            new_state = new_state.model_copy(
                update={"handled": (*new_state.handled, key)}
            )
            next_gen = base.generation + 1
            if limit is not None and next_gen > limit:
                commands.append(self._escalate(base, report_id, next_gen, payload))
            else:
                commands.append(self._rework(base, report_id, next_gen, payload))
        return Reaction(state=new_state, commands=tuple(commands))

    def _rework(
        self, base: AttemptMeta, report_id: str, next_gen: int, payload: GateDecidedEvt
    ) -> ProposedCommand[ReworkCommand]:
        tk = identity.task_key(base.project_id, base.stage_id, base.partition_key)
        new_aid = identity.attempt_id(
            tk, identity.input_binding_digest(base.bindings), next_gen
        )
        cmd = CreateTaskAttemptCmd(
            attempt_id=new_aid, project_id=base.project_id, stage_id=base.stage_id,
            partition_key=base.partition_key, output_key=base.output_key,
            series_id=base.series_id, exact_refs=base.bindings,
            execution_generation=next_gen, rework_of_attempt=base.attempt_id,
            rework_report_ref=report_id,
            rework_reason=payload.feedback or "same_input_rework",
        )
        return ProposedCommand(
            reaction_name=f"rework:{base.partition_key}",
            command_key=f"rework:{report_id}:{new_aid}",
            target=identity.attempt_stream(new_aid),
            payload=cmd,
        )

    def _escalate(
        self, base: AttemptMeta, report_id: str, next_gen: int, payload: GateDecidedEvt
    ) -> ProposedCommand[ReworkCommand]:
        cmd = EscalateAwaitHumanCmd(
            project_id=base.project_id, stage_id=base.stage_id,
            partition_key=base.partition_key, report_ref=report_id,
            generation=next_gen,
            reason=payload.feedback or "rework_limit_exceeded",
        )
        return ProposedCommand(
            reaction_name=f"escalate:{base.partition_key}",
            command_key=f"escalate:{report_id}:{base.stage_id}:{base.partition_key}",
            target=identity.project_stream(base.project_id),
            payload=cmd,
        )
