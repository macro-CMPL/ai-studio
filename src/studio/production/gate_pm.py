"""闸门决策管理器:消费已接受的质量报告 -> 跑闸门策略 -> 记录决策 -> 应用结局。

分工(锁定):
- 质量评价器只出报告(QCReportPayload,自动接受);本 PM 不产报告,只消费。
- 两跳:
  1. 质量报告被接受 -> 用该层 GatePolicy 得 GateOutcome -> 发 DecideGateCmd(闸门流按报告隔离)。
  2. GateDecided 事实回流 -> 按层与判决应用产物生命周期变更:
     - 提示词/结果层(主体为"提议未接受"):PASS 接受主体;REWORK/BLOCK 拒绝主体。
     - 阶段层(主体已接受):REWORK 按返工范围逐分区撤销已接受图像;PASS/BLOCK 不动产物
       (PASS 交由交付管理器;BLOCK 等待人工)。
- 返工任务的创建不在本 PM(由返工管理器消费同一 GateDecided 事实完成),职责分离。

阶段层的 DecideGateCmd.target_ref 取报告 subject_refs[0] 作为锚点(仅审计),
实际逐分区动作以 rework_scope 为准。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from studio.domain.artifacts import ArtifactRef, QCReportPayload
from studio.domain.enums import GateVerdict
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.kernel.process_manager import ProposedCommand, Reaction

from . import identity
from .gate import DecideGateCmd, GateDecidedEvt
from .payloads import (
    AcceptArtifactVersionCmd,
    ArtifactCandidateProducedEvt,
    ArtifactVersionAcceptedEvt,
    ArtifactVersionProposedEvt,
    RejectArtifactVersionCmd,
    RevokeArtifactAcceptanceCmd,
)
from .quality import QCLayer, QCLayerSpec, QualityConfig

GateDecisionCommand = (
    DecideGateCmd
    | AcceptArtifactVersionCmd
    | RejectArtifactVersionCmd
    | RevokeArtifactAcceptanceCmd
)


class ReportCacheEntry(BaseModel):
    model_config = ConfigDict(frozen=True)
    candidate_id: str
    payload: QCReportPayload
    qc_stage_id: str


class ProposalEntry(BaseModel):
    model_config = ConfigDict(frozen=True)
    artifact_id: str
    candidate_id: str
    series_id: str
    project_id: str
    output_key: str
    partition_key: str | None


class AcceptedEntry(BaseModel):
    model_config = ConfigDict(frozen=True)
    output_key: str
    partition_key: str | None
    series_id: str
    project_id: str
    ref: ArtifactRef


class ReportRouting(BaseModel):
    model_config = ConfigDict(frozen=True)
    report_id: str
    qc_stage_id: str


class GateDecisionState(BaseModel):
    model_config = ConfigDict(frozen=True)
    report_cache: tuple[ReportCacheEntry, ...] = ()
    proposals: tuple[ProposalEntry, ...] = ()  # 门控主体的提议(接受/拒绝时定位 candidate)
    accepted: tuple[AcceptedEntry, ...] = ()  # 当前已接受门控主体(阶段返工撤销用)
    routings: tuple[ReportRouting, ...] = ()  # report_id -> qc_stage_id(应用时查层)
    decided: tuple[str, ...] = ()  # 已发 DecideGate 的 report_id
    applied: tuple[str, ...] = ()  # 已应用结局的 report_id

    def report_of(self, candidate_id: str) -> ReportCacheEntry | None:
        return next(
            (r for r in self.report_cache if r.candidate_id == candidate_id), None
        )

    def proposal_of(self, artifact_id: str) -> ProposalEntry | None:
        return next((p for p in self.proposals if p.artifact_id == artifact_id), None)

    def accepted_of(
        self, output_key: str, partition_key: str | None
    ) -> AcceptedEntry | None:
        return next(
            (
                a
                for a in self.accepted
                if a.output_key == output_key and a.partition_key == partition_key
            ),
            None,
        )

    def routing_of(self, report_id: str) -> ReportRouting | None:
        return next((r for r in self.routings if r.report_id == report_id), None)


class GateDecisionProcessManager:
    pm_id = "gate-decision-pm"

    def __init__(self, config: QualityConfig) -> None:
        self._config = config

    def initial_state(self) -> GateDecisionState:
        return GateDecisionState()

    def react(
        self, state: GateDecisionState, event: EventEnvelope[MessagePayload]
    ) -> Reaction[GateDecisionState, GateDecisionCommand]:
        payload = event.payload
        if isinstance(payload, ArtifactCandidateProducedEvt):
            return self._on_candidate(state, payload)
        if isinstance(payload, ArtifactVersionProposedEvt):
            return self._on_proposed(state, payload)
        if isinstance(payload, ArtifactVersionAcceptedEvt):
            return self._on_accepted(state, payload)
        if isinstance(payload, GateDecidedEvt):
            return self._on_gate_decided(state, payload)
        return Reaction(state=state, commands=())

    # -- 追踪 ------------------------------------------------------------- #

    def _on_candidate(
        self, state: GateDecisionState, payload: ArtifactCandidateProducedEvt
    ) -> Reaction[GateDecisionState, GateDecisionCommand]:
        if not isinstance(payload.payload, QCReportPayload):
            return Reaction(state=state, commands=())
        if self._config.layer_by_qc_stage(payload.output_key) is None:
            return Reaction(state=state, commands=())  # 非受管质检层
        entry = ReportCacheEntry(
            candidate_id=payload.candidate_id,
            payload=payload.payload,
            qc_stage_id=payload.output_key,
        )
        return Reaction(
            state=state.model_copy(
                update={"report_cache": (*state.report_cache, entry)}
            ),
            commands=(),
        )

    def _on_proposed(
        self, state: GateDecisionState, payload: ArtifactVersionProposedEvt
    ) -> Reaction[GateDecisionState, GateDecisionCommand]:
        if not self._config.is_gated(payload.output_key):
            return Reaction(state=state, commands=())
        entry = ProposalEntry(
            artifact_id=payload.artifact_ref.artifact_id,
            candidate_id=payload.candidate_id,
            series_id=payload.series_id,
            project_id=payload.project_id,
            output_key=payload.output_key,
            partition_key=payload.partition_key,
        )
        return Reaction(
            state=state.model_copy(update={"proposals": (*state.proposals, entry)}),
            commands=(),
        )

    def _on_accepted(
        self, state: GateDecisionState, payload: ArtifactVersionAcceptedEvt
    ) -> Reaction[GateDecisionState, GateDecisionCommand]:
        spec = self._config.layer_by_qc_stage(payload.output_key)
        if spec is not None:
            return self._decide(state, payload, spec)
        if self._config.is_gated(payload.output_key):
            # 更新"当前已接受门控主体"(同 output_key+partition 只保留最新)
            entry = AcceptedEntry(
                output_key=payload.output_key,
                partition_key=payload.partition_key,
                series_id=payload.series_id,
                project_id=payload.project_id,
                ref=payload.artifact_ref,
            )
            others = tuple(
                a
                for a in state.accepted
                if not (
                    a.output_key == entry.output_key
                    and a.partition_key == entry.partition_key
                )
            )
            return Reaction(
                state=state.model_copy(update={"accepted": (*others, entry)}),
                commands=(),
            )
        return Reaction(state=state, commands=())

    # -- 决策(第一跳)---------------------------------------------------- #

    def _decide(
        self,
        state: GateDecisionState,
        payload: ArtifactVersionAcceptedEvt,
        spec: QCLayerSpec,
    ) -> Reaction[GateDecisionState, GateDecisionCommand]:
        report_id = payload.artifact_ref.artifact_id
        if report_id in state.decided:
            return Reaction(state=state, commands=())  # 幂等
        report = state.report_of(payload.candidate_id)
        if report is None or not report.payload.subject_refs:
            return Reaction(state=state, commands=())  # 报告或主体缺失(不应发生)
        policy = self._config.policy(spec.layer)
        outcome = policy.decide(report.payload)
        cmd = DecideGateCmd(
            report_ref=payload.artifact_ref,
            target_ref=report.payload.subject_refs[0],
            target_partition=report.payload.target_partition,
            verdict=outcome.verdict,
            rework_scope=outcome.rework_scope,
            feedback=outcome.feedback,
            policy_id=policy.policy_id,
            policy_version=policy.policy_version,
        )
        new_state = state.model_copy(
            update={
                "decided": (*state.decided, report_id),
                "routings": (
                    *state.routings,
                    ReportRouting(report_id=report_id, qc_stage_id=spec.qc_stage_id),
                ),
            }
        )
        return Reaction(
            state=new_state,
            commands=(
                ProposedCommand(
                    reaction_name=f"decide:{spec.layer.value}",
                    command_key=f"decide:{report_id}",
                    target=identity.gate_stream(report_id),
                    payload=cmd,
                ),
            ),
        )

    # -- 应用(第二跳)---------------------------------------------------- #

    def _on_gate_decided(
        self, state: GateDecisionState, payload: GateDecidedEvt
    ) -> Reaction[GateDecisionState, GateDecisionCommand]:
        report_id = payload.report_ref.artifact_id
        if report_id in state.applied:
            return Reaction(state=state, commands=())  # 幂等
        routing = state.routing_of(report_id)
        if routing is None:
            return Reaction(state=state, commands=())  # 非本 PM 追踪
        spec = self._config.layer_by_qc_stage(routing.qc_stage_id)
        if spec is None:
            return Reaction(state=state, commands=())
        commands = (
            self._apply_stage(state, spec, payload)
            if spec.layer is QCLayer.STAGE
            else self._apply_subject(state, payload)
        )
        return Reaction(
            state=state.model_copy(update={"applied": (*state.applied, report_id)}),
            commands=commands,
        )

    def _apply_subject(
        self, state: GateDecisionState, payload: GateDecidedEvt
    ) -> tuple[ProposedCommand[GateDecisionCommand], ...]:
        """提示词/结果层:主体为提议-未接受。PASS 接受;REWORK/BLOCK 拒绝。"""
        proposal = state.proposal_of(payload.target_ref.artifact_id)
        if proposal is None:
            return ()  # 主体提议未追踪(不应发生)
        report_id = payload.report_ref.artifact_id
        if payload.verdict is GateVerdict.PASS:
            accept = AcceptArtifactVersionCmd(
                project_id=proposal.project_id,
                series_id=proposal.series_id,
                candidate_id=proposal.candidate_id,
                decision_ref=report_id,
            )
            return (
                ProposedCommand(
                    reaction_name="accept",
                    command_key=f"accept:{report_id}:{proposal.candidate_id}",
                    target=identity.series_stream(proposal.series_id),
                    payload=accept,
                ),
            )
        if payload.verdict in (GateVerdict.REWORK, GateVerdict.BLOCK):
            reject = RejectArtifactVersionCmd(
                project_id=proposal.project_id,
                series_id=proposal.series_id,
                candidate_id=proposal.candidate_id,
                report_ref=report_id,
                reason=payload.feedback or payload.verdict.value,
            )
            return (
                ProposedCommand(
                    reaction_name="reject",
                    command_key=f"reject:{report_id}:{proposal.candidate_id}",
                    target=identity.series_stream(proposal.series_id),
                    payload=reject,
                ),
            )
        return ()

    def _apply_stage(
        self, state: GateDecisionState, spec: QCLayerSpec, payload: GateDecidedEvt
    ) -> tuple[ProposedCommand[GateDecisionCommand], ...]:
        """阶段层:REWORK 按返工范围逐分区撤销已接受主体;PASS/BLOCK 不动产物。"""
        if payload.verdict is not GateVerdict.REWORK:
            return ()
        report_id = payload.report_ref.artifact_id
        commands: list[ProposedCommand[GateDecisionCommand]] = []
        for partition in payload.rework_scope:
            acc = state.accepted_of(spec.subject_output_key, partition)
            if acc is None:
                continue  # 该分区无当前已接受主体(可能已被撤销)
            revoke = RevokeArtifactAcceptanceCmd(
                project_id=acc.project_id,
                series_id=acc.series_id,
                artifact_ref=acc.ref,
                report_ref=report_id,
                reason=payload.feedback or "stage_rework",
            )
            commands.append(
                ProposedCommand(
                    reaction_name=f"revoke:{partition}",
                    command_key=f"revoke:{report_id}:{acc.ref.artifact_id}",
                    target=identity.series_stream(acc.series_id),
                    payload=revoke,
                )
            )
        return tuple(commands)
