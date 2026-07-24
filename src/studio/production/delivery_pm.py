"""交付管理器:阶段质检通过后,创建交付任务(一次)。

分工(锁定):
- 仅在阶段质检 PASS 后交付(阶段报告失效前的 REWORK/BLOCK 一律不交付)。
- 以全部当前已接受图像为交付输入(确定性转换 -> 交付包)。
- 交付只发生一次:按项目去重。
- 通过追踪阶段质检报告提议(output_key == stage_qc_stage_id)识别哪些 GateDecided
  属于阶段层,避免依赖 GateDecidedEvt 未携带的 output_key/project 字段。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from studio.domain import ids as domain_ids
from studio.domain.artifacts import ArtifactRef
from studio.domain.enums import GateVerdict, PropagationMode
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.kernel.process_manager import ProposedCommand, Reaction

from . import identity
from .gate import GateDecidedEvt
from .payloads import (
    ArtifactAcceptanceRevokedEvt,
    ArtifactVersionAcceptedEvt,
    ArtifactVersionProposedEvt,
    CreateTaskAttemptCmd,
)
from .values import BindingItem


class AcceptedImage(BaseModel):
    model_config = ConfigDict(frozen=True)
    partition_key: str
    ref: ArtifactRef


class StageReport(BaseModel):
    model_config = ConfigDict(frozen=True)
    report_id: str
    project_id: str


class DeliveryState(BaseModel):
    model_config = ConfigDict(frozen=True)
    accepted: tuple[AcceptedImage, ...] = ()
    stage_reports: tuple[StageReport, ...] = ()
    delivered: tuple[str, ...] = ()  # 已交付项目(去重)

    def current_of(self, partition: str) -> ArtifactRef | None:
        return next(
            (a.ref for a in self.accepted if a.partition_key == partition), None
        )

    def with_accepted(self, partition: str, ref: ArtifactRef) -> tuple[AcceptedImage, ...]:
        others = tuple(a for a in self.accepted if a.partition_key != partition)
        return (*others, AcceptedImage(partition_key=partition, ref=ref))

    def project_of_report(self, report_id: str) -> str | None:
        return next(
            (r.project_id for r in self.stage_reports if r.report_id == report_id), None
        )


class DeliveryProcessManager:
    pm_id = "delivery-pm"

    def __init__(
        self,
        *,
        subject_output_key: str,
        subject_requirement_key: str,
        stage_qc_stage_id: str,
        delivery_stage_id: str,
    ) -> None:
        self._subject_output_key = subject_output_key
        self._subject_requirement_key = subject_requirement_key
        self._stage_qc_stage_id = stage_qc_stage_id
        self._delivery_stage_id = delivery_stage_id

    def initial_state(self) -> DeliveryState:
        return DeliveryState()

    def react(
        self, state: DeliveryState, event: EventEnvelope[MessagePayload]
    ) -> Reaction[DeliveryState, CreateTaskAttemptCmd]:
        payload = event.payload
        if isinstance(payload, ArtifactVersionProposedEvt):
            if payload.output_key != self._stage_qc_stage_id:
                return Reaction(state=state, commands=())
            entry = StageReport(
                report_id=payload.artifact_ref.artifact_id,
                project_id=payload.project_id,
            )
            return Reaction(
                state=state.model_copy(
                    update={"stage_reports": (*state.stage_reports, entry)}
                ),
                commands=(),
            )
        if isinstance(payload, ArtifactVersionAcceptedEvt):
            if (
                payload.output_key != self._subject_output_key
                or payload.partition_key is None
            ):
                return Reaction(state=state, commands=())
            return Reaction(
                state=state.model_copy(
                    update={
                        "accepted": state.with_accepted(
                            payload.partition_key, payload.artifact_ref
                        )
                    }
                ),
                commands=(),
            )
        if isinstance(payload, ArtifactAcceptanceRevokedEvt):
            partition = _partition_of(state, payload.artifact_ref)
            if partition is None:
                return Reaction(state=state, commands=())
            remaining = tuple(
                a for a in state.accepted if a.partition_key != partition
            )
            return Reaction(
                state=state.model_copy(update={"accepted": remaining}), commands=()
            )
        if isinstance(payload, GateDecidedEvt):
            return self._on_gate_decided(state, payload)
        return Reaction(state=state, commands=())

    def _on_gate_decided(
        self, state: DeliveryState, payload: GateDecidedEvt
    ) -> Reaction[DeliveryState, CreateTaskAttemptCmd]:
        if payload.verdict is not GateVerdict.PASS:
            return Reaction(state=state, commands=())
        report_id = payload.report_ref.artifact_id
        project_id = state.project_of_report(report_id)
        if project_id is None:
            return Reaction(state=state, commands=())  # 非阶段层报告
        if project_id in state.delivered:
            return Reaction(state=state, commands=())  # 幂等:只交付一次
        if not state.accepted:
            return Reaction(state=state, commands=())  # 无可交付图像(不应发生)

        bindings = tuple(
            BindingItem.from_ref(
                requirement_key=self._subject_requirement_key,
                logical_slot=self._subject_output_key,
                partition_key=img.partition_key,
                ref=img.ref,
                propagation_mode=PropagationMode.AGGREGATE,
            )
            for img in sorted(state.accepted, key=lambda a: a.partition_key)
        )
        tk = identity.task_key(project_id, self._delivery_stage_id, None)
        aid = identity.attempt_id(tk, identity.input_binding_digest(bindings), 0)
        series = domain_ids.series_id(project_id, self._delivery_stage_id, None)
        cmd = CreateTaskAttemptCmd(
            attempt_id=aid, project_id=project_id, stage_id=self._delivery_stage_id,
            partition_key=None, output_key=self._delivery_stage_id, series_id=series,
            exact_refs=bindings,
        )
        return Reaction(
            state=state.model_copy(update={"delivered": (*state.delivered, project_id)}),
            commands=(
                ProposedCommand(
                    reaction_name="deliver",
                    command_key=f"deliver:{report_id}",
                    target=identity.attempt_stream(aid),
                    payload=cmd,
                ),
            ),
        )


def _partition_of(state: DeliveryState, ref: ArtifactRef) -> str | None:
    return next((a.partition_key for a in state.accepted if a.ref == ref), None)
