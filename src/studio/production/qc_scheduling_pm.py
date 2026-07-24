"""质量评价调度管理器:被检查产物就绪后,创建对应的质量检查任务(提示词/结果层)。

职责(锁定):
- 提示词质检:plan(提示词)提议后创建 prompt_qc 任务,检查通过才允许调用付费出图。
- 结果质检:image(镜头图像)提议后创建 result_qc 任务,通过后该图像版本才被接受。
- 阶段质检不在此调度(等待所有镜头通过后由阶段汇合管理器启动)。

被检查产物按"提议(proposed)"触发,而非"接受(accepted)":提示词/图像均为门控产物,
先提议、经质检+闸门后才接受。质检本身是对提议版本的评价,不算下游"提前消费"。
每个 subject 版本(按 artifact_id 区分)只调度一次;返工产生新版本(新 artifact_id)
时会自然地再次调度一份质检。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from studio.domain import ids as domain_ids
from studio.domain.enums import PropagationMode
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.kernel.process_manager import ProposedCommand, Reaction

from . import identity
from .payloads import ArtifactVersionProposedEvt, CreateTaskAttemptCmd
from .quality import QCLayer, QualityConfig
from .values import BindingItem


class QCSchedulingState(BaseModel):
    model_config = ConfigDict(frozen=True)
    scheduled: tuple[str, ...] = ()  # 已排质检的 subject artifact_id(幂等)

    def is_scheduled(self, subject_artifact_id: str) -> bool:
        return subject_artifact_id in self.scheduled


class QCEvaluationSchedulingProcessManager:
    pm_id = "qc-scheduling-pm"

    def __init__(self, config: QualityConfig) -> None:
        self._config = config

    def initial_state(self) -> QCSchedulingState:
        return QCSchedulingState()

    def react(
        self, state: QCSchedulingState, event: EventEnvelope[MessagePayload]
    ) -> Reaction[QCSchedulingState, CreateTaskAttemptCmd]:
        payload = event.payload
        if not isinstance(payload, ArtifactVersionProposedEvt):
            return Reaction(state=state, commands=())
        # 仅逐分区层(提示词/结果)在此调度;阶段层由阶段汇合管理器启动。
        # 显式排除阶段层并与 layers 顺序无关(阶段层的 subject 亦可能是 image)。
        layer = next(
            (
                s
                for s in self._config.layers
                if s.subject_output_key == payload.output_key
                and s.layer is not QCLayer.STAGE
            ),
            None,
        )
        if layer is None:
            return Reaction(state=state, commands=())
        subject_id = payload.artifact_ref.artifact_id
        if state.is_scheduled(subject_id):
            return Reaction(state=state, commands=())  # 幂等:同一版本只查一次

        binding = BindingItem.from_ref(
            requirement_key=layer.subject_requirement_key,
            logical_slot=payload.output_key,
            partition_key=payload.partition_key,
            ref=payload.artifact_ref,
            propagation_mode=PropagationMode.PARTITION_PRESERVING,
        )
        bindings = (binding,)
        tk = identity.task_key(
            payload.project_id, layer.qc_stage_id, payload.partition_key
        )
        aid = identity.attempt_id(tk, identity.input_binding_digest(bindings), 0)
        series = domain_ids.series_id(
            payload.project_id, layer.qc_stage_id, payload.partition_key
        )
        cmd = CreateTaskAttemptCmd(
            attempt_id=aid,
            project_id=payload.project_id,
            stage_id=layer.qc_stage_id,
            partition_key=payload.partition_key,
            output_key=layer.qc_stage_id,
            series_id=series,
            exact_refs=bindings,
        )
        return Reaction(
            state=state.model_copy(
                update={"scheduled": (*state.scheduled, subject_id)}
            ),
            commands=(
                ProposedCommand(
                    reaction_name=f"schedule-qc:{layer.layer.value}",
                    command_key=f"qc:{layer.qc_stage_id}:{subject_id}",
                    target=identity.attempt_stream(aid),
                    payload=cmd,
                ),
            ),
        )
