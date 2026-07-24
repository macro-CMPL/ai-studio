"""阶段汇合管理器:所有镜头结果通过后,聚合发起阶段质检;返工后按新图像集重跑。

分工(锁定):
- 等待"预期分区全部就绪"(每个分区都有当前已接受的图像)后,创建阶段质检任务,
  以全部当前图像为聚合输入。
- 预期分区集来自上游扇出阶段的 StageExpanded 事实(一次给全,避免分区未齐就提前触发)。
- 撤销某分区的已接受图像 -> 该分区暂不完整 -> 不触发;返工后新图像被接受 ->
  当前图像集变化 -> 触发新一轮阶段质检(新绑定 -> 新任务身份 -> 新阶段报告版本)。
- 用已触发的图像集指纹去重,避免同一图像集重复发起阶段质检。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from studio.domain import ids as domain_ids
from studio.domain.artifacts import ArtifactRef
from studio.domain.enums import PropagationMode
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.kernel.process_manager import ProposedCommand, Reaction
from studio.serialization import digest

from . import identity
from .payloads import (
    ArtifactAcceptanceRevokedEvt,
    ArtifactVersionAcceptedEvt,
    CreateTaskAttemptCmd,
    StageExpandedEvt,
)
from .values import BindingItem


class AcceptedImage(BaseModel):
    model_config = ConfigDict(frozen=True)
    partition_key: str
    ref: ArtifactRef


class ProjectConfluence(BaseModel):
    model_config = ConfigDict(frozen=True)
    project_id: str
    expected: tuple[str, ...] = ()  # 预期分区(来自 StageExpanded)
    accepted: tuple[AcceptedImage, ...] = ()  # 当前已接受图像(按分区)
    triggered: tuple[str, ...] = ()  # 已触发的图像集指纹(幂等/去重)

    def current_of(self, partition: str) -> ArtifactRef | None:
        return next(
            (a.ref for a in self.accepted if a.partition_key == partition), None
        )

    def with_accepted(self, partition: str, ref: ArtifactRef) -> tuple[AcceptedImage, ...]:
        others = tuple(a for a in self.accepted if a.partition_key != partition)
        return (*others, AcceptedImage(partition_key=partition, ref=ref))

    def without_accepted(self, partition: str) -> tuple[AcceptedImage, ...]:
        return tuple(a for a in self.accepted if a.partition_key != partition)


class ConfluenceState(BaseModel):
    model_config = ConfigDict(frozen=True)
    projects: tuple[ProjectConfluence, ...] = ()

    def project(self, project_id: str) -> ProjectConfluence:
        return next(
            (p for p in self.projects if p.project_id == project_id),
            ProjectConfluence(project_id=project_id),
        )

    def with_project(self, updated: ProjectConfluence) -> ConfluenceState:
        others = tuple(p for p in self.projects if p.project_id != updated.project_id)
        return self.model_copy(update={"projects": (*others, updated)})


class StageConfluenceProcessManager:
    """subject_output_key 的所有预期分区就绪后,发起 stage_qc 聚合质检。

    - expected_from_stage:定义预期分区集的上游扇出阶段(如 plan)。
    - subject_output_key:被聚合的逐分区产物(如 image)。
    - stage_qc_stage_id:阶段质检报告的 stage(如 stage_qc)。
    """

    pm_id = "stage-confluence-pm"

    def __init__(
        self,
        *,
        expected_from_stage: str,
        subject_output_key: str,
        subject_requirement_key: str,
        stage_qc_stage_id: str,
    ) -> None:
        self._expected_from_stage = expected_from_stage
        self._subject_output_key = subject_output_key
        self._subject_requirement_key = subject_requirement_key
        self._stage_qc_stage_id = stage_qc_stage_id

    def initial_state(self) -> ConfluenceState:
        return ConfluenceState()

    def react(
        self, state: ConfluenceState, event: EventEnvelope[MessagePayload]
    ) -> Reaction[ConfluenceState, CreateTaskAttemptCmd]:
        payload = event.payload
        if isinstance(payload, StageExpandedEvt):
            if payload.stage_id != self._expected_from_stage:
                return Reaction(state=state, commands=())
            proj = state.project(payload.project_id).model_copy(
                update={"expected": tuple(sorted(set(payload.partitions)))}
            )
            return self._maybe_trigger(state.with_project(proj), payload.project_id)
        if isinstance(payload, ArtifactVersionAcceptedEvt):
            if (
                payload.output_key != self._subject_output_key
                or payload.partition_key is None
            ):
                return Reaction(state=state, commands=())
            proj = state.project(payload.project_id)
            proj = proj.model_copy(
                update={
                    "accepted": proj.with_accepted(
                        payload.partition_key, payload.artifact_ref
                    )
                }
            )
            return self._maybe_trigger(state.with_project(proj), payload.project_id)
        if isinstance(payload, ArtifactAcceptanceRevokedEvt):
            proj = state.project(payload.project_id)
            partition = _partition_of(proj, payload.artifact_ref)
            if partition is None:
                return Reaction(state=state, commands=())
            proj = proj.model_copy(
                update={"accepted": proj.without_accepted(partition)}
            )
            return Reaction(state=state.with_project(proj), commands=())
        return Reaction(state=state, commands=())

    def _maybe_trigger(
        self, state: ConfluenceState, project_id: str
    ) -> Reaction[ConfluenceState, CreateTaskAttemptCmd]:
        proj = state.project(project_id)
        if not proj.expected:
            return Reaction(state=state, commands=())
        refs: list[tuple[str, ArtifactRef]] = []
        for partition in proj.expected:
            ref = proj.current_of(partition)
            if ref is None:
                return Reaction(state=state, commands=())  # 尚未全部就绪
            refs.append((partition, ref))
        fingerprint = digest([r.artifact_id for _, r in sorted(refs)])
        if fingerprint in proj.triggered:
            return Reaction(state=state, commands=())  # 该图像集已触发

        bindings = tuple(
            BindingItem.from_ref(
                requirement_key=self._subject_requirement_key,
                logical_slot=self._subject_output_key,
                partition_key=partition,
                ref=ref,
                propagation_mode=PropagationMode.AGGREGATE,
            )
            for partition, ref in sorted(refs)
        )
        tk = identity.task_key(project_id, self._stage_qc_stage_id, None)
        aid = identity.attempt_id(tk, identity.input_binding_digest(bindings), 0)
        series = domain_ids.series_id(project_id, self._stage_qc_stage_id, None)
        cmd = CreateTaskAttemptCmd(
            attempt_id=aid, project_id=project_id, stage_id=self._stage_qc_stage_id,
            partition_key=None, output_key=self._stage_qc_stage_id, series_id=series,
            exact_refs=bindings,
        )
        updated = proj.model_copy(update={"triggered": (*proj.triggered, fingerprint)})
        return Reaction(
            state=state.with_project(updated),
            commands=(
                ProposedCommand(
                    reaction_name="stage-qc",
                    command_key=f"stage-qc:{aid}",
                    target=identity.attempt_stream(aid),
                    payload=cmd,
                ),
            ),
        )


def _partition_of(proj: ProjectConfluence, ref: ArtifactRef) -> str | None:
    return next(
        (a.partition_key for a in proj.accepted if a.ref == ref), None
    )
