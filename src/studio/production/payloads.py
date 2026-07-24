"""M3 命令与事件 payload(判别联合)。"""

from __future__ import annotations

from typing import Literal

from pydantic import PositiveInt

from studio.domain.artifacts import ArtifactPayload, ArtifactRef
from studio.domain.enums import AcceptanceMode, PropagationMode
from studio.kernel.envelopes import MessagePayload

from .values import BindingItem

# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #


class InitializePipelineCmd(MessagePayload):
    type: Literal["initialize_pipeline"] = "initialize_pipeline"
    project_id: str
    pipeline_spec_id: str


class ExpandStageCmd(MessagePayload):
    type: Literal["expand_stage"] = "expand_stage"
    project_id: str
    stage_id: str
    driver_ref: ArtifactRef
    partitions: tuple[str, ...]
    task_keys: tuple[str, ...]


class CreateTaskAttemptCmd(MessagePayload):
    type: Literal["create_task_attempt"] = "create_task_attempt"
    attempt_id: str
    project_id: str
    stage_id: str
    partition_key: str | None
    output_key: str
    series_id: str
    exact_refs: tuple[BindingItem, ...]


class ProposeArtifactVersionCmd(MessagePayload):
    type: Literal["propose_artifact_version"] = "propose_artifact_version"
    project_id: str
    series_id: str
    candidate_id: str
    output_key: str
    partition_key: str | None
    digest: str
    payload: ArtifactPayload
    acceptance_mode: AcceptanceMode
    produced_by_attempt: str


class MarkArtifactStaleCmd(MessagePayload):
    type: Literal["mark_artifact_stale"] = "mark_artifact_stale"
    target_ref: ArtifactRef
    invalidated_input_ref: ArtifactRef
    replacement_ref: ArtifactRef
    root_cause_event_id: str
    scope: PropagationMode
    task_key: str
    partition_key: str | None


class AcceptArtifactVersionCmd(MessagePayload):
    """门控接受:由闸门决策发起,把某已提议候选正式接受(GATED 路径)。"""

    type: Literal["accept_artifact_version"] = "accept_artifact_version"
    project_id: str
    series_id: str
    candidate_id: str
    decision_ref: str  # 触发本次接受的闸门决策/报告引用(审计)


class RejectArtifactVersionCmd(MessagePayload):
    """拒绝:候选尚未接受,质检直接不通过(已提议 -> 已拒绝)。"""

    type: Literal["reject_artifact_version"] = "reject_artifact_version"
    project_id: str
    series_id: str
    candidate_id: str
    report_ref: str
    reason: str


class RevokeArtifactAcceptanceCmd(MessagePayload):
    """撤销:曾被接受的版本经阶段质检发现问题而撤销接受(已接受 -> 接受已撤销)。"""

    type: Literal["revoke_artifact_acceptance"] = "revoke_artifact_acceptance"
    project_id: str
    series_id: str
    artifact_ref: ArtifactRef
    report_ref: str
    reason: str


ProductionCommand = (
    InitializePipelineCmd
    | ExpandStageCmd
    | CreateTaskAttemptCmd
    | ProposeArtifactVersionCmd
    | MarkArtifactStaleCmd
    | AcceptArtifactVersionCmd
    | RejectArtifactVersionCmd
    | RevokeArtifactAcceptanceCmd
)


# --------------------------------------------------------------------------- #
# Events
# --------------------------------------------------------------------------- #


class PipelineInitializedEvt(MessagePayload):
    type: Literal["pipeline_initialized"] = "pipeline_initialized"
    project_id: str
    pipeline_spec_id: str


class StageExpandedEvt(MessagePayload):
    type: Literal["stage_expanded"] = "stage_expanded"
    project_id: str
    stage_id: str
    driver_ref: ArtifactRef
    partitions: tuple[str, ...]
    task_keys: tuple[str, ...]


class TaskAttemptCreatedEvt(MessagePayload):
    type: Literal["task_attempt_created"] = "task_attempt_created"
    attempt_id: str
    project_id: str
    stage_id: str
    partition_key: str | None
    output_key: str
    series_id: str


class TaskInputsBoundEvt(MessagePayload):
    type: Literal["task_inputs_bound"] = "task_inputs_bound"
    attempt_id: str
    exact_refs: tuple[BindingItem, ...]


class ArtifactCandidateProducedEvt(MessagePayload):
    type: Literal["artifact_candidate_produced"] = "artifact_candidate_produced"
    candidate_id: str
    attempt_id: str
    project_id: str
    series_id: str
    output_key: str
    partition_key: str | None
    digest: str
    payload: ArtifactPayload
    status_revision: int | None = None


class ArtifactVersionProposedEvt(MessagePayload):
    type: Literal["artifact_version_proposed"] = "artifact_version_proposed"
    project_id: str
    series_id: str
    revision: PositiveInt
    artifact_ref: ArtifactRef
    candidate_id: str
    produced_by_attempt: str
    output_key: str
    partition_key: str | None


class ArtifactVersionAcceptedEvt(MessagePayload):
    type: Literal["artifact_version_accepted"] = "artifact_version_accepted"
    project_id: str
    series_id: str
    revision: PositiveInt
    artifact_ref: ArtifactRef
    previous_current_ref: ArtifactRef | None
    candidate_id: str
    produced_by_attempt: str
    output_key: str
    partition_key: str | None


class ArtifactMarkedStaleEvt(MessagePayload):
    type: Literal["artifact_marked_stale"] = "artifact_marked_stale"
    target_ref: ArtifactRef
    invalidated_input_ref: ArtifactRef
    replacement_ref: ArtifactRef
    root_cause_event_id: str
    scope: PropagationMode
    task_key: str
    partition_key: str | None


class ArtifactVersionRejectedEvt(MessagePayload):
    type: Literal["artifact_version_rejected"] = "artifact_version_rejected"
    project_id: str
    series_id: str
    revision: PositiveInt
    artifact_ref: ArtifactRef
    candidate_id: str
    report_ref: str
    reason: str


class ArtifactAcceptanceRevokedEvt(MessagePayload):
    type: Literal["artifact_acceptance_revoked"] = "artifact_acceptance_revoked"
    project_id: str
    series_id: str
    revision: PositiveInt
    artifact_ref: ArtifactRef
    report_ref: str
    reason: str
    # 撤销后回退到的当前版本(同 series 剩余最高已接受未撤销版本;可能为空)
    new_current_ref: ArtifactRef | None


ProductionEvent = (
    PipelineInitializedEvt
    | StageExpandedEvt
    | TaskAttemptCreatedEvt
    | TaskInputsBoundEvt
    | ArtifactCandidateProducedEvt
    | ArtifactVersionProposedEvt
    | ArtifactVersionAcceptedEvt
    | ArtifactMarkedStaleEvt
    | ArtifactVersionRejectedEvt
    | ArtifactAcceptanceRevokedEvt
)
