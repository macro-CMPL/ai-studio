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


ProductionCommand = (
    InitializePipelineCmd
    | ExpandStageCmd
    | CreateTaskAttemptCmd
    | ProposeArtifactVersionCmd
    | MarkArtifactStaleCmd
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


ProductionEvent = (
    PipelineInitializedEvt
    | StageExpandedEvt
    | TaskAttemptCreatedEvt
    | TaskInputsBoundEvt
    | ArtifactCandidateProducedEvt
    | ArtifactVersionProposedEvt
    | ArtifactVersionAcceptedEvt
    | ArtifactMarkedStaleEvt
)
