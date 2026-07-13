"""ProjectDecider(project 流):协调 —— 初始化与 StageExpanded。"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from studio.kernel.decisions import Accepted, ProposedEvent, Rejected

from . import identity
from .payloads import (
    ExpandStageCmd,
    InitializePipelineCmd,
    PipelineInitializedEvt,
    ProductionCommand,
    ProductionEvent,
    StageExpandedEvt,
)


class ProjectState(BaseModel):
    model_config = ConfigDict(frozen=True)

    initialized: bool = False
    expanded: tuple[str, ...] = ()


class ProjectDecider:
    def initial_state(self) -> ProjectState:
        return ProjectState()

    def decide(
        self, state: ProjectState, command: ProductionCommand
    ) -> Accepted[ProductionEvent] | Rejected:
        if isinstance(command, InitializePipelineCmd):
            if state.initialized:
                return Rejected("already_initialized", "流水线已初始化")
            return Accepted(
                (
                    ProposedEvent(
                        "pipeline-initialized",
                        PipelineInitializedEvt(
                            project_id=command.project_id,
                            pipeline_spec_id=command.pipeline_spec_id,
                        ),
                    ),
                )
            )
        if isinstance(command, ExpandStageCmd):
            if not state.initialized:
                return Rejected("not_initialized", "流水线未初始化")
            parts = command.partitions
            if list(parts) != sorted(set(parts)):
                return Rejected("bad_partitions", "partitions 必须排序且唯一")
            expected_keys = tuple(
                sorted(
                    identity.task_key(command.project_id, command.stage_id, p)
                    for p in parts
                )
            )
            if tuple(sorted(command.task_keys)) != expected_keys:
                return Rejected("bad_task_keys", "task_keys 与派生不一致")
            key = _expansion_key(command.stage_id, command.driver_ref.artifact_id)
            if key in state.expanded:
                return Rejected("already_expanded", f"{key} 已展开")
            return Accepted(
                (
                    ProposedEvent(
                        f"stage-expanded:{key}",
                        StageExpandedEvt(
                            project_id=command.project_id,
                            stage_id=command.stage_id,
                            driver_ref=command.driver_ref,
                            partitions=command.partitions,
                            task_keys=command.task_keys,
                        ),
                    ),
                )
            )
        return Rejected("unexpected_command", f"project 流不处理 {command.type}")

    def evolve(self, state: ProjectState, event: ProductionEvent) -> ProjectState:
        if isinstance(event, PipelineInitializedEvt):
            return state.model_copy(update={"initialized": True})
        if isinstance(event, StageExpandedEvt):
            key = _expansion_key(event.stage_id, event.driver_ref.artifact_id)
            return state.model_copy(update={"expanded": (*state.expanded, key)})
        return state


def _expansion_key(stage_id: str, driver_artifact_id: str) -> str:
    return f"{stage_id}:{driver_artifact_id}"
