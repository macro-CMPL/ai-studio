"""ProjectDecider(project 流):协调 —— 初始化与 StageExpanded。"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from studio.kernel.decisions import Accepted, ProposedEvent, Rejected

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
            if command.stage_id in state.expanded:
                return Rejected("already_expanded", f"stage {command.stage_id} 已展开")
            return Accepted(
                (
                    ProposedEvent(
                        f"stage-expanded:{command.stage_id}",
                        StageExpandedEvt(
                            project_id=command.project_id,
                            stage_id=command.stage_id,
                            driver_ref=command.driver_ref,
                            partitions=command.partitions,
                        ),
                    ),
                )
            )
        return Rejected("unexpected_command", f"project 流不处理 {command.type}")

    def evolve(self, state: ProjectState, event: ProductionEvent) -> ProjectState:
        if isinstance(event, PipelineInitializedEvt):
            return state.model_copy(update={"initialized": True})
        if isinstance(event, StageExpandedEvt):
            return state.model_copy(
                update={"expanded": (*state.expanded, event.stage_id)}
            )
        return state
