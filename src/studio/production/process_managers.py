"""M3 四个 Process Manager:Publish / Expansion / Lineage / Recompute。

全部纯函数 react;协调靠稳定身份(task_key/attempt_id)+ 幂等,而非合并 PM。
标脏(LineagePM)与排重算(RecomputePM)严格分离,中间隔一条显式 stale 事实。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from studio.domain import ids as domain_ids
from studio.domain.artifacts import ArtifactRef, StoryboardPayload
from studio.domain.enums import AcceptanceMode, PropagationMode
from studio.kernel.envelopes import EventEnvelope
from studio.kernel.process_manager import ProposedCommand, Reaction

from . import identity
from .payloads import (
    ArtifactCandidateProducedEvt,
    ArtifactMarkedStaleEvt,
    ArtifactVersionAcceptedEvt,
    CreateTaskAttemptCmd,
    ExpandStageCmd,
    MarkArtifactStaleCmd,
    PipelineInitializedEvt,
    ProductionCommand,
    ProductionEvent,
    ProposeArtifactVersionCmd,
    StageExpandedEvt,
    TaskAttemptCreatedEvt,
    TaskInputsBoundEvt,
)
from .values import BindingItem

_STORYBOARD = "storyboard"
_PLAN = "plan"
_IMAGE = "image"


def _frozen() -> ConfigDict:
    return ConfigDict(frozen=True)


# --------------------------------------------------------------------------- #
# PublishPM:candidate -> ProposeArtifactVersion
# --------------------------------------------------------------------------- #


class PublishState(BaseModel):
    model_config = _frozen()
    published: int = 0


class PublishProcessManager:
    pm_id = "publish-pm"

    def initial_state(self) -> PublishState:
        return PublishState()

    def react(
        self, state: PublishState, event: EventEnvelope[ProductionEvent]
    ) -> Reaction[PublishState, ProductionCommand]:
        payload = event.payload
        if not isinstance(payload, ArtifactCandidateProducedEvt):
            return Reaction(state=state, commands=())
        cmd = ProposeArtifactVersionCmd(
            series_id=payload.series_id,
            candidate_id=payload.candidate_id,
            output_key=payload.output_key,
            partition_key=payload.partition_key,
            digest=payload.digest,
            payload=payload.payload,
            acceptance_mode=AcceptanceMode.AUTO,
            produced_by_attempt=payload.attempt_id,
        )
        return Reaction(
            state=state.model_copy(update={"published": state.published + 1}),
            commands=(
                ProposedCommand(
                    reaction_name="publish",
                    command_key=payload.candidate_id,
                    target=identity.series_stream(payload.series_id),
                    payload=cmd,
                ),
            ),
        )


# --------------------------------------------------------------------------- #
# ExpansionPM:实例图从无到有(bootstrap + fan-out + 1:1 展开)
# --------------------------------------------------------------------------- #


class AcceptedRefEntry(BaseModel):
    model_config = _frozen()
    output_key: str
    partition_key: str | None
    ref: ArtifactRef


class ExpansionState(BaseModel):
    model_config = _frozen()
    project_id: str | None = None
    shots: tuple[str, ...] = ()
    accepted: tuple[AcceptedRefEntry, ...] = ()

    def find_ref(self, output_key: str, partition_key: str | None) -> ArtifactRef | None:
        for e in self.accepted:
            if e.output_key == output_key and e.partition_key == partition_key:
                return e.ref
        return None


def _create_attempt_cmd(
    *,
    project_id: str,
    stage_id: str,
    partition_key: str | None,
    output_key: str,
    bindings: tuple[BindingItem, ...],
) -> tuple[str, CreateTaskAttemptCmd]:
    tk = identity.task_key(project_id, stage_id, partition_key)
    aid = identity.attempt_id(tk, identity.input_binding_digest(bindings), 0)
    series = domain_ids.series_id(project_id, output_key, partition_key)
    cmd = CreateTaskAttemptCmd(
        attempt_id=aid,
        project_id=project_id,
        stage_id=stage_id,
        partition_key=partition_key,
        output_key=output_key,
        series_id=series,
        exact_refs=bindings,
    )
    return aid, cmd


class ExpansionProcessManager:
    pm_id = "expansion-pm"

    def initial_state(self) -> ExpansionState:
        return ExpansionState()

    def react(
        self, state: ExpansionState, event: EventEnvelope[ProductionEvent]
    ) -> Reaction[ExpansionState, ProductionCommand]:
        payload = event.payload
        if isinstance(payload, PipelineInitializedEvt):
            return self._bootstrap(state, payload)
        if isinstance(payload, ArtifactCandidateProducedEvt):
            return self._cache_shots(state, payload)
        if isinstance(payload, ArtifactVersionAcceptedEvt):
            return self._on_accepted(state, payload)
        if isinstance(payload, StageExpandedEvt):
            return self._on_stage_expanded(state, payload)
        return Reaction(state=state, commands=())

    def _bootstrap(
        self, state: ExpansionState, payload: PipelineInitializedEvt
    ) -> Reaction[ExpansionState, ProductionCommand]:
        _, cmd = _create_attempt_cmd(
            project_id=payload.project_id,
            stage_id=_STORYBOARD,
            partition_key=None,
            output_key=_STORYBOARD,
            bindings=(),
        )
        return Reaction(
            state=state.model_copy(update={"project_id": payload.project_id}),
            commands=(
                ProposedCommand(
                    reaction_name="bootstrap-storyboard",
                    command_key=f"create:{cmd.attempt_id}",
                    target=identity.attempt_stream(cmd.attempt_id),
                    payload=cmd,
                ),
            ),
        )

    def _cache_shots(
        self, state: ExpansionState, payload: ArtifactCandidateProducedEvt
    ) -> Reaction[ExpansionState, ProductionCommand]:
        if payload.output_key == _STORYBOARD and isinstance(
            payload.payload, StoryboardPayload
        ):
            shots = tuple(sorted(s.shot_id for s in payload.payload.shots))
            return Reaction(state=state.model_copy(update={"shots": shots}), commands=())
        return Reaction(state=state, commands=())

    def _on_accepted(
        self, state: ExpansionState, payload: ArtifactVersionAcceptedEvt
    ) -> Reaction[ExpansionState, ProductionCommand]:
        entry = AcceptedRefEntry(
            output_key=payload.output_key,
            partition_key=payload.partition_key,
            ref=payload.artifact_ref,
        )
        new_state = state.model_copy(update={"accepted": (*state.accepted, entry)})
        project = new_state.project_id
        if project is None:
            return Reaction(state=new_state, commands=())

        if payload.output_key == _STORYBOARD:
            cmd = ExpandStageCmd(
                project_id=project,
                stage_id=_PLAN,
                driver_ref=payload.artifact_ref,
                partitions=new_state.shots,
            )
            return Reaction(
                state=new_state,
                commands=(
                    ProposedCommand(
                        reaction_name="expand-plan",
                        command_key="expand:plan",
                        target=identity.project_stream(project),
                        payload=cmd,
                    ),
                ),
            )
        if payload.output_key == _PLAN and payload.partition_key is not None:
            binding = BindingItem.from_ref(
                requirement_key=_PLAN,
                logical_slot=_PLAN,
                partition_key=payload.partition_key,
                ref=payload.artifact_ref,
                propagation_mode=PropagationMode.PARTITION_PRESERVING,
            )
            aid, cmd2 = _create_attempt_cmd(
                project_id=project,
                stage_id=_IMAGE,
                partition_key=payload.partition_key,
                output_key=_IMAGE,
                bindings=(binding,),
            )
            return Reaction(
                state=new_state,
                commands=(
                    ProposedCommand(
                        reaction_name="create-image",
                        command_key=f"create:{aid}",
                        target=identity.attempt_stream(aid),
                        payload=cmd2,
                    ),
                ),
            )
        return Reaction(state=new_state, commands=())

    def _on_stage_expanded(
        self, state: ExpansionState, payload: StageExpandedEvt
    ) -> Reaction[ExpansionState, ProductionCommand]:
        if payload.stage_id != _PLAN or state.project_id is None:
            return Reaction(state=state, commands=())
        storyboard_ref = state.find_ref(_STORYBOARD, None)
        if storyboard_ref is None:
            return Reaction(state=state, commands=())
        commands: list[ProposedCommand[ProductionCommand]] = []
        for partition in payload.partitions:
            binding = BindingItem.from_ref(
                requirement_key=_STORYBOARD,
                logical_slot=_STORYBOARD,
                partition_key=partition,
                ref=storyboard_ref,
                propagation_mode=PropagationMode.AGGREGATE,
            )
            aid, cmd = _create_attempt_cmd(
                project_id=state.project_id,
                stage_id=_PLAN,
                partition_key=partition,
                output_key=_PLAN,
                bindings=(binding,),
            )
            commands.append(
                ProposedCommand(
                    reaction_name="create-plan",
                    command_key=f"create:{aid}",
                    target=identity.attempt_stream(aid),
                    payload=cmd,
                )
            )
        return Reaction(state=state, commands=tuple(commands))


# --------------------------------------------------------------------------- #
# LineagePM:维护消费边,检测版本替换,确定性发 MarkArtifactStale
# --------------------------------------------------------------------------- #


class AttemptMeta(BaseModel):
    model_config = _frozen()
    attempt_id: str
    task_key: str
    partition_key: str | None


class AttemptInputs(BaseModel):
    model_config = _frozen()
    attempt_id: str
    bindings: tuple[BindingItem, ...]


class AttemptTarget(BaseModel):
    model_config = _frozen()
    attempt_id: str
    ref: ArtifactRef


class LineageState(BaseModel):
    model_config = _frozen()
    metas: tuple[AttemptMeta, ...] = ()
    inputs: tuple[AttemptInputs, ...] = ()
    targets: tuple[AttemptTarget, ...] = ()

    def meta_of(self, attempt_id: str) -> AttemptMeta | None:
        return next((m for m in self.metas if m.attempt_id == attempt_id), None)

    def inputs_of(self, attempt_id: str) -> tuple[BindingItem, ...]:
        found = next((i for i in self.inputs if i.attempt_id == attempt_id), None)
        return found.bindings if found is not None else ()

    def target_of(self, attempt_id: str) -> ArtifactRef | None:
        found = next((t for t in self.targets if t.attempt_id == attempt_id), None)
        return found.ref if found is not None else None


class LineageProcessManager:
    pm_id = "lineage-pm"

    def initial_state(self) -> LineageState:
        return LineageState()

    def react(
        self, state: LineageState, event: EventEnvelope[ProductionEvent]
    ) -> Reaction[LineageState, ProductionCommand]:
        payload = event.payload
        if isinstance(payload, TaskAttemptCreatedEvt):
            meta = AttemptMeta(
                attempt_id=payload.attempt_id,
                task_key=identity.task_key(
                    payload.project_id, payload.stage_id, payload.partition_key
                ),
                partition_key=payload.partition_key,
            )
            return Reaction(
                state=state.model_copy(update={"metas": (*state.metas, meta)}),
                commands=(),
            )
        if isinstance(payload, TaskInputsBoundEvt):
            entry = AttemptInputs(
                attempt_id=payload.attempt_id, bindings=payload.exact_refs
            )
            return Reaction(
                state=state.model_copy(update={"inputs": (*state.inputs, entry)}),
                commands=(),
            )
        if isinstance(payload, ArtifactVersionAcceptedEvt):
            return self._on_accepted(state, event, payload)
        return Reaction(state=state, commands=())

    def _on_accepted(
        self,
        state: LineageState,
        event: EventEnvelope[ProductionEvent],
        payload: ArtifactVersionAcceptedEvt,
    ) -> Reaction[LineageState, ProductionCommand]:
        target_entry = AttemptTarget(
            attempt_id=payload.produced_by_attempt, ref=payload.artifact_ref
        )
        new_state = state.model_copy(
            update={"targets": (*state.targets, target_entry)}
        )
        if payload.previous_current_ref is None:
            return Reaction(state=new_state, commands=())

        invalidated = payload.previous_current_ref
        commands: list[ProposedCommand[ProductionCommand]] = []
        for inputs in new_state.inputs:
            binding = next(
                (
                    b
                    for b in inputs.bindings
                    if b.artifact_id == invalidated.artifact_id
                ),
                None,
            )
            if binding is None:
                continue
            target_ref = new_state.target_of(inputs.attempt_id)
            meta = new_state.meta_of(inputs.attempt_id)
            if target_ref is None or meta is None:
                continue
            cmd = MarkArtifactStaleCmd(
                target_ref=target_ref,
                invalidated_input_ref=invalidated,
                replacement_ref=payload.artifact_ref,
                root_cause_event_id=event.event_id,
                scope=binding.propagation_mode,
                task_key=meta.task_key,
                partition_key=meta.partition_key,
            )
            commands.append(
                ProposedCommand(
                    reaction_name="mark-stale",
                    command_key=f"{target_ref.artifact_id}:{invalidated.artifact_id}",
                    target=identity.series_stream(target_ref.series_id),
                    payload=cmd,
                )
            )
        return Reaction(state=new_state, commands=tuple(commands))


# --------------------------------------------------------------------------- #
# RecomputePM:消费 stale 事实,决定创建新 Attempt(新绑定 -> 新 attempt_id)
# --------------------------------------------------------------------------- #


class RecomputeMeta(BaseModel):
    model_config = _frozen()
    attempt_id: str
    project_id: str
    stage_id: str
    partition_key: str | None
    output_key: str
    series_id: str


class RecomputeState(BaseModel):
    model_config = _frozen()
    metas: tuple[RecomputeMeta, ...] = ()
    inputs: tuple[AttemptInputs, ...] = ()
    target_to_attempt: tuple[tuple[str, str], ...] = ()  # (target_artifact_id, attempt)

    def meta_of(self, attempt_id: str) -> RecomputeMeta | None:
        return next((m for m in self.metas if m.attempt_id == attempt_id), None)

    def inputs_of(self, attempt_id: str) -> tuple[BindingItem, ...]:
        found = next((i for i in self.inputs if i.attempt_id == attempt_id), None)
        return found.bindings if found is not None else ()

    def attempt_for_target(self, target_artifact_id: str) -> str | None:
        return next(
            (a for (t, a) in self.target_to_attempt if t == target_artifact_id), None
        )


class RecomputeProcessManager:
    pm_id = "recompute-pm"

    def initial_state(self) -> RecomputeState:
        return RecomputeState()

    def react(
        self, state: RecomputeState, event: EventEnvelope[ProductionEvent]
    ) -> Reaction[RecomputeState, ProductionCommand]:
        payload = event.payload
        if isinstance(payload, TaskAttemptCreatedEvt):
            meta = RecomputeMeta(
                attempt_id=payload.attempt_id,
                project_id=payload.project_id,
                stage_id=payload.stage_id,
                partition_key=payload.partition_key,
                output_key=payload.output_key,
                series_id=payload.series_id,
            )
            return Reaction(
                state=state.model_copy(update={"metas": (*state.metas, meta)}),
                commands=(),
            )
        if isinstance(payload, TaskInputsBoundEvt):
            entry = AttemptInputs(
                attempt_id=payload.attempt_id, bindings=payload.exact_refs
            )
            return Reaction(
                state=state.model_copy(update={"inputs": (*state.inputs, entry)}),
                commands=(),
            )
        if isinstance(payload, ArtifactVersionAcceptedEvt):
            mapping = (payload.artifact_ref.artifact_id, payload.produced_by_attempt)
            return Reaction(
                state=state.model_copy(
                    update={"target_to_attempt": (*state.target_to_attempt, mapping)}
                ),
                commands=(),
            )
        if isinstance(payload, ArtifactMarkedStaleEvt):
            return self._on_stale(state, payload)
        return Reaction(state=state, commands=())

    def _on_stale(
        self, state: RecomputeState, payload: ArtifactMarkedStaleEvt
    ) -> Reaction[RecomputeState, ProductionCommand]:
        attempt = state.attempt_for_target(payload.target_ref.artifact_id)
        if attempt is None:
            return Reaction(state=state, commands=())
        meta = state.meta_of(attempt)
        bindings = state.inputs_of(attempt)
        if meta is None or not bindings:
            return Reaction(state=state, commands=())

        new_bindings = tuple(
            BindingItem.from_ref(
                requirement_key=b.requirement_key,
                logical_slot=b.logical_slot,
                partition_key=b.partition_key,
                ref=payload.replacement_ref,
                propagation_mode=b.propagation_mode,
            )
            if b.artifact_id == payload.invalidated_input_ref.artifact_id
            else b
            for b in bindings
        )
        tk = identity.task_key(meta.project_id, meta.stage_id, meta.partition_key)
        aid = identity.attempt_id(tk, identity.input_binding_digest(new_bindings), 0)
        cmd = CreateTaskAttemptCmd(
            attempt_id=aid,
            project_id=meta.project_id,
            stage_id=meta.stage_id,
            partition_key=meta.partition_key,
            output_key=meta.output_key,
            series_id=meta.series_id,
            exact_refs=new_bindings,
        )
        return Reaction(
            state=state,
            commands=(
                ProposedCommand(
                    reaction_name="recompute",
                    command_key=f"recompute:{payload.target_ref.artifact_id}",
                    target=identity.attempt_stream(aid),
                    payload=cmd,
                ),
            ),
        )
