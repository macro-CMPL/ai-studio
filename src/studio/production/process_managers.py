"""M3 四个 Process Manager:Publish / Expansion / Lineage / Recompute。

全部纯函数 react;协调靠稳定身份(task_key/attempt_id)+ 幂等,而非合并 PM。
标脏(LineagePM)与排重算(RecomputePM)严格分离,中间隔一条显式 stale 事实。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from studio.domain.artifacts import ArtifactRef
from studio.domain.enums import AcceptanceMode, PropagationMode
from studio.kernel.envelopes import EventEnvelope
from studio.kernel.process_manager import ProposedCommand, Reaction

from . import identity
from .compile import CompiledPipelineSpec, CompiledStage, StageMode
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
from .pipeline import PartitionSelector
from .values import BindingItem


def _frozen() -> ConfigDict:
    return ConfigDict(frozen=True)


def _create_attempt(
    *,
    project_id: str,
    stage: CompiledStage,
    partition_key: str | None,
    bindings: tuple[BindingItem, ...],
) -> tuple[str, CreateTaskAttemptCmd]:
    from studio.domain import ids as domain_ids

    tk = identity.task_key(project_id, stage.stage_id, partition_key)
    aid = identity.attempt_id(tk, identity.input_binding_digest(bindings), 0)
    series = domain_ids.series_id(project_id, stage.output_key, partition_key)
    cmd = CreateTaskAttemptCmd(
        attempt_id=aid,
        project_id=project_id,
        stage_id=stage.stage_id,
        partition_key=partition_key,
        output_key=stage.output_key,
        series_id=series,
        exact_refs=bindings,
    )
    return aid, cmd


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
            project_id=payload.project_id,
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
# ExpansionPM:按注入的 PipelineSpec 展开;状态按 project 隔离
# --------------------------------------------------------------------------- #


class AcceptedRefEntry(BaseModel):
    model_config = _frozen()
    output_key: str
    partition_key: str | None
    ref: ArtifactRef


class PartitionCacheEntry(BaseModel):
    model_config = _frozen()
    candidate_id: str
    stage_id: str
    partitions: tuple[str, ...]


class ProjectExpansion(BaseModel):
    model_config = _frozen()
    project_id: str
    partition_cache: tuple[PartitionCacheEntry, ...] = ()
    accepted: tuple[AcceptedRefEntry, ...] = ()
    activated: tuple[tuple[str, str], ...] = ()  # (stage_id, partition) 分区源已就绪
    created: tuple[tuple[str, str], ...] = ()  # (stage_id, partition) 已发起创建

    def partitions(self, candidate_id: str, stage_id: str) -> tuple[str, ...]:
        return next(
            (
                e.partitions
                for e in self.partition_cache
                if e.candidate_id == candidate_id and e.stage_id == stage_id
            ),
            (),
        )

    def resolve(self, output_key: str, partition: str | None) -> ArtifactRef | None:
        return next(
            (
                e.ref
                for e in self.accepted
                if e.output_key == output_key and e.partition_key == partition
            ),
            None,
        )

    def activated_partitions(self, stage_id: str) -> tuple[str, ...]:
        return tuple(p for (sid, p) in self.activated if sid == stage_id)

    def is_created(self, stage_id: str, partition: str) -> bool:
        return (stage_id, partition) in self.created


class ExpansionState(BaseModel):
    model_config = _frozen()
    projects: tuple[ProjectExpansion, ...] = ()

    def project(self, project_id: str) -> ProjectExpansion:
        return next(
            (p for p in self.projects if p.project_id == project_id),
            ProjectExpansion(project_id=project_id),
        )

    def with_project(self, updated: ProjectExpansion) -> ExpansionState:
        others = tuple(p for p in self.projects if p.project_id != updated.project_id)
        return self.model_copy(update={"projects": (*others, updated)})


class ExpansionProcessManager:
    """按 CompiledPipelineSpec 展开;内含 InputAssembler。

    齐备**所有** Requirement 后才创建 Attempt(多输入不丢)。
    """

    pm_id = "expansion-pm"

    def __init__(
        self,
        spec: CompiledPipelineSpec,
        selectors: dict[tuple[str, str], PartitionSelector],
    ) -> None:
        self._spec = spec
        self._selectors = selectors
        # fail fast:每个 FANOUT 阶段声明的 selector (id, version) 必须已注册。
        for stage in spec.stages:
            if stage.mode is StageMode.FANOUT and stage.partition_source is not None:
                key = (
                    stage.partition_source.partition_selector_id,
                    stage.partition_source.partition_selector_version,
                )
                if key[0] is None or key[1] is None or key not in selectors:
                    raise ValueError(f"stage {stage.stage_id} 的 selector {key} 未注册")

    def initial_state(self) -> ExpansionState:
        return ExpansionState()

    def react(
        self, state: ExpansionState, event: EventEnvelope[ProductionEvent]
    ) -> Reaction[ExpansionState, ProductionCommand]:
        payload = event.payload
        if isinstance(payload, PipelineInitializedEvt):
            return self._bootstrap(state, payload)
        if isinstance(payload, ArtifactCandidateProducedEvt):
            return self._cache_partitions(state, payload)
        if isinstance(payload, ArtifactVersionAcceptedEvt):
            return self._on_accepted(state, payload)
        if isinstance(payload, StageExpandedEvt):
            return self._on_stage_expanded(state, payload)
        return Reaction(state=state, commands=())

    def _bootstrap(
        self, state: ExpansionState, payload: PipelineInitializedEvt
    ) -> Reaction[ExpansionState, ProductionCommand]:
        if payload.pipeline_spec_id != self._spec.spec_id:
            return Reaction(state=state, commands=())
        commands: list[ProposedCommand[ProductionCommand]] = []
        for stage in self._spec.root_stages():
            aid, cmd = _create_attempt(
                project_id=payload.project_id, stage=stage, partition_key=None, bindings=()
            )
            commands.append(
                ProposedCommand(
                    reaction_name=f"bootstrap:{stage.stage_id}",
                    command_key=f"create:{aid}",
                    target=identity.attempt_stream(aid),
                    payload=cmd,
                )
            )
        return Reaction(
            state=state.with_project(state.project(payload.project_id)),
            commands=tuple(commands),
        )

    def _cache_partitions(
        self, state: ExpansionState, payload: ArtifactCandidateProducedEvt
    ) -> Reaction[ExpansionState, ProductionCommand]:
        fanouts = self._spec.fanout_driven_by(payload.output_key)
        if not fanouts:
            return Reaction(state=state, commands=())
        proj = state.project(payload.project_id)
        new_entries: list[PartitionCacheEntry] = []
        for stage in fanouts:
            req = stage.partition_source
            assert req is not None
            selector = self._selectors[
                (req.partition_selector_id, req.partition_selector_version)  # type: ignore[index]
            ]
            parts = selector(payload.payload)
            if parts:
                new_entries.append(
                    PartitionCacheEntry(
                        candidate_id=payload.candidate_id,
                        stage_id=stage.stage_id,
                        partitions=parts,
                    )
                )
        if not new_entries:
            return Reaction(state=state, commands=())
        updated = proj.model_copy(
            update={"partition_cache": (*proj.partition_cache, *new_entries)}
        )
        return Reaction(state=state.with_project(updated), commands=())

    def _on_accepted(
        self, state: ExpansionState, payload: ArtifactVersionAcceptedEvt
    ) -> Reaction[ExpansionState, ProductionCommand]:
        proj = state.project(payload.project_id)
        proj = proj.model_copy(
            update={
                "accepted": (
                    *proj.accepted,
                    AcceptedRefEntry(
                        output_key=payload.output_key,
                        partition_key=payload.partition_key,
                        ref=payload.artifact_ref,
                    ),
                )
            }
        )
        commands: list[ProposedCommand[ProductionCommand]] = []

        # FANOUT:本产物是扇出 driver -> 发 ExpandStage(两步展开)
        for stage in self._spec.fanout_driven_by(payload.output_key):
            partitions = tuple(
                sorted(set(proj.partitions(payload.candidate_id, stage.stage_id)))
            )
            task_keys = tuple(
                sorted(
                    identity.task_key(payload.project_id, stage.stage_id, p)
                    for p in partitions
                )
            )
            commands.append(
                ProposedCommand(
                    reaction_name=f"expand:{stage.stage_id}",
                    command_key=f"expand:{stage.stage_id}:{payload.artifact_ref.artifact_id}",
                    target=identity.project_stream(payload.project_id),
                    payload=ExpandStageCmd(
                        project_id=payload.project_id,
                        stage_id=stage.stage_id,
                        driver_ref=payload.artifact_ref,
                        partitions=partitions,
                        task_keys=task_keys,
                    ),
                )
            )

        # 输入装配:该产物可能完成某些下游 join
        for stage, req in self._spec.consumers_of(payload.output_key):
            if (
                stage.mode is StageMode.PER_PARTITION
                and stage.partition_source is not None
                and req.requirement_key == stage.partition_source.requirement_key
                and payload.partition_key is not None
            ):
                # 分区源到达:激活该分区并尝试装配
                proj = proj.model_copy(
                    update={
                        "activated": (
                            *proj.activated,
                            (stage.stage_id, payload.partition_key),
                        )
                    }
                )
                candidate_partitions: tuple[str, ...] = (payload.partition_key,)
            else:
                # 聚合/侧输入到达:重试所有已激活分区
                candidate_partitions = proj.activated_partitions(stage.stage_id)
            for p in candidate_partitions:
                proj, cmd = self._try_create(proj, payload.project_id, stage, p)
                if cmd is not None:
                    commands.append(cmd)

        return Reaction(state=state.with_project(proj), commands=tuple(commands))

    def _on_stage_expanded(
        self, state: ExpansionState, payload: StageExpandedEvt
    ) -> Reaction[ExpansionState, ProductionCommand]:
        stage = self._spec.by_stage(payload.stage_id)
        if stage is None or stage.mode is not StageMode.FANOUT:
            return Reaction(state=state, commands=())
        proj = state.project(payload.project_id)
        commands: list[ProposedCommand[ProductionCommand]] = []
        for partition in payload.partitions:
            proj = proj.model_copy(
                update={"activated": (*proj.activated, (stage.stage_id, partition))}
            )
            proj, cmd = self._try_create(proj, payload.project_id, stage, partition)
            if cmd is not None:
                commands.append(cmd)
        return Reaction(state=state.with_project(proj), commands=tuple(commands))

    def _try_create(
        self,
        proj: ProjectExpansion,
        project_id: str,
        stage: CompiledStage,
        partition: str,
    ) -> tuple[ProjectExpansion, ProposedCommand[ProductionCommand] | None]:
        if proj.is_created(stage.stage_id, partition):
            return proj, None
        bindings: list[BindingItem] = []
        for r in stage.requirements:
            is_source = (
                stage.partition_source is not None
                and r.requirement_key == stage.partition_source.requirement_key
            )
            if stage.mode is StageMode.FANOUT and is_source:
                ref = proj.resolve(r.logical_slot, None)  # driver 为聚合/singleton
            elif r.propagation_mode is PropagationMode.PARTITION_PRESERVING:
                ref = proj.resolve(r.logical_slot, partition)
            else:
                ref = proj.resolve(r.logical_slot, None)
            if ref is None:
                return proj, None  # 尚未齐备
            bindings.append(
                BindingItem.from_ref(
                    requirement_key=r.requirement_key,
                    logical_slot=r.logical_slot,
                    partition_key=partition,
                    ref=ref,
                    propagation_mode=r.propagation_mode,
                )
            )
        aid, cmd = _create_attempt(
            project_id=project_id,
            stage=stage,
            partition_key=partition,
            bindings=tuple(bindings),
        )
        proj = proj.model_copy(
            update={"created": (*proj.created, (stage.stage_id, partition))}
        )
        command: ProposedCommand[ProductionCommand] = ProposedCommand(
            reaction_name=f"create:{stage.stage_id}:{partition}",
            command_key=f"create:{aid}",
            target=identity.attempt_stream(aid),
            payload=cmd,
        )
        return proj, command


# --------------------------------------------------------------------------- #
# LineagePM:维护消费边 + 各 series 当前版本,双向检测失效
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


class CurrentRef(BaseModel):
    model_config = _frozen()
    series_id: str
    ref: ArtifactRef
    accepted_event_id: str


class LineageState(BaseModel):
    model_config = _frozen()
    metas: tuple[AttemptMeta, ...] = ()
    inputs: tuple[AttemptInputs, ...] = ()
    targets: tuple[AttemptTarget, ...] = ()
    current: tuple[CurrentRef, ...] = ()

    def meta_of(self, attempt_id: str) -> AttemptMeta | None:
        return next((m for m in self.metas if m.attempt_id == attempt_id), None)

    def inputs_of(self, attempt_id: str) -> tuple[BindingItem, ...]:
        found = next((i for i in self.inputs if i.attempt_id == attempt_id), None)
        return found.bindings if found is not None else ()

    def target_of(self, attempt_id: str) -> ArtifactRef | None:
        found = next((t for t in self.targets if t.attempt_id == attempt_id), None)
        return found.ref if found is not None else None

    def current_of(self, series_id: str) -> CurrentRef | None:
        return next((c for c in self.current if c.series_id == series_id), None)


def _stale_cmd(
    *,
    target_ref: ArtifactRef,
    invalidated: ArtifactRef,
    replacement: ArtifactRef,
    root_cause_event_id: str,
    binding: BindingItem,
    meta: AttemptMeta,
) -> ProposedCommand[ProductionCommand]:
    return ProposedCommand(
        reaction_name="mark-stale",
        command_key=f"{target_ref.artifact_id}:{invalidated.artifact_id}",
        target=identity.series_stream(target_ref.series_id),
        payload=MarkArtifactStaleCmd(
            target_ref=target_ref,
            invalidated_input_ref=invalidated,
            replacement_ref=replacement,
            root_cause_event_id=root_cause_event_id,
            scope=binding.propagation_mode,
            task_key=meta.task_key,
            partition_key=meta.partition_key,
        ),
    )


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
        others = tuple(c for c in state.current if c.series_id != payload.series_id)
        new_state = state.model_copy(
            update={
                "targets": (*state.targets, target_entry),
                "current": (
                    *others,
                    CurrentRef(
                        series_id=payload.series_id,
                        ref=payload.artifact_ref,
                        accepted_event_id=event.event_id,
                    ),
                ),
            }
        )
        commands: list[ProposedCommand[ProductionCommand]] = []

        # (a) 上游替换:标记已完成的旧版本消费者
        if payload.previous_current_ref is not None:
            invalidated = payload.previous_current_ref
            for inputs in new_state.inputs:
                binding = _binding_for(inputs.bindings, invalidated.artifact_id)
                if binding is None:
                    continue
                target_ref = new_state.target_of(inputs.attempt_id)
                meta = new_state.meta_of(inputs.attempt_id)
                if target_ref is None or meta is None:
                    continue
                commands.append(
                    _stale_cmd(
                        target_ref=target_ref,
                        invalidated=invalidated,
                        replacement=payload.artifact_ref,
                        root_cause_event_id=event.event_id,
                        binding=binding,
                        meta=meta,
                    )
                )

        # (b) 下游晚完成:本产物绑定的某上游已非 current
        meta = new_state.meta_of(payload.produced_by_attempt)
        if meta is not None:
            for binding in new_state.inputs_of(payload.produced_by_attempt):
                current = new_state.current_of(binding.series_id)
                if current is not None and current.ref.artifact_id != binding.artifact_id:
                    commands.append(
                        _stale_cmd(
                            target_ref=payload.artifact_ref,
                            invalidated=binding.to_ref(),
                            replacement=current.ref,
                            # root cause 指向上游 replacement 的接受事件,而非本下游事件
                            root_cause_event_id=current.accepted_event_id,
                            binding=binding,
                            meta=meta,
                        )
                    )
        return Reaction(state=new_state, commands=tuple(commands))


def _binding_for(
    bindings: tuple[BindingItem, ...], artifact_id: str
) -> BindingItem | None:
    return next((b for b in bindings if b.artifact_id == artifact_id), None)


# --------------------------------------------------------------------------- #
# RecomputePM:消费 stale 事实,累计多输入替换,基于累计绑定算新 attempt_id
# --------------------------------------------------------------------------- #


class RecomputeMeta(BaseModel):
    model_config = _frozen()
    attempt_id: str
    project_id: str
    stage_id: str
    partition_key: str | None
    output_key: str
    series_id: str


class DesiredBindings(BaseModel):
    model_config = _frozen()
    target_artifact_id: str
    bindings: tuple[BindingItem, ...]


class RecomputeState(BaseModel):
    model_config = _frozen()
    metas: tuple[RecomputeMeta, ...] = ()
    inputs: tuple[AttemptInputs, ...] = ()
    target_to_attempt: tuple[tuple[str, str], ...] = ()
    desired: tuple[DesiredBindings, ...] = ()

    def meta_of(self, attempt_id: str) -> RecomputeMeta | None:
        return next((m for m in self.metas if m.attempt_id == attempt_id), None)

    def inputs_of(self, attempt_id: str) -> tuple[BindingItem, ...]:
        found = next((i for i in self.inputs if i.attempt_id == attempt_id), None)
        return found.bindings if found is not None else ()

    def attempt_for_target(self, target_artifact_id: str) -> str | None:
        return next(
            (a for (t, a) in self.target_to_attempt if t == target_artifact_id), None
        )

    def desired_of(self, target_artifact_id: str) -> tuple[BindingItem, ...] | None:
        found = next(
            (d for d in self.desired if d.target_artifact_id == target_artifact_id),
            None,
        )
        return found.bindings if found is not None else None


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
        target_aid = payload.target_ref.artifact_id
        attempt = state.attempt_for_target(target_aid)
        if attempt is None:
            return Reaction(state=state, commands=())
        meta = state.meta_of(attempt)
        base = state.desired_of(target_aid) or state.inputs_of(attempt)
        if meta is None or not base:
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
            for b in base
        )
        others = tuple(d for d in state.desired if d.target_artifact_id != target_aid)
        new_state = state.model_copy(
            update={
                "desired": (
                    *others,
                    DesiredBindings(
                        target_artifact_id=target_aid, bindings=new_bindings
                    ),
                )
            }
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
            state=new_state,
            commands=(
                ProposedCommand(
                    reaction_name="recompute",
                    command_key=f"recompute:{target_aid}:{aid}",
                    target=identity.attempt_stream(aid),
                    payload=cmd,
                ),
            ),
        )
