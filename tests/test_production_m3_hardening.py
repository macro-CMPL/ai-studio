"""M3 hardening 对抗测试:串项目、身份不变式、可重复展开、Lineage 双向、多输入累计。"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from kernel_helpers import FakeClock
from production_helpers import build_production_stack, init_command
from studio.application.routing_worker import RoutingCommandWorker
from studio.domain import ids as domain_ids
from studio.domain.artifacts import (
    ArtifactRef,
    ImagePayload,
    ScriptPayload,
    ShotSpec,
    StoryboardPayload,
)
from studio.domain.enums import (
    AcceptanceMode,
    ArtifactType,
    CardinalityKind,
    ControlRole,
    CostMode,
    CurrencyStatus,
    DependencyStatus,
    ExecutorKind,
    PartitioningKind,
    PropagationMode,
    ToolEffectLevel,
)
from studio.domain.stages import (
    Cardinality,
    OutputSpec,
    Partitioning,
    Requirement,
    StageSpec,
)
from studio.infrastructure.memory._state import MemoryDatabase
from studio.infrastructure.memory.unit_of_work import (
    MemoryCommandBus,
    MemoryUnitOfWorkFactory,
)
from studio.kernel.decisions import Accepted
from studio.kernel.envelopes import CommandEnvelope, EventEnvelope
from studio.kernel.errors import ContractViolation, IdempotencyConflict
from studio.production import identity
from studio.production.attempt import TaskAttemptDecider
from studio.production.compile import CompilationError, compile_from
from studio.production.dispatch import canonical_target
from studio.production.payloads import (
    ArtifactCandidateProducedEvt,
    ArtifactMarkedStaleEvt,
    ArtifactVersionAcceptedEvt,
    CreateTaskAttemptCmd,
    ExpandStageCmd,
    InitializePipelineCmd,
    MarkArtifactStaleCmd,
    PipelineInitializedEvt,
    ProductionEvent,
    ProposeArtifactVersionCmd,
    StageExpandedEvt,
    TaskAttemptCreatedEvt,
    TaskInputsBoundEvt,
)
from studio.production.pipeline import golden_compiled, golden_selectors
from studio.production.process_managers import (
    ExpansionProcessManager,
    LineageProcessManager,
    RecomputeProcessManager,
)
from studio.production.project import ProjectDecider
from studio.production.projections import ArtifactLifecycleView
from studio.production.series import ArtifactSeriesDecider
from studio.production.values import BindingItem

_TS = datetime(2026, 1, 1, tzinfo=UTC)
_FRESH_CURRENT = (CurrencyStatus.CURRENT, DependencyStatus.FRESH)


def _ref(series_id: str, revision: int, digest_hex: str = "a" * 64) -> ArtifactRef:
    return ArtifactRef(
        artifact_id=domain_ids.artifact_id(series_id, revision),
        series_id=series_id,
        revision=revision,
        digest=digest_hex,
    )


def _binding(ref: ArtifactRef, key: str) -> BindingItem:
    return BindingItem.from_ref(
        requirement_key=key,
        logical_slot=key,
        partition_key=None,
        ref=ref,
        propagation_mode=PropagationMode.AGGREGATE,
    )


def _envelope(pos: int, payload: ProductionEvent) -> EventEnvelope[ProductionEvent]:
    return EventEnvelope(
        event_id=f"evt-{pos}",
        schema_version=1,
        stream_id="s",
        sequence=pos,
        global_position=pos,
        correlation_id="c",
        causation_id="x",
        recorded_at=_TS,
        payload=payload,
    )


def _run_pm(pm: Any, payloads: list[ProductionEvent]) -> list[Any]:
    state = pm.initial_state()
    commands: list[Any] = []
    for pos, payload in enumerate(payloads):
        reaction = pm.react(state, _envelope(pos, payload))
        state = reaction.state
        commands.extend(reaction.commands)
    return commands


# --- Blocker 1:ExpansionPM 不串项目 --- #


def test_expansion_does_not_leak_across_projects() -> None:
    s = build_production_stack()
    s.bus.publish(init_command("proj-A", command_id="init-A"))
    s.bus.publish(init_command("proj-B", command_id="init-B"))
    s.driver.run_until_quiescent()

    view = ArtifactLifecycleView.build(s.db.state.events)
    for project in ("proj-A", "proj-B"):
        for shot in ("shot_01", "shot_02"):
            series = domain_ids.series_id(project, "image", shot)
            cur = view.current_ref(series)
            assert cur is not None, f"{project}/{shot} 缺失(发生串项目)"
            assert (view.currency(cur.artifact_id), view.dependency(cur.artifact_id)) == (
                _FRESH_CURRENT
            )


# --- Blocker 2:身份不变式 --- #


def test_router_rejects_target_payload_mismatch() -> None:
    worker = RoutingCommandWorker(
        deciders={},
        resolve_kind=identity.stream_kind,
        canonical_target=canonical_target,
        bus=MemoryCommandBus(),
        uow_factory=MemoryUnitOfWorkFactory(MemoryDatabase()),
        clock=FakeClock(),
    )
    forged: CommandEnvelope[Any] = CommandEnvelope(
        command_id="c1",
        schema_version=1,
        target=identity.project_stream("proj-A"),
        command_key="init",
        correlation_id="x",
        causation_id=None,
        issued_at=_TS,
        payload=InitializePipelineCmd(project_id="proj-B", pipeline_spec_id="spec"),
    )
    worker._bus.publish(forged)  # type: ignore[attr-defined]
    with pytest.raises(ContractViolation):
        worker.tick()


def test_attempt_decider_rejects_forged_identity() -> None:
    decider = TaskAttemptDecider(
        {"image": lambda stage, refs, part: ImagePayload(
            shot_id=part or "", prompt="p", blob_ref="b"
        )}
    )
    project, stage, partition = "p", "image", "shot_01"
    series = domain_ids.series_id(project, "image", partition)
    tk = identity.task_key(project, stage, partition)
    aid = identity.attempt_id(tk, identity.input_binding_digest(()), 0)
    valid = CreateTaskAttemptCmd(
        attempt_id=aid,
        project_id=project,
        stage_id=stage,
        partition_key=partition,
        output_key="image",
        series_id=series,
        exact_refs=(),
    )
    assert isinstance(decider.decide(decider.initial_state(), valid), Accepted)

    forged_attempt = valid.model_copy(update={"attempt_id": "forged"})
    assert decider.decide(decider.initial_state(), forged_attempt).code == "forged_attempt"  # type: ignore[union-attr]
    forged_series = valid.model_copy(update={"series_id": "wrong"})
    assert decider.decide(decider.initial_state(), forged_series).code == "forged_series"  # type: ignore[union-attr]


def test_series_decider_digest_and_candidate_conflict() -> None:
    decider = ArtifactSeriesDecider()
    series = domain_ids.series_id("p", "image", "shot_01")
    payload1 = ImagePayload(shot_id="shot_01", prompt="a", blob_ref="b")
    from studio.serialization import digest

    good = ProposeArtifactVersionCmd(
        project_id="p",
        series_id=series,
        candidate_id="cand-1",
        output_key="image",
        partition_key="shot_01",
        digest=digest(payload1),
        payload=payload1,
        acceptance_mode=AcceptanceMode.AUTO,
        produced_by_attempt="att-1",
    )
    # digest 不一致 -> Rejected
    bad_digest = good.model_copy(update={"digest": "b" * 64})
    assert decider.decide(decider.initial_state(), bad_digest).code == "digest_mismatch"  # type: ignore[union-attr]

    # 折叠首次 propose 的事件
    state = decider.initial_state()
    decision = decider.decide(state, good)
    assert isinstance(decision, Accepted)
    for pe in decision.events:
        state = decider.evolve(state, pe.payload)

    # 同 candidate_id 异内容 -> IdempotencyConflict
    payload2 = ImagePayload(shot_id="shot_01", prompt="CHANGED", blob_ref="b")
    conflict = good.model_copy(
        update={"payload": payload2, "digest": digest(payload2)}
    )
    with pytest.raises(IdempotencyConflict):
        decider.decide(state, conflict)


# --- Blocker 3:同一 Stage 可用新 driver 再次展开 --- #


def test_stage_can_reexpand_with_new_driver() -> None:
    decider = ProjectDecider()
    state = decider.initial_state()
    init = decider.decide(state, InitializePipelineCmd(project_id="p", pipeline_spec_id="s"))
    assert isinstance(init, Accepted)
    for pe in init.events:
        state = decider.evolve(state, pe.payload)

    driver_v1 = _ref(domain_ids.series_id("p", "storyboard", None), 1)
    driver_v2 = _ref(domain_ids.series_id("p", "storyboard", None), 2, "b" * 64)
    task_keys = (identity.task_key("p", "plan", "shot_01"),)

    def expand(driver: ArtifactRef) -> ExpandStageCmd:
        return ExpandStageCmd(
            project_id="p",
            stage_id="plan",
            driver_ref=driver,
            partitions=("shot_01",),
            task_keys=task_keys,
        )

    d1 = decider.decide(state, expand(driver_v1))
    assert isinstance(d1, Accepted)
    for pe in d1.events:
        state = decider.evolve(state, pe.payload)

    # 同 driver 再展开 -> 拒绝
    assert decider.decide(state, expand(driver_v1)).code == "already_expanded"  # type: ignore[union-attr]
    # 新 driver -> 允许
    assert isinstance(decider.decide(state, expand(driver_v2)), Accepted)


# --- Blocker 4:Lineage 处理"旧输入执行中、上游先更新" --- #


def test_lineage_marks_late_downstream_stale() -> None:
    a_v1 = _ref("series-A", 1)
    a_v2 = _ref("series-A", 2, "b" * 64)
    t_v1 = _ref("series-T", 1, "c" * 64)
    binding_a1 = _binding(a_v1, "plan")

    events: list[ProductionEvent] = [
        TaskAttemptCreatedEvt(
            attempt_id="att-consumer",
            project_id="p",
            stage_id="image",
            partition_key="shot_01",
            output_key="image",
            series_id="series-T",
        ),
        TaskInputsBoundEvt(attempt_id="att-consumer", exact_refs=(binding_a1,)),
        ArtifactVersionAcceptedEvt(
            project_id="p", series_id="series-A", revision=1, artifact_ref=a_v1,
            previous_current_ref=None, candidate_id="ca1", produced_by_attempt="att-A1",
            output_key="plan", partition_key="shot_01",
        ),
        ArtifactVersionAcceptedEvt(
            project_id="p", series_id="series-A", revision=2, artifact_ref=a_v2,
            previous_current_ref=a_v1, candidate_id="ca2", produced_by_attempt="att-A2",
            output_key="plan", partition_key="shot_01",
        ),
        ArtifactVersionAcceptedEvt(
            project_id="p", series_id="series-T", revision=1, artifact_ref=t_v1,
            previous_current_ref=None, candidate_id="ct1",
            produced_by_attempt="att-consumer", output_key="image", partition_key="shot_01",
        ),
    ]
    commands = _run_pm(LineageProcessManager(), events)
    stale = [c.payload for c in commands if isinstance(c.payload, MarkArtifactStaleCmd)]
    assert len(stale) == 1
    assert stale[0].target_ref.artifact_id == t_v1.artifact_id
    assert stale[0].invalidated_input_ref.artifact_id == a_v1.artifact_id
    assert stale[0].replacement_ref.artifact_id == a_v2.artifact_id
    # root cause 必须指向上游 replacement(A-v2)的接受事件(evt-3),而非下游自身(evt-4)
    assert stale[0].root_cause_event_id == "evt-3"


# --- Blocker 5:多输入失效累计 --- #


def test_recompute_accumulates_multi_input_replacements() -> None:
    a_v1 = _ref("series-A", 1)
    a_v2 = _ref("series-A", 2, "b" * 64)
    b_v1 = _ref("series-B", 1, "c" * 64)
    b_v2 = _ref("series-B", 2, "d" * 64)
    t_v1 = _ref("series-T", 1, "e" * 64)

    events: list[ProductionEvent] = [
        TaskAttemptCreatedEvt(
            attempt_id="att-T", project_id="p", stage_id="stitch", partition_key=None,
            output_key="stitch", series_id="series-T",
        ),
        TaskInputsBoundEvt(
            attempt_id="att-T",
            exact_refs=(_binding(a_v1, "a"), _binding(b_v1, "b")),
        ),
        ArtifactVersionAcceptedEvt(
            project_id="p", series_id="series-T", revision=1, artifact_ref=t_v1,
            previous_current_ref=None, candidate_id="ct1", produced_by_attempt="att-T",
            output_key="stitch", partition_key=None,
        ),
        ArtifactMarkedStaleEvt(
            target_ref=t_v1, invalidated_input_ref=a_v1, replacement_ref=a_v2,
            root_cause_event_id="e1", scope=PropagationMode.AGGREGATE,
            task_key="tk", partition_key=None,
        ),
        ArtifactMarkedStaleEvt(
            target_ref=t_v1, invalidated_input_ref=b_v1, replacement_ref=b_v2,
            root_cause_event_id="e2", scope=PropagationMode.AGGREGATE,
            task_key="tk", partition_key=None,
        ),
    ]
    commands = _run_pm(RecomputeProcessManager(), events)
    creates = [c.payload for c in commands if isinstance(c.payload, CreateTaskAttemptCmd)]
    assert len(creates) == 2
    last_ids = {b.artifact_id for b in creates[-1].exact_refs}
    assert last_ids == {a_v2.artifact_id, b_v2.artifact_id}  # 两个替换累计


# --- 投影次要问题:未知 artifact --- #


def test_projection_unknown_artifact_raises() -> None:
    view = ArtifactLifecycleView.build([])
    with pytest.raises(LookupError):
        view.currency("nope")


# --- Blocker 1(补):candidate 乱序接受仍绑定各自 partitions --- #


def test_expansion_binds_partitions_per_candidate() -> None:
    pm = ExpansionProcessManager(golden_compiled(), golden_selectors())
    project = "p"
    sb_series = domain_ids.series_id(project, "storyboard", None)
    sb_ref = _ref(sb_series, 1)

    events: list[ProductionEvent] = [
        PipelineInitializedEvt(
            project_id=project, pipeline_spec_id=golden_compiled().spec_id
        ),
        ArtifactCandidateProducedEvt(
            candidate_id="c1", attempt_id="att-1", project_id=project,
            series_id=sb_series, output_key="storyboard", partition_key=None,
            digest="a" * 64,
            payload=StoryboardPayload(shots=(ShotSpec(shot_id="shot_A", description="d"),)),
        ),
        ArtifactCandidateProducedEvt(
            candidate_id="c2", attempt_id="att-2", project_id=project,
            series_id=sb_series, output_key="storyboard", partition_key=None,
            digest="b" * 64,
            payload=StoryboardPayload(shots=(ShotSpec(shot_id="shot_B", description="d"),)),
        ),
        ArtifactVersionAcceptedEvt(
            project_id=project, series_id=sb_series, revision=1, artifact_ref=sb_ref,
            previous_current_ref=None, candidate_id="c1", produced_by_attempt="att-1",
            output_key="storyboard", partition_key=None,
        ),
    ]
    commands = _run_pm(pm, events)
    expands = [c.payload for c in commands if isinstance(c.payload, ExpandStageCmd)]
    assert len(expands) == 1
    assert expands[0].partitions == ("shot_A",)  # 用 c1 的分区,而非 c2 的 shot_B


# --- Blocker 2(补):非 Storyboard 的可配置 FANOUT selector --- #


def test_configurable_fanout_selector() -> None:
    spec = compile_from(
        (
            _producer_stage("root", ArtifactType.SCRIPT, "root"),
            _fanout_stage("leaf", "leaf", ArtifactType.SCRIPT, "root", "twoparts"),
        )
    )
    selectors = {("twoparts", "1"): lambda payload: ("p1", "p2")}
    pm = ExpansionProcessManager(spec, selectors)
    project = "p"
    root_series = domain_ids.series_id(project, "root", None)
    root_ref = _ref(root_series, 1)
    events: list[ProductionEvent] = [
        PipelineInitializedEvt(project_id=project, pipeline_spec_id=spec.spec_id),
        ArtifactCandidateProducedEvt(
            candidate_id="c1", attempt_id="att-1", project_id=project,
            series_id=root_series, output_key="root", partition_key=None,
            digest="a" * 64,
            payload=ScriptPayload(title="t", logline="l", beats=("b",)),
        ),
        ArtifactVersionAcceptedEvt(
            project_id=project, series_id=root_series, revision=1, artifact_ref=root_ref,
            previous_current_ref=None, candidate_id="c1", produced_by_attempt="att-1",
            output_key="root", partition_key=None,
        ),
    ]
    commands = _run_pm(pm, events)
    expands = [c.payload for c in commands if isinstance(c.payload, ExpandStageCmd)]
    assert len(expands) == 1
    assert expands[0].partitions == ("p1", "p2")


# --- Blocker 3(补):forged series 与不存在 target 的 stale 被拒 --- #


def test_series_rejects_forged_series_and_unknown_stale_target() -> None:
    decider = ArtifactSeriesDecider()
    payload = ImagePayload(shot_id="shot_01", prompt="a", blob_ref="b")
    from studio.serialization import digest

    forged = ProposeArtifactVersionCmd(
        project_id="p", series_id="not-canonical", candidate_id="c1",
        output_key="image", partition_key="shot_01", digest=digest(payload),
        payload=payload, acceptance_mode=AcceptanceMode.AUTO, produced_by_attempt="att",
    )
    assert decider.decide(decider.initial_state(), forged).code == "forged_series"  # type: ignore[union-attr]

    series = domain_ids.series_id("p", "image", "shot_01")
    ghost = _ref(series, 99)
    stale = MarkArtifactStaleCmd(
        target_ref=ghost, invalidated_input_ref=_ref(series, 1),
        replacement_ref=_ref(series, 2, "b" * 64), root_cause_event_id="e",
        scope=PropagationMode.PARTITION_PRESERVING, task_key="tk", partition_key="shot_01",
    )
    assert decider.decide(decider.initial_state(), stale).code == "unknown_target"  # type: ignore[union-attr]


# --- Blocker 3(补):ProjectDecider 校验 partitions/task_keys --- #


def test_project_rejects_bad_partitions_and_task_keys() -> None:
    decider = ProjectDecider()
    state = decider.initial_state()
    init = decider.decide(state, InitializePipelineCmd(project_id="p", pipeline_spec_id="s"))
    assert isinstance(init, Accepted)
    for pe in init.events:
        state = decider.evolve(state, pe.payload)

    driver = _ref(domain_ids.series_id("p", "storyboard", None), 1)
    bad_parts = ExpandStageCmd(
        project_id="p", stage_id="plan", driver_ref=driver,
        partitions=("z", "a", "a"), task_keys=("x",),
    )
    assert decider.decide(state, bad_parts).code == "bad_partitions"  # type: ignore[union-attr]

    good_parts = ("shot_01", "shot_02")
    bad_keys = ExpandStageCmd(
        project_id="p", stage_id="plan", driver_ref=driver,
        partitions=good_parts, task_keys=("wrong",),
    )
    assert decider.decide(state, bad_keys).code == "bad_task_keys"  # type: ignore[union-attr]


# --- StageSpec 构造助手(用于编译器/展开测试) --- #


def _producer_stage(stage_id: str, artifact_type: ArtifactType, slot: str) -> StageSpec:
    return StageSpec(
        stage_id=stage_id,
        executor_kind=ExecutorKind.AGENT,
        control_role=ControlRole.PRODUCER,
        allowed_tool_effects=frozenset({ToolEffectLevel.PURE}),
        cost_mode=CostMode.FREE,
        requires=(),
        produces=(
            OutputSpec(
                artifact_type=artifact_type,
                logical_slot=slot,
                schema_version=1,
                partitioning=Partitioning(kind=PartitioningKind.SINGLETON),
            ),
        ),
    )


def _fanout_stage(
    stage_id: str,
    out_slot: str,
    driver_type: ArtifactType,
    driver_slot: str,
    selector_id: str,
    version: str = "1",
) -> StageSpec:
    return StageSpec(
        stage_id=stage_id,
        executor_kind=ExecutorKind.AGENT,
        control_role=ControlRole.PRODUCER,
        allowed_tool_effects=frozenset({ToolEffectLevel.PURE}),
        cost_mode=CostMode.FREE,
        requires=(
            Requirement(
                artifact_type=driver_type,
                logical_slot=driver_slot,
                cardinality=Cardinality(
                    kind=CardinalityKind.DYNAMIC_PARTITION_BY, partition_by="k"
                ),
                propagation_mode=PropagationMode.AGGREGATE,
                partition_selector_id=selector_id,
                partition_selector_version=version,
            ),
        ),
        produces=(
            OutputSpec(
                artifact_type=ArtifactType.IMAGE,
                logical_slot=out_slot,
                schema_version=1,
                partitioning=Partitioning(
                    kind=PartitioningKind.DYNAMIC_FROM, from_key="k"
                ),
            ),
        ),
    )


def _accept_root(project: str, slot: str) -> list[ProductionEvent]:
    series = domain_ids.series_id(project, slot, None)
    return [
        PipelineInitializedEvt(project_id=project, pipeline_spec_id="ignored"),
        ArtifactCandidateProducedEvt(
            candidate_id="c1", attempt_id="att-1", project_id=project,
            series_id=series, output_key=slot, partition_key=None, digest="a" * 64,
            payload=ScriptPayload(title="t", logline="l", beats=("b",)),
        ),
        ArtifactVersionAcceptedEvt(
            project_id=project, series_id=series, revision=1, artifact_ref=_ref(series, 1),
            previous_current_ref=None, candidate_id="c1", produced_by_attempt="att-1",
            output_key=slot, partition_key=None,
        ),
    ]


def test_double_fanout_uses_own_selectors() -> None:
    spec = compile_from(
        (
            _producer_stage("root", ArtifactType.SCRIPT, "root"),
            _fanout_stage("left", "left", ArtifactType.SCRIPT, "root", "selL"),
            _fanout_stage("right", "right", ArtifactType.SCRIPT, "root", "selR"),
        )
    )
    selectors = {
        ("selL", "1"): lambda p: ("Lp",),
        ("selR", "1"): lambda p: ("Rp",),
    }
    pm = ExpansionProcessManager(spec, selectors)
    events = _accept_root("p", "root")
    events[0] = PipelineInitializedEvt(project_id="p", pipeline_spec_id=spec.spec_id)
    commands = _run_pm(pm, events)
    by_stage = {
        c.payload.stage_id: c.payload.partitions
        for c in commands
        if isinstance(c.payload, ExpandStageCmd)
    }
    assert by_stage == {"left": ("Lp",), "right": ("Rp",)}


def test_selector_version_mismatch_fails_fast() -> None:
    spec = compile_from(
        (
            _producer_stage("root", ArtifactType.SCRIPT, "root"),
            _fanout_stage("leaf", "leaf", ArtifactType.SCRIPT, "root", "sel", version="2"),
        )
    )
    with pytest.raises(ValueError):
        ExpansionProcessManager(spec, {("sel", "1"): lambda p: ("x",)})


def test_series_rejects_forged_target_digest() -> None:
    from studio.serialization import digest

    decider = ArtifactSeriesDecider()
    series = domain_ids.series_id("p", "image", "shot_01")
    payload = ImagePayload(shot_id="shot_01", prompt="a", blob_ref="b")
    propose = ProposeArtifactVersionCmd(
        project_id="p", series_id=series, candidate_id="c1", output_key="image",
        partition_key="shot_01", digest=digest(payload), payload=payload,
        acceptance_mode=AcceptanceMode.AUTO, produced_by_attempt="att",
    )
    state = decider.initial_state()
    decision = decider.decide(state, propose)
    assert isinstance(decision, Accepted)
    for pe in decision.events:
        state = decider.evolve(state, pe.payload)

    real_ref = _ref(series, 1, digest(payload))
    forged = real_ref.model_copy(update={"digest": "f" * 64})
    stale = MarkArtifactStaleCmd(
        target_ref=forged, invalidated_input_ref=_ref(series, 1),
        replacement_ref=_ref(series, 2, "b" * 64), root_cause_event_id="e",
        scope=PropagationMode.PARTITION_PRESERVING, task_key="tk", partition_key="shot_01",
    )
    assert decider.decide(state, stale).code == "unknown_target"  # type: ignore[union-attr]


def test_project_rejects_unsorted_task_keys() -> None:
    decider = ProjectDecider()
    state = decider.initial_state()
    init = decider.decide(state, InitializePipelineCmd(project_id="p", pipeline_spec_id="s"))
    assert isinstance(init, Accepted)
    for pe in init.events:
        state = decider.evolve(state, pe.payload)

    driver = _ref(domain_ids.series_id("p", "storyboard", None), 1)
    parts = ("shot_01", "shot_02")
    expected = tuple(sorted(identity.task_key("p", "plan", s) for s in parts))
    reversed_keys = tuple(reversed(expected))
    cmd = ExpandStageCmd(
        project_id="p", stage_id="plan", driver_ref=driver,
        partitions=parts, task_keys=reversed_keys,
    )
    if reversed_keys != expected:  # 若恰好相同则跳过(概率极低)
        assert decider.decide(state, cmd).code == "bad_task_keys"  # type: ignore[union-attr]


def test_compiler_rejects_ambiguous_producer() -> None:
    with pytest.raises(CompilationError):
        compile_from(
            (
                _producer_stage("a", ArtifactType.SCRIPT, "dup"),
                _producer_stage("b", ArtifactType.SCRIPT, "dup"),
                _fanout_stage("c", "c", ArtifactType.SCRIPT, "dup", "sel"),
            )
        )


def _multi_input_stages() -> tuple[StageSpec, ...]:
    """sb(root) -> plan(fanout) ; style(root) ; img(per-partition: plan 保持 + style 聚合)。"""
    img = StageSpec(
        stage_id="img",
        executor_kind=ExecutorKind.PROVIDER,
        control_role=ControlRole.PRODUCER,
        allowed_tool_effects=frozenset({ToolEffectLevel.COSTED}),
        cost_mode=CostMode.METERED,
        requires=(
            Requirement(
                artifact_type=ArtifactType.IMAGE, logical_slot="plan",
                cardinality=Cardinality(kind=CardinalityKind.STATIC),
                propagation_mode=PropagationMode.PARTITION_PRESERVING,
            ),
            Requirement(
                artifact_type=ArtifactType.SCRIPT, logical_slot="style",
                cardinality=Cardinality(kind=CardinalityKind.STATIC),
                propagation_mode=PropagationMode.AGGREGATE,
            ),
        ),
        produces=(
            OutputSpec(
                artifact_type=ArtifactType.STITCH, logical_slot="img", schema_version=1,
                partitioning=Partitioning(kind=PartitioningKind.INHERIT_PARTITION),
            ),
        ),
    )
    return (
        _producer_stage("sb", ArtifactType.STORYBOARD, "sb"),
        _fanout_stage("plan", "plan", ArtifactType.STORYBOARD, "sb", "selP"),
        _producer_stage("style", ArtifactType.SCRIPT, "style"),
        img,
    )


def test_compiler_preserves_multi_input_and_stable_digest() -> None:
    stages = _multi_input_stages()
    spec = compile_from(stages)
    img = spec.by_stage("img")
    assert img is not None
    assert len(img.requirements) == 2
    assert compile_from(stages).spec_id == spec.spec_id


def _accepted_evt(
    project: str, slot: str, partition: str | None, revision: int, attempt: str, digest_hex: str
) -> ArtifactVersionAcceptedEvt:
    series = domain_ids.series_id(project, slot, partition)
    return ArtifactVersionAcceptedEvt(
        project_id=project, series_id=series, revision=revision,
        artifact_ref=_ref(series, revision, digest_hex), previous_current_ref=None,
        candidate_id=f"c-{slot}-{partition}", produced_by_attempt=attempt,
        output_key=slot, partition_key=partition,
    )


def test_assembler_waits_for_all_inputs_multi_input() -> None:
    spec = compile_from(_multi_input_stages())
    selectors = {("selP", "1"): lambda p: ("shot_01",)}
    pm = ExpansionProcessManager(spec, selectors)
    project = "p"
    sb_series = domain_ids.series_id(project, "sb", None)

    events: list[ProductionEvent] = [
        PipelineInitializedEvt(project_id=project, pipeline_spec_id=spec.spec_id),
        ArtifactCandidateProducedEvt(
            candidate_id="c-sb", attempt_id="att-sb", project_id=project,
            series_id=sb_series, output_key="sb", partition_key=None, digest="a" * 64,
            payload=StoryboardPayload(shots=(ShotSpec(shot_id="shot_01", description="d"),)),
        ),
        _accepted_evt(project, "sb", None, 1, "att-sb", "a" * 64),
        StageExpandedEvt(
            project_id=project, stage_id="plan",
            driver_ref=_ref(sb_series, 1, "a" * 64),
            partitions=("shot_01",),
            task_keys=(identity.task_key(project, "plan", "shot_01"),),
        ),
        # plan[shot_01] 接受(分区源),但 style 尚未接受 -> 不应创建 img
        _accepted_evt(project, "plan", "shot_01", 1, "att-plan", "b" * 64),
        # style 聚合输入接受 -> 现在 img[shot_01] 齐备
        _accepted_evt(project, "style", None, 1, "att-style", "c" * 64),
    ]
    commands = _run_pm(pm, events)
    img_creates = [
        c.payload
        for c in commands
        if isinstance(c.payload, CreateTaskAttemptCmd) and c.payload.stage_id == "img"
    ]
    assert len(img_creates) == 1  # 齐备后只创建一次
    slots = {b.logical_slot for b in img_creates[0].exact_refs}
    assert slots == {"plan", "style"}  # 两个输入都绑定,不丢 style


def test_compiler_rejects_dependency_cycle() -> None:
    # a 需要 b 的输出,b 需要 a 的输出 -> 环
    a = StageSpec(
        stage_id="a", executor_kind=ExecutorKind.TRANSFORM, control_role=ControlRole.PRODUCER,
        allowed_tool_effects=frozenset({ToolEffectLevel.PURE}), cost_mode=CostMode.FREE,
        requires=(
            Requirement(
                artifact_type=ArtifactType.STORYBOARD, logical_slot="outb",
                cardinality=Cardinality(kind=CardinalityKind.STATIC),
                propagation_mode=PropagationMode.PARTITION_PRESERVING,
            ),
        ),
        produces=(
            OutputSpec(
                artifact_type=ArtifactType.SCRIPT, logical_slot="outa", schema_version=1,
                partitioning=Partitioning(kind=PartitioningKind.INHERIT_PARTITION),
            ),
        ),
    )
    b = StageSpec(
        stage_id="b", executor_kind=ExecutorKind.TRANSFORM, control_role=ControlRole.PRODUCER,
        allowed_tool_effects=frozenset({ToolEffectLevel.PURE}), cost_mode=CostMode.FREE,
        requires=(
            Requirement(
                artifact_type=ArtifactType.SCRIPT, logical_slot="outa",
                cardinality=Cardinality(kind=CardinalityKind.STATIC),
                propagation_mode=PropagationMode.PARTITION_PRESERVING,
            ),
        ),
        produces=(
            OutputSpec(
                artifact_type=ArtifactType.STORYBOARD, logical_slot="outb", schema_version=1,
                partitioning=Partitioning(kind=PartitioningKind.INHERIT_PARTITION),
            ),
        ),
    )
    with pytest.raises(CompilationError):
        compile_from((a, b))


def test_compiler_rejects_partition_contract_mismatch() -> None:
    # 动态输入(FANOUT)却声明 SINGLETON 输出 -> 契约冲突
    bad = StageSpec(
        stage_id="bad", executor_kind=ExecutorKind.AGENT, control_role=ControlRole.PRODUCER,
        allowed_tool_effects=frozenset({ToolEffectLevel.PURE}), cost_mode=CostMode.FREE,
        requires=(
            Requirement(
                artifact_type=ArtifactType.SCRIPT, logical_slot="root",
                cardinality=Cardinality(
                    kind=CardinalityKind.DYNAMIC_PARTITION_BY, partition_by="k"
                ),
                propagation_mode=PropagationMode.AGGREGATE,
                partition_selector_id="s", partition_selector_version="1",
            ),
        ),
        produces=(
            OutputSpec(
                artifact_type=ArtifactType.IMAGE, logical_slot="bad", schema_version=1,
                partitioning=Partitioning(kind=PartitioningKind.SINGLETON),  # 应为 DYNAMIC_FROM
            ),
        ),
    )
    with pytest.raises(CompilationError):
        compile_from((_producer_stage("root", ArtifactType.SCRIPT, "root"), bad))
