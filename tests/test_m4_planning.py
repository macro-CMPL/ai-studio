"""M4 步骤3:ExecutionPlanningPM 双事件关联 + ResultMapper 注册表。"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from studio.domain import ids as domain_ids
from studio.domain.artifacts import (
    ArtifactRef,
    ImagePlanPayload,
    OperationParam,
    PlannedOperation,
    ScriptPayload,
)
from studio.domain.enums import ArtifactType, PropagationMode
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.kernel.errors import ContractViolation
from studio.production import identity
from studio.production.attempt_payloads import RecordExecutionSpecCmd
from studio.production.payloads import (
    ArtifactCandidateProducedEvt,
    ArtifactVersionAcceptedEvt,
    TaskAttemptCreatedEvt,
    TaskInputsBoundEvt,
)
from studio.production.pipeline import golden_compiled
from studio.production.planning_pm import (
    ExecutionPlanningProcessManager,
    ProviderBinding,
    QuoteResult,
)
from studio.production.provider_op import ProviderResultRef
from studio.production.result_mapper import (
    ResultMapperRegistry,
    default_result_mappers,
    image_result_mapper,
)
from studio.production.values import BindingItem
from studio.serialization import digest

_P = "p"
_PART = "shot_01"
_TS = datetime(2026, 1, 1, tzinfo=UTC)
_BINDINGS = {
    "image": ProviderBinding(provider_id="fake", provider_version="1", pricing_version="1")
}


def _quote(pid: str, pver: str, op: PlannedOperation, price_ver: str) -> QuoteResult:
    return QuoteResult(estimated_cost=Decimal("12.5"), currency="CNY")


def _pm() -> ExecutionPlanningProcessManager:
    return ExecutionPlanningProcessManager(golden_compiled(), _BINDINGS, _quote)


def _plan_payload() -> ImagePlanPayload:
    return ImagePlanPayload(
        operations=(
            PlannedOperation(
                logical_operation_key="shot_01:image:v0", op_type="gen",
                params=(
                    OperationParam(key="shot", value=_PART),
                    OperationParam(key="prompt", value="a cat"),
                ),
            ),
        )
    )


def _plan_ref() -> ArtifactRef:
    series = domain_ids.series_id(_P, "plan", _PART)
    return ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, 1), series_id=series,
        revision=1, digest=digest(_plan_payload()),
    )


def _plan_binding() -> BindingItem:
    return BindingItem.from_ref(
        requirement_key="image_plan:plan", logical_slot="plan", partition_key=_PART,
        ref=_plan_ref(), propagation_mode=PropagationMode.PARTITION_PRESERVING,
    )


def _image_attempt_id() -> str:
    tk = identity.task_key(_P, "image", _PART)
    return identity.attempt_id(tk, identity.input_binding_digest((_plan_binding(),)), 0)


def _created_evt() -> TaskAttemptCreatedEvt:
    return TaskAttemptCreatedEvt(
        attempt_id=_image_attempt_id(), project_id=_P, stage_id="image",
        partition_key=_PART, output_key="image",
        series_id=domain_ids.series_id(_P, "image", _PART),
    )


def _candidate_evt(candidate_id: str = "cand-plan") -> ArtifactCandidateProducedEvt:
    return ArtifactCandidateProducedEvt(
        candidate_id=candidate_id, attempt_id="att-plan", project_id=_P,
        series_id=domain_ids.series_id(_P, "plan", _PART), output_key="plan",
        partition_key=_PART, digest=digest(_plan_payload()), payload=_plan_payload(),
    )


def _accepted_evt(candidate_id: str = "cand-plan") -> ArtifactVersionAcceptedEvt:
    return ArtifactVersionAcceptedEvt(
        project_id=_P, series_id=domain_ids.series_id(_P, "plan", _PART), revision=1,
        artifact_ref=_plan_ref(), previous_current_ref=None, candidate_id=candidate_id,
        produced_by_attempt="att-plan", output_key="plan", partition_key=_PART,
    )


def _bound_evt() -> TaskInputsBoundEvt:
    return TaskInputsBoundEvt(attempt_id=_image_attempt_id(), exact_refs=(_plan_binding(),))


def _run(pm: Any, payloads: list[MessagePayload]) -> list[Any]:
    state = pm.initial_state()
    commands: list[Any] = []
    for pos, payload in enumerate(payloads):
        env: EventEnvelope[MessagePayload] = EventEnvelope(
            event_id=f"evt-{pos}", schema_version=1, stream_id="s", sequence=pos,
            global_position=pos, correlation_id="c", causation_id="x",
            recorded_at=_TS, payload=payload,
        )
        reaction = pm.react(state, env)
        state = reaction.state
        commands.extend(reaction.commands)
    return commands


# --------------------------------------------------------------------------- #


def test_planning_emits_execution_spec() -> None:
    cmds = _run(_pm(), [_created_evt(), _candidate_evt(), _accepted_evt(), _bound_evt()])
    specs = [c.payload for c in cmds if isinstance(c.payload, RecordExecutionSpecCmd)]
    assert len(specs) == 1
    spec = specs[0].spec
    assert spec.attempt_id == _image_attempt_id()
    assert spec.plan_ref == _plan_ref()
    assert spec.estimated_cost == Decimal("12.5")
    assert spec.currency == "CNY"
    assert spec.operation.logical_operation_key == "shot_01:image:v0"


def test_planning_join_order_independent() -> None:
    # Attempt 先齐备(created+bound),Plan 稍后接受 -> 接受时应回补规划
    cmds = _run(_pm(), [_created_evt(), _bound_evt(), _candidate_evt(), _accepted_evt()])
    specs = [c.payload for c in cmds if isinstance(c.payload, RecordExecutionSpecCmd)]
    assert len(specs) == 1


def test_planning_idempotent_once() -> None:
    events = [_created_evt(), _candidate_evt(), _accepted_evt(), _bound_evt(), _accepted_evt()]
    cmds = _run(_pm(), events)
    specs = [c for c in cmds if isinstance(c.payload, RecordExecutionSpecCmd)]
    assert len(specs) == 1  # 二次 accepted 不重复规划


def test_planning_waits_for_plan_acceptance() -> None:
    # 只有 created + bound,plan 未接受 -> 不规划
    cmds = _run(_pm(), [_created_evt(), _bound_evt()])
    assert not [c for c in cmds if isinstance(c.payload, RecordExecutionSpecCmd)]


def test_planning_ignores_non_provider_attempt() -> None:
    plan_created = TaskAttemptCreatedEvt(
        attempt_id="att-plan-x", project_id=_P, stage_id="plan", partition_key=_PART,
        output_key="plan", series_id=domain_ids.series_id(_P, "plan", _PART),
    )
    bound = TaskInputsBoundEvt(attempt_id="att-plan-x", exact_refs=())
    cmds = _run(_pm(), [plan_created, _candidate_evt(), _accepted_evt(), bound])
    assert not [c for c in cmds if isinstance(c.payload, RecordExecutionSpecCmd)]


def test_planning_ctor_requires_binding() -> None:
    with pytest.raises(ValueError, match="ProviderBinding"):
        ExecutionPlanningProcessManager(golden_compiled(), {}, _quote)


# --- ResultMapper --- #


def test_image_result_mapper_builds_payload() -> None:
    from studio.production.execution_spec import ProviderExecutionSpec

    spec = ProviderExecutionSpec.from_plan(
        attempt_id=_image_attempt_id(), plan_ref=_plan_ref(), plan_payload=_plan_payload(),
        provider_id="fake", provider_version="1", estimated_cost=Decimal("12.5"),
        currency="CNY", pricing_version="1", request_ref="req",
    )
    stage = golden_compiled().by_stage("image")
    assert stage is not None
    result_ref = ProviderResultRef(blob_ref="blob://final", digest="a" * 64)
    payload = image_result_mapper(spec, result_ref, stage)
    assert payload.kind is ArtifactType.IMAGE
    assert payload.blob_ref == "blob://final"
    assert payload.shot_id == _PART
    assert payload.prompt == "a cat"


def test_registry_dispatches_and_validates() -> None:
    from studio.production.execution_spec import ProviderExecutionSpec

    spec = ProviderExecutionSpec.from_plan(
        attempt_id=_image_attempt_id(), plan_ref=_plan_ref(), plan_payload=_plan_payload(),
        provider_id="fake", provider_version="1", estimated_cost=Decimal("12.5"),
        currency="CNY", pricing_version="1", request_ref="req",
    )
    stage = golden_compiled().by_stage("image")
    assert stage is not None
    result_ref = ProviderResultRef(blob_ref="blob://final", digest="a" * 64)
    assert default_result_mappers().build(spec, result_ref, stage).kind is ArtifactType.IMAGE
    empty = ResultMapperRegistry({})
    with pytest.raises(ContractViolation):
        empty.build(spec, result_ref, stage)


def test_registry_rejects_wrong_mapper_output() -> None:
    from studio.production.execution_spec import ProviderExecutionSpec

    def bad_mapper(spec: Any, ref: Any, stage: Any) -> Any:
        return ScriptPayload(title="t", logline="l", beats=("b",))

    registry = ResultMapperRegistry({ArtifactType.IMAGE: bad_mapper})
    spec = ProviderExecutionSpec.from_plan(
        attempt_id=_image_attempt_id(), plan_ref=_plan_ref(), plan_payload=_plan_payload(),
        provider_id="fake", provider_version="1", estimated_cost=Decimal("12.5"),
        currency="CNY", pricing_version="1", request_ref="req",
    )
    stage = golden_compiled().by_stage("image")
    assert stage is not None
    with pytest.raises(ContractViolation):
        registry.build(spec, ProviderResultRef(blob_ref="b", digest="a" * 64), stage)
