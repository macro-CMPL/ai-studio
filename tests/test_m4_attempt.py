"""M4 步骤3(基础):TaskAttemptDecider 的 PROVIDER 生命周期状态机 + 反例。"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from studio.domain import ids as domain_ids
from studio.domain.artifacts import (
    ArtifactRef,
    ImagePayload,
    ImagePlanPayload,
    OperationParam,
    PlannedOperation,
)
from studio.domain.enums import PropagationMode, TaskAttemptStatus
from studio.kernel.decisions import Accepted, Rejected
from studio.kernel.errors import IdempotencyConflict
from studio.production import identity
from studio.production.attempt import TaskAttemptDecider
from studio.production.attempt_payloads import (
    MarkBlockedCmd,
    MarkWaitingProviderCmd,
    MarkWaitingReconciliationCmd,
    RecordExecutionSpecCmd,
    RecordProviderResultCmd,
)
from studio.production.execution_spec import ProviderExecutionSpec
from studio.production.payloads import CreateTaskAttemptCmd
from studio.production.pipeline import golden_compiled
from studio.production.values import BindingItem
from studio.serialization import digest

_S = TaskAttemptStatus
_PROJECT = "p"
_PART = "shot_01"


def _apply(decider: Any, state: Any, cmd: Any) -> tuple[Any, Any]:
    decision = decider.decide(state, cmd)
    if isinstance(decision, Accepted):
        for pe in decision.events:
            state = decider.evolve(state, pe.payload)
    return state, decision


def _plan_payload() -> ImagePlanPayload:
    return ImagePlanPayload(
        operations=(
            PlannedOperation(
                logical_operation_key="shot_01:image:v0", op_type="gen",
                params=(OperationParam(key="shot", value=_PART),),
            ),
        )
    )


def _plan_binding() -> BindingItem:
    payload = _plan_payload()
    series = domain_ids.series_id(_PROJECT, "plan", _PART)
    ref = ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, 1), series_id=series,
        revision=1, digest=digest(payload),
    )
    return BindingItem.from_ref(
        requirement_key="plan", logical_slot="plan", partition_key=_PART,
        ref=ref, propagation_mode=PropagationMode.PARTITION_PRESERVING,
    )


def _image_attempt_id(binding: BindingItem) -> str:
    tk = identity.task_key(_PROJECT, "image", _PART)
    return identity.attempt_id(tk, identity.input_binding_digest((binding,)), 0)


def _create_image_cmd() -> CreateTaskAttemptCmd:
    binding = _plan_binding()
    return CreateTaskAttemptCmd(
        attempt_id=_image_attempt_id(binding), project_id=_PROJECT, stage_id="image",
        partition_key=_PART, output_key="image",
        series_id=domain_ids.series_id(_PROJECT, "image", _PART),
        exact_refs=(binding,),
    )


def _spec(attempt_id: str) -> ProviderExecutionSpec:
    payload = _plan_payload()
    return ProviderExecutionSpec.from_plan(
        attempt_id=attempt_id, plan_ref=_plan_binding().to_ref(), plan_payload=payload,
        provider_id="fake", provider_version="1", estimated_cost=Decimal(10),
        currency="CNY", pricing_version="1", request_ref="req",
    )


def _rec_spec(aid: str) -> RecordExecutionSpecCmd:
    return RecordExecutionSpecCmd(attempt_id=aid, spec=_spec(aid))


def _provider_decider() -> TaskAttemptDecider:
    # image 无 executor -> 走 provider 流水线
    return TaskAttemptDecider(golden_compiled(), {})


def _image_at_waiting_provider() -> tuple[TaskAttemptDecider, Any, str]:
    d = _provider_decider()
    cmd = _create_image_cmd()
    s, _ = _apply(d, d.initial_state(), cmd)
    s, _ = _apply(d, s, _rec_spec(cmd.attempt_id))
    s, _ = _apply(d, s, MarkWaitingProviderCmd(attempt_id=cmd.attempt_id))
    return d, s, cmd.attempt_id


# --------------------------------------------------------------------------- #


def test_provider_stage_create_produces_no_candidate() -> None:
    d = _provider_decider()
    cmd = _create_image_cmd()
    s, dec = _apply(d, d.initial_state(), cmd)
    assert isinstance(dec, Accepted)
    types = [pe.payload.type for pe in dec.events]
    assert types == ["task_attempt_created", "task_inputs_bound"]  # 无候选
    assert s.status is _S.INPUTS_BOUND


def test_executor_stage_produces_candidate_synchronously() -> None:
    d = TaskAttemptDecider(
        golden_compiled(),
        {"plan": lambda stage, refs, part: _plan_payload()},
    )
    sb_series = domain_ids.series_id(_PROJECT, "storyboard", None)
    sb_binding = BindingItem.from_ref(
        requirement_key="storyboard", logical_slot="storyboard", partition_key=_PART,
        ref=ArtifactRef(
            artifact_id=domain_ids.artifact_id(sb_series, 1), series_id=sb_series,
            revision=1, digest="a" * 64,
        ),
        propagation_mode=PropagationMode.AGGREGATE,
    )
    tk = identity.task_key(_PROJECT, "plan", _PART)
    aid = identity.attempt_id(tk, identity.input_binding_digest((sb_binding,)), 0)
    cmd = CreateTaskAttemptCmd(
        attempt_id=aid, project_id=_PROJECT, stage_id="plan", partition_key=_PART,
        output_key="plan", series_id=domain_ids.series_id(_PROJECT, "plan", _PART),
        exact_refs=(sb_binding,),
    )
    s, dec = _apply(d, d.initial_state(), cmd)
    types = [pe.payload.type for pe in dec.events]  # type: ignore[union-attr]
    assert "artifact_candidate_produced" in types
    assert s.status is _S.SUCCEEDED


def test_record_spec_requires_membership() -> None:
    d = _provider_decider()
    cmd = _create_image_cmd()
    s, _ = _apply(d, d.initial_state(), cmd)
    # spec 的 plan_ref 不在 exact_refs -> plan_not_bound
    other_series = domain_ids.series_id(_PROJECT, "plan", "shot_99")
    other_payload = _plan_payload()
    other_ref = ArtifactRef(
        artifact_id=domain_ids.artifact_id(other_series, 1), series_id=other_series,
        revision=1, digest=digest(other_payload),
    )
    bad_spec = ProviderExecutionSpec.from_plan(
        attempt_id=cmd.attempt_id, plan_ref=other_ref, plan_payload=other_payload,
        provider_id="fake", provider_version="1", estimated_cost=Decimal(10),
        currency="CNY", pricing_version="1", request_ref="req",
    )
    dec = d.decide(s, RecordExecutionSpecCmd(attempt_id=cmd.attempt_id, spec=bad_spec))
    assert isinstance(dec, Rejected) and dec.code == "plan_not_bound"


def test_record_spec_advances_to_waiting_budget() -> None:
    d = _provider_decider()
    cmd = _create_image_cmd()
    s, _ = _apply(d, d.initial_state(), cmd)
    s, dec = _apply(d, s, _rec_spec(cmd.attempt_id))
    assert isinstance(dec, Accepted)
    assert s.status is _S.WAITING_BUDGET


def test_waiting_provider_then_result_succeeds() -> None:
    d, s, aid = _image_at_waiting_provider()
    assert s.status is _S.WAITING_PROVIDER
    spec = _spec(aid)
    payload = ImagePayload(shot_id=_PART, prompt="p", blob_ref="blob://final")
    s, dec = _apply(
        d, s,
        RecordProviderResultCmd(
            attempt_id=aid, operation_id=spec.operation_id, blob_ref="blob://final",
            payload=payload,
        ),
    )
    assert isinstance(dec, Accepted)
    assert [pe.payload.type for pe in dec.events] == ["artifact_candidate_produced"]
    assert s.status is _S.SUCCEEDED


def test_result_operation_mismatch_rejected() -> None:
    d, s, aid = _image_at_waiting_provider()
    payload = ImagePayload(shot_id=_PART, prompt="p", blob_ref="blob://final")
    dec = d.decide(
        s,
        RecordProviderResultCmd(
            attempt_id=aid, operation_id="wrong-op", blob_ref="blob://final",
            payload=payload,
        ),
    )
    assert isinstance(dec, Rejected) and dec.code == "operation_mismatch"


def test_result_blob_ref_mismatch_rejected() -> None:
    d, s, aid = _image_at_waiting_provider()
    spec = _spec(aid)
    payload = ImagePayload(shot_id=_PART, prompt="p", blob_ref="blob://X")
    dec = d.decide(
        s,
        RecordProviderResultCmd(
            attempt_id=aid, operation_id=spec.operation_id, blob_ref="blob://Y",
            payload=payload,
        ),
    )
    assert isinstance(dec, Rejected) and dec.code == "blob_ref_mismatch"


def test_result_idempotent_and_conflict() -> None:
    d, s, aid = _image_at_waiting_provider()
    spec = _spec(aid)

    def result(blob: str) -> RecordProviderResultCmd:
        return RecordProviderResultCmd(
            attempt_id=aid, operation_id=spec.operation_id, blob_ref=blob,
            payload=ImagePayload(shot_id=_PART, prompt="p", blob_ref=blob),
        )

    s, _ = _apply(d, s, result("blob://final"))
    _, dec = _apply(d, s, result("blob://final"))
    assert isinstance(dec, Accepted) and dec.events == ()
    with pytest.raises(IdempotencyConflict):
        d.decide(s, result("blob://OTHER"))


def test_blocked_and_reconciliation_transitions() -> None:
    d = _provider_decider()
    cmd = _create_image_cmd()
    s, _ = _apply(d, d.initial_state(), cmd)
    s, _ = _apply(d, s, _rec_spec(cmd.attempt_id))
    # WAITING_BUDGET -> BLOCKED
    blocked, dec = _apply(d, s, MarkBlockedCmd(attempt_id=cmd.attempt_id, reason="budget"))
    assert blocked.status is _S.BLOCKED
    # 回到 waiting_provider 再进 reconciliation
    s, _ = _apply(d, s, MarkWaitingProviderCmd(attempt_id=cmd.attempt_id))
    s, _ = _apply(d, s, MarkWaitingReconciliationCmd(attempt_id=cmd.attempt_id))
    assert s.status is _S.WAITING_RECONCILIATION
    # reconciliation 后可回 waiting_provider
    s, _ = _apply(d, s, MarkWaitingProviderCmd(attempt_id=cmd.attempt_id))
    assert s.status is _S.WAITING_PROVIDER


def test_result_bad_transition_from_inputs_bound() -> None:
    d = _provider_decider()
    cmd = _create_image_cmd()
    s, _ = _apply(d, d.initial_state(), cmd)  # INPUTS_BOUND
    spec = _spec(cmd.attempt_id)
    dec = d.decide(
        s,
        RecordProviderResultCmd(
            attempt_id=cmd.attempt_id, operation_id=spec.operation_id,
            blob_ref="b", payload=ImagePayload(shot_id=_PART, prompt="p", blob_ref="b"),
        ),
    )
    assert isinstance(dec, Rejected) and dec.code == "bad_transition"
