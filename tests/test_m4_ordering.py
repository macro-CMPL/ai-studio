"""M4:异步乱序保护 — status_revision(causal event global_position)。

证明:
1. status_revision < last_status_revision → Accepted(()) (幂等忽略过时命令)
2. status_revision > last_status_revision → 允许跳过中间状态(新命令先到)
3. 无 status_revision(None) → 向后兼容
4. 3→2→1 顺序最终收敛到事实序列 3 对应状态
5. UNKNOWN 先于 INITIATED 到达,不被永久拒绝
6. Result 先于 WaitingProvider 到达,最终保持 SUCCEEDED
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from studio.domain import ids as domain_ids
from studio.domain.artifacts import (
    ArtifactRef,
    ImagePayload,
    ImagePlanPayload,
    OperationParam,
    PlannedOperation,
)
from studio.domain.enums import PropagationMode, TaskAttemptStatus
from studio.kernel.decisions import Accepted
from studio.production import identity
from studio.production.attempt import TaskAttemptDecider
from studio.production.attempt_payloads import (
    MarkFailedCmd,
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

_P = "p"
_PART = "shot_01"
_S = TaskAttemptStatus


def _apply(decider: Any, state: Any, cmd: Any) -> tuple[Any, Any]:
    dec = decider.decide(state, cmd)
    if isinstance(dec, Accepted):
        for pe in dec.events:
            state = decider.evolve(state, pe.payload)
    return state, dec


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
    series = domain_ids.series_id(_P, "plan", _PART)
    ref = ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, 1), series_id=series,
        revision=1, digest=digest(_plan_payload()),
    )
    return BindingItem.from_ref(
        requirement_key="image_plan:plan", logical_slot="plan", partition_key=_PART,
        ref=ref, propagation_mode=PropagationMode.PARTITION_PRESERVING,
    )


def _image_attempt_id(binding: BindingItem) -> str:
    tk = identity.task_key(_P, "image", _PART)
    return identity.attempt_id(tk, identity.input_binding_digest((binding,)), 0)


def _spec(aid: str) -> ProviderExecutionSpec:
    return ProviderExecutionSpec.from_plan(
        attempt_id=aid, plan_ref=_plan_binding().to_ref(),
        plan_payload=_plan_payload(), provider_id="fake", provider_version="1",
        estimated_cost=Decimal(10), currency="CNY", pricing_version="1", request_ref="req",
    )


def _at_waiting_budget() -> tuple[TaskAttemptDecider, Any, str]:
    d = TaskAttemptDecider(golden_compiled(), {})
    binding = _plan_binding()
    aid = _image_attempt_id(binding)
    cmd = CreateTaskAttemptCmd(
        attempt_id=aid, project_id=_P, stage_id="image", partition_key=_PART,
        output_key="image", series_id=domain_ids.series_id(_P, "image", _PART),
        exact_refs=(binding,),
    )
    s, _ = _apply(d, d.initial_state(), cmd)
    s, _ = _apply(d, s, RecordExecutionSpecCmd(attempt_id=aid, spec=_spec(aid)))
    # Now at WAITING_BUDGET, last_status_revision set from spec event
    return d, s, aid


# --------------------------------------------------------------------------- #


def test_stale_command_idempotent_ignored() -> None:
    """revision=5 已设置;收到 revision=3 -> 幂等忽略。"""
    d, s, aid = _at_waiting_budget()
    # First move to WAITING_PROVIDER at revision=10
    s, _ = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=10))
    assert s.status is _S.WAITING_PROVIDER and s.last_status_revision == 10
    # Stale command: revision=5 < 10 -> ignored
    s2, dec = _apply(d, s, MarkWaitingReconciliationCmd(attempt_id=aid, status_revision=5))
    assert isinstance(dec, Accepted) and dec.events == ()
    assert s2.status is _S.WAITING_PROVIDER  # 未回退


def test_newer_command_skips_intermediate_states() -> None:
    """revision=20 到达时当前 revision=1;允许跳过中间状态直接迁移。"""
    d, s, aid = _at_waiting_budget()
    # revision=1(from spec evolve); 收到 revision=20 的 MarkFailed -> 直接迁移
    s2, dec = _apply(d, s, MarkFailedCmd(attempt_id=aid, reason="abort", status_revision=20))
    assert isinstance(dec, Accepted) and len(dec.events) == 1
    assert s2.status is _S.FAILED and s2.last_status_revision == 20


def test_no_revision_backward_compatible() -> None:
    """status_revision=None -> 不做乱序校验。"""
    d, s, aid = _at_waiting_budget()
    s2, dec = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=None))
    assert isinstance(dec, Accepted) and len(dec.events) == 1
    assert s2.status is _S.WAITING_PROVIDER


def test_order_3_2_1_converges_to_state_3() -> None:
    """事实顺序 INITIATED(1)→UNKNOWN(2)→SUBMITTED(3),命令 3→2→1 到达。

    最终状态应该是 SUBMITTED 对应的 WAITING_PROVIDER(revision=3)。
    """
    d, s, aid = _at_waiting_budget()
    # SUBMITTED arrives first (revision=30)
    s, dec = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=30))
    assert s.status is _S.WAITING_PROVIDER and s.last_status_revision == 30
    # UNKNOWN arrives second (revision=20) — stale, ignored
    s, dec = _apply(d, s, MarkWaitingReconciliationCmd(attempt_id=aid, status_revision=20))
    assert isinstance(dec, Accepted) and dec.events == ()
    assert s.status is _S.WAITING_PROVIDER  # 未回退
    # INITIATED arrives third (revision=10) — stale, ignored
    s, dec = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=10))
    assert isinstance(dec, Accepted) and dec.events == ()
    assert s.status is _S.WAITING_PROVIDER


def test_unknown_before_initiated_not_rejected() -> None:
    """UNKNOWN(revision=20) 先于 INITIATED(revision=10) 到达。

    UNKNOWN 对应 WAITING_RECONCILIATION 应直接被接受(跳过 WAITING_PROVIDER)。
    INITIATED 后到应被忽略。
    """
    d, s, aid = _at_waiting_budget()
    # UNKNOWN arrives first: WAITING_BUDGET -> WAITING_RECONCILIATION
    s, dec = _apply(d, s, MarkWaitingReconciliationCmd(attempt_id=aid, status_revision=20))
    assert isinstance(dec, Accepted) and len(dec.events) == 1
    assert s.status is _S.WAITING_RECONCILIATION
    # INITIATED arrives later (revision=10) -> stale, ignored
    s, dec = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=10))
    assert isinstance(dec, Accepted) and dec.events == ()
    assert s.status is _S.WAITING_RECONCILIATION


def test_result_before_waiting_provider_succeeds() -> None:
    """RecordProviderResult(revision=30) 先于 WaitingProvider(revision=10) 到达。

    Result 从 WAITING_BUDGET 被接受(status_revision 允许跳过中间状态);
    后到的 WaitingProvider 被忽略,最终保持 SUCCEEDED。
    """
    d, s, aid = _at_waiting_budget()
    spec = _spec(aid)
    payload = ImagePayload(shot_id=_PART, prompt="p", blob_ref="blob://final")
    # Result arrives first (revision=30)
    s, dec = _apply(d, s, RecordProviderResultCmd(
        attempt_id=aid, operation_id=spec.operation_id, blob_ref="blob://final",
        payload=payload, status_revision=30,
    ))
    assert isinstance(dec, Accepted) and len(dec.events) == 1
    assert s.status is _S.SUCCEEDED
    # WaitingProvider arrives later (revision=10) -> terminal state, stale -> ignored
    s2, dec2 = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, status_revision=10))
    assert isinstance(dec2, Accepted) and dec2.events == ()
    assert s2.status is _S.SUCCEEDED


def test_result_without_revision_from_waiting_budget_accepted() -> None:
    """RecordProviderResult(None revision) 从 WAITING_BUDGET 也被接受(跳过中间)。"""
    d, s, aid = _at_waiting_budget()
    spec = _spec(aid)
    payload = ImagePayload(shot_id=_PART, prompt="p", blob_ref="blob://final")
    s, dec = _apply(d, s, RecordProviderResultCmd(
        attempt_id=aid, operation_id=spec.operation_id, blob_ref="blob://final",
        payload=payload, status_revision=None,
    ))
    assert isinstance(dec, Accepted) and len(dec.events) == 1
    assert s.status is _S.SUCCEEDED
