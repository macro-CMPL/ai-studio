"""M4:异步乱序保护 — provider_phase 单调递增,过时命令幂等忽略。

证明:
1. min_provider_phase < current_phase → Accepted(()) (幂等忽略,不拒绝/不回退)
2. min_provider_phase == current_phase → 正常迁移(或被 allowed 集合拒绝)
3. 无 min_provider_phase(None) → 向后兼容(不做 phase 校验)
4. SchedulingPM 每次推进状态都递增 phase,不同事件顺序下收敛到正确终态
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from studio.domain import ids as domain_ids
from studio.domain.artifacts import (
    ArtifactRef,
    ImagePlanPayload,
    OperationParam,
    PlannedOperation,
)
from studio.domain.enums import PropagationMode, TaskAttemptStatus
from studio.kernel.decisions import Accepted
from studio.production import identity
from studio.production.attempt import TaskAttemptDecider
from studio.production.attempt_payloads import (
    MarkBlockedCmd,
    MarkFailedCmd,
    MarkWaitingProviderCmd,
    MarkWaitingReconciliationCmd,
    RecordExecutionSpecCmd,
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


def _at_waiting_provider() -> tuple[TaskAttemptDecider, Any, str]:
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
    # phase is now 1 (after WAITING_BUDGET)
    s, _ = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, min_provider_phase=1))
    # phase is now 2
    return d, s, aid


# --------------------------------------------------------------------------- #


def test_stale_command_idempotent_ignored() -> None:
    """phase=2(WAITING_PROVIDER), 收到 min_phase=1 的 MarkWaitingReconciliation -> 忽略。"""
    d, s, aid = _at_waiting_provider()
    assert s.provider_phase == 2
    # 过时命令:phase=1 < current=2 -> 幂等忽略
    s2, dec = _apply(
        d, s, MarkWaitingReconciliationCmd(attempt_id=aid, min_provider_phase=1)
    )
    assert isinstance(dec, Accepted) and dec.events == ()
    assert s2.status is _S.WAITING_PROVIDER  # 状态未回退


def test_current_phase_command_succeeds() -> None:
    """phase=2(WAITING_PROVIDER), 收到 min_phase=2 的 MarkWaitingReconciliation -> 正常迁移。"""
    d, s, aid = _at_waiting_provider()
    s2, dec = _apply(
        d, s, MarkWaitingReconciliationCmd(attempt_id=aid, min_provider_phase=2)
    )
    assert isinstance(dec, Accepted) and len(dec.events) == 1
    assert s2.status is _S.WAITING_RECONCILIATION
    assert s2.provider_phase == 3


def test_no_phase_backward_compatible() -> None:
    """min_provider_phase=None(legacy/M3) -> 不做 phase 校验,直接按 allowed 决定。"""
    d, s, aid = _at_waiting_provider()
    # None -> allowed 里有 WAITING_PROVIDER -> 正常
    s2, dec = _apply(
        d, s, MarkWaitingReconciliationCmd(attempt_id=aid, min_provider_phase=None)
    )
    assert isinstance(dec, Accepted) and len(dec.events) == 1
    assert s2.status is _S.WAITING_RECONCILIATION


def test_stale_mark_failed_ignored() -> None:
    """MarkFailed 的 stale 命令也被幂等忽略,不会永久拒绝。"""
    d, s, aid = _at_waiting_provider()
    s2, dec = _apply(d, s, MarkFailedCmd(attempt_id=aid, reason="old", min_provider_phase=0))
    assert isinstance(dec, Accepted) and dec.events == ()
    assert s2.status is _S.WAITING_PROVIDER


def test_phase_increments_through_lifecycle() -> None:
    """完整生命周期:phase 单调递增,每次迁移 +1。"""
    d = TaskAttemptDecider(golden_compiled(), {})
    binding = _plan_binding()
    aid = _image_attempt_id(binding)
    cmd = CreateTaskAttemptCmd(
        attempt_id=aid, project_id=_P, stage_id="image", partition_key=_PART,
        output_key="image", series_id=domain_ids.series_id(_P, "image", _PART),
        exact_refs=(binding,),
    )
    s, _ = _apply(d, d.initial_state(), cmd)
    assert s.provider_phase == 0  # create 不增 phase

    s, _ = _apply(d, s, RecordExecutionSpecCmd(attempt_id=aid, spec=_spec(aid)))
    assert s.provider_phase == 1  # WAITING_BUDGET

    s, _ = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, min_provider_phase=1))
    assert s.provider_phase == 2  # WAITING_PROVIDER

    s, _ = _apply(d, s, MarkWaitingReconciliationCmd(attempt_id=aid, min_provider_phase=2))
    assert s.provider_phase == 3  # WAITING_RECONCILIATION

    s, _ = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, min_provider_phase=3))
    assert s.provider_phase == 4  # back to WAITING_PROVIDER

    s, _ = _apply(d, s, MarkFailedCmd(attempt_id=aid, reason="fail", min_provider_phase=4))
    assert s.provider_phase == 5  # FAILED
    assert s.status is _S.FAILED


def test_competing_commands_last_wins() -> None:
    """模拟竞争:MarkBlocked(phase=1) vs MarkWaitingProvider(phase=1),先到者胜。"""
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
    assert s.provider_phase == 1

    # MarkWaitingProvider 先到:成功,phase->2
    s, dec = _apply(d, s, MarkWaitingProviderCmd(attempt_id=aid, min_provider_phase=1))
    assert isinstance(dec, Accepted) and len(dec.events) == 1
    assert s.status is _S.WAITING_PROVIDER and s.provider_phase == 2

    # MarkBlocked 迟到:phase=1 < current=2 -> 幂等忽略,不永久 Rejected
    s2, dec2 = _apply(d, s, MarkBlockedCmd(attempt_id=aid, reason="late", min_provider_phase=1))
    assert isinstance(dec2, Accepted) and dec2.events == ()
    assert s2.status is _S.WAITING_PROVIDER  # 未回退
