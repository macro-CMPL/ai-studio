"""M4 步骤1-2:Budget / ProviderOperation 纯 Decider 的状态机穷举迁移测试。"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from studio.domain import ids as domain_ids
from studio.domain.artifacts import (
    ArtifactRef,
    ImagePlanPayload,
    OperationParam,
    PlannedOperation,
)
from studio.domain.enums import LedgerDirection, ProviderOpStatus
from studio.kernel.decisions import Accepted, Rejected
from studio.kernel.errors import IdempotencyConflict
from studio.production import identity
from studio.production.budget import (
    BudgetDecider,
    BudgetOverrunRecordedEvt,
    InitializeBudgetCmd,
    ReleaseBudgetCmd,
    ReserveBudgetCmd,
    SettleBudgetCmd,
)
from studio.production.execution_spec import ProviderExecutionSpec
from studio.production.provider_op import (
    AbortBeforeSubmissionCmd,
    ClaimSubmissionCmd,
    InitiateProviderOpCmd,
    ProviderOperationDecider,
    ProviderResultRef,
    ReconcileSucceededCmd,
    RecordFailedCmd,
    RecordSubmissionUnknownCmd,
    RecordSubmittedCmd,
    RecordSucceededCmd,
)
from studio.serialization import digest


def _apply(decider: Any, state: Any, cmd: Any) -> tuple[Any, Any]:
    decision = decider.decide(state, cmd)
    if isinstance(decision, Accepted):
        for pe in decision.events:
            state = decider.evolve(state, pe.payload)
    return state, decision


def _event_types(decision: Any) -> list[str]:
    return [pe.payload.type for pe in decision.events]


# --------------------------------------------------------------------------- #
# Budget
# --------------------------------------------------------------------------- #


def _init_budget(total: str = "100") -> tuple[BudgetDecider, Any]:
    d = BudgetDecider()
    state, _ = _apply(
        d, d.initial_state(),
        InitializeBudgetCmd(project_id="p", total=Decimal(total), currency="CNY"),
    )
    return d, state


def _reserve(op: str, amount: str, quote: str = "a" * 64) -> ReserveBudgetCmd:
    return ReserveBudgetCmd(
        project_id="p", operation_id=op, amount=Decimal(amount), currency="CNY",
        quote_digest=quote,
    )


def test_budget_reserve_and_available() -> None:
    d, s = _init_budget("100")
    s, dec = _apply(d, s, _reserve("op1", "30"))
    assert isinstance(dec, Accepted)
    assert s.available == Decimal(70)


def test_budget_reserve_idempotent_and_conflict() -> None:
    d, s = _init_budget()
    s, _ = _apply(d, s, _reserve("op1", "30"))
    _, dec = _apply(d, s, _reserve("op1", "30"))
    assert isinstance(dec, Accepted) and dec.events == ()
    with pytest.raises(IdempotencyConflict):
        d.decide(s, _reserve("op1", "40"))


def test_budget_reserve_declined_when_insufficient() -> None:
    d, s = _init_budget("50")
    s, dec = _apply(d, s, _reserve("op1", "80"))
    assert _event_types(dec) == ["budget_reservation_declined"]
    assert s.reservation("op1") is None  # 未占用


def test_budget_reserve_currency_mismatch_rejected() -> None:
    d, s = _init_budget()
    dec = d.decide(
        s,
        ReserveBudgetCmd(project_id="p", operation_id="op1", amount=Decimal(1),
                         currency="USD", quote_digest="a" * 64),
    )
    assert isinstance(dec, Rejected) and dec.code == "currency_mismatch"


def _settle(op: str, actual: str, quote: str = "a" * 64) -> SettleBudgetCmd:
    return SettleBudgetCmd(
        project_id="p", operation_id=op, actual=Decimal(actual), currency="CNY",
        quote_digest=quote,
    )


def test_budget_settle_under_reserve_captures_and_releases() -> None:
    d, s = _init_budget("100")
    s, _ = _apply(d, s, _reserve("op1", "30"))
    s, dec = _apply(d, s, _settle("op1", "20"))
    assert _event_types(dec) == [
        "budget_captured",
        "budget_released",
        "budget_settlement_completed",
    ]
    assert s.captured_total == Decimal(20)
    assert s.available == Decimal(80)  # 30 预留释放,仅扣 20


def test_budget_settle_equal_reserve() -> None:
    d, s = _init_budget("100")
    s, _ = _apply(d, s, _reserve("op1", "30"))
    _, dec = _apply(d, s, _settle("op1", "30"))
    assert _event_types(dec) == ["budget_captured", "budget_settlement_completed"]


def test_budget_settle_over_reserve_records_overrun_and_blocks() -> None:
    d, s = _init_budget("100")
    s, _ = _apply(d, s, _reserve("op1", "30"))
    s, dec = _apply(d, s, _settle("op1", "45"))
    assert _event_types(dec) == [
        "budget_captured", "budget_overrun_recorded", "budget_settlement_completed",
    ]
    assert s.overrun is True
    # 超预算后新的 reserve 被拒
    _, dec2 = _apply(d, s, _reserve("op2", "1"))
    assert _event_types(dec2) == ["budget_reservation_declined"]


def test_budget_capture_never_rejected_even_if_over_available() -> None:
    d, s = _init_budget("40")
    s, _ = _apply(d, s, _reserve("op1", "40"))
    # 实际远超预留与可用 -> 仍必须 CAPTURE + OVERRUN,不拒绝
    s, dec = _apply(d, s, _settle("op1", "100"))
    assert "budget_captured" in _event_types(dec)
    assert isinstance(dec, Accepted)
    assert any(isinstance(pe.payload, BudgetOverrunRecordedEvt) for pe in dec.events)


def test_budget_settle_idempotent_and_conflict() -> None:
    d, s = _init_budget()
    s, _ = _apply(d, s, _reserve("op1", "30"))
    s, _ = _apply(d, s, _settle("op1", "20"))
    _, dec = _apply(d, s, _settle("op1", "20"))
    assert isinstance(dec, Accepted) and dec.events == ()
    with pytest.raises(IdempotencyConflict):
        d.decide(s, _settle("op1", "25"))


def test_budget_settle_unknown_reservation_rejected() -> None:
    d, s = _init_budget()
    dec = d.decide(s, _settle("ghost", "10"))
    assert isinstance(dec, Rejected) and dec.code == "unknown_reservation"


def test_budget_release_uncharged() -> None:
    d, s = _init_budget("100")
    s, _ = _apply(d, s, _reserve("op1", "30"))
    s, dec = _apply(
        d, s, ReleaseBudgetCmd(project_id="p", operation_id="op1", quote_digest="a" * 64)
    )
    assert _event_types(dec) == ["budget_released", "budget_settlement_completed"]
    assert s.available == Decimal(100)  # 全额释放


def test_budget_adjust_credit_debit() -> None:
    d, s = _init_budget("100")
    s, _ = _apply(d, s, _adjust("e1", LedgerDirection.CREDIT, "50"))
    assert s.available == Decimal(150)
    s, _ = _apply(d, s, _adjust("e2", LedgerDirection.DEBIT, "20"))
    assert s.available == Decimal(130)


def _adjust(entry: str, direction: LedgerDirection, amount: str) -> Any:
    from studio.production.budget import AdjustBudgetCmd

    return AdjustBudgetCmd(
        project_id="p", entry_id=entry, direction=direction, amount=Decimal(amount),
        currency="CNY", reason="manual", authority_ref="admin",
    )


# --------------------------------------------------------------------------- #
# ProviderOperation
# --------------------------------------------------------------------------- #


def _plan_payload(op_key: str = "shot_01:image:v0") -> ImagePlanPayload:
    return ImagePlanPayload(
        operations=(
            PlannedOperation(
                logical_operation_key=op_key, op_type="gen",
                params=(OperationParam(key="shot", value="shot_01"),),
            ),
        )
    )


def _plan_ref(payload: ImagePlanPayload) -> ArtifactRef:
    series = domain_ids.series_id("proj", "plan", "shot_01")
    return ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, 1), series_id=series,
        revision=1, digest=digest(payload),
    )


def _spec(op_key: str = "shot_01:image:v0", cost: str = "10") -> ProviderExecutionSpec:
    payload = _plan_payload(op_key)
    return ProviderExecutionSpec.from_plan(
        attempt_id="att-1", plan_ref=_plan_ref(payload), plan_payload=payload,
        provider_id="fake", provider_version="1", estimated_cost=Decimal(cost),
        currency="CNY", pricing_version="1", request_ref="req://1",
    )


def _op_id() -> str:
    s = _spec()
    return identity.operation_id(s.attempt_id, s.logical_operation_key)


def _result() -> ProviderResultRef:
    return ProviderResultRef(blob_ref="blob://x", digest="b" * 64)


def _succ(op: str, event_id: str, cost: str = "10") -> RecordSucceededCmd:
    return RecordSucceededCmd(
        operation_id=op, result_ref=_result(), cost_actual=Decimal(cost),
        cost_currency="CNY", provider_event_id=event_id,
    )


def _initiated() -> tuple[ProviderOperationDecider, Any]:
    d = ProviderOperationDecider()
    op = _op_id()
    s, _ = _apply(d, d.initial_state(), InitiateProviderOpCmd(operation_id=op, spec=_spec()))
    return d, s


def test_provider_initiate_forged_id_rejected() -> None:
    d = ProviderOperationDecider()
    dec = d.decide(d.initial_state(), InitiateProviderOpCmd(operation_id="forged", spec=_spec()))
    assert isinstance(dec, Rejected) and dec.code == "forged_operation_id"


def test_provider_happy_path_transitions() -> None:
    d, s = _initiated()
    op = _op_id()
    assert s.status is ProviderOpStatus.INITIATED
    s, _ = _apply(d, s, ClaimSubmissionCmd(operation_id=op))
    assert s.status is ProviderOpStatus.CLAIMED
    s, _ = _apply(d, s, RecordSubmittedCmd(operation_id=op, job_id="J1", provider_event_id="e1"))
    assert s.status is ProviderOpStatus.SUBMITTED and s.job_id == "J1"
    s, _ = _apply(
        d, s,
        _succ(op, "e2"),
    )
    assert s.status is ProviderOpStatus.SUCCEEDED


def test_provider_claim_bad_transition() -> None:
    d, s = _initiated()
    op = _op_id()
    s, _ = _apply(d, s, ClaimSubmissionCmd(operation_id=op))
    s, _ = _apply(d, s, RecordSubmittedCmd(operation_id=op, job_id="J1", provider_event_id="e1"))
    # claim 已提交后不允许
    dec = d.decide(s, ClaimSubmissionCmd(operation_id=op))
    assert isinstance(dec, Rejected) and dec.code == "bad_transition"


def test_provider_abort_tombstone_rejects_late_claim() -> None:
    d, s = _initiated()
    op = _op_id()
    s, _ = _apply(d, s, AbortBeforeSubmissionCmd(operation_id=op, reason="orphan"))
    assert s.status is ProviderOpStatus.ABORTED
    dec = d.decide(s, ClaimSubmissionCmd(operation_id=op))
    assert isinstance(dec, Rejected)


def test_provider_webhook_dedup_and_terminal_conflict() -> None:
    d, s = _initiated()
    op = _op_id()
    s, _ = _apply(d, s, ClaimSubmissionCmd(operation_id=op))
    s, _ = _apply(d, s, RecordSubmittedCmd(operation_id=op, job_id="J1", provider_event_id="e1"))
    s, _ = _apply(
        d, s,
        _succ(op, "e2"),
    )
    # 重复 webhook(同 provider_event_id)-> 去重空
    _, dec = _apply(
        d, s,
        _succ(op, "e2"),
    )
    assert isinstance(dec, Accepted) and dec.events == ()
    # 同终态不同 cost -> 冲突
    with pytest.raises(IdempotencyConflict):
        d.decide(
            s,
            _succ(op, "e3", cost="99"),
        )


def test_provider_submission_unknown_parked_then_reconciled() -> None:
    d, s = _initiated()
    op = _op_id()
    s, _ = _apply(d, s, ClaimSubmissionCmd(operation_id=op))
    s, _ = _apply(d, s, RecordSubmissionUnknownCmd(operation_id=op, reason="no lookup"))
    assert s.status is ProviderOpStatus.SUBMISSION_UNKNOWN
    # 人工/对账恢复到 SUCCEEDED
    s, dec = _apply(
        d, s,
        ReconcileSucceededCmd(
            operation_id=op, result_ref=_result(), cost_actual=Decimal(10),
            cost_currency="CNY", authority_ref="ops-1",
        ),
    )
    assert s.status is ProviderOpStatus.SUCCEEDED
    assert _event_types(dec) == ["provider_op_succeeded"]


def test_provider_failed_from_submitted() -> None:
    d, s = _initiated()
    op = _op_id()
    s, _ = _apply(d, s, ClaimSubmissionCmd(operation_id=op))
    s, _ = _apply(d, s, RecordSubmittedCmd(operation_id=op, job_id="J1", provider_event_id="e1"))
    s, _ = _apply(
        d, s,
        RecordFailedCmd(operation_id=op, charged=True, cost_actual=Decimal(10), cost_currency="CNY",
                        provider_event_id="e2"),
    )
    assert s.status is ProviderOpStatus.FAILED and s.charged is True


# --------------------------------------------------------------------------- #
# 对抗:owner 身份 / 内容指纹 / claim 后禁 abort / Plan 契约
# --------------------------------------------------------------------------- #


def test_provider_reinit_same_op_different_spec_conflict() -> None:
    d, s = _initiated()
    op = _op_id()
    # 同 operation_id、不同 spec(不同报价) -> 冲突
    other = _spec(cost="99")
    with pytest.raises(IdempotencyConflict):
        d.decide(s, InitiateProviderOpCmd(operation_id=op, spec=other))


def test_provider_command_operation_id_mismatch_rejected() -> None:
    d, s = _initiated()
    dec = d.decide(s, ClaimSubmissionCmd(operation_id="wrong-op"))
    assert isinstance(dec, Rejected) and dec.code == "wrong_operation"


def test_provider_same_event_id_different_payload_conflict() -> None:
    d, s = _initiated()
    op = _op_id()
    s, _ = _apply(d, s, ClaimSubmissionCmd(operation_id=op))
    s, _ = _apply(d, s, RecordSubmittedCmd(operation_id=op, job_id="J1", provider_event_id="e1"))
    # 同 provider_event_id 但 job 不同 -> 冲突(不是静默 no-op)
    with pytest.raises(IdempotencyConflict):
        d.decide(s, RecordSubmittedCmd(operation_id=op, job_id="J2", provider_event_id="e1"))


def test_provider_claimed_cannot_abort() -> None:
    d, s = _initiated()
    op = _op_id()
    s, _ = _apply(d, s, ClaimSubmissionCmd(operation_id=op))
    dec = d.decide(s, AbortBeforeSubmissionCmd(operation_id=op, reason="x"))
    assert isinstance(dec, Rejected) and dec.code == "bad_transition"


def test_provider_abort_then_late_initiate_rejected() -> None:
    d = ProviderOperationDecider()
    op = _op_id()
    s, _ = _apply(d, d.initial_state(), AbortBeforeSubmissionCmd(operation_id=op, reason="orphan"))
    assert s.status is ProviderOpStatus.ABORTED
    dec = d.decide(s, InitiateProviderOpCmd(operation_id=op, spec=_spec()))
    assert isinstance(dec, Rejected) and dec.code == "aborted"


def test_provider_uncharged_failure_with_nonzero_actual_rejected() -> None:
    d, s = _initiated()
    op = _op_id()
    s, _ = _apply(d, s, ClaimSubmissionCmd(operation_id=op))
    s, _ = _apply(d, s, RecordSubmittedCmd(operation_id=op, job_id="J1", provider_event_id="e1"))
    dec = d.decide(
        s,
        RecordFailedCmd(
            operation_id=op, charged=False, cost_actual=Decimal(5),
            cost_currency="CNY", provider_event_id="e2",
        ),
    )
    assert isinstance(dec, Rejected) and dec.code == "charged_cost_mismatch"


def test_execution_spec_requires_exactly_one_operation() -> None:
    two_ops = ImagePlanPayload(
        operations=(
            PlannedOperation(logical_operation_key="a", op_type="gen", params=()),
            PlannedOperation(logical_operation_key="b", op_type="gen", params=()),
        )
    )
    with pytest.raises(ValueError, match="恰好一个"):
        ProviderExecutionSpec.from_plan(
            attempt_id="att-1", plan_ref=_plan_ref(two_ops), plan_payload=two_ops,
            provider_id="fake", provider_version="1", estimated_cost=Decimal(10),
            currency="CNY", pricing_version="1", request_ref="req://1",
        )


def test_execution_spec_membership_check() -> None:
    from studio.domain.enums import PropagationMode
    from studio.production.values import BindingItem

    spec = _spec()
    good = BindingItem.from_ref(
        requirement_key="plan", logical_slot="plan", partition_key="shot_01",
        ref=spec.plan_ref, propagation_mode=PropagationMode.PARTITION_PRESERVING,
    )
    spec.verify_membership((good,))  # 不抛
    other_series = domain_ids.series_id("proj", "plan", "shot_99")
    wrong = BindingItem.from_ref(
        requirement_key="plan", logical_slot="plan", partition_key="shot_99",
        ref=ArtifactRef(
            artifact_id=domain_ids.artifact_id(other_series, 1), series_id=other_series,
            revision=1, digest="d" * 64,
        ),
        propagation_mode=PropagationMode.PARTITION_PRESERVING,
    )
    with pytest.raises(ValueError, match="不属于"):
        spec.verify_membership((wrong,))


# --------------------------------------------------------------------------- #
# 对抗:预算 owner / release quote / adjustment 指纹
# --------------------------------------------------------------------------- #


def test_budget_wrong_project_rejected() -> None:
    d, s = _init_budget("100")
    dec = d.decide(
        s,
        ReserveBudgetCmd(project_id="other", operation_id="op1", amount=Decimal(1),
                         currency="CNY", quote_digest="a" * 64),
    )
    assert isinstance(dec, Rejected) and dec.code == "wrong_project"


def test_budget_release_wrong_quote_conflict() -> None:
    d, s = _init_budget("100")
    s, _ = _apply(d, s, _reserve("op1", "30", quote="a" * 64))
    with pytest.raises(IdempotencyConflict):
        d.decide(s, ReleaseBudgetCmd(project_id="p", operation_id="op1", quote_digest="b" * 64))


def test_budget_reserve_same_op_different_currency_conflict() -> None:
    d, s = _init_budget("100")
    s, _ = _apply(d, s, _reserve("op1", "30", quote="a" * 64))
    # 同 op 换币种 -> 幂等冲突(而非 currency rejection)
    with pytest.raises(IdempotencyConflict):
        d.decide(
            s,
            ReserveBudgetCmd(project_id="p", operation_id="op1", amount=Decimal(30),
                             currency="USD", quote_digest="a" * 64),
        )


def test_budget_adjustment_same_entry_different_payload_conflict() -> None:
    d, s = _init_budget("100")
    s, _ = _apply(d, s, _adjust("e1", LedgerDirection.CREDIT, "50"))
    with pytest.raises(IdempotencyConflict):
        d.decide(s, _adjust("e1", LedgerDirection.DEBIT, "50"))




# --------------------------------------------------------------------------- #
# 对抗:Plan 契约不可绕过 + 币种端到端指纹
# --------------------------------------------------------------------------- #


def test_spec_direct_construction_cannot_bypass_multi_operation() -> None:
    from pydantic import ValidationError

    two_ops = ImagePlanPayload(
        operations=(
            PlannedOperation(logical_operation_key="a", op_type="gen", params=()),
            PlannedOperation(logical_operation_key="b", op_type="gen", params=()),
        )
    )
    ref = _plan_ref(two_ops)
    with pytest.raises(ValidationError):
        ProviderExecutionSpec(
            attempt_id="att-1", logical_operation_key="a", provider_id="fake",
            provider_version="1", plan_ref=ref, plan_payload=two_ops,
            request_ref="r", request_digest=digest(two_ops.operations[0]),
            estimated_cost=Decimal(10), currency="CNY", pricing_version="1",
        )


def test_spec_plan_payload_digest_must_equal_plan_ref() -> None:
    from pydantic import ValidationError

    payload = _plan_payload()
    series = domain_ids.series_id("proj", "plan", "shot_01")
    wrong_ref = ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, 1), series_id=series,
        revision=1, digest="d" * 64,  # 与 payload 摘要不符
    )
    with pytest.raises(ValidationError):
        ProviderExecutionSpec(
            attempt_id="att-1", logical_operation_key=payload.operations[0].logical_operation_key,
            provider_id="fake", provider_version="1", plan_ref=wrong_ref, plan_payload=payload,
            request_ref="r", request_digest=digest(payload.operations[0]),
            estimated_cost=Decimal(10), currency="CNY", pricing_version="1",
        )


def test_membership_requires_full_artifact_ref_equality() -> None:
    from studio.domain.enums import PropagationMode
    from studio.production.values import BindingItem

    spec = _spec()
    # 同 artifact_id、伪造 digest -> to_ref() != plan_ref -> 拒绝
    forged = spec.plan_ref.model_copy(update={"digest": "f" * 64})
    binding = BindingItem.from_ref(
        requirement_key="plan", logical_slot="plan", partition_key="shot_01",
        ref=forged, propagation_mode=PropagationMode.PARTITION_PRESERVING,
    )
    with pytest.raises(ValueError, match="不属于"):
        spec.verify_membership((binding,))


def test_non_sha_quote_digest_rejected() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ReserveBudgetCmd(
            project_id="p", operation_id="op1", amount=Decimal(1), currency="CNY",
            quote_digest="not-a-sha",
        )


def test_budget_settle_same_op_different_currency_conflict() -> None:
    d, s = _init_budget("100")
    s, _ = _apply(d, s, _reserve("op1", "30"))
    with pytest.raises(IdempotencyConflict):
        d.decide(
            s,
            SettleBudgetCmd(project_id="p", operation_id="op1", actual=Decimal(30),
                            currency="USD", quote_digest="a" * 64),
        )


def test_provider_cost_currency_must_match_spec() -> None:
    d, s = _initiated()
    op = _op_id()
    s, _ = _apply(d, s, ClaimSubmissionCmd(operation_id=op))
    s, _ = _apply(d, s, RecordSubmittedCmd(operation_id=op, job_id="J1", provider_event_id="e1"))
    dec = d.decide(
        s,
        RecordSucceededCmd(
            operation_id=op, result_ref=_result(), cost_actual=Decimal(10),
            cost_currency="USD", provider_event_id="e2",
        ),
    )
    assert isinstance(dec, Rejected) and dec.code == "currency_mismatch"

