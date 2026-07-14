"""M4 步骤3 集成:Golden 流水线跑通付费 provider 路径(ActivityWorker 手工桩)。

演示的不只是"能调用生成 API",而是付费副作用在重复投递、预算竞争、孤儿回收下仍正确。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from m4_helpers import (
    build_m4_stack,
    claim_command,
    init_budget_command,
    init_pipeline_command,
    initiated_ops,
    reconcile_submitted_command,
    reconcile_succeeded_command,
    record_unknown_command,
    submit_command,
    succeed_command,
    tick_command,
)
from studio.domain import ids as domain_ids
from studio.production.attempt_payloads import AttemptWaitingReconciliationEvt
from studio.production.budget import BudgetCapturedEvt, BudgetReleasedEvt
from studio.production.projections import ArtifactLifecycleView
from studio.production.provider_op import ProviderOperationAbortedEvt

_T0 = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


def _payloads(stack: Any, cls: type) -> list[Any]:
    return [e.payload for e in stack.db.state.events if isinstance(e.payload, cls)]


def _image_current(stack: Any, shot: str) -> Any:
    view = ArtifactLifecycleView.build(stack.db.state.events)
    return view.current_ref(domain_ids.series_id("p", "image", shot))


# --------------------------------------------------------------------------- #


def test_golden_provider_pipeline_end_to_end() -> None:
    stack = build_m4_stack()
    stack.bus.publish(init_budget_command("p"))
    stack.bus.publish(init_pipeline_command("p"))
    stack.driver.run_until_quiescent()

    ops = initiated_ops(stack)
    assert len(ops) == 2  # shot_01 + shot_02 各一个 provider 操作

    # 手工代替 ActivityWorker:认领 -> 提交 -> 成功
    for op, spec in ops:
        stack.bus.publish(claim_command(op))
        stack.bus.publish(submit_command(op))
        stack.bus.publish(succeed_command(op, spec))
    stack.driver.run_until_quiescent()

    for shot in ("shot_01", "shot_02"):
        assert _image_current(stack, shot) is not None, f"{shot} 图像未接受"

    captured = _payloads(stack, BudgetCapturedEvt)
    assert len(captured) == 2
    assert sum(c.amount for c in captured) == Decimal("20")


def test_duplicate_activity_delivery_is_idempotent() -> None:
    stack = build_m4_stack()
    stack.bus.publish(init_budget_command("p"))
    stack.bus.publish(init_pipeline_command("p"))
    stack.driver.run_until_quiescent()
    ops = initiated_ops(stack)

    for _ in range(2):  # 二次投递(重复 webhook / relay 重投)
        for op, spec in ops:
            stack.bus.publish(claim_command(op))
            stack.bus.publish(submit_command(op))
            stack.bus.publish(succeed_command(op, spec))
        stack.driver.run_until_quiescent()

    # 仍然恰好扣费两次、恰好两张图
    captured = _payloads(stack, BudgetCapturedEvt)
    assert len(captured) == 2
    assert sum(c.amount for c in captured) == Decimal("20")
    for shot in ("shot_01", "shot_02"):
        assert _image_current(stack, shot) is not None


def test_unknown_recovery_reaches_success() -> None:
    stack = build_m4_stack()
    stack.bus.publish(init_budget_command("p"))
    stack.bus.publish(init_pipeline_command("p"))
    stack.driver.run_until_quiescent()
    ops = initiated_ops(stack)

    # 提交结果未知 -> 对账确认已提交 -> 对账确认成功
    for op, spec in ops:
        stack.bus.publish(claim_command(op))
        stack.bus.publish(record_unknown_command(op))
        stack.bus.publish(reconcile_submitted_command(op))
        stack.bus.publish(reconcile_succeeded_command(op, spec))
    stack.driver.run_until_quiescent()

    # UNKNOWN 已接入 PM:attempt 经过 WAITING_RECONCILIATION
    assert len(_payloads(stack, AttemptWaitingReconciliationEvt)) == 2
    for shot in ("shot_01", "shot_02"):
        assert _image_current(stack, shot) is not None
    assert len(_payloads(stack, BudgetCapturedEvt)) == 2


def test_orphan_reconcile_recycles_and_releases() -> None:
    stack = build_m4_stack()
    stack.bus.publish(init_budget_command("p"))
    stack.bus.publish(init_pipeline_command("p"))
    stack.driver.run_until_quiescent()
    ops = initiated_ops(stack)
    assert len(ops) == 2  # 已预留 + 已 INITIATED,但没有 ActivityWorker 推进

    # 第一轮 tick:记录 first_seen,尚未到回收阈值 -> 不动作
    stack.bus.publish(tick_command(_T0, 1))
    stack.driver.run_until_quiescent()
    assert not _payloads(stack, ProviderOperationAbortedEvt)

    # 第二轮 tick:超过 recycle_after -> Abort -> (第二轮)Release
    stack.bus.publish(tick_command(_T0 + timedelta(minutes=15), 2))
    stack.driver.run_until_quiescent()

    aborted = _payloads(stack, ProviderOperationAbortedEvt)
    released = _payloads(stack, BudgetReleasedEvt)
    assert len(aborted) == 2
    assert len(released) == 2
    # 未发生真实扣费:全部释放,没有 capture
    assert not _payloads(stack, BudgetCapturedEvt)
    # 图像未接受(attempt 被标记失败)
    for shot in ("shot_01", "shot_02"):
        assert _image_current(stack, shot) is None
