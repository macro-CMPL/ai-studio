"""Step 10:M5 黄金场景端到端。

创建项目 -> 剧本 -> 开发包 -> 分镜 -> 两镜头提示词质检通过 -> 出图 -> 结果质检通过 ->
第一轮阶段质检发现 shot_02 不一致 -> 撤销 shot_02 图像第一版 -> 仅重做 shot_02(相同输入,
代数+1) -> 结果质检通过 -> 第二轮阶段质检通过 -> 交付。

验证:三层质检、精确撤销、精确分区返工、相同输入新身份、二次扣费、总扣费=30、交付一次。
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from m5_helpers import build_m5_stack, init_budget_command, init_pipeline_command
from studio.domain.enums import AcceptanceStatus, ArtifactType
from studio.production.budget import BudgetSettlementCompletedEvt
from studio.production.payloads import (
    ArtifactAcceptanceRevokedEvt,
    ArtifactVersionAcceptedEvt,
    ArtifactVersionProposedEvt,
    ProjectAwaitingHumanEvt,
    TaskAttemptCreatedEvt,
)
from studio.production.projections import ArtifactLifecycleView
from studio.production.provider_op import ProviderOperationInitiatedEvt

_P = "golden"


def _payloads(stack: object, kind: type) -> list[object]:
    db = stack.db  # type: ignore[attr-defined]
    return [e.payload for e in db.state.events if isinstance(e.payload, kind)]


@pytest.fixture(scope="module")
def stack() -> object:
    s = build_m5_stack()
    s.bus.publish(init_budget_command(_P))
    s.bus.publish(init_pipeline_command(_P))
    s.driver.run_until_quiescent()
    return s


def test_golden_delivers_with_total_charge_30(stack: object) -> None:
    # 出图操作:shot_01 一次 + shot_02 两次(gen0 + gen1)= 3 次
    ops = [p.operation_id for p in _payloads(stack, ProviderOperationInitiatedEvt)]  # type: ignore[attr-defined]
    assert len(ops) == 3
    for op in ops:
        assert stack.provider.charge_count(op) == 1  # type: ignore[attr-defined]

    # 总扣费 = 30(shot_01×1 + shot_02×2)
    settled = _payloads(stack, BudgetSettlementCompletedEvt)
    total = sum(s.captured_amount for s in settled)  # type: ignore[attr-defined]
    assert total == Decimal("30")

    # 交付恰好一次
    delivered = [
        p for p in _payloads(stack, ArtifactVersionAcceptedEvt)
        if p.output_key == "delivery"  # type: ignore[attr-defined]
    ]
    assert len(delivered) == 1


def test_golden_precise_revocation_and_rework(stack: object) -> None:
    # shot_02 图像第一版被撤销
    revoked = _payloads(stack, ArtifactAcceptanceRevokedEvt)
    assert len(revoked) == 1

    # 仅 shot_02 有第二版图像;shot_01 只有一版
    created = _payloads(stack, TaskAttemptCreatedEvt)
    image_gens: dict[str, set[int]] = {}
    for c in created:
        if c.stage_id == "image":  # type: ignore[attr-defined]
            image_gens.setdefault(c.partition_key, set()).add(c.execution_generation)  # type: ignore[attr-defined]
    assert image_gens["shot_01"] == {0}
    assert image_gens["shot_02"] == {0, 1}


def test_golden_no_await_human(stack: object) -> None:
    # 黄金场景只返工一次,未触发返工上限升级
    assert _payloads(stack, ProjectAwaitingHumanEvt) == []


def test_golden_final_lifecycle_states(stack: object) -> None:
    view = ArtifactLifecycleView.build(stack.db.state.events)  # type: ignore[attr-defined]

    # 收集各 image 版本
    proposed = [
        p for p in _payloads(stack, ArtifactVersionProposedEvt)
        if p.output_key == "image"  # type: ignore[attr-defined]
    ]
    by_partition: dict[str, list] = {}
    for p in proposed:
        by_partition.setdefault(p.partition_key, []).append(p)  # type: ignore[attr-defined]

    # shot_01:唯一版本当前有效、已接受
    s1 = by_partition["shot_01"]
    assert len(s1) == 1
    assert view.acceptance(s1[0].artifact_ref.artifact_id) is AcceptanceStatus.ACCEPTED

    # shot_02:第一版撤销、第二版当前接受
    s2 = sorted(by_partition["shot_02"], key=lambda p: p.revision)
    assert len(s2) == 2
    assert view.acceptance(s2[0].artifact_ref.artifact_id) is AcceptanceStatus.REVOKED
    assert view.acceptance(s2[1].artifact_ref.artifact_id) is AcceptanceStatus.ACCEPTED


def test_golden_three_qc_layers_present(stack: object) -> None:
    accepted = _payloads(stack, ArtifactVersionAcceptedEvt)
    qc_stages = {
        p.output_key for p in accepted  # type: ignore[attr-defined]
        if p.output_key in ("prompt_qc", "result_qc", "stage_qc")  # type: ignore[attr-defined]
    }
    assert qc_stages == {"prompt_qc", "result_qc", "stage_qc"}
    # 阶段质检两轮报告(第一轮失效、第二轮通过)
    stage_reports = [
        p for p in _payloads(stack, ArtifactVersionProposedEvt)
        if p.output_key == "stage_qc"  # type: ignore[attr-defined]
    ]
    assert len(stage_reports) == 2
    _ = ArtifactType  # 保留导入(语义占位)
