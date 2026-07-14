"""M4 步骤3:payload-bearing Plan 重算测试。

证明:上游 Plan 新版本走正常 candidate → accepted 路径时,下游 image attempt v2
的 ExecutionPlanningPM 能正确生成 ExecutionSpec。

流程:
1. Pipeline 初始化 → plan v1 accepted → image v1 provider 完成(via activity stub)。
2. Supersede storyboard → plan v2 produced+accepted (by normal attempt path)。
3. Lineage 标脏 image v1 → Recompute 创建 image attempt v2(绑定 plan v2)。
4. ExecutionPlanningPM 使用 plan v2 payload 生成新 ExecutionSpec。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from m4_helpers import (
    build_m4_stack,
    claim_command,
    init_budget_command,
    init_pipeline_command,
    initiated_ops,
    submit_command,
    succeed_command,
)
from studio.domain import ids as domain_ids
from studio.domain.artifacts import (
    ShotSpec,
    StoryboardPayload,
)
from studio.domain.enums import AcceptanceMode
from studio.kernel.envelopes import CommandEnvelope
from studio.production import identity
from studio.production.attempt_payloads import ProviderExecutionSpecRecordedEvt
from studio.production.payloads import (
    ArtifactVersionAcceptedEvt,
    ProposeArtifactVersionCmd,
)
from studio.serialization import digest

_P = "p"
_TS = datetime(2026, 1, 1, tzinfo=UTC)


def _cmd(target: str, key: str, payload: object, cid: str) -> CommandEnvelope[object]:
    return CommandEnvelope(
        command_id=cid, schema_version=1, target=target, command_key=key,
        correlation_id="act", causation_id=None, issued_at=_TS, payload=payload,  # type: ignore[arg-type]
    )


def _payloads(stack: Any, cls: type) -> list[Any]:
    return [e.payload for e in stack.db.state.events if isinstance(e.payload, cls)]


def test_plan_rework_triggers_new_execution_spec() -> None:
    stack = build_m4_stack()
    stack.bus.publish(init_budget_command(_P))
    stack.bus.publish(init_pipeline_command(_P))
    stack.driver.run_until_quiescent()

    # --- Phase 1: 完成 image v1 (via activity stub) --- #
    ops_v1 = initiated_ops(stack)
    assert len(ops_v1) == 2  # shot_01 + shot_02
    for op, spec in ops_v1:
        stack.bus.publish(claim_command(op))
        stack.bus.publish(submit_command(op))
        stack.bus.publish(succeed_command(op, spec))
    stack.driver.run_until_quiescent()

    # 验证 image v1 全部接受
    img_accepted_v1 = [
        e.payload for e in stack.db.state.events
        if isinstance(e.payload, ArtifactVersionAcceptedEvt)
        and e.payload.output_key == "image"
    ]
    assert len(img_accepted_v1) == 2
    specs_before = _payloads(stack, ProviderExecutionSpecRecordedEvt)
    assert len(specs_before) == 2

    # --- Phase 2: supersede storyboard -> plan v2 -> image v2 spec --- #
    sb_series = domain_ids.series_id(_P, "storyboard", None)
    sb_v2_payload = StoryboardPayload(
        shots=(
            ShotSpec(shot_id="shot_01", description="revised"),
            ShotSpec(shot_id="shot_02", description="same"),
        )
    )
    propose_sb = ProposeArtifactVersionCmd(
        project_id=_P, series_id=sb_series, candidate_id="cand-sb-v2",
        output_key="storyboard", partition_key=None,
        digest=digest(sb_v2_payload), payload=sb_v2_payload,
        acceptance_mode=AcceptanceMode.AUTO, produced_by_attempt="manual",
    )
    stack.bus.publish(_cmd(
        identity.series_stream(sb_series), "cand-sb-v2", propose_sb, "cmd-sb-v2"
    ))
    stack.driver.run_until_quiescent()

    # Storyboard v2 接受 → plan v1 标脏 → recompute plan v2 attempts
    # → plan v2 同步产出候选 → publish → accepted
    # → image v1 标脏 → recompute image v2 attempts(绑定 plan v2)
    # → ExecutionPlanningPM 使用 plan v2 payload 生成 ExecutionSpec

    specs_after = _payloads(stack, ProviderExecutionSpecRecordedEvt)
    # 恰好 2 original + 2 rework = 4(收紧为 ==,防止重复重算)
    assert len(specs_after) == 4, f"Expected == 4, got {len(specs_after)}"

    # rework spec 的 attempt_id 必须与 v1 不同(证明 plan v2 进入了规划)
    v1_attempt_ids = {s.attempt_id for s in specs_before}
    rework_specs = [s for s in specs_after if s.attempt_id not in v1_attempt_ids]
    assert len(rework_specs) == 2, (
        f"应有恰好 2 个 rework ExecutionSpec (新 attempt),got {len(rework_specs)}"
    )
    # 验证 rework spec 的 plan_ref 指向 plan v2(revision > 1)
    for rs in rework_specs:
        assert rs.spec.plan_ref.revision > 1, "rework spec 应绑定 plan v2"

    # 验证 image v2 attempt 被创建(通过 attempt-created 事件而非命令)
    from studio.production.payloads import TaskAttemptCreatedEvt
    img_created_evts = [
        e.payload for e in stack.db.state.events
        if isinstance(e.payload, TaskAttemptCreatedEvt) and e.payload.stage_id == "image"
    ]
    # 恰好 4:2 v1 + 2 v2,无重复重算
    assert len(img_created_evts) == 4, f"Expected == 4 image created, got {len(img_created_evts)}"
