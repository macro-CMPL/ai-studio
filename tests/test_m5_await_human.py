"""Step 7(其三配套):ProjectDecider 的返工上限升级事实(等待人工决策)。"""

from __future__ import annotations

from typing import Any

from studio.kernel.decisions import Accepted, Rejected
from studio.production.payloads import (
    EscalateAwaitHumanCmd,
    InitializePipelineCmd,
    ProjectAwaitingHumanEvt,
)
from studio.production.project import ProjectDecider


def _apply(decider: Any, state: Any, cmd: Any) -> tuple[Any, Any]:
    decision = decider.decide(state, cmd)
    if isinstance(decision, Accepted):
        for pe in decision.events:
            state = decider.evolve(state, pe.payload)
    return state, decision


def _initialized() -> tuple[ProjectDecider, Any]:
    d = ProjectDecider()
    s, _ = _apply(
        d, d.initial_state(),
        InitializePipelineCmd(project_id="p", pipeline_spec_id="s"),
    )
    return d, s


def _escalate(partition: str = "shot_02", gen: int = 2) -> EscalateAwaitHumanCmd:
    return EscalateAwaitHumanCmd(
        project_id="p", stage_id="image", partition_key=partition,
        report_ref="report:stage-1", generation=gen, reason="rework_limit_exceeded",
    )


def test_escalate_emits_awaiting_human_fact() -> None:
    d, s = _initialized()
    s, dec = _apply(d, s, _escalate())
    assert isinstance(dec, Accepted)
    evt = dec.events[0].payload
    assert isinstance(evt, ProjectAwaitingHumanEvt)
    assert evt.stage_id == "image"
    assert evt.partition_key == "shot_02"
    assert evt.generation == 2
    assert "shot_02" in s.awaiting_human[0]


def test_escalate_idempotent_per_partition() -> None:
    d, s = _initialized()
    s, _ = _apply(d, s, _escalate())
    s, dec = _apply(d, s, _escalate())
    assert isinstance(dec, Accepted) and dec.events == ()  # 幂等,不重复发事实


def test_escalate_before_init_rejected() -> None:
    d = ProjectDecider()
    dec = d.decide(d.initial_state(), _escalate())
    assert isinstance(dec, Rejected) and dec.code == "not_initialized"


def test_escalate_distinct_partitions_independent() -> None:
    d, s = _initialized()
    s, _ = _apply(d, s, _escalate("shot_02"))
    s, dec = _apply(d, s, _escalate("shot_01"))
    assert isinstance(dec, Accepted) and len(dec.events) == 1
    assert len(s.awaiting_human) == 2
