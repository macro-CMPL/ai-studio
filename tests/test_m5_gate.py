"""M5 第 3 步:质量闸门确定性策略 + GateDecided 事实(幂等 / 异决策冲突)。"""

from __future__ import annotations

import pytest

from studio.domain import ids as domain_ids
from studio.domain.artifacts import ArtifactRef, QCFinding, QCReportPayload
from studio.domain.enums import GateVerdict, Severity
from studio.kernel.decisions import Accepted
from studio.kernel.errors import IdempotencyConflict
from studio.production.gate import (
    DecideGateCmd,
    GateDecidedEvt,
    GateDecider,
    GatePolicy,
)
from studio.serialization import digest

_PROJECT = "p"


def _ref(output_key: str, partition: str | None, revision: int, seed: str) -> ArtifactRef:
    series = domain_ids.series_id(_PROJECT, output_key, partition)
    return ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, revision),
        series_id=series,
        revision=revision,
        digest=digest(seed),
    )


def _report(
    *,
    passed: bool,
    findings: tuple[QCFinding, ...] = (),
    rework_scope: tuple[str, ...] = (),
    partition: str | None = "shot_02",
) -> QCReportPayload:
    subject = _ref("image", partition, 1, f"img-{partition}")
    return QCReportPayload(
        subject_refs=(subject,),
        target_partition=partition,
        evaluator="stage-consistency",
        evaluator_version="1",
        criteria_version="1",
        passed=passed,
        findings=findings,
        rework_scope=rework_scope,
        feedback="见问题项",
    )


def _finding(rule_id: str, partition: str | None = "shot_02") -> QCFinding:
    return QCFinding(
        rule_id=rule_id,
        severity=Severity.ERROR,
        description="跨镜头人物不一致",
        suggested_action="重做该镜头",
        target_partition=partition,
    )


# --------------------------------------------------------------------------- #
# 策略(纯函数)
# --------------------------------------------------------------------------- #


def _policy(blocking: frozenset[str] = frozenset()) -> GatePolicy:
    return GatePolicy(policy_id="default", policy_version="1", blocking_rule_ids=blocking)


def test_policy_pass_when_report_passed() -> None:
    out = _policy().decide(_report(passed=True))
    assert out.verdict is GateVerdict.PASS
    assert out.rework_scope == ()


def test_policy_rework_uses_report_scope() -> None:
    out = _policy().decide(
        _report(passed=False, findings=(_finding("consistency"),), rework_scope=("shot_02",))
    )
    assert out.verdict is GateVerdict.REWORK
    assert out.rework_scope == ("shot_02",)


def test_policy_rework_scope_falls_back_to_finding_partitions() -> None:
    # 报告未显式给返工范围 -> 由问题项目标分区推导(去重排序)。
    out = _policy().decide(
        _report(
            passed=False,
            findings=(_finding("c", "shot_02"), _finding("c", "shot_02")),
            rework_scope=(),
        )
    )
    assert out.verdict is GateVerdict.REWORK
    assert out.rework_scope == ("shot_02",)


def test_policy_block_on_blocking_rule() -> None:
    out = _policy(blocking=frozenset({"forbidden_content"})).decide(
        _report(passed=False, findings=(_finding("forbidden_content"),))
    )
    assert out.verdict is GateVerdict.BLOCK
    assert out.rework_scope == ()


def test_policy_is_deterministic() -> None:
    report = _report(passed=False, findings=(_finding("c"),), rework_scope=("shot_02",))
    assert _policy().decide(report) == _policy().decide(report)


# --------------------------------------------------------------------------- #
# GateDecider(闸门流:幂等 / 异决策冲突)
# --------------------------------------------------------------------------- #


def _decide_cmd(verdict: GateVerdict, scope: tuple[str, ...]) -> DecideGateCmd:
    report = _ref("stage_qc", None, 1, "report-1")
    target = _ref("image", "shot_02", 1, "img-shot_02")
    return DecideGateCmd(
        report_ref=report,
        target_ref=target,
        target_partition="shot_02",
        verdict=verdict,
        rework_scope=scope,
        feedback="见问题项",
        policy_id="default",
        policy_version="1",
    )


def test_gate_decider_records_decision() -> None:
    dec = GateDecider()
    decision = dec.decide(dec.initial_state(), _decide_cmd(GateVerdict.REWORK, ("shot_02",)))
    assert isinstance(decision, Accepted)
    assert len(decision.events) == 1
    assert isinstance(decision.events[0].payload, GateDecidedEvt)
    assert decision.events[0].payload.verdict is GateVerdict.REWORK


def test_gate_decider_same_decision_idempotent() -> None:
    dec = GateDecider()
    cmd = _decide_cmd(GateVerdict.REWORK, ("shot_02",))
    state = dec.initial_state()
    d1 = dec.decide(state, cmd)
    assert isinstance(d1, Accepted)
    state = dec.evolve(state, d1.events[0].payload)
    # 同报告同决策重投 -> 幂等空事件
    d2 = dec.decide(state, cmd)
    assert isinstance(d2, Accepted)
    assert d2.events == ()


def test_gate_decider_different_verdict_same_report_conflicts() -> None:
    dec = GateDecider()
    state = dec.initial_state()
    d1 = dec.decide(state, _decide_cmd(GateVerdict.REWORK, ("shot_02",)))
    assert isinstance(d1, Accepted)
    state = dec.evolve(state, d1.events[0].payload)
    # 同报告不同决策 -> 幂等冲突(报警)
    with pytest.raises(IdempotencyConflict):
        dec.decide(state, _decide_cmd(GateVerdict.PASS, ()))
