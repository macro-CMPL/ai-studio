"""质量闸门:确定性策略 + GateDecided 事实(闸门流,按报告隔离)。

分工(锁定):
- 质量评价器只"观察并出报告"(QCReportPayload,不可变产物),不直接改流程。
- GatePolicy 是纯函数:把一份报告确定性映射为 GateOutcome(verdict + 返工范围 + 反馈)。
- GateDecider 把决策记为显式事实 GateDecidedEvt(闸门流 gate:{report_id})。
  同一报告重复决策 -> 幂等;同一报告出现不同决策 -> IdempotencyConflict。

verdict 语义:
- PASS   :报告通过,下游接受目标产物。
- REWORK :报告不通过且可返工,返工范围给出需重做的分区。
- BLOCK  :命中阻断规则(如禁止内容),不自动重试,等待人工。
- AWAIT_HUMAN 不由本策略产出,而由返工上限升级(见返工管理器)。
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

from studio.domain.artifacts import ArtifactRef, QCReportPayload
from studio.domain.enums import GateVerdict
from studio.kernel.decisions import Accepted, ProposedEvent, Rejected
from studio.kernel.envelopes import MessagePayload
from studio.kernel.errors import IdempotencyConflict
from studio.serialization import digest


class GateOutcome(BaseModel):
    """纯策略输出:控制决策 + 返工范围(分区) + 反馈。"""

    model_config = ConfigDict(frozen=True)
    verdict: GateVerdict
    rework_scope: tuple[str, ...]
    feedback: str


class GatePolicy:
    """确定性闸门策略(纯函数,版本化)。

    - 通过 -> PASS(无返工范围)。
    - 不通过且命中阻断规则(rule_id ∈ blocking_rule_ids)-> BLOCK。
    - 不通过且可返工 -> REWORK,范围取报告建议;为空则由问题项的目标分区推导。
    """

    def __init__(
        self,
        *,
        policy_id: str,
        policy_version: str,
        blocking_rule_ids: frozenset[str] = frozenset(),
    ) -> None:
        self.policy_id = policy_id
        self.policy_version = policy_version
        self._blocking = blocking_rule_ids

    def decide(self, report: QCReportPayload) -> GateOutcome:
        if report.passed:
            return GateOutcome(
                verdict=GateVerdict.PASS, rework_scope=(), feedback=report.feedback
            )
        if any(f.rule_id in self._blocking for f in report.findings):
            return GateOutcome(
                verdict=GateVerdict.BLOCK, rework_scope=(), feedback=report.feedback
            )
        scope = report.rework_scope or tuple(
            sorted({f.target_partition for f in report.findings if f.target_partition})
        )
        return GateOutcome(
            verdict=GateVerdict.REWORK, rework_scope=scope, feedback=report.feedback
        )


# --------------------------------------------------------------------------- #
# 命令 / 事件
# --------------------------------------------------------------------------- #


class DecideGateCmd(MessagePayload):
    type: Literal["decide_gate"] = "decide_gate"
    report_ref: ArtifactRef
    target_ref: ArtifactRef
    target_partition: str | None
    verdict: GateVerdict
    rework_scope: tuple[str, ...]
    feedback: str
    policy_id: str
    policy_version: str


class GateDecidedEvt(MessagePayload):
    type: Literal["gate_decided"] = "gate_decided"
    report_ref: ArtifactRef
    target_ref: ArtifactRef
    target_partition: str | None
    verdict: GateVerdict
    rework_scope: tuple[str, ...]
    feedback: str
    policy_id: str
    policy_version: str


GateCommand = DecideGateCmd
GateEvent = GateDecidedEvt


# --------------------------------------------------------------------------- #
# Decider(闸门流:gate:{report_id},按报告隔离)
# --------------------------------------------------------------------------- #


def _decision_fingerprint(cmd: DecideGateCmd) -> str:
    return digest(
        {
            "report": cmd.report_ref.artifact_id,
            "target": cmd.target_ref.artifact_id,
            "target_partition": cmd.target_partition,
            "verdict": cmd.verdict.value,
            "rework_scope": list(cmd.rework_scope),
            "feedback": cmd.feedback,
            "policy_id": cmd.policy_id,
            "policy_version": cmd.policy_version,
        }
    )


class GateState(BaseModel):
    model_config = ConfigDict(frozen=True)
    decided: bool = False
    fingerprint: str | None = None


class GateDecider:
    def initial_state(self) -> GateState:
        return GateState()

    def decide(
        self, state: GateState, command: GateCommand
    ) -> Accepted[GateEvent] | Rejected:
        if not isinstance(command, DecideGateCmd):
            return Rejected("unexpected_command", "闸门流不处理该命令")
        fingerprint = _decision_fingerprint(command)
        if state.decided:
            if state.fingerprint == fingerprint:
                return Accepted(())  # 同报告同决策:幂等
            raise IdempotencyConflict(
                command.report_ref.artifact_id, "同一报告出现不同闸门决策"
            )
        return Accepted(
            (
                ProposedEvent(
                    "gate-decided",
                    GateDecidedEvt(
                        report_ref=command.report_ref,
                        target_ref=command.target_ref,
                        target_partition=command.target_partition,
                        verdict=command.verdict,
                        rework_scope=command.rework_scope,
                        feedback=command.feedback,
                        policy_id=command.policy_id,
                        policy_version=command.policy_version,
                    ),
                ),
            )
        )

    def evolve(self, state: GateState, event: GateEvent) -> GateState:
        if isinstance(event, GateDecidedEvt):
            return state.model_copy(
                update={
                    "decided": True,
                    "fingerprint": _decision_fingerprint(
                        DecideGateCmd(
                            report_ref=event.report_ref,
                            target_ref=event.target_ref,
                            target_partition=event.target_partition,
                            verdict=event.verdict,
                            rework_scope=event.rework_scope,
                            feedback=event.feedback,
                            policy_id=event.policy_id,
                            policy_version=event.policy_version,
                        )
                    ),
                }
            )
        return state
