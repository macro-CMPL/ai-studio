"""对账时钟 Decider + OrphanReconciler(纯)。

定时器作为数据进入:ReconciliationTick(scope, as_of, policy_version)。Reconciler 仅消费
Tick + 自身事件状态做纯决策,不读 Clock;按 op 记录 first_seen_as_of。

不同状态不同动作(禁止仅凭超时直接 RELEASE):
- Reserved 但 provider-op 未推进(None)-> 幂等重发 InitiateProviderOp(重发丢失的 Initiate)
- None / INITIATED 且超过回收阈值 -> AbortBeforeSubmission(留取消墓碑)
- 观察到 ProviderOperationAborted 后才 -> ReleaseBudget(第二轮,保证安全)
- CLAIMED / SUBMITTED / SUBMISSION_UNKNOWN -> 不自动 Abort,仅 hold
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal

from pydantic import BaseModel, ConfigDict

from studio.domain._base import UtcDatetime
from studio.domain.enums import ProviderOpStatus
from studio.kernel.decisions import Accepted, ProposedEvent, Rejected
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.kernel.process_manager import ProposedCommand, Reaction

from . import identity
from .attempt_payloads import ProviderExecutionSpecRecordedEvt
from .budget import BudgetReservedEvt, BudgetSettlementCompletedEvt, ReleaseBudgetCmd
from .execution_spec import ProviderExecutionSpec
from .payloads import TaskAttemptCreatedEvt
from .provider_op import (
    AbortBeforeSubmissionCmd,
    InitiateProviderOpCmd,
    ProviderOperationAbortedEvt,
    ProviderOperationFailedEvt,
    ProviderOperationInitiatedEvt,
    ProviderOperationSubmissionUnknownEvt,
    ProviderOperationSubmittedEvt,
    ProviderOperationSucceededEvt,
    SubmissionAttemptClaimedEvt,
)

# --------------------------------------------------------------------------- #
# 对账时钟(把定时器变成可重放的事件数据)
# --------------------------------------------------------------------------- #


class EmitReconciliationTickCmd(MessagePayload):
    type: Literal["emit_reconciliation_tick"] = "emit_reconciliation_tick"
    scope: str
    as_of: UtcDatetime
    policy_version: str


class ReconciliationTickEvt(MessagePayload):
    type: Literal["reconciliation_tick"] = "reconciliation_tick"
    scope: str
    as_of: UtcDatetime
    policy_version: str
    sequence: int


class ReconciliationClockState(BaseModel):
    model_config = ConfigDict(frozen=True)
    last_as_of: datetime | None = None
    sequence: int = 0


class ReconciliationClockDecider:
    """把外部定时器发的 Tick 命令转为单调递增的 Tick 事件(as_of 严格递增)。"""

    def initial_state(self) -> ReconciliationClockState:
        return ReconciliationClockState()

    def decide(
        self, state: ReconciliationClockState, command: EmitReconciliationTickCmd
    ) -> Accepted[ReconciliationTickEvt] | Rejected:
        if state.last_as_of is not None and command.as_of <= state.last_as_of:
            return Rejected("stale_tick", "as_of 必须严格递增")
        return Accepted(
            (
                ProposedEvent(
                    "tick",
                    ReconciliationTickEvt(
                        scope=command.scope, as_of=command.as_of,
                        policy_version=command.policy_version,
                        sequence=state.sequence + 1,
                    ),
                ),
            )
        )

    def evolve(
        self, state: ReconciliationClockState, event: ReconciliationTickEvt
    ) -> ReconciliationClockState:
        return state.model_copy(
            update={"last_as_of": event.as_of, "sequence": event.sequence}
        )


# --------------------------------------------------------------------------- #
# OrphanReconciler
# --------------------------------------------------------------------------- #


class ReconcilePolicy(BaseModel):
    model_config = ConfigDict(frozen=True)
    version: str
    recycle_after: timedelta


_HOLD = frozenset(
    {
        ProviderOpStatus.CLAIMED,
        ProviderOpStatus.SUBMITTED,
        ProviderOpStatus.SUBMISSION_UNKNOWN,
    }
)
_TERMINAL = frozenset(
    {ProviderOpStatus.SUCCEEDED, ProviderOpStatus.FAILED, ProviderOpStatus.ABORTED}
)


class OpTracking(BaseModel):
    model_config = ConfigDict(frozen=True)
    operation_id: str
    attempt_id: str
    project_id: str | None
    spec: ProviderExecutionSpec
    reserved: bool = False
    op_status: ProviderOpStatus | None = None
    abort_requested: bool = False
    aborted: bool = False
    released: bool = False
    first_seen_as_of: datetime | None = None


class ReconcilerState(BaseModel):
    model_config = ConfigDict(frozen=True)
    project_by_attempt: tuple[tuple[str, str], ...] = ()
    ops: tuple[OpTracking, ...] = ()

    def project_of(self, attempt_id: str) -> str | None:
        return next((p for (a, p) in self.project_by_attempt if a == attempt_id), None)

    def op_of(self, operation_id: str) -> OpTracking | None:
        return next((o for o in self.ops if o.operation_id == operation_id), None)

    def with_op(self, updated: OpTracking) -> ReconcilerState:
        others = tuple(o for o in self.ops if o.operation_id != updated.operation_id)
        return self.model_copy(update={"ops": (*others, updated)})


ReconcileCommand = InitiateProviderOpCmd | AbortBeforeSubmissionCmd | ReleaseBudgetCmd


def _reservation_matches(
    spec: ProviderExecutionSpec, amount: object, currency: str, quote_digest: str
) -> bool:
    return (
        amount == spec.estimated_cost
        and currency == spec.currency
        and quote_digest == spec.quote_digest()
    )


class OrphanReconciliationProcessManager:
    pm_id = "orphan-reconciler-pm"

    def __init__(self, policy: ReconcilePolicy, scope: str = "global") -> None:
        self._policy = policy
        self._scope = scope

    def initial_state(self) -> ReconcilerState:
        return ReconcilerState()

    def react(
        self, state: ReconcilerState, event: EventEnvelope[MessagePayload]
    ) -> Reaction[ReconcilerState, ReconcileCommand]:
        payload = event.payload
        if isinstance(payload, TaskAttemptCreatedEvt):
            return Reaction(
                state=state.model_copy(
                    update={
                        "project_by_attempt": (
                            *state.project_by_attempt,
                            (payload.attempt_id, payload.project_id),
                        )
                    }
                ),
                commands=(),
            )
        if isinstance(payload, ProviderExecutionSpecRecordedEvt):
            return self._track(state, payload)
        if isinstance(payload, BudgetReservedEvt):
            return self._on_reserved(state, payload)
        if isinstance(payload, ProviderOperationInitiatedEvt):
            return Reaction(
                state=self._patch(
                    state, payload.operation_id, status=ProviderOpStatus.INITIATED
                ),
                commands=(),
            )
        if isinstance(payload, SubmissionAttemptClaimedEvt):
            return Reaction(
                state=self._patch(
                    state, payload.operation_id, status=ProviderOpStatus.CLAIMED
                ),
                commands=(),
            )
        if isinstance(payload, ProviderOperationSubmittedEvt):
            return Reaction(
                state=self._patch(
                    state, payload.operation_id, status=ProviderOpStatus.SUBMITTED
                ),
                commands=(),
            )
        if isinstance(payload, ProviderOperationSubmissionUnknownEvt):
            return Reaction(
                state=self._patch(
                    state, payload.operation_id, status=ProviderOpStatus.SUBMISSION_UNKNOWN
                ),
                commands=(),
            )
        if isinstance(payload, ProviderOperationSucceededEvt):
            return Reaction(
                state=self._patch(
                    state, payload.operation_id, status=ProviderOpStatus.SUCCEEDED
                ),
                commands=(),
            )
        if isinstance(payload, ProviderOperationFailedEvt):
            return Reaction(
                state=self._patch(
                    state, payload.operation_id, status=ProviderOpStatus.FAILED
                ),
                commands=(),
            )
        if isinstance(payload, ProviderOperationAbortedEvt):
            return self._on_aborted(state, payload)
        if isinstance(payload, BudgetSettlementCompletedEvt):
            if payload.outcome == "released":
                return Reaction(
                    state=self._patch(state, payload.operation_id, released=True),
                    commands=(),
                )
            return Reaction(state=state, commands=())
        if isinstance(payload, ReconciliationTickEvt):
            return self._on_tick(state, payload)
        return Reaction(state=state, commands=())

    def _on_reserved(
        self, state: ReconcilerState, payload: BudgetReservedEvt
    ) -> Reaction[ReconcilerState, ReconcileCommand]:
        op = state.op_of(payload.operation_id)
        if op is None:
            return Reaction(state=state, commands=())
        # 只有预留内容与 spec 一致才确认;否则不 reserved(_on_aborted 不会释放)。
        if not _reservation_matches(
            op.spec, payload.amount, payload.currency, payload.quote_digest
        ):
            return Reaction(state=state, commands=())
        return Reaction(
            state=state.with_op(op.model_copy(update={"reserved": True})), commands=()
        )

    def _track(
        self, state: ReconcilerState, payload: ProviderExecutionSpecRecordedEvt
    ) -> Reaction[ReconcilerState, ReconcileCommand]:
        spec = payload.spec
        if state.op_of(spec.operation_id) is not None:
            return Reaction(state=state, commands=())
        op = OpTracking(
            operation_id=spec.operation_id, attempt_id=spec.attempt_id,
            project_id=state.project_of(spec.attempt_id), spec=spec,
        )
        return Reaction(state=state.model_copy(update={"ops": (*state.ops, op)}), commands=())

    def _patch(
        self,
        state: ReconcilerState,
        operation_id: str,
        *,
        status: ProviderOpStatus | None = None,
        released: bool | None = None,
    ) -> ReconcilerState:
        op = state.op_of(operation_id)
        if op is None:
            return state
        updates: dict[str, object] = {}
        if status is not None:
            updates["op_status"] = status
        if released is not None:
            updates["released"] = released
        return state.with_op(op.model_copy(update=updates))

    def _on_aborted(
        self, state: ReconcilerState, payload: ProviderOperationAbortedEvt
    ) -> Reaction[ReconcilerState, ReconcileCommand]:
        op = state.op_of(payload.operation_id)
        if op is None or op.aborted:
            return Reaction(state=state, commands=())
        new_state = state.with_op(
            op.model_copy(update={"aborted": True, "op_status": ProviderOpStatus.ABORTED})
        )
        # 仅在预留已确认、未释放、project 已知时释放:避免外部墓碑触发对不存在预留的释放。
        if not op.reserved or op.released or op.project_id is None:
            return Reaction(state=new_state, commands=())
        # 第二轮:观察到取消墓碑后才释放预算。
        return Reaction(
            state=new_state,
            commands=(
                ProposedCommand(
                    reaction_name="release",
                    command_key=f"release:{op.operation_id}",
                    target=identity.budget_stream(op.project_id),
                    payload=ReleaseBudgetCmd(
                        project_id=op.project_id, operation_id=op.operation_id,
                        quote_digest=op.spec.quote_digest(),
                    ),
                ),
            ),
        )

    def _on_tick(
        self, state: ReconcilerState, tick: ReconciliationTickEvt
    ) -> Reaction[ReconcilerState, ReconcileCommand]:
        # scope 隔离:只处理本 Reconciler 负责的 scope;policy_version 亦须匹配。
        if tick.scope != self._scope or tick.policy_version != self._policy.version:
            return Reaction(state=state, commands=())
        new_ops: list[OpTracking] = []
        commands: list[ProposedCommand[ReconcileCommand]] = []
        for op in state.ops:
            updated, command = self._evaluate(op, tick.as_of)
            new_ops.append(updated)
            if command is not None:
                commands.append(command)
        return Reaction(
            state=state.model_copy(update={"ops": tuple(new_ops)}),
            commands=tuple(commands),
        )

    def _evaluate(
        self, op: OpTracking, as_of: datetime
    ) -> tuple[OpTracking, ProposedCommand[ReconcileCommand] | None]:
        inactive = (
            op.aborted
            or op.released
            or op.abort_requested
            or (op.op_status in _HOLD)
            or (op.op_status in _TERMINAL)
        )
        if inactive or not op.reserved:
            return op, None
        # reserved 且 op_status ∈ {None, INITIATED}:需要推进或回收。
        first_seen = op.first_seen_as_of or as_of
        op = op.model_copy(update={"first_seen_as_of": first_seen})
        if as_of - first_seen >= self._policy.recycle_after:
            return (
                op.model_copy(update={"abort_requested": True}),
                ProposedCommand(
                    reaction_name="abort",
                    command_key=f"abort:{op.operation_id}",
                    target=identity.provider_op_stream(op.operation_id),
                    payload=AbortBeforeSubmissionCmd(
                        operation_id=op.operation_id, reason="reconcile_recycle"
                    ),
                ),
            )
        if op.op_status is None:
            # 幂等重发丢失的 Initiate(而非直接释放)。
            return (
                op,
                ProposedCommand(
                    reaction_name="reinitiate",
                    command_key=f"reinitiate:{op.operation_id}",
                    target=identity.provider_op_stream(op.operation_id),
                    payload=InitiateProviderOpCmd(
                        operation_id=op.operation_id, spec=op.spec
                    ),
                ),
            )
        return op, None  # INITIATED 但未到回收阈值:等待 ActivityWorker
