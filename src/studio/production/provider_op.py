"""ProviderOperationDecider(provider-op:{operation_id} 流):外部操作生命周期状态机。

INITIATED → (Claim) CLAIMED → (Submitted) SUBMITTED → SUCCEEDED / FAILED
CLAIMED →(对账不可判定)→ SUBMISSION_UNKNOWN(parked,可经对账恢复)
None / INITIATED →(提交前取消)→ ABORTED(墓碑;迟到的 Initiate/Claim 被拒)

owner 身份封闭:state 保存 operation_id + spec;后续命令校验 operation_id;
重复 Initiate 同 spec 才幂等,异 spec -> IdempotencyConflict。
去重按 (provider_event_id, fingerprint):同 id 同内容幂等,同 id 异内容 -> IdempotencyConflict。
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict

from studio.domain._base import NonNegativeMoney, Sha256Hex
from studio.domain.enums import ProviderOpStatus
from studio.kernel.decisions import Accepted, ProposedEvent, Rejected
from studio.kernel.envelopes import MessagePayload
from studio.kernel.errors import IdempotencyConflict
from studio.serialization import digest

from .execution_spec import ProviderExecutionSpec, canon_money


class ProviderResultRef(BaseModel):
    model_config = ConfigDict(frozen=True)
    blob_ref: str
    digest: Sha256Hex


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #


class InitiateProviderOpCmd(MessagePayload):
    type: Literal["initiate_provider_op"] = "initiate_provider_op"
    operation_id: str
    spec: ProviderExecutionSpec


class ClaimSubmissionCmd(MessagePayload):
    type: Literal["claim_submission"] = "claim_submission"
    operation_id: str


class RecordSubmittedCmd(MessagePayload):
    type: Literal["record_submitted"] = "record_submitted"
    operation_id: str
    job_id: str
    provider_event_id: str


class RecordSucceededCmd(MessagePayload):
    type: Literal["record_succeeded"] = "record_succeeded"
    operation_id: str
    result_ref: ProviderResultRef
    cost_actual: NonNegativeMoney
    provider_event_id: str


class RecordFailedCmd(MessagePayload):
    type: Literal["record_failed"] = "record_failed"
    operation_id: str
    charged: bool
    cost_actual: NonNegativeMoney
    provider_event_id: str


class RecordSubmissionUnknownCmd(MessagePayload):
    type: Literal["record_submission_unknown"] = "record_submission_unknown"
    operation_id: str
    reason: str


class AbortBeforeSubmissionCmd(MessagePayload):
    type: Literal["abort_before_submission"] = "abort_before_submission"
    operation_id: str
    reason: str


class ReconcileSubmittedCmd(MessagePayload):
    type: Literal["reconcile_submitted"] = "reconcile_submitted"
    operation_id: str
    job_id: str
    authority_ref: str


class ReconcileSucceededCmd(MessagePayload):
    type: Literal["reconcile_succeeded"] = "reconcile_succeeded"
    operation_id: str
    result_ref: ProviderResultRef
    cost_actual: NonNegativeMoney
    authority_ref: str


class ReconcileFailedCmd(MessagePayload):
    type: Literal["reconcile_failed"] = "reconcile_failed"
    operation_id: str
    charged: bool
    cost_actual: NonNegativeMoney
    authority_ref: str


ProviderOpCommand = (
    InitiateProviderOpCmd
    | ClaimSubmissionCmd
    | RecordSubmittedCmd
    | RecordSucceededCmd
    | RecordFailedCmd
    | RecordSubmissionUnknownCmd
    | AbortBeforeSubmissionCmd
    | ReconcileSubmittedCmd
    | ReconcileSucceededCmd
    | ReconcileFailedCmd
)


# --------------------------------------------------------------------------- #
# Events
# --------------------------------------------------------------------------- #


class ProviderOperationInitiatedEvt(MessagePayload):
    type: Literal["provider_op_initiated"] = "provider_op_initiated"
    operation_id: str
    spec: ProviderExecutionSpec


class SubmissionAttemptClaimedEvt(MessagePayload):
    type: Literal["submission_attempt_claimed"] = "submission_attempt_claimed"
    operation_id: str


class ProviderOperationSubmittedEvt(MessagePayload):
    type: Literal["provider_op_submitted"] = "provider_op_submitted"
    operation_id: str
    job_id: str
    provider_event_id: str


class ProviderOperationSucceededEvt(MessagePayload):
    type: Literal["provider_op_succeeded"] = "provider_op_succeeded"
    operation_id: str
    result_ref: ProviderResultRef
    cost_actual: NonNegativeMoney
    provider_event_id: str


class ProviderOperationFailedEvt(MessagePayload):
    type: Literal["provider_op_failed"] = "provider_op_failed"
    operation_id: str
    charged: bool
    cost_actual: NonNegativeMoney
    provider_event_id: str


class ProviderOperationSubmissionUnknownEvt(MessagePayload):
    type: Literal["provider_op_submission_unknown"] = "provider_op_submission_unknown"
    operation_id: str
    reason: str


class ProviderOperationAbortedEvt(MessagePayload):
    type: Literal["provider_op_aborted"] = "provider_op_aborted"
    operation_id: str
    reason: str


ProviderOpEvent = (
    ProviderOperationInitiatedEvt
    | SubmissionAttemptClaimedEvt
    | ProviderOperationSubmittedEvt
    | ProviderOperationSucceededEvt
    | ProviderOperationFailedEvt
    | ProviderOperationSubmissionUnknownEvt
    | ProviderOperationAbortedEvt
)


# --------------------------------------------------------------------------- #
# State + Decider
# --------------------------------------------------------------------------- #


class ProviderEventRecord(BaseModel):
    model_config = ConfigDict(frozen=True)
    provider_event_id: str
    fingerprint: str


class ProviderOpState(BaseModel):
    model_config = ConfigDict(frozen=True)
    status: ProviderOpStatus | None = None
    operation_id: str | None = None
    spec: ProviderExecutionSpec | None = None
    job_id: str | None = None
    events: tuple[ProviderEventRecord, ...] = ()
    result_ref: ProviderResultRef | None = None
    cost_actual: Decimal | None = None
    charged: bool | None = None

    def event_seen(self, provider_event_id: str) -> ProviderEventRecord | None:
        return next(
            (e for e in self.events if e.provider_event_id == provider_event_id), None
        )


_TERMINAL = (ProviderOpStatus.SUCCEEDED, ProviderOpStatus.FAILED, ProviderOpStatus.ABORTED)


class ProviderOperationDecider:
    def initial_state(self) -> ProviderOpState:
        return ProviderOpState()

    def decide(
        self, state: ProviderOpState, command: ProviderOpCommand
    ) -> Accepted[ProviderOpEvent] | Rejected:
        # owner 校验:一旦确定 operation_id,后续命令必须一致。
        if state.operation_id is not None and command.operation_id != state.operation_id:
            return Rejected("wrong_operation", "operation_id 与本流不一致")

        if isinstance(command, InitiateProviderOpCmd):
            return self._initiate(state, command)
        if isinstance(command, ClaimSubmissionCmd):
            return self._claim(state, command)
        if isinstance(command, AbortBeforeSubmissionCmd):
            return self._abort(state, command)
        if isinstance(command, RecordSubmittedCmd | ReconcileSubmittedCmd):
            return self._submitted(state, command)
        if isinstance(command, RecordSucceededCmd | ReconcileSucceededCmd):
            return self._succeeded(state, command)
        if isinstance(command, RecordFailedCmd | ReconcileFailedCmd):
            return self._failed(state, command)
        if isinstance(command, RecordSubmissionUnknownCmd):
            return self._unknown(state, command)
        return Rejected("unexpected_command", "provider-op 流不处理该命令")

    def _initiate(
        self, state: ProviderOpState, cmd: InitiateProviderOpCmd
    ) -> Accepted[ProviderOpEvent] | Rejected:
        if cmd.operation_id != cmd.spec.operation_id:
            return Rejected("forged_operation_id", "operation_id 与 spec 派生不一致")
        if state.status is None:
            return Accepted(
                (
                    ProposedEvent(
                        "initiated",
                        ProviderOperationInitiatedEvt(
                            operation_id=cmd.operation_id, spec=cmd.spec
                        ),
                    ),
                )
            )
        if state.status is ProviderOpStatus.ABORTED:
            return Rejected("aborted", "墓碑上不得重新 initiate")
        if state.spec == cmd.spec:
            return Accepted(())  # 幂等:同 spec
        raise IdempotencyConflict(cmd.operation_id, "同 operation_id 不同 spec")

    def _claim(
        self, state: ProviderOpState, cmd: ClaimSubmissionCmd
    ) -> Accepted[ProviderOpEvent] | Rejected:
        if state.status is ProviderOpStatus.CLAIMED:
            return Accepted(())
        if state.status is not ProviderOpStatus.INITIATED:
            return Rejected("bad_transition", f"claim 不允许自 {state.status}")
        return Accepted(
            (
                ProposedEvent(
                    "claimed", SubmissionAttemptClaimedEvt(operation_id=cmd.operation_id)
                ),
            )
        )

    def _abort(
        self, state: ProviderOpState, cmd: AbortBeforeSubmissionCmd
    ) -> Accepted[ProviderOpEvent] | Rejected:
        if state.status is ProviderOpStatus.ABORTED:
            return Accepted(())
        # 仅提交前(None / INITIATED)可取消;CLAIMED 之后外部调用可能已发生,不得 abort。
        if state.status not in (None, ProviderOpStatus.INITIATED):
            return Rejected("bad_transition", f"abort 不允许自 {state.status}")
        return Accepted(
            (
                ProposedEvent(
                    "aborted",
                    ProviderOperationAbortedEvt(
                        operation_id=cmd.operation_id, reason=cmd.reason
                    ),
                ),
            )
        )

    def _dedup(
        self, state: ProviderOpState, event_id: str, fingerprint: str
    ) -> bool:
        rec = state.event_seen(event_id)
        if rec is None:
            return False
        if rec.fingerprint != fingerprint:
            raise IdempotencyConflict(event_id, "同 provider_event_id 不同内容")
        return True

    def _submitted(
        self, state: ProviderOpState, cmd: RecordSubmittedCmd | ReconcileSubmittedCmd
    ) -> Accepted[ProviderOpEvent] | Rejected:
        event_id = (
            cmd.provider_event_id
            if isinstance(cmd, RecordSubmittedCmd)
            else f"reconcile:{cmd.authority_ref}"
        )
        fingerprint = digest({"kind": "submitted", "job_id": cmd.job_id})
        if self._dedup(state, event_id, fingerprint):
            return Accepted(())
        if state.status is ProviderOpStatus.SUBMITTED:
            if state.job_id == cmd.job_id:
                return Accepted(())
            raise IdempotencyConflict(cmd.operation_id, "SUBMITTED job_id 不一致")
        allowed = (
            (ProviderOpStatus.CLAIMED,)
            if isinstance(cmd, RecordSubmittedCmd)
            else (ProviderOpStatus.SUBMISSION_UNKNOWN,)
        )
        if state.status not in allowed:
            return Rejected("bad_transition", f"submitted 不允许自 {state.status}")
        return Accepted(
            (
                ProposedEvent(
                    "submitted",
                    ProviderOperationSubmittedEvt(
                        operation_id=cmd.operation_id, job_id=cmd.job_id,
                        provider_event_id=event_id,
                    ),
                ),
            )
        )

    def _succeeded(
        self, state: ProviderOpState, cmd: RecordSucceededCmd | ReconcileSucceededCmd
    ) -> Accepted[ProviderOpEvent] | Rejected:
        event_id = (
            cmd.provider_event_id
            if isinstance(cmd, RecordSucceededCmd)
            else f"reconcile:{cmd.authority_ref}"
        )
        fingerprint = digest(
            {
                "kind": "succeeded",
                "result": cmd.result_ref.model_dump(mode="json"),
                "cost": canon_money(cmd.cost_actual),
            }
        )
        if self._dedup(state, event_id, fingerprint):
            return Accepted(())
        if state.status is ProviderOpStatus.SUCCEEDED:
            if state.result_ref == cmd.result_ref and state.cost_actual == cmd.cost_actual:
                return Accepted(())
            raise IdempotencyConflict(cmd.operation_id, "SUCCEEDED result/cost 不一致")
        if state.status in _TERMINAL:
            raise IdempotencyConflict(cmd.operation_id, f"已处于终态 {state.status}")
        if state.status not in (ProviderOpStatus.SUBMITTED, ProviderOpStatus.SUBMISSION_UNKNOWN):
            return Rejected("bad_transition", f"succeeded 不允许自 {state.status}")
        return Accepted(
            (
                ProposedEvent(
                    "succeeded",
                    ProviderOperationSucceededEvt(
                        operation_id=cmd.operation_id, result_ref=cmd.result_ref,
                        cost_actual=cmd.cost_actual, provider_event_id=event_id,
                    ),
                ),
            )
        )

    def _failed(
        self, state: ProviderOpState, cmd: RecordFailedCmd | ReconcileFailedCmd
    ) -> Accepted[ProviderOpEvent] | Rejected:
        if not cmd.charged and cmd.cost_actual != Decimal(0):
            return Rejected("charged_cost_mismatch", "charged=False 时 cost_actual 必须为 0")
        event_id = (
            cmd.provider_event_id
            if isinstance(cmd, RecordFailedCmd)
            else f"reconcile:{cmd.authority_ref}"
        )
        fingerprint = digest(
            {"kind": "failed", "charged": cmd.charged, "cost": canon_money(cmd.cost_actual)}
        )
        if self._dedup(state, event_id, fingerprint):
            return Accepted(())
        if state.status is ProviderOpStatus.FAILED:
            if state.charged == cmd.charged and state.cost_actual == cmd.cost_actual:
                return Accepted(())
            raise IdempotencyConflict(cmd.operation_id, "FAILED charged/cost 不一致")
        if state.status in _TERMINAL:
            raise IdempotencyConflict(cmd.operation_id, f"已处于终态 {state.status}")
        if state.status not in (ProviderOpStatus.SUBMITTED, ProviderOpStatus.SUBMISSION_UNKNOWN):
            return Rejected("bad_transition", f"failed 不允许自 {state.status}")
        return Accepted(
            (
                ProposedEvent(
                    "failed",
                    ProviderOperationFailedEvt(
                        operation_id=cmd.operation_id, charged=cmd.charged,
                        cost_actual=cmd.cost_actual, provider_event_id=event_id,
                    ),
                ),
            )
        )

    def _unknown(
        self, state: ProviderOpState, cmd: RecordSubmissionUnknownCmd
    ) -> Accepted[ProviderOpEvent] | Rejected:
        if state.status is ProviderOpStatus.SUBMISSION_UNKNOWN:
            return Accepted(())
        if state.status is not ProviderOpStatus.CLAIMED:
            return Rejected("bad_transition", f"unknown 不允许自 {state.status}")
        return Accepted(
            (
                ProposedEvent(
                    "unknown",
                    ProviderOperationSubmissionUnknownEvt(
                        operation_id=cmd.operation_id, reason=cmd.reason
                    ),
                ),
            )
        )

    def evolve(self, state: ProviderOpState, event: ProviderOpEvent) -> ProviderOpState:
        if isinstance(event, ProviderOperationInitiatedEvt):
            return state.model_copy(
                update={
                    "status": ProviderOpStatus.INITIATED,
                    "operation_id": event.operation_id,
                    "spec": event.spec,
                }
            )
        if isinstance(event, SubmissionAttemptClaimedEvt):
            return state.model_copy(update={"status": ProviderOpStatus.CLAIMED})
        if isinstance(event, ProviderOperationSubmittedEvt):
            return state.model_copy(
                update={
                    "status": ProviderOpStatus.SUBMITTED,
                    "job_id": event.job_id,
                    "events": (
                        *state.events,
                        ProviderEventRecord(
                            provider_event_id=event.provider_event_id,
                            fingerprint=digest(
                                {"kind": "submitted", "job_id": event.job_id}
                            ),
                        ),
                    ),
                }
            )
        if isinstance(event, ProviderOperationSucceededEvt):
            return state.model_copy(
                update={
                    "status": ProviderOpStatus.SUCCEEDED,
                    "result_ref": event.result_ref,
                    "cost_actual": event.cost_actual,
                    "events": (
                        *state.events,
                        ProviderEventRecord(
                            provider_event_id=event.provider_event_id,
                            fingerprint=digest(
                                {
                                    "kind": "succeeded",
                                    "result": event.result_ref.model_dump(mode="json"),
                                    "cost": canon_money(event.cost_actual),
                                }
                            ),
                        ),
                    ),
                }
            )
        if isinstance(event, ProviderOperationFailedEvt):
            return state.model_copy(
                update={
                    "status": ProviderOpStatus.FAILED,
                    "charged": event.charged,
                    "cost_actual": event.cost_actual,
                    "events": (
                        *state.events,
                        ProviderEventRecord(
                            provider_event_id=event.provider_event_id,
                            fingerprint=digest(
                                {
                                    "kind": "failed",
                                    "charged": event.charged,
                                    "cost": canon_money(event.cost_actual),
                                }
                            ),
                        ),
                    ),
                }
            )
        if isinstance(event, ProviderOperationSubmissionUnknownEvt):
            return state.model_copy(update={"status": ProviderOpStatus.SUBMISSION_UNKNOWN})
        if isinstance(event, ProviderOperationAbortedEvt):
            return state.model_copy(
                update={
                    "status": ProviderOpStatus.ABORTED,
                    "operation_id": event.operation_id,
                }
            )
        return state
