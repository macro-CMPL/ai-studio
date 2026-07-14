"""M4:attempt 流的 PROVIDER 生命周期命令/事件(在 M3 基础上扩展)。"""

from __future__ import annotations

from typing import Literal

from studio.domain.artifacts import ArtifactPayload
from studio.kernel.envelopes import MessagePayload

from .execution_spec import ProviderExecutionSpec
from .payloads import (
    ArtifactCandidateProducedEvt,
    CreateTaskAttemptCmd,
    TaskAttemptCreatedEvt,
    TaskInputsBoundEvt,
)

# --------------------------------------------------------------------------- #
# Commands(均 target attempt 流)
# --------------------------------------------------------------------------- #


class RecordExecutionSpecCmd(MessagePayload):
    type: Literal["record_execution_spec"] = "record_execution_spec"
    attempt_id: str
    spec: ProviderExecutionSpec


class MarkWaitingProviderCmd(MessagePayload):
    type: Literal["mark_waiting_provider"] = "mark_waiting_provider"
    attempt_id: str
    min_provider_phase: int | None = None


class MarkWaitingReconciliationCmd(MessagePayload):
    type: Literal["mark_waiting_reconciliation"] = "mark_waiting_reconciliation"
    attempt_id: str
    min_provider_phase: int | None = None


class MarkBlockedCmd(MessagePayload):
    type: Literal["mark_blocked"] = "mark_blocked"
    attempt_id: str
    reason: str
    min_provider_phase: int | None = None


class MarkFailedCmd(MessagePayload):
    type: Literal["mark_failed"] = "mark_failed"
    attempt_id: str
    reason: str
    min_provider_phase: int | None = None


class RecordProviderResultCmd(MessagePayload):
    type: Literal["record_provider_result"] = "record_provider_result"
    attempt_id: str
    operation_id: str
    blob_ref: str
    payload: ArtifactPayload


AttemptCommand = (
    CreateTaskAttemptCmd
    | RecordExecutionSpecCmd
    | MarkWaitingProviderCmd
    | MarkWaitingReconciliationCmd
    | MarkBlockedCmd
    | MarkFailedCmd
    | RecordProviderResultCmd
)


# --------------------------------------------------------------------------- #
# Events
# --------------------------------------------------------------------------- #


class ProviderExecutionSpecRecordedEvt(MessagePayload):
    type: Literal["provider_execution_spec_recorded"] = "provider_execution_spec_recorded"
    attempt_id: str
    spec: ProviderExecutionSpec


class AttemptWaitingProviderEvt(MessagePayload):
    type: Literal["attempt_waiting_provider"] = "attempt_waiting_provider"
    attempt_id: str


class AttemptWaitingReconciliationEvt(MessagePayload):
    type: Literal["attempt_waiting_reconciliation"] = "attempt_waiting_reconciliation"
    attempt_id: str


class AttemptBlockedEvt(MessagePayload):
    type: Literal["attempt_blocked"] = "attempt_blocked"
    attempt_id: str
    reason: str


class AttemptFailedEvt(MessagePayload):
    type: Literal["attempt_failed"] = "attempt_failed"
    attempt_id: str
    reason: str


AttemptEvent = (
    TaskAttemptCreatedEvt
    | TaskInputsBoundEvt
    | ArtifactCandidateProducedEvt
    | ProviderExecutionSpecRecordedEvt
    | AttemptWaitingProviderEvt
    | AttemptWaitingReconciliationEvt
    | AttemptBlockedEvt
    | AttemptFailedEvt
)
