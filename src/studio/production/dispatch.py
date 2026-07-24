"""命令 -> 权威目标流。Router 用它校验 payload aggregate id 与 target 流一致。"""

from __future__ import annotations

from typing import Any

from studio.kernel.errors import ContractViolation

from . import identity
from .attempt_payloads import (
    MarkBlockedCmd,
    MarkFailedCmd,
    MarkWaitingProviderCmd,
    MarkWaitingReconciliationCmd,
    RecordExecutionSpecCmd,
    RecordProviderResultCmd,
)
from .budget import (
    AdjustBudgetCmd,
    InitializeBudgetCmd,
    ReleaseBudgetCmd,
    ReserveBudgetCmd,
    SettleBudgetCmd,
)
from .payloads import (
    AcceptArtifactVersionCmd,
    CreateTaskAttemptCmd,
    ExpandStageCmd,
    InitializePipelineCmd,
    MarkArtifactStaleCmd,
    ProposeArtifactVersionCmd,
    RejectArtifactVersionCmd,
    RevokeArtifactAcceptanceCmd,
)
from .provider_op import (
    AbortBeforeSubmissionCmd,
    ClaimSubmissionCmd,
    InitiateProviderOpCmd,
    ReconcileFailedCmd,
    ReconcileSubmittedCmd,
    ReconcileSucceededCmd,
    RecordFailedCmd,
    RecordSubmissionUnknownCmd,
    RecordSubmittedCmd,
    RecordSucceededCmd,
)
from .reconcile import EmitReconciliationTickCmd


def canonical_target(command: Any) -> str:
    if isinstance(command, InitializePipelineCmd | ExpandStageCmd):
        return identity.project_stream(command.project_id)
    if isinstance(
        command,
        ProposeArtifactVersionCmd
        | AcceptArtifactVersionCmd
        | RejectArtifactVersionCmd,
    ):
        return identity.series_stream(command.series_id)
    if isinstance(command, MarkArtifactStaleCmd):
        return identity.series_stream(command.target_ref.series_id)
    if isinstance(command, RevokeArtifactAcceptanceCmd):
        return identity.series_stream(command.artifact_ref.series_id)
    if isinstance(
        command,
        InitializeBudgetCmd
        | ReserveBudgetCmd
        | SettleBudgetCmd
        | ReleaseBudgetCmd
        | AdjustBudgetCmd,
    ):
        return identity.budget_stream(command.project_id)
    if isinstance(
        command,
        InitiateProviderOpCmd
        | ClaimSubmissionCmd
        | RecordSubmittedCmd
        | RecordSucceededCmd
        | RecordFailedCmd
        | RecordSubmissionUnknownCmd
        | AbortBeforeSubmissionCmd
        | ReconcileSubmittedCmd
        | ReconcileSucceededCmd
        | ReconcileFailedCmd,
    ):
        return identity.provider_op_stream(command.operation_id)
    if isinstance(
        command,
        CreateTaskAttemptCmd
        | RecordExecutionSpecCmd
        | MarkWaitingProviderCmd
        | MarkWaitingReconciliationCmd
        | MarkBlockedCmd
        | MarkFailedCmd
        | RecordProviderResultCmd,
    ):
        return identity.attempt_stream(command.attempt_id)
    if isinstance(command, EmitReconciliationTickCmd):
        return identity.reconciliation_stream(command.scope)
    raise ContractViolation(f"未知命令类型:{type(command).__name__}")
