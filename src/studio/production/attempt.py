"""TaskAttemptDecider(attempt 流,唯一 writer)。

- AGENT/TRANSFORM(或已注册 mock executor 的 stage):同步产候选(M3 行为)。
- PROVIDER 且无 executor:只产 Created+InputsBound,经异步 provider 流水线产候选。
状态机严格封死;executor_kind 以注入的 CompiledPipelineSpec 按 stage_id 校验,不信任命令字段。
"""

from __future__ import annotations

from collections.abc import Callable

from pydantic import BaseModel, ConfigDict

from studio.domain import ids as domain_ids
from studio.domain.artifacts import ArtifactPayload
from studio.domain.enums import ExecutorKind, TaskAttemptStatus
from studio.kernel.decisions import Accepted, ProposedEvent, Rejected
from studio.kernel.errors import IdempotencyConflict
from studio.serialization import digest

from . import identity
from .attempt_payloads import (
    AttemptBlockedEvt,
    AttemptCommand,
    AttemptEvent,
    AttemptFailedEvt,
    AttemptWaitingProviderEvt,
    AttemptWaitingReconciliationEvt,
    MarkBlockedCmd,
    MarkFailedCmd,
    MarkWaitingProviderCmd,
    MarkWaitingReconciliationCmd,
    ProviderExecutionSpecRecordedEvt,
    RecordExecutionSpecCmd,
    RecordProviderResultCmd,
)
from .compile import CompiledPipelineSpec
from .execution_spec import ProviderExecutionSpec
from .payloads import (
    ArtifactCandidateProducedEvt,
    CreateTaskAttemptCmd,
    TaskAttemptCreatedEvt,
    TaskInputsBoundEvt,
)
from .values import BindingItem

Executor = Callable[[str, tuple[BindingItem, ...], str | None], ArtifactPayload]

_S = TaskAttemptStatus


class AttemptState(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: TaskAttemptStatus | None = None
    attempt_id: str | None = None
    stage_id: str | None = None
    project_id: str | None = None
    output_key: str | None = None
    series_id: str | None = None
    partition_key: str | None = None
    exact_refs: tuple[BindingItem, ...] = ()
    spec: ProviderExecutionSpec | None = None
    result_fingerprint: str | None = None


class TaskAttemptDecider:
    def __init__(
        self, spec: CompiledPipelineSpec, executors: dict[str, Executor]
    ) -> None:
        # PROVIDER stage 必须无条件走异步路径;注册同步 executor 属配置错误,fail-fast。
        for stage_id in executors:
            st = spec.by_stage(stage_id)
            if st is not None and st.executor_kind is ExecutorKind.PROVIDER:
                raise ValueError(
                    f"PROVIDER stage {stage_id} 不得注册同步 executor(必须走异步流水线)"
                )
        self._spec = spec
        self._executors = executors

    def initial_state(self) -> AttemptState:
        return AttemptState()

    def decide(
        self, state: AttemptState, command: AttemptCommand
    ) -> Accepted[AttemptEvent] | Rejected:
        # owner 封闭:一旦创建,后续命令的 attempt_id 必须一致。
        if state.attempt_id is not None and command.attempt_id != state.attempt_id:
            return Rejected("wrong_attempt", "attempt_id 与本流不一致")
        if isinstance(command, CreateTaskAttemptCmd):
            return self._create(state, command)
        if isinstance(command, RecordExecutionSpecCmd):
            return self._record_spec(state, command)
        if isinstance(command, MarkWaitingProviderCmd):
            return self._transition(
                state, {_S.WAITING_BUDGET, _S.WAITING_RECONCILIATION}, _S.WAITING_PROVIDER,
                AttemptWaitingProviderEvt(attempt_id=command.attempt_id), "waiting-provider",
            )
        if isinstance(command, MarkWaitingReconciliationCmd):
            return self._transition(
                state, {_S.WAITING_PROVIDER}, _S.WAITING_RECONCILIATION,
                AttemptWaitingReconciliationEvt(attempt_id=command.attempt_id),
                "waiting-reconciliation",
            )
        if isinstance(command, MarkBlockedCmd):
            return self._transition(
                state, {_S.WAITING_BUDGET}, _S.BLOCKED,
                AttemptBlockedEvt(attempt_id=command.attempt_id, reason=command.reason),
                "blocked",
            )
        if isinstance(command, MarkFailedCmd):
            return self._transition(
                state,
                {_S.WAITING_BUDGET, _S.WAITING_PROVIDER, _S.WAITING_RECONCILIATION},
                _S.FAILED,
                AttemptFailedEvt(attempt_id=command.attempt_id, reason=command.reason),
                "failed",
            )
        if isinstance(command, RecordProviderResultCmd):
            return self._record_result(state, command)
        return Rejected("unexpected_command", "attempt 流不处理该命令")

    # -- CreateTaskAttempt ------------------------------------------------- #

    def _create(
        self, state: AttemptState, cmd: CreateTaskAttemptCmd
    ) -> Accepted[AttemptEvent] | Rejected:
        if state.status is not None:
            return Rejected("already_created", "attempt 已创建")
        stage = self._spec.by_stage(cmd.stage_id)
        if stage is None:
            return Rejected("unknown_stage", f"未知 stage {cmd.stage_id}")
        if cmd.output_key != stage.output_key:
            return Rejected("output_key_mismatch", "output_key 与 stage 输出不一致")
        if cmd.series_id != domain_ids.series_id(
            cmd.project_id, cmd.output_key, cmd.partition_key
        ):
            return Rejected("forged_series", "series_id 与派生不一致")
        tk = identity.task_key(cmd.project_id, cmd.stage_id, cmd.partition_key)
        if cmd.attempt_id != identity.attempt_id(
            tk, identity.input_binding_digest(cmd.exact_refs), 0
        ):
            return Rejected("forged_attempt", "attempt_id 与派生不一致")

        base: list[ProposedEvent[AttemptEvent]] = [
            ProposedEvent(
                "task-attempt-created",
                TaskAttemptCreatedEvt(
                    attempt_id=cmd.attempt_id, project_id=cmd.project_id,
                    stage_id=cmd.stage_id, partition_key=cmd.partition_key,
                    output_key=cmd.output_key, series_id=cmd.series_id,
                ),
            ),
            ProposedEvent(
                "task-inputs-bound",
                TaskInputsBoundEvt(attempt_id=cmd.attempt_id, exact_refs=cmd.exact_refs),
            ),
        ]

        if stage.executor_kind is ExecutorKind.PROVIDER:
            # 无条件异步:PROVIDER 只产 Created+InputsBound,经 provider 流水线产候选。
            return Accepted(tuple(base))
        executor = self._executors.get(cmd.stage_id)
        if executor is None:
            return Rejected("no_executor", f"stage {cmd.stage_id} 无 executor")
        payload = executor(cmd.stage_id, cmd.exact_refs, cmd.partition_key)
        base.append(
            ProposedEvent(
                "artifact-candidate-produced",
                ArtifactCandidateProducedEvt(
                    candidate_id=identity.candidate_id(cmd.attempt_id, cmd.output_key),
                    attempt_id=cmd.attempt_id, project_id=cmd.project_id,
                    series_id=cmd.series_id, output_key=cmd.output_key,
                    partition_key=cmd.partition_key, digest=digest(payload),
                    payload=payload,
                ),
            )
        )
        return Accepted(tuple(base))

    # -- RecordExecutionSpec ----------------------------------------------- #

    def _record_spec(
        self, state: AttemptState, cmd: RecordExecutionSpecCmd
    ) -> Accepted[AttemptEvent] | Rejected:
        if state.stage_id is None:
            return Rejected("bad_transition", "attempt 未创建")
        stage = self._spec.by_stage(state.stage_id)
        if stage is None or stage.executor_kind is not ExecutorKind.PROVIDER:
            return Rejected("not_provider_stage", "仅 PROVIDER stage 记录 ExecutionSpec")
        if state.status is _S.WAITING_BUDGET:
            if state.spec == cmd.spec:
                return Accepted(())  # 幂等
            raise IdempotencyConflict(cmd.attempt_id, "ExecutionSpec 冲突")
        if state.status is not _S.INPUTS_BOUND:
            return Rejected("bad_transition", f"record_spec 不允许自 {state.status}")
        if cmd.spec.attempt_id != cmd.attempt_id:
            return Rejected("spec_attempt_mismatch", "spec.attempt_id 不一致")
        try:
            cmd.spec.verify_membership(state.exact_refs)
        except ValueError:
            return Rejected("plan_not_bound", "plan_ref 不属于该 Attempt")
        return Accepted(
            (
                ProposedEvent(
                    "execution-spec-recorded",
                    ProviderExecutionSpecRecordedEvt(
                        attempt_id=cmd.attempt_id, spec=cmd.spec
                    ),
                ),
            )
        )

    # -- RecordProviderResult ---------------------------------------------- #

    def _record_result(
        self, state: AttemptState, cmd: RecordProviderResultCmd
    ) -> Accepted[AttemptEvent] | Rejected:
        if state.status not in (
            _S.SUCCEEDED, _S.WAITING_PROVIDER, _S.WAITING_RECONCILIATION
        ):
            return Rejected("bad_transition", f"record_result 不允许自 {state.status}")
        # owner(attempt_id)已在 decide 顶层校验;此处先封 operation_id / 输出类型 / blob_ref,
        # 再比对结果指纹,避免伪造 operation_id/blob_ref 借相同 payload 冒充幂等成功。
        if state.spec is None or cmd.operation_id != state.spec.operation_id:
            return Rejected("operation_mismatch", "operation_id 与 spec 不一致")
        stage = self._spec.by_stage(state.stage_id) if state.stage_id else None
        assert stage is not None
        if cmd.payload.kind is not stage.output_artifact_type:
            return Rejected("output_type_mismatch", "payload 类型与 stage 输出不一致")
        payload_blob = getattr(cmd.payload, "blob_ref", None)
        if payload_blob is not None and payload_blob != cmd.blob_ref:
            return Rejected("blob_ref_mismatch", "payload.blob_ref 与结果不一致")
        fingerprint = _result_fingerprint(
            cmd.attempt_id, cmd.operation_id, cmd.blob_ref, cmd.payload
        )
        if state.status is _S.SUCCEEDED:
            if state.result_fingerprint == fingerprint:
                return Accepted(())  # 幂等(全字段一致)
            raise IdempotencyConflict(cmd.attempt_id, "provider 结果不一致")
        assert state.output_key is not None
        return Accepted(
            (
                ProposedEvent(
                    "artifact-candidate-produced",
                    ArtifactCandidateProducedEvt(
                        candidate_id=identity.candidate_id(
                            cmd.attempt_id, state.output_key
                        ),
                        attempt_id=cmd.attempt_id, project_id=state.project_id or "",
                        series_id=state.series_id or "", output_key=state.output_key,
                        partition_key=state.partition_key, digest=digest(cmd.payload),
                        payload=cmd.payload,
                    ),
                ),
            )
        )

    # -- helpers ----------------------------------------------------------- #

    def _transition(
        self,
        state: AttemptState,
        allowed: set[TaskAttemptStatus],
        target: TaskAttemptStatus,
        event: AttemptEvent,
        key: str,
    ) -> Accepted[AttemptEvent] | Rejected:
        if state.status is target:
            return Accepted(())  # 幂等
        if state.status not in allowed:
            return Rejected("bad_transition", f"{target} 不允许自 {state.status}")
        return Accepted((ProposedEvent(key, event),))

    def evolve(self, state: AttemptState, event: AttemptEvent) -> AttemptState:
        if isinstance(event, TaskAttemptCreatedEvt):
            return state.model_copy(
                update={
                    "status": _S.INPUTS_BOUND,
                    "attempt_id": event.attempt_id,
                    "stage_id": event.stage_id,
                    "project_id": event.project_id,
                    "output_key": event.output_key,
                    "series_id": event.series_id,
                    "partition_key": event.partition_key,
                }
            )
        if isinstance(event, TaskInputsBoundEvt):
            return state.model_copy(update={"exact_refs": event.exact_refs})
        if isinstance(event, ProviderExecutionSpecRecordedEvt):
            return state.model_copy(
                update={"status": _S.WAITING_BUDGET, "spec": event.spec}
            )
        if isinstance(event, AttemptWaitingProviderEvt):
            return state.model_copy(update={"status": _S.WAITING_PROVIDER})
        if isinstance(event, AttemptWaitingReconciliationEvt):
            return state.model_copy(update={"status": _S.WAITING_RECONCILIATION})
        if isinstance(event, AttemptBlockedEvt):
            return state.model_copy(update={"status": _S.BLOCKED})
        if isinstance(event, AttemptFailedEvt):
            return state.model_copy(update={"status": _S.FAILED})
        if isinstance(event, ArtifactCandidateProducedEvt):
            op_id = state.spec.operation_id if state.spec else None
            blob = getattr(event.payload, "blob_ref", None)
            return state.model_copy(
                update={
                    "status": _S.SUCCEEDED,
                    "result_fingerprint": _result_fingerprint(
                        state.attempt_id, op_id, blob, event.payload
                    ),
                }
            )
        return state


def _result_fingerprint(
    attempt_id: str | None,
    operation_id: str | None,
    blob_ref: str | None,
    payload: ArtifactPayload,
) -> str:
    """结果幂等指纹:含 owner + operation + blob_ref + payload,异结果必冲突。"""
    return digest(
        {
            "attempt_id": attempt_id,
            "operation_id": operation_id,
            "blob_ref": blob_ref,
            "payload_digest": digest(payload),
        }
    )
