"""TaskAttemptDecider(attempt 流):原子产出 Created + InputsBound + CandidateProduced。

candidate 由注入的确定性 mock executor 生成(M4 换真实 Provider)。
Attempt 只产 candidate,不分配 revision(revision 归 ArtifactSeriesDecider)。
"""

from __future__ import annotations

from collections.abc import Callable

from pydantic import BaseModel, ConfigDict

from studio.domain import ids as domain_ids
from studio.domain.artifacts import ArtifactPayload
from studio.kernel.decisions import Accepted, ProposedEvent, Rejected
from studio.serialization import digest

from . import identity
from .payloads import (
    ArtifactCandidateProducedEvt,
    CreateTaskAttemptCmd,
    ProductionCommand,
    ProductionEvent,
    TaskAttemptCreatedEvt,
    TaskInputsBoundEvt,
)
from .values import BindingItem

# executor(stage_id, exact_refs, partition_key) -> 候选产物内容
Executor = Callable[[str, tuple[BindingItem, ...], str | None], ArtifactPayload]


class AttemptState(BaseModel):
    model_config = ConfigDict(frozen=True)

    created: bool = False


class TaskAttemptDecider:
    def __init__(self, executors: dict[str, Executor]) -> None:
        self._executors = executors

    def initial_state(self) -> AttemptState:
        return AttemptState()

    def decide(
        self, state: AttemptState, command: ProductionCommand
    ) -> Accepted[ProductionEvent] | Rejected:
        if not isinstance(command, CreateTaskAttemptCmd):
            return Rejected("unexpected_command", f"attempt 流不处理 {command.type}")
        if state.created:
            return Rejected("already_created", "attempt 已创建")

        # 重新计算并验证确定性身份,拒绝伪造的 series_id / attempt_id。
        expected_series = domain_ids.series_id(
            command.project_id, command.output_key, command.partition_key
        )
        if command.series_id != expected_series:
            return Rejected("forged_series", "series_id 与派生不一致")
        tk = identity.task_key(
            command.project_id, command.stage_id, command.partition_key
        )
        expected_attempt = identity.attempt_id(
            tk, identity.input_binding_digest(command.exact_refs), 0
        )
        if command.attempt_id != expected_attempt:
            return Rejected("forged_attempt", "attempt_id 与派生不一致")

        executor = self._executors.get(command.stage_id)
        if executor is None:
            return Rejected("no_executor", f"stage {command.stage_id} 无 executor")
        payload = executor(command.stage_id, command.exact_refs, command.partition_key)
        candidate = identity.candidate_id(command.attempt_id, command.output_key)

        return Accepted(
            (
                ProposedEvent(
                    "task-attempt-created",
                    TaskAttemptCreatedEvt(
                        attempt_id=command.attempt_id,
                        project_id=command.project_id,
                        stage_id=command.stage_id,
                        partition_key=command.partition_key,
                        output_key=command.output_key,
                        series_id=command.series_id,
                    ),
                ),
                ProposedEvent(
                    "task-inputs-bound",
                    TaskInputsBoundEvt(
                        attempt_id=command.attempt_id, exact_refs=command.exact_refs
                    ),
                ),
                ProposedEvent(
                    "artifact-candidate-produced",
                    ArtifactCandidateProducedEvt(
                        candidate_id=candidate,
                        attempt_id=command.attempt_id,
                        project_id=command.project_id,
                        series_id=command.series_id,
                        output_key=command.output_key,
                        partition_key=command.partition_key,
                        digest=digest(payload),
                        payload=payload,
                    ),
                ),
            )
        )

    def evolve(self, state: AttemptState, event: ProductionEvent) -> AttemptState:
        if isinstance(event, TaskAttemptCreatedEvt):
            return state.model_copy(update={"created": True})
        return state
