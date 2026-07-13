"""ArtifactSeriesDecider(artifact-series 流):分配 revision、接受、记录 stale 事实。

- revision 由本 decider 依据 series 状态单调分配(杜绝并发 attempt 各自认领同一 revision)。
- candidate 指纹去重:同一 candidate_id 异内容 => IdempotencyConflict;同内容 => 幂等空操作。
- 校验 digest(payload) 与命令 digest 一致。
- stale 采用"原因级"幂等:(target, invalidated, replacement) 相同才视为重复。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from studio.domain import ids as domain_ids
from studio.domain.artifacts import ArtifactRef
from studio.domain.enums import AcceptanceMode
from studio.kernel.decisions import Accepted, ProposedEvent, Rejected
from studio.kernel.errors import IdempotencyConflict
from studio.serialization import digest as compute_digest

from .payloads import (
    ArtifactMarkedStaleEvt,
    ArtifactVersionAcceptedEvt,
    ArtifactVersionProposedEvt,
    MarkArtifactStaleCmd,
    ProductionCommand,
    ProductionEvent,
    ProposeArtifactVersionCmd,
)


class CandidateRecord(BaseModel):
    model_config = ConfigDict(frozen=True)
    candidate_id: str
    digest: str
    produced_by_attempt: str
    output_key: str


class SeriesState(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_revision: int = 0
    current_ref: ArtifactRef | None = None
    candidates: tuple[CandidateRecord, ...] = ()
    stale_reasons: tuple[tuple[str, str, str], ...] = ()

    def candidate(self, candidate_id: str) -> CandidateRecord | None:
        return next(
            (c for c in self.candidates if c.candidate_id == candidate_id), None
        )


class ArtifactSeriesDecider:
    def initial_state(self) -> SeriesState:
        return SeriesState()

    def decide(
        self, state: SeriesState, command: ProductionCommand
    ) -> Accepted[ProductionEvent] | Rejected:
        if isinstance(command, ProposeArtifactVersionCmd):
            return self._propose(state, command)
        if isinstance(command, MarkArtifactStaleCmd):
            return self._mark_stale(state, command)
        return Rejected("unexpected_command", f"series 流不处理 {command.type}")

    def _propose(
        self, state: SeriesState, command: ProposeArtifactVersionCmd
    ) -> Accepted[ProductionEvent] | Rejected:
        if compute_digest(command.payload) != command.digest:
            return Rejected("digest_mismatch", "digest 与 payload 不一致")

        prior = state.candidate(command.candidate_id)
        if prior is not None:
            same = (
                prior.digest == command.digest
                and prior.produced_by_attempt == command.produced_by_attempt
                and prior.output_key == command.output_key
            )
            if not same:
                raise IdempotencyConflict(
                    command.candidate_id, "同 candidate_id 复用于不同内容"
                )
            return Accepted(())  # 幂等:同一 candidate 不再分配新 revision

        revision = state.max_revision + 1
        ref = ArtifactRef(
            artifact_id=domain_ids.artifact_id(command.series_id, revision),
            series_id=command.series_id,
            revision=revision,
            digest=command.digest,
        )
        events: list[ProposedEvent[ProductionEvent]] = [
            ProposedEvent(
                f"proposed:r{revision}",
                ArtifactVersionProposedEvt(
                    project_id=command.project_id,
                    series_id=command.series_id,
                    revision=revision,
                    artifact_ref=ref,
                    candidate_id=command.candidate_id,
                    produced_by_attempt=command.produced_by_attempt,
                    output_key=command.output_key,
                    partition_key=command.partition_key,
                ),
            )
        ]
        if command.acceptance_mode is AcceptanceMode.AUTO:
            events.append(
                ProposedEvent(
                    f"accepted:r{revision}",
                    ArtifactVersionAcceptedEvt(
                        project_id=command.project_id,
                        series_id=command.series_id,
                        revision=revision,
                        artifact_ref=ref,
                        previous_current_ref=state.current_ref,
                        candidate_id=command.candidate_id,
                        produced_by_attempt=command.produced_by_attempt,
                        output_key=command.output_key,
                        partition_key=command.partition_key,
                    ),
                )
            )
        return Accepted(tuple(events))

    def _mark_stale(
        self, state: SeriesState, command: MarkArtifactStaleCmd
    ) -> Accepted[ProductionEvent] | Rejected:
        reason = (
            command.target_ref.artifact_id,
            command.invalidated_input_ref.artifact_id,
            command.replacement_ref.artifact_id,
        )
        if reason in state.stale_reasons:
            return Accepted(())  # 原因级幂等
        return Accepted(
            (
                ProposedEvent(
                    f"stale:{reason[0]}:{reason[1]}",
                    ArtifactMarkedStaleEvt(
                        target_ref=command.target_ref,
                        invalidated_input_ref=command.invalidated_input_ref,
                        replacement_ref=command.replacement_ref,
                        root_cause_event_id=command.root_cause_event_id,
                        scope=command.scope,
                        task_key=command.task_key,
                        partition_key=command.partition_key,
                    ),
                ),
            )
        )

    def evolve(self, state: SeriesState, event: ProductionEvent) -> SeriesState:
        if isinstance(event, ArtifactVersionProposedEvt):
            record = CandidateRecord(
                candidate_id=event.candidate_id,
                digest=event.artifact_ref.digest,
                produced_by_attempt=event.produced_by_attempt,
                output_key=event.output_key,
            )
            return state.model_copy(
                update={
                    "max_revision": event.revision,
                    "candidates": (*state.candidates, record),
                }
            )
        if isinstance(event, ArtifactVersionAcceptedEvt):
            return state.model_copy(update={"current_ref": event.artifact_ref})
        if isinstance(event, ArtifactMarkedStaleEvt):
            reason = (
                event.target_ref.artifact_id,
                event.invalidated_input_ref.artifact_id,
                event.replacement_ref.artifact_id,
            )
            return state.model_copy(
                update={"stale_reasons": (*state.stale_reasons, reason)}
            )
        return state
