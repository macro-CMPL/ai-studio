"""ExecutionPlanningPM(纯):把已接受的 Plan 关联到 PROVIDER Attempt,产出 ExecutionSpec。

双事件关联(ArtifactVersionAccepted 不携带 payload):
  ArtifactCandidateProduced(candidate_id, plan_payload) -> 缓存 candidate_id -> payload
  ArtifactVersionAccepted(candidate_id, artifact_ref)    -> 建立 artifact_id -> payload
再与 PROVIDER Attempt 的 TaskInputsBound 做 join(两边任一先到都重试规划)。
从 CompiledStage 的 requirements 确定唯一 IMAGE_PLAN 输入(构造期校验缺失/多个)。
报价用版本化纯函数 quote(provider_id, provider_version, operation, pricing_version)。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

from pydantic import BaseModel, ConfigDict

from studio.domain._base import Currency, NonNegativeMoney
from studio.domain.artifacts import ArtifactRef, ImagePlanPayload, PlannedOperation
from studio.domain.enums import ArtifactType, ExecutorKind
from studio.kernel.envelopes import EventEnvelope, MessagePayload
from studio.kernel.process_manager import ProposedCommand, Reaction

from . import identity
from .attempt_payloads import RecordExecutionSpecCmd
from .compile import CompiledPipelineSpec, CompiledStage
from .execution_spec import ProviderExecutionSpec
from .payloads import (
    ArtifactCandidateProducedEvt,
    ArtifactVersionAcceptedEvt,
    TaskAttemptCreatedEvt,
    TaskInputsBoundEvt,
)
from .values import BindingItem

_PLAN_TYPE = ArtifactType.IMAGE_PLAN


class ProviderBinding(BaseModel):
    """PROVIDER stage 的固定 provider 绑定(报价与执行的身份维度)。"""

    model_config = ConfigDict(frozen=True)
    provider_id: str
    provider_version: str
    pricing_version: str


class QuoteResult(BaseModel):
    model_config = ConfigDict(frozen=True)
    estimated_cost: NonNegativeMoney
    currency: Currency


QuoteFn = Callable[[str, str, PlannedOperation, str], QuoteResult]


# --------------------------------------------------------------------------- #
# State
# --------------------------------------------------------------------------- #


class PlanCandidate(BaseModel):
    model_config = ConfigDict(frozen=True)
    candidate_id: str
    payload: ImagePlanPayload


class AcceptedPlan(BaseModel):
    model_config = ConfigDict(frozen=True)
    artifact_id: str
    payload: ImagePlanPayload


class PlanningAttempt(BaseModel):
    model_config = ConfigDict(frozen=True)
    attempt_id: str
    stage_id: str
    exact_refs: tuple[BindingItem, ...] = ()
    bound: bool = False


class PlanningState(BaseModel):
    model_config = ConfigDict(frozen=True)
    candidates: tuple[PlanCandidate, ...] = ()
    accepted: tuple[AcceptedPlan, ...] = ()
    attempts: tuple[PlanningAttempt, ...] = ()
    planned: tuple[str, ...] = ()

    def candidate_payload(self, candidate_id: str) -> ImagePlanPayload | None:
        return next(
            (c.payload for c in self.candidates if c.candidate_id == candidate_id), None
        )

    def accepted_payload(self, artifact_id: str) -> ImagePlanPayload | None:
        return next(
            (a.payload for a in self.accepted if a.artifact_id == artifact_id), None
        )

    def attempt(self, attempt_id: str) -> PlanningAttempt | None:
        return next((a for a in self.attempts if a.attempt_id == attempt_id), None)

    def with_attempt(self, updated: PlanningAttempt) -> tuple[PlanningAttempt, ...]:
        others = tuple(a for a in self.attempts if a.attempt_id != updated.attempt_id)
        return (*others, updated)


# --------------------------------------------------------------------------- #
# PM
# --------------------------------------------------------------------------- #


class ExecutionPlanningProcessManager:
    pm_id = "execution-planning-pm"

    def __init__(
        self,
        spec: CompiledPipelineSpec,
        bindings: Mapping[str, ProviderBinding],
        quote: QuoteFn,
    ) -> None:
        self._spec = spec
        self._bindings = dict(bindings)
        self._quote = quote
        # fail fast:每个 PROVIDER stage 必须有恰好一个 IMAGE_PLAN 输入 + 已注册绑定。
        for stage in spec.stages:
            if stage.executor_kind is not ExecutorKind.PROVIDER:
                continue
            plan_reqs = [
                r for r in stage.requirements if r.artifact_type is _PLAN_TYPE
            ]
            if len(plan_reqs) != 1:
                raise ValueError(
                    f"PROVIDER stage {stage.stage_id} 必须恰好一个 {_PLAN_TYPE} 输入,"
                    f"实得 {len(plan_reqs)}"
                )
            if stage.stage_id not in self._bindings:
                raise ValueError(f"PROVIDER stage {stage.stage_id} 缺少 ProviderBinding")

    def initial_state(self) -> PlanningState:
        return PlanningState()

    def react(
        self, state: PlanningState, event: EventEnvelope[MessagePayload]
    ) -> Reaction[PlanningState, RecordExecutionSpecCmd]:
        payload = event.payload
        if isinstance(payload, TaskAttemptCreatedEvt):
            stage = self._spec.by_stage(payload.stage_id)
            if stage is None or stage.executor_kind is not ExecutorKind.PROVIDER:
                return Reaction(state=state, commands=())
            entry = PlanningAttempt(attempt_id=payload.attempt_id, stage_id=payload.stage_id)
            state = state.model_copy(update={"attempts": state.with_attempt(entry)})
            return self._plan_all(state)
        if isinstance(payload, TaskInputsBoundEvt):
            existing = state.attempt(payload.attempt_id)
            if existing is None:
                return Reaction(state=state, commands=())
            updated = existing.model_copy(
                update={"exact_refs": payload.exact_refs, "bound": True}
            )
            state = state.model_copy(update={"attempts": state.with_attempt(updated)})
            return self._plan_all(state)
        if isinstance(payload, ArtifactCandidateProducedEvt):
            if not isinstance(payload.payload, ImagePlanPayload):
                return Reaction(state=state, commands=())
            entry_c = PlanCandidate(candidate_id=payload.candidate_id, payload=payload.payload)
            return Reaction(
                state=state.model_copy(update={"candidates": (*state.candidates, entry_c)}),
                commands=(),
            )
        if isinstance(payload, ArtifactVersionAcceptedEvt):
            plan_payload = state.candidate_payload(payload.candidate_id)
            if plan_payload is None:
                return Reaction(state=state, commands=())  # 非 Plan 产物
            entry_a = AcceptedPlan(
                artifact_id=payload.artifact_ref.artifact_id, payload=plan_payload
            )
            state = state.model_copy(update={"accepted": (*state.accepted, entry_a)})
            return self._plan_all(state)
        return Reaction(state=state, commands=())

    def _plan_all(
        self, state: PlanningState
    ) -> Reaction[PlanningState, RecordExecutionSpecCmd]:
        commands: list[ProposedCommand[RecordExecutionSpecCmd]] = []
        for attempt in state.attempts:
            if attempt.attempt_id in state.planned or not attempt.bound:
                continue
            stage = self._spec.by_stage(attempt.stage_id)
            assert stage is not None
            plan_ref = self._plan_ref(stage, attempt.exact_refs)
            if plan_ref is None:
                continue  # 该 Attempt 没有绑定 Plan 输入(异常配置)
            plan_payload = state.accepted_payload(plan_ref.artifact_id)
            if plan_payload is None:
                continue  # Plan 尚未接受,稍后重试
            spec = self._build_spec(attempt.attempt_id, stage, plan_ref, plan_payload)
            commands.append(
                ProposedCommand(
                    reaction_name="plan",
                    command_key=attempt.attempt_id,
                    target=identity.attempt_stream(attempt.attempt_id),
                    payload=RecordExecutionSpecCmd(
                        attempt_id=attempt.attempt_id, spec=spec
                    ),
                )
            )
            state = state.model_copy(update={"planned": (*state.planned, attempt.attempt_id)})
        return Reaction(state=state, commands=tuple(commands))

    def _plan_ref(
        self, stage: CompiledStage, exact_refs: tuple[BindingItem, ...]
    ) -> ArtifactRef | None:
        req = next(r for r in stage.requirements if r.artifact_type is _PLAN_TYPE)
        binding = next(
            (b for b in exact_refs if b.requirement_key == req.requirement_key), None
        )
        return binding.to_ref() if binding is not None else None

    def _build_spec(
        self,
        attempt_id: str,
        stage: CompiledStage,
        plan_ref: ArtifactRef,
        plan_payload: ImagePlanPayload,
    ) -> ProviderExecutionSpec:
        binding = self._bindings[stage.stage_id]
        op = plan_payload.operations[0]
        quote = self._quote(
            binding.provider_id, binding.provider_version, op, binding.pricing_version
        )
        return ProviderExecutionSpec.from_plan(
            attempt_id=attempt_id,
            plan_ref=plan_ref,
            plan_payload=plan_payload,
            provider_id=binding.provider_id,
            provider_version=binding.provider_version,
            estimated_cost=quote.estimated_cost,
            currency=quote.currency,
            pricing_version=binding.pricing_version,
            request_ref=plan_ref.artifact_id,
        )
