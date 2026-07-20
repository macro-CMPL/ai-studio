"""M4 Step 4:ProviderActivityWorker + FakeProvider 集成/单元测试。

覆盖:happy path 恰好一次计费、ambiguous 提交经 lookup 恢复、真实崩溃(RecordSubmitted
落库前)经 lookup 恢复不重复扣费、webhook 与 poll 竞争只结算一次、retryable 未发出时
CLAIMED 不被自动释放、activity 命令稳定身份、poll 轮转公平。
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from fake_provider import FakeProvider
from kernel_helpers import FakeClock
from m4_helpers import (
    _TS,
    _cmd,
    build_activity_stack,
    init_budget_command,
    init_pipeline_command,
    initiated_ops,
    tick_command,
)
from studio.application.routing_worker import RoutingCommandWorker
from studio.domain import ids as domain_ids
from studio.domain.artifacts import (
    ArtifactRef,
    ImagePlanPayload,
    OperationParam,
    PlannedOperation,
)
from studio.domain.enums import ProviderOpStatus
from studio.infrastructure.memory._state import MemoryDatabase
from studio.infrastructure.memory.unit_of_work import (
    MemoryCommandBus,
    MemoryUnitOfWorkFactory,
)
from studio.production import identity
from studio.production.activity_worker import ProviderActivityWorker, activity_command_id
from studio.production.budget import BudgetReleasedEvt, BudgetSettlementCompletedEvt
from studio.production.dispatch import canonical_target
from studio.production.execution_spec import ProviderExecutionSpec
from studio.production.payloads import ArtifactVersionAcceptedEvt
from studio.production.provider_op import (
    InitiateProviderOpCmd,
    ProviderOperationAbortedEvt,
    ProviderOperationDecider,
    ProviderOperationSubmissionUnknownEvt,
)
from studio.production.provider_port import ProviderRequest
from studio.serialization import digest

_PROJECT = "p"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #


def _payloads(stack: object, kind: type) -> list[object]:
    db = stack.db  # type: ignore[attr-defined]
    return [e.payload for e in db.state.events if isinstance(e.payload, kind)]


def _accepted(stack: object, output_key: str) -> list[ArtifactVersionAcceptedEvt]:
    return [
        p
        for p in _payloads(stack, ArtifactVersionAcceptedEvt)
        if p.output_key == output_key  # type: ignore[attr-defined]
    ]


def _make_spec(attempt_id: str, shot: str) -> ProviderExecutionSpec:
    series = domain_ids.series_id(_PROJECT, "plan", shot)
    plan_payload = ImagePlanPayload(
        operations=(
            PlannedOperation(
                logical_operation_key=f"{shot}:image:v0",
                op_type="gen",
                params=(OperationParam(key="shot", value=shot),),
            ),
        )
    )
    plan_ref = ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, 1),
        series_id=series,
        revision=1,
        digest=digest(plan_payload),
    )
    return ProviderExecutionSpec.from_plan(
        attempt_id=attempt_id,
        plan_ref=plan_ref,
        plan_payload=plan_payload,
        provider_id="fake",
        provider_version="1",
        estimated_cost=Decimal("10"),
        currency="CNY",
        pricing_version="1",
        request_ref="req",
    )


def _request(spec: ProviderExecutionSpec) -> ProviderRequest:
    return ProviderRequest(
        operation_id=spec.operation_id,
        request_digest=spec.request_digest,
        provider_id=spec.provider_id,
        provider_version=spec.provider_version,
        expected_cost=spec.estimated_cost,
        currency=spec.currency,
    )


class _OpHarness:
    """provider-op 流 + ActivityWorker 的最小手动 tick 装配(不含完整流水线)。"""

    def __init__(self, provider: FakeProvider) -> None:
        self.db = MemoryDatabase()
        self.bus = MemoryCommandBus()
        self.clock = FakeClock()
        self.factory = MemoryUnitOfWorkFactory(self.db)
        self.provider = provider
        self.worker = RoutingCommandWorker(
            deciders={"provider-op": ProviderOperationDecider()},
            resolve_kind=identity.stream_kind,
            canonical_target=canonical_target,
            bus=self.bus,
            uow_factory=self.factory,
            clock=self.clock,
        )
        self.activity = ProviderActivityWorker(
            provider=provider, bus=self.bus, uow_factory=self.factory, clock=self.clock
        )

    def new_worker(self) -> ProviderActivityWorker:
        """模拟进程重启:全新 ActivityWorker(空内存簿记)。"""
        return ProviderActivityWorker(
            provider=self.provider, bus=self.bus, uow_factory=self.factory,
            clock=self.clock,
        )

    def drain_worker(self) -> None:
        while self.worker.tick():
            pass

    def initiate(self, spec: ProviderExecutionSpec) -> str:
        op = spec.operation_id
        self.bus.publish(
            _cmd(
                identity.provider_op_stream(op), f"init:{op}",
                InitiateProviderOpCmd(operation_id=op, spec=spec), f"init-{op}",
            )
        )
        self.drain_worker()
        return op

    def op_status(self, op: str) -> ProviderOpStatus | None:
        dec = ProviderOperationDecider()
        state = dec.initial_state()
        stream = identity.provider_op_stream(op)
        evs = sorted(
            (e for e in self.db.state.events if e.stream_id == stream),
            key=lambda e: e.global_position,
        )
        for e in evs:
            state = dec.evolve(state, e.payload)
        return state.status


# --------------------------------------------------------------------------- #
# 1. happy path:恰好一次计费
# --------------------------------------------------------------------------- #


def test_happy_path_charges_exactly_once() -> None:
    stack = build_activity_stack()
    stack.bus.publish(init_budget_command(_PROJECT))
    stack.bus.publish(init_pipeline_command(_PROJECT))
    stack.driver.run_until_quiescent()

    ops = initiated_ops(stack)
    assert len(ops) == 2
    assert stack.provider is not None
    for op, _ in ops:
        assert stack.provider.charge_count(op) == 1

    assert len(_accepted(stack, "image")) == 2
    settled = _payloads(stack, BudgetSettlementCompletedEvt)
    assert len(settled) == 2
    assert all(s.outcome == "captured" for s in settled)  # type: ignore[attr-defined]
    total = sum(s.captured_amount for s in settled)  # type: ignore[attr-defined]
    assert total == Decimal("20")


# --------------------------------------------------------------------------- #
# 2. ambiguous 提交(进程存活)-> UNKNOWN -> lookup 恢复
# --------------------------------------------------------------------------- #


def test_ambiguous_submission_recovers_via_lookup() -> None:
    provider = FakeProvider(ambiguous_all=True)
    stack = build_activity_stack(provider)
    stack.bus.publish(init_budget_command(_PROJECT))
    stack.bus.publish(init_pipeline_command(_PROJECT))
    stack.driver.run_until_quiescent()

    ops = initiated_ops(stack)
    assert len(ops) == 2
    # 每个 op 都经过 SUBMISSION_UNKNOWN
    assert len(_payloads(stack, ProviderOperationSubmissionUnknownEvt)) == 2
    for op, _ in ops:
        assert provider.charge_count(op) == 1  # ambiguous 只接单一次
    assert len(_accepted(stack, "image")) == 2


# --------------------------------------------------------------------------- #
# 3. 真实崩溃:submit 已接单并返回,RecordSubmitted 落库前进程死亡
#    -> 重启 worker 经 lookup 恢复,不二次扣费
# --------------------------------------------------------------------------- #


def test_process_crash_before_record_submitted_no_double_charge() -> None:
    provider = FakeProvider()
    h = _OpHarness(provider)
    spec = _make_spec("att-shot01", "shot_01")
    op = h.initiate(spec)
    assert h.op_status(op) is ProviderOpStatus.INITIATED

    # worker claim -> CLAIMED
    assert h.activity.tick() is True
    h.drain_worker()
    assert h.op_status(op) is ProviderOpStatus.CLAIMED

    # 崩溃窗口:submit 已在 provider 侧接单(charge=1),但 RecordSubmitted 丢失
    provider.force_submit(op, _request(spec))
    assert provider.charge_count(op) == 1

    # 重启:全新 worker 经 lookup 命中 -> RecordSubmitted(不再 submit)
    fresh = h.new_worker()
    assert fresh.tick() is True
    h.drain_worker()
    assert h.op_status(op) is ProviderOpStatus.SUBMITTED
    assert provider.charge_count(op) == 1  # 关键:未二次扣费

    # 继续 poll -> 成功,仍恰好一次计费
    assert fresh.tick() is True
    h.drain_worker()
    assert h.op_status(op) is ProviderOpStatus.SUCCEEDED
    assert provider.charge_count(op) == 1


# --------------------------------------------------------------------------- #
# 4. webhook 与 poll 竞争:两条终态都到达,只结算一次
# --------------------------------------------------------------------------- #


def test_webhook_and_poll_settle_once() -> None:
    stack = build_activity_stack()  # 健康 provider:poll 直接成功
    stack.bus.publish(init_budget_command(_PROJECT))
    stack.bus.publish(init_pipeline_command(_PROJECT))
    stack.driver.run_until_quiescent()

    ops = initiated_ops(stack)
    assert stack.provider is not None and stack.webhook is not None
    assert len(_payloads(stack, BudgetSettlementCompletedEvt)) == 2

    # poll 已结算后,webhook 再投递(不同 provider_event_id,相同结果)
    for op, spec in ops:
        stack.webhook.deliver_succeeded(
            operation_id=op,
            result_ref=stack.provider.result_ref_for(op),
            cost_actual=spec.estimated_cost,
            cost_currency=spec.currency,
            provider_event_id=f"hook-{op}",
        )
    stack.driver.run_until_quiescent()

    # 幂等:仍只有 2 条结算,计费仍为 1
    assert len(_payloads(stack, BudgetSettlementCompletedEvt)) == 2
    for op, _ in ops:
        assert stack.provider.charge_count(op) == 1


# --------------------------------------------------------------------------- #
# 5. retryable(确定未发出):CLAIMED 不被自动 abort/release
# --------------------------------------------------------------------------- #


def test_claimed_not_auto_released_on_retryable_send() -> None:
    provider = FakeProvider(retryable_all=True)
    stack = build_activity_stack(provider)
    stack.bus.publish(init_budget_command(_PROJECT))
    stack.bus.publish(init_pipeline_command(_PROJECT))
    stack.driver.run_until_quiescent()

    ops = initiated_ops(stack)
    assert len(ops) == 2
    dec = ProviderOperationDecider()
    for op, _ in ops:
        # 停在 CLAIMED,零计费(submit 在建单前失败)
        state = dec.initial_state()
        stream = identity.provider_op_stream(op)
        for e in sorted(
            (x for x in stack.db.state.events if x.stream_id == stream),
            key=lambda x: x.global_position,
        ):
            state = dec.evolve(state, e.payload)
        assert state.status is ProviderOpStatus.CLAIMED
        assert provider.charge_count(op) == 0

    # 越过 recycle_after 的对账 tick:CLAIMED 不得被 abort/release
    stack.bus.publish(tick_command(_TS + timedelta(hours=1), 1))
    stack.driver.run_until_quiescent()
    assert _payloads(stack, ProviderOperationAbortedEvt) == []
    assert _payloads(stack, BudgetReleasedEvt) == []


# --------------------------------------------------------------------------- #
# 6. activity 命令稳定身份
# --------------------------------------------------------------------------- #


def test_activity_command_id_is_stable_and_evidence_scoped() -> None:
    a = activity_command_id("w", "op1", "claim", "evt1")
    b = activity_command_id("w", "op1", "claim", "evt1")
    assert a == b  # 同 (worker, op, action, evidence) -> 稳定
    assert a != activity_command_id("w", "op1", "claim", "evt2")  # evidence 不同
    assert a != activity_command_id("w", "op1", "submitted", "evt1")  # action 不同
    assert a != activity_command_id("w2", "op1", "claim", "evt1")  # worker 不同


# --------------------------------------------------------------------------- #
# 7. poll 轮转公平:多个 SUBMITTED-Pending op 各被 poll 一次,不饿死
# --------------------------------------------------------------------------- #


def test_poll_fairness_rotates_across_ops() -> None:
    provider = FakeProvider(pending_first_poll_all=True)
    h = _OpHarness(provider)
    ops: list[str] = []
    for shot in ("shot_01", "shot_02", "shot_03"):
        ops.append(h.initiate(_make_spec(f"att-{shot}", shot)))

    # 交替驱动到全部 SUBMITTED,并让每个 op 被 poll 一次(首个 poll 返回 Pending)
    for _ in range(60):
        progressed = h.activity.tick()
        h.drain_worker()
        if not progressed:
            break

    # 每个 op 恰好 submit 一次、poll 一次(cursor 轮转,无饥饿/无重复)
    for op in ops:
        assert h.op_status(op) is ProviderOpStatus.SUBMITTED
        assert provider.charge_count(op) == 1
        assert provider.poll_count_for(op) == 1
