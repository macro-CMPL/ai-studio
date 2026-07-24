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
    ClaimSubmissionCmd,
    InitiateProviderOpCmd,
    ProviderOperationAbortedEvt,
    ProviderOperationDecider,
    ProviderOperationSubmissionUnknownEvt,
    ProviderResultRef,
    RecordSubmittedCmd,
    RecordSucceededCmd,
)
from studio.production.provider_port import (
    ProviderCapabilities,
    ProviderRegistry,
    ProviderRequest,
)
from studio.production.webhook_ingress import ProviderWebhookIngress
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


def _make_spec(
    attempt_id: str, shot: str, *, provider_id: str = "fake", provider_version: str = "1"
) -> ProviderExecutionSpec:
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
        provider_id=provider_id,
        provider_version=provider_version,
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

    def __init__(
        self,
        provider: FakeProvider,
        *,
        registry: ProviderRegistry | None = None,
        clock: object | None = None,
    ) -> None:
        self.db = MemoryDatabase()
        self.bus = MemoryCommandBus()
        self.clock = clock or FakeClock()  # type: ignore[assignment]
        self.factory = MemoryUnitOfWorkFactory(self.db)
        self.provider = provider
        self.registry = registry or ProviderRegistry({("fake", "1"): provider})
        self.worker = RoutingCommandWorker(
            deciders={"provider-op": ProviderOperationDecider()},
            resolve_kind=identity.stream_kind,
            canonical_target=canonical_target,
            bus=self.bus,
            uow_factory=self.factory,
            clock=self.clock,  # type: ignore[arg-type]
        )
        self.activity = ProviderActivityWorker(
            registry=self.registry, bus=self.bus, uow_factory=self.factory,
            clock=self.clock,  # type: ignore[arg-type]
        )

    def new_worker(self) -> ProviderActivityWorker:
        """模拟进程重启:全新 ActivityWorker(空内存簿记)。"""
        return ProviderActivityWorker(
            registry=self.registry, bus=self.bus, uow_factory=self.factory,
            clock=self.clock,  # type: ignore[arg-type]
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


# --------------------------------------------------------------------------- #
# 8. Blocker 1:owner/routing barrier —— 只路由到匹配 (provider_id, version)
# --------------------------------------------------------------------------- #


class _FixedClock:
    """不推进的时钟:用于验证 clamp 后同一时刻不忙循环。"""

    def __init__(self, t: object | None = None) -> None:
        from datetime import UTC, datetime

        self._t = t or datetime(2026, 1, 1, tzinfo=UTC)

    def now(self) -> object:
        return self._t


def test_activity_routes_only_to_matching_provider_version() -> None:
    provider = FakeProvider()
    h = _OpHarness(provider)  # registry 只含 ("fake", "1")
    op1 = h.initiate(_make_spec("att-1", "shot_01", provider_version="1"))
    op2 = h.initiate(_make_spec("att-2", "shot_02", provider_version="2"))

    for _ in range(30):
        if not h.activity.tick():
            break
        h.drain_worker()

    # 匹配 v1:正常推进并恰好一次计费
    assert h.op_status(op1) is not ProviderOpStatus.INITIATED
    assert provider.charge_count(op1) == 1
    # 不匹配 v2:parked 于 INITIATED,零计费(绝不交给错误适配器)
    assert h.op_status(op2) is ProviderOpStatus.INITIATED
    assert provider.charge_count(op2) == 0


# --------------------------------------------------------------------------- #
# 9. 契约锁定:无 lease 时 lookup-only + NotFound 必须 park(不盲目提交)
# --------------------------------------------------------------------------- #


def test_strong_lookup_only_not_found_parks_without_lease() -> None:
    # 仅强 lookup、submit 非幂等、无 lease:NotFound 是时点真相,两 worker 可能同时
    # NotFound 后双提交,故必须 park(不提交),等待人工/lease。
    provider = FakeProvider(
        capabilities=ProviderCapabilities(
            idempotent_submit=False, strong_lookup_by_key=True, webhook=False
        ),
        strict_non_idempotent=True,
    )
    h = _OpHarness(provider)
    op = h.initiate(_make_spec("att-1", "shot_01"))

    assert h.activity.tick() is True  # claim
    h.drain_worker()
    assert h.op_status(op) is ProviderOpStatus.CLAIMED

    # lookup NotFound + 非幂等 + 无 lease -> park:不 submit、零扣费、停在 CLAIMED
    assert h.activity.tick() is False
    h.drain_worker()
    assert h.op_status(op) is ProviderOpStatus.CLAIMED
    assert provider.charge_count(op) == 0

    # 再 tick 也不再重复 lookup/submit(parked,仍零扣费)
    assert h.activity.tick() is False
    assert provider.charge_count(op) == 0


def test_lookup_plus_idempotent_submits_after_not_found() -> None:
    # 强 lookup + 幂等 submit:NotFound 后可安全首提(幂等兜底并发双提交)。
    provider = FakeProvider(
        capabilities=ProviderCapabilities(
            idempotent_submit=True, strong_lookup_by_key=True, webhook=False
        )
    )
    h = _OpHarness(provider)
    op = h.initiate(_make_spec("att-1", "shot_01"))

    assert h.activity.tick() is True  # claim
    h.drain_worker()
    assert h.activity.tick() is True  # lookup NotFound -> submit
    h.drain_worker()
    assert h.op_status(op) is ProviderOpStatus.SUBMITTED
    assert provider.charge_count(op) == 1


# --------------------------------------------------------------------------- #
# 10. Blocker 3:activity/webhook 命令继承 root correlation,不用 operation_id 截断
# --------------------------------------------------------------------------- #


def test_activity_preserves_root_correlation() -> None:
    stack = build_activity_stack()
    stack.bus.publish(init_budget_command(_PROJECT))
    stack.bus.publish(init_pipeline_command(_PROJECT))
    stack.driver.run_until_quiescent()

    from studio.production.provider_op import (
        ProviderOperationInitiatedEvt,
        ProviderOperationSubmittedEvt,
        ProviderOperationSucceededEvt,
    )

    events = stack.db.state.events
    initiated = [e for e in events if isinstance(e.payload, ProviderOperationInitiatedEvt)]
    assert len(initiated) == 2
    for init_evt in initiated:
        op = init_evt.payload.operation_id  # type: ignore[attr-defined]
        root = init_evt.correlation_id
        assert root != op  # root 不是 operation_id
        # 后续 activity 驱动的终态事件必须继承 root correlation
        chain = [
            e
            for e in events
            if e.stream_id == identity.provider_op_stream(op)
            and isinstance(
                e.payload, ProviderOperationSubmittedEvt | ProviderOperationSucceededEvt
            )
        ]
        assert len(chain) >= 2
        for e in chain:
            assert e.correlation_id == root  # 不被 activity 截断为 operation_id


# --------------------------------------------------------------------------- #
# 11. Blocker 4:webhook 终态先于 RecordSubmitted 到达,不被永久拒绝
# --------------------------------------------------------------------------- #


def test_webhook_succeeded_before_submitted_is_not_rejected() -> None:
    provider = FakeProvider()
    h = _OpHarness(provider)
    spec = _make_spec("att-1", "shot_01")
    op = h.initiate(spec)
    assert h.activity.tick() is True  # claim
    h.drain_worker()
    assert h.op_status(op) is ProviderOpStatus.CLAIMED

    # 崩溃窗口:submit 已接单(charge=1),RecordSubmitted 未落库
    provider.force_submit(op, _request(spec))
    assert provider.charge_count(op) == 1

    # webhook 成功终态先到:CLAIMED -> SUCCEEDED(不 bad_transition)
    ingress = ProviderWebhookIngress(
        bus=h.bus, uow_factory=h.factory, clock=h.clock  # type: ignore[arg-type]
    )
    ingress.deliver_succeeded(
        operation_id=op, result_ref=provider.result_ref_for(op),
        cost_actual=spec.estimated_cost, cost_currency=spec.currency,
        provider_event_id=f"hook-{op}",
    )
    h.drain_worker()
    assert h.op_status(op) is ProviderOpStatus.SUCCEEDED
    assert provider.charge_count(op) == 1


def test_webhook_failed_before_submitted_is_not_rejected() -> None:
    provider = FakeProvider()
    h = _OpHarness(provider)
    spec = _make_spec("att-1", "shot_01")
    op = h.initiate(spec)
    assert h.activity.tick() is True  # claim
    h.drain_worker()
    assert h.op_status(op) is ProviderOpStatus.CLAIMED

    provider.force_submit(op, _request(spec))

    ingress = ProviderWebhookIngress(
        bus=h.bus, uow_factory=h.factory, clock=h.clock  # type: ignore[arg-type]
    )
    ingress.deliver_failed(
        operation_id=op, charged=True, cost_actual=spec.estimated_cost,
        cost_currency=spec.currency, provider_event_id=f"hookfail-{op}",
    )
    h.drain_worker()
    assert h.op_status(op) is ProviderOpStatus.FAILED


# --------------------------------------------------------------------------- #
# 12. Pending(0) 被 clamp,同一时刻不忙循环
# --------------------------------------------------------------------------- #


def test_zero_retry_after_does_not_spin() -> None:
    provider = FakeProvider(pending_always=True, retry_after=timedelta(0))
    clock = _FixedClock()  # 不推进:同一逻辑时刻反复 tick
    h = _OpHarness(provider, clock=clock)
    op = h.initiate(_make_spec("att-1", "shot_01"))

    assert h.activity.tick() is True  # claim
    h.drain_worker()
    assert h.activity.tick() is True  # submit -> SUBMITTED
    h.drain_worker()
    assert h.op_status(op) is ProviderOpStatus.SUBMITTED

    assert h.activity.tick() is True  # 首次 poll -> Pending(0) 被 clamp
    h.drain_worker()
    # 同一时刻再 tick:next_due 已 clamp 到 >now -> 不再 poll(无忙循环)
    assert h.activity.tick() is False
    assert provider.poll_count_for(op) == 1


# --------------------------------------------------------------------------- #
# 13. 迟到 RecordSubmitted 被终态安全吸收(webhook 抢先 CLAIMED->终态后)
# --------------------------------------------------------------------------- #


def test_late_submitted_after_terminal_is_absorbed() -> None:
    from studio.kernel.decisions import Accepted

    dec = ProviderOperationDecider()
    spec = _make_spec("att-1", "shot_01")
    op = spec.operation_id
    state = dec.initial_state()
    # initiate -> claim(op 进入 CLAIMED)
    for cmd in (
        InitiateProviderOpCmd(operation_id=op, spec=spec),
        ClaimSubmissionCmd(operation_id=op),
    ):
        decision = dec.decide(state, cmd)
        assert isinstance(decision, Accepted)
        for pe in decision.events:
            state = dec.evolve(state, pe.payload)
    assert state.status is ProviderOpStatus.CLAIMED

    # webhook 成功终态自 CLAIMED 直接收敛
    succ = RecordSucceededCmd(
        operation_id=op,
        result_ref=ProviderResultRef(blob_ref="blob", digest="a" * 64),
        cost_actual=spec.estimated_cost, cost_currency=spec.currency,
        provider_event_id="hook-1",
    )
    decision = dec.decide(state, succ)
    assert isinstance(decision, Accepted)
    for pe in decision.events:
        state = dec.evolve(state, pe.payload)
    assert state.status is ProviderOpStatus.SUCCEEDED

    # 迟到的 RecordSubmitted:被终态安全吸收(Accepted 空事件),不 bad_transition、不改状态
    late = RecordSubmittedCmd(operation_id=op, job_id="job-1", provider_event_id="sub-1")
    decision = dec.decide(state, late)
    assert isinstance(decision, Accepted)
    assert decision.events == ()
