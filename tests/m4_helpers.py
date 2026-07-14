"""M4 集成装配:M3 六件套 + 四个 M4 PM + budget/provider-op/reconciliation Decider。

image stage 无同步 executor -> 走异步 provider 流水线。ActivityWorker 是第 4 步,
本装配用手工注入 Claim/RecordSubmitted/RecordSucceeded 命令代替它。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from kernel_helpers import FakeClock
from production_helpers import _plan_exec, _storyboard_exec
from studio.application.driver import Driver, SupportsPumpTick
from studio.application.event_pump import EventPump
from studio.application.outbox_relay import OutboxRelay
from studio.application.routing_worker import RoutingCommandWorker
from studio.domain.artifacts import PlannedOperation
from studio.infrastructure.memory._state import MemoryDatabase
from studio.infrastructure.memory.unit_of_work import (
    MemoryCommandBus,
    MemoryUnitOfWorkFactory,
)
from studio.kernel.envelopes import CommandEnvelope
from studio.production import identity
from studio.production.attempt import TaskAttemptDecider
from studio.production.budget import BudgetDecider, InitializeBudgetCmd
from studio.production.dispatch import canonical_target
from studio.production.execution_spec import ProviderExecutionSpec
from studio.production.pipeline import golden_compiled, golden_selectors
from studio.production.planning_pm import (
    ExecutionPlanningProcessManager,
    ProviderBinding,
    QuoteResult,
)
from studio.production.process_managers import (
    ExpansionProcessManager,
    LineageProcessManager,
    PublishProcessManager,
    RecomputeProcessManager,
)
from studio.production.project import ProjectDecider
from studio.production.provider_op import (
    ClaimSubmissionCmd,
    ProviderOperationDecider,
    ProviderResultRef,
    RecordSubmittedCmd,
    RecordSucceededCmd,
)
from studio.production.reconcile import (
    EmitReconciliationTickCmd,
    OrphanReconciliationProcessManager,
    ReconcilePolicy,
    ReconciliationClockDecider,
)
from studio.production.result_mapper import default_result_mappers
from studio.production.result_pm import ProviderResultProcessManager
from studio.production.scheduling_pm import ProviderSchedulingProcessManager
from studio.production.series import ArtifactSeriesDecider

_TS = datetime(2026, 1, 1, tzinfo=UTC)
_UNIT_COST = Decimal("10")
_CURRENCY = "CNY"
_POLICY_VERSION = "1"
_SCOPE = "global"
_RECONCILE = OrphanReconciliationProcessManager(
    ReconcilePolicy(version=_POLICY_VERSION, recycle_after=timedelta(minutes=10))
)
_BINDINGS = {
    "image": ProviderBinding(
        provider_id="fake-image", provider_version="1", pricing_version="1"
    )
}


def _quote(pid: str, pver: str, op: PlannedOperation, price_ver: str) -> QuoteResult:
    return QuoteResult(estimated_cost=_UNIT_COST, currency=_CURRENCY)


@dataclass
class M4Stack:
    db: MemoryDatabase
    bus: MemoryCommandBus
    clock: FakeClock
    uow_factory: MemoryUnitOfWorkFactory
    driver: Driver


def build_m4_stack() -> M4Stack:
    db = MemoryDatabase()
    bus = MemoryCommandBus()
    clock = FakeClock()
    factory = MemoryUnitOfWorkFactory(db)

    deciders = {
        "project": ProjectDecider(),
        # image 无同步 executor -> 异步 provider 流水线
        "attempt": TaskAttemptDecider(
            golden_compiled(),
            {"storyboard": _storyboard_exec, "plan": _plan_exec},
        ),
        "artifact-series": ArtifactSeriesDecider(),
        "budget": BudgetDecider(),
        "provider-op": ProviderOperationDecider(),
        "reconciliation": ReconciliationClockDecider(),
    }
    worker = RoutingCommandWorker(
        deciders=deciders,
        resolve_kind=identity.stream_kind,
        canonical_target=canonical_target,
        bus=bus,
        uow_factory=factory,
        clock=clock,
    )

    def pump(pm: object) -> EventPump[object, object, object]:
        return EventPump(process_manager=pm, uow_factory=factory, clock=clock)  # type: ignore[arg-type]

    pumps: list[SupportsPumpTick] = [
        pump(PublishProcessManager()),
        pump(ExpansionProcessManager(golden_compiled(), golden_selectors())),
        pump(LineageProcessManager()),
        pump(RecomputeProcessManager()),
        pump(ExecutionPlanningProcessManager(golden_compiled(), _BINDINGS, _quote)),
        pump(ProviderSchedulingProcessManager()),
        pump(ProviderResultProcessManager(golden_compiled(), default_result_mappers())),
        pump(_RECONCILE),
    ]
    relay = OutboxRelay(uow_factory=factory, bus=bus)
    driver = Driver(worker=worker, pumps=pumps, relay=relay)
    return M4Stack(db=db, bus=bus, clock=clock, uow_factory=factory, driver=driver)


def _cmd(target: str, key: str, payload: object, cid: str) -> CommandEnvelope[object]:
    return CommandEnvelope(
        command_id=cid, schema_version=1, target=target, command_key=key,
        correlation_id="act", causation_id=None, issued_at=_TS, payload=payload,  # type: ignore[arg-type]
    )


def init_budget_command(
    project_id: str, total: Decimal = Decimal("1000"), currency: str = _CURRENCY
) -> CommandEnvelope[object]:
    return _cmd(
        identity.budget_stream(project_id), "init-budget",
        InitializeBudgetCmd(project_id=project_id, total=total, currency=currency),
        f"init-budget-{project_id}",
    )


def initiated_ops(stack: M4Stack) -> list[tuple[str, ProviderExecutionSpec]]:
    from studio.production.provider_op import ProviderOperationInitiatedEvt

    out: list[tuple[str, ProviderExecutionSpec]] = []
    for env in stack.db.state.events:
        if isinstance(env.payload, ProviderOperationInitiatedEvt):
            out.append((env.payload.operation_id, env.payload.spec))
    return out


def claim_command(op: str) -> CommandEnvelope[object]:
    return _cmd(
        identity.provider_op_stream(op), f"claim:{op}",
        ClaimSubmissionCmd(operation_id=op), f"claim-{op}",
    )


def submit_command(op: str) -> CommandEnvelope[object]:
    return _cmd(
        identity.provider_op_stream(op), f"submit:{op}",
        RecordSubmittedCmd(operation_id=op, job_id=f"job-{op}", provider_event_id=f"sub-{op}"),
        f"submit-{op}",
    )


def succeed_command(op: str, spec: ProviderExecutionSpec) -> CommandEnvelope[object]:
    return _cmd(
        identity.provider_op_stream(op), f"succeed:{op}",
        RecordSucceededCmd(
            operation_id=op,
            result_ref=ProviderResultRef(blob_ref=f"blob://{op}", digest="a" * 64),
            cost_actual=spec.estimated_cost, cost_currency=spec.currency,
            provider_event_id=f"ok-{op}",
        ),
        f"succeed-{op}",
    )


def tick_command(as_of: datetime, seq: int) -> CommandEnvelope[object]:
    return _cmd(
        identity.reconciliation_stream(_SCOPE), f"tick:{seq}",
        EmitReconciliationTickCmd(
            scope=_SCOPE, as_of=as_of, policy_version=_POLICY_VERSION
        ),
        f"tick-{seq}",
    )
