"""M5 完整栈装配:M3 六件套 + M4 四 PM + M5 五 PM + Gate Decider + ActivityWorker。

用于黄金场景端到端:立项 -> 剧本 -> 开发包 -> 资产(此处以 storyboard/plan 抽象)->
分镜 -> 提示词质检 -> 出图 -> 结果质检 -> 阶段质检 -> 交付。

质检/交付为确定性 TRANSFORM 评价器/转换(注入到 AttemptDecider);image 仍走 M4 付费
FakeProvider 异步链。stage_qc 评价器在 shot_02 图像修订版 < pass_at_revision 时判返工,
用以驱动"第一轮阶段质检发现 shot_02 不一致 -> 仅重做 shot_02 -> 第二轮通过"。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fake_provider import FakeProvider
from kernel_helpers import FakeClock
from production_helpers import _plan_exec, _storyboard_exec
from studio.application.driver import Driver, SupportsPumpTick
from studio.application.event_pump import EventPump
from studio.application.outbox_relay import OutboxRelay
from studio.application.routing_worker import RoutingCommandWorker
from studio.domain.artifacts import (
    ArtifactPayload,
    DeliveryPayload,
    PlannedOperation,
    QCFinding,
    QCReportPayload,
)
from studio.domain.enums import Severity
from studio.infrastructure.memory._state import MemoryDatabase
from studio.infrastructure.memory.unit_of_work import (
    MemoryCommandBus,
    MemoryUnitOfWorkFactory,
)
from studio.kernel.envelopes import CommandEnvelope
from studio.production import identity
from studio.production.attempt import TaskAttemptDecider
from studio.production.budget import BudgetDecider, InitializeBudgetCmd
from studio.production.delivery_pm import DeliveryProcessManager
from studio.production.dispatch import canonical_target
from studio.production.gate import GateDecider
from studio.production.gate_pm import GateDecisionProcessManager
from studio.production.payloads import InitializePipelineCmd
from studio.production.pipeline import golden_m5_compiled, golden_selectors
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
from studio.production.provider_op import ProviderOperationDecider
from studio.production.provider_port import ProviderRegistry
from studio.production.qc_scheduling_pm import QCEvaluationSchedulingProcessManager
from studio.production.quality import golden_m5_quality_config
from studio.production.reconcile import (
    OrphanReconciliationProcessManager,
    ReconcilePolicy,
    ReconciliationClockDecider,
)
from studio.production.result_mapper import default_result_mappers
from studio.production.result_pm import ProviderResultProcessManager
from studio.production.rework_pm import ReworkProcessManager
from studio.production.scheduling_pm import ProviderSchedulingProcessManager
from studio.production.series import ArtifactSeriesDecider
from studio.production.stage_confluence_pm import StageConfluenceProcessManager
from studio.production.values import BindingItem
from studio.production.webhook_ingress import ProviderWebhookIngress

_TS = datetime(2026, 1, 1, tzinfo=UTC)
_UNIT_COST = Decimal("10")
_CURRENCY = "CNY"
_PROVIDER_ID = "fake-image"
_PROVIDER_VERSION = "1"
_BINDINGS = {
    "image": ProviderBinding(
        provider_id=_PROVIDER_ID, provider_version=_PROVIDER_VERSION, pricing_version="1"
    )
}

Executor = Callable[[str, tuple[BindingItem, ...], str | None], ArtifactPayload]


def _quote(pid: str, pver: str, op: PlannedOperation, price_ver: str) -> QuoteResult:
    return QuoteResult(estimated_cost=_UNIT_COST, currency=_CURRENCY)


# --- 确定性质检评价器 / 交付转换 ------------------------------------------ #


def _pass_report(
    refs: tuple[BindingItem, ...], partition: str | None, evaluator: str
) -> QCReportPayload:
    return QCReportPayload(
        subject_refs=tuple(b.to_ref() for b in refs),
        target_partition=partition,
        evaluator=evaluator, evaluator_version="1", criteria_version="1",
        passed=True, findings=(), rework_scope=(), feedback="通过",
    )


def _prompt_qc_exec(
    stage: str, refs: tuple[BindingItem, ...], partition: str | None
) -> ArtifactPayload:
    return _pass_report(refs, partition, "提示词质检评价器")


def _result_qc_exec(
    stage: str, refs: tuple[BindingItem, ...], partition: str | None
) -> ArtifactPayload:
    return _pass_report(refs, partition, "结果质检评价器")


def _make_stage_qc_exec(pass_at_revision: int) -> Executor:
    """阶段质检评价器:shot_02 图像修订版 < pass_at_revision 时判返工,否则通过。"""

    def _stage_qc_exec(
        stage: str, refs: tuple[BindingItem, ...], partition: str | None
    ) -> ArtifactPayload:
        subject_refs = tuple(sorted((b.to_ref() for b in refs), key=lambda r: r.artifact_id))
        shot_02 = next((b for b in refs if b.partition_key == "shot_02"), None)
        if shot_02 is not None and shot_02.revision < pass_at_revision:
            return QCReportPayload(
                subject_refs=subject_refs, target_partition=None,
                evaluator="阶段质检评价器", evaluator_version="1", criteria_version="1",
                passed=False,
                findings=(
                    QCFinding(
                        rule_id="cross_shot_consistency", severity=Severity.ERROR,
                        description="shot_02 与其余镜头跨镜头风格不一致",
                        suggested_action="以相同提示词重做 shot_02",
                        target_partition="shot_02",
                    ),
                ),
                rework_scope=("shot_02",), feedback="shot_02 跨镜头不一致",
            )
        return QCReportPayload(
            subject_refs=subject_refs, target_partition=None,
            evaluator="阶段质检评价器", evaluator_version="1", criteria_version="1",
            passed=True, findings=(), rework_scope=(), feedback="整体一致",
        )

    return _stage_qc_exec


def _delivery_exec(
    stage: str, refs: tuple[BindingItem, ...], partition: str | None
) -> ArtifactPayload:
    ordered = sorted(refs, key=lambda b: b.partition_key or "")
    source = ordered[0].to_ref()
    return DeliveryPayload(source_ref=source, delivery_uri="delivery://final")


@dataclass
class M5Stack:
    db: MemoryDatabase
    bus: MemoryCommandBus
    clock: FakeClock
    uow_factory: MemoryUnitOfWorkFactory
    driver: Driver
    provider: FakeProvider
    webhook: ProviderWebhookIngress


def build_m5_stack(*, stage_pass_at_revision: int = 2, image_rework_limit: int = 2) -> M5Stack:
    prov = FakeProvider()
    db = MemoryDatabase()
    bus = MemoryCommandBus()
    clock = FakeClock()
    factory = MemoryUnitOfWorkFactory(db)
    spec = golden_m5_compiled()
    config = golden_m5_quality_config(image_rework_limit=image_rework_limit)

    executors: dict[str, Executor] = {
        "storyboard": _storyboard_exec,
        "plan": _plan_exec,
        # image:PROVIDER,无同步 executor
        "prompt_qc": _prompt_qc_exec,
        "result_qc": _result_qc_exec,
        "stage_qc": _make_stage_qc_exec(stage_pass_at_revision),
        "delivery": _delivery_exec,
    }

    deciders = {
        "project": ProjectDecider(),
        "attempt": TaskAttemptDecider(spec, executors),
        "artifact-series": ArtifactSeriesDecider(),
        "budget": BudgetDecider(),
        "provider-op": ProviderOperationDecider(),
        "reconciliation": ReconciliationClockDecider(),
        "gate": GateDecider(),
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

    reconcile = OrphanReconciliationProcessManager(
        ReconcilePolicy(version="1", recycle_after=timedelta(minutes=10))
    )
    pumps: list[SupportsPumpTick] = [
        pump(PublishProcessManager(config.gated_output_keys)),
        pump(ExpansionProcessManager(spec, golden_selectors())),
        pump(LineageProcessManager()),
        pump(RecomputeProcessManager()),
        pump(ExecutionPlanningProcessManager(spec, _BINDINGS, _quote)),
        pump(ProviderSchedulingProcessManager()),
        pump(ProviderResultProcessManager(spec, default_result_mappers())),
        pump(reconcile),
        pump(QCEvaluationSchedulingProcessManager(config)),
        pump(GateDecisionProcessManager(config)),
        pump(ReworkProcessManager(config)),
        pump(
            StageConfluenceProcessManager(
                expected_from_stage="plan", subject_output_key="image",
                subject_requirement_key="image:image", stage_qc_stage_id="stage_qc",
            )
        ),
        pump(
            DeliveryProcessManager(
                subject_output_key="image", subject_requirement_key="image:image",
                stage_qc_stage_id="stage_qc", delivery_stage_id="delivery",
            )
        ),
    ]
    relay = OutboxRelay(uow_factory=factory, bus=bus)
    registry = ProviderRegistry({(_PROVIDER_ID, _PROVIDER_VERSION): prov})
    from studio.production.activity_worker import ProviderActivityWorker

    activity = ProviderActivityWorker(
        registry=registry, bus=bus, uow_factory=factory, clock=clock
    )
    driver = Driver(worker=worker, pumps=pumps, relay=relay, activity=[activity])
    ingress = ProviderWebhookIngress(bus=bus, uow_factory=factory, clock=clock)
    return M5Stack(
        db=db, bus=bus, clock=clock, uow_factory=factory, driver=driver,
        provider=prov, webhook=ingress,
    )


def init_budget_command(
    project_id: str, total: Decimal = Decimal("1000")
) -> CommandEnvelope[object]:
    return CommandEnvelope(
        command_id=f"init-budget-{project_id}", schema_version=1,
        target=identity.budget_stream(project_id), command_key="init-budget",
        correlation_id="golden", causation_id=None, issued_at=_TS,
        payload=InitializeBudgetCmd(project_id=project_id, total=total, currency=_CURRENCY),  # type: ignore[arg-type]
    )


def init_pipeline_command(project_id: str) -> CommandEnvelope[object]:
    return CommandEnvelope(
        command_id=f"init-{project_id}", schema_version=1,
        target=identity.project_stream(project_id), command_key="init",
        correlation_id="golden", causation_id=None, issued_at=_TS,
        payload=InitializePipelineCmd(
            project_id=project_id, pipeline_spec_id=golden_m5_compiled().spec_id
        ),  # type: ignore[arg-type]
    )
