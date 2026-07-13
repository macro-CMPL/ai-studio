"""M3 测试装配:三 Decider + 四 PM + 路由 Worker + 四 Pump + Relay + Driver。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from kernel_helpers import FakeClock
from studio.application.driver import Driver, SupportsPumpTick
from studio.application.event_pump import EventPump
from studio.application.outbox_relay import OutboxRelay
from studio.application.routing_worker import RoutingCommandWorker
from studio.domain.artifacts import (
    ArtifactPayload,
    ImagePayload,
    ImagePlanPayload,
    OperationParam,
    PlannedOperation,
    ShotSpec,
    StoryboardPayload,
)
from studio.domain.enums import AcceptanceMode
from studio.infrastructure.memory._state import MemoryDatabase
from studio.infrastructure.memory.unit_of_work import (
    MemoryCommandBus,
    MemoryUnitOfWorkFactory,
)
from studio.kernel.envelopes import CommandEnvelope
from studio.production import identity
from studio.production.attempt import TaskAttemptDecider
from studio.production.payloads import (
    InitializePipelineCmd,
    ProductionCommand,
    ProposeArtifactVersionCmd,
)
from studio.production.process_managers import (
    ExpansionProcessManager,
    LineageProcessManager,
    PublishProcessManager,
    RecomputeProcessManager,
)
from studio.production.project import ProjectDecider
from studio.production.series import ArtifactSeriesDecider
from studio.production.values import BindingItem
from studio.serialization import digest

_ISSUED_AT = datetime(2026, 1, 1, tzinfo=UTC)


def _storyboard_exec(
    stage: str, refs: tuple[BindingItem, ...], partition: str | None
) -> ArtifactPayload:
    return StoryboardPayload(
        shots=(
            ShotSpec(shot_id="shot_01", description="opening"),
            ShotSpec(shot_id="shot_02", description="climax"),
        )
    )


def _plan_exec(
    stage: str, refs: tuple[BindingItem, ...], partition: str | None
) -> ArtifactPayload:
    return ImagePlanPayload(
        operations=(
            PlannedOperation(
                logical_operation_key=f"{partition}:image:v0",
                op_type="gen",
                params=(OperationParam(key="shot", value=partition or ""),),
            ),
        )
    )


def _image_exec(
    stage: str, refs: tuple[BindingItem, ...], partition: str | None
) -> ArtifactPayload:
    return ImagePayload(
        shot_id=partition or "",
        prompt=f"prompt-{partition}",
        blob_ref=f"blob://{partition}",
    )


@dataclass
class ProductionStack:
    db: MemoryDatabase
    bus: MemoryCommandBus
    clock: FakeClock
    uow_factory: MemoryUnitOfWorkFactory
    driver: Driver


def build_production_stack() -> ProductionStack:
    db = MemoryDatabase()
    bus = MemoryCommandBus()
    clock = FakeClock()
    factory = MemoryUnitOfWorkFactory(db)

    deciders = {
        "project": ProjectDecider(),
        "attempt": TaskAttemptDecider(
            {
                "storyboard": _storyboard_exec,
                "plan": _plan_exec,
                "image": _image_exec,
            }
        ),
        "artifact-series": ArtifactSeriesDecider(),
    }
    worker = RoutingCommandWorker(
        deciders=deciders,
        resolve_kind=identity.stream_kind,
        bus=bus,
        uow_factory=factory,
        clock=clock,
    )
    pumps: list[SupportsPumpTick] = [
        EventPump(process_manager=PublishProcessManager(), uow_factory=factory, clock=clock),
        EventPump(process_manager=ExpansionProcessManager(), uow_factory=factory, clock=clock),
        EventPump(process_manager=LineageProcessManager(), uow_factory=factory, clock=clock),
        EventPump(process_manager=RecomputeProcessManager(), uow_factory=factory, clock=clock),
    ]
    relay = OutboxRelay(uow_factory=factory, bus=bus)
    driver = Driver(worker=worker, pumps=pumps, relay=relay)
    return ProductionStack(
        db=db, bus=bus, clock=clock, uow_factory=factory, driver=driver
    )


def init_command(
    project_id: str, *, command_id: str = "cmd-init"
) -> CommandEnvelope[ProductionCommand]:
    return CommandEnvelope(
        command_id=command_id,
        schema_version=1,
        target=identity.project_stream(project_id),
        command_key="init",
        correlation_id=project_id,
        causation_id=None,
        issued_at=_ISSUED_AT,
        payload=InitializePipelineCmd(
            project_id=project_id, pipeline_spec_id="spec-v1"
        ),
    )


def supersede_plan_command(
    project_id: str,
    partition: str,
    *,
    command_id: str,
    candidate_id: str,
) -> CommandEnvelope[ProductionCommand]:
    """桩:直接为 plan[partition] 提出新版本(v2),触发 supersession。"""
    from studio.domain import ids as domain_ids

    series = domain_ids.series_id(project_id, "plan", partition)
    payload = ImagePlanPayload(
        operations=(
            PlannedOperation(
                logical_operation_key=f"{partition}:image:v1",
                op_type="gen",
                params=(OperationParam(key="rev", value="2"),),
            ),
        )
    )
    return CommandEnvelope(
        command_id=command_id,
        schema_version=1,
        target=identity.series_stream(series),
        command_key=candidate_id,
        correlation_id=project_id,
        causation_id=None,
        issued_at=_ISSUED_AT,
        payload=ProposeArtifactVersionCmd(
            series_id=series,
            candidate_id=candidate_id,
            output_key="plan",
            partition_key=partition,
            digest=digest(payload),
            payload=payload,
            acceptance_mode=AcceptanceMode.AUTO,
            produced_by_attempt="manual-stub",
        ),
    )
