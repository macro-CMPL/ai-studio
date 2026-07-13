"""Milestone 1 验收:领域对象契约、不变式与确定性。"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import TypeAdapter, ValidationError

from studio.domain import ids
from studio.domain.artifacts import (
    Artifact,
    ArtifactPayload,
    ArtifactRef,
    ImagePayload,
    PlannedOperation,
    ScriptPayload,
)
from studio.domain.budget import LedgerEntry
from studio.domain.enums import (
    AcceptanceStatus,
    ArtifactType,
    CardinalityKind,
    ControlRole,
    CurrencyStatus,
    DependencyStatus,
    ExecutorKind,
    LedgerEntryType,
    LedgerSubjectType,
    PartitioningKind,
    SideEffectLevel,
)
from studio.domain.stages import (
    Cardinality,
    OutputSpec,
    Partitioning,
    Requirement,
    StageSpec,
)
from studio.serialization import digest

_TS = datetime(2026, 1, 1, tzinfo=UTC)

_PAYLOAD_ADAPTER: TypeAdapter[ArtifactPayload] = TypeAdapter(ArtifactPayload)


def _image_artifact(series: str, revision: int, partition: str) -> Artifact:
    payload = ImagePayload(shot_id=partition, prompt="p", blob_ref="blob://x")
    return Artifact(
        artifact_id=ids.artifact_id(series, revision),
        series_id=series,
        revision=revision,
        type=ArtifactType.IMAGE,
        logical_slot="storyboard_frame",
        partition_key=partition,
        digest=digest(payload),
        produced_by_attempt="att-1",
        supersedes_id=None,
        acceptance=AcceptanceStatus.PROPOSED,
        currency=CurrencyStatus.CURRENT,
        dependency=DependencyStatus.FRESH,
        created_at=_TS,
        payload=payload,
    )


def test_payload_discriminated_union_round_trips() -> None:
    payload = ScriptPayload(title="T", logline="L", beats=("a", "b"))
    raw = _PAYLOAD_ADAPTER.dump_python(payload, mode="json")
    assert raw["kind"] == ArtifactType.SCRIPT.value
    back = _PAYLOAD_ADAPTER.validate_python(raw)
    assert isinstance(back, ScriptPayload)
    assert back == payload


def test_artifact_ref_derived_from_artifact() -> None:
    art = _image_artifact("shot_02.image", 1, "shot_02")
    ref = art.ref()
    assert isinstance(ref, ArtifactRef)
    assert ref.artifact_id == art.artifact_id
    assert ref.revision == 1


def test_operation_id_stable_and_attempt_scoped() -> None:
    key = "shot_02:image:variant_0"
    # 技术重试:同 attempt + 同逻辑键 => 同一 operation_id
    assert ids.operation_id("att-1", key) == ids.operation_id("att-1", key)
    # 业务返工:新 attempt => 新 operation_id
    assert ids.operation_id("att-1", key) != ids.operation_id("att-2", key)


def test_agent_stage_forbids_costed_side_effects() -> None:
    with pytest.raises(ValidationError):
        StageSpec(
            stage_id="director",
            executor_kind=ExecutorKind.AGENT,
            control_role=ControlRole.PRODUCER,
            side_effect_level=SideEffectLevel.COSTED,
            requires=(),
            produces=(),
        )


def test_provider_stage_allows_costed_side_effects() -> None:
    spec = StageSpec(
        stage_id="image",
        executor_kind=ExecutorKind.PROVIDER,
        control_role=ControlRole.PRODUCER,
        side_effect_level=SideEffectLevel.COSTED,
        requires=(
            Requirement(
                artifact_type=ArtifactType.IMAGE_PLAN,
                logical_slot="image_plan",
                cardinality=Cardinality(
                    kind=CardinalityKind.DYNAMIC_PARTITION_BY,
                    partition_by="shot_id",
                ),
            ),
        ),
        produces=(
            OutputSpec(
                artifact_type=ArtifactType.IMAGE,
                logical_slot="storyboard_frame",
                schema_version=1,
                partitioning=Partitioning(
                    kind=PartitioningKind.DYNAMIC_FROM, from_key="shot_id"
                ),
            ),
        ),
    )
    assert spec.produces[0].partitioning.from_key == "shot_id"


def test_partitioning_validators() -> None:
    with pytest.raises(ValidationError):
        Partitioning(kind=PartitioningKind.DYNAMIC_FROM)  # 缺 from_key
    with pytest.raises(ValidationError):
        Partitioning(kind=PartitioningKind.SINGLETON, from_key="x")  # 多余 from_key


def test_cardinality_validators() -> None:
    with pytest.raises(ValidationError):
        Cardinality(kind=CardinalityKind.DYNAMIC_PARTITION_BY)  # 缺 partition_by
    with pytest.raises(ValidationError):
        Cardinality(kind=CardinalityKind.STATIC, partition_by="x")  # 多余


def test_digest_is_canonical_and_order_independent() -> None:
    p1 = PlannedOperation(
        logical_operation_key="k", op_type="gen", params={"a": "1", "b": "2"}
    )
    p2 = PlannedOperation(
        logical_operation_key="k", op_type="gen", params={"b": "2", "a": "1"}
    )
    # 键顺序不同,canonical digest 相同
    assert digest(p1) == digest(p2)


def test_ledger_entry_uses_decimal_and_generic_subject() -> None:
    entry = LedgerEntry(
        entry_id="e1",
        budget_id="b1",
        entry_type=LedgerEntryType.CAPTURE,
        amount=Decimal("20"),
        currency="CNY",
        subject_type=LedgerSubjectType.TASK_ATTEMPT,  # TRANSFORM(stitch) 也计费
        subject_id="att-stitch-1",
        reservation_id="r1",
        created_at=_TS,
    )
    assert entry.amount == Decimal("20")
    assert entry.subject_type is LedgerSubjectType.TASK_ATTEMPT
