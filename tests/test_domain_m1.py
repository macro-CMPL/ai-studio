"""Milestone 1 验收:领域对象契约、不变式、深度不可变与确定性。

含攻击性反例:验证不变式被代码真正封住,而非可被伪造。
"""

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
    OperationParam,
    PlannedOperation,
    ScriptPayload,
)
from studio.domain.budget import LedgerEntry
from studio.domain.enums import (
    AcceptanceStatus,
    ArtifactType,
    CardinalityKind,
    ControlRole,
    CostMode,
    CurrencyStatus,
    DependencyStatus,
    ExecutorKind,
    LedgerEntryType,
    LedgerSubjectType,
    PartitioningKind,
    ProviderOpStatus,
    ToolEffectLevel,
)
from studio.domain.stages import (
    Cardinality,
    OutputSpec,
    Partitioning,
    Requirement,
    StageSpec,
)
from studio.domain.tasks import ProviderOperation
from studio.serialization import digest

_TS = datetime(2026, 1, 1, tzinfo=UTC)
_NAIVE = datetime(2026, 1, 1)  # noqa: DTZ001 - 故意构造 naive 用于反例

_PAYLOAD_ADAPTER: TypeAdapter[ArtifactPayload] = TypeAdapter(ArtifactPayload)


def _image_payload(shot: str = "shot_02") -> ImagePayload:
    return ImagePayload(shot_id=shot, prompt="p", blob_ref="blob://x")


def _valid_artifact_kwargs() -> dict[str, object]:
    series = ids.series_id("proj_1", "storyboard_frame", "shot_02")
    payload = _image_payload()
    rev = 1
    return {
        "artifact_id": ids.artifact_id(series, rev),
        "series_id": series,
        "revision": rev,
        "type": ArtifactType.IMAGE,
        "logical_slot": "storyboard_frame",
        "partition_key": "shot_02",
        "digest": digest(payload),
        "produced_by_attempt": "att-1",
        "supersedes_id": None,
        "acceptance": AcceptanceStatus.PROPOSED,
        "currency": CurrencyStatus.CURRENT,
        "dependency": DependencyStatus.FRESH,
        "created_at": _TS,
        "payload": payload,
    }


# --------------------------------------------------------------------------- #
# 契约与判别联合
# --------------------------------------------------------------------------- #


def test_payload_discriminated_union_round_trips() -> None:
    payload = ScriptPayload(title="T", logline="L", beats=("a", "b"))
    raw = _PAYLOAD_ADAPTER.dump_python(payload, mode="json")
    assert raw["kind"] == ArtifactType.SCRIPT.value
    back = _PAYLOAD_ADAPTER.validate_python(raw)
    assert isinstance(back, ScriptPayload)
    assert back == payload


def test_artifact_factory_produces_valid_and_ref() -> None:
    series = ids.series_id("proj_1", "storyboard_frame", "shot_02")
    art = Artifact.create(
        series_id=series,
        revision=1,
        logical_slot="storyboard_frame",
        partition_key="shot_02",
        payload=_image_payload(),
        produced_by_attempt="att-1",
        created_at=_TS,
    )
    ref = art.ref()
    assert isinstance(ref, ArtifactRef)
    assert ref.artifact_id == art.artifact_id
    assert art.type is ArtifactType.IMAGE


# --------------------------------------------------------------------------- #
# 攻击性反例:Artifact 不变式不可伪造
# --------------------------------------------------------------------------- #


def test_artifact_rejects_type_payload_mismatch() -> None:
    kwargs = _valid_artifact_kwargs()
    kwargs["type"] = ArtifactType.SCRIPT  # 与 image payload 不符
    with pytest.raises(ValidationError):
        Artifact(**kwargs)


def test_artifact_rejects_wrong_digest() -> None:
    kwargs = _valid_artifact_kwargs()
    kwargs["digest"] = "wrong"
    with pytest.raises(ValidationError):
        Artifact(**kwargs)


def test_artifact_rejects_wrong_deterministic_id() -> None:
    kwargs = _valid_artifact_kwargs()
    kwargs["artifact_id"] = "forged"
    with pytest.raises(ValidationError):
        Artifact(**kwargs)


def test_artifact_requires_positive_revision() -> None:
    kwargs = _valid_artifact_kwargs()
    kwargs["revision"] = -1
    with pytest.raises(ValidationError):
        Artifact(**kwargs)


def test_artifact_requires_aware_datetime() -> None:
    kwargs = _valid_artifact_kwargs()
    kwargs["created_at"] = _NAIVE
    with pytest.raises(ValidationError):
        Artifact(**kwargs)


def test_revision_requires_supersedes_id() -> None:
    series = ids.series_id("proj_1", "storyboard_frame", "shot_02")
    payload = _image_payload()
    kwargs = _valid_artifact_kwargs()
    kwargs.update(
        revision=2,
        artifact_id=ids.artifact_id(series, 2),
        supersedes_id=None,
        digest=digest(payload),
    )
    with pytest.raises(ValidationError):
        Artifact(**kwargs)


def test_revision_one_rejects_supersedes_id() -> None:
    kwargs = _valid_artifact_kwargs()
    kwargs["supersedes_id"] = "anything"  # revision==1 不得有
    with pytest.raises(ValidationError):
        Artifact(**kwargs)


def test_revision_requires_exact_previous_artifact() -> None:
    series = ids.series_id("proj_1", "storyboard_frame", "shot_02")
    payload = _image_payload()
    kwargs = _valid_artifact_kwargs()
    kwargs.update(
        series_id=series,
        revision=2,
        artifact_id=ids.artifact_id(series, 2),
        digest=digest(payload),
        payload=payload,
    )
    # 指向错误的上一版本 -> 拒绝
    bad = {**kwargs, "supersedes_id": "wrong"}
    with pytest.raises(ValidationError):
        Artifact(**bad)
    # 精确指向 revision 1 -> 接受
    good = {**kwargs, "supersedes_id": ids.artifact_id(series, 1)}
    art = Artifact(**good)
    assert art.supersedes_id == ids.artifact_id(series, 1)


# --------------------------------------------------------------------------- #
# 攻击性反例:ArtifactRef 身份/摘要不可伪造
# --------------------------------------------------------------------------- #


def test_artifact_ref_rejects_wrong_deterministic_id() -> None:
    series = ids.series_id("proj_1", "storyboard_frame", "shot_02")
    with pytest.raises(ValidationError):
        ArtifactRef(
            artifact_id="forged",
            series_id=series,
            revision=1,
            digest="a" * 64,
        )


def test_artifact_ref_rejects_invalid_digest() -> None:
    series = ids.series_id("proj_1", "storyboard_frame", "shot_02")
    with pytest.raises(ValidationError):
        ArtifactRef(
            artifact_id=ids.artifact_id(series, 1),
            series_id=series,
            revision=1,
            digest="not-a-sha256",
        )


def test_series_id_distinguishes_none_and_empty_partition() -> None:
    singleton = ids.series_id("p", "slot", None)
    empty = ids.series_id("p", "slot", "")
    assert singleton != empty


# --------------------------------------------------------------------------- #
# 深度不可变
# --------------------------------------------------------------------------- #


def test_planned_operation_is_deeply_immutable() -> None:
    op = PlannedOperation(
        logical_operation_key="k",
        op_type="gen",
        params=(OperationParam(key="prompt", value="a"),),
    )
    # 元素是 frozen model:不能改
    with pytest.raises(ValidationError):
        op.params[0].value = "mutated"  # type: ignore[misc]
    # 容器是 tuple:不能按下标赋值
    with pytest.raises(TypeError):
        op.params[0] = OperationParam(key="x", value="y")  # type: ignore[index]


def test_planned_operation_rejects_duplicate_param_keys() -> None:
    with pytest.raises(ValidationError):
        PlannedOperation(
            logical_operation_key="k",
            op_type="gen",
            params=(
                OperationParam(key="p", value="1"),
                OperationParam(key="p", value="2"),
            ),
        )


# --------------------------------------------------------------------------- #
# 确定性 ID / 幂等键
# --------------------------------------------------------------------------- #


def test_operation_id_stable_and_attempt_scoped() -> None:
    key = "shot_02:image:variant_0"
    assert ids.operation_id("att-1", key) == ids.operation_id("att-1", key)
    assert ids.operation_id("att-1", key) != ids.operation_id("att-2", key)


def test_uuid_encoding_is_unambiguous() -> None:
    # 分隔符注入不得导致碰撞
    assert ids._v5("a\x1fb", "c") != ids._v5("a", "b\x1fc")


def test_series_id_is_project_scoped() -> None:
    a = ids.series_id("proj_a", "storyboard_frame", "shot_02")
    b = ids.series_id("proj_b", "storyboard_frame", "shot_02")
    assert a != b


def test_digest_is_deterministic() -> None:
    p1 = PlannedOperation(
        logical_operation_key="k",
        op_type="gen",
        params=(OperationParam(key="a", value="1"),),
    )
    p2 = PlannedOperation(
        logical_operation_key="k",
        op_type="gen",
        params=(OperationParam(key="a", value="1"),),
    )
    assert digest(p1) == digest(p2)


# --------------------------------------------------------------------------- #
# Stage:工具副作用与执行成本分离
# --------------------------------------------------------------------------- #


def test_agent_stage_forbids_costed_tool_effects() -> None:
    with pytest.raises(ValidationError):
        StageSpec(
            stage_id="director",
            executor_kind=ExecutorKind.AGENT,
            control_role=ControlRole.PRODUCER,
            allowed_tool_effects=frozenset({ToolEffectLevel.COSTED}),
            cost_mode=CostMode.METERED,
            requires=(),
            produces=(),
        )


def test_agent_stage_allows_metered_cost_with_pure_tools() -> None:
    # 真实 Agent:自身计费(METERED),但只调 PURE/READ_ONLY 工具 —— 合法。
    spec = StageSpec(
        stage_id="writer",
        executor_kind=ExecutorKind.AGENT,
        control_role=ControlRole.PRODUCER,
        allowed_tool_effects=frozenset(
            {ToolEffectLevel.PURE, ToolEffectLevel.READ_ONLY}
        ),
        cost_mode=CostMode.METERED,
        requires=(),
        produces=(),
    )
    assert spec.cost_mode is CostMode.METERED


def test_provider_stage_allows_costed_tools() -> None:
    spec = StageSpec(
        stage_id="image",
        executor_kind=ExecutorKind.PROVIDER,
        control_role=ControlRole.PRODUCER,
        allowed_tool_effects=frozenset({ToolEffectLevel.COSTED}),
        cost_mode=CostMode.METERED,
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
    assert spec.requires[0].acceptance_filter is AcceptanceStatus.ACCEPTED


def test_partitioning_validators() -> None:
    with pytest.raises(ValidationError):
        Partitioning(kind=PartitioningKind.DYNAMIC_FROM)
    with pytest.raises(ValidationError):
        Partitioning(kind=PartitioningKind.SINGLETON, from_key="x")


def test_cardinality_validators() -> None:
    with pytest.raises(ValidationError):
        Cardinality(kind=CardinalityKind.DYNAMIC_PARTITION_BY)
    with pytest.raises(ValidationError):
        Cardinality(kind=CardinalityKind.STATIC, partition_by="x")


# --------------------------------------------------------------------------- #
# ProviderOperation / 预算约束
# --------------------------------------------------------------------------- #


def test_provider_operation_requires_job_id_when_submitted() -> None:
    with pytest.raises(ValidationError):
        ProviderOperation.create(
            attempt_id="att-1",
            logical_operation_key="k",
            status=ProviderOpStatus.SUBMITTED,
            job_id=None,
            cost_estimate=Decimal("10"),
        )


def test_provider_operation_initiated_forbids_job_id() -> None:
    with pytest.raises(ValidationError):
        ProviderOperation.create(
            attempt_id="att-1",
            logical_operation_key="k",
            status=ProviderOpStatus.INITIATED,
            job_id="job-1",
            cost_estimate=Decimal("10"),
        )


def test_provider_operation_rejects_negative_cost() -> None:
    with pytest.raises(ValidationError):
        ProviderOperation.create(
            attempt_id="att-1",
            logical_operation_key="k",
            status=ProviderOpStatus.INITIATED,
            cost_estimate=Decimal("-1"),
        )


def test_provider_operation_rejects_wrong_deterministic_id() -> None:
    with pytest.raises(ValidationError):
        ProviderOperation(
            operation_id="forged",
            attempt_id="att-1",
            logical_operation_key="k",
            status=ProviderOpStatus.INITIATED,
            job_id=None,
            cost_estimate=Decimal("10"),
            cost_actual=None,
        )


def test_ledger_entry_valid() -> None:
    entry = LedgerEntry(
        entry_id="e1",
        budget_id="b1",
        entry_type=LedgerEntryType.CAPTURE,
        amount=Decimal("20"),
        currency="CNY",
        subject_type=LedgerSubjectType.TASK_ATTEMPT,
        subject_id="att-stitch-1",
        reservation_id="r1",
        created_at=_TS,
    )
    assert entry.amount == Decimal("20")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", Decimal("-1")),
        ("currency", ""),
        ("subject_id", ""),
        ("created_at", _NAIVE),
    ],
)
def test_ledger_entry_rejects_bad_fields(field: str, value: object) -> None:
    base = {
        "entry_id": "e1",
        "budget_id": "b1",
        "entry_type": LedgerEntryType.CAPTURE,
        "amount": Decimal("20"),
        "currency": "CNY",
        "subject_type": LedgerSubjectType.TASK_ATTEMPT,
        "subject_id": "att-1",
        "reservation_id": None,
        "created_at": _TS,
    }
    base[field] = value
    with pytest.raises(ValidationError):
        LedgerEntry(**base)
