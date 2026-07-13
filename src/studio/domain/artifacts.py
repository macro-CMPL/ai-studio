"""产物模型:不可变版本 + 按类型区分的 payload 契约。

设计要点(已锁定):
- 不可变版本模型:artifact_id 唯一标识一个版本;series_id 标识逻辑系列;
  revision 递增;supersedes_id 指向被取代的旧版本。
- Plan / QCReport 不是第二套实体,而是 ArtifactPayload 的一种。
- ArtifactRef 的精确身份是 artifact_id,revision/digest 用于校验与展示。
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from .enums import (
    AcceptanceStatus,
    ArtifactType,
    CurrencyStatus,
    DependencyStatus,
    Severity,
)


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ArtifactRef(_Frozen):
    """对某个不可变产物版本的精确引用。"""

    artifact_id: str
    series_id: str
    revision: int
    digest: str


# --------------------------------------------------------------------------- #
# Payloads:每种类型一个契约,以 kind 作为判别式。
# --------------------------------------------------------------------------- #


class ScriptPayload(_Frozen):
    kind: Literal[ArtifactType.SCRIPT] = ArtifactType.SCRIPT
    title: str
    logline: str
    beats: tuple[str, ...]


class ShotSpec(_Frozen):
    shot_id: str
    description: str


class StoryboardPayload(_Frozen):
    kind: Literal[ArtifactType.STORYBOARD] = ArtifactType.STORYBOARD
    shots: tuple[ShotSpec, ...]


class PlannedOperation(_Frozen):
    """计划内的一项操作。logical_operation_key 是稳定逻辑键。

    例:``shot_02:image:variant_0``。用于派生 operation_id(见 ids.operation_id)。
    """

    logical_operation_key: str
    op_type: str
    params: dict[str, str]


class ImagePlanPayload(_Frozen):
    kind: Literal[ArtifactType.IMAGE_PLAN] = ArtifactType.IMAGE_PLAN
    operations: tuple[PlannedOperation, ...]


class ImagePayload(_Frozen):
    kind: Literal[ArtifactType.IMAGE] = ArtifactType.IMAGE
    shot_id: str
    prompt: str
    blob_ref: str


class StitchPayload(_Frozen):
    kind: Literal[ArtifactType.STITCH] = ArtifactType.STITCH
    source_refs: tuple[ArtifactRef, ...]
    blob_ref: str


class QCFinding(_Frozen):
    target_refs: tuple[ArtifactRef, ...]
    target_partition: str | None
    severity: Severity
    note: str


class QCReportPayload(_Frozen):
    kind: Literal[ArtifactType.QC_REPORT] = ArtifactType.QC_REPORT
    subject_refs: tuple[ArtifactRef, ...]
    evaluator: str
    evaluator_version: str
    criteria: tuple[str, ...]
    findings: tuple[QCFinding, ...]


class DeliveryPayload(_Frozen):
    kind: Literal[ArtifactType.DELIVERY] = ArtifactType.DELIVERY
    source_ref: ArtifactRef
    delivery_uri: str


ArtifactPayload = Annotated[
    ScriptPayload
    | StoryboardPayload
    | ImagePlanPayload
    | ImagePayload
    | StitchPayload
    | QCReportPayload
    | DeliveryPayload,
    Field(discriminator="kind"),
]


# --------------------------------------------------------------------------- #
# Artifact:通用不可变元数据 + payload。
# --------------------------------------------------------------------------- #


class Artifact(_Frozen):
    artifact_id: str
    series_id: str
    revision: int
    type: ArtifactType
    logical_slot: str
    partition_key: str | None
    digest: str
    produced_by_attempt: str | None
    supersedes_id: str | None
    acceptance: AcceptanceStatus
    currency: CurrencyStatus
    dependency: DependencyStatus
    created_at: datetime
    payload: ArtifactPayload

    def ref(self) -> ArtifactRef:
        return ArtifactRef(
            artifact_id=self.artifact_id,
            series_id=self.series_id,
            revision=self.revision,
            digest=self.digest,
        )
