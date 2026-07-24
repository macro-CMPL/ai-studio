"""产物模型:不可变版本 + 按类型区分的 payload 契约。

设计要点(已锁定):
- 不可变版本模型:artifact_id 唯一标识一个版本;series_id 标识逻辑系列;
  revision 递增;supersedes_id 指向被取代的旧版本。
- Plan / QCReport 不是第二套实体,而是 ArtifactPayload 的一种。
- ArtifactRef 的精确身份是 artifact_id,revision/digest 用于校验与展示。

不变式由代码封住(工厂 + model validator 双保险,反序列化也无法绕过):
- artifact_id == ids.artifact_id(series_id, revision)
- type == payload.kind
- digest == digest(payload)
- revision >= 1;revision > 1 时必须有 supersedes_id
- created_at 必须带时区(UtcDatetime)
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import Field, PositiveInt, model_validator

from studio.serialization import digest as compute_digest

from . import ids
from ._base import FrozenModel, Sha256Hex, UtcDatetime
from .enums import ArtifactType, Severity


class ArtifactRef(FrozenModel):
    """对某个不可变产物版本的精确引用。身份与摘要不可伪造。"""

    artifact_id: str
    series_id: str
    revision: PositiveInt
    digest: Sha256Hex

    @model_validator(mode="after")
    def _check_deterministic_id(self) -> ArtifactRef:
        expected = ids.artifact_id(self.series_id, self.revision)
        if self.artifact_id != expected:
            raise ValueError("ArtifactRef.artifact_id 与 (series_id, revision) 不一致")
        return self


# --------------------------------------------------------------------------- #
# Payloads:每种类型一个契约,以 kind 作为判别式。均为深度不可变。
# --------------------------------------------------------------------------- #


class ScriptPayload(FrozenModel):
    kind: Literal[ArtifactType.SCRIPT] = ArtifactType.SCRIPT
    title: str
    logline: str
    beats: tuple[str, ...]


class ShotSpec(FrozenModel):
    shot_id: str
    description: str


class StoryboardPayload(FrozenModel):
    kind: Literal[ArtifactType.STORYBOARD] = ArtifactType.STORYBOARD
    shots: tuple[ShotSpec, ...]


class OperationParam(FrozenModel):
    """深度不可变的键值参数(替代可变 dict)。"""

    key: str
    value: str


class PlannedOperation(FrozenModel):
    """计划内的一项操作。logical_operation_key 是稳定逻辑键。

    例:``shot_02:image:variant_0``。用于派生 operation_id(见 ids.operation_id)。
    params 为不可变元组,键唯一。
    """

    logical_operation_key: str
    op_type: str
    params: tuple[OperationParam, ...]

    @model_validator(mode="after")
    def _unique_param_keys(self) -> PlannedOperation:
        keys = [p.key for p in self.params]
        if len(keys) != len(set(keys)):
            raise ValueError("PlannedOperation.params 的 key 必须唯一")
        return self


class ImagePlanPayload(FrozenModel):
    kind: Literal[ArtifactType.IMAGE_PLAN] = ArtifactType.IMAGE_PLAN
    operations: tuple[PlannedOperation, ...]


class ImagePayload(FrozenModel):
    kind: Literal[ArtifactType.IMAGE] = ArtifactType.IMAGE
    shot_id: str
    prompt: str
    blob_ref: str


class StitchPayload(FrozenModel):
    kind: Literal[ArtifactType.STITCH] = ArtifactType.STITCH
    source_refs: tuple[ArtifactRef, ...]
    blob_ref: str


class QCFinding(FrozenModel):
    """单条质检问题项:规则编号 + 严重程度 + 问题描述 + 建议动作 + 目标分区。"""

    rule_id: str
    severity: Severity
    description: str
    suggested_action: str
    target_partition: str | None


class QCReportPayload(FrozenModel):
    """质量报告:不可变产物。评价器只"观察并出报告",流程决策由闸门策略另行产生。

    不变式:
    - 通过(passed=True)时不得携带返工范围;
    - 不通过时必须至少给出一条问题项(须说明原因)。
    报告编号/生成时间由所属 ArtifactVersion 的 artifact_id/created_at 承载,不在 payload 重复。
    """

    kind: Literal[ArtifactType.QC_REPORT] = ArtifactType.QC_REPORT
    subject_refs: tuple[ArtifactRef, ...]
    target_partition: str | None
    evaluator: str
    evaluator_version: str
    criteria_version: str
    passed: bool
    findings: tuple[QCFinding, ...]
    rework_scope: tuple[str, ...]
    feedback: str

    @model_validator(mode="after")
    def _check_conclusion(self) -> QCReportPayload:
        if self.passed and self.rework_scope:
            raise ValueError("通过的质量报告不得携带返工范围")
        if not self.passed and not self.findings:
            raise ValueError("不通过的质量报告必须至少给出一条问题项")
        return self


class DeliveryPayload(FrozenModel):
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
# Artifact:通用不可变元数据 + payload,不变式由 validator 强制。
# --------------------------------------------------------------------------- #


class ArtifactVersion(FrozenModel):
    """不可变的内容寻址产物版本。生命周期状态(acceptance/currency/dependency)
    不在此建模,而是由事件 + 投影(ArtifactLifecycleView)得到。"""

    artifact_id: str
    series_id: str
    revision: PositiveInt
    type: ArtifactType
    logical_slot: str
    partition_key: str | None
    digest: Sha256Hex
    produced_by_attempt: str | None
    supersedes_id: str | None
    created_at: UtcDatetime
    payload: ArtifactPayload

    @model_validator(mode="after")
    def _enforce_invariants(self) -> ArtifactVersion:
        expected_id = ids.artifact_id(self.series_id, self.revision)
        if self.artifact_id != expected_id:
            raise ValueError(
                f"artifact_id 与 (series_id, revision) 不一致:期望 {expected_id}"
            )
        if self.type != self.payload.kind:
            raise ValueError(
                f"type={self.type} 与 payload.kind={self.payload.kind} 不一致"
            )
        expected_digest = compute_digest(self.payload)
        if self.digest != expected_digest:
            raise ValueError("digest 与 payload 内容不匹配(内容寻址被破坏)")
        # supersedes 必须精确指向同 series 的上一版本,否则会污染 Lineage。
        if self.revision == 1:
            if self.supersedes_id is not None:
                raise ValueError("revision == 1 不得设置 supersedes_id")
        else:
            expected_prev = ids.artifact_id(self.series_id, self.revision - 1)
            if self.supersedes_id != expected_prev:
                raise ValueError(
                    "supersedes_id 必须精确指向同 series 的上一版本 "
                    f"(revision {self.revision - 1})"
                )
        return self

    def ref(self) -> ArtifactRef:
        return ArtifactRef(
            artifact_id=self.artifact_id,
            series_id=self.series_id,
            revision=self.revision,
            digest=self.digest,
        )

    @classmethod
    def create(
        cls,
        *,
        series_id: str,
        revision: int,
        logical_slot: str,
        partition_key: str | None,
        payload: ArtifactPayload,
        produced_by_attempt: str | None,
        created_at: datetime,
    ) -> ArtifactVersion:
        """唯一推荐的构造入口:派生字段(artifact_id/type/digest/supersedes_id)由此计算。"""
        supersedes_id = (
            None if revision == 1 else ids.artifact_id(series_id, revision - 1)
        )
        return cls(
            artifact_id=ids.artifact_id(series_id, revision),
            series_id=series_id,
            revision=revision,
            type=payload.kind,
            logical_slot=logical_slot,
            partition_key=partition_key,
            digest=compute_digest(payload),
            produced_by_attempt=produced_by_attempt,
            supersedes_id=supersedes_id,
            created_at=created_at,
            payload=payload,
        )
