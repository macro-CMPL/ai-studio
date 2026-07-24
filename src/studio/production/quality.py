"""M5 质量层配置:三层质检定义 + 门控产物集合 + 返工上限。

五个质量相关 PM(质量评价调度、闸门决策、返工、阶段汇合、交付)共享这份静态配置,
统一对齐:哪个产物属于哪一层质检、被检查产物是谁、用哪套闸门策略、每阶段最多返工几次。

三层质检(锁定):
- 提示词质检(PROMPT):出图前逐镜头检查提示词,通过后才允许调用付费出图。
- 结果质检(RESULT):逐张图检查,通过后该图像版本才正式接受。
- 阶段质检(STAGE):所有镜头结果通过后聚合检查跨镜头一致性,可精确指定某镜头返工。
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from studio.domain.enums import ArtifactType

from .gate import GatePolicy


class QCLayer(StrEnum):
    """三层质量检查。"""

    PROMPT = "prompt"  # 提示词质检
    RESULT = "result"  # 结果质检
    STAGE = "stage"  # 阶段质检


class QCLayerSpec(BaseModel):
    """一层质检的静态定义。qc_stage_id 约定与其 output_key 同名(与既有 stage 一致)。"""

    model_config = ConfigDict(frozen=True)

    layer: QCLayer
    qc_stage_id: str  # 质检报告产物的 stage_id(== output_key)
    subject_output_key: str  # 被检查产物的 output_key(提示词层为 plan,结果层为 image)
    subject_artifact_type: ArtifactType  # 被检查产物类型(用于派生 requirement_key)

    @property
    def subject_requirement_key(self) -> str:
        # 与 compile._requirement_key 对齐:f"{artifact_type}:{logical_slot}"。
        return f"{self.subject_artifact_type.value}:{self.subject_output_key}"


class QualityConfig:
    """质量层配置的单一定义源。

    - layers:三层质检定义。
    - gated_output_keys:需门控接受的产物 output_key(如 plan、image),
      其提议不自动接受,须经闸门决策显式接受。
    - policies:每层的确定性闸门策略(纯函数,版本化)。
    - rework_limits:每阶段(按被检查产物的 stage_id / output_key)最大返工次数。
    """

    def __init__(
        self,
        *,
        layers: tuple[QCLayerSpec, ...],
        gated_output_keys: frozenset[str],
        policies: Mapping[QCLayer, GatePolicy],
        rework_limits: Mapping[str, int],
    ) -> None:
        self.layers = layers
        self.gated_output_keys = gated_output_keys
        self._policies = dict(policies)
        self._rework_limits = dict(rework_limits)
        # fail-fast:每层必须注册对应闸门策略,且 qc_stage_id 唯一。
        stage_ids = [lyr.qc_stage_id for lyr in layers]
        if len(set(stage_ids)) != len(stage_ids):
            raise ValueError("QCLayerSpec.qc_stage_id 必须唯一")
        for lyr in layers:
            if lyr.layer not in self._policies:
                raise ValueError(f"质检层 {lyr.layer} 缺少闸门策略")

    def layer_by_subject(self, output_key: str) -> QCLayerSpec | None:
        return next((s for s in self.layers if s.subject_output_key == output_key), None)

    def layer_by_qc_stage(self, qc_stage_id: str) -> QCLayerSpec | None:
        return next((s for s in self.layers if s.qc_stage_id == qc_stage_id), None)

    def policy(self, layer: QCLayer) -> GatePolicy:
        return self._policies[layer]

    def is_gated(self, output_key: str) -> bool:
        return output_key in self.gated_output_keys

    def rework_limit(self, stage_id: str) -> int | None:
        return self._rework_limits.get(stage_id)
