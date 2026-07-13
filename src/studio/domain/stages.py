"""流水线静态模板:StageSpec / Requirement / OutputSpec。

这是类型/slot 级的静态模板;运行期按 partition 展开成实例图。
编译期可据此校验:每个 Requirement 的 type/slot 是否有人生产、
partition 是否兼容、fan-in/fan-out 是否明确、schema 版本是否兼容。
"""

from __future__ import annotations

from pydantic import PositiveInt, field_serializer, model_validator

from ._base import FrozenModel
from .enums import (
    AcceptanceStatus,
    ArtifactType,
    CardinalityKind,
    ControlRole,
    CostMode,
    ExecutorKind,
    PartitioningKind,
    ToolEffectLevel,
)


class Partitioning(FrozenModel):
    kind: PartitioningKind
    from_key: str | None = None

    @model_validator(mode="after")
    def _check(self) -> Partitioning:
        if self.kind is PartitioningKind.DYNAMIC_FROM and not self.from_key:
            raise ValueError("DYNAMIC_FROM 分区必须指定 from_key")
        if self.kind is not PartitioningKind.DYNAMIC_FROM and self.from_key:
            raise ValueError("仅 DYNAMIC_FROM 分区允许 from_key")
        return self


class Cardinality(FrozenModel):
    kind: CardinalityKind
    partition_by: str | None = None

    @model_validator(mode="after")
    def _check(self) -> Cardinality:
        dyn = self.kind is CardinalityKind.DYNAMIC_PARTITION_BY
        if dyn and not self.partition_by:
            raise ValueError("DYNAMIC_PARTITION_BY 必须指定 partition_by")
        if not dyn and self.partition_by:
            raise ValueError("仅 DYNAMIC_PARTITION_BY 允许 partition_by")
        return self


class Requirement(FrozenModel):
    """一个 Stage 对上游产物的输入依赖声明。"""

    artifact_type: ArtifactType
    logical_slot: str
    partition_selector: str | None = None
    acceptance_filter: AcceptanceStatus = AcceptanceStatus.ACCEPTED
    cardinality: Cardinality


class OutputSpec(FrozenModel):
    """一个 Stage 的产出声明。"""

    artifact_type: ArtifactType
    logical_slot: str
    schema_version: PositiveInt
    partitioning: Partitioning


class StageSpec(FrozenModel):
    """流水线中的一个工位模板。

    两个正交维度:
    - executor_kind × control_role
    - allowed_tool_effects(可调用工具的副作用) 与 cost_mode(执行器自身是否计费)
    """

    stage_id: str
    executor_kind: ExecutorKind
    control_role: ControlRole
    allowed_tool_effects: frozenset[ToolEffectLevel]
    cost_mode: CostMode
    requires: tuple[Requirement, ...]
    produces: tuple[OutputSpec, ...]

    @field_serializer("allowed_tool_effects")
    def _ser_effects(self, effects: frozenset[ToolEffectLevel]) -> list[str]:
        # frozenset 迭代顺序不稳定;canonical 输出前排序以保证确定性。
        return sorted(e.value for e in effects)

    @model_validator(mode="after")
    def _check_agent_tool_effects(self) -> StageSpec:
        # P0A 约束:Agent 只能调用 PURE / READ_ONLY 工具,付费/写副作用外置为 Provider。
        # 注意:Agent 的 Model 调用费用由 cost_mode(METERED) 表达,不属于工具副作用。
        if self.executor_kind is ExecutorKind.AGENT:
            forbidden = self.allowed_tool_effects - {
                ToolEffectLevel.PURE,
                ToolEffectLevel.READ_ONLY,
            }
            if forbidden:
                names = ", ".join(sorted(e.value for e in forbidden))
                raise ValueError(
                    f"P0A 禁止 Agent stage '{self.stage_id}' 调用带副作用的工具:{names};"
                    f"付费/写副作用应外置为 Provider stage"
                )
        return self
