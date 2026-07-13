"""流水线静态模板:StageSpec / Requirement / OutputSpec。

这是类型/slot 级的静态模板;运行期按 partition 展开成实例图。
编译期可据此校验:每个 Requirement 的 type/slot 是否有人生产、
partition 是否兼容、fan-in/fan-out 是否明确、schema 版本是否兼容。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, model_validator

from .enums import (
    ArtifactType,
    CardinalityKind,
    ControlRole,
    ExecutorKind,
    PartitioningKind,
    SideEffectLevel,
)


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Partitioning(_Frozen):
    kind: PartitioningKind
    from_key: str | None = None

    @model_validator(mode="after")
    def _check(self) -> Partitioning:
        if self.kind is PartitioningKind.DYNAMIC_FROM and not self.from_key:
            raise ValueError("DYNAMIC_FROM 分区必须指定 from_key")
        if self.kind is not PartitioningKind.DYNAMIC_FROM and self.from_key:
            raise ValueError("仅 DYNAMIC_FROM 分区允许 from_key")
        return self


class Cardinality(_Frozen):
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


class Requirement(_Frozen):
    """一个 Stage 对上游产物的输入依赖声明。"""

    artifact_type: ArtifactType
    logical_slot: str
    partition_selector: str | None = None
    acceptance_filter: str = "accepted"
    cardinality: Cardinality


class OutputSpec(_Frozen):
    """一个 Stage 的产出声明。"""

    artifact_type: ArtifactType
    logical_slot: str
    schema_version: int
    partitioning: Partitioning


class StageSpec(_Frozen):
    """流水线中的一个工位模板。executor_kind 与 control_role 正交。"""

    stage_id: str
    executor_kind: ExecutorKind
    control_role: ControlRole
    side_effect_level: SideEffectLevel
    requires: tuple[Requirement, ...]
    produces: tuple[OutputSpec, ...]

    @model_validator(mode="after")
    def _check_agent_side_effects(self) -> StageSpec:
        # P0A 约束:Agent 只能是 PURE / READ_ONLY,付费副作用外置到 Provider。
        if self.executor_kind is ExecutorKind.AGENT and self.side_effect_level in (
            SideEffectLevel.COSTED,
            SideEffectLevel.MUTATING,
        ):
            raise ValueError(
                f"P0A 禁止 Agent stage '{self.stage_id}' 携带 "
                f"{self.side_effect_level.value} 副作用;付费/写副作用应外置为 Provider stage"
            )
        return self
