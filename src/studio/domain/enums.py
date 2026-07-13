"""领域枚举:所有状态与分类维度的单一定义源。"""

from __future__ import annotations

from enum import StrEnum


class ExecutorKind(StrEnum):
    """Stage 的执行方式(与 control_role 正交)。"""

    AGENT = "agent"
    TRANSFORM = "transform"
    PROVIDER = "provider"
    HUMAN = "human"


class ControlRole(StrEnum):
    """Stage 在控制流中的角色(与 executor_kind 正交)。"""

    PRODUCER = "producer"
    EVALUATOR = "evaluator"
    GATE = "gate"


class SideEffectLevel(StrEnum):
    """副作用等级。P0A 中 Agent 只允许 PURE / READ_ONLY。"""

    PURE = "pure"
    READ_ONLY = "read_only"
    COSTED = "costed"
    MUTATING = "mutating"


class ArtifactType(StrEnum):
    """产物类型。同时作为 ArtifactPayload 的判别式取值。"""

    SCRIPT = "script"
    STORYBOARD = "storyboard"
    IMAGE_PLAN = "image_plan"
    IMAGE = "image"
    STITCH = "stitch"
    QC_REPORT = "qc_report"
    DELIVERY = "delivery"


class AcceptanceStatus(StrEnum):
    """验收状态。允许 ACCEPTED 被后续 QC 撤销。"""

    PROPOSED = "proposed"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class CurrencyStatus(StrEnum):
    """时效状态:是否为该 series 的当前版本。"""

    CURRENT = "current"
    SUPERSEDED = "superseded"


class DependencyStatus(StrEnum):
    """依赖状态:上游变更后是否需要重算。"""

    FRESH = "fresh"
    STALE = "stale"


class ProviderOpStatus(StrEnum):
    """外部 Provider 操作生命周期。SUBMISSION_UNKNOWN 需对账。"""

    INITIATED = "initiated"
    SUBMITTED = "submitted"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SUBMISSION_UNKNOWN = "submission_unknown"


class TaskAttemptStatus(StrEnum):
    """TaskAttempt 生命周期。"""

    CREATED = "created"
    INPUTS_BOUND = "inputs_bound"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class LedgerEntryType(StrEnum):
    """追加式预算账本条目类型。"""

    RESERVE = "reserve"
    CAPTURE = "capture"
    RELEASE = "release"
    ADJUSTMENT = "adjustment"


class LedgerSubjectType(StrEnum):
    """账本条目关联的成本主体(不止 Provider)。"""

    MODEL_OPERATION = "model_operation"
    PROVIDER_OPERATION = "provider_operation"
    TASK_ATTEMPT = "task_attempt"


class PartitioningKind(StrEnum):
    """OutputSpec 的分区产出规则。"""

    SINGLETON = "singleton"
    INHERIT_PARTITION = "inherit_partition"
    DYNAMIC_FROM = "dynamic_from"


class CardinalityKind(StrEnum):
    """Requirement 的基数规则。"""

    STATIC = "static"
    DYNAMIC_PARTITION_BY = "dynamic_partition_by"


class GateVerdict(StrEnum):
    """GatePolicy 由 QCReport 推导出的确定性控制决策。"""

    PASS = "pass"
    REWORK = "rework"
    BLOCK = "block"


class Severity(StrEnum):
    """QC finding 严重级别。"""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class BudgetState(StrEnum):
    """订单预算闸门状态。"""

    OK = "ok"
    BLOCKED_BUDGET = "blocked_budget"
    OVER_BUDGET = "over_budget"

