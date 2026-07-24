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


class ToolEffectLevel(StrEnum):
    """Stage 执行器**允许调用的工具**的副作用等级。

    注意:它描述的是可调用工具的副作用,不描述 Model executor 自身的费用
    (后者由 CostMode 表达)。P0A 中 Agent 只允许 PURE / READ_ONLY 工具。
    """

    PURE = "pure"
    READ_ONLY = "read_only"
    COSTED = "costed"
    MUTATING = "mutating"


class CostMode(StrEnum):
    """Stage 执行器**自身**是否产生费用(与工具副作用正交)。

    - FREE:如 mock 模型、纯本地 Transform
    - METERED:如真实 LLM(Token 计费)、付费 Provider
    """

    FREE = "free"
    METERED = "metered"


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
    """验收状态。

    - PROPOSED:候选版本已提议,尚未接受。
    - ACCEPTED:通过接受(自动或门控),成为可被下游消费的版本。
    - REJECTED:候选版本从未被接受,质检直接不通过(已提议 -> 已拒绝)。
    - REVOKED:曾被接受,后续阶段质检发现问题而撤销接受(已接受 -> 接受已撤销)。
    """

    PROPOSED = "proposed"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    REVOKED = "revoked"


class CurrencyStatus(StrEnum):
    """时效状态:是否为该 series 的当前版本。"""

    CURRENT = "current"
    SUPERSEDED = "superseded"


class DependencyStatus(StrEnum):
    """依赖状态:上游变更后是否需要重算。"""

    FRESH = "fresh"
    STALE = "stale"


class ProviderOpStatus(StrEnum):
    """外部 Provider 操作生命周期。

    INITIATED(写前意图)→ CLAIMED(activity 认领,提交前墓碑)→ SUBMITTED → 终态。
    SUBMISSION_UNKNOWN 是 parked 状态(非终态),可经对账恢复。
    ABORTED 是提交前取消墓碑,使迟到的 Claim/Submit 被拒。
    """

    INITIATED = "initiated"
    CLAIMED = "claimed"
    SUBMITTED = "submitted"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SUBMISSION_UNKNOWN = "submission_unknown"
    ABORTED = "aborted"


class TaskAttemptStatus(StrEnum):
    """TaskAttempt 生命周期。"""

    CREATED = "created"
    INPUTS_BOUND = "inputs_bound"
    RUNNING = "running"
    WAITING_BUDGET = "waiting_budget"
    WAITING_PROVIDER = "waiting_provider"
    WAITING_RECONCILIATION = "waiting_reconciliation"
    BLOCKED = "blocked"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class LedgerDirection(StrEnum):
    """ADJUSTMENT 的方向(非负金额需显式方向)。"""

    CREDIT = "credit"
    DEBIT = "debit"


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


class PropagationMode(StrEnum):
    """失效传播范围,在绑定时编译进 lineage edge(不在运行时靠 cardinality 推测)。"""

    PARTITION_PRESERVING = "partition_preserving"
    AGGREGATE = "aggregate"
    GLOBAL = "global"


class AcceptanceMode(StrEnum):
    """产物接受模式。M3 固定 AUTO;M5 对需 QC 的 Stage 用 GATED。"""

    AUTO = "auto"
    GATED = "gated"

