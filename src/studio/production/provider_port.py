"""Provider 端口:ActivityWorker 与真实/伪造 provider 之间的能力感知契约。

能力(ProviderCapabilities)决定 CLAIMED 的默认恢复动作:
- strong_lookup_by_key:先 lookup(operation_id),命中恢复,未命中再 submit;
- 仅 idempotent_submit:直接以同 key 重复 submit,靠幂等收敛;
- 两者皆无:禁止自动执行,parked 等待人工对账。

结构化结果 + 两类异常,把"确定未发出"与"可能已接单"严格区分:
- RetryableBeforeSendError:确定未发出 -> 保持 CLAIMED,稍后重试;
- AmbiguousSubmissionError:可能已接单 -> SUBMISSION_UNKNOWN(parked,靠 lookup 恢复)。
lookup 必须是按 operation_id 的**强一致**查询;仅能按 job_id 查询不算恢复能力
(灰色窗口里还没有 job_id)。本 Step 不建模"provider 明确拒绝且无 job"的终态。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True)
class ProviderCapabilities:
    idempotent_submit: bool
    strong_lookup_by_key: bool
    webhook: bool


@dataclass(frozen=True)
class ProviderRequest:
    """提交给 provider 的最小请求。key(operation_id)单独作为幂等键传入。"""

    operation_id: str
    request_digest: str
    provider_id: str
    provider_version: str
    expected_cost: Decimal
    currency: str


@dataclass(frozen=True)
class SubmitOutcome:
    job_id: str
    provider_event_id: str


@dataclass(frozen=True)
class ResultRef:
    blob_ref: str
    digest: str


# --- lookup 结果 --- #


@dataclass(frozen=True)
class LookupNotFound:
    pass


@dataclass(frozen=True)
class LookupFound:
    job_id: str
    provider_event_id: str


LookupResult = LookupNotFound | LookupFound


# --- poll 结果 --- #


@dataclass(frozen=True)
class PollPending:
    retry_after: timedelta


@dataclass(frozen=True)
class PollSucceeded:
    result_ref: ResultRef
    cost_actual: Decimal
    cost_currency: str
    provider_event_id: str


@dataclass(frozen=True)
class PollFailed:
    charged: bool
    cost_actual: Decimal
    cost_currency: str
    provider_event_id: str


PollResult = PollPending | PollSucceeded | PollFailed


# --- 异常 --- #


class AmbiguousSubmissionError(Exception):
    """提交可能已被接单但结果不确定 -> SUBMISSION_UNKNOWN(靠 lookup 恢复)。"""


class RetryableBeforeSendError(Exception):
    """确定未发出(如连接前失败)-> 保持 CLAIMED,稍后重试。"""


class ProviderPort(Protocol):
    @property
    def capabilities(self) -> ProviderCapabilities: ...

    def submit(self, idempotency_key: str, request: ProviderRequest) -> SubmitOutcome: ...

    def lookup(self, idempotency_key: str) -> LookupResult: ...

    def poll(self, job_id: str) -> PollResult: ...
