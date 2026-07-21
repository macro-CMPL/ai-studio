"""确定性 FakeProvider:强一致 lookup + 幂等 submit,带故障注入与计费账本。

严格证明:charge_count(operation_id) == 1(付费副作用 effectively-once 的可验证下界)。
所有非确定性都来自构造期注入脚本,保证测试可重放。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal

from studio.production.provider_port import (
    AmbiguousSubmissionError,
    LookupFound,
    LookupNotFound,
    LookupResult,
    PollFailed,
    PollPending,
    PollResult,
    PollSucceeded,
    ProviderCapabilities,
    ProviderRequest,
    ResultRef,
    RetryableBeforeSendError,
    SubmitOutcome,
)


def _hex(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


@dataclass
class _Job:
    job_id: str
    key: str
    cost: Decimal
    currency: str


@dataclass
class FakeProvider:
    """支持强 lookup + 幂等 submit;可注入 ambiguous/retryable/pending/fail。"""

    capabilities: ProviderCapabilities = field(
        default_factory=lambda: ProviderCapabilities(
            idempotent_submit=True, strong_lookup_by_key=True, webhook=True
        )
    )
    ambiguous_keys: frozenset[str] = frozenset()
    retryable_keys: frozenset[str] = frozenset()
    pending_first_poll_keys: frozenset[str] = frozenset()
    fail_keys: frozenset[str] = frozenset()
    ambiguous_all: bool = False
    retryable_all: bool = False
    pending_first_poll_all: bool = False
    pending_always: bool = False
    fail_all: bool = False
    strict_non_idempotent: bool = False  # submit 每次都计费/建新 job(严格非幂等替身)
    retry_after: timedelta = timedelta(hours=1)

    _jobs: dict[str, _Job] = field(default_factory=dict)
    _by_job_id: dict[str, _Job] = field(default_factory=dict)
    _poll_count: dict[str, int] = field(default_factory=dict)
    charges: dict[str, int] = field(default_factory=dict)

    def _is_retryable(self, key: str) -> bool:
        return self.retryable_all or key in self.retryable_keys

    def _is_ambiguous(self, key: str) -> bool:
        return self.ambiguous_all or key in self.ambiguous_keys

    def _is_pending_first(self, key: str) -> bool:
        return self.pending_first_poll_all or key in self.pending_first_poll_keys

    def _is_fail(self, key: str) -> bool:
        return self.fail_all or key in self.fail_keys

    def submit(self, idempotency_key: str, request: ProviderRequest) -> SubmitOutcome:
        if self._is_retryable(idempotency_key):
            raise RetryableBeforeSendError(idempotency_key)
        # 幂等:同 key 只建一次 job、只计费一次。
        # strict_non_idempotent:每次 submit 都计费并建新 job(暴露盲目重提的双扣费)。
        if self.strict_non_idempotent or idempotency_key not in self._jobs:
            n = self.charges.get(idempotency_key, 0) + 1
            job_id = (
                f"job-{idempotency_key}-{n}"
                if self.strict_non_idempotent
                else f"job-{idempotency_key}"
            )
            job = _Job(
                job_id=job_id, key=idempotency_key,
                cost=request.expected_cost, currency=request.currency,
            )
            self._jobs[idempotency_key] = job
            self._by_job_id[job.job_id] = job
            self.charges[idempotency_key] = n
        if self._is_ambiguous(idempotency_key):
            raise AmbiguousSubmissionError(idempotency_key)
        job = self._jobs[idempotency_key]
        return SubmitOutcome(job_id=job.job_id, provider_event_id=f"sub-{idempotency_key}")

    def lookup(self, idempotency_key: str) -> LookupResult:
        job = self._jobs.get(idempotency_key)
        if job is None:
            return LookupNotFound()
        return LookupFound(job_id=job.job_id, provider_event_id=f"sub-{idempotency_key}")

    def poll(self, job_id: str) -> PollResult:
        job = self._by_job_id[job_id]
        self._poll_count[job_id] = self._poll_count.get(job_id, 0) + 1
        if self.pending_always:
            return PollPending(retry_after=self.retry_after)
        if self._is_pending_first(job.key) and self._poll_count[job_id] == 1:
            return PollPending(retry_after=self.retry_after)
        if self._is_fail(job.key):
            return PollFailed(
                charged=True, cost_actual=job.cost, cost_currency=job.currency,
                provider_event_id=f"fail-{job.key}",
            )
        return PollSucceeded(
            result_ref=ResultRef(blob_ref=f"blob-{job.key}", digest=_hex(job.key)),
            cost_actual=job.cost, cost_currency=job.currency,
            provider_event_id=f"ok-{job.key}",
        )

    # -- 测试用外部注入 -------------------------------------------------- #

    def force_submit(self, idempotency_key: str, request: ProviderRequest) -> str:
        """模拟"submit 已接单并返回",但调用方(worker)在落库前崩溃。"""
        try:
            out = self.submit(idempotency_key, request)
        except AmbiguousSubmissionError:
            return f"job-{idempotency_key}"
        return out.job_id

    def charge_count(self, idempotency_key: str) -> int:
        return self.charges.get(idempotency_key, 0)

    def poll_count_for(self, idempotency_key: str) -> int:
        return self._poll_count.get(f"job-{idempotency_key}", 0)

    def result_ref_for(self, idempotency_key: str) -> ResultRef:
        return ResultRef(
            blob_ref=f"blob-{idempotency_key}", digest=_hex(idempotency_key)
        )
