"""ProviderActivityWorker:唯一做真实 provider I/O 的组件(活在纯度边界之外)。

读 provider-op 事件 -> ProviderRegistry 按 spec 路由 provider -> capability-aware 决定动作
-> 一次外部 I/O -> 稳定身份命令投 bus。Decider/PM 完全不变。

owner/routing barrier:单 Worker + ProviderRegistry[(provider_id, provider_version)];
无匹配 provider -> park(不 claim),防止跨 provider 重复真实提交。

状态表:
  INITIATED            发布 Claim(不做 provider I/O;先确认有匹配 provider)
  CLAIMED              capability-aware:强 lookup 命中恢复 / NotFound 即可首次 submit;
                       仅幂等 submit 则直接重提;两者皆无 park。
  SUBMISSION_UNKNOWN   lookup;NotFound 且可提交则同 key 重提
  SUBMITTED            到期后 poll(retry_after 被 clamp 到 >0,防 Pending(0) 忙循环)
  SUCCEEDED/FAILED/ABORTED  无动作

幂等身份:activity_command_id = UUIDv5(worker_id, operation_id, action, evidence_id);
不使用时钟/poll 次数/运行时计数器派生 command_id 或 provider_event_id。
correlation:继承 Initiated Envelope 的 root correlation_id,provider_event_id 只作 evidence。
公平:轮转 cursor;每 tick 最多一次外部 I/O;Pending/retryable 靠 next_due 退避。
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from studio.kernel.envelopes import CommandEnvelope, MessagePayload
from studio.kernel.ports import Clock, CommandBus, UnitOfWorkFactory

from . import identity
from .execution_spec import ProviderExecutionSpec
from .provider_op import (
    ClaimSubmissionCmd,
    ProviderOperationDecider,
    ProviderOperationInitiatedEvt,
    ProviderResultRef,
    ReconcileSubmittedCmd,
    RecordFailedCmd,
    RecordSubmissionUnknownCmd,
    RecordSubmittedCmd,
    RecordSucceededCmd,
)
from .provider_port import (
    AmbiguousSubmissionError,
    LookupFound,
    PollFailed,
    PollPending,
    PollSucceeded,
    ProviderPort,
    ProviderRegistry,
    ProviderRequest,
    RetryableBeforeSendError,
)

_ACTIVITY_NS = uuid.UUID("d5e6f7a8-1b2c-5d3e-8f4a-6b7c8d9e0f1a")


def activity_command_id(
    worker_id: str, operation_id: str, action: str, evidence_id: str
) -> str:
    """Activity 命令的确定性去重键(不经 PM 派生机制)。"""
    name = json.dumps(
        ["activity", worker_id, operation_id, action, evidence_id],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return str(uuid.uuid5(_ACTIVITY_NS, name))


@dataclass
class _OpView:
    operation_id: str
    status: str
    spec: ProviderExecutionSpec
    job_id: str | None
    initiated_event_id: str
    correlation_id: str
    first_gp: int


@dataclass
class _Bookkeeping:
    """进程内存态:重启丢失只影响短期公平/重复投递(下游幂等吸收)。"""

    acted: dict[str, str] = field(default_factory=dict)  # op -> "status:evidence"
    next_due: dict[str, datetime] = field(default_factory=dict)  # op -> 最早重试时刻
    cursor: int = 0


# ProviderOpStatus 的字符串值
_INITIATED = "initiated"
_CLAIMED = "claimed"
_UNKNOWN = "submission_unknown"
_SUBMITTED = "submitted"


class ProviderActivityWorker:
    def __init__(
        self,
        *,
        registry: ProviderRegistry,
        bus: CommandBus,
        uow_factory: UnitOfWorkFactory,
        clock: Clock,
        worker_id: str = "provider-activity",
        backoff: timedelta = timedelta(hours=1),
        min_retry_after: timedelta = timedelta(seconds=1),
        schema_version: int = 1,
    ) -> None:
        self._registry = registry
        self._bus = bus
        self._uow = uow_factory
        self._clock = clock
        self._worker_id = worker_id
        self._backoff = backoff
        self._min_retry_after = max(min_retry_after, timedelta(microseconds=1))
        self._schema_version = schema_version
        self._decider = ProviderOperationDecider()
        self._bk = _Bookkeeping()

    # -- 主 tick ----------------------------------------------------------- #

    def tick(self) -> bool:
        views = self._build_views()
        now = self._clock.now()

        # Phase A:claim(无 provider I/O;仅当存在匹配 provider 才 claim)
        for v in views:
            if v.status == _INITIATED and self._bk.acted.get(v.operation_id) != _claim_key(v):
                if self._resolve(v) is None:
                    continue  # park:无匹配 provider,不 claim
                self._publish_claim(v)
                self._bk.acted[v.operation_id] = _claim_key(v)
                return True

        # Phase B/C:每 tick 最多一次外部 I/O;轮转 cursor 保证公平
        n = len(views)
        if n == 0:
            return False
        for i in range(n):
            v = views[(self._bk.cursor + i) % n]
            provider = self._resolve(v)
            if provider is None:
                continue  # park:无匹配 provider
            if self._act_on(provider, v, now):
                self._bk.cursor = (self._bk.cursor + i + 1) % n
                return True
        return False

    def _resolve(self, v: _OpView) -> ProviderPort | None:
        return self._registry.resolve(v.spec.provider_id, v.spec.provider_version)

    def _act_on(self, provider: ProviderPort, v: _OpView, now: datetime) -> bool:
        op = v.operation_id
        if v.status == _CLAIMED:
            if self._bk.acted.get(op) == _CLAIMED or not self._due(op, now):
                return False
            return self._recover_claimed(provider, v, now)
        if v.status == _UNKNOWN:
            if self._bk.acted.get(op) == _UNKNOWN or not self._due(op, now):
                return False
            return self._recover_unknown(provider, v, now)
        if v.status == _SUBMITTED:
            if not self._due(op, now):
                return False
            return self._poll(provider, v, now)
        return False

    # -- 读侧投影 --------------------------------------------------------- #

    def _build_views(self) -> list[_OpView]:
        with self._uow() as uow:
            events = uow.event_store.read_all(-1)
        streams: dict[str, list[object]] = {}
        for env in events:
            if env.stream_id.startswith("provider-op:"):
                streams.setdefault(env.stream_id, []).append(env)
        views: list[_OpView] = []
        for evs in streams.values():
            ordered = sorted(evs, key=lambda e: e.global_position)  # type: ignore[attr-defined]
            state = self._decider.initial_state()
            initiated_event_id: str | None = None
            correlation_id: str | None = None
            for e in ordered:
                state = self._decider.evolve(state, e.payload)  # type: ignore[attr-defined]
                if isinstance(e.payload, ProviderOperationInitiatedEvt):  # type: ignore[attr-defined]
                    initiated_event_id = e.event_id  # type: ignore[attr-defined]
                    correlation_id = e.correlation_id  # type: ignore[attr-defined]
            if state.status is None or state.operation_id is None or state.spec is None:
                continue
            if initiated_event_id is None or correlation_id is None:
                continue
            views.append(
                _OpView(
                    operation_id=state.operation_id,
                    status=state.status.value,
                    spec=state.spec,
                    job_id=state.job_id,
                    initiated_event_id=initiated_event_id,
                    correlation_id=correlation_id,
                    first_gp=ordered[0].global_position,  # type: ignore[attr-defined]
                )
            )
        views.sort(key=lambda v: v.first_gp)
        return views

    def _due(self, op: str, now: datetime) -> bool:
        due_at = self._bk.next_due.get(op)
        return due_at is None or now >= due_at

    # -- CLAIMED / UNKNOWN 恢复 ------------------------------------------- #

    def _recover_claimed(self, provider: ProviderPort, v: _OpView, now: datetime) -> bool:
        caps = provider.capabilities
        if caps.strong_lookup_by_key:
            found = provider.lookup(v.operation_id)
            if isinstance(found, LookupFound):
                self._publish_submitted(v, found.job_id, found.provider_event_id)
                self._bk.acted[v.operation_id] = _CLAIMED
                return True
            # 强一致 NotFound:已证明未接单,可安全首次 submit(无需 idempotent_submit)。
            return self._do_submit(provider, v, now, source=_CLAIMED)
        if caps.idempotent_submit:
            return self._do_submit(provider, v, now, source=_CLAIMED)
        # 两者皆无:park 等待人工(HA 需 lease,不在 M4 范围)。
        self._bk.acted[v.operation_id] = _CLAIMED
        return False

    def _recover_unknown(self, provider: ProviderPort, v: _OpView, now: datetime) -> bool:
        caps = provider.capabilities
        if caps.strong_lookup_by_key:
            found = provider.lookup(v.operation_id)
            if isinstance(found, LookupFound):
                self._publish_reconcile_submitted(v, found.job_id, found.provider_event_id)
                self._bk.acted[v.operation_id] = _UNKNOWN
                return True
            if caps.idempotent_submit:
                return self._do_submit(provider, v, now, source=_UNKNOWN)
            self._bk.acted[v.operation_id] = _UNKNOWN  # 强 lookup NotFound 但不可幂等重提
            return False
        if caps.idempotent_submit:
            return self._do_submit(provider, v, now, source=_UNKNOWN)
        self._bk.acted[v.operation_id] = _UNKNOWN
        return False

    def _do_submit(
        self, provider: ProviderPort, v: _OpView, now: datetime, *, source: str
    ) -> bool:
        request = _request_of(v.spec)
        try:
            out = provider.submit(v.operation_id, request)
        except RetryableBeforeSendError:
            # 确定未发出:保持 CLAIMED,稍后(时钟推进后)重试。
            self._bk.next_due[v.operation_id] = now + self._backoff
            return True
        except AmbiguousSubmissionError:
            # 可能已接单:转 SUBMISSION_UNKNOWN,靠 lookup 恢复。
            self._publish_unknown(v, reason="ambiguous_submission")
            self._bk.acted[v.operation_id] = source
            return True
        if source == _CLAIMED:
            self._publish_submitted(v, out.job_id, out.provider_event_id)
        else:
            self._publish_reconcile_submitted(v, out.job_id, out.provider_event_id)
        self._bk.acted[v.operation_id] = source
        return True

    # -- SUBMITTED poll --------------------------------------------------- #

    def _poll(self, provider: ProviderPort, v: _OpView, now: datetime) -> bool:
        assert v.job_id is not None
        result = provider.poll(v.job_id)
        if isinstance(result, PollPending):
            # clamp 到 >0,防 Pending(0) 在同一时刻忙循环。
            delay = max(result.retry_after, self._min_retry_after)
            self._bk.next_due[v.operation_id] = now + delay
            return True
        self._bk.next_due[v.operation_id] = now + self._backoff  # 防终态落库前重复 poll
        if isinstance(result, PollSucceeded):
            self._publish_succeeded(v, result)
            return True
        assert isinstance(result, PollFailed)
        self._publish_failed(v, result)
        return True

    # -- 命令发布(稳定身份 + 继承 root correlation) --------------------- #

    def _publish_claim(self, v: _OpView) -> None:
        cid = activity_command_id(
            self._worker_id, v.operation_id, "claim", v.initiated_event_id
        )
        self._publish(
            v, f"claim:{v.operation_id}",
            ClaimSubmissionCmd(operation_id=v.operation_id), cid,
        )

    def _publish_submitted(self, v: _OpView, job_id: str, provider_event_id: str) -> None:
        cid = activity_command_id(
            self._worker_id, v.operation_id, "submitted", provider_event_id
        )
        self._publish(
            v, f"submitted:{v.operation_id}",
            RecordSubmittedCmd(
                operation_id=v.operation_id, job_id=job_id,
                provider_event_id=provider_event_id,
            ),
            cid,
        )

    def _publish_reconcile_submitted(
        self, v: _OpView, job_id: str, evidence: str
    ) -> None:
        authority = f"lookup:{evidence}"
        cid = activity_command_id(
            self._worker_id, v.operation_id, "reconcile-submitted", authority
        )
        self._publish(
            v, f"reconcile-submitted:{v.operation_id}",
            ReconcileSubmittedCmd(
                operation_id=v.operation_id, job_id=job_id, authority_ref=authority
            ),
            cid,
        )

    def _publish_unknown(self, v: _OpView, reason: str) -> None:
        cid = activity_command_id(
            self._worker_id, v.operation_id, "submission-unknown", v.initiated_event_id
        )
        self._publish(
            v, f"submission-unknown:{v.operation_id}",
            RecordSubmissionUnknownCmd(operation_id=v.operation_id, reason=reason),
            cid,
        )

    def _publish_succeeded(self, v: _OpView, result: PollSucceeded) -> None:
        cid = activity_command_id(
            self._worker_id, v.operation_id, "succeeded", result.provider_event_id
        )
        self._publish(
            v, f"succeeded:{v.operation_id}",
            RecordSucceededCmd(
                operation_id=v.operation_id,
                result_ref=ProviderResultRef(
                    blob_ref=result.result_ref.blob_ref, digest=result.result_ref.digest
                ),
                cost_actual=result.cost_actual, cost_currency=result.cost_currency,
                provider_event_id=result.provider_event_id,
            ),
            cid,
        )

    def _publish_failed(self, v: _OpView, result: PollFailed) -> None:
        cid = activity_command_id(
            self._worker_id, v.operation_id, "failed", result.provider_event_id
        )
        self._publish(
            v, f"failed:{v.operation_id}",
            RecordFailedCmd(
                operation_id=v.operation_id, charged=result.charged,
                cost_actual=result.cost_actual, cost_currency=result.cost_currency,
                provider_event_id=result.provider_event_id,
            ),
            cid,
        )

    def _publish(
        self, v: _OpView, command_key: str, payload: MessagePayload, command_id: str
    ) -> None:
        env: CommandEnvelope[MessagePayload] = CommandEnvelope(
            command_id=command_id,
            schema_version=self._schema_version,
            target=identity.provider_op_stream(v.operation_id),
            command_key=command_key,
            correlation_id=v.correlation_id,  # 继承 root correlation(不用 operation_id)
            causation_id=v.initiated_event_id,
            issued_at=self._clock.now(),
            payload=payload,
        )
        self._bus.publish(env)


def _claim_key(v: _OpView) -> str:
    return f"{_INITIATED}:{v.initiated_event_id}"


def _request_of(spec: ProviderExecutionSpec) -> ProviderRequest:
    return ProviderRequest(
        operation_id=spec.operation_id,
        request_digest=spec.request_digest,
        provider_id=spec.provider_id,
        provider_version=spec.provider_version,
        expected_cost=spec.estimated_cost,
        currency=spec.currency,
    )
