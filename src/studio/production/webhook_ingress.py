"""ProviderWebhookIngress:薄回调适配器 —— 验证 provider 回调并发布终态命令。

真实系统在这里做 HMAC/签名校验与 replay 防护(留作债务);本适配器只负责把
已验证的回调翻译成既有 provider-op 终态命令,投到 bus。与 poll 竞争时二者产出内容
一致的终态命令,由 ProviderOperationDecider 按 (provider_event_id, fingerprint) 与
终态结果相等性去重 -> 只结算一次。命令身份复用 activity_command_id,webhook 重复投递幂等。

correlation:从 provider-op 流的 Initiated Envelope 读取 root correlation_id 并继承,
不用 operation_id 截断工作流 trace(与 ActivityWorker 一致)。
"""

from __future__ import annotations

from decimal import Decimal

from studio.kernel.envelopes import CommandEnvelope, MessagePayload
from studio.kernel.ports import Clock, CommandBus, UnitOfWorkFactory

from . import identity
from .activity_worker import activity_command_id
from .provider_op import (
    ProviderOperationInitiatedEvt,
    ProviderResultRef,
    RecordFailedCmd,
    RecordSucceededCmd,
)
from .provider_port import ResultRef


class ProviderWebhookIngress:
    def __init__(
        self,
        *,
        bus: CommandBus,
        uow_factory: UnitOfWorkFactory,
        clock: Clock,
        worker_id: str = "provider-webhook",
        schema_version: int = 1,
    ) -> None:
        self._bus = bus
        self._uow = uow_factory
        self._clock = clock
        self._worker_id = worker_id
        self._schema_version = schema_version

    def deliver_succeeded(
        self,
        *,
        operation_id: str,
        result_ref: ResultRef,
        cost_actual: Decimal,
        cost_currency: str,
        provider_event_id: str,
    ) -> None:
        cid = activity_command_id(
            self._worker_id, operation_id, "succeeded", provider_event_id
        )
        self._publish(
            operation_id, f"succeeded:{operation_id}",
            RecordSucceededCmd(
                operation_id=operation_id,
                result_ref=ProviderResultRef(
                    blob_ref=result_ref.blob_ref, digest=result_ref.digest
                ),
                cost_actual=cost_actual, cost_currency=cost_currency,
                provider_event_id=provider_event_id,
            ),
            cid,
        )

    def deliver_failed(
        self,
        *,
        operation_id: str,
        charged: bool,
        cost_actual: Decimal,
        cost_currency: str,
        provider_event_id: str,
    ) -> None:
        cid = activity_command_id(
            self._worker_id, operation_id, "failed", provider_event_id
        )
        self._publish(
            operation_id, f"failed:{operation_id}",
            RecordFailedCmd(
                operation_id=operation_id, charged=charged,
                cost_actual=cost_actual, cost_currency=cost_currency,
                provider_event_id=provider_event_id,
            ),
            cid,
        )

    def _root_correlation(self, operation_id: str) -> tuple[str, str]:
        """从 provider-op 流读取 Initiated 的 (correlation_id, event_id)。"""
        stream = identity.provider_op_stream(operation_id)
        with self._uow() as uow:
            events = uow.event_store.read_stream(stream)
        for env in events:
            if isinstance(env.payload, ProviderOperationInitiatedEvt):
                return env.correlation_id, env.event_id
        return operation_id, operation_id  # 兜底(正常不会命中)

    def _publish(
        self, operation_id: str, command_key: str, payload: MessagePayload,
        command_id: str,
    ) -> None:
        correlation_id, causation_id = self._root_correlation(operation_id)
        env: CommandEnvelope[MessagePayload] = CommandEnvelope(
            command_id=command_id,
            schema_version=self._schema_version,
            target=identity.provider_op_stream(operation_id),
            command_key=command_key,
            correlation_id=correlation_id,
            causation_id=causation_id,
            issued_at=self._clock.now(),
            payload=payload,
        )
        self._bus.publish(env)
