"""M3 确定性身份:task_key / input_binding_digest / attempt_id + 流 ID 约定。"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable

from studio.domain import ids as _domain_ids
from studio.kernel.errors import ContractViolation
from studio.serialization import digest

from .values import BindingItem


def operation_id(attempt_id_value: str, logical_operation_key: str) -> str:
    """Provider 操作幂等键(复用 M1 domain.ids,attempt + 逻辑键派生)。"""
    return _domain_ids.operation_id(attempt_id_value, logical_operation_key)

NAMESPACE = uuid.UUID("c3a1e2d4-5b6f-5c7a-8d9e-0f1a2b3c4d5e")


def _v5(*parts: str | None) -> str:
    name = json.dumps(list(parts), ensure_ascii=False, separators=(",", ":"))
    return str(uuid.uuid5(NAMESPACE, name))


def task_key(project_id: str, stage_id: str, partition_key: str | None) -> str:
    return _v5("task", project_id, stage_id, partition_key)


def input_binding_digest(bindings: Iterable[BindingItem]) -> str:
    items = sorted(
        bindings,
        key=lambda b: (b.requirement_key, b.partition_key or "", b.artifact_id),
    )
    return digest(
        [
            {
                "requirement_key": b.requirement_key,
                "logical_slot": b.logical_slot,
                "partition_key": b.partition_key,
                "artifact_id": b.artifact_id,
                "revision": b.revision,
                "digest": b.digest,
            }
            for b in items
        ]
    )


def attempt_id(
    task_key_value: str, binding_digest: str, execution_generation: int = 0
) -> str:
    return _v5("attempt", task_key_value, binding_digest, str(execution_generation))


def candidate_id(attempt_id_value: str, output_key: str) -> str:
    return _v5("candidate", attempt_id_value, output_key)


# --- 流 ID 约定 --- #


def project_stream(project_id: str) -> str:
    return f"project:{project_id}"


def attempt_stream(attempt_id_value: str) -> str:
    return f"attempt:{attempt_id_value}"


def series_stream(series_id: str) -> str:
    return f"artifact-series:{series_id}"


def budget_stream(project_id: str) -> str:
    return f"budget:{project_id}"


def provider_op_stream(operation_id_value: str) -> str:
    return f"provider-op:{operation_id_value}"


def reconciliation_stream(scope: str) -> str:
    return f"reconciliation:{scope}"


def gate_stream(report_id: str) -> str:
    return f"gate:{report_id}"


def stream_kind(target: str) -> str:
    return target.split(":", 1)[0]


def require_budget_owner(
    *, stream_id: str, project_id: str, operation_id: str
) -> None:
    """预算事件必须来自本项目的 ``budget:{project}`` 流,否则跨项目串账。

    scheduling / result / reconcile 三个 provider PM 仅按 operation_id + 内容指纹
    (amount/currency/quote_digest)核对预算事件是不够的:内容完全一致但来自
    ``budget:{other}`` 的伪造/误路由事件会导致跨项目串账 —— 在正确项目未预留时
    发起付费操作、跨项目结算、或抑制正确项目的释放。owner(来源流)不一致时抛
    ContractViolation,绝不静默接受。
    """
    expected = budget_stream(project_id)
    if stream_id != expected:
        raise ContractViolation(
            f"预算事件来自非本项目流:operation={operation_id} "
            f"expected={expected} got={stream_id}"
        )
