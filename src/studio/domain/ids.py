"""确定性 ID 生成。

所有业务身份用 UUIDv5 从稳定的逻辑键派生,保证:
- 技术重试:同一 attempt + 同一 logical_operation_key => 同一 operation_id => 不重复扣费
- 业务返工:新 attempt => 新 operation_id => 允许重新生成
- 命令去重:同一 (pm, event, reaction, command_key) => 同一 command_id

编码规则:UUID name 用结构化 JSON 数组编码,避免分隔符歧义导致的碰撞。
"""

from __future__ import annotations

import json
import uuid

# 项目根命名空间(固定,勿改;改动会使历史 ID 失配)。
NAMESPACE = uuid.UUID("6f0c8a1e-2b3d-5e4f-9a7b-1c2d3e4f5a6b")


def _v5(*parts: str) -> str:
    # 结构化编码:不同的 parts 列表永远映射到不同 name,无分隔符注入歧义。
    name = json.dumps(list(parts), ensure_ascii=False, separators=(",", ":"))
    return str(uuid.uuid5(NAMESPACE, name))


def series_id(project_id: str, logical_slot: str, partition_key: str | None) -> str:
    """逻辑产物系列身份。必须包含 Project 作用域,避免跨项目系列冲突。"""
    return _v5("series", project_id, logical_slot, partition_key or "")


def artifact_id(series_id: str, revision: int) -> str:
    """不可变产物版本的稳定身份。"""
    return _v5("artifact", series_id, str(revision))


def operation_id(attempt_id: str, logical_operation_key: str) -> str:
    """Provider 操作幂等键。稳定于计划内的逻辑键,不受计划顺序变动影响。"""
    return _v5("provider-op", attempt_id, logical_operation_key)


def command_id(
    process_manager_id: str,
    event_id: str,
    reaction_name: str,
    command_key: str,
) -> str:
    """Process Manager 反应产出的命令去重键。"""
    return _v5("command", process_manager_id, event_id, reaction_name, command_key)
