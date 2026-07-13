"""内核确定性 ID 派生(独立命名空间,不依赖 domain)。

- event_id  = UUIDv5(command_id, event_key)     event_key 在同一 Decision 内唯一
- command_id = UUIDv5(pm_id, causation_event_id, reaction_name, command_key)

稳定逻辑键(event_key / command_key)而非序号,避免重构位移导致 ID 漂移。
"""

from __future__ import annotations

import json
import uuid

# 内核命名空间(与 domain 命名空间区分)。
NAMESPACE = uuid.UUID("b7d4c2a0-9e1f-5a3b-8c6d-2f4e6a8c0b1d")


def _v5(*parts: str) -> str:
    name = json.dumps(list(parts), ensure_ascii=False, separators=(",", ":"))
    return str(uuid.uuid5(NAMESPACE, name))


def event_id(command_id: str, event_key: str) -> str:
    """由触发命令 + 稳定 event_key 派生的确定性事件 ID。"""
    return _v5("event", command_id, event_key)


def command_id(
    process_manager_id: str,
    causation_event_id: str,
    reaction_name: str,
    command_key: str,
) -> str:
    """由 PM 反应派生的确定性命令 ID(去重键)。"""
    return _v5(
        "command", process_manager_id, causation_event_id, reaction_name, command_key
    )
