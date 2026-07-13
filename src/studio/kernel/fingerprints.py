"""内容指纹:用于命令去重时校验"同 ID 是否同内容"。

指纹**排除**时间等非业务字段(issued_at),只覆盖 target + command_key + payload。
"""

from __future__ import annotations

from studio.serialization import digest

from .envelopes import MessagePayload


def command_fingerprint(
    target: str, command_key: str, payload: MessagePayload
) -> str:
    return digest(
        {
            "target": target,
            "command_key": command_key,
            "payload": payload.model_dump(mode="json"),
        }
    )
