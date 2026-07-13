"""Canonical JSON 序列化与摘要。

用于:
1. 计算 Artifact 的 digest(基于 payload 的规范化字节)。
2. P0A 验收:对最终 state 做 canonical-JSON digest,断言等于 golden。

"逐字节一致" 定义为 canonical JSON(键排序、紧凑分隔符、UTF-8)的 SHA-256
相同,而非比较 Python 对象内存或非规范 JSON。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    return value


def canonical_bytes(value: Any) -> bytes:
    """将任意 pydantic 模型 / JSON 兼容对象序列化为规范化字节。"""
    payload = _jsonable(value)
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def digest(value: Any) -> str:
    """规范化字节的 SHA-256 十六进制摘要。"""
    return hashlib.sha256(canonical_bytes(value)).hexdigest()
