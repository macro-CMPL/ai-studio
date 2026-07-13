"""命令 -> 权威目标流。Router 用它校验 payload aggregate id 与 target 流一致。"""

from __future__ import annotations

from typing import Any

from studio.kernel.errors import ContractViolation

from . import identity
from .payloads import (
    CreateTaskAttemptCmd,
    ExpandStageCmd,
    InitializePipelineCmd,
    MarkArtifactStaleCmd,
    ProposeArtifactVersionCmd,
)


def canonical_target(command: Any) -> str:
    if isinstance(command, InitializePipelineCmd | ExpandStageCmd):
        return identity.project_stream(command.project_id)
    if isinstance(command, CreateTaskAttemptCmd):
        return identity.attempt_stream(command.attempt_id)
    if isinstance(command, ProposeArtifactVersionCmd):
        return identity.series_stream(command.series_id)
    if isinstance(command, MarkArtifactStaleCmd):
        return identity.series_stream(command.target_ref.series_id)
    raise ContractViolation(f"未知命令类型:{type(command).__name__}")
