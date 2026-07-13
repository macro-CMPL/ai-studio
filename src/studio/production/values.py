"""M3 值对象:输入绑定项。"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, PositiveInt

from studio.domain._base import Sha256Hex
from studio.domain.artifacts import ArtifactRef
from studio.domain.enums import PropagationMode


class BindingItem(BaseModel):
    """一条精确输入绑定;propagation_mode 在绑定时编译进,决定失效传播范围。"""

    model_config = ConfigDict(frozen=True, extra="forbid")

    requirement_key: str
    logical_slot: str
    partition_key: str | None
    series_id: str
    artifact_id: str
    revision: PositiveInt
    digest: Sha256Hex
    propagation_mode: PropagationMode

    def to_ref(self) -> ArtifactRef:
        return ArtifactRef(
            artifact_id=self.artifact_id,
            series_id=self.series_id,
            revision=self.revision,
            digest=self.digest,
        )

    @classmethod
    def from_ref(
        cls,
        *,
        requirement_key: str,
        logical_slot: str,
        partition_key: str | None,
        ref: ArtifactRef,
        propagation_mode: PropagationMode,
    ) -> BindingItem:
        return cls(
            requirement_key=requirement_key,
            logical_slot=logical_slot,
            partition_key=partition_key,
            series_id=ref.series_id,
            artifact_id=ref.artifact_id,
            revision=ref.revision,
            digest=ref.digest,
            propagation_mode=propagation_mode,
        )
