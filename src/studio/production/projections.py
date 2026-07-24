"""ArtifactLifecycleView:由事件折叠出 acceptance / currency / dependency 投影。

这些状态不存于不可变的 ArtifactVersion,而在此按事件推导(标脏=事实,查询=投影)。
生命周期:已提议 -> 已接受 / 已拒绝;已接受 -> 接受已撤销。撤销后当前版本回退至
剩余最高已接受版本(可能为空)。
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from studio.domain.artifacts import ArtifactRef
from studio.domain.enums import AcceptanceStatus, CurrencyStatus, DependencyStatus
from studio.kernel.envelopes import EventEnvelope

from .payloads import (
    ArtifactAcceptanceRevokedEvt,
    ArtifactMarkedStaleEvt,
    ArtifactVersionAcceptedEvt,
    ArtifactVersionProposedEvt,
    ArtifactVersionRejectedEvt,
)


class ArtifactLifecycleView:
    def __init__(self) -> None:
        self._proposed: set[str] = set()
        self._accepted: set[str] = set()
        self._rejected: set[str] = set()
        self._revoked: set[str] = set()
        self._stale: set[str] = set()
        # artifact_id -> series_id
        self._series_of: dict[str, str] = {}
        # series_id -> {revision: ref}(所有已接受过的版本,用于撤销后重算当前版本)
        self._accepted_by_series: dict[str, dict[int, ArtifactRef]] = {}
        # artifact_id -> revision
        self._revision_of: dict[str, int] = {}

    @classmethod
    def build(cls, events: Iterable[EventEnvelope[Any]]) -> ArtifactLifecycleView:
        view = cls()
        for env in sorted(events, key=lambda e: e.global_position):
            view._apply(env.payload)
        return view

    def _apply(self, payload: Any) -> None:
        if isinstance(payload, ArtifactVersionProposedEvt):
            aid = payload.artifact_ref.artifact_id
            self._proposed.add(aid)
            self._series_of[aid] = payload.series_id
            self._revision_of[aid] = payload.revision
        elif isinstance(payload, ArtifactVersionAcceptedEvt):
            aid = payload.artifact_ref.artifact_id
            self._accepted.add(aid)
            self._revoked.discard(aid)  # 重新接受可翻转撤销(防御;M5 不复用同 ref)
            self._series_of[aid] = payload.series_id
            self._revision_of[aid] = payload.revision
            self._accepted_by_series.setdefault(payload.series_id, {})[
                payload.revision
            ] = payload.artifact_ref
        elif isinstance(payload, ArtifactVersionRejectedEvt):
            self._rejected.add(payload.artifact_ref.artifact_id)
        elif isinstance(payload, ArtifactAcceptanceRevokedEvt):
            self._revoked.add(payload.artifact_ref.artifact_id)
        elif isinstance(payload, ArtifactMarkedStaleEvt):
            self._stale.add(payload.target_ref.artifact_id)

    def current_ref(self, series_id: str) -> ArtifactRef | None:
        """当前版本 = 已接受且未撤销中最高 revision 的 ref。"""
        accepted = self._accepted_by_series.get(series_id)
        if not accepted:
            return None
        live = [
            (rev, ref)
            for rev, ref in accepted.items()
            if ref.artifact_id not in self._revoked
        ]
        if not live:
            return None
        return max(live, key=lambda pair: pair[0])[1]

    def is_known(self, artifact_id: str) -> bool:
        return artifact_id in self._series_of

    def _require_known(self, artifact_id: str) -> None:
        if artifact_id not in self._series_of:
            raise LookupError(f"未知 artifact:{artifact_id}")

    def acceptance(self, artifact_id: str) -> AcceptanceStatus:
        self._require_known(artifact_id)
        if artifact_id in self._revoked:
            return AcceptanceStatus.REVOKED
        if artifact_id in self._rejected:
            return AcceptanceStatus.REJECTED
        if artifact_id in self._accepted:
            return AcceptanceStatus.ACCEPTED
        return AcceptanceStatus.PROPOSED

    def currency(self, artifact_id: str) -> CurrencyStatus:
        self._require_known(artifact_id)
        series = self._series_of.get(artifact_id)
        if series is not None:
            current = self.current_ref(series)
            if current is not None and current.artifact_id == artifact_id:
                return CurrencyStatus.CURRENT
        return CurrencyStatus.SUPERSEDED

    def dependency(self, artifact_id: str) -> DependencyStatus:
        self._require_known(artifact_id)
        return (
            DependencyStatus.STALE
            if artifact_id in self._stale
            else DependencyStatus.FRESH
        )

    def status(
        self, artifact_id: str
    ) -> tuple[AcceptanceStatus, CurrencyStatus, DependencyStatus]:
        return (
            self.acceptance(artifact_id),
            self.currency(artifact_id),
            self.dependency(artifact_id),
        )
