"""ArtifactLifecycleView:由事件折叠出 acceptance / currency / dependency 投影。

这些状态不存于不可变的 ArtifactVersion,而在此按事件推导(标脏=事实,查询=投影)。
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from studio.domain.artifacts import ArtifactRef
from studio.domain.enums import AcceptanceStatus, CurrencyStatus, DependencyStatus
from studio.kernel.envelopes import EventEnvelope

from .payloads import (
    ArtifactMarkedStaleEvt,
    ArtifactVersionAcceptedEvt,
    ArtifactVersionProposedEvt,
)


class ArtifactLifecycleView:
    def __init__(self) -> None:
        self._proposed: set[str] = set()
        self._accepted: set[str] = set()
        self._stale: set[str] = set()
        # series_id -> (max_accepted_revision, ref)
        self._current: dict[str, tuple[int, ArtifactRef]] = {}
        # artifact_id -> series_id
        self._series_of: dict[str, str] = {}

    @classmethod
    def build(cls, events: Iterable[EventEnvelope[Any]]) -> ArtifactLifecycleView:
        view = cls()
        for env in sorted(events, key=lambda e: e.global_position):
            view._apply(env.payload)
        return view

    def _apply(self, payload: Any) -> None:
        if isinstance(payload, ArtifactVersionProposedEvt):
            self._proposed.add(payload.artifact_ref.artifact_id)
            self._series_of[payload.artifact_ref.artifact_id] = payload.series_id
        elif isinstance(payload, ArtifactVersionAcceptedEvt):
            aid = payload.artifact_ref.artifact_id
            self._accepted.add(aid)
            self._series_of[aid] = payload.series_id
            best = self._current.get(payload.series_id)
            if best is None or payload.revision > best[0]:
                self._current[payload.series_id] = (payload.revision, payload.artifact_ref)
        elif isinstance(payload, ArtifactMarkedStaleEvt):
            self._stale.add(payload.target_ref.artifact_id)

    def current_ref(self, series_id: str) -> ArtifactRef | None:
        entry = self._current.get(series_id)
        return entry[1] if entry is not None else None

    def is_known(self, artifact_id: str) -> bool:
        return artifact_id in self._series_of

    def _require_known(self, artifact_id: str) -> None:
        if artifact_id not in self._series_of:
            raise LookupError(f"未知 artifact:{artifact_id}")

    def acceptance(self, artifact_id: str) -> AcceptanceStatus:
        self._require_known(artifact_id)
        if artifact_id in self._accepted:
            return AcceptanceStatus.ACCEPTED
        return AcceptanceStatus.PROPOSED

    def currency(self, artifact_id: str) -> CurrencyStatus:
        self._require_known(artifact_id)
        series = self._series_of.get(artifact_id)
        if series is not None:
            current = self._current.get(series)
            if current is not None and current[1].artifact_id == artifact_id:
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
