"""M5 第 1 步:产物生命周期扩展 —— 门控接受 / 拒绝 / 撤销 + 投影。

覆盖:AUTO 提议即接受(保持 M3);GATED 仅提议;显式接受;拒绝;撤销后当前版本回退;
各转换的幂等与冲突;投影正确反映 已提议/已接受/已拒绝/接受已撤销 与当前性。
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from studio.domain import ids as domain_ids
from studio.domain.artifacts import ArtifactRef, ImagePayload
from studio.domain.enums import (
    AcceptanceMode,
    AcceptanceStatus,
    CurrencyStatus,
)
from studio.kernel.decisions import Accepted, Rejected
from studio.kernel.envelopes import EventEnvelope
from studio.kernel.errors import IdempotencyConflict
from studio.production.payloads import (
    AcceptArtifactVersionCmd,
    ProposeArtifactVersionCmd,
    RejectArtifactVersionCmd,
    RevokeArtifactAcceptanceCmd,
)
from studio.production.projections import ArtifactLifecycleView
from studio.production.series import ArtifactSeriesDecider, SeriesState
from studio.serialization import digest

_PROJECT = "p"
_OUTPUT = "image"
_PART = "shot_01"
_SERIES = domain_ids.series_id(_PROJECT, _OUTPUT, _PART)
_TS = datetime(2026, 1, 1, tzinfo=UTC)


def _payload(blob: str) -> ImagePayload:
    return ImagePayload(shot_id=_PART, prompt="p", blob_ref=blob)


def _propose(candidate_id: str, blob: str, *, mode: AcceptanceMode) -> ProposeArtifactVersionCmd:
    payload = _payload(blob)
    return ProposeArtifactVersionCmd(
        project_id=_PROJECT,
        series_id=_SERIES,
        candidate_id=candidate_id,
        output_key=_OUTPUT,
        partition_key=_PART,
        digest=digest(payload),
        payload=payload,
        acceptance_mode=mode,
        produced_by_attempt=f"att-{candidate_id}",
    )


def _accept(candidate_id: str) -> AcceptArtifactVersionCmd:
    return AcceptArtifactVersionCmd(
        project_id=_PROJECT, series_id=_SERIES, candidate_id=candidate_id,
        decision_ref="gate-1",
    )


def _reject(candidate_id: str) -> RejectArtifactVersionCmd:
    return RejectArtifactVersionCmd(
        project_id=_PROJECT, series_id=_SERIES, candidate_id=candidate_id,
        report_ref="report-1", reason="质检不通过",
    )


def _revoke(ref: ArtifactRef) -> RevokeArtifactAcceptanceCmd:
    return RevokeArtifactAcceptanceCmd(
        project_id=_PROJECT, series_id=_SERIES, artifact_ref=ref,
        report_ref="stage-report-1", reason="跨镜头不一致",
    )


class _Driver:
    """驱动 ArtifactSeriesDecider,并累积事件以便构建投影。"""

    def __init__(self) -> None:
        self.dec = ArtifactSeriesDecider()
        self.state: SeriesState = self.dec.initial_state()
        self.events: list[EventEnvelope[object]] = []
        self._gp = 0

    def apply(self, cmd: object) -> Accepted[object] | Rejected:
        decision = self.dec.decide(self.state, cmd)  # type: ignore[arg-type]
        if isinstance(decision, Accepted):
            for pe in decision.events:
                self.state = self.dec.evolve(self.state, pe.payload)
                self.events.append(
                    EventEnvelope(
                        event_id=f"e{self._gp}", schema_version=1, stream_id=_SERIES,
                        sequence=self._gp, global_position=self._gp, correlation_id="c",
                        causation_id="x", recorded_at=_TS, payload=pe.payload,
                    )
                )
                self._gp += 1
        return decision

    def view(self) -> ArtifactLifecycleView:
        return ArtifactLifecycleView.build(self.events)


def _ref(revision: int, blob: str) -> ArtifactRef:
    payload = _payload(blob)
    return ArtifactRef(
        artifact_id=domain_ids.artifact_id(_SERIES, revision),
        series_id=_SERIES, revision=revision, digest=digest(payload),
    )


# --------------------------------------------------------------------------- #
# AUTO vs GATED 提议
# --------------------------------------------------------------------------- #


def test_auto_propose_is_accepted() -> None:
    d = _Driver()
    d.apply(_propose("c1", "b1", mode=AcceptanceMode.AUTO))
    ref = _ref(1, "b1")
    assert d.view().acceptance(ref.artifact_id) is AcceptanceStatus.ACCEPTED
    assert d.state.current_ref == ref


def test_gated_propose_stays_proposed() -> None:
    d = _Driver()
    d.apply(_propose("c1", "b1", mode=AcceptanceMode.GATED))
    ref = _ref(1, "b1")
    assert d.view().acceptance(ref.artifact_id) is AcceptanceStatus.PROPOSED
    assert d.state.current_ref is None  # 未接受 -> 无当前版本


# --------------------------------------------------------------------------- #
# 门控接受 / 拒绝
# --------------------------------------------------------------------------- #


def test_gated_accept_moves_to_accepted() -> None:
    d = _Driver()
    d.apply(_propose("c1", "b1", mode=AcceptanceMode.GATED))
    d.apply(_accept("c1"))
    ref = _ref(1, "b1")
    assert d.view().acceptance(ref.artifact_id) is AcceptanceStatus.ACCEPTED
    assert d.view().currency(ref.artifact_id) is CurrencyStatus.CURRENT
    assert d.state.current_ref == ref


def test_reject_proposed_version() -> None:
    d = _Driver()
    d.apply(_propose("c1", "b1", mode=AcceptanceMode.GATED))
    d.apply(_reject("c1"))
    ref = _ref(1, "b1")
    assert d.view().acceptance(ref.artifact_id) is AcceptanceStatus.REJECTED
    assert d.state.current_ref is None


def test_accept_after_reject_conflicts() -> None:
    d = _Driver()
    d.apply(_propose("c1", "b1", mode=AcceptanceMode.GATED))
    d.apply(_reject("c1"))
    with pytest.raises(IdempotencyConflict):
        d.apply(_accept("c1"))


def test_reject_is_idempotent() -> None:
    d = _Driver()
    d.apply(_propose("c1", "b1", mode=AcceptanceMode.GATED))
    d.apply(_reject("c1"))
    dec = d.apply(_reject("c1"))
    assert isinstance(dec, Accepted) and dec.events == ()


def test_accept_is_idempotent() -> None:
    d = _Driver()
    d.apply(_propose("c1", "b1", mode=AcceptanceMode.GATED))
    d.apply(_accept("c1"))
    dec = d.apply(_accept("c1"))
    assert isinstance(dec, Accepted) and dec.events == ()


def test_reject_after_accept_conflicts() -> None:
    d = _Driver()
    d.apply(_propose("c1", "b1", mode=AcceptanceMode.GATED))
    d.apply(_accept("c1"))
    with pytest.raises(IdempotencyConflict):
        d.apply(_reject("c1"))


# --------------------------------------------------------------------------- #
# 接受撤销 + 当前版本回退
# --------------------------------------------------------------------------- #


def test_revoke_falls_back_to_none_when_only_version() -> None:
    d = _Driver()
    d.apply(_propose("c1", "b1", mode=AcceptanceMode.GATED))
    d.apply(_accept("c1"))
    ref1 = _ref(1, "b1")
    d.apply(_revoke(ref1))
    v = d.view()
    assert v.acceptance(ref1.artifact_id) is AcceptanceStatus.REVOKED
    assert v.currency(ref1.artifact_id) is CurrencyStatus.SUPERSEDED
    assert v.current_ref(_SERIES) is None
    assert d.state.current_ref is None


def test_revoke_falls_back_to_lower_accepted_revision() -> None:
    d = _Driver()
    # v1 接受,v2 接受(当前=v2),撤销 v2 -> 当前回退 v1
    d.apply(_propose("c1", "b1", mode=AcceptanceMode.GATED))
    d.apply(_accept("c1"))
    d.apply(_propose("c2", "b2", mode=AcceptanceMode.GATED))
    d.apply(_accept("c2"))
    ref1, ref2 = _ref(1, "b1"), _ref(2, "b2")
    assert d.state.current_ref == ref2
    d.apply(_revoke(ref2))
    v = d.view()
    assert v.acceptance(ref2.artifact_id) is AcceptanceStatus.REVOKED
    assert v.current_ref(_SERIES) == ref1  # 回退至 v1
    assert v.currency(ref1.artifact_id) is CurrencyStatus.CURRENT
    assert d.state.current_ref == ref1


def test_revoke_non_accepted_is_rejected() -> None:
    d = _Driver()
    d.apply(_propose("c1", "b1", mode=AcceptanceMode.GATED))  # 仅提议,未接受
    ref1 = _ref(1, "b1")
    dec = d.apply(_revoke(ref1))
    assert isinstance(dec, Rejected) and dec.code == "not_accepted"


def test_revoke_is_idempotent() -> None:
    d = _Driver()
    d.apply(_propose("c1", "b1", mode=AcceptanceMode.GATED))
    d.apply(_accept("c1"))
    ref1 = _ref(1, "b1")
    d.apply(_revoke(ref1))
    dec = d.apply(_revoke(ref1))
    assert isinstance(dec, Accepted) and dec.events == ()


def test_revoke_then_reaccept_new_revision_is_current() -> None:
    # 撤销 v1 后接受 v2(黄金场景 shot_02 返工形态)
    d = _Driver()
    d.apply(_propose("c1", "b1", mode=AcceptanceMode.GATED))
    d.apply(_accept("c1"))
    ref1 = _ref(1, "b1")
    d.apply(_revoke(ref1))
    d.apply(_propose("c2", "b2", mode=AcceptanceMode.GATED))
    d.apply(_accept("c2"))
    ref2 = _ref(2, "b2")
    v = d.view()
    assert v.acceptance(ref1.artifact_id) is AcceptanceStatus.REVOKED  # 历史保留
    assert v.acceptance(ref2.artifact_id) is AcceptanceStatus.ACCEPTED
    assert v.current_ref(_SERIES) == ref2
    assert d.state.current_ref == ref2
