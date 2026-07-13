"""Milestone 3 Golden 场景:动态展开 + 精确绑定 + Lineage 选择性失效 + 重算。"""

from __future__ import annotations

from production_helpers import (
    build_production_stack,
    init_command,
    supersede_plan_command,
)
from studio.domain import ids as domain_ids
from studio.domain.enums import (
    AcceptanceStatus,
    CurrencyStatus,
    DependencyStatus,
)
from studio.production.projections import ArtifactLifecycleView

_PROJECT = "proj-1"
_FRESH_CURRENT = (
    AcceptanceStatus.ACCEPTED,
    CurrencyStatus.CURRENT,
    DependencyStatus.FRESH,
)


def _img_series(partition: str) -> str:
    return domain_ids.series_id(_PROJECT, "image", partition)


def _run_initial() -> object:
    s = build_production_stack()
    s.bus.publish(init_command(_PROJECT))
    s.driver.run_until_quiescent()
    return s


def test_dynamic_expansion_produces_two_images() -> None:
    s = _run_initial()
    view = ArtifactLifecycleView.build(s.db.state.events)  # type: ignore[attr-defined]

    img1 = view.current_ref(_img_series("shot_01"))
    img2 = view.current_ref(_img_series("shot_02"))
    assert img1 is not None and img2 is not None
    assert view.status(img1.artifact_id) == _FRESH_CURRENT
    assert view.status(img2.artifact_id) == _FRESH_CURRENT
    # 两个 image attempt 写各自独立的流(无冲突):stream 数量含 2 个 image attempt
    assert img1.revision == 1
    assert img2.revision == 1


def test_selective_invalidation_and_recompute() -> None:
    s = build_production_stack()
    s.bus.publish(init_command(_PROJECT))
    s.driver.run_until_quiescent()

    view0 = ArtifactLifecycleView.build(s.db.state.events)
    img1_v1 = view0.current_ref(_img_series("shot_01"))
    img2_v1 = view0.current_ref(_img_series("shot_02"))
    assert img1_v1 is not None and img2_v1 is not None

    # 桩:取代 plan[shot_02] -> v2
    s.bus.publish(
        supersede_plan_command(
            _PROJECT, "shot_02", command_id="cmd-plan2v2", candidate_id="cand-plan2-v2"
        )
    )
    s.driver.run_until_quiescent()

    view = ArtifactLifecycleView.build(s.db.state.events)

    # 只有消费 plan[shot_02] v1 的 image[shot_02] v1 被标脏并取代
    assert view.currency(img2_v1.artifact_id) == CurrencyStatus.SUPERSEDED
    assert view.dependency(img2_v1.artifact_id) == DependencyStatus.STALE

    # image[shot_02] 生产前沿是 v2,CURRENT/FRESH
    img2_cur = view.current_ref(_img_series("shot_02"))
    assert img2_cur is not None
    assert img2_cur.revision == 2
    assert view.status(img2_cur.artifact_id) == _FRESH_CURRENT

    # image[shot_01] 完全未受影响:仍是同一个 v1,CURRENT/FRESH
    img1_cur = view.current_ref(_img_series("shot_01"))
    assert img1_cur is not None
    assert img1_cur.artifact_id == img1_v1.artifact_id
    assert img1_cur.revision == 1
    assert view.status(img1_cur.artifact_id) == _FRESH_CURRENT


def test_replay_projection_is_deterministic() -> None:
    s = build_production_stack()
    s.bus.publish(init_command(_PROJECT))
    s.driver.run_until_quiescent()
    s.bus.publish(
        supersede_plan_command(
            _PROJECT, "shot_02", command_id="cmd-plan2v2", candidate_id="cand-plan2-v2"
        )
    )
    s.driver.run_until_quiescent()

    v1 = ArtifactLifecycleView.build(s.db.state.events)
    v2 = ArtifactLifecycleView.build(s.db.state.events)
    img2 = v1.current_ref(_img_series("shot_02"))
    assert img2 is not None
    assert v1.status(img2.artifact_id) == v2.status(img2.artifact_id)
