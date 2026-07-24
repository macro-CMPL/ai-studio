"""Step 11:工作室页面状态投影 —— 逐工位中文状态视图(黄金日志最终态 + 返工中快照)。"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from m5_helpers import build_m5_stack, init_budget_command, init_pipeline_command
from studio.production.page_projection import StationStatus, StudioPageProjection
from studio.production.payloads import ArtifactAcceptanceRevokedEvt
from studio.production.pipeline import golden_m5_compiled

_P = "golden"


@pytest.fixture(scope="module")
def events() -> list[Any]:
    s = build_m5_stack()
    s.bus.publish(init_budget_command(_P))
    s.bus.publish(init_pipeline_command(_P))
    s.driver.run_until_quiescent()
    return list(s.db.state.events)


def _proj(events: list[Any]) -> StudioPageProjection:
    return StudioPageProjection.build(golden_m5_compiled(), events)


# --------------------------------------------------------------------------- #


def test_image_station_final_state(events: list[Any]) -> None:
    station = _proj(events).station("image")
    assert station.display_name == "出图工位"
    assert station.status is StationStatus.DONE  # 最终已完成
    assert station.task_count == 3  # shot_01 gen0 + shot_02 gen0 + shot_02 gen1
    assert station.completed_count == 2  # 两镜头各有当前接受图像
    assert station.blocked_count == 0
    assert station.rework_count == 1  # shot_02 返工一次
    assert station.accumulated_cost == Decimal("30")  # 三次出图


def test_delivery_station_done(events: list[Any]) -> None:
    station = _proj(events).station("delivery")
    assert station.display_name == "交付工位"
    assert station.status is StationStatus.DONE
    assert station.completed_count == 1


def test_all_stations_present(events: list[Any]) -> None:
    stations = {s.stage_id for s in _proj(events).stations()}
    assert stations == {
        "storyboard", "plan", "image", "prompt_qc", "result_qc", "stage_qc", "delivery"
    }


def test_reworking_snapshot(events: list[Any]) -> None:
    # 截取至(含)shot_02 撤销的前缀,复现"出图工位 返工中 已完成 1/2"
    revoke_gp = next(
        e.global_position
        for e in events
        if isinstance(e.payload, ArtifactAcceptanceRevokedEvt)
    )
    prefix = [e for e in events if e.global_position <= revoke_gp]
    station = _proj(prefix).station("image")
    assert station.status is StationStatus.REWORKING
    assert station.completed_count == 1  # 仅 shot_01
    assert station.current_partition == "shot_02"  # 返工分区
    assert station.rework_count == 0  # 撤销时刻 gen1 尚未创建


def test_stations_before_any_work_are_idle() -> None:
    # 空事件:所有工位待命
    proj = StudioPageProjection.build(golden_m5_compiled(), [])
    for station in proj.stations():
        assert station.status is StationStatus.IDLE
        assert station.task_count == 0
        assert station.accumulated_cost == Decimal(0)
