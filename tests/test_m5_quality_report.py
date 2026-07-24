"""M5 第 2 步:质量报告契约(不可变产物 + 评价器只观察 + AUTO 接受不再套质检)。"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from studio.domain import ids as domain_ids
from studio.domain.artifacts import (
    ArtifactRef,
    ArtifactVersion,
    QCFinding,
    QCReportPayload,
)
from studio.domain.enums import AcceptanceMode, ArtifactType, Severity
from studio.production.payloads import (
    ArtifactVersionAcceptedEvt,
    ArtifactVersionProposedEvt,
    ProposeArtifactVersionCmd,
)
from studio.production.series import ArtifactSeriesDecider
from studio.serialization import digest

_TS = datetime(2026, 1, 1, tzinfo=UTC)
_PROJECT = "p"


def _image_ref(partition: str, revision: int = 1) -> ArtifactRef:
    series = domain_ids.series_id(_PROJECT, "image", partition)
    from studio.domain.artifacts import ImagePayload

    payload = ImagePayload(shot_id=partition, prompt="p", blob_ref=f"blob://{partition}")
    return ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, revision),
        series_id=series,
        revision=revision,
        digest=digest(payload),
    )


def _passing_report(partition: str) -> QCReportPayload:
    return QCReportPayload(
        subject_refs=(_image_ref(partition),),
        target_partition=partition,
        evaluator="result-qc",
        evaluator_version="1",
        criteria_version="1",
        passed=True,
        findings=(),
        rework_scope=(),
        feedback="全部规则通过",
    )


def _failing_report(partition: str) -> QCReportPayload:
    return QCReportPayload(
        subject_refs=(_image_ref(partition),),
        target_partition=partition,
        evaluator="stage-qc",
        evaluator_version="1",
        criteria_version="1",
        passed=False,
        findings=(
            QCFinding(
                rule_id="cross_shot_consistency",
                severity=Severity.ERROR,
                description="shot_02 与 shot_01 人物风格不一致",
                suggested_action="重做 shot_02",
                target_partition=partition,
            ),
        ),
        rework_scope=(partition,),
        feedback="需要定向返工 shot_02",
    )


# --------------------------------------------------------------------------- #
# 契约 / 不变式
# --------------------------------------------------------------------------- #


def test_passing_report_is_valid() -> None:
    report = _passing_report("shot_01")
    assert report.kind is ArtifactType.QC_REPORT
    assert report.passed is True
    assert report.rework_scope == ()


def test_failing_report_is_valid() -> None:
    report = _failing_report("shot_02")
    assert report.passed is False
    assert report.rework_scope == ("shot_02",)
    assert report.findings[0].rule_id == "cross_shot_consistency"
    assert report.findings[0].severity is Severity.ERROR


def test_passed_report_cannot_carry_rework_scope() -> None:
    with pytest.raises(ValidationError):
        QCReportPayload(
            subject_refs=(_image_ref("shot_01"),),
            target_partition="shot_01",
            evaluator="e",
            evaluator_version="1",
            criteria_version="1",
            passed=True,
            findings=(),
            rework_scope=("shot_01",),  # 通过却要返工 -> 非法
            feedback="",
        )


def test_failed_report_requires_finding() -> None:
    with pytest.raises(ValidationError):
        QCReportPayload(
            subject_refs=(_image_ref("shot_01"),),
            target_partition="shot_01",
            evaluator="e",
            evaluator_version="1",
            criteria_version="1",
            passed=False,
            findings=(),  # 不通过却无问题项 -> 非法
            rework_scope=("shot_01",),
            feedback="",
        )


def test_report_is_immutable() -> None:
    report = _passing_report("shot_01")
    with pytest.raises(ValidationError):
        report.passed = False  # type: ignore[misc]


def test_report_digest_is_deterministic() -> None:
    a = _failing_report("shot_02")
    b = _failing_report("shot_02")
    assert digest(a) == digest(b)


# --------------------------------------------------------------------------- #
# 作为不可变产物版本
# --------------------------------------------------------------------------- #


def test_report_is_a_valid_artifact_version() -> None:
    report = _passing_report("shot_01")
    series = domain_ids.series_id(_PROJECT, "result_qc", "shot_01")
    version = ArtifactVersion.create(
        series_id=series,
        revision=1,
        logical_slot="result_qc",
        partition_key="shot_01",
        payload=report,
        produced_by_attempt="att-qc",
        created_at=_TS,
    )
    assert version.type is ArtifactType.QC_REPORT
    assert version.digest == digest(report)
    assert version.artifact_id == domain_ids.artifact_id(series, 1)


# --------------------------------------------------------------------------- #
# 质量报告本身 AUTO 接受,不再套一层质检
# --------------------------------------------------------------------------- #


def test_report_auto_accepted_without_nested_qc() -> None:
    dec = ArtifactSeriesDecider()
    state = dec.initial_state()
    report = _failing_report("shot_02")
    series = domain_ids.series_id(_PROJECT, "stage_qc", None)
    cmd = ProposeArtifactVersionCmd(
        project_id=_PROJECT,
        series_id=series,
        candidate_id="cand-report-1",
        output_key="stage_qc",
        partition_key=None,
        digest=digest(report),
        payload=report,
        acceptance_mode=AcceptanceMode.AUTO,
        produced_by_attempt="att-stage-qc",
    )
    decision = dec.decide(state, cmd)
    from studio.kernel.decisions import Accepted

    assert isinstance(decision, Accepted)
    kinds = [type(pe.payload) for pe in decision.events]
    # AUTO:提议即接受;报告不再触发下游质检
    assert ArtifactVersionProposedEvt in kinds
    assert ArtifactVersionAcceptedEvt in kinds
    assert len(decision.events) == 2
