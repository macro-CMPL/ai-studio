"""M5 步骤6:相同输入强制重做的返工代数。

验证:相同输入 + 返工代数递增 => 新任务身份(attempt_id)=> 新提供方操作身份
(operation_id)=> 允许合理的二次扣费;同时禁止伪造代数,并记录返工血缘。
"""

from __future__ import annotations

from typing import Any

from studio.domain import ids as domain_ids
from studio.domain.artifacts import (
    ArtifactRef,
    ImagePlanPayload,
    OperationParam,
    PlannedOperation,
)
from studio.domain.enums import PropagationMode, TaskAttemptStatus
from studio.kernel.decisions import Accepted, Rejected
from studio.production import identity
from studio.production.attempt import TaskAttemptDecider
from studio.production.payloads import CreateTaskAttemptCmd, TaskAttemptCreatedEvt
from studio.production.pipeline import golden_compiled
from studio.production.values import BindingItem
from studio.serialization import digest

_S = TaskAttemptStatus
_PROJECT = "p"
_PART = "shot_02"
_OP_KEY = "shot_02:image:v0"


def _apply(decider: Any, state: Any, cmd: Any) -> tuple[Any, Any]:
    decision = decider.decide(state, cmd)
    if isinstance(decision, Accepted):
        for pe in decision.events:
            state = decider.evolve(state, pe.payload)
    return state, decision


def _plan_payload() -> ImagePlanPayload:
    return ImagePlanPayload(
        operations=(
            PlannedOperation(
                logical_operation_key=_OP_KEY, op_type="gen",
                params=(OperationParam(key="shot", value=_PART),),
            ),
        )
    )


def _plan_binding() -> BindingItem:
    payload = _plan_payload()
    series = domain_ids.series_id(_PROJECT, "plan", _PART)
    ref = ArtifactRef(
        artifact_id=domain_ids.artifact_id(series, 1), series_id=series,
        revision=1, digest=digest(payload),
    )
    return BindingItem.from_ref(
        requirement_key="plan", logical_slot="plan", partition_key=_PART,
        ref=ref, propagation_mode=PropagationMode.PARTITION_PRESERVING,
    )


def _image_attempt_id(binding: BindingItem, generation: int) -> str:
    tk = identity.task_key(_PROJECT, "image", _PART)
    return identity.attempt_id(
        tk, identity.input_binding_digest((binding,)), generation
    )


def _create_cmd(generation: int, **overrides: Any) -> CreateTaskAttemptCmd:
    binding = _plan_binding()
    base: dict[str, Any] = dict(
        attempt_id=_image_attempt_id(binding, generation), project_id=_PROJECT,
        stage_id="image", partition_key=_PART, output_key="image",
        series_id=domain_ids.series_id(_PROJECT, "image", _PART),
        exact_refs=(binding,), execution_generation=generation,
    )
    base.update(overrides)
    return CreateTaskAttemptCmd(**base)


def _provider_decider() -> TaskAttemptDecider:
    return TaskAttemptDecider(golden_compiled(), {})


# --------------------------------------------------------------------------- #


def test_same_input_different_generation_yields_different_attempt_id() -> None:
    """相同 task_key + 相同输入绑定,代数不同 => attempt_id 不同。"""
    binding = _plan_binding()
    gen0 = _image_attempt_id(binding, 0)
    gen1 = _image_attempt_id(binding, 1)
    gen2 = _image_attempt_id(binding, 2)
    assert gen0 != gen1 != gen2
    assert len({gen0, gen1, gen2}) == 3


def test_new_generation_yields_new_operation_id() -> None:
    """代数递增改变 attempt_id,进而改变 operation_id,允许合理二次扣费。

    operation_id = f(attempt_id, logical_operation_key);相同逻辑操作键在不同代数
    下派生出不同 operation_id,预算流不会把二次返工识别为技术重投而拒绝扣费。
    """
    binding = _plan_binding()
    op0 = identity.operation_id(_image_attempt_id(binding, 0), _OP_KEY)
    op1 = identity.operation_id(_image_attempt_id(binding, 1), _OP_KEY)
    assert op0 != op1


def test_create_generation_one_records_rework_provenance() -> None:
    """代数1的创建必须携带并落库返工血缘(来源 attempt / 报告 / 原因)。"""
    d = _provider_decider()
    gen0 = _image_attempt_id(_plan_binding(), 0)
    cmd = _create_cmd(
        1,
        rework_of_attempt=gen0,
        rework_report_ref="report:stage-1",
        rework_reason="阶段质检发现 shot_02 跨镜头不一致",
    )
    s, dec = _apply(d, d.initial_state(), cmd)
    assert isinstance(dec, Accepted)
    created = dec.events[0].payload
    assert isinstance(created, TaskAttemptCreatedEvt)
    assert created.execution_generation == 1
    assert created.rework_of_attempt == gen0
    assert created.rework_report_ref == "report:stage-1"
    assert created.rework_reason == "阶段质检发现 shot_02 跨镜头不一致"
    # 状态投影记录代数与血缘
    assert s.execution_generation == 1
    assert s.rework_of_attempt == gen0
    assert s.rework_report_ref == "report:stage-1"
    assert s.status is _S.INPUTS_BOUND


def test_forged_generation_mismatch_rejected() -> None:
    """attempt_id 由代数0派生,却声明代数1 => attempt_id 校验不过。"""
    d = _provider_decider()
    binding = _plan_binding()
    forged = CreateTaskAttemptCmd(
        attempt_id=_image_attempt_id(binding, 0),  # 代数0 的 id
        project_id=_PROJECT, stage_id="image", partition_key=_PART,
        output_key="image", series_id=domain_ids.series_id(_PROJECT, "image", _PART),
        exact_refs=(binding,), execution_generation=1,  # 却声明代数1
    )
    dec = d.decide(d.initial_state(), forged)
    assert isinstance(dec, Rejected) and dec.code == "forged_attempt"


def test_negative_generation_rejected() -> None:
    """负数代数直接拒绝。"""
    d = _provider_decider()
    binding = _plan_binding()
    bad = CreateTaskAttemptCmd(
        attempt_id=_image_attempt_id(binding, -1), project_id=_PROJECT,
        stage_id="image", partition_key=_PART, output_key="image",
        series_id=domain_ids.series_id(_PROJECT, "image", _PART),
        exact_refs=(binding,), execution_generation=-1,
    )
    dec = d.decide(d.initial_state(), bad)
    assert isinstance(dec, Rejected) and dec.code == "bad_generation"


def test_generation_zero_has_no_rework_provenance() -> None:
    """首次执行(代数0)无返工血缘,字段保持 None。"""
    d = _provider_decider()
    s, dec = _apply(d, d.initial_state(), _create_cmd(0))
    assert isinstance(dec, Accepted)
    created = dec.events[0].payload
    assert isinstance(created, TaskAttemptCreatedEvt)
    assert created.execution_generation == 0
    assert created.rework_of_attempt is None
    assert created.rework_report_ref is None
    assert created.rework_reason is None
    assert s.execution_generation == 0
    assert s.rework_of_attempt is None


def test_two_generations_are_independent_streams() -> None:
    """代数0与代数1是两个独立 attempt 流,各自从初始态创建成功。"""
    d = _provider_decider()
    s0, dec0 = _apply(d, d.initial_state(), _create_cmd(0))
    s1, dec1 = _apply(d, d.initial_state(), _create_cmd(1, rework_of_attempt=s0.attempt_id))
    assert isinstance(dec0, Accepted) and isinstance(dec1, Accepted)
    assert s0.attempt_id != s1.attempt_id
    assert s0.execution_generation == 0 and s1.execution_generation == 1
