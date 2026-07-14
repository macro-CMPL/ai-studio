"""注入的纯 ResultMapper 注册表:provider 结果 -> 领域 ArtifactPayload。

ProviderResultPM 不硬编码 ImagePayload,而是按 stage 输出类型查表构造。
mapper 是纯函数:(spec, result_ref, compiled_stage) -> ArtifactPayload。
AttemptDecider 最终仍会校验 payload.kind 与 blob_ref,mapper 只负责构造。
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

from studio.domain.artifacts import ArtifactPayload, ImagePayload
from studio.domain.enums import ArtifactType
from studio.kernel.errors import ContractViolation

from .compile import CompiledStage
from .execution_spec import ProviderExecutionSpec
from .provider_op import ProviderResultRef

ResultMapperFn = Callable[
    [ProviderExecutionSpec, ProviderResultRef, CompiledStage], ArtifactPayload
]


def image_result_mapper(
    spec: ProviderExecutionSpec, result_ref: ProviderResultRef, stage: CompiledStage
) -> ImagePayload:
    params = {p.key: p.value for p in spec.operation.params}
    shot_id = params.get("shot_id") or params.get("shot") or ""
    prompt = params.get("prompt") or spec.logical_operation_key
    return ImagePayload(shot_id=shot_id, prompt=prompt, blob_ref=result_ref.blob_ref)


class ResultMapperRegistry:
    """按 stage 输出产物类型分发的纯注册表。"""

    def __init__(self, mappers: Mapping[ArtifactType, ResultMapperFn]) -> None:
        self._mappers = dict(mappers)

    def build(
        self,
        spec: ProviderExecutionSpec,
        result_ref: ProviderResultRef,
        stage: CompiledStage,
    ) -> ArtifactPayload:
        mapper = self._mappers.get(stage.output_artifact_type)
        if mapper is None:
            raise ContractViolation(
                f"无 {stage.output_artifact_type} 的 ResultMapper"
            )
        payload = mapper(spec, result_ref, stage)
        if payload.kind is not stage.output_artifact_type:
            raise ContractViolation("ResultMapper 产出类型与 stage 输出不一致")
        return payload


def default_result_mappers() -> ResultMapperRegistry:
    """M4 默认:注册 Image mapper。"""
    return ResultMapperRegistry({ArtifactType.IMAGE: image_result_mapper})
