"""消息 Envelope 与 payload 基类(泛型)。

判别字段只在 payload.type;Envelope 不重复保存 type/message_id。
recorded_at = 系统持久化时刻(非业务时间);业务时间须显式进入 payload。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Generic, TypeVar

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
)


def _to_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


UtcDatetime = Annotated[AwareDatetime, AfterValidator(_to_utc)]


class MessagePayload(BaseModel):
    """事件/命令 payload 的基类。子类以 ``type: Literal[...]`` 作为判别字段。"""

    model_config = ConfigDict(frozen=True, extra="forbid")


TPayload = TypeVar("TPayload", bound=MessagePayload)


class EventEnvelope(BaseModel, Generic[TPayload]):
    model_config = ConfigDict(frozen=True)

    event_id: str
    schema_version: int = Field(ge=1)
    stream_id: str
    sequence: int = Field(ge=0)
    global_position: int = Field(ge=0)
    correlation_id: str
    causation_id: str
    recorded_at: UtcDatetime
    payload: TPayload


class CommandEnvelope(BaseModel, Generic[TPayload]):
    model_config = ConfigDict(frozen=True)

    command_id: str
    schema_version: int = Field(ge=1)
    target: str
    command_key: str
    correlation_id: str
    causation_id: str | None
    issued_at: UtcDatetime
    payload: TPayload
