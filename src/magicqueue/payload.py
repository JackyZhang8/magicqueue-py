"""消息载荷与处理结果。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Protocol, Union, runtime_checkable


@dataclass
class Payload:
    """消息载荷。"""

    topic: str
    group: str = ""
    body: Any = None
    priority: int = 0
    """优先级：>0 高，==0 普通（默认），<0 低。同档 FIFO。"""
    max_retry: int = 0
    retry: int = 0
    is_persist: bool = False
    id: str = ""

    def to_json(self) -> bytes:
        return json.dumps(
            {
                "id": self.id,
                "is_persist": self.is_persist,
                "topic": self.topic,
                "group": self.group,
                "body": self.body,
                "priority": self.priority,
                "max_retry": self.max_retry,
                "retry": self.retry,
            },
            ensure_ascii=False,
        ).encode("utf-8")

    @staticmethod
    def from_json(data: bytes) -> Payload:
        obj = json.loads(data)
        return Payload(
            topic=obj.get("topic", ""),
            group=obj.get("group", ""),
            body=obj.get("body"),
            priority=int(obj.get("priority", 0)),
            max_retry=int(obj.get("max_retry", 0)),
            retry=int(obj.get("retry", 0)),
            is_persist=bool(obj.get("is_persist", False)),
            id=obj.get("id", ""),
        )


@dataclass
class JobResult:
    """处理结果。``state == False`` 会触发重试。"""

    state: bool
    message: str = ""
    data: Any = None

    @staticmethod
    def ok(message: str = "ok", data: Any = None) -> JobResult:
        return JobResult(True, message, data)

    @staticmethod
    def fail(message: str = "failed", data: Any = None) -> JobResult:
        return JobResult(False, message, data)


@runtime_checkable
class Handler(Protocol):
    """对象式处理器：实现 ``execute(payload) -> JobResult``。"""

    def execute(self, payload: Payload) -> JobResult: ...


# 处理器既可以是实现 execute 的对象，也可以是普通可调用对象。
HandlerLike = Union[Handler, Callable[["Payload"], "JobResult"]]


def call_handler(handler: HandlerLike, payload: Payload) -> JobResult:
    """统一调用处理器（对象或可调用）。"""
    if isinstance(handler, Handler):
        return handler.execute(payload)
    return handler(payload)
