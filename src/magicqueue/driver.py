"""队列驱动接口。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class QueueItem:
    """一条待批量入队的消息。"""

    queue_key: str
    message: bytes
    priority: int


class Driver(ABC):
    """底层队列驱动接口（内存 / Redis 等实现它）。

    ``queue_key`` 是逻辑队列名；驱动内部按优先级把它拆成多个子队列，
    调用方只需传入基础 key 与消息优先级。
    """

    @abstractmethod
    def push(self, queue_key: str, message: bytes, priority: int) -> None:
        """按优先级将消息推入队列。"""

    @abstractmethod
    def pop(self, queue_key: str) -> bytes | None:
        """按优先级（高 -> 低）取出一条消息；队列为空返回 None。"""

    @abstractmethod
    def size(self, queue_key: str) -> int:
        """返回队列消息总数（所有优先级之和）。"""

    @abstractmethod
    def clear(self, queue_key: str) -> None:
        """清空指定队列（所有优先级）。"""

    def supports_blocking(self) -> bool:
        """是否支持阻塞弹出（如 Redis 的 BLPOP）。默认 False。"""
        return False

    def bpop(self, queue_key: str, timeout: float) -> bytes | None:
        """阻塞地按优先级取出一条消息，最多等待 timeout 秒。默认退化为 pop。"""
        return self.pop(queue_key)

    def push_batch(self, items: list[QueueItem]) -> None:
        """批量推送。默认逐条 push；驱动可重写以使用 pipeline 等优化。"""
        for it in items:
            self.push(it.queue_key, it.message, it.priority)

    def close(self) -> None:  # noqa: B027 - 默认空实现，驱动可选择重写
        """释放驱动占用的资源。"""
