"""队列相关异常。"""

from __future__ import annotations


class MagicQueueError(Exception):
    """所有 MagicQueue 异常的基类。"""


class DriverNotSetError(MagicQueueError):
    """尚未设置队列驱动。"""

    def __init__(self) -> None:
        super().__init__("queue driver not set")


class EmptyTopicError(MagicQueueError):
    """topic 为空。"""

    def __init__(self) -> None:
        super().__init__("topic can not be empty")


class AlreadyStartedError(MagicQueueError):
    """队列已经启动。"""

    def __init__(self) -> None:
        super().__init__("queue already started")


class QueueFullError(MagicQueueError):
    """内存队列已满。"""

    def __init__(self) -> None:
        super().__init__("memory queue is full")
