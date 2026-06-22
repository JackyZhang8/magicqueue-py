"""MagicQueue —— 轻量消息队列库（Python 版）。

支持可插拔驱动（内存 / Redis）、基于 sqlite 的持久化与崩溃恢复、三档消息优先级、
批量入队、带指数退避的自动重试、可选死信队列，以及优雅关闭。

交付语义为 at-least-once（至少一次），消息可能被重复投递，处理器应保证幂等。

示例::

    from magicqueue import MQueue, Payload, JobResult

    q = MQueue("svc").use_memory()
    q.set_handler("email", "notify", lambda p: JobResult.ok())
    q.start_workers(4)
    q.enqueue(Payload("email", group="notify", priority=10))
    q.stop()
"""

from __future__ import annotations

from .driver import Driver, QueueItem
from .errors import (
    AlreadyStartedError,
    DriverNotSetError,
    EmptyTopicError,
    MagicQueueError,
    QueueFullError,
)
from .logger import DefaultLogger, Logger
from .memory import MemoryDriver
from .payload import Handler, HandlerLike, JobResult, Payload
from .persistence import SqliteStore, Store
from .queue import MQueue, Options, RecoveryListener
from .redis_driver import RedisDriver

__all__ = [
    "MQueue",
    "Options",
    "RecoveryListener",
    "Payload",
    "JobResult",
    "Handler",
    "HandlerLike",
    "Driver",
    "QueueItem",
    "MemoryDriver",
    "RedisDriver",
    "Store",
    "SqliteStore",
    "Logger",
    "DefaultLogger",
    "MagicQueueError",
    "DriverNotSetError",
    "EmptyTopicError",
    "AlreadyStartedError",
    "QueueFullError",
]

__version__ = "0.1.0"
