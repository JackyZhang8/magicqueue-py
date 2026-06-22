"""基于内存的优先级队列驱动，适用于开发与测试。"""

from __future__ import annotations

import threading
from collections import deque

from .driver import Driver, QueueItem
from .errors import QueueFullError
from .priority import NUM_LEVELS, POP_ORDER, priority_level


class MemoryDriver(Driver):
    """内存优先级队列。进程退出后内容丢失（除非启用持久化）。"""

    def __init__(self, max_queue_size: int = 0) -> None:
        """max_queue_size 为单个逻辑队列（跨所有优先级合计）的最大容量，0 表示不限制。"""
        self._lock = threading.Lock()
        self._queues: dict[str, list[deque]] = {}
        self._max_size = max_queue_size

    def _buckets(self, queue_key: str) -> list[deque]:
        buckets = self._queues.get(queue_key)
        if buckets is None:
            buckets = [deque() for _ in range(NUM_LEVELS)]
            self._queues[queue_key] = buckets
        return buckets

    def _push_locked(self, queue_key: str, message: bytes, priority: int) -> None:
        buckets = self._buckets(queue_key)
        if self._max_size > 0 and sum(len(b) for b in buckets) >= self._max_size:
            raise QueueFullError()
        buckets[priority_level(priority)].append(message)

    def push(self, queue_key: str, message: bytes, priority: int) -> None:
        with self._lock:
            self._push_locked(queue_key, message, priority)

    def push_batch(self, items: list[QueueItem]) -> None:
        with self._lock:
            for it in items:
                self._push_locked(it.queue_key, it.message, it.priority)

    def pop(self, queue_key: str) -> bytes | None:
        with self._lock:
            buckets = self._queues.get(queue_key)
            if buckets is None:
                return None
            for level in POP_ORDER:
                if buckets[level]:
                    return buckets[level].popleft()
            return None

    def size(self, queue_key: str) -> int:
        with self._lock:
            buckets = self._queues.get(queue_key)
            if buckets is None:
                return 0
            return sum(len(b) for b in buckets)

    def clear(self, queue_key: str) -> None:
        with self._lock:
            self._queues.pop(queue_key, None)
