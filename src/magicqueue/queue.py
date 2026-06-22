"""队列核心实现。"""

from __future__ import annotations

import queue as _queue
import random
import threading
import traceback
import uuid
from dataclasses import dataclass
from typing import Callable

from .driver import Driver, QueueItem
from .errors import AlreadyStartedError, DriverNotSetError, EmptyTopicError
from .logger import DefaultLogger, Logger
from .memory import MemoryDriver
from .payload import HandlerLike, JobResult, Payload, call_handler
from .persistence import SqliteStore

RecoveryListener = Callable[[str], None]


@dataclass
class Options:
    """队列运行时选项，所有字段都有合理默认值。"""

    poll_interval: float = 0.2
    """驱动不支持阻塞弹出时的轮询间隔（秒）。"""
    stats_interval: float = 60.0
    """统计日志输出间隔（秒）；<=0 表示关闭统计。"""
    retry_base_delay: float = 0.5
    """重试指数退避基准（秒）。"""
    retry_max_delay: float = 30.0
    """重试退避上限（秒）。"""
    enable_dead_letter: bool = False
    """为 True 时重试耗尽的消息进入死信队列而非丢弃。"""


def _format_queue_key(name: str, topic: str, group: str) -> str:
    if group:
        return f"{name}_{group}::{topic}"
    return f"{name}_{topic}"


def _format_handler_key(topic: str, group: str) -> str:
    if topic and group:
        return f"{group}::{topic}"
    return topic


def _parse_handler_key(key: str) -> tuple[str, str]:
    idx = key.find("::")
    if idx == -1:
        return key, ""
    return key[idx + 2 :], key[:idx]


def _dead_letter_key(name: str, topic: str, group: str) -> str:
    return _format_queue_key(name, topic, group) + "::dead"


class MQueue:
    """队列核心类型。

    典型用法::

        q = MQueue("svc").use_memory()
        q.set_handler("topic", "group", handler)
        q.start_workers(4)
        try:
            q.enqueue(Payload("topic", group="group"))
        finally:
            q.stop()
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._driver: Driver | None = None
        self._store: SqliteStore | None = None
        self._logger: Logger = DefaultLogger()
        self._opts = Options()
        self._handlers: dict[str, HandlerLike] = {}
        self._on_recovery: RecoveryListener | None = None
        self._err: Exception | None = None

        # 运行时状态。
        self._started = False
        self._stop = threading.Event()
        self._jobs: _queue.Queue | None = None
        self._threads: list[threading.Thread] = []
        self._timers: list[threading.Timer] = []
        self._timers_lock = threading.Lock()

    # ---- 链式配置 ----

    def set_logger(self, logger: Logger) -> MQueue:
        if logger is not None:
            self._logger = logger
        return self

    def with_options(self, opts: Options) -> MQueue:
        self._opts = opts
        return self

    def register_on_interrupt(self, listener: RecoveryListener) -> MQueue:
        self._on_recovery = listener
        return self

    def set_handler(self, topic: str, group: str, handler: HandlerLike) -> MQueue:
        """注册 (topic, group) 对应的处理器（须在 start_workers 前调用）。"""
        self._handlers[_format_handler_key(topic, group)] = handler
        return self

    def use_memory(self, max_queue_size: int = 0) -> MQueue:
        self._driver = MemoryDriver(max_queue_size)
        return self

    def use_redis(self, client) -> MQueue:
        """复用调用方已配置好的 redis.Redis 客户端。"""
        from .redis_driver import RedisDriver

        self._driver = RedisDriver(client)
        return self

    def use_redis_url(self, url: str) -> MQueue:
        """根据连接串自建 Redis 驱动并测试连通性。"""
        from .redis_driver import RedisDriver

        try:
            self._driver = RedisDriver.from_url(url)
        except Exception as e:  # noqa: BLE001 - 收集到 err，在 start 时统一抛出
            self._set_err(e)
        return self

    def use_driver(self, driver: Driver) -> MQueue:
        self._driver = driver
        return self

    def use_persistence(self, path: str) -> MQueue:
        """启用 sqlite 持久化，用于崩溃恢复。"""
        try:
            self._store = SqliteStore(path)
        except Exception as e:  # noqa: BLE001
            self._set_err(e)
        return self

    def _set_err(self, err: Exception) -> None:
        if self._err is None:
            self._err = err

    @property
    def err(self) -> Exception | None:
        """返回链式配置过程中累计的第一个错误。"""
        return self._err

    # ---- 队列信息 ----

    def queue_size(self, topic: str, group: str = "") -> int:
        if self._driver is None:
            return 0
        try:
            return self._driver.size(_format_queue_key(self.name, topic, group))
        except Exception as e:  # noqa: BLE001
            self._logger.log(f"failed to get queue size: {e}")
            return 0

    def dead_letter_size(self, topic: str, group: str = "") -> int:
        if self._driver is None:
            return 0
        try:
            return self._driver.size(_dead_letter_key(self.name, topic, group))
        except Exception as e:  # noqa: BLE001
            self._logger.log(f"failed to get dead-letter size: {e}")
            return 0

    # ---- 入队 ----

    def _prepare(self, payload: Payload) -> None:
        if not payload.topic:
            raise EmptyTopicError()
        payload.id = str(uuid.uuid4())

    def enqueue(self, payload: Payload) -> str:
        """将消息入队，返回消息 ID。"""
        if self._driver is None:
            raise DriverNotSetError()
        self._prepare(payload)
        data = payload.to_json()

        if payload.is_persist:
            if self._store is None:
                self._logger.log(
                    f"warning: is_persist=True but persistence not configured, "
                    f"message {payload.id} won't be recoverable"
                )
            else:
                self._store.put(payload.id, data)

        key = _format_queue_key(self.name, payload.topic, payload.group)
        try:
            self._driver.push(key, data, payload.priority)
        except Exception:
            if payload.is_persist and self._store is not None:
                self._store.delete(payload.id)
            raise
        return payload.id

    def enqueue_batch(self, payloads: list[Payload]) -> list[str]:
        """批量入队，返回各消息 ID（顺序与入参一致）。任一条校验失败则整体不入队。"""
        if self._driver is None:
            raise DriverNotSetError()
        if not payloads:
            return []

        ids: list[str] = []
        items: list[QueueItem] = []
        persist_rows: list[tuple[str, bytes]] = []

        for p in payloads:
            self._prepare(p)
            data = p.to_json()
            ids.append(p.id)
            items.append(
                QueueItem(
                    queue_key=_format_queue_key(self.name, p.topic, p.group),
                    message=data,
                    priority=p.priority,
                )
            )
            if p.is_persist:
                if self._store is None:
                    self._logger.log(
                        f"warning: is_persist=True but persistence not configured, "
                        f"message {p.id} won't be recoverable"
                    )
                else:
                    persist_rows.append((p.id, data))

        if persist_rows and self._store is not None:
            self._store.put_batch(persist_rows)

        try:
            self._driver.push_batch(items)
        except Exception:
            if persist_rows and self._store is not None:
                self._store.delete_batch([pid for pid, _ in persist_rows])
            raise
        return ids

    # ---- 生命周期 ----

    def start_workers(self, worker_num: int) -> None:
        """启动恢复、消费、worker 与统计线程，立即返回。退出前应调用 stop。"""
        if self._err is not None:
            err, self._err = self._err, None
            raise err
        if self._driver is None:
            raise DriverNotSetError()
        if self._started:
            raise AlreadyStartedError()
        worker_num = max(worker_num, 1)

        self._started = True
        self._stop.clear()
        self._jobs = _queue.Queue(maxsize=worker_num * 2)

        # 先恢复，再启动消费者，避免重复消费。
        self._recover()

        for key in list(self._handlers.keys()):
            topic, group = _parse_handler_key(key)
            t = threading.Thread(
                target=self._consume, args=(topic, group), daemon=True, name=f"consume-{key}"
            )
            t.start()
            self._threads.append(t)

        for n in range(worker_num):
            t = threading.Thread(target=self._worker, args=(n,), daemon=True, name=f"worker-{n}")
            t.start()
            self._threads.append(t)

        if self._opts.stats_interval > 0:
            t = threading.Thread(target=self._stats_reporter, daemon=True, name="stats")
            t.start()
            self._threads.append(t)

        self._logger.log(f"queue {self.name!r} started with {worker_num} workers")

    def stop(self) -> None:
        """优雅停止：取消信号、等待所有线程退出并释放资源。"""
        if not self._started:
            return
        self._started = False
        self._stop.set()

        with self._timers_lock:
            timers, self._timers = self._timers, []
        for timer in timers:
            timer.cancel()

        for t in self._threads:
            t.join()
        self._threads = []

        if self._store is not None:
            self._store.close()
        if self._driver is not None:
            self._driver.close()
        self._logger.log(f"queue {self.name!r} stopped")

    def __enter__(self) -> MQueue:
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    # ---- 内部 ----

    def _recover(self) -> None:
        if self._store is None or self._driver is None:
            return
        recovered = 0
        for msg_id, data in self._store.all():
            try:
                payload = Payload.from_json(data)
            except Exception as e:  # noqa: BLE001
                self._logger.log(f"recovery: failed to unmarshal payload {msg_id}: {e}")
                continue
            key = _format_queue_key(self.name, payload.topic, payload.group)
            try:
                self._driver.push(key, data, payload.priority)
            except Exception as e:  # noqa: BLE001
                self._logger.log(f"recovery: failed to requeue {payload.id}: {e}")
                continue
            recovered += 1
        if recovered > 0:
            self._logger.log(f"recovered {recovered} persistent message(s)")

    def _consume(self, topic: str, group: str) -> None:
        assert self._driver is not None and self._jobs is not None
        queue_key = _format_queue_key(self.name, topic, group)
        blocking = self._driver.supports_blocking()

        while not self._stop.is_set():
            try:
                if blocking:
                    data = self._driver.bpop(queue_key, 1.0)
                else:
                    data = self._driver.pop(queue_key)
            except Exception as e:  # noqa: BLE001
                if self._stop.is_set():
                    return
                self._logger.log(f"consume {queue_key}: pop error: {e}")
                if self._stop.wait(self._opts.poll_interval):
                    return
                continue

            if data is None:
                if not blocking and self._stop.wait(self._opts.poll_interval):
                    return
                continue

            try:
                payload = Payload.from_json(data)
            except Exception as e:  # noqa: BLE001
                self._logger.log(f"consume {queue_key}: unmarshal error: {e}")
                continue

            while not self._stop.is_set():
                try:
                    self._jobs.put(payload, timeout=0.2)
                    break
                except _queue.Full:
                    continue

    def _worker(self, worker_id: int) -> None:
        assert self._jobs is not None
        while not self._stop.is_set():
            try:
                job = self._jobs.get(timeout=0.2)
            except _queue.Empty:
                continue
            self._handle(job)

    def _handle(self, job: Payload) -> None:
        handler = self._handlers.get(_format_handler_key(job.topic, job.group))
        if handler is None:
            self._logger.log(
                f"no handler for topic={job.topic!r} group={job.group!r} (job {job.id})"
            )
            return

        result = self._safe_execute(handler, job)

        if result is not None and result.state:
            self._ack(job)
            return

        if job.retry < job.max_retry:
            self._schedule_retry(job)
            return

        self._logger.log(f"job {job.id} failed permanently after {job.retry} retries")
        if self._opts.enable_dead_letter:
            self._to_dead_letter(job)
        self._ack(job)

    def _safe_execute(self, handler: HandlerLike, job: Payload) -> JobResult:
        try:
            return call_handler(handler, job)
        except Exception:  # noqa: BLE001 - 单条消息异常不应拖垮 worker
            stack = traceback.format_exc()
            msg = f"exception while processing job {job.id}:\n{stack}"
            self._logger.log(msg)
            if self._on_recovery is not None:
                self._on_recovery(msg)
            return JobResult.fail("handler raised")

    def _ack(self, job: Payload) -> None:
        if job.is_persist and self._store is not None:
            try:
                self._store.delete(job.id)
            except Exception as e:  # noqa: BLE001
                self._logger.log(f"failed to delete job {job.id} from store: {e}")

    def _schedule_retry(self, job: Payload) -> None:
        job.retry += 1
        delay = self._backoff(job.retry)
        self._logger.log(
            f"job {job.id} failed, retry {job.retry}/{job.max_retry} in {delay:.3f}s"
        )

        def _fire() -> None:
            if self._stop.is_set():
                return
            try:
                self._requeue(job)
            except Exception as e:  # noqa: BLE001
                self._logger.log(f"failed to requeue job {job.id}: {e}")

        timer = threading.Timer(delay, _fire)
        timer.daemon = True
        with self._timers_lock:
            if self._started:
                self._timers.append(timer)
                timer.start()
            # 若已停止则不再调度。

    def _requeue(self, job: Payload) -> None:
        assert self._driver is not None
        data = job.to_json()
        if job.is_persist and self._store is not None:
            self._store.put(job.id, data)
        key = _format_queue_key(self.name, job.topic, job.group)
        self._driver.push(key, data, job.priority)

    def _to_dead_letter(self, job: Payload) -> None:
        assert self._driver is not None
        data = job.to_json()
        key = _dead_letter_key(self.name, job.topic, job.group)
        try:
            self._driver.push(key, data, job.priority)
        except Exception as e:  # noqa: BLE001
            self._logger.log(f"failed to move job {job.id} to dead-letter: {e}")

    def _backoff(self, retry: int) -> float:
        retry = max(retry, 1)
        base = self._opts.retry_base_delay
        max_delay = self._opts.retry_max_delay
        d = base * (2 ** (retry - 1))
        if d <= 0 or d > max_delay:
            d = max_delay
        # 抖动：[0.5d, 1.0d]，避免重试风暴。
        half = d / 2
        return half + random.uniform(0, half)

    def _stats_reporter(self) -> None:
        assert self._driver is not None
        while not self._stop.wait(self._opts.stats_interval):
            self._logger.log("=== Queue Statistics ===")
            for key in list(self._handlers.keys()):
                topic, group = _parse_handler_key(key)
                qk = _format_queue_key(self.name, topic, group)
                try:
                    size = self._driver.size(qk)
                except Exception:  # noqa: BLE001
                    size = 0
                self._logger.log(f"Queue {qk}: {size} messages")
            self._logger.log("=====================")
