import threading

import pytest
from conftest import wait_until

from magicqueue import (
    AlreadyStartedError,
    DriverNotSetError,
    EmptyTopicError,
    JobResult,
    MemoryDriver,
    MQueue,
    Options,
    Payload,
)


def test_enqueue_validation():
    q = MQueue("t")
    with pytest.raises(DriverNotSetError):
        q.enqueue(Payload("x"))

    q = MQueue("t").use_memory()
    with pytest.raises(EmptyTopicError):
        q.enqueue(Payload(""))
    msg_id = q.enqueue(Payload("ok"))
    assert msg_id


def test_end_to_end_memory():
    count = {"n": 0}
    lock = threading.Lock()

    def handler(_p):
        with lock:
            count["n"] += 1
        return JobResult.ok()

    q = MQueue("e2e").use_memory()
    q.set_handler("t", "", handler)
    q.start_workers(2)
    try:
        for _ in range(3):
            q.enqueue(Payload("t"))
        assert wait_until(lambda: count["n"] >= 3)
    finally:
        q.stop()


def test_retry_then_success():
    attempts = {"n": 0}
    lock = threading.Lock()

    def handler(_p):
        with lock:
            attempts["n"] += 1
            n = attempts["n"]
        return JobResult.ok() if n >= 3 else JobResult.fail("transient")

    opts = Options(retry_base_delay=0.02, retry_max_delay=1.0)
    q = MQueue("retry").use_memory().with_options(opts)
    q.set_handler("t", "", handler)
    q.start_workers(1)
    try:
        q.enqueue(Payload("t", max_retry=5))
        assert wait_until(lambda: attempts["n"] >= 3, timeout=5)
    finally:
        q.stop()


def test_dead_letter():
    q = MQueue("dlq").use_memory().with_options(
        Options(enable_dead_letter=True, retry_base_delay=0.01, retry_max_delay=0.1)
    )
    q.set_handler("t", "", lambda _p: JobResult.fail("always"))
    q.start_workers(1)
    try:
        q.enqueue(Payload("t", max_retry=1))
        assert wait_until(lambda: q.dead_letter_size("t") == 1, timeout=5)
    finally:
        q.stop()


def test_handler_exception_recovery():
    recovered = {"n": 0}
    processed = {"n": 0}
    lock = threading.Lock()

    def on_recovery(_msg):
        with lock:
            recovered["n"] += 1

    def handler(_p):
        with lock:
            processed["n"] += 1
        raise RuntimeError("boom")

    q = MQueue("panic").use_memory().register_on_interrupt(on_recovery)
    q.set_handler("t", "", handler)
    q.start_workers(1)
    try:
        q.enqueue(Payload("t"))
        q.enqueue(Payload("t"))  # 验证 worker 没被异常拖垮
        assert wait_until(lambda: recovered["n"] >= 1 and processed["n"] >= 2, timeout=5)
    finally:
        q.stop()


def test_batch_enqueue():
    count = {"n": 0}
    lock = threading.Lock()

    def handler(_p):
        with lock:
            count["n"] += 1
        return JobResult.ok()

    q = MQueue("batch").use_memory()
    q.set_handler("t", "", handler)
    q.start_workers(2)
    try:
        ids = q.enqueue_batch([Payload("t") for _ in range(4)])
        assert len(ids) == 4
        assert all(ids)
        assert wait_until(lambda: count["n"] >= 4)
    finally:
        q.stop()


def test_batch_enqueue_validation():
    q = MQueue("batch").use_memory()
    with pytest.raises(EmptyTopicError):
        q.enqueue_batch([Payload("ok"), Payload("")])


def test_end_to_end_priority_order():
    order = []
    lock = threading.Lock()

    def handler(p):
        with lock:
            order.append(p.body)
        return JobResult.ok()

    q = MQueue("prio").use_memory()
    q.set_handler("t", "", handler)
    # 先入队（无消费者），再单 worker 处理，顺序确定。
    for tag, pri in [("normal", 0), ("low", -1), ("high", 9), ("normal2", 0)]:
        q.enqueue(Payload("t", priority=pri, body=tag))
    q.start_workers(1)
    try:
        assert wait_until(lambda: len(order) >= 4)
    finally:
        q.stop()
    assert order == ["high", "normal", "normal2", "low"]


def test_start_twice_fails():
    q = MQueue("twice").use_memory()
    q.set_handler("t", "", lambda _p: JobResult.ok())
    q.start_workers(1)
    try:
        with pytest.raises(AlreadyStartedError):
            q.start_workers(1)
    finally:
        q.stop()


def _has_plyvel() -> bool:
    import importlib.util

    return importlib.util.find_spec("plyvel") is not None


_BACKENDS = [
    ("sqlite", lambda q, path: q.use_persistence(path), "mq.db"),
]
if _has_plyvel():
    _BACKENDS.append(("leveldb", lambda q, path: q.use_leveldb(path), "mq.leveldb"))


@pytest.mark.parametrize("backend,configure,filename", _BACKENDS, ids=[b[0] for b in _BACKENDS])
def test_persistence_recovery(tmp_path, backend, configure, filename):
    db = str(tmp_path / filename)

    # 阶段 1：持久化入队但不启动 worker（模拟崩溃）。
    q1 = configure(MQueue("persist").use_memory(), db)
    assert q1.err is None
    for _ in range(3):
        q1.enqueue(Payload("job", is_persist=True))
    q1._store.close()  # 释放句柄，模拟进程退出

    # 阶段 2：新实例恢复并处理。
    count = {"n": 0}
    lock = threading.Lock()

    def handler(_p):
        with lock:
            count["n"] += 1
        return JobResult.ok()

    q2 = configure(MQueue("persist").use_memory(), db)
    q2.set_handler("job", "", handler)
    q2.start_workers(2)
    try:
        assert wait_until(lambda: count["n"] >= 3, timeout=5)
    finally:
        q2.stop()


def test_custom_driver_via_base():
    count = {"n": 0}
    lock = threading.Lock()

    def handler(_p):
        with lock:
            count["n"] += 1
        return JobResult.ok()

    q = MQueue("custom").use_driver(MemoryDriver())
    q.set_handler("t", "", handler)
    q.start_workers(1)
    try:
        q.enqueue(Payload("t"))
        assert wait_until(lambda: count["n"] >= 1)
    finally:
        q.stop()


def test_object_handler_with_execute():
    count = {"n": 0}
    lock = threading.Lock()

    class MyHandler:
        def execute(self, _p):
            with lock:
                count["n"] += 1
            return JobResult.ok()

    q = MQueue("obj").use_memory()
    q.set_handler("t", "", MyHandler())
    q.start_workers(1)
    try:
        q.enqueue(Payload("t"))
        assert wait_until(lambda: count["n"] >= 1)
    finally:
        q.stop()
