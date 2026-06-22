"""MagicQueue 内存驱动微基准，用于三语言版本横向对比。

运行：python bench.py
"""

from __future__ import annotations

import itertools
import statistics
import threading
import time

from magicqueue import JobResult, MQueue, Options, Payload

ENQUEUE_N = 200_000
BATCH_N = 200_000
BATCH_B = 1000
E2E_N = 200_000
WORKERS = 4
REPS = 3


def bench_enqueue() -> float:
    q = MQueue("bench").use_memory()
    start = time.perf_counter()
    for _ in range(ENQUEUE_N):
        q.enqueue(Payload("t"))
    return ENQUEUE_N / (time.perf_counter() - start)


def bench_batch() -> float:
    q = MQueue("bench").use_memory()
    start = time.perf_counter()
    for i in range(0, BATCH_N, BATCH_B):
        n = min(BATCH_B, BATCH_N - i)
        q.enqueue_batch([Payload("t") for _ in range(n)])
    return BATCH_N / (time.perf_counter() - start)


def bench_e2e() -> float:
    counter = itertools.count(1)  # CPython GIL 下 next() 线程安全
    done = threading.Event()

    def handler(_p):
        if next(counter) == E2E_N:
            done.set()
        return JobResult.ok()

    q = MQueue("bench").use_memory().with_options(Options(stats_interval=0))
    q.set_handler("t", "", handler)
    for _ in range(E2E_N):
        q.enqueue(Payload("t"))

    start = time.perf_counter()
    q.start_workers(WORKERS)
    done.wait()
    elapsed = time.perf_counter() - start
    q.stop()
    return E2E_N / elapsed


def run(name: str, fn) -> None:
    results = [fn() for _ in range(REPS)]
    print(f"{name:<28} {statistics.median(results):>12.0f} ops/s")


def main() -> None:
    print(
        f"MagicQueue Python bench (N={ENQUEUE_N}, batch={BATCH_B}, "
        f"workers={WORKERS}, reps={REPS})"
    )
    run("enqueue (single, memory)", bench_enqueue)
    run("enqueue (batch=1000, memory)", bench_batch)
    run("end-to-end (4 workers)", bench_e2e)


if __name__ == "__main__":
    main()
