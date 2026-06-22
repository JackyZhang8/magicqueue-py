"""批量入队 + 消息优先级。运行：python examples/batch_priority.py"""

import threading
import time

from magicqueue import JobResult, MQueue, Payload


def main() -> None:
    done = {"n": 0}
    lock = threading.Lock()

    def handler(p: Payload) -> JobResult:
        print(f"processed {p.body:<10} (priority={p.priority})")
        with lock:
            done["n"] += 1
        return JobResult.ok()

    q = MQueue("work").use_memory()
    q.set_handler("task", "", handler)

    # 故意按 普通/低/高/普通/高 顺序批量入队。
    payloads = [
        Payload("task", priority=0, body="normal-1"),
        Payload("task", priority=-1, body="low-1"),
        Payload("task", priority=10, body="high-1"),
        Payload("task", priority=0, body="normal-2"),
        Payload("task", priority=5, body="high-2"),
    ]
    total = len(payloads)

    ids = q.enqueue_batch(payloads)
    print(f"batch-enqueued {len(ids)} messages: {ids}")

    # 入队后再启动单 worker，处理顺序确定。
    q.start_workers(1)
    try:
        while done["n"] < total:
            time.sleep(0.02)
        print("expected order: high-1, high-2, normal-1, normal-2, low-1")
    finally:
        q.stop()


if __name__ == "__main__":
    main()
