"""优雅关闭：持续生产消息，收到 SIGINT/SIGTERM 后等待在途任务完成再退出。

运行：python examples/graceful_shutdown.py  （Ctrl-C 退出，或 3 秒后自动退出）
"""

import signal
import threading
import time

from magicqueue import JobResult, MQueue, Payload


def main() -> None:
    processed = {"n": 0}
    lock = threading.Lock()
    stop = threading.Event()

    def handler(_p: Payload) -> JobResult:
        time.sleep(0.05)
        with lock:
            processed["n"] += 1
        return JobResult.ok()

    q = MQueue("worker").use_memory()
    q.set_handler("task", "", handler)
    q.start_workers(4)

    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    # 没有信号时 3 秒后自动停止，便于演示。
    threading.Timer(3.0, stop.set).start()

    produced = 0
    try:
        while not stop.is_set():
            q.enqueue(Payload("task"))
            produced += 1
            time.sleep(0.01)
    finally:
        print(f"shutting down: produced={produced}")
        q.stop()  # 阻塞直到在途任务完成
        print(f"processed={processed['n']} (graceful)")


if __name__ == "__main__":
    main()
