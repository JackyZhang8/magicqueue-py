"""同一实例上注册多个 (topic, group) 处理器。

运行：python examples/multiple_handlers.py
"""

import threading
import time

from magicqueue import JobResult, MQueue, Payload


def main() -> None:
    done = {"n": 0}
    lock = threading.Lock()

    def make(label: str):
        def handler(p: Payload) -> JobResult:
            print(f"[{label}] handling job {p.id} (topic={p.topic} group={p.group})")
            with lock:
                done["n"] += 1
            return JobResult.ok()

        return handler

    q = MQueue("multi").use_memory()
    q.set_handler("email", "notification", make("email"))
    q.set_handler("sms", "notification", make("sms"))
    q.set_handler("report", "", make("report"))
    q.start_workers(4)
    try:
        q.enqueue(Payload("email", group="notification"))
        q.enqueue(Payload("email", group="notification"))
        q.enqueue(Payload("sms", group="notification"))
        q.enqueue(Payload("report"))
        while done["n"] < 4:
            time.sleep(0.02)
        print("all jobs processed")
    finally:
        q.stop()


if __name__ == "__main__":
    main()
