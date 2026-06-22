"""内存队列最简上手。运行：python examples/basic_memory.py"""

import threading
import time

from magicqueue import JobResult, MQueue, Payload


def main() -> None:
    done = {"n": 0}
    lock = threading.Lock()

    def handler(p: Payload) -> JobResult:
        print(f"Hello, {p.body['name']}! (job {p.id})")
        with lock:
            done["n"] += 1
        return JobResult.ok()

    q = MQueue("greeter").use_memory()
    q.set_handler("greet", "", handler)
    q.start_workers(2)
    try:
        for name in ["Alice", "Bob", "Carol"]:
            msg_id = q.enqueue(Payload("greet", body={"name": name}))
            print(f"enqueued job {msg_id}")
        while done["n"] < 3:
            time.sleep(0.02)
        print("all tasks processed")
    finally:
        q.stop()


if __name__ == "__main__":
    main()
