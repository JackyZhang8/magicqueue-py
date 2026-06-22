"""自动重试 + 死信队列。运行：python examples/retry_deadletter.py"""

import threading
import time

from magicqueue import JobResult, MQueue, Options, Payload


def main() -> None:
    attempts: dict[str, int] = {}
    lock = threading.Lock()

    def handler(p: Payload) -> JobResult:
        kind = p.body
        with lock:
            attempts[kind] = attempts.get(kind, 0) + 1
            n = attempts[kind]
        if kind == "recoverable":
            if n < 3:
                print(f"recoverable: attempt {n} -> transient failure")
                return JobResult.fail("transient")
            print(f"recoverable: attempt {n} -> success")
            return JobResult.ok()
        print(f"doomed: attempt {n} -> fail (will exhaust retries)")
        return JobResult.fail("permanent")

    q = MQueue("jobs").use_memory().with_options(
        Options(retry_base_delay=0.05, retry_max_delay=2.0, enable_dead_letter=True)
    )
    q.set_handler("process", "", handler)
    q.start_workers(2)
    try:
        q.enqueue(Payload("process", max_retry=5, body="recoverable"))
        q.enqueue(Payload("process", max_retry=2, body="doomed"))
        time.sleep(3)
        print(f"dead-letter queue size: {q.dead_letter_size('process')}")
    finally:
        q.stop()


if __name__ == "__main__":
    main()
