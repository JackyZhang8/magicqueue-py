"""Redis 驱动 + 复用 client + 优先级。

需要本地有 Redis：docker run -p 6379:6379 redis
运行：python examples/redis_example.py
"""

import os
import threading
import time

from magicqueue import JobResult, MQueue, Payload


def main() -> None:
    import redis  # pip install "magicqueue[redis]"

    url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
    client = redis.Redis.from_url(url)

    done = {"n": 0}
    lock = threading.Lock()

    def handler(p: Payload) -> JobResult:
        print(f"sending email to {p.body['to']}, subject {p.body['subject']!r}")
        with lock:
            done["n"] += 1
        return JobResult.ok()

    q = MQueue("email_service").use_redis(client)  # 复用调用方的 client
    q.set_handler("email", "notification", handler)
    q.start_workers(4)
    try:
        for i in range(1, 6):
            msg_id = q.enqueue(
                Payload(
                    "email",
                    group="notification",
                    max_retry=3,
                    priority=10 if i == 1 else 0,
                    body={"to": f"user{i}@example.com", "subject": "Hi"},
                )
            )
            print(f"enqueued {msg_id}")
        deadline = time.time() + 3
        while done["n"] < 5 and time.time() < deadline:
            time.sleep(0.1)
    finally:
        q.stop()


if __name__ == "__main__":
    main()
