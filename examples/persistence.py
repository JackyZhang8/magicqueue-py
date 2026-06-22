"""sqlite 持久化与崩溃恢复。

运行两次：
    python examples/persistence.py write
    python examples/persistence.py recover
"""

import sys
import time

from magicqueue import JobResult, MQueue, Payload

DB_PATH = "./data/persistence_example.db"


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "recover"

    if mode == "write":
        q = MQueue("persist").use_memory().use_persistence(DB_PATH)
        if q.err is not None:
            raise SystemExit(f"setup: {q.err}")
        for i in range(3):
            msg_id = q.enqueue(Payload("job", body=i, is_persist=True))
            print(f"persisted job {msg_id} (not processed, simulating crash)")
        q._store.close()
        print("now run: python examples/persistence.py recover")
    elif mode == "recover":
        q = MQueue("persist").use_memory().use_persistence(DB_PATH)
        q.set_handler(
            "job", "", lambda p: (print(f"processed recovered/new job {p.id}"), JobResult.ok())[1]
        )
        q.start_workers(2)
        try:
            time.sleep(2)
            print("recovery example finished")
        finally:
            q.stop()
    else:
        print(f"unknown mode {mode!r}; use 'write' or 'recover'")


if __name__ == "__main__":
    main()
