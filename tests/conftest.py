import time


def wait_until(predicate, timeout=3.0, interval=0.01):
    """轮询直到 predicate() 为真或超时。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()
