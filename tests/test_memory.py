import pytest

from magicqueue import MemoryDriver, QueueFullError


def test_fifo_and_maxsize():
    d = MemoryDriver(max_queue_size=2)
    d.push("k", b"a", 0)
    d.push("k", b"b", 0)
    with pytest.raises(QueueFullError):
        d.push("k", b"c", 0)
    assert d.size("k") == 2
    assert d.pop("k") == b"a"
    assert d.pop("missing") is None


def test_priority_order():
    d = MemoryDriver()
    d.push("k", b"normal", 0)
    d.push("k", b"low", -1)
    d.push("k", b"high", 5)
    d.push("k", b"normal2", 0)

    assert [d.pop("k") for _ in range(4)] == [b"high", b"normal", b"normal2", b"low"]


def test_maxsize_across_priorities():
    d = MemoryDriver(max_queue_size=2)
    d.push("k", b"a", 5)
    d.push("k", b"b", -1)
    with pytest.raises(QueueFullError):
        d.push("k", b"c", 0)


def test_clear():
    d = MemoryDriver()
    d.push("k", b"a", 0)
    d.clear("k")
    assert d.size("k") == 0
    assert d.pop("k") is None
