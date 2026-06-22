import importlib.util

import pytest

plyvel_missing = importlib.util.find_spec("plyvel") is None
pytestmark = pytest.mark.skipif(plyvel_missing, reason="plyvel not installed")


def test_leveldb_store_crud(tmp_path):
    from magicqueue.leveldb_store import LevelDBStore

    path = str(tmp_path / "store.leveldb")
    store = LevelDBStore(path)
    try:
        store.put("a", b"1")
        store.put_batch([("b", b"2"), ("c", b"3")])
        assert dict(store.all()) == {"a": b"1", "b": b"2", "c": b"3"}

        store.delete("a")
        store.delete_batch(["b"])
        assert dict(store.all()) == {"c": b"3"}
    finally:
        store.close()

    # 重新打开，验证数据持久化（崩溃恢复语义）。
    store2 = LevelDBStore(path)
    try:
        assert dict(store2.all()) == {"c": b"3"}
    finally:
        store2.close()
