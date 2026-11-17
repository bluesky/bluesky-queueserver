import asyncio
import os
import random
import sys

import pytest
import redis.asyncio

from bluesky_queueserver.manager.plan_queue_ops import PlanQueueOperations
from bluesky_queueserver.manager.sqlite_storage import SQLiteStorage, get_default_sqlite_path

from .common import (
    _test_redis_name_prefix,
    _user,
    _user_group,
    condition_manager_idle,
    re_manager_cmd,  # noqa: F401
    requires_redis,
    wait_for_condition,
    zmq_request,
)

_list_key = f"{_test_redis_name_prefix}_sqlite_test_list"
_kv_key = f"{_test_redis_name_prefix}_sqlite_test_kv"


def _generate_ops(rng, n_ops):
    values = ["a", "b", "c", "d", "e"]
    ops = []
    for _ in range(n_ops):
        v = rng.choice(values)
        op = rng.choice(
            ["rpush", "rpush", "lpush", "lpop", "rpop", "llen", "lindex", "lrange", "lrem", "linsert", "ltrim"]
        )
        if op in ("rpush", "lpush"):
            ops.append((op, (_list_key, v)))
        elif op in ("lpop", "rpop", "llen"):
            ops.append((op, (_list_key,)))
        elif op == "lindex":
            ops.append((op, (_list_key, rng.randint(-8, 8))))
        elif op == "lrange":
            ops.append((op, (_list_key, rng.randint(-8, 8), rng.randint(-8, 8))))
        elif op == "lrem":
            ops.append((op, (_list_key, rng.randint(-2, 2), v)))
        elif op == "linsert":
            ops.append((op, (_list_key, rng.choice(["BEFORE", "AFTER"]), rng.choice(values), v)))
        elif op == "ltrim" and rng.random() < 0.2:
            ops.append((op, (_list_key, rng.randint(-3, 1), rng.randint(-2, 8))))
    return ops


async def _apply_ops(storage, ops):
    results = []
    for name, args in ops:
        results.append(await getattr(storage, name)(*args))
        results.append(await storage.lrange(_list_key, 0, -1))
    return results


# fmt: off
@pytest.mark.parametrize("seed", range(10))
# fmt: on
@requires_redis
def test_sqlite_storage_matches_redis_lists(tmp_path, seed):
    """
    ``SQLiteStorage``: list operations return the same results as the same operations executed by Redis.
    """
    ops = _generate_ops(random.Random(seed), 300)

    async def testing():
        r = redis.asyncio.from_url("redis://localhost", encoding="utf-8", decode_responses=True)
        s = SQLiteStorage(os.path.join(tmp_path, "test.db"))
        try:
            await r.delete(_list_key)
            results_redis = await _apply_ops(r, ops)
            results_sqlite = await _apply_ops(s, ops)
            assert results_sqlite == results_redis
        finally:
            await r.delete(_list_key)
            await r.aclose()
            await s.aclose()

    asyncio.run(testing())


def test_sqlite_storage_kv(tmp_path):
    """
    ``SQLiteStorage``: key-value operations and persistence of data after the database is reopened.
    """
    db_path = os.path.join(tmp_path, "test.db")

    async def testing():
        s = SQLiteStorage(db_path)
        assert await s.ping() is True
        assert await s.get(_kv_key) is None
        assert await s.set(_kv_key, "abc") is True
        assert await s.get(_kv_key) == "abc"
        assert await s.set(_kv_key, "def") is True
        assert await s.get(_kv_key) == "def"
        assert await s.rpush(_list_key, "a", "b", "c") == 3
        await s.aclose()

        s = SQLiteStorage(db_path)
        assert await s.get(_kv_key) == "def"
        assert await s.lrange(_list_key, 0, -1) == ["a", "b", "c"]
        assert await s.delete(_kv_key, _list_key, "non_existing_key") == 2
        assert await s.get(_kv_key) is None
        assert await s.llen(_list_key) == 0
        await s.aclose()

        with pytest.raises(RuntimeError, match="closed"):
            await s.get(_kv_key)

    asyncio.run(testing())


def test_pq_sqlite_persistence(tmp_path):
    """
    ``PlanQueueOperations`` with SQLite storage: the queue, history, running item, queue mode and
    lock info are preserved after the storage is closed and reopened.
    """
    db_path = os.path.join(tmp_path, "test.db")
    lock_info = {"environment": True, "queue": False, "user": "abc"}

    async def testing():
        pq = PlanQueueOperations(name_prefix=_test_redis_name_prefix, sqlite_path=db_path)
        await pq.start()
        assert isinstance(pq._r_pool, SQLiteStorage)

        for name in ("a", "b", "c", "d"):
            await pq.add_item_to_queue({"item_type": "plan", "name": name})
        queue, _ = await pq.get_queue()
        await pq.move_item(uid=queue[3]["item_uid"], before_uid=queue[1]["item_uid"])
        await pq.add_item_to_queue({"item_type": "plan", "name": "e"}, after_uid=queue[0]["item_uid"])

        await pq.set_next_item_as_running()
        await pq.set_processed_item_as_completed(
            exit_status="completed", run_uids=[], scan_ids=[], err_msg="", err_tb=""
        )
        await pq.set_next_item_as_running()

        await pq.set_plan_queue_mode({"loop": True})
        await pq.lock_info_save(lock_info)

        queue, _ = await pq.get_queue()
        history, _ = await pq.get_history()
        running_item = pq.get_running_item_info()
        assert [_["name"] for _ in queue] == ["d", "b", "c"]
        assert [_["name"] for _ in history] == ["a"]
        assert running_item["name"] == "e"
        await pq.stop()

        pq = PlanQueueOperations(name_prefix=_test_redis_name_prefix, sqlite_path=db_path)
        await pq.start()
        assert (await pq.get_queue())[0] == queue
        assert (await pq.get_history())[0] == history
        assert pq.get_running_item_info() == running_item
        assert pq.plan_queue_mode["loop"] is True
        assert await pq.lock_info_retrieve() == lock_info
        await pq.stop()

    asyncio.run(testing())


def test_pq_sqlite_open_failure(tmp_path):
    """
    ``PlanQueueOperations`` with SQLite storage: failure to open the database.
    """
    db_path = os.path.join(tmp_path, "non_existing_dir", "test.db")

    async def testing():
        pq = PlanQueueOperations(name_prefix=_test_redis_name_prefix, sqlite_path=db_path)
        with pytest.raises(OSError, match="Failed to open SQLite database"):
            await pq.start()

    asyncio.run(testing())


def test_manager_sqlite_storage(re_manager_cmd, tmp_path):  # noqa: F811
    """
    RE Manager started with ``--storage-backend=sqlite``: the queue is preserved after the manager
    is restarted.
    """
    db_path = os.path.join(tmp_path, "qserver.db")
    params = ["--storage-backend=sqlite", f"--sqlite-path={db_path}"]
    re = re_manager_cmd(params)
    assert os.path.isfile(db_path)

    item = {"name": "count", "args": [["det1", "det2"]], "item_type": "plan"}
    resp1, _ = zmq_request("queue_item_add", {"item": item, "user": _user, "user_group": _user_group})
    assert resp1["success"] is True, resp1

    re.stop_manager(cleanup=False)
    re.start_manager(params=params, cleanup=False)
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    resp2, _ = zmq_request("queue_get")
    assert resp2["success"] is True
    assert len(resp2["items"]) == 1
    assert resp2["items"][0]["item_uid"] == resp1["item"]["item_uid"]


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="XDG_CACHE_HOME is only used on Linux")
def test_manager_sqlite_storage_default_path(re_manager_cmd, tmp_path, monkeypatch):  # noqa: F811
    """
    RE Manager started with ``--storage-backend=sqlite`` and no ``--sqlite-path``: the database
    is created in the user cache directory.
    """
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    db_path = os.path.join(tmp_path, "bluesky-queueserver", "qserver.sqlite")
    assert get_default_sqlite_path() == db_path

    re_manager_cmd(["--storage-backend=sqlite"])
    assert os.path.isfile(db_path)


@requires_redis
def test_manager_sqlite_path_ignored_with_redis(re_manager_cmd, tmp_path):  # noqa: F811
    """
    ``--sqlite-path`` is ignored if the Redis storage backend is used.
    """
    db_path = os.path.join(tmp_path, "qserver.db")
    re_manager_cmd(["--storage-backend=redis", f"--sqlite-path={db_path}"])
    assert not os.path.exists(db_path)
