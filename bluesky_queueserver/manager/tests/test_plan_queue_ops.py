import asyncio
import copy
import json
import pprint
import re
import time as ttime

import pytest

from bluesky_queueserver.manager.plan_queue_ops import PlanQueueOperations

from .common import _test_redis_name_prefix

errmsg_wrong_plan_type = "Parameter 'item' should be a dictionary"


class PQ:
    """
    The class that creates and initializes an instance of plan queue for testing and performs necessary
    cleanup afterwards. Intended for use with ``async with``.
    """

    def __init__(self):
        self._pq = None

    async def __aenter__(self):
        """
        Returns initialized instance of plan queue.
        """
        self._pq = PlanQueueOperations(name_prefix=_test_redis_name_prefix)
        await self._pq.start()
        # Clear any pool entries
        await self._pq.delete_pool_entries()
        await self._pq.user_group_permissions_clear()
        await self._pq.lock_info_clear()
        await self._pq.autostart_mode_clear()
        await self._pq.stop_pending_clear()
        return self._pq

    async def __aexit__(self, exc_t, exc_v, exc_tb):
        """
        Cleanup after test.
        """
        # Don't leave any test entries in the pool
        await self._pq.delete_pool_entries()
        await self._pq.user_group_permissions_clear()
        await self._pq.lock_info_clear()
        await self._pq.autostart_mode_clear()
        await self._pq.stop_pending_clear()


def test_pq_start_stop():
    """
    Test for the ``PlanQueueOperations.start()`` and ``PlanQueueOperations.stop()``
    """

    async def testing():
        pq = PlanQueueOperations(name_prefix=_test_redis_name_prefix)
        await pq.start()
        await pq._r_pool.ping()
        await pq.stop()
        assert pq._r_pool is None

        await pq.start()
        await pq._r_pool.ping()
        await pq.stop()
        assert pq._r_pool is None

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("item_in, item_out", [
    ({"name": "plan1", "item_uid": "abcde"}, {"name": "plan1", "item_uid": "abcde"}),
    ({"user": "user1", "user_group": "group1"}, {"user": "user1", "user_group": "group1"}),
    ({"args": [1, 2], "kwargs": {"a": 1, "b": 2}}, {"args": [1, 2], "kwargs": {"a": 1, "b": 2}}),
    ({"item_type": "plan", "meta": {"md1": 1, "md2": 2}}, {"item_type": "plan", "meta": {"md1": 1, "md2": 2}}),
    ({"name": "plan1", "result": {}}, {"name": "plan1"}),
])
# fmt: on
def test_filter_item_parameters(item_in, item_out):
    """
    Tests for ``filter_item_parameters``.
    """

    async def testing():
        async with PQ() as pq:
            item = pq.filter_item_parameters(item_in)
            assert item == item_out

    asyncio.run(testing())


def test_running_plan_info():
    """
    Basic test for the following methods:
    `PlanQueueOperations.is_item_running()`
    `PlanQueueOperations.get_running_item_info()`
    `PlanQueueOperations.delete_pool_entries()`
    """

    async def testing():
        async with PQ() as pq:
            assert await pq.get_running_item_info() == {}
            assert await pq.is_item_running() is False

            some_plan = {"some_key": "some_value"}
            await pq._set_running_item_info(some_plan)
            assert await pq.get_running_item_info() == some_plan

            assert await pq.is_item_running() is True

            await pq._clear_running_item_info()
            assert await pq.get_running_item_info() == {}
            assert await pq.is_item_running() is False

            await pq._set_running_item_info(some_plan)
            await pq.delete_pool_entries()
            assert await pq.get_running_item_info() == {}
            assert await pq.is_item_running() is False

    asyncio.run(testing())


def test_redis_name_prefix():
    """
    Test that the prefix is correctly appended to the name of the redis key (test with one key).
    """
    pq = PlanQueueOperations(name_prefix=_test_redis_name_prefix)
    assert pq._name_plan_queue == _test_redis_name_prefix + "_plan_queue"

    pq = PlanQueueOperations(name_prefix="")
    assert pq._name_plan_queue == "plan_queue"


# fmt: off
@pytest.mark.parametrize("plan_running, plans, result_running, result_plans", [
    ({"testing": 1}, [{"testing": 2}, {"item_uid": "ab", "name": "nm"}, {"testing": 2}],
     {}, [{"item_uid": "ab", "name": "nm"}]),
    ({"testing": 1}, [{"testing": 2}, {"item_uid": "ab", "name": "nm"}, {"testing": 3}],
     {}, [{"item_uid": "ab", "name": "nm"}]),
    ({"item_uid": "a"}, [{"item_uid": "a1"}, {"item_uid": "a2"}, {"item_uid": "a3"}],
     {"item_uid": "a"}, [{"item_uid": "a1"}, {"item_uid": "a2"}, {"item_uid": "a3"}]),
])
# fmt: on
def test_queue_clean(plan_running, plans, result_running, result_plans):
    """
    Test for ``_queue_clean()`` method
    """

    async def testing():
        async with PQ() as pq:
            await pq._set_running_item_info(plan_running)
            for plan in plans:
                await pq._r_pool.rpush(pq._name_plan_queue, json.dumps(plan))

            assert await pq.get_running_item_info() == plan_running
            plan_queue, _ = await pq.get_queue()
            assert plan_queue == plans

            await pq._queue_clean()

            assert await pq.get_running_item_info() == result_running
            plan_queue, _ = await pq.get_queue()
            assert plan_queue == result_plans

    asyncio.run(testing())


def test_set_plan_queue_mode_1():
    """
    ``set_plan_queue_mode``: basic functionality.
    """

    async def testing():
        async with PQ() as pq:
            # Initially plan queue mode must be default queue mode
            assert pq.plan_queue_mode == pq.plan_queue_mode_default
            assert pq.plan_queue_mode["loop"] is False
            assert pq.plan_queue_mode["ignore_failures"] is False

            # The properties are expecte to return copies
            assert pq.plan_queue_mode is not pq._plan_queue_mode
            assert pq.plan_queue_mode_default is not pq._plan_queue_mode_default

            mode_expected = pq.plan_queue_mode
            assert mode_expected == dict(loop=False, ignore_failures=False)

            mode_new = {"loop": True}
            mode_expected.update(mode_new)
            await pq.set_plan_queue_mode(mode_new, update=True)
            assert pq._plan_queue_mode is not mode_new  # Verify that 'set' operation performs copy
            assert pq.plan_queue_mode == mode_expected

            mode_new = {"ignore_failures": True}
            mode_expected.update(mode_new)
            await pq.set_plan_queue_mode(mode_new, update=True)
            assert pq._plan_queue_mode is not mode_new  # Verify that 'set' operation performs copy
            assert pq.plan_queue_mode == mode_expected

            mode_new = {"loop": False, "ignore_failures": False}
            mode_expected.update(mode_new)
            await pq.set_plan_queue_mode(mode_new, update=False)
            assert pq._plan_queue_mode is not mode_new  # Verify that 'set' operation performs copy
            assert pq.plan_queue_mode == mode_expected

            for update in (False, True):
                print(f"update={update}")
                with pytest.raises(ValueError, match="Unsupported plan queue mode parameter 'nonexisting_key'"):
                    await pq.set_plan_queue_mode({"nonexisting_key": True}, update=update)

            await pq.set_plan_queue_mode({}, update=True)
            assert pq.plan_queue_mode == mode_expected

            with pytest.raises(ValueError, match="Parameters .* are missing"):
                await pq.set_plan_queue_mode({}, update=False)

            mode_new = {"loop": True}
            with pytest.raises(ValueError, match="Parameters {'ignore_failures'} are missing"):
                await pq.set_plan_queue_mode(mode_new, update=False)

            with pytest.raises(TypeError, match="Unsupported type .* of the parameter 'loop'"):
                await pq.set_plan_queue_mode({"loop": 10}, update=True)

            with pytest.raises(TypeError, match="Unsupported type .* of the parameter 'ignore_failures'"):
                await pq.set_plan_queue_mode({"ignore_failures": []}, update=True)

            assert pq.plan_queue_mode == mode_expected

            # Verify that the queue mode is saved to Redis
            queue_mode = {"loop": True, "ignore_failures": True}
            await pq.set_plan_queue_mode(queue_mode, update=False)

            pq2 = PlanQueueOperations(name_prefix=_test_redis_name_prefix)
            await pq2.start()
            assert pq2.plan_queue_mode == queue_mode

            # Set queue mode to default
            assert pq.plan_queue_mode != pq.plan_queue_mode_default
            await pq.set_plan_queue_mode("default", update=update)
            assert pq.plan_queue_mode == pq.plan_queue_mode_default

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("plan, result",
                         [({"a": 10}, True),
                          ([10, 20], False),
                          (50, False),
                          ("abc", False)])
# fmt: on
def test_verify_item_type(plan, result):
    async def testing():
        async with PQ() as pq:
            if result:
                pq._verify_item_type(plan)
            else:
                with pytest.raises(TypeError, match=errmsg_wrong_plan_type):
                    pq._verify_item_type(plan)

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize(
    "plan, f_kwargs, result, errmsg",
    [({"a": 10}, {}, False, "Item does not have UID"),
     ([10, 20], {}, False, errmsg_wrong_plan_type),
     ({"item_uid": "one"}, {}, True, ""),
     ({"item_uid": "two"}, {}, False, "Item with UID .+ is already in the queue"),
     ({"item_uid": "three"}, {}, False, "Item with UID .+ is already in the queue"),
     ({"item_uid": "two"}, {"ignore_uids": None}, False, "Item with UID .+ is already in the queue"),
     ({"item_uid": "two"}, {"ignore_uids": ["two"]}, True, ""),
     ({"item_uid": "two"}, {"ignore_uids": ["two", "three"]}, True, ""),
     ({"item_uid": "two"}, {"ignore_uids": ["one", "three"]}, False, "Item with UID .+ is already in the queue"),
     ])
# fmt: on
def test_verify_item(plan, f_kwargs, result, errmsg):
    """
    Tests for method ``_verify_item()``.
    """
    # Set two existiing plans and then set one of them as running
    existing_plans = [{"item_type": "plan", "item_uid": "two"}, {"item_type": "plan", "item_uid": "three"}]

    async def testing():
        async with PQ() as pq:

            async def set_plans():
                # Add plan to queue
                for plan in existing_plans:
                    await pq.add_item_to_queue(plan)
                # Set one plan as currently running
                await pq.set_next_item_as_running()

                # Verify that setup is correct
                assert await pq.is_item_running() is True
                assert await pq.get_queue_size() == 1

            await set_plans()

            if result:
                pq._verify_item(plan, **f_kwargs)
            else:
                with pytest.raises(Exception, match=errmsg):
                    pq._verify_item(plan, **f_kwargs)

    asyncio.run(testing())


def test_new_item_uid():
    """
    Smoke test for the method ``new_item_uid()``.
    """
    assert isinstance(PlanQueueOperations.new_item_uid(), str)


# fmt: off
@pytest.mark.parametrize("plan", [
    {"name": "a"},
    {"item_uid": "some_uid", "name": "a"},
])
# fmt: on
def test_set_new_item_uuid(plan):
    """
    Basic test for the method ``set_new_item_uuid()``.
    """

    async def testing():
        async with PQ() as pq:
            uid = plan.get("item_uid", None)

            # The function is supposed to create or replace UID
            new_plan = pq.set_new_item_uuid(plan)

            assert "item_uid" in new_plan
            assert isinstance(new_plan["item_uid"], str)
            assert new_plan["item_uid"] != uid

    asyncio.run(testing())


def test_get_index_by_uid_1():
    """
    Test for ``_get_index_by_uid()``
    """
    plans = [
        {"item_uid": "a", "name": "name_a"},
        {"item_uid": "b", "name": "name_b"},
        {"item_uid": "c", "name": "name_c"},
    ]

    async def testing():
        async with PQ() as pq:
            for plan in plans:
                await pq.add_item_to_queue(plan)

            assert await pq._get_index_by_uid(uid="b") == 1

            with pytest.raises(IndexError, match="No plan with UID 'nonexistent'"):
                assert await pq._get_index_by_uid(uid="nonexistent")

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("sequence, indices_expected", [
    (["a", "b", "c"], [0, 1, 2]),
    (["a", "b"], [0, 1]),
    (["c", "b"], [2, 1]),
    (["c", "d", "b"], [2, -1, 1]),
])
# fmt: on
def test_get_index_by_uid_batch_1(sequence, indices_expected):
    """
    Test for ``_get_index_by_uid_batch()``
    """
    plans = [
        {"item_uid": "a", "name": "name_a"},
        {"item_uid": "b", "name": "name_b"},
        {"item_uid": "c", "name": "name_c"},
    ]

    async def testing():
        async with PQ() as pq:
            for plan in plans:
                await pq.add_item_to_queue(plan)

            indices = await pq._get_index_by_uid_batch(uids=sequence)
            assert indices == indices_expected

    asyncio.run(testing())


def test_uid_dict_1():
    """
    Basic test for functions associated with `_uid_dict`
    """
    plan_a = {"item_uid": "a", "name": "name_a"}
    plan_b = {"item_uid": "b", "name": "name_b"}
    plan_c = {"item_uid": "c", "name": "name_c"}

    plan_b_updated = {"item_uid": "b", "name": "name_b_updated"}

    async def testing():
        async with PQ() as pq:
            pq._uid_dict_add(plan_a)
            pq._uid_dict_add(plan_b)

            assert pq._is_uid_in_dict(plan_a["item_uid"]) is True
            assert pq._is_uid_in_dict(plan_b["item_uid"]) is True
            assert pq._is_uid_in_dict(plan_c["item_uid"]) is False

            assert pq._uid_dict_get_item(plan_b["item_uid"]) == plan_b
            pq._uid_dict_update(plan_b_updated)
            assert pq._uid_dict_get_item(plan_b["item_uid"]) == plan_b_updated

            pq._uid_dict_remove(plan_a["item_uid"])
            assert pq._is_uid_in_dict(plan_a["item_uid"]) is False
            assert pq._is_uid_in_dict(plan_b["item_uid"]) is True

            pq._uid_dict_clear()
            assert pq._is_uid_in_dict(plan_a["item_uid"]) is False
            assert pq._is_uid_in_dict(plan_b["item_uid"]) is False

    asyncio.run(testing())


def test_uid_dict_2():
    """
    Test if functions changing `pq._uid_dict` are also updating `pq.plan_queue_uid`.
    """
    plan_a = {"item_uid": "a", "name": "name_a"}
    plan_a_updated = {"item_uid": "a", "name": "name_a_updated"}

    async def testing():
        async with PQ() as pq:
            pq_uid = pq.plan_queue_uid

            pq._uid_dict_add(plan_a)

            assert pq.plan_queue_uid != pq_uid
            pq_uid = pq.plan_queue_uid

            pq._uid_dict_update(plan_a_updated)

            assert pq.plan_queue_uid != pq_uid
            pq_uid = pq.plan_queue_uid

            pq._uid_dict_remove(plan_a_updated["item_uid"])

            assert pq.plan_queue_uid != pq_uid
            pq_uid = pq.plan_queue_uid

            pq._uid_dict_clear()

            assert pq.plan_queue_uid != pq_uid

    asyncio.run(testing())


def test_uid_dict_3_initialize():
    """
    Basic test for functions associated with ``_uid_dict_initialize()``
    """

    async def testing():
        async with PQ() as pq:
            await pq.add_item_to_queue({"name": "a"})
            await pq.add_item_to_queue({"name": "b"})
            await pq.add_item_to_queue({"name": "c"})
            plans, _ = await pq.get_queue()
            uid_dict = {_["item_uid"]: _ for _ in plans}

            pq_uid = pq.plan_queue_uid

            await pq._uid_dict_initialize()

            assert pq._uid_dict == uid_dict
            assert pq.plan_queue_uid != pq_uid

    asyncio.run(testing())


def test_uid_dict_4_failing():
    """
    Failing cases for functions associated with `_uid_dict`
    """
    plan_a = {"item_uid": "a", "name": "name_a"}
    plan_b = {"item_uid": "b", "name": "name_b"}
    plan_c = {"item_uid": "c", "name": "name_c"}

    async def testing():
        async with PQ() as pq:
            pq._uid_dict_add(plan_a)
            pq._uid_dict_add(plan_b)

            # Add plan with UID that already exists
            pq_uid = pq.plan_queue_uid
            with pytest.raises(RuntimeError, match=f"'{plan_a['item_uid']}', which is already in the queue"):
                pq._uid_dict_add(plan_a)
            assert pq.plan_queue_uid == pq_uid

            assert len(pq._uid_dict) == 2

            # Remove plan with UID does not exist exists
            with pytest.raises(RuntimeError, match=f"'{plan_c['item_uid']}', which is not in the queue"):
                pq._uid_dict_remove(plan_c["item_uid"])
            assert pq.plan_queue_uid == pq_uid

            assert len(pq._uid_dict) == 2

            # Update plan with UID does not exist exists
            with pytest.raises(RuntimeError, match=f"'{plan_c['item_uid']}', which is not in the queue"):
                pq._uid_dict_update(plan_c)
            assert pq.plan_queue_uid == pq_uid

            assert len(pq._uid_dict) == 2

    asyncio.run(testing())


def test_remove_item():
    """
    Basic test for functions associated with ``_remove_plan()``
    """

    async def testing():
        async with PQ() as pq:
            plan_list = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
            for plan in plan_list:
                await pq.add_item_to_queue(plan)

            plans, _ = await pq.get_queue()
            plan_to_remove = [_ for _ in plans if _["name"] == "b"][0]

            # Remove one plan
            await pq._remove_item(plan_to_remove)
            plans, _ = await pq.get_queue()
            assert len(plans) == 2

            # Add a copy of a plan (queue is not supposed to have copies in real life)
            plan_to_add = plans[0]
            await pq._r_pool.lpush(pq._name_plan_queue, json.dumps(plan_to_add))
            # Now remove both plans
            await pq._remove_item(plan_to_add, single=False)  # Allow deleting multiple or no plans
            assert await pq.get_queue_size() == 1

            # Delete the plan again (the plan is not in the queue, but it shouldn't raise an exception)
            await pq._remove_item(plan_to_add, single=False)  # Allow deleting multiple or no plans
            assert await pq.get_queue_size() == 1

            with pytest.raises(RuntimeError, match="One item is expected"):
                await pq._remove_item(plan_to_add)
            assert await pq.get_queue_size() == 1

            # Now add 'plan_to_add' twice (create two copies)
            await pq._r_pool.lpush(pq._name_plan_queue, json.dumps(plan_to_add))
            await pq._r_pool.lpush(pq._name_plan_queue, json.dumps(plan_to_add))
            assert await pq.get_queue_size() == 3
            # Attempt to delete two copies
            with pytest.raises(RuntimeError, match="One item is expected"):
                await pq._remove_item(plan_to_add)
            # Exception is raised, but both copies are deleted
            assert await pq.get_queue_size() == 1

    asyncio.run(testing())


def test_get_queue_full_1():
    """
    Basic test for the functions ``PlanQueueOperations.get_queue()`` and
    ``PlanQueueOperations.get_queue_full()``
    """

    async def testing():
        async with PQ() as pq:
            plans = [
                {"item_type": "plan", "item_uid": "one", "name": "a"},
                {"item_type": "plan", "item_uid": "two", "name": "b"},
                {"item_type": "plan", "item_uid": "three", "name": "c"},
            ]

            for p in plans:
                await pq.add_item_to_queue(p)
            await pq.set_next_item_as_running()

            pq_uid = pq.plan_queue_uid
            queue1, uid1 = await pq.get_queue()
            running_item1 = await pq.get_running_item_info()
            queue2, running_item2, uid2 = await pq.get_queue_full()

            assert queue1 == plans[1:]
            assert queue2 == plans[1:]
            assert {k: v for k, v in running_item1.items() if k != "properties"} == plans[0]
            assert running_item2 == running_item1

            assert isinstance(running_item1["properties"]["time_start"], float)
            assert ttime.time() - running_item1["properties"]["time_start"] < 10

            assert uid1 == pq_uid
            assert uid2 == pq_uid

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("params, name", [
    ({"pos": "front"}, "a"),
    ({"pos": "back"}, "c"),
    ({"pos": 0}, "a"),
    ({"pos": 1}, "b"),
    ({"pos": 2}, "c"),
    ({"pos": 3}, None),  # Index out of range
    ({"pos": -1}, "c"),
    ({"pos": -2}, "b"),
    ({"pos": -3}, "a"),
    ({"pos": -4}, None),  # Index out of range
    ({"uid": "one"}, "a"),
    ({"uid": "two"}, "b"),
    ({"uid": "nonexistent"}, None),
])
# fmt: on
def test_get_item_1(params, name):
    """
    Basic test for the function ``PlanQueueOperations.get_item()``
    """

    async def testing():
        async with PQ() as pq:
            pq_uid = pq.plan_queue_uid
            await pq.add_item_to_queue({"item_uid": "one", "name": "a"})
            await pq.add_item_to_queue({"item_uid": "two", "name": "b"})
            await pq.add_item_to_queue({"item_uid": "three", "name": "c"})
            assert await pq.get_queue_size() == 3
            assert pq.plan_queue_uid != pq_uid

            if name is not None:
                plan = await pq.get_item(**params)
                assert plan["name"] == name
            else:
                msg = "Index .* is out of range" if "pos" in params else "is not in the queue"
                with pytest.raises(IndexError, match=msg):
                    await pq.get_item(**params)

    asyncio.run(testing())


def test_get_item_2_fail():
    """
    Basic test for the function ``PlanQueueOperations.get_item()``.
    Attempt to retrieve a running plan.
    """

    async def testing():
        async with PQ() as pq:
            await pq.add_item_to_queue({"item_type": "plan", "item_uid": "one", "name": "a"})
            await pq.add_item_to_queue({"item_type": "plan", "item_uid": "two", "name": "b"})
            await pq.add_item_to_queue({"item_type": "plan", "item_uid": "three", "name": "c"})
            assert await pq.get_queue_size() == 3

            pq_uid = pq.plan_queue_uid
            await pq.set_next_item_as_running()
            assert await pq.get_queue_size() == 2
            assert pq.plan_queue_uid != pq_uid

            with pytest.raises(IndexError, match="is currently running"):
                await pq.get_item(uid="one")

            # Ambiguous parameters (position and UID is passed)
            with pytest.raises(ValueError, match="Ambiguous parameters"):
                await pq.get_item(pos=5, uid="abc")

    asyncio.run(testing())


def test_add_item_to_queue_1():
    """
    Basic test for the function ``PlanQueueOperations.add_item_to_queue()``
    """

    async def testing():
        async with PQ() as pq:

            async def add_plan(plan, n, **kwargs):
                plan_added, qsize = await pq.add_item_to_queue(plan, **kwargs)
                assert plan_added["name"] == plan["name"], f"plan: {plan}"
                assert qsize == n, f"plan: {plan}"

            await add_plan({"name": "a"}, 1)
            await add_plan({"name": "b"}, 2)
            await add_plan({"name": "c"}, 3, pos="back")
            await add_plan({"name": "d"}, 4, pos="front")
            await add_plan({"name": "e"}, 5, pos=0)  # front
            await add_plan({"name": "f"}, 6, pos=5)  # back (index == queue size)
            await add_plan({"name": "g"}, 7, pos=5)  # previous to last
            await add_plan({"name": "h"}, 8, pos=-1)  # last
            await add_plan({"name": "i"}, 9, pos=3)  # arbitrary index
            await add_plan({"name": "j"}, 10, pos=100)  # back (index some large number)
            await add_plan({"name": "k"}, 11, pos=-11)  # front (precisely)
            await add_plan({"name": "l"}, 12, pos=-11)  # 2nd element
            await add_plan({"name": "m"}, 13, pos=-100)  # front (index some large negative number)
            await add_plan({"name": "n"}, 14, pos=-2)  # previous to lasst

            assert await pq.get_queue_size() == 14

            plans, _ = await pq.get_queue()
            name_sequence = [_["name"] for _ in plans]
            assert name_sequence == ["m", "k", "l", "e", "d", "a", "i", "b", "c", "g", "f", "h", "n", "j"]

            await pq.clear_queue()

    asyncio.run(testing())


def test_add_item_to_queue_2():
    """
    Basic test for the function ``PlanQueueOperations.add_item_to_queue()``
    """

    async def testing():
        async with PQ() as pq:

            async def add_plan(plan, n, **kwargs):
                plan_added, qsize = await pq.add_item_to_queue(plan, **kwargs)
                assert plan_added["name"] == plan["name"], f"plan: {plan}"
                assert qsize == n, f"plan: {plan}"

            await add_plan({"item_type": "plan", "name": "a"}, 1)
            await add_plan({"item_type": "plan", "name": "b"}, 2)
            await add_plan({"item_type": "plan", "name": "c"}, 3, pos="back")

            plan_queue, _ = await pq.get_queue()
            displaced_uid = plan_queue[1]["item_uid"]

            await add_plan({"item_type": "plan", "name": "d"}, 4, before_uid=displaced_uid)
            await add_plan({"item_type": "plan", "name": "e"}, 5, after_uid=displaced_uid)

            # This reduces the number of elements in the queue by one
            await pq.set_next_item_as_running()

            displaced_uid = plan_queue[0]["item_uid"]
            await add_plan({"name": "f"}, 5, after_uid=displaced_uid)

            with pytest.raises(
                IndexError, match="Can not insert a plan in the queue before a currently running plan"
            ):
                await add_plan({"name": "g"}, 5, before_uid=displaced_uid)

            with pytest.raises(IndexError, match="is not in the queue"):
                await add_plan({"name": "h"}, 5, before_uid="nonexistent_uid")

            assert await pq.get_queue_size() == 5

            plans, _ = await pq.get_queue()
            name_sequence = [_["name"] for _ in plans]
            assert name_sequence == ["f", "d", "b", "e", "c"]

            await pq.clear_queue()

    asyncio.run(testing())


@pytest.mark.parametrize("filter_params", [False, True])
def test_add_item_to_queue_3(filter_params):
    """
    Test if parameter filtering works as expected with `add_item_to_queue` function.
    """

    async def testing():
        async with PQ() as pq:
            # Parameter 'result' should be removed if filtering is enabled
            plan1 = {"item_type": "plan", "name": "a", "item_uid": "1"}
            plan2 = plan1.copy()
            plan2["result"] = {}

            plan_added, qsize = await pq.add_item_to_queue(plan2, filter_parameters=filter_params)

            if filter_params:
                assert plan_added == plan1
            else:
                assert plan_added == plan2

    asyncio.run(testing())


def test_add_item_to_queue_4_fail():
    """
    Failing tests for the function ``PlanQueueOperations.add_item_to_queue()``
    """

    async def testing():
        async with PQ() as pq:
            pq_uid = pq.plan_queue_uid
            with pytest.raises(ValueError, match="Parameter 'pos' has incorrect value"):
                await pq.add_item_to_queue({"name": "a"}, pos="something")
            assert pq.plan_queue_uid == pq_uid

            with pytest.raises(TypeError, match=errmsg_wrong_plan_type):
                await pq.add_item_to_queue("plan_is_not_string")
            assert pq.plan_queue_uid == pq_uid

            # Duplicate plan UID
            plan = {"item_uid": "abc", "name": "a"}
            await pq.add_item_to_queue(plan)
            pq_uid = pq.plan_queue_uid
            with pytest.raises(RuntimeError, match="Item with UID .+ is already in the queue"):
                await pq.add_item_to_queue(plan)
            assert pq.plan_queue_uid == pq_uid

            # Ambiguous parameters (position and UID is passed)
            with pytest.raises(ValueError, match="Ambiguous parameters"):
                await pq.add_item_to_queue({"name": "abc"}, pos=5, before_uid="abc")
            assert pq.plan_queue_uid == pq_uid

            # Ambiguous parameters ('before_uid' and 'after_uid' is specified)
            with pytest.raises(ValueError, match="Ambiguous parameters"):
                await pq.add_item_to_queue({"name": "abc"}, before_uid="abc", after_uid="abc")
            assert pq.plan_queue_uid == pq_uid

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("batch_params, queue_seq, batch_seq, expected_seq", [
    ({}, "", "", ""),  # Add an empty batch
    ({}, "", "567", "567"),
    ({"pos": "front"}, "", "567", "567"),
    ({"pos": "back"}, "", "567", "567"),
    ({}, "1234", "567", "1234567"),
    ({"pos": "front"}, "1234", "567", "5671234"),
    ({"pos": "back"}, "1234", "567", "1234567"),
    ({"pos": 0}, "1234", "567", "5671234"),
    ({"pos": 1}, "1234", "567", "1567234"),
    ({"pos": 2}, "1234", "567", "1256734"),
    ({"pos": 4}, "1234", "567", "1234567"),
    ({"pos": 100}, "1234", "567", "1234567"),
    ({"pos": -1}, "1234", "567", "1234567"),
    ({"pos": -2}, "1234", "567", "1235674"),
    ({"pos": -3}, "1234", "567", "1256734"),
    ({"pos": -4}, "1234", "567", "1567234"),
    ({"pos": -5}, "1234", "567", "5671234"),
    ({"pos": -100}, "1234", "567", "5671234"),
    ({"before_uid": "1"}, "1234", "567", "5671234"),
    ({"before_uid": "2"}, "1234", "567", "1567234"),
    ({"before_uid": "3"}, "1234", "567", "1256734"),
    ({"after_uid": "1"}, "1234", "567", "1567234"),
    ({"after_uid": "2"}, "1234", "567", "1256734"),
    ({"after_uid": "4"}, "1234", "567", "1234567"),
])
# fmt: on
def test_add_batch_to_queue_1(batch_params, queue_seq, batch_seq, expected_seq):
    """
    Basic test for the function ``PlanQueueOperations.add_batch_to_queue()``
    """

    async def testing():
        async with PQ() as pq:

            async def add_plan(plan, n, **kwargs):
                plan_added, qsize = await pq.add_item_to_queue(plan, **kwargs)
                assert plan_added["name"] == plan["name"], f"plan: {plan}"
                assert qsize == n, f"plan: {plan}"

            # Create the queue with plans
            for n, p_name in enumerate(queue_seq):
                await add_plan({"name": p_name, "item_uid": f"{p_name}{p_name}"}, n + 1)

            def fix_uid(uid):
                return f"{uid}{uid}"

            if "before_uid" in batch_params:
                batch_params["before_uid"] = fix_uid(batch_params["before_uid"])
            if "after_uid" in batch_params:
                batch_params["after_uid"] = fix_uid(batch_params["after_uid"])

            items = []
            for p_name in batch_seq:
                items.append({"name": p_name, "item_uid": p_name + p_name})
            items_added, results, qsize, success = await pq.add_batch_to_queue(items, **batch_params)
            assert success is True, pprint.pformat(results)
            assert qsize == len(queue_seq) + len(batch_seq)
            assert await pq.get_queue_size() == len(queue_seq) + len(batch_seq)
            assert len(items_added) == len(items)
            assert len(results) == len(items)

            # Verify that the results are set correctly (success)
            for res in results:
                assert res["success"] is True, pprint.pformat(results)
                assert res["msg"] == "", pprint.pformat(results)

            # Verify the sequence of items in the queue
            queue, _ = await pq.get_queue()
            queue_sequence = [_["name"] for _ in queue]
            queue_sequence = "".join(queue_sequence)
            assert queue_sequence == expected_seq

            await pq.clear_queue()

    asyncio.run(testing())


@pytest.mark.parametrize("filter_params", [False, True, None])
def test_add_batch_to_queue_2(filter_params):
    """
    Test if parameter filtering works as expected with `add_batch_to_queue` function.
    """

    async def testing():
        async with PQ() as pq:
            # Parameter 'result' should be removed if filtering is enabled.
            params = {"filter_parameters": filter_params} if (filter_params is not None) else {}
            do_filtering = True if (filter_params is None) else filter_params

            items = [{"name": _, "result": {}} for _ in ("a", "b", "c")]
            items_added, results, qsize, success = await pq.add_batch_to_queue(items, **params)
            assert success is True
            assert qsize == len(items)
            assert len(items_added) == len(items)
            assert len(results) == len(items)

            queue, _ = await pq.get_queue()

            assert [_["name"] for _ in queue] == [_["name"] for _ in items]
            for queue_item in queue:
                if do_filtering:
                    assert "result" not in queue_item, pprint.pformat(queue_item)
                else:
                    assert "result" in queue_item, pprint.pformat(queue_item)

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("params, queue_seq, batch_seq, err_msgs", [
    ({}, "abcd", "eaf", ["", "Item with UID .+ is already in the queue", ""]),
    ({}, "abcd", "ead", [""] + ["Item with UID .+ is already in the queue"] * 2),
    ({"before_uid": "unknown"}, "abcd", "efg", ["Plan with UID .* is not in the queue"] * 3),
    ({"after_uid": "unknown"}, "abcd", "efg", ["Plan with UID .* is not in the queue"] * 3),
    ({"before_uid": "unknown", "after_uid": "unknown"}, "abcd", "efg", ["Ambiguous parameters"] * 3),
    ({"pos": "front", "after_uid": "unknown"}, "abcd", "efg", ["Ambiguous parameters"] * 3),
])
# fmt: on
def test_add_batch_to_queue_3_fail(params, queue_seq, batch_seq, err_msgs):
    """
    Failing cases for the function ``PlanQueueOperations.add_batch_to_queue()``
    """

    async def testing():
        async with PQ() as pq:

            async def add_plan(plan, n, **kwargs):
                plan_added, qsize = await pq.add_item_to_queue(plan, **kwargs)
                assert plan_added["name"] == plan["name"], f"plan: {plan}"
                assert qsize == n, f"plan: {plan}"

            # Create the queue with plans
            for n, p_name in enumerate(queue_seq):
                await add_plan({"name": p_name, "item_uid": p_name + p_name}, n + 1)

            items = []
            for p_name in batch_seq:
                items.append({"name": p_name, "item_uid": p_name + p_name})
            items_added, results, qsize, success = await pq.add_batch_to_queue(items, **params)

            assert success is False
            assert qsize == len(queue_seq)
            assert len(items_added) == len(items)
            assert len(results) == len(items)

            # The returned item list is expected to be identical to the original list
            assert items_added == items, pprint.pformat(items_added)

            for res, msg in zip(results, err_msgs):
                if not msg:
                    assert res["success"] is True, pprint.pformat(res)
                    assert res["msg"] == "", pprint.pformat(res)
                else:
                    assert res["success"] is False, pprint.pformat(res)
                    assert re.search(msg, res["msg"]), pprint.pformat(res)

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("replace_uid", [False, True])
# fmt: on
def test_replace_item_1(replace_uid):
    """
    Basic functionality of ``PlanQueueOperations.replace_item()`` function.
    """

    async def testing():
        async with PQ() as pq:
            plans = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
            plans_added = [None] * len(plans)
            qsizes = [None] * len(plans)

            for n, plan in enumerate(plans):
                plans_added[n], qsizes[n] = await pq.add_item_to_queue(plan)

            assert qsizes == [1, 2, 3]
            assert await pq.get_queue_size() == 3

            # Change name, but keep UID
            plan_names_new = ["d", "e", "f"]
            for n in range(len(plans_added)):
                plan = plans_added[n].copy()
                plan["name"] = plan_names_new[n]

                uid_to_replace = plan["item_uid"]
                if replace_uid:
                    plan["item_uid"] = pq.new_item_uid()  # Generate new UID

                pq_uid = pq.plan_queue_uid
                plan_new, qsize = await pq.replace_item(plan, item_uid=uid_to_replace)
                assert plan_new["name"] == plan["name"]
                assert plan_new["item_uid"] == plan["item_uid"]
                assert pq.plan_queue_uid != pq_uid

                assert await pq.get_queue_size() == 3
                plans_added[n] = plan_new

                # Make sure that the plan can be correctly extracted by uid
                assert await pq.get_item(uid=plan["item_uid"]) == plan
                assert await pq.get_queue_size() == 3

                # Initialize '_uid_dict' and see if the plan can still be extracted using correct UID.
                await pq._uid_dict_initialize()
                assert await pq.get_item(uid=plan["item_uid"]) == plan
                assert await pq.get_queue_size() == 3

    asyncio.run(testing())


def test_replace_item_2():
    """
    ``PlanQueueOperations.replace_item()`` function: not UID in the plan - random UID is assigned.
    """

    async def testing():
        async with PQ() as pq:
            plans = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
            plans_added = [None] * len(plans)
            qsizes = [None] * len(plans)

            for n, plan in enumerate(plans):
                plans_added[n], qsizes[n] = await pq.add_item_to_queue(plan)

            assert qsizes == [1, 2, 3]
            assert await pq.get_queue_size() == 3

            new_name = "h"
            plan_new = {"name": new_name}  # No UID in the plan. It should still be inserted
            pq_uid = pq.plan_queue_uid
            plan_replaced, qsize = await pq.replace_item(plan_new, item_uid=plans_added[1]["item_uid"])
            assert pq.plan_queue_uid != pq_uid

            new_uid = plan_replaced["item_uid"]
            assert new_uid != plans_added[1]["item_uid"]
            assert plan_replaced["name"] == plan_new["name"]

            plan = await pq.get_item(uid=new_uid)
            assert plan["item_uid"] == new_uid
            assert plan["name"] == new_name

            # Initialize '_uid_dict' and see if the plan can still be extracted using correct UID.
            await pq._uid_dict_initialize()
            plan = await pq.get_item(uid=new_uid)
            assert plan["item_uid"] == new_uid
            assert plan["name"] == new_name

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("mode", ["exact_copy", "change_uid", "change_plan"])
# fmt: on
def test_replace_item_3(mode):
    """
    ``PlanQueueOperations.replace_item()`` function: make sure that filtering
      is applied to the parameters of edited plan if the plan was changed
    """

    async def testing():
        async with PQ() as pq:
            plans = [{"name": "a", "result": {}}, {"name": "b"}, {"name": "c"}]

            for n, plan in enumerate(plans):
                await pq.add_item_to_queue(plan, filter_parameters=False)

            assert await pq.get_queue_size() == 3

            queue, _ = await pq.get_queue()
            plan0_before = queue[0]
            assert "result" in plan0_before

            if mode == "exact_copy":
                # Replace the plan with the exact copy of it. Filtering is not be applied.
                plan_new = plan0_before
                plan_replaced, qsize = await pq.replace_item(plan_new, item_uid=plan_new["item_uid"])

                queue, _ = await pq.get_queue()
                plan0_after = queue[0]

                assert plan_replaced == plan0_after
                assert "result" in plan_replaced, pprint.pformat(plan_replaced)

            elif mode == "change_uid":
                # Modify plan UID. Filtering is applied
                plan_new = plan0_before
                item_uid_to_replace = plan_new["item_uid"]
                plan_new = pq.set_new_item_uuid(plan_new)
                plan_replaced, qsize = await pq.replace_item(plan_new, item_uid=item_uid_to_replace)

                queue, _ = await pq.get_queue()
                plan0_after = queue[0]

                assert plan_replaced == plan0_after
                assert "result" not in plan_replaced, pprint.pformat(plan_replaced)

            elif mode == "change_plan":
                # Modify plan, keep UID the same. Filtering is applied
                plan_new = plan0_before
                new_name = "h"
                plan_new["name"] = new_name
                plan_replaced, qsize = await pq.replace_item(plan_new, item_uid=plan_new["item_uid"])

                queue, _ = await pq.get_queue()
                plan0_after = queue[0]

                assert plan_replaced == plan0_after
                assert "result" not in plan_replaced, pprint.pformat(plan_replaced)

            else:
                raise Exception(f"Test mode '{mode}' does not exist.")

    asyncio.run(testing())


def test_replace_item_4_failing():
    """
    ``PlanQueueOperations.replace_item()`` - failing cases
    """

    async def testing():
        async with PQ() as pq:
            plans = [
                {"item_type": "plan", "name": "a"},
                {"item_type": "plan", "name": "b"},
                {"item_type": "plan", "name": "c"},
            ]
            plans_added = [None] * len(plans)
            qsizes = [None] * len(plans)

            for n, plan in enumerate(plans):
                plans_added[n], qsizes[n] = await pq.add_item_to_queue(plan)

            assert qsizes == [1, 2, 3]
            assert await pq.get_queue_size() == 3

            # Set the first item as 'running'
            running_plan = await pq.set_next_item_as_running()
            assert {k: v for k, v in running_plan.items() if k != "properties"} == plans_added[0]

            queue, _ = await pq.get_queue()
            running_item_info = await pq.get_running_item_info()

            pq_uid = pq.plan_queue_uid

            plan_new = {"name": "h"}  # No UID in the plan. It should still be inserted
            # Case: attempt to replace a plan that is currently running
            with pytest.raises(
                RuntimeError, match="Failed to replace item: Item with UID .* is currently running"
            ):
                await pq.replace_item(plan_new, item_uid=running_plan["item_uid"])
            assert pq.plan_queue_uid == pq_uid

            # Case: attempt to replace a plan that is not in queue
            with pytest.raises(RuntimeError, match="Failed to replace item: Item with UID .* is not in the queue"):
                await pq.replace_item(plan_new, item_uid="uid-that-does-not-exist")
            assert pq.plan_queue_uid == pq_uid

            # Case: attempt to replace a plan with another plan that already exists in the queue
            plan = plans_added[1]
            with pytest.raises(RuntimeError, match="Item with UID .* is already in the queue"):
                await pq.replace_item(plan, item_uid=plans_added[2]["item_uid"])
            assert pq.plan_queue_uid == pq_uid

            # Case: attempt to replace a plan with currently running plan
            with pytest.raises(RuntimeError, match="Item with UID .* is already in the queue"):
                await pq.replace_item(running_plan, item_uid=plans_added[2]["item_uid"])
            assert pq.plan_queue_uid == pq_uid

            # Make sure that the queue did not change during the test
            plan_queue, _ = await pq.get_queue()
            assert plan_queue == queue
            assert await pq.get_running_item_info() == running_item_info

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("params, src, order, success, pquid_changed, msg", [
    ({"pos": 1, "pos_dest": 1}, 1, "abcde", True, False, ""),
    ({"pos": "front", "pos_dest": "front"}, 0, "abcde", True, False, ""),
    ({"pos": "back", "pos_dest": "back"}, 4, "abcde", True, False, ""),
    ({"pos": "front", "pos_dest": "back"}, 0, "bcdea", True, True, ""),
    ({"pos": "back", "pos_dest": "front"}, 4, "eabcd", True, True, ""),
    ({"pos": 1, "pos_dest": 2}, 1, "acbde", True, True, ""),
    ({"pos": 2, "pos_dest": 1}, 2, "acbde", True, True, ""),
    ({"pos": 0, "pos_dest": 4}, 0, "bcdea", True, True, ""),
    ({"pos": 4, "pos_dest": 0}, 4, "eabcd", True, True, ""),
    ({"pos": 3, "pos_dest": "front"}, 3, "dabce", True, True, ""),
    ({"pos": 2, "pos_dest": "back"}, 2, "abdec", True, True, ""),
    ({"pos": -1, "pos_dest": 0}, 4, "eabcd", True, True, ""),
    ({"pos": -1, "pos_dest": 1}, 4, "aebcd", True, True, ""),
    ({"pos": -2, "pos_dest": 0}, 3, "dabce", True, True, ""),
    ({"pos": -2, "pos_dest": 1}, 3, "adbce", True, True, ""),
    ({"pos": -1, "pos_dest": -5}, 4, "eabcd", True, True, ""),
    ({"pos": -1, "pos_dest": -4}, 4, "aebcd", True, True, ""),
    ({"pos": -2, "pos_dest": -5}, 3, "dabce", True, True, ""),
    ({"pos": -2, "pos_dest": -4}, 3, "adbce", True, True, ""),
    ({"pos": -2, "pos_dest": -3}, 3, "abdce", True, True, ""),
    ({"pos": -4, "pos_dest": -2}, 1, "acdbe", True, True, ""),
    ({"pos": -3, "pos_dest": -2}, 2, "abdce", True, True, ""),
    ({"pos": -2, "pos_dest": -2}, 3, "abcde", True, False, ""),
    ({"uid": "p3", "after_uid": "p3"}, 2, "abcde", True, False, ""),
    ({"uid": "p1", "before_uid": "p2"}, 0, "abcde", True, True, ""),
    ({"uid": "p1", "after_uid": "p2"}, 0, "bacde", True, True, ""),
    ({"uid": "p2", "pos_dest": "front"}, 1, "bacde", True, True, ""),
    ({"uid": "p2", "pos_dest": "back"}, 1, "acdeb", True, True, ""),
    ({"uid": "p1", "pos_dest": "front"}, 0, "abcde", True, False, ""),
    ({"uid": "p5", "pos_dest": "back"}, 4, "abcde", True, False, ""),
    ({"pos": 1, "after_uid": "p4"}, 1, "acdbe", True, True, ""),
    ({"pos": "front", "after_uid": "p4"}, 0, "bcdae", True, True, ""),
    ({"pos": 3, "after_uid": "p1"}, 3, "adbce", True, True, ""),
    ({"pos": "back", "after_uid": "p1"}, 4, "aebcd", True, True, ""),
    ({"pos": 1, "before_uid": "p4"}, 1, "acbde", True, True, ""),
    ({"pos": "front", "before_uid": "p4"}, 0, "bcade", True, True, ""),
    ({"pos": 3, "before_uid": "p1"}, 3, "dabce", True, True, ""),
    ({"pos": "back", "before_uid": "p1"}, 4, "eabcd", True, True, ""),
    ({"pos": "back", "after_uid": "p5"}, 4, "abcde", True, False, ""),
    ({"pos": "front", "before_uid": "p1"}, 0, "abcde", True, False, ""),
    ({"pos": 50, "before_uid": "p1"}, 0, "", False, False, r"Source plan \(position 50\) was not found"),
    ({"uid": "abc", "before_uid": "p1"}, 0, "", False, False, r"Source plan \(UID 'abc'\) was not found"),
    ({"pos": 3, "pos_dest": 50}, 0, "", False, False, r"Destination plan \(position 50\) was not found"),
    ({"uid": "p1", "before_uid": "abc"}, 0, "", False, False, r"Destination plan \(UID 'abc'\) was not found"),
    ({"before_uid": "p1"}, 0, "", False, None, r"Source position or UID is not specified"),
    ({"pos": 3}, 0, "", False, False, r"Destination position or UID is not specified"),
    ({"pos": 1, "uid": "p1", "before_uid": "p4"}, 1, "", False, False, "Ambiguous parameters"),
    ({"pos": 1, "pos_dest": 4, "before_uid": "p4"}, 1, "", False, False, "Ambiguous parameters"),
    ({"pos": 1, "after_uid": "p4", "before_uid": "p4"}, 1, "", False, False, "Ambiguous parameters"),
])
# fmt: on
def test_move_item_1(params, src, order, success, pquid_changed, msg):
    """
    Basic tests for ``move_item()``.
    """

    async def testing():
        async with PQ() as pq:
            plans = [
                {"item_uid": "p1", "name": "a"},
                {"item_uid": "p2", "name": "b"},
                {"item_uid": "p3", "name": "c"},
                {"item_uid": "p4", "name": "d"},
                {"item_uid": "p5", "name": "e"},
            ]

            for plan in plans:
                await pq.add_item_to_queue(plan)

            assert await pq.get_queue_size() == len(plans)
            pq_uid = pq.plan_queue_uid

            if success:
                plan, qsize = await pq.move_item(**params)
                assert qsize == len(plans)
                assert plan["name"] == plans[src]["name"]

                queue, _ = await pq.get_queue()
                names = [_["name"] for _ in queue]
                names = "".join(names)
                assert names == order

                if pquid_changed:
                    assert pq.plan_queue_uid != pq_uid
                else:
                    assert pq.plan_queue_uid == pq_uid

            else:
                with pytest.raises(Exception, match=msg):
                    await pq.move_item(**params)
                assert pq.plan_queue_uid == pq_uid

    asyncio.run(testing())


def test_move_item_2():
    """
    ``move_item``: test if the item is moved 'as is', i.e. no parameter filtering is applied to it.
    """

    async def testing():
        async with PQ() as pq:
            plans = [
                {"item_uid": "p1", "name": "a", "result": {}},
                {"item_uid": "p2", "name": "b"},
                {"item_uid": "p3", "name": "c"},
            ]

            for plan in plans:
                await pq.add_item_to_queue(plan, filter_parameters=False)

            queue, _ = await pq.get_queue()
            assert await pq.get_queue_size() == 3
            assert "result" in queue[0]

            uid_src = queue[0]["item_uid"]
            uid_dest = queue[1]["item_uid"]

            await pq.move_item(uid=uid_src, after_uid=uid_dest)

            queue, _ = await pq.get_queue()
            assert await pq.get_queue_size() == 3
            # Make sure that the items were rearranged
            assert queue[0]["item_uid"] == uid_dest, pprint.pformat(queue)
            assert queue[1]["item_uid"] == uid_src, pprint.pformat(queue)
            # Make sure that the 'result' parameter was not removed
            assert "result" in queue[1]

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("batch_params, queue_seq, selection_seq, batch_seq, expected_seq, success, msg", [
    ({"pos_dest": "front"}, "0123456", "23", "23", "2301456", True, ""),
    ({"before_uid": "0"}, "0123456", "23", "23", "2301456", True, ""),
    ({"pos_dest": "back"}, "0123456", "23", "23", "0145623", True, ""),
    ({"after_uid": "6"}, "0123456", "23", "23", "0145623", True, ""),
    ({"before_uid": "5"}, "0123456", "23", "23", "0142356", True, ""),
    ({"after_uid": "5"}, "0123456", "23", "23", "0145236", True, ""),

    ({"pos_dest": 0}, "0123456", "23", "23", "2301456", True, ""),
    ({"pos_dest": -7}, "0123456", "23", "23", "2301456", True, ""),
    ({"pos_dest": 1}, "0123456", "23", "23", "0231456", True, ""),
    ({"pos_dest": -6}, "0123456", "23", "23", "0231456", True, ""),
    ({"pos_dest": 5}, "0123456", "23", "23", "0145236", True, ""),
    ({"pos_dest": -2}, "0123456", "23", "23", "0145236", True, ""),
    ({"pos_dest": 6}, "0123456", "23", "23", "0145623", True, ""),
    ({"pos_dest": -1}, "0123456", "23", "23", "0145623", True, ""),

    # Controlling the order of moved items
    ({"after_uid": "5"}, "0123456", "023", "023", "1450236", True, ""),
    ({"after_uid": "5"}, "0123456", "203", "203", "1452036", True, ""),
    ({"after_uid": "5"}, "0123456", "302", "302", "1453026", True, ""),
    ({"after_uid": "5", "reorder": False}, "0123456", "302", "302", "1453026", True, ""),
    ({"after_uid": "5", "reorder": True}, "0123456", "023", "023", "1450236", True, ""),
    ({"after_uid": "5", "reorder": True}, "0123456", "203", "023", "1450236", True, ""),
    ({"after_uid": "5", "reorder": True}, "0123456", "302", "023", "1450236", True, ""),
    # Empty list of UIDS
    ({"pos_dest": "front"}, "0123456", "", "", "0123456", True, ""),
    ({"pos_dest": "front"}, "", "", "", "", True, ""),
    # Move the batch which is already in the front or back to front or back of the queue
    #   (nothing is done, but operation is still successful)
    ({"pos_dest": "front"}, "0123456", "01", "01", "0123456", True, ""),
    ({"pos_dest": "back"}, "0123456", "56", "56", "0123456", True, ""),
    # Same, but change the order of moved items
    ({"pos_dest": "front"}, "0123456", "10", "10", "1023456", True, ""),
    ({"pos_dest": "back"}, "0123456", "65", "65", "0123465", True, ""),
    # Failing cases
    ({}, "0123456", "23", "23", "0123456", False, "Destination for the batch is not specified"),
    ({"pos_dest": "front", "before_uid": "5"}, "0123456", "23", "23", "0123456", False,
     "more than one mutually exclusive parameter"),
    ({"after_uid": "3"}, "0123456", "023", "023", "0123456", False, "item with UID '33' is in the batch"),
    ({"before_uid": "3"}, "0123456", "023", "023", "0123456", False, "item with UID '33' is in the batch"),
    ({"after_uid": "5"}, "0123456", "093", "093", "0123456", False,
     re.escape("The queue does not contain items with the following UIDs: ['99']")),
    ({"after_uid": "5"}, "0123456", "07893", "07893", "0123456", False,
     re.escape("The queue does not contain items with the following UIDs: ['77', '88', '99']")),
    ({"after_uid": "5"}, "0123456", "0223", "0223", "0123456", False,
     re.escape("The list of contains repeated UIDs (1 UIDs)")),
])
# fmt: on
def test_move_batch_1(batch_params, queue_seq, selection_seq, batch_seq, expected_seq, success, msg):
    async def testing():
        async with PQ() as pq:

            async def add_plan(plan, n, **kwargs):
                plan_added, qsize = await pq.add_item_to_queue(plan, **kwargs)
                assert plan_added["name"] == plan["name"], f"plan: {plan}"
                assert qsize == n, f"plan: {plan}"

            def name_to_uid(uid):
                return f"{uid}{uid}"

            # Create the queue with plans
            for n, p_name in enumerate(queue_seq):
                await add_plan({"name": p_name, "item_uid": f"{name_to_uid(p_name)}"}, n + 1)
            qsize = await pq.get_queue_size()
            assert qsize == len(queue_seq)

            if "before_uid" in batch_params:
                batch_params["before_uid"] = name_to_uid(batch_params["before_uid"])
            if "after_uid" in batch_params:
                batch_params["after_uid"] = name_to_uid(batch_params["after_uid"])

            uids = [name_to_uid(_) for _ in selection_seq]
            if success:
                items_moved, qsize_after = await pq.move_batch(uids=uids, **batch_params)
                items_seq = "".join([_["name"] for _ in items_moved])
                assert items_seq == batch_seq
                assert qsize_after == qsize
            else:
                with pytest.raises(Exception, match=msg):
                    await pq.move_batch(uids=uids, **batch_params)

            # Make sure that the queue is in the correct state
            queue, _ = await pq.get_queue()
            seq = "".join([_["name"] for _ in queue])
            assert seq == expected_seq
            assert len(queue) == qsize

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("pos, name", [
    ("front", "a"),
    ("back", "c"),
    (0, "a"),
    (1, "b"),
    (2, "c"),
    (3, None),  # Index out of range
    (-1, "c"),
    (-2, "b"),
    (-3, "a"),
    (-4, None)  # Index out of range
])
# fmt: on
def test_pop_item_from_queue_1(pos, name):
    """
    Basic test for the function ``PlanQueueOperations.pop_item_from_queue()``
    """

    async def testing():
        async with PQ() as pq:
            await pq.add_item_to_queue({"name": "a"})
            await pq.add_item_to_queue({"name": "b"})
            await pq.add_item_to_queue({"name": "c"})
            assert await pq.get_queue_size() == 3

            pq_uid = pq.plan_queue_uid

            if name is not None:
                plan, qsize = await pq.pop_item_from_queue(pos=pos)
                assert plan["name"] == name
                assert qsize == 2
                assert await pq.get_queue_size() == 2
                # Push the plan back to the queue (proves that UID is removed from '_uid_dict')
                await pq.add_item_to_queue(plan)
                assert await pq.get_queue_size() == 3
                assert pq.plan_queue_uid != pq_uid
            else:
                with pytest.raises(IndexError, match="Index .* is out of range"):
                    await pq.pop_item_from_queue(pos=pos)
                assert pq.plan_queue_uid == pq_uid

    asyncio.run(testing())


@pytest.mark.parametrize("pos", ["front", "back", 0, 1, -1])
def test_pop_item_from_queue_2(pos):
    """
    Test for the function ``PlanQueueOperations.pop_item_from_queue()``:
    the case of empty queue.
    """

    async def testing():
        async with PQ() as pq:
            assert await pq.get_queue_size() == 0
            pq_uid = pq.plan_queue_uid
            with pytest.raises(IndexError, match="Index .* is out of range|Queue is empty"):
                await pq.pop_item_from_queue(pos=pos)
            assert pq.plan_queue_uid == pq_uid

    asyncio.run(testing())


def test_pop_item_from_queue_3():
    """
    Pop plans by UID.
    """

    async def testing():
        async with PQ() as pq:
            await pq.add_item_to_queue({"item_type": "plan", "name": "a"})
            await pq.add_item_to_queue({"item_type": "plan", "name": "b"})
            await pq.add_item_to_queue({"item_type": "plan", "name": "c"})
            assert await pq.get_queue_size() == 3

            plans, _ = await pq.get_queue()
            assert len(plans) == 3
            plan_to_remove = [_ for _ in plans if _["name"] == "b"][0]

            # Remove one plan
            pq_uid = pq.plan_queue_uid
            await pq.pop_item_from_queue(uid=plan_to_remove["item_uid"])
            assert await pq.get_queue_size() == 2
            assert pq.plan_queue_uid != pq_uid

            # Attempt to remove the plan again. This should raise an exception.
            pq_uid = pq.plan_queue_uid
            with pytest.raises(
                IndexError, match=f"Plan with UID '{plan_to_remove['item_uid']}' " f"is not in the queue"
            ):
                await pq.pop_item_from_queue(uid=plan_to_remove["item_uid"])
            assert await pq.get_queue_size() == 2
            assert pq.plan_queue_uid == pq_uid

            # Attempt to remove the plan that is running. This should raise an exception.
            await pq.set_next_item_as_running()
            assert await pq.get_queue_size() == 1
            pq_uid = pq.plan_queue_uid
            with pytest.raises(IndexError, match="Can not remove an item which is currently running"):
                await pq.pop_item_from_queue(uid=plans[0]["item_uid"])
            assert await pq.get_queue_size() == 1
            assert pq.plan_queue_uid == pq_uid

    asyncio.run(testing())


def test_pop_item_from_queue_4_fail():
    """
    Failing tests for the function ``PlanQueueOperations.pop_item_from_queue()``
    """

    async def testing():
        async with PQ() as pq:
            pq_uid = pq.plan_queue_uid
            with pytest.raises(ValueError, match="Parameter 'pos' has incorrect value"):
                await pq.pop_item_from_queue(pos="something")
            assert pq.plan_queue_uid == pq_uid

            # Ambiguous parameters (position and UID is passed)
            with pytest.raises(ValueError, match="Ambiguous parameters"):
                await pq.pop_item_from_queue(pos=5, uid="abc")
            assert pq.plan_queue_uid == pq_uid

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("batch_params, queue_seq, selection_seq, batch_seq, expected_seq, success, msg", [
    ({}, "0123456", "", "", "0123456", True, ""),
    ({}, "0123456", "23", "23", "01456", True, ""),
    ({}, "0123456", "32", "32", "01456", True, ""),
    ({}, "0123456", "06", "06", "12345", True, ""),
    ({}, "0123456", "283", "23", "01456", True, ""),
    ({}, "0123456", "2893", "23", "01456", True, ""),
    ({}, "0123456", "2443", "243", "0156", True, ""),
    ({"ignore_missing": True}, "0123456", "2443", "243", "0156", True, ""),
    ({"ignore_missing": True}, "0123456", "283", "23", "01456", True, ""),
    ({"ignore_missing": False}, "0123456", "2443", "", "0123456", False, "The list of contains repeated UIDs"),
    ({"ignore_missing": False}, "0123456", "283", "", "0123456", False,
     "The queue does not contain items with the following UIDs"),
    ({"ignore_missing": False}, "0123456", "2883", "", "0123456", False, "The list of contains repeated UIDs"),
    ({}, "0123456", "", "", "0123456", True, ""),
    ({}, "", "", "", "", True, ""),
    ({}, "", "23", "", "", True, ""),
    ({"ignore_missing": False}, "", "", "", "", True, ""),
    ({"ignore_missing": False}, "", "23", "", "", False,
     "The queue does not contain items with the following UIDs"),
])
# fmt: on
def test_pop_items_from_queue_batch_1(
    batch_params, queue_seq, selection_seq, batch_seq, expected_seq, success, msg
):
    """
    Tests for ``pop_items_from_queue_batch``.
    """

    async def testing():
        async with PQ() as pq:

            async def add_plan(plan, n, **kwargs):
                plan_added, qsize = await pq.add_item_to_queue(plan, **kwargs)
                assert plan_added["name"] == plan["name"], f"plan: {plan}"
                assert qsize == n, f"plan: {plan}"

            def name_to_uid(uid):
                return f"{uid}{uid}"

            # Create the queue with plans
            for n, p_name in enumerate(queue_seq):
                await add_plan({"name": p_name, "item_uid": f"{name_to_uid(p_name)}"}, n + 1)
            qsize = await pq.get_queue_size()
            assert qsize == len(queue_seq)

            uids = [name_to_uid(_) for _ in selection_seq]
            if success:
                items_removed, qsize_after = await pq.pop_item_from_queue_batch(uids=uids, **batch_params)
                items_seq = "".join([_["name"] for _ in items_removed])
                assert items_seq == batch_seq
                assert qsize_after == len(expected_seq)
            else:
                with pytest.raises(Exception, match=msg):
                    await pq.pop_item_from_queue_batch(uids=uids, **batch_params)

            # Make sure that the queue is in the correct state
            queue, _ = await pq.get_queue()
            seq = "".join([_["name"] for _ in queue])
            assert seq == expected_seq
            assert len(queue) == len(expected_seq)

    asyncio.run(testing())


def test_clear_queue():
    """
    Test for ``PlanQueueOperations.clear_queue`` function
    """

    async def testing():
        async with PQ() as pq:
            await pq.add_item_to_queue({"item_type": "plan", "name": "a"})
            await pq.add_item_to_queue({"item_type": "plan", "name": "b"})
            await pq.add_item_to_queue({"item_type": "plan", "name": "c"})
            # Set one of 3 plans as running (removes it from the queue)
            await pq.set_next_item_as_running()

            assert await pq.get_queue_size() == 2
            assert len(pq._uid_dict) == 3

            pq_uid = pq.plan_queue_uid

            # Clears the queue only (doesn't touch the running plan)
            await pq.clear_queue()

            assert await pq.get_queue_size() == 0
            assert len(pq._uid_dict) == 1
            assert pq.plan_queue_uid != pq_uid

            with pytest.raises(ValueError, match="Parameter 'pos' has incorrect value"):
                await pq.pop_item_from_queue(pos="something")

    asyncio.run(testing())


def test_add_to_history_functions():
    """
    Test for ``PlanQueueOperations._add_to_history()`` method.
    """

    async def testing():
        async with PQ() as pq:
            assert await pq.get_history_size() == 0

            plans = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
            ph_uid = pq.plan_history_uid
            for plan in plans:
                await pq._add_to_history(plan)
            assert await pq.get_history_size() == 3
            assert pq.plan_history_uid != ph_uid

            ph_uid = pq.plan_history_uid
            plan_history, plan_history_uid_1 = await pq.get_history()
            assert pq.plan_history_uid == plan_history_uid_1
            assert pq.plan_history_uid == ph_uid

            assert len(plan_history) == 3
            assert plan_history == plans

            ph_uid = pq.plan_history_uid
            await pq.clear_history()
            assert pq.plan_history_uid != ph_uid

            plan_history, _ = await pq.get_history()
            assert plan_history == []

    asyncio.run(testing())


@pytest.mark.parametrize("immediate_execution", [False, True])
@pytest.mark.parametrize("func", ["process_next_item", "set_next_item_as_running"])
@pytest.mark.parametrize("loop_mode", [False, True])
def test_process_next_item_1(func, loop_mode, immediate_execution):
    """
    Test for ``PlanQueueOperations.process_next_item()`` and
    ``PlanQueueOperations.set_next_item_as_running()`` functions.
    Those functions are interchangeable for plans, but ``process_next_item`` works
    for items and plans.
    """

    async def testing():
        async with PQ() as pq:
            await pq.set_plan_queue_mode({"loop": loop_mode})

            # Apply to empty queue
            assert await pq.get_queue_size() == 0
            assert await pq.is_item_running() is False
            assert await pq.set_next_item_as_running() == {}
            assert await pq.get_queue_size() == 0
            assert await pq.is_item_running() is False

            # Apply to a queue with several plans
            p1, _ = await pq.add_item_to_queue({"item_type": "plan", "name": "a"})
            p2, _ = await pq.add_item_to_queue({"item_type": "plan", "name": "b"})
            p3, _ = await pq.add_item_to_queue({"item_type": "plan", "name": "c"})

            # Plan used for immediate execution
            plan4 = {"item_type": "plan", "name": "d"}

            if func == "process_next_item":
                f = pq.process_next_item
            elif func == "set_next_item_as_running":
                f = pq.set_next_item_as_running
            else:
                assert False, f"Unknown test case: '{func}'"

            pq_uid1 = pq.plan_queue_uid
            queue_1, _ = await pq.get_queue()

            # If the next plan is set as running, it is removed from the queue and
            #   the queue UID is changed. If a plan is set for immediate execution,
            #   then the queue and the queue UID remain unchanged.
            params = {"item": plan4} if immediate_execution else {}
            running_plan = await f(**params)
            queue_2, _ = await pq.get_queue()

            assert running_plan != {}

            if immediate_execution:
                assert running_plan["name"] == "d"
                assert "properties" in running_plan
                assert running_plan["properties"]["immediate_execution"] is True
                assert pq.plan_queue_uid != pq_uid1
                assert await pq.get_queue_size() == 3
                assert len(pq._uid_dict) == 3
                assert queue_1 == queue_2
            else:
                assert running_plan["name"] == "a"
                assert pq.plan_queue_uid != pq_uid1
                assert await pq.get_queue_size() == 2
                assert len(pq._uid_dict) == 3

            # Apply if a plan is already running
            pq_uid1 = pq.plan_queue_uid
            assert await f() == {}
            assert pq.plan_queue_uid == pq_uid1
            assert await pq.get_queue_size() == (3 if immediate_execution else 2)
            assert len(pq._uid_dict) == 3

    asyncio.run(testing())


@pytest.mark.parametrize("immediate_execution", [False, True])
@pytest.mark.parametrize("loop_mode", [False, True])
def test_process_next_item_2(loop_mode, immediate_execution):
    """
    Test for ``PlanQueueOperations.process_next_item()`` and
    ``PlanQueueOperations.set_next_item_as_running()`` functions.
    Those functions are interchangeable for plans, but ``process_next_item`` works
    for items and plans.

    Test with 'instruction' item.
    """

    async def testing():
        async with PQ() as pq:
            await pq.set_plan_queue_mode({"loop": loop_mode})

            # Apply to empty queue
            assert await pq.get_queue_size() == 0
            assert await pq.is_item_running() is False
            assert await pq.set_next_item_as_running() == {}
            assert await pq.get_queue_size() == 0
            assert await pq.is_item_running() is False

            # Apply to a queue with several plans
            p0, _ = await pq.add_item_to_queue({"item_type": "instruction", "name": "a"})
            p1, _ = await pq.add_item_to_queue({"item_type": "plan", "name": "b"})
            p2, _ = await pq.add_item_to_queue({"item_type": "plan", "name": "c"})

            item4 = {"item_type": "instruction", "name": "d"}

            pq_uid = pq.plan_queue_uid
            queue_1 = await pq.get_queue()

            params = {"item": item4} if immediate_execution else {}
            running_item = await pq.process_next_item(**params)
            queue_2 = await pq.get_queue()

            if immediate_execution:
                assert {_: running_item[_] for _ in ("name", "item_type")} == item4
                assert pq.plan_queue_uid == pq_uid
                assert await pq.get_queue_size() == 3
                assert len(pq._uid_dict) == 3
                assert queue_2 == queue_1
            else:
                assert running_item == p0
                assert pq.plan_queue_uid != pq_uid
                assert await pq.get_queue_size() == 3 if loop_mode else 2
                assert len(pq._uid_dict) == 3 if loop_mode else 2

                queue, _ = await pq.get_queue()
                assert queue[0] == p1
                assert queue[1] == p2
                if loop_mode:
                    assert queue[2]["name"] == p0["name"]
                    assert queue[2]["item_uid"] != p0["name"]

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("has_uid", [False, True])
@pytest.mark.parametrize("item_type", ["plan", "instruction"])
# fmt: on
def test_process_next_item_3(item_type, has_uid):
    """
    Verify if the function ``process_next_item`` is assigning new UID to plans, but not instructions.
    """

    async def testing():
        async with PQ() as pq:
            # Apply to a queue with several plans
            await pq.add_item_to_queue({"item_type": "instruction", "name": "a"})
            await pq.add_item_to_queue({"item_type": "plan", "name": "b"})
            await pq.add_item_to_queue({"item_type": "plan", "name": "c"})

            item4 = {"item_type": item_type, "name": "d"}
            if has_uid:
                item4 = pq.set_new_item_uuid(item4)

            running_item = await pq.process_next_item(item=item4)

            if has_uid:
                assert running_item["item_uid"] == item4["item_uid"]
            else:
                assert "item_uid" in running_item
                assert isinstance(running_item["item_uid"], str)

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("immediate_execution", [False, True])
# fmt: on
def test_process_next_item_4_fail(immediate_execution):
    """
    Try using 'set_next_item_as_running' with an instruction. Exception should be raised.
    The queue should remain unchanged.
    """

    async def testing():
        async with PQ() as pq:
            # Apply to a queue with several plans
            await pq.add_item_to_queue({"item_type": "instruction", "name": "a"})
            await pq.add_item_to_queue({"item_type": "plan", "name": "b"})
            await pq.add_item_to_queue({"item_type": "plan", "name": "c"})

            item4 = {"item_type": "instruction", "name": "d"}

            pq_uid = pq.plan_queue_uid
            queue_1, _ = await pq.get_queue()

            params = {"item": item4} if immediate_execution else {}

            with pytest.raises(RuntimeError, match="was called for an item other than plan"):
                await pq.set_next_item_as_running(**params)

            queue_2, _ = await pq.get_queue()  # The queue should remain untouched

            assert pq.plan_queue_uid == pq_uid
            assert await pq.get_queue_size() == 3
            assert len(pq._uid_dict) == 3
            assert queue_2 == queue_1

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("item_in, item_out", [
    ({"name": "count"}, {"name": "count"}),
    ({"name": "count", "properties": {}}, {"name": "count"}),
    ({"name": "count", "properties": {"nonexisting": 10}}, {"name": "count", "properties": {"nonexisting": 10}}),
    ({"name": "count", "properties": {"immediate_execution": True}}, {"name": "count"}),
])
# fmt: on
def test_clean_item_properties_1(item_in, item_out):
    """
    Basic test for `_clean_item_properties` function.
    """

    async def testing():
        async with PQ() as pq:
            item_cleaned = pq._clean_item_properties(item_in)
            assert item_cleaned == item_out

    asyncio.run(testing())


def test_set_processed_item_as_completed_1():
    """
    Test for ``PlanQueueOperations.set_processed_item_as_completed()`` function.
    The function moves currently running plan to history.
    """

    plan_uids = [1, 2, 3]
    plans = [
        {"item_type": "plan", "item_uid": plan_uids[0], "name": "a"},
        {"item_type": "plan", "item_uid": plan_uids[1], "name": "b"},
        {"item_type": "plan", "item_uid": plan_uids[2], "name": "c"},
    ]
    plans_run_uids = [["abc1"], ["abc2", "abc3"], []]
    plans_scan_ids = [[1], [100, 101], []]

    def add_status_to_plans(plans, run_uids, scan_ids, exit_status):
        plans = copy.deepcopy(plans)
        plans_modified = []
        for plan, run_uid, scan_id in zip(plans, run_uids, scan_ids):
            plan.setdefault("result", {})
            plan["result"]["exit_status"] = exit_status
            plan["result"]["run_uids"] = run_uid
            plan["result"]["scan_ids"] = scan_id
            plan["result"]["msg"] = ""
            plan["result"]["traceback"] = ""
            plans_modified.append(plan)
        return plans_modified

    def add_msg_to_plan_history(plan_history, run_uids, msg, tb):
        plan_history = copy.deepcopy(plan_history)
        for p in plan_history:
            if p["item_uid"] in run_uids:
                p["result"]["msg"] = msg
                p["result"]["traceback"] = tb
        return plan_history

    def check_plan_history(plan_history, plan_history_expected):
        ph = copy.deepcopy(plan_history)
        for _ in ph:
            del _["result"]["time_start"]
            del _["result"]["time_stop"]
        assert ph == plan_history_expected
        for _ in plan_history:
            assert isinstance(_["result"]["time_start"], float)
            assert isinstance(_["result"]["time_stop"], float)
            assert _["result"]["time_start"] < _["result"]["time_stop"]

    async def testing():
        async with PQ() as pq:
            for plan in plans:
                await pq.add_item_to_queue(plan)

            # No plan is running
            pq_uid = pq.plan_queue_uid
            ph_uid = pq.plan_history_uid
            plan = await pq.set_processed_item_as_completed(
                exit_status="completed",
                run_uids=plans_run_uids[0],
                scan_ids=plans_scan_ids[0],
                err_msg="",
                err_tb="",
            )
            assert plan == {}
            assert pq.plan_queue_uid == pq_uid
            assert pq.plan_history_uid == ph_uid

            # Execute the first plan
            await pq.set_next_item_as_running()
            pq_uid = pq.plan_queue_uid
            plan = await pq.set_processed_item_as_completed(
                exit_status="completed",
                run_uids=plans_run_uids[0],
                scan_ids=plans_scan_ids[0],
                err_msg="Test message",
                err_tb="Traceback",
            )
            assert pq.plan_queue_uid != pq_uid
            assert pq.plan_history_uid != ph_uid

            assert await pq.get_queue_size() == 2
            assert await pq.get_history_size() == 1
            assert plan["name"] == plans[0]["name"]
            assert plan["result"]["exit_status"] == "completed"
            assert plan["result"]["run_uids"] == plans_run_uids[0]
            assert plan["result"]["scan_ids"] == plans_scan_ids[0]

            plan_history, _ = await pq.get_history()
            plan_history_expected = add_status_to_plans(
                plans[0:1], plans_run_uids[0:1], plans_scan_ids[0:1], "completed"
            )
            plan_history_expected = add_msg_to_plan_history(
                plan_history_expected, [plan_uids[0]], "Test message", "Traceback"
            )
            print(plan_history)
            print(plan_history_expected)
            check_plan_history(plan_history, plan_history_expected)

            # Execute the second plan
            await pq.set_next_item_as_running()
            plan = await pq.set_processed_item_as_completed(
                exit_status="completed",
                run_uids=plans_run_uids[1],
                scan_ids=plans_scan_ids[1],
                err_msg="",
                err_tb="",
            )

            assert await pq.get_queue_size() == 1
            assert await pq.get_history_size() == 2
            assert plan["name"] == plans[1]["name"]
            assert plan["result"]["exit_status"] == "completed"
            assert plan["result"]["run_uids"] == plans_run_uids[1]
            assert plan["result"]["scan_ids"] == plans_scan_ids[1]

            plan_history, _ = await pq.get_history()
            plan_history_expected = add_status_to_plans(
                plans[0:2], plans_run_uids[0:2], plans_scan_ids[0:2], "completed"
            )
            plan_history_expected = add_msg_to_plan_history(
                plan_history_expected, [plan_uids[0]], "Test message", "Traceback"
            )
            check_plan_history(plan_history, plan_history_expected)

    asyncio.run(testing())


def test_set_processed_item_as_completed_2():
    """
    Test for ``PlanQueueOperations.set_processed_item_as_completed()`` function.
    Similar test as the previous one, but with LOOP mode ENABLED.
    """

    plan_uids = [1, 2, 3]
    plans = [
        {"item_type": "plan", "item_uid": plan_uids[0], "name": "a"},
        {"item_type": "plan", "item_uid": plan_uids[1], "name": "b"},
        {"item_type": "plan", "item_uid": plan_uids[2], "name": "c"},
    ]
    plans_run_uids = [["abc1"], ["abc2", "abc3"], []]
    plans_scan_ids = [[1], [100, 101], []]

    async def testing():
        async with PQ() as pq:
            for plan in plans:
                await pq.add_item_to_queue(plan)

            # Enable LOOP mode: the completed item will be pushed to the back of the queue
            await pq.set_plan_queue_mode({"loop": True})

            # No plan is running
            pq_uid = pq.plan_queue_uid
            ph_uid = pq.plan_history_uid
            plan = await pq.set_processed_item_as_completed(
                exit_status="completed",
                run_uids=plans_run_uids[0],
                scan_ids=plans_scan_ids[0],
                err_msg="",
                err_tb="",
            )
            assert plan == {}
            assert pq.plan_queue_uid == pq_uid
            assert pq.plan_history_uid == ph_uid

            queue, _ = await pq.get_queue()
            assert [_["name"] for _ in queue] == ["a", "b", "c"]

            # Execute the first plan
            await pq.set_next_item_as_running()
            pq_uid = pq.plan_queue_uid
            plan = await pq.set_processed_item_as_completed(
                exit_status="completed",
                run_uids=plans_run_uids[0],
                scan_ids=plans_scan_ids[0],
                err_msg="",
                err_tb="",
            )
            assert pq.plan_queue_uid != pq_uid
            assert pq.plan_history_uid != ph_uid

            queue, _ = await pq.get_queue()
            assert [_["name"] for _ in queue] == ["b", "c", "a"]

            assert await pq.get_queue_size() == 3
            assert await pq.get_history_size() == 1
            assert plan["name"] == plans[0]["name"]
            assert plan["result"]["exit_status"] == "completed"
            assert plan["result"]["run_uids"] == plans_run_uids[0]
            assert plan["result"]["scan_ids"] == plans_scan_ids[0]
            assert plan["result"]["msg"] == ""
            assert plan["result"]["traceback"] == ""
            assert plan["result"]["time_stop"] > plan["result"]["time_start"]

            # Execute the second plan
            await pq.set_next_item_as_running()
            plan = await pq.set_processed_item_as_completed(
                exit_status="unknown",
                run_uids=plans_run_uids[1],
                scan_ids=plans_scan_ids[1],
                err_msg="Unknown exit status",
                err_tb="Some traceback",
            )

            queue, _ = await pq.get_queue()
            assert [_["name"] for _ in queue] == ["c", "a", "b"]

            assert await pq.get_queue_size() == 3
            assert await pq.get_history_size() == 2
            assert plan["name"] == plans[1]["name"]
            assert plan["result"]["exit_status"] == "unknown"
            assert plan["result"]["run_uids"] == plans_run_uids[1]
            assert plan["result"]["scan_ids"] == plans_scan_ids[1]
            assert plan["result"]["msg"] == "Unknown exit status"
            assert plan["result"]["traceback"] == "Some traceback"
            assert plan["result"]["time_stop"] > plan["result"]["time_start"]

    asyncio.run(testing())


def test_set_processed_item_as_stopped_1():
    """
    Test for ``PlanQueueOperations.set_processed_item_as_stopped()`` function.
    The function pushes running plan back to the queue unless ``exit_status=="stopped"``
    and saves it in history as well.

    Typically execution of single-run plans result in no UIDs, but in this test we still
    assign UIDs to test functionality.
    """
    plan_uids = [1, 2, 3]
    plans = [
        {"item_type": "plan", "item_uid": plan_uids[0], "name": "a"},
        {"item_type": "plan", "item_uid": plan_uids[1], "name": "b"},
        {"item_type": "plan", "item_uid": plan_uids[2], "name": "c"},
    ]
    plans_run_uids = [["abc1"], ["abc2", "abc3"], []]
    plans_scan_ids = [[1], [100, 101], []]

    def add_status_to_plans(plans, run_uids, scan_ids, exit_status):
        plans = copy.deepcopy(plans)

        if isinstance(exit_status, list):
            assert len(exit_status) == len(plans)
        else:
            exit_status = [exit_status] * len(plans)

        plans_modified = []
        for plan, run_uid, scan_id, es in zip(plans, run_uids, scan_ids, exit_status):
            plan.setdefault("result", {})
            plan["result"]["exit_status"] = es
            plan["result"]["run_uids"] = run_uid
            plan["result"]["scan_ids"] = scan_id
            plan["result"]["msg"] = ""
            plan["result"]["traceback"] = ""
            plans_modified.append(plan)
        return plans_modified

    def add_msg_to_plan_history(plan_history, run_uids, msg, tb):
        plan_history = copy.deepcopy(plan_history)
        for p in plan_history:
            if p["item_uid"] in run_uids:
                p["result"]["msg"] = msg
                p["result"]["traceback"] = tb
        return plan_history

    def check_plan_history(plan_history, plan_history_expected):
        ph = copy.deepcopy(plan_history)
        for _ in ph:
            del _["result"]["time_start"]
            del _["result"]["time_stop"]
        assert ph == plan_history_expected
        for _ in plan_history:
            assert isinstance(_["result"]["time_start"], float)
            assert isinstance(_["result"]["time_stop"], float)
            assert _["result"]["time_start"] < _["result"]["time_stop"]

    async def testing():
        async with PQ() as pq:
            for plan in plans:
                await pq.add_item_to_queue(plan)

            # No plan is running
            pq_uid = pq.plan_queue_uid
            ph_uid = pq.plan_history_uid
            plan = await pq.set_processed_item_as_stopped(
                exit_status="stopped",
                run_uids=plans_run_uids[0],
                scan_ids=plans_scan_ids[0],
                err_msg="Test",
                err_tb="",
            )
            assert plan == {}
            assert pq.plan_queue_uid == pq_uid
            assert pq.plan_history_uid == ph_uid

            # Execute the first plan
            await pq.set_next_item_as_running()
            running_uid1 = (await pq.get_running_item_info())["item_uid"]
            pq_uid = pq.plan_queue_uid
            plan = await pq.set_processed_item_as_stopped(
                exit_status="failed",
                run_uids=plans_run_uids[0],
                scan_ids=plans_scan_ids[0],
                err_msg="Plan failed",
                err_tb="Traceback",
            )
            assert pq.plan_queue_uid != pq_uid
            assert pq.plan_history_uid != ph_uid

            assert await pq.get_queue_size() == 3
            assert await pq.get_history_size() == 1
            assert plan["name"] == plans[0]["name"]
            assert plan["result"]["exit_status"] == "failed"
            assert plan["result"]["run_uids"] == plans_run_uids[0]
            assert plan["result"]["scan_ids"] == plans_scan_ids[0]
            assert plan["result"]["msg"] == "Plan failed"
            assert plan["result"]["traceback"] == "Traceback"
            assert plan["result"]["time_stop"] > plan["result"]["time_start"]
            assert plan["item_uid"] == plans[0]["item_uid"]

            # New plan UID is generated when the plan is pushed back into the queue
            queue, _ = await pq.get_queue()
            plan_modified_uid = queue[0]["item_uid"]
            # Make sure that the item UID was changed
            assert plan_modified_uid != plans[0]["item_uid"]

            plan_history, _ = await pq.get_history()
            plan_history_expected = add_status_to_plans(
                [plans[0]], [plans_run_uids[0]], [plans_scan_ids[0]], "failed"
            )
            plan_history_expected = add_msg_to_plan_history(
                plan_history_expected, [running_uid1], "Plan failed", "Traceback"
            )
            check_plan_history(plan_history, plan_history_expected)

            # Execute the second plan
            await pq.set_next_item_as_running()
            running_uid2 = (await pq.get_running_item_info())["item_uid"]
            plan = await pq.set_processed_item_as_stopped(
                exit_status="stopped",
                run_uids=plans_run_uids[1],
                scan_ids=plans_scan_ids[1],
                err_msg="Plan stopped",
                err_tb="Traceback 2",
            )

            assert await pq.get_queue_size() == 2
            assert await pq.get_history_size() == 2
            assert plan["name"] == plans[0]["name"]
            assert plan["result"]["exit_status"] == "stopped"
            assert plan["result"]["run_uids"] == plans_run_uids[1]
            assert plan["result"]["scan_ids"] == plans_scan_ids[1]
            assert plan["result"]["msg"] == "Plan stopped"
            assert plan["result"]["traceback"] == "Traceback 2"
            assert plan["result"]["time_stop"] > plan["result"]["time_start"]

            plan_history, _ = await pq.get_history()
            plan_history_expected = add_status_to_plans(
                [plans[0].copy(), plans[0].copy()],
                [plans_run_uids[0], plans_run_uids[1]],
                [plans_scan_ids[0], plans_scan_ids[1]],
                ["failed", "stopped"],
            )
            # Plan 0 has different UID after it was inserted in the queue during the 1st attempt
            plan_history_expected[1]["item_uid"] = plan_modified_uid
            plan_history_expected = add_msg_to_plan_history(
                plan_history_expected, [running_uid1], "Plan failed", "Traceback"
            )
            plan_history_expected = add_msg_to_plan_history(
                plan_history_expected, [running_uid2], "Plan stopped", "Traceback 2"
            )
            check_plan_history(plan_history, plan_history_expected)

            # Verify that `_uid_dict` still has correct size. `_uid_dict` should never be accessed directly.
            assert len(pq._uid_dict) == 2
            # Also it should not contain UIDs of already executed plans.
            for plan in plan_history:
                assert plan["item_uid"] not in pq._uid_dict

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("func", ["completed", "unknown", "failed", "stopped", "aborted", "halted"])
@pytest.mark.parametrize("loop_mode", [False, True])
@pytest.mark.parametrize("immediate_execution", [False, True])
# fmt: on
def test_set_processed_item_as_stopped_2(loop_mode, func, immediate_execution):
    """
    ``set_processed_item_as_completed`` and ``set_processed_item_as_stopped`` processing of an item set
    for normal and immediate execution with/without LOOP mode.
    """
    plans = [
        {"item_type": "plan", "item_uid": 1, "name": "a"},
        {"item_type": "plan", "item_uid": 2, "name": "b"},
        {"item_type": "plan", "item_uid": 3, "name": "c"},
    ]
    plan4 = {"item_type": "plan", "item_uid": 3, "name": "d"}
    plan4_run_uids = ["abc2", "abc3"]
    plan4_scan_ids = [100, 101]

    async def testing():
        async with PQ() as pq:
            for plan in plans:
                await pq.add_item_to_queue(plan)

            # Enable LOOP mode: the completed item will be pushed to the back of the queue
            await pq.set_plan_queue_mode({"loop": loop_mode})

            pq_uid = pq.plan_queue_uid
            ph_uid = pq.plan_history_uid
            queue_1, _ = await pq.get_queue()

            if immediate_execution:
                await pq.process_next_item(item=plan4)
            else:
                await pq.process_next_item()

            assert pq.plan_queue_uid != pq_uid
            pq_uid2 = pq.plan_queue_uid
            assert pq.plan_history_uid == ph_uid

            err_msg, err_tb = f"Plan {func}", f"Traceback {func}"
            if func in ("completed", "unknown"):
                plan = await pq.set_processed_item_as_completed(
                    exit_status=func,
                    run_uids=plan4_run_uids,
                    scan_ids=plan4_scan_ids,
                    err_msg=err_msg,
                    err_tb=err_tb,
                )
            elif func in ("failed", "stopped", "aborted", "halted"):
                plan = await pq.set_processed_item_as_stopped(
                    exit_status=func,
                    run_uids=plan4_run_uids,
                    scan_ids=plan4_scan_ids,
                    err_msg=err_msg,
                    err_tb=err_tb,
                )
            else:
                assert False, f"Unknown value of parameter 'func': '{func}'"

            queue_2, _ = await pq.get_queue()

            if immediate_execution:
                assert queue_1 == queue_2
                assert await pq.get_queue_size() == 3
            else:
                assert queue_1 != queue_2
                qsize = 2 if func in ("completed", "unknown", "stopped") and not loop_mode else 3
                assert await pq.get_queue_size() == qsize
            assert await pq.get_history_size() == 1
            assert pq.plan_queue_uid != pq_uid2
            assert pq.plan_history_uid != ph_uid

            def check_plan(p):
                assert p["name"] == plan4["name"] if immediate_execution else plans[0]["name"]
                assert p["result"]["exit_status"] == func
                assert p["result"]["run_uids"] == plan4_run_uids
                assert p["result"]["scan_ids"] == plan4_scan_ids
                assert p["result"]["msg"] == err_msg
                assert p["result"]["traceback"] == err_tb
                assert plan["result"]["time_stop"] > plan["result"]["time_start"]

            check_plan(plan)

            history, _ = await pq.get_history()
            check_plan(history[0])

            assert history[0]["item_uid"] == plan["item_uid"]

    asyncio.run(testing())


@pytest.mark.parametrize("loop_mode", [False, True])
@pytest.mark.parametrize("func", ["completed", "stopped"])
def test_set_processed_item_as_stopped_3(loop_mode, func):
    """
    Test for ``PlanQueueOperations.set_processed_item_as_completed()`` and
    ``PlanQueueOperations.set_processed_item_as_stopped()`` function.
    Make sure that ``item["properties"]["immediate_execution"] is removed before the plan
    is added to history and the plan is not pushed back into the queue if the plan is
    stopped. The behavior of both functions is identical for the plans executed
    in 'immediate execution' mode.
    """

    plan = {"item_type": "plan", "item_uid": 1, "name": "a", "properties": {"immediate_execution": True}}

    async def testing():
        async with PQ() as pq:
            nonlocal plan
            await pq.add_item_to_queue(plan)
            await pq.set_plan_queue_mode({"loop": loop_mode})

            assert await pq.get_queue_size() == 1
            assert await pq.get_history_size() == 0

            await pq.set_next_item_as_running()

            assert await pq.get_queue_size() == 0
            assert await pq.get_history_size() == 0

            if func == "completed":
                plan1 = await pq.set_processed_item_as_completed(
                    exit_status="completed", run_uids=["abc"], scan_ids=[1], err_msg="", err_tb=""
                )
            elif func == "stopped":
                plan1 = await pq.set_processed_item_as_stopped(
                    exit_status="stopped", run_uids=["abc"], scan_ids=[1], err_msg="", err_tb=""
                )
            else:
                raise Exception(f"Unsupported parameter value func={func!r}")

            # assert False, plan1
            assert plan1["name"] == plan["name"]
            assert "properties" not in plan1

            assert await pq.get_queue_size() == 0
            assert await pq.get_history_size() == 1

            history, _ = await pq.get_history()
            assert history[0] == plan1
            assert "properties" not in history[0]

    asyncio.run(testing())


# ==============================================================================================
#                    Saving and retrieving user group permissions
def test_user_group_permissions_1():
    """
    Test for saving and retrieving and clearing user group permissions, which are backed up in Redis.
    """
    ug_permissions_1 = {"some_key_1": "some_value_1"}
    ug_permissions_2 = {"some_key_2": "some_value_2"}

    async def testing():
        async with PQ() as pq:
            assert await pq.user_group_permissions_retrieve() is None

            await pq.user_group_permissions_save(ug_permissions_1)
            assert await pq.user_group_permissions_retrieve() == ug_permissions_1

            await pq.user_group_permissions_save(ug_permissions_2)
            assert await pq.user_group_permissions_retrieve() == ug_permissions_2

            await pq.user_group_permissions_clear()
            assert await pq.user_group_permissions_retrieve() is None

    asyncio.run(testing())


# ==============================================================================================
#                           Saving and retrieving lock info
def test_lock_info_1():
    """
    Test for saving and retrieving and clearing user group permissions, which are backed up in Redis.
    """
    lock_info_1 = {"environment": False, "queue": True, "lock_key": "abcde"}
    lock_info_2 = {"environment": True, "queue": False, "lock_key": "fghijk"}

    async def testing():
        async with PQ() as pq:
            assert await pq.lock_info_retrieve() is None

            await pq.lock_info_save(lock_info_1)
            assert await pq.lock_info_retrieve() == lock_info_1

            await pq.lock_info_save(lock_info_2)
            assert await pq.lock_info_retrieve() == lock_info_2

            await pq.lock_info_clear()
            assert await pq.lock_info_retrieve() is None

    asyncio.run(testing())


# ==============================================================================================
#                           Saving and retrieving 'stop pending' info
def test_stop_pending_info_1():
    """
    Test for saving and retrieving and clearing user group permissions, which are backed up in Redis.
    """
    stop_pending_info_1 = {"enabled": False}
    stop_pending_info_2 = {"enabled": True}

    async def testing():
        async with PQ() as pq:
            assert await pq.stop_pending_retrieve() is None

            await pq.stop_pending_save(stop_pending_info_1)
            assert await pq.stop_pending_retrieve() == stop_pending_info_1

            await pq.stop_pending_save(stop_pending_info_2)
            assert await pq.stop_pending_retrieve() == stop_pending_info_2

            await pq.stop_pending_clear()
            assert await pq.stop_pending_retrieve() is None

    asyncio.run(testing())


# ==============================================================================================
#                           Saving and retrieving autostart info
def test_autostart_mode_info_1():
    """
    Test for saving and retrieving and clearing user group permissions, which are backed up in Redis.
    """
    autostart_info_1 = {"enabled": False}
    autostart_info_2 = {"enabled": True}

    async def testing():
        async with PQ() as pq:
            assert await pq.autostart_mode_retrieve() is None

            await pq.autostart_mode_save(autostart_info_1)
            assert await pq.autostart_mode_retrieve() == autostart_info_1

            await pq.autostart_mode_save(autostart_info_2)
            assert await pq.autostart_mode_retrieve() == autostart_info_2

            await pq.autostart_mode_clear()
            assert await pq.autostart_mode_retrieve() is None

    asyncio.run(testing())
