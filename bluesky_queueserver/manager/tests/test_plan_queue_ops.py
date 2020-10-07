import asyncio
import pytest
import json
import copy
from bluesky_queueserver.manager.plan_queue_ops import PlanQueueOperations


errmsg_wrong_plan_type = "Parameter 'plan' should be a dictionary"


@pytest.fixture
def pq():
    pq = PlanQueueOperations()
    asyncio.run(pq.start())
    # Clear any pool entries
    asyncio.run(pq.delete_pool_entries())
    yield pq
    # Don't leave any test entries in the pool
    asyncio.run(pq.delete_pool_entries())


def test_running_plan_info(pq):
    """
    Basic test for the following methods:
    `PlanQueueOperations.is_plan_running()`
    `PlanQueueOperations.get_running_plan_info()`
    `PlanQueueOperations.delete_pool_entries()`
    """

    async def testing():

        assert await pq.get_running_plan_info() == {}
        assert await pq.is_plan_running() is False

        some_plan = {"some_key": "some_value"}
        await pq._set_running_plan_info(some_plan)
        assert await pq.get_running_plan_info() == some_plan

        assert await pq.is_plan_running() is True

        await pq._clear_running_plan_info()
        assert await pq.get_running_plan_info() == {}
        assert await pq.is_plan_running() is False

        await pq._set_running_plan_info(some_plan)
        await pq.delete_pool_entries()
        assert await pq.get_running_plan_info() == {}
        assert await pq.is_plan_running() is False

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("plan_running, plans, result_running, result_plans", [
    ({"testing": 1}, [{"testing": 2}, {"plan_uid": "ab", "name": "nm"}, {"testing": 2}],
     {}, [{"plan_uid": "ab", "name": "nm"}]),
    ({"testing": 1}, [{"testing": 2}, {"plan_uid": "ab", "name": "nm"}, {"testing": 3}],
     {}, [{"plan_uid": "ab", "name": "nm"}]),
    ({"plan_uid": "a"}, [{"plan_uid": "a1"}, {"plan_uid": "a2"}, {"plan_uid": "a3"}],
     {"plan_uid": "a"}, [{"plan_uid": "a1"}, {"plan_uid": "a2"}, {"plan_uid": "a3"}]),
])
# fmt: on
def test_queue_clean(pq, plan_running, plans, result_running, result_plans):
    """
    Test for ``_queue_clean()`` method
    """

    async def testing():
        await pq._set_running_plan_info(plan_running)
        for plan in plans:
            await pq._r_pool.rpush(pq._name_plan_queue, json.dumps(plan))

        assert await pq.get_running_plan_info() == plan_running
        assert await pq.get_plan_queue() == plans

        await pq._queue_clean()

        assert await pq.get_running_plan_info() == result_running
        assert await pq.get_plan_queue() == result_plans

    asyncio.run(testing())


# fmt: off
@pytest.mark.parametrize("plan, result",
                         [({"a": 10}, True),
                          ([10, 20], False),
                          (50, False),
                          ("abc", False)])
# fmt: on
def test_verify_plan_type(pq, plan, result):
    if result:
        pq._verify_plan_type(plan)
    else:
        with pytest.raises(TypeError, match=errmsg_wrong_plan_type):
            pq._verify_plan_type(plan)


# fmt: off
@pytest.mark.parametrize(
    "plan, result, errmsg",
    [({"a": 10}, False, "Plan does not have UID"),
     ([10, 20], False, errmsg_wrong_plan_type),
     ({"plan_uid": "one"}, True, ""),
     ({"plan_uid": "two"}, False, "Plan with UID .+ is already in the queue"),
     ({"plan_uid": "three"}, False, "Plan with UID .+ is already in the queue")])
# fmt: on
def test_verify_plan(pq, plan, result, errmsg):
    """
    Tests for method ``_verify_plan()``.
    """
    # Set two existiing plans and then set one of them as running
    existing_plans = [{"plan_uid": "two"}, {"plan_uid": "three"}]

    async def set_plans():
        # Add plan to queue
        for plan in existing_plans:
            await pq.add_plan_to_queue(plan)
        # Set one plan as currently running
        await pq.set_next_plan_as_running()

        # Verify that setup is correct
        assert await pq.is_plan_running() is True
        assert await pq.get_plan_queue_size() == 1

    asyncio.run(set_plans())

    if result:
        pq._verify_plan(plan)
    else:
        with pytest.raises(Exception, match=errmsg):
            pq._verify_plan(plan)


def test_new_plan_uid(pq):
    """
    Smoke test for the method ``_new_plan_uid()``.
    """
    assert isinstance(pq._new_plan_uid(), str)


# fmt: off
@pytest.mark.parametrize("plan", [
    {"name": "a"},
    {"plan_uid": "some_uid", "name": "a"},
])
# fmt: on
def test_set_new_plan_uuid(pq, plan):
    """
    Basic test for the method ``set_new_plan_uuid()``.
    """
    uid = plan.get("plan_uid", None)

    # The function is supposed to create or replace UID
    new_plan = pq.set_new_plan_uuid(plan)

    assert "plan_uid" in new_plan
    assert isinstance(new_plan["plan_uid"], str)
    assert new_plan["plan_uid"] != uid


def test_uid_set(pq):
    """
    Basic test for functions associated with `_uid_set`
    """
    pq._uid_set_add("a")
    pq._uid_set_add("b")

    assert pq._is_uid_in_set("a") is True
    assert pq._is_uid_in_set("b") is True
    assert pq._is_uid_in_set("c") is False

    pq._uid_set_remove("a")
    assert pq._is_uid_in_set("a") is False
    assert pq._is_uid_in_set("b") is True

    pq._uid_set_clear()
    assert pq._is_uid_in_set("a") is False
    assert pq._is_uid_in_set("b") is False


def test_uid_set_initialize(pq):
    """
    Basic test for functions associated with ``_uid_set_initialize()``
    """

    async def testing():
        await pq.add_plan_to_queue({"name": "a"})
        await pq.add_plan_to_queue({"name": "b"})
        await pq.add_plan_to_queue({"name": "c"})
        plans = await pq.get_plan_queue()
        uid_set = set([_["plan_uid"] for _ in plans])

        await pq._uid_set_initialize()
        assert pq._uid_set == uid_set

    asyncio.run(testing())


def test_remove_plan(pq):
    """
    Basic test for functions associated with ``_remove_plan()``
    """

    async def testing():
        plan_list = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
        for plan in plan_list:
            await pq.add_plan_to_queue(plan)

        plans = await pq.get_plan_queue()
        plan_to_remove = [_ for _ in plans if _["name"] == "b"][0]

        # Remove one plan
        await pq._remove_plan(plan_to_remove)
        plans = await pq.get_plan_queue()
        assert len(plans) == 2

        # Add a copy of a plan (queue is not supposed to have copies in real life)
        plan_to_add = plans[0]
        await pq._r_pool.lpush(pq._name_plan_queue, json.dumps(plan_to_add))
        # Now remove both plans
        await pq._remove_plan(plan_to_add, single=False)  # Allow deleting multiple or no plans
        assert await pq.get_plan_queue_size() == 1

        # Delete the plan again (the plan is not in the queue, but it shouldn't raise an exception)
        await pq._remove_plan(plan_to_add, single=False)  # Allow deleting multiple or no plans
        assert await pq.get_plan_queue_size() == 1

        with pytest.raises(RuntimeError, match="One plans is expected to be removed"):
            await pq._remove_plan(plan_to_add)
        assert await pq.get_plan_queue_size() == 1

        # Now add 'plan_to_add' twice (create two copies)
        await pq._r_pool.lpush(pq._name_plan_queue, json.dumps(plan_to_add))
        await pq._r_pool.lpush(pq._name_plan_queue, json.dumps(plan_to_add))
        assert await pq.get_plan_queue_size() == 3
        # Attempt to delete two copies
        with pytest.raises(RuntimeError, match="One plans is expected to be removed"):
            await pq._remove_plan(plan_to_add)
        # Exception is raised, but both copies are deleted
        assert await pq.get_plan_queue_size() == 1

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
def test_get_plan_1(pq, pos, name):
    """
    Basic test for the function ``PlanQueueOperations.get_plan()``
    """

    async def testing():

        await pq.add_plan_to_queue({"name": "a"})
        await pq.add_plan_to_queue({"name": "b"})
        await pq.add_plan_to_queue({"name": "c"})
        assert await pq.get_plan_queue_size() == 3

        if name is not None:
            plan = await pq.get_plan(pos)
            assert plan["name"] == name
        else:
            with pytest.raises(IndexError, match="Index .* is out of range"):
                await pq.get_plan(pos)

    asyncio.run(testing())


def test_add_plan_to_queue_1(pq):
    """
    Basic test for the function ``PlanQueueOperations.add_plan_to_queue()``
    """

    async def add_plan(plan, n, **kwargs):
        plan_added, qsize = await pq.add_plan_to_queue(plan, **kwargs)
        assert plan_added["name"] == plan["name"], f"plan: {plan}"
        assert qsize == n, f"plan: {plan}"

    async def testing():
        await add_plan({"name": "a"}, 1)
        await add_plan({"name": "b"}, 2)
        await add_plan({"name": "c"}, 3, pos="back")
        await add_plan({"name": "d"}, 4, pos="front")
        await add_plan({"name": "e"}, 5, pos=0)  # front
        await add_plan({"name": "f"}, 6, pos=5)  # back (index == queue size)
        await add_plan({"name": "g"}, 7, pos=5)  # previous to last
        await add_plan({"name": "h"}, 8, pos=-1)  # previous to last
        await add_plan({"name": "i"}, 9, pos=3)  # arbitrary index
        await add_plan({"name": "j"}, 10, pos=100)  # back (index some large number)
        await add_plan({"name": "k"}, 11, pos=-10)  # front (precisely negative queue size)
        await add_plan({"name": "l"}, 12, pos=-100)  # front (index some large negative number)

        assert await pq.get_plan_queue_size() == 12

        plans = await pq.get_plan_queue()
        name_sequence = [_["name"] for _ in plans]
        assert name_sequence == ["l", "k", "e", "d", "a", "i", "b", "c", "g", "h", "f", "j"]

        await pq.clear_plan_queue()

    asyncio.run(testing())


def test_add_plan_to_queue_2_fail(pq):
    """
    Failing tests for the function ``PlanQueueOperations.add_plan_to_queue()``
    """

    async def testing():

        with pytest.raises(ValueError, match="Parameter 'pos' has incorrect value"):
            await pq.add_plan_to_queue({"name": "a"}, pos="something")

        with pytest.raises(TypeError, match=errmsg_wrong_plan_type):
            await pq.add_plan_to_queue("plan_is_not_string")

        # Duplicate plan UID
        plan = {"plan_uid": "abc", "name": "a"}
        await pq.add_plan_to_queue(plan)
        with pytest.raises(RuntimeError, match="Plan with UID .+ is already in the queue"):
            await pq.add_plan_to_queue(plan)

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
def test_pop_plan_from_queue_1(pq, pos, name):
    """
    Basic test for the function ``PlanQueueOperations.pop_plan_from_queue()``
    """

    async def testing():

        await pq.add_plan_to_queue({"name": "a"})
        await pq.add_plan_to_queue({"name": "b"})
        await pq.add_plan_to_queue({"name": "c"})
        assert await pq.get_plan_queue_size() == 3

        if name is not None:
            plan, qsize = await pq.pop_plan_from_queue(pos)
            assert plan["name"] == name
            assert qsize == 2
            assert await pq.get_plan_queue_size() == 2
            # Push the plan back to the queue (proves that UID is removed from '_uid_set')
            await pq.add_plan_to_queue(plan)
            assert await pq.get_plan_queue_size() == 3
        else:
            with pytest.raises(IndexError, match="Index .* is out of range"):
                await pq.pop_plan_from_queue(pos)

    asyncio.run(testing())


@pytest.mark.parametrize("pos", ["front", "back", 0, 1, -1])
def test_pop_plan_from_queue_2(pq, pos):
    """
    Test for the function ``PlanQueueOperations.pop_plan_from_queue()``:
    the case of empty queue.
    """

    async def testing():
        assert await pq.get_plan_queue_size() == 0
        with pytest.raises(IndexError, match="Index .* is out of range|Queue is empty"):
            await pq.pop_plan_from_queue(pos)

    asyncio.run(testing())


def test_pop_plan_from_queue_3_fail(pq):
    """
    Failing tests for the function ``PlanQueueOperations.pop_plan_from_queue()``
    """

    async def testing():
        with pytest.raises(ValueError, match="Parameter 'pos' has incorrect value"):
            await pq.pop_plan_from_queue(pos="something")

    asyncio.run(testing())


def test_clear_plan_queue(pq):
    """
    Test for ``PlanQueueOperations.clear_plan_queue`` function
    """

    async def testing():
        await pq.add_plan_to_queue({"name": "a"})
        await pq.add_plan_to_queue({"name": "b"})
        await pq.add_plan_to_queue({"name": "c"})
        # Set one of 3 plans as running (removes it from the queue)
        await pq.set_next_plan_as_running()

        assert await pq.get_plan_queue_size() == 2
        assert len(pq._uid_set) == 3

        # Clears the queue only (doesn't touch the running plan)
        await pq.clear_plan_queue()

        assert await pq.get_plan_queue_size() == 0
        assert len(pq._uid_set) == 1

        with pytest.raises(ValueError, match="Parameter 'pos' has incorrect value"):
            await pq.pop_plan_from_queue(pos="something")

    asyncio.run(testing())


def test_plan_to_history_functions(pq):
    """
    Test for ``PlanQueueOperations._add_plan_to_history()`` method.
    """

    async def testing():
        assert await pq.get_plan_history_size() == 0

        plans = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
        for plan in plans:
            await pq._add_plan_to_history(plan)
        assert await pq.get_plan_history_size() == 3

        plan_history = await pq.get_plan_history()

        assert len(plan_history) == 3
        assert plan_history == plans

        await pq.clear_plan_history()

        plan_history = await pq.get_plan_history()
        assert plan_history == []

    asyncio.run(testing())


def test_set_next_plan_as_running(pq):
    """
    Test for ``PlanQueueOperations.set_next_plan_as_running()`` function
    """

    async def testing():
        # Apply to empty queue
        assert await pq.get_plan_queue_size() == 0
        assert await pq.is_plan_running() is False
        assert await pq.set_next_plan_as_running() == {}
        assert await pq.get_plan_queue_size() == 0
        assert await pq.is_plan_running() is False

        # Apply to a queue with several plans
        await pq.add_plan_to_queue({"name": "a"})
        await pq.add_plan_to_queue({"name": "b"})
        await pq.add_plan_to_queue({"name": "c"})
        # Set one of 3 plans as running (removes it from the queue)
        assert await pq.set_next_plan_as_running() != {}

        assert await pq.get_plan_queue_size() == 2
        assert len(pq._uid_set) == 3

        # Apply if a plan is already running
        assert await pq.set_next_plan_as_running() == {}
        assert await pq.get_plan_queue_size() == 2
        assert len(pq._uid_set) == 3

    asyncio.run(testing())


def test_set_processed_plan_as_completed(pq):
    """
    Test for ``PlanQueueOperations.set_processed_plan_as_completed()`` function.
    The function moves currently running plan to history.
    """

    plans = [{"plan_uid": 1, "name": "a"}, {"plan_uid": 2, "name": "b"}, {"plan_uid": 3, "name": "c"}]

    def add_status_to_plans(plans, exit_status):
        plans = copy.deepcopy(plans)
        plans_modified = []
        for plan in plans:
            plan["exit_status"] = exit_status
            plans_modified.append(plan)
        return plans_modified

    async def testing():
        for plan in plans:
            await pq.add_plan_to_queue(plan)

        # No plan is running
        plan = await pq.set_processed_plan_as_completed(exit_status="completed")
        assert plan == {}

        # Execute the first plan
        await pq.set_next_plan_as_running()
        plan = await pq.set_processed_plan_as_completed(exit_status="completed")

        assert await pq.get_plan_queue_size() == 2
        assert await pq.get_plan_history_size() == 1
        assert plan["name"] == plans[0]["name"]
        assert plan["exit_status"] == "completed"

        plan_history = await pq.get_plan_history()
        plan_history_expected = add_status_to_plans(plans[0:1], "completed")
        assert plan_history == plan_history_expected

        # Execute the second plan
        await pq.set_next_plan_as_running()
        plan = await pq.set_processed_plan_as_completed(exit_status="completed")

        assert await pq.get_plan_queue_size() == 1
        assert await pq.get_plan_history_size() == 2
        assert plan["name"] == plans[1]["name"]
        assert plan["exit_status"] == "completed"

        plan_history = await pq.get_plan_history()
        plan_history_expected = add_status_to_plans(plans[0:2], "completed")
        assert plan_history == plan_history_expected

    asyncio.run(testing())


def test_set_processed_plan_as_stopped(pq):
    """
    Test for ``PlanQueueOperations.set_processed_plan_as_stopped()`` function.
    The function pushes running plan back to the queue and saves it in history as well.
    """
    plans = [{"plan_uid": 1, "name": "a"}, {"plan_uid": 2, "name": "b"}, {"plan_uid": 3, "name": "c"}]

    def add_status_to_plans(plans, exit_status):
        plans = copy.deepcopy(plans)
        plans_modified = []
        for plan in plans:
            plan["exit_status"] = exit_status
            plans_modified.append(plan)
        return plans_modified

    async def testing():
        for plan in plans:
            await pq.add_plan_to_queue(plan)

        # No plan is running
        plan = await pq.set_processed_plan_as_stopped(exit_status="stopped")
        assert plan == {}

        # Execute the first plan
        await pq.set_next_plan_as_running()
        plan = await pq.set_processed_plan_as_stopped(exit_status="stopped")

        assert await pq.get_plan_queue_size() == 3
        assert await pq.get_plan_history_size() == 1
        assert plan["name"] == plans[0]["name"]
        assert plan["exit_status"] == "stopped"

        plan_history = await pq.get_plan_history()
        plan_history_expected = add_status_to_plans([plans[0]], "stopped")
        assert plan_history == plan_history_expected

        # Execute the second plan
        await pq.set_next_plan_as_running()
        plan = await pq.set_processed_plan_as_stopped(exit_status="stopped")

        assert await pq.get_plan_queue_size() == 3
        assert await pq.get_plan_history_size() == 2
        assert plan["name"] == plans[0]["name"]
        assert plan["exit_status"] == "stopped"

        plan_history = await pq.get_plan_history()
        plan_history_expected = add_status_to_plans([plans[0], plans[0]], "stopped")
        assert plan_history == plan_history_expected

    asyncio.run(testing())
