import asyncio
import pytest
from bluesky_queueserver.manager.plan_queue_ops import PlanQueueOperations


errmsg_wrong_plan_type = "Parameter 'plan' should be of type dict"


def test_set_get_clear_running_plan_info():
    """
    Basic test for the following functions:
    `PlanQueueOperations.set_running_plan_info()`
    `PlanQueueOperations.get_running_plan_info()`
    `PlanQueueOperations.clear_running_plan_info()`
    """
    pq = PlanQueueOperations()

    async def testing():
        await pq.start()

        await pq.delete_pool_entries()
        assert await pq.get_running_plan_info() == {}

        some_plan = {"some_key": "some_value"}
        await pq.set_running_plan_info(some_plan)
        assert await pq.get_running_plan_info() == some_plan

        await pq.clear_running_plan_info()
        assert await pq.get_running_plan_info() == {}

        await pq.delete_pool_entries()
        assert await pq.get_running_plan_info() == {}

    asyncio.run(testing())


def test_set_running_plan_info_fail():
    """
    Failing test for the function `PlanQueueOperations.set_running_plan_info()`.
    """
    pq = PlanQueueOperations()

    async def testing():
        await pq.start()
        with pytest.raises(TypeError, match=errmsg_wrong_plan_type):
            await pq.set_running_plan_info([])

    asyncio.run(testing())


def test_get_plan_queue_size():
    """
    Basic tests for the function:
    `PlanQueueOperations.get_plan_queue_size()`
    `PlanQueueOperations.add_plan_to_queue()`
    `PlanQueueOperations.clear_plan_queue()`
    """
    pq = PlanQueueOperations()

    async def testing():
        await pq.start()

        await pq.delete_pool_entries()
        assert await pq.get_plan_queue_size() == 0

        await pq.add_plan_to_queue({"name": "a"})
        assert await pq.get_plan_queue_size() == 1

        await pq.add_plan_to_queue({"name": "b"})
        assert await pq.get_plan_queue_size() == 2

        await pq.clear_plan_queue()
        assert await pq.get_plan_queue_size() == 0

    asyncio.run(testing())


def test_get_plan_queue():
    """
    Basic test for the function `PlanQueueOperations.get_plan_queue()`
    """
    pq = PlanQueueOperations()

    async def testing():
        await pq.start()

        await pq.delete_pool_entries()
        assert await pq.get_plan_queue() == []

        await pq.add_plan_to_queue({"name": "a"})
        assert await pq.get_plan_queue() == [{"name": "a"}]

        await pq.add_plan_to_queue({"name": "b"})
        assert await pq.get_plan_queue() == [{"name": "a"}, {"name": "b"}]

        await pq.clear_plan_queue()
        assert await pq.get_plan_queue() == []

    asyncio.run(testing())


def test_pop_first_plan():
    """
    Basic test for the function `PlanQueueOperations.pop_first_plan()`
    """
    pq = PlanQueueOperations()

    async def testing():
        await pq.start()

        await pq.delete_pool_entries()
        assert await pq.pop_first_plan() == {}

        await pq.add_plan_to_queue({"name": "a"})
        await pq.add_plan_to_queue({"name": "b"})

        assert await pq.pop_first_plan() == {"name": "a"}
        assert await pq.pop_first_plan() == {"name": "b"}
        assert await pq.pop_first_plan() == {}

    asyncio.run(testing())


def test_pop_last_plan():
    """
    Basic test for the function `PlanQueueOperations.pop_first_plan()`
    """
    pq = PlanQueueOperations()

    async def testing():
        await pq.start()

        await pq.delete_pool_entries()
        assert await pq.pop_first_plan() == {}

        await pq.add_plan_to_queue({"name": "a"})
        await pq.add_plan_to_queue({"name": "b"})

        assert await pq.pop_last_plan() == {"name": "b"}
        assert await pq.pop_last_plan() == {"name": "a"}
        assert await pq.pop_last_plan() == {}

    asyncio.run(testing())


def test_add_plan_to_queue():
    """
    Basic test for the function `PlanQueueOperations.add_plan_to_queue()`
    """
    pq = PlanQueueOperations()

    async def testing():
        await pq.start()

        await pq.delete_pool_entries()
        assert await pq.add_plan_to_queue({"name": "a"}) == 1
        assert await pq.add_plan_to_queue({"name": "b"}) == 2
        assert await pq.get_plan_queue() == [{"name": "a"}, {"name": "b"}]

        await pq.clear_plan_queue()

    asyncio.run(testing())


def test_add_plan_to_queue_fail():
    """
    Failing case for the function `PlanQueueOperations.add_plan_to_queue()`.
    """
    pq = PlanQueueOperations()

    async def testing():
        await pq.start()
        with pytest.raises(TypeError, match=errmsg_wrong_plan_type):
            await pq.add_plan_to_queue("abc")  # Must be a dict

    asyncio.run(testing())


def test_push_plan_to_front_of_queue():
    """
    Basic test for the function `PlanQueueOperations.push_plan_to_front_of_queue()`
    """
    pq = PlanQueueOperations()

    async def testing():
        await pq.start()

        await pq.delete_pool_entries()
        assert await pq.push_plan_to_front_of_queue({"name": "a"}) == 1
        assert await pq.push_plan_to_front_of_queue({"name": "b"}) == 2
        assert await pq.get_plan_queue() == [{"name": "b"}, {"name": "a"}]

        await pq.clear_plan_queue()

    asyncio.run(testing())


def test_push_plan_to_front_of_queue_fail():
    """
    Failing case for the function `PlanQueueOperations.push_plan_to_front_of_queue()`.
    """
    pq = PlanQueueOperations()

    async def testing():
        await pq.start()
        with pytest.raises(TypeError, match=errmsg_wrong_plan_type):
            await pq.push_plan_to_front_of_queue("abc")  # Must be a dict

    asyncio.run(testing())
