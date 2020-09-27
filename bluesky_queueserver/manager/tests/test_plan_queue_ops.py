import asyncio
import pytest
from bluesky_queueserver.manager.plan_queue_ops import PlanQueueOperations


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
        with pytest.raises(TypeError, match="Parameter 'plan' should be of type dict"):
            await pq.set_running_plan_info([])

    asyncio.run(testing())
