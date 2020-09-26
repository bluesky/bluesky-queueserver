import aioredis
import json


class PlanQueueOperations:
    def __init__(self):
        self._name_running_plan = "running_plan"
        self._name_plan_queue = "plan_queue"
        self._r_pool = None

    async def start(self):
        self._r_pool = await aioredis.create_redis_pool("redis://localhost", encoding="utf8")

    async def delete_pool_entries(self):
        """
        Delete pool entries used by RE Manager. This method is mostly for use in testing.
        """
        await self._r_pool.delete(self._name_running_plan)
        await self._r_pool.delete(self._name_plan_queue)

    # -------------------------------------------------------------
    #                       Running Plan

    async def exists_running_plan_info(self):
        """
        Check if plan exists in the ppol
        """
        return await self._r_pool.exists(self._name_running_plan)

    async def init_running_plan_info(self):
        """
        Initialize running plan info: create Redis entry that hold empty plan ({})
        a record doesn't exist.
        """
        # Create entry 'running_plan' in the pool if it does not exist yet
        if (not await self.exists_running_plan_info()) or (not await self.get_running_plan_info()):
            await self.clear_running_plan_info()

    async def set_running_plan_info(self, plan):
        """
        Write info on the currently running to Redis
        """
        await self._r_pool.set(self._name_running_plan, json.dumps(plan))

    async def get_running_plan_info(self):
        """
        Read info on the currently running plan from Redis.

        Returns
        -------
        dict
            Dictionary representing currently running plan. Empty dictionary if
            no plan is currently running.
        """
        return json.loads(await self._r_pool.get(self._name_running_plan))

    async def clear_running_plan_info(self):
        """
        Clear info on the currently running plan in Redis.
        """
        await self.set_running_plan_info({})

    # -------------------------------------------------------------
    #                       Plan Queue
    async def get_plan_queue_size(self):
        """
        Get number of plan in the queue.

        Returns
        -------
        int
            The number of plans in the queue.
        """
        return await self._r_pool.llen(self._name_plan_queue)

    async def get_plan_queue(self):
        """
        Get the list of all plans in the queue. The first element of the list is the first
        plan in the queue.

        Returns
        -------
        list(dict)
            The list of plans in the queue. Each plan is represented as a dictionary.
            Empty list is returned if the queue is empty.
        """
        all_plans_json = await self._r_pool.lrange(self._name_plan_queue, 0, -1)
        return [json.loads(_) for _ in all_plans_json]

    async def pop_first_plan(self):
        """
        Pop the first plan from the queue.

        Returns
        -------
        dict or None
            The first plan in the queue represented as a dictionary or None if the queue is empty.
        """
        plan_json = await self._r_pool.lpop(self._name_plan_queue)
        plan = json.loads(plan_json) if plan_json is not None else None
        return plan

    async def pop_last_plan(self):
        """
        Pop the last plan from the queue

        Returns
        -------
        dict or None
            The first plan in the queue represented as a dictionary or None if the queue is empty.
        """
        plan_json = await self._r_pool.rpop(self._name_plan_queue)
        plan = json.loads(plan_json) if plan_json is not None else None
        return plan

    async def push_plan_to_front_of_queue(self, plan):
        """
        Add the plan to the the front of the queue.

        Parameters
        ----------
        plan: dict
            Plan represented as a dictionary of parameters
        """
        return await self._r_pool.lpush(self._name_plan_queue, json.dumps(plan))

    async def add_plan_to_queue(self, plan):
        """
        Add the plan to the back of the queue.

        Parameters
        ----------
        plan: dict
            Plan represented as a dictionary of parameters
        """
        return await self._r_pool.rpush(self._name_plan_queue, json.dumps(plan))

    async def clear_plan_queue(self):
        """
        Remove all entries from the plan queue.
        """
        while True:
            plan = await self.pop_last_plan()
            if plan is None:
                break
