import aioredis
import json


class PlanQueueOperations:
    def __init__(self, redis_host="localhost"):
        self._redis_host = redis_host
        self._name_running_plan = "running_plan"
        self._name_plan_queue = "plan_queue"
        self._r_pool = None

    def _verify_plan(self, plan):
        if not isinstance(plan, dict):
            raise TypeError(f"Parameter 'plan' should be of type dict: '{plan}', (type '{type(plan)}')")

    async def start(self):
        self._r_pool = await aioredis.create_redis_pool(f"redis://{self._redis_host}", encoding="utf8")

    async def delete_pool_entries(self):
        """
        Delete pool entries used by RE Manager. This method is mostly intended for use in testing.
        """
        await self._r_pool.delete(self._name_running_plan)
        await self._r_pool.delete(self._name_plan_queue)

    # -------------------------------------------------------------
    #                   Currently Running Plan

    async def set_running_plan_info(self, plan):
        """
        Write info on the currently running plan to Redis

        Parameters
        ----------
        plan: dict
            dictionary that contains plan parameters
        """
        self._verify_plan(plan)
        await self._r_pool.set(self._name_running_plan, json.dumps(plan))

    async def get_running_plan_info(self):
        """
        Read info on the currently running plan from Redis.

        Returns
        -------
        dict
            Dictionary representing currently running plan. Empty dictionary if
            no plan is currently running (key value is `{}` or the key does not exist).
        """
        plan = await self._r_pool.get(self._name_running_plan)
        return json.loads(plan) if plan else {}

    async def clear_running_plan_info(self):
        """
        Clear info on the currently running plan in Redis.
        """
        await self.set_running_plan_info({})

    # -------------------------------------------------------------
    #                       Plan Queue

    async def get_plan_queue_size(self):
        """
        Get the number of plans in the queue.

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
            The first plan in the queue represented as a dictionary or `{}` if the queue is empty.
        """
        plan_json = await self._r_pool.lpop(self._name_plan_queue)
        plan = json.loads(plan_json) if plan_json else {}
        return plan

    async def pop_last_plan(self):
        """
        Pop the last plan from the queue

        Returns
        -------
        dict or None
            The last plan in the queue represented as a dictionary or None if the queue is empty.
        """
        plan_json = await self._r_pool.rpop(self._name_plan_queue)
        plan = json.loads(plan_json) if plan_json else {}
        return plan

    async def push_plan_to_front_of_queue(self, plan):
        """
        Add the plan to the the front of the queue.

        Parameters
        ----------
        plan: dict
            Plan represented as a dictionary of parameters

        Returns
        -------
        int
            The new size of the queue.
        """
        self._verify_plan(plan)
        return await self._r_pool.lpush(self._name_plan_queue, json.dumps(plan))

    async def add_plan_to_queue(self, plan):
        """
        Add the plan to the back of the queue.

        Parameters
        ----------
        plan: dict
            Plan represented as a dictionary of parameters

        Returns
        -------
        int
            The new size of the queue.
        """
        self._verify_plan(plan)
        return await self._r_pool.rpush(self._name_plan_queue, json.dumps(plan))

    async def clear_plan_queue(self):
        """
        Remove all entries from the plan queue.
        """
        while await self.get_plan_queue_size():
            await self.pop_last_plan()
