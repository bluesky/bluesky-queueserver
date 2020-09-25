import aioredis


class PlanQueueOperations:

    def __init__(self):
        self._r_pool = await aioredis.create_redis_pool(
            'redis://localhost', encoding='utf8')
        self._name_running_plan = "running_plan"
        self._name_plan_queue = "plan_queue"

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
        if (not await self.exists_running_plan_info()) \
                or (not await self.get_running_plan_info()):
            await self.clear_running_plan_info()

    async def set_running_plan_info(self, plan):
        """
        Write info on the currently running to Redis
        """
        await self._r_pool.set(self._name_running_plan, json.dumps(plan))

    async def get_running_plan_info(self):
        """
        Read info on the currently running plan from Redis
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
        Returns the number of plan in the queue
        """
        return await self._r_pool.llen(self._name_plan_queue)

    async def get_plan_queue(self):
        """
        Returns the list of all plans in the queue
        """
        return await self._r_pool.lrange(self._name_plan_queue, 0, -1)

    async def pop_first_plan(self):
        """
        Pop the first plan from the queue
        """
        return await self._r_pool.lpop(self._name_plan_queue)

    async def pop_last_plan(self):
        """
        Pop the first plan from the queue
        """
        return await self._r_pool.rpop(self._name_plan_queue)

    async def add_plan_to_queue(self, plan):
        """
        Add the plan to the back of the queue
        """
        return await self._r_pool.rpush(self._name_plan_queue, plan)

    async def clear_plan_queue(self):
        """
        Remove all entries from the plan queue.
        """
        while True:
            plan = await self.pop_last_plan()
            if plan is None:
                break
