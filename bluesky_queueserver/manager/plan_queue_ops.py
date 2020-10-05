import aioredis
import asyncio
import json
import uuid


class PlanQueueOperations:
    """
    The class supports operations with plan queue based on Redis. The public methods
    of the class are protected with ``asyncio.Lock``.

    Parameters
    ----------
    redis_host: str
        Address of Redis host.

    Examples
    --------

    .. code-block:: python

        pq = PlanQueueOperations()  # Redis located at `localhost`
        await pq.start()

        # Fill queue
        await pq.add_plan_to_queue(<plan1>)
        await pq.add_plan_to_queue(<plan2>)
        await pq.add_plan_to_queue(<plan3>)

        # Number of plans in the queue
        qsize = await pq.get_plan_queue_size()

        # Read the queue (as a list)
        queue = await pq.get_plan_queue()

        # Start the first plan (This doesn't actually execute the plan. It is just for bookkeeping.)
        plan = await pq.set_next_plan_as_running()
        # ...
        # Here place the code for executing the plan in dictionary `plan`

        # Again this only shows whether a plan was set as running. Expected to be True in
        #   this example.
        is_running = await pq.is_plan_running()

        # Assume that plan execution is completed, so move the plan to history
        #   This also clears the currently processed plan.
        plan = await pq.set_processed_plan_as_completed(exit_status="completed")

        # We are ready to start the next plan
        plan = await pq.set_next_plan_as_running()

        # Assume that we paused and then stopped the plan. Clear the running plan and
        #   push it back to the queue. Also create the respective history entry.
        plan = await pq.set_processed_plan_as_stopped(exit_status="stopped")
    """

    def __init__(self, redis_host="localhost"):
        self._redis_host = redis_host
        self._uid_set = set()
        self._r_pool = None

        self._name_running_plan = "running_plan"
        self._name_plan_queue = "plan_queue"
        self._name_plan_history = "plan_history"

        self._lock = None

    async def start(self):
        """
        Create the pool and initialize the set of UIDs from the queue if it exists in the pool.
        """
        if not self._r_pool:  # Initialize only once
            self._lock = asyncio.Lock()
            async with self._lock:
                self._r_pool = await aioredis.create_redis_pool(f"redis://{self._redis_host}", encoding="utf8")
                await self._queue_clean()
                await self._uid_set_initialize()

    async def _queue_clean(self):
        """
        Delete all the invalid queue entries (there could be some entries from failed unit tests).
        """
        pq = await self._get_plan_queue()

        def verify_plan(plan):
            # The criteria may be changed.
            return "plan_uid" in plan

        plans_to_remove = []
        for plan in pq:
            if not verify_plan(plan):
                plans_to_remove.append(plan)

        for plan in plans_to_remove:
            await self._remove_plan(plan, single=False)

        # Clean running plan info also (on the development computer it may contain garbage)
        plan = await self._get_running_plan_info()
        if plan and not verify_plan(plan):
            await self._clear_running_plan_info()

    async def _delete_pool_entries(self):
        """
        See ``self.delete_pool_entries()`` method.
        """
        await self._r_pool.delete(self._name_running_plan)
        await self._r_pool.delete(self._name_plan_queue)
        await self._r_pool.delete(self._name_plan_history)
        self._uid_set_clear()

    async def delete_pool_entries(self):
        """
        Delete pool entries used by RE Manager. This method is mostly intended for use in testing,
        but may be used for other purposes if needed.
        """
        async with self._lock:
            await self._delete_pool_entries()

    def _verify_plan_type(self, plan):
        """
        Check that the plan is a dictionary.
        """
        if not isinstance(plan, dict):
            raise TypeError(f"Parameter 'plan' should be a dictionary: '{plan}', (type '{type(plan)}')")

    def _verify_plan(self, plan):
        """
        Verify that plan structure is valid enough to be put in the queue.
        Current checks: plan is a dictionary, ``plan_uid`` key is present, Plan with the UID is not in
        the queue or currently running.
        """
        self._verify_plan_type(plan)
        # Verify plan UID
        if "plan_uid" not in plan:
            raise ValueError("Plan does not have UID.")
        if self._is_uid_in_set(plan["plan_uid"]):
            raise RuntimeError(f"Plan with UID {plan['plan_uid']} is already in the queue")

    def _new_plan_uid(self):
        """
        Generate UID for a plan.
        """
        return str(uuid.uuid4())

    def set_new_plan_uuid(self, plan):
        """
        Replaces Plan UID with a new one or creates a new UID.

        Parameters
        ----------
        plan: dict
            Dictionary of plan parameters. The dictionary may or may not have the key ``plan_uid``.

        Returns
        -------
        dict
            Plan with new UID.
        """
        self._verify_plan_type(plan)
        plan["plan_uid"] = self._new_plan_uid()
        return plan

    # --------------------------------------------------------------------------
    #                          Operations with UID set
    # TODO: in future consider replacing this method with `self._uid_set.clear()`
    #   The following private methods were introduced in case the set of UIDs
    #   need to be moved to a different structure (e.g. to Redis). They could
    #   be removed if such need does not materialize.
    def _uid_set_clear(self):
        """
        Clear ``self._uid_set``.
        """
        self._uid_set.clear()

    # TODO: in future consider replacing this method with `in self._uid_set`
    def _is_uid_in_set(self, uid):
        """
        Checks if UID exists in ``self._uid_set``.
        """
        return uid in self._uid_set

    # TODO: in future consider replacing this method with `self._uid_set.add()`
    def _uid_set_add(self, uid):
        """
        Add UID to ``self._uid_set``.
        """
        self._uid_set.add(uid)

    # TODO: in future consider replacing this method with `self._uid_set.remove()`
    def _uid_set_remove(self, uid):
        """
        Remove UID from ``self._uid_set``.
        """
        self._uid_set.remove(uid)

    async def _uid_set_initialize(self):
        """
        Initialize ``self._uid_set`` with UIDs extracted from the plans in the queue.
        """
        pq = await self._get_plan_queue()
        self._uid_set_clear()
        # Go over all plans in the queue
        for plan in pq:
            self._uid_set_add(plan["plan_uid"])
        # If plan is currently running
        plan = await self._get_running_plan_info()
        if plan:
            self._uid_set_add(plan["plan_uid"])

    # -------------------------------------------------------------
    #                   Currently Running Plan

    async def _is_plan_running(self):
        """
        See ``self.is_plan_running()`` method.
        """
        return bool(await self._get_running_plan_info())

    async def is_plan_running(self):
        """
        Check if a plan is set as running. True does not indicate that the plan is actually running.

        Returns
        -------
        boolean
            True - a plan is set as running, False otherwise.
        """
        async with self._lock:
            return await self._is_plan_running()

    async def _get_running_plan_info(self):
        """
        See ``self._get_running_plan_info()`` method.
        """
        plan = await self._r_pool.get(self._name_running_plan)
        return json.loads(plan) if plan else {}

    async def get_running_plan_info(self):
        """
        Read info on the currently running plan from Redis.

        Returns
        -------
        dict
            Dictionary representing currently running plan. Empty dictionary if
            no plan is currently running (key value is ``{}`` or the key does not exist).
        """
        async with self._lock:
            return await self._get_running_plan_info()

    async def _set_running_plan_info(self, plan):
        """
        Write info on the currently running plan to Redis

        Parameters
        ----------
        plan: dict
            dictionary that contains plan parameters
        """
        await self._r_pool.set(self._name_running_plan, json.dumps(plan))

    async def _clear_running_plan_info(self):
        """
        Clear info on the currently running plan in Redis.
        """
        await self._set_running_plan_info({})

    # -------------------------------------------------------------
    #                       Plan Queue

    async def _get_plan_queue_size(self):
        """
        See ``self.get_plan_queue_size()`` method.
        """
        return await self._r_pool.llen(self._name_plan_queue)

    async def get_plan_queue_size(self):
        """
        Get the number of plans in the queue.

        Returns
        -------
        int
            The number of plans in the queue.
        """
        async with self._lock:
            return await self._get_plan_queue_size()

    async def _get_plan_queue(self):
        """
        See ``self.get_plan_queue()`` method.
        """
        all_plans_json = await self._r_pool.lrange(self._name_plan_queue, 0, -1)
        return [json.loads(_) for _ in all_plans_json]

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
        async with self._lock:
            return await self._get_plan_queue()

    async def _get_plan(self, pos):
        """
        See ``self._get_plan()`` method.
        """
        if pos == "back":
            index = -1
        elif pos == "front":
            index = 0
        elif isinstance(pos, int):
            index = pos
        else:
            raise TypeError(f"Parameter 'pos' has incorrect type: pos={str(pos)} (type={type(pos)})")

        plan_json = await self._r_pool.lindex(self._name_plan_queue, index)
        if plan_json is None:
            raise IndexError(f"Index '{index}' is out of range (parameter pos = '{pos}')")

        plan = json.loads(plan_json) if plan_json else {}
        return plan

    async def get_plan(self, pos):
        """
        Get plan at a given position.

        Parameters
        ----------
        pos: int or str
            Position of the element ``(0, ..)`` or ``(-1, ..)``, ``front`` or ``back``.

        Returns
        -------
        dict
            Dictionary of plan parameters.

        Raises
        ------
        TypeError
            Incorrect value of ``pos`` (most likely a string different from ``front`` or ``back``)
        IndexError
            No element with position ``pos`` exists in the queue (index is out of range).
        """
        async with self._lock:
            return await self._get_plan(pos)

    async def _remove_plan(self, plan, single=True):
        """
        Remove exactly a plan from the queue. If ``single=True`` then the exception is
        raised in case of no or multiple matching plans are found in the queue.
        The function is not part of user API and shouldn't be used on exception from
        the other methods of the class.

        Parameters
        ----------
        plan: dict
            Dictionary of plan parameters. Must be identical to the plan that is
            expected to be deleted.
        single: boolean
            True - RuntimeError exception is raised if no or more than one matching
            plan is found, the plans are removed anyway; False - no exception is
            raised.

        Raises
        ------
        RuntimeError
            No or multiple matching plans are removed and ``single=True``.
        """
        n_rem_plans = await self._r_pool.lrem(self._name_plan_queue, 0, json.dumps(plan))
        if (n_rem_plans != 1) and single:
            raise RuntimeError(
                f"The number of removed plans is {n_rem_plans}. One plans is expected to be removed."
            )

    async def _pop_plan_from_queue(self, pos="back"):
        """
        See ``self._pop_plan_from_queue()`` method
        """
        if pos == "back":
            plan_json = await self._r_pool.rpop(self._name_plan_queue)
            if plan_json is None:
                raise IndexError("Queue is empty")
            plan = json.loads(plan_json) if plan_json else {}
        elif pos == "front":
            plan_json = await self._r_pool.lpop(self._name_plan_queue)
            if plan_json is None:
                raise IndexError("Queue is empty")
            plan = json.loads(plan_json) if plan_json else {}
        elif isinstance(pos, int):
            plan = await self._get_plan(pos)
            if plan:
                await self._remove_plan(plan)
        else:
            raise ValueError(f"Parameter 'pos' has incorrect value:: pos={str(pos)} (type={type(pos)})")

        if plan:
            self._uid_set_remove(plan["plan_uid"])

        qsize = await self._get_plan_queue_size()

        return plan, qsize

    async def pop_plan_from_queue(self, pos="back"):
        """
        Pop a plan from the queue. Raises ``IndexError`` if plan with index ``pos`` is unavailable
        or if the queue is empty.

        Parameters
        ----------
        pos: int or str
            Integer index specified position in the queue. Available string values: "front" or "back".
            The range for the index is ``-qsize..qsize-1``: ``0, -qsize`` - front element of the queue,
            ``-1, qsize-1`` - back element of the queue.

        Returns
        -------
        dict or None
            The last plan in the queue represented as a dictionary.

        Raises
        ------
        ValueError
            Incorrect value of the parameter ``pos`` (typically unrecognized string).
        IndexError
            Position ``pos`` does not exist or the queue is empty.
        """
        async with self._lock:
            return await self._pop_plan_from_queue(pos=pos)

    async def _add_plan_to_queue(self, plan, pos="back"):
        """
        See ``self.add_plan_to_queue`` method.
        """
        if "plan_uid" not in plan:
            plan = self.set_new_plan_uuid(plan)
        else:
            self._verify_plan(plan)

        qsize0 = await self._get_plan_queue_size()
        if pos == "back" or (isinstance(pos, int) and pos >= qsize0):
            qsize = await self._r_pool.rpush(self._name_plan_queue, json.dumps(plan))
        elif pos == "front" or (isinstance(pos, int) and (pos == 0 or pos <= -qsize0)):
            qsize = await self._r_pool.lpush(self._name_plan_queue, json.dumps(plan))
        elif isinstance(pos, int):
            # Put the position in the range
            plan_to_displace = await self._get_plan(pos)
            if plan_to_displace:
                qsize = await self._r_pool.linsert(
                    self._name_plan_queue, json.dumps(plan_to_displace), json.dumps(plan), before=True
                )
            else:
                raise RuntimeError(f"Could not find an existing plan at {pos}. Queue size: {qsize0}")
        else:
            raise ValueError(f"Parameter 'pos' has incorrect value: pos='{str(pos)}' (type={type(pos)})")

        self._uid_set_add(plan["plan_uid"])
        return plan, qsize

    async def add_plan_to_queue(self, plan, pos="back"):
        """
        Add the plan to the back of the queue. If position is integer, it is
        clipped to fit within the range of meaningful indices. For the index
        too large or too low, the plan is pushed to the front or the back of the queue.

        Parameters
        ----------
        plan: dict
            Plan represented as a dictionary of parameters
        pos: int or str
            Integer that specifies the position index, "front" or "back".
            If ``pos`` is in the range ``1..qsize-1`` the plan is inserted
            to the specified position and plans at positions ``pos..qsize-1``
            are shifted by one position to the right. If ``-qsize<pos<0`` the
            plan is inserted at the positon counting from the back of the queue
            (-1 - the last element of the queue). If ``pos>=qsize``,
            the plan is added to the back of the queue. If ``pos==0`` or
            ``pos<=-qsize``, the plan is pushed to the front of the queue.

        Returns
        -------
        int
            The new size of the queue.

        Raises
        ------
        ValueError
            Incorrect value of the parameter ``pos`` (typically unrecognized string).
        TypeError
            Incorrect type of ``plan`` (should be dict)
        """
        async with self._lock:
            return await self._add_plan_to_queue(plan, pos=pos)

    async def _clear_plan_queue(self):
        """
        See ``self.clear_plan_queue()`` method.
        """
        while await self._get_plan_queue_size():
            await self._pop_plan_from_queue()

    async def clear_plan_queue(self):
        """
        Remove all entries from the plan queue. Does not touch the running plan.
        The plan may be pushed back into the queue if it is stopped.
        """
        async with self._lock:
            await self._clear_plan_queue()

    # -----------------------------------------------------------------------
    #                          Plan History

    async def _add_plan_to_history(self, plan):
        """
        Add the plan to history.

        Parameters
        ----------
        plan: dict
            Plan represented as a dictionary of parameters. No verifications are performed
            on the plan. The function is not intended to be used outside of this class.

        Returns
        -------
        int
            The new size of the history.
        """
        history_size = await self._r_pool.rpush(self._name_plan_history, json.dumps(plan))
        return history_size

    async def _get_plan_history_size(self):
        """
        See ``self.get_plan_history_size()`` method.
        """
        return await self._r_pool.llen(self._name_plan_history)

    async def get_plan_history_size(self):
        """
        Get the number of items in the plan history.

        Returns
        -------
        int
            The number of plans in the history.
        """
        async with self._lock:
            return await self._get_plan_history_size()

    async def _get_plan_history(self):
        """
        See ``self.get_plan_history()`` method.
        """
        all_plans_json = await self._r_pool.lrange(self._name_plan_history, 0, -1)
        return [json.loads(_) for _ in all_plans_json]

    async def get_plan_history(self):
        """
        Get the list of all plans in the history. The first element of the list is
        the oldest history entry.

        Returns
        -------
        list(dict)
            The list of plans in the queue. Each plan is represented as a dictionary.
            Empty list is returned if the queue is empty.
        """
        async with self._lock:
            return await self._get_plan_history()

    async def _clear_plan_history(self):
        """
        See ``self.clear_plan_history()`` method.
        """
        while await self._get_plan_history_size():
            await self._r_pool.rpop(self._name_plan_history)

    async def clear_plan_history(self):
        """
        Remove all entries from the plan queue. Does not touch the running plan.
        The plan may be pushed back into the queue if it is stopped.
        """
        async with self._lock:
            await self._clear_plan_history()

    # ----------------------------------------------------------------------
    #          Standard plan operations during queue execution

    async def _set_next_plan_as_running(self):
        """
        See ``self.set_next_plan_as_running()`` method.
        """
        # UID remains in the `self._uid_set` after this operation.
        if not await self._is_plan_running():
            plan_json = await self._r_pool.lpop(self._name_plan_queue)
            if plan_json:
                plan = json.loads(plan_json)
                await self._set_running_plan_info(plan)
            else:
                plan = {}
        else:
            plan = {}
        return plan

    async def set_next_plan_as_running(self):
        """
        Sets the next plan from the queue as a running plan. The plan is removed
        from the queue. UID remains in ``self._uid_set``, i.e. plan with the same UID
        may not be added to the queue while it is being executed.

        Returns
        -------
        dict
            The plan that was set as currently running. If another plan is currently
            running or the queue is empty, then ``{}`` is returned.
        """
        async with self._lock:
            return await self._set_next_plan_as_running()

    async def _set_processed_plan_as_completed(self, exit_status):
        """
        See ``self.set_processed_plan_as_completed`` method.
        """
        # Note: UID remains in the `self._uid_set` after this operation
        if await self._is_plan_running():
            plan = await self._get_running_plan_info()
            plan["exit_status"] = exit_status
            await self._clear_running_plan_info()
            self._uid_set_remove(plan["plan_uid"])
            await self._add_plan_to_history(plan)
        else:
            plan = {}
        return plan

    async def set_processed_plan_as_completed(self, exit_status):
        """
        Moves currently executed plan to history and sets ``exit_status`` key.
        UID is removed from ``self._uid_set``, so a copy of the plan with
        the same UID may be added to the queue.

        Parameters
        ----------
        exit_status: str
            Completion status of the plan.

        Returns
        -------
        dict
            The plan added to the history including ``exit_status``. If another no plan is currently
            running, then ``{}`` is returned.
        """
        async with self._lock:
            return await self._set_processed_plan_as_completed(exit_status=exit_status)

    async def _set_processed_plan_as_stopped(self, exit_status):
        """
        See ``self.set_prcessed_plan_as_stopped()`` method.
        """
        # Note: UID is removed from `self._uid_set`.
        if await self._is_plan_running():
            plan = await self._get_running_plan_info()
            await self._clear_running_plan_info()
            await self._r_pool.lpush(self._name_plan_queue, json.dumps(plan))
            plan["exit_status"] = exit_status
            await self._add_plan_to_history(plan)
        else:
            plan = {}
        return plan

    async def set_processed_plan_as_stopped(self, exit_status):
        """
        Pushes currently executed plan to the beginning of the queue and adds
        it to history with additional sets ``exit_status`` key.
        UID is remains in ``self._uid_set``.

        Parameters
        ----------
        exit_status: str
            Completion status of the plan.

        Returns
        -------
        dict
            The plan added to the history including ``exit_status``. If another no plan is currently
            running, then ``{}`` is returned.
        """
        async with self._lock:
            return await self._set_processed_plan_as_stopped(exit_status=exit_status)
