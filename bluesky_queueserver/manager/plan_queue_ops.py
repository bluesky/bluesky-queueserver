import aioredis
import asyncio
import copy
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
        await pq.add_item_to_queue(<plan1>)
        await pq.add_item_to_queue(<plan2>)
        await pq.add_item_to_queue(<instruction1>)
        await pq.add_item_to_queue(<plan3>)

        # Number of plans in the queue
        qsize = await pq.get_queue_size()

        # Read the queue (as a list)
        queue, _ = await pq.get_queue()

        # Start the first plan (This doesn't actually execute the plan. It is just for bookkeeping.)
        plan = await pq.set_next_item_as_running()
        # ...
        # Here place the code for executing the plan in dictionary `plan`

        # Again this only shows whether a plan was set as running. Expected to be True in
        #   this example.
        is_running = await pq.is_item_running()

        # Assume that plan execution is completed, so move the plan to history
        #   This also clears the currently processed plan.
        plan = await pq.set_processed_item_as_completed(exit_status="completed", run_uids=[])

        # We are ready to start the next plan
        plan = await pq.set_next_item_as_running()

        # Assume that we paused and then stopped the plan. Clear the running plan and
        #   push it back to the queue. Also create the respective history entry.
        plan = await pq.set_processed_item_as_stopped(exit_status="stopped")
    """

    def __init__(self, redis_host="localhost"):
        self._redis_host = redis_host
        self._uid_dict = dict()
        self._r_pool = None

        self._name_running_plan = "running_plan"
        self._name_plan_queue = "plan_queue"
        self._name_plan_history = "plan_history"
        self._name_plan_queue_mode = "plan_queue_mode"

        # Plan queue UID is expected to change each time the contents of the queue is changed.
        #   Since `self._uid_dict` is modified each time the queue is updated, it is sufficient
        #   to update Plan queue UID in the functions that update `self._uid_dict`.
        self._plan_queue_uid = self.new_item_uid()
        # Plan history UID is expected to change each time the history is changed.
        self._plan_history_uid = self.new_item_uid()

        self._lock = None

        # Settings that determine the mode of queue operation. The set of supported modes
        #      may be extended if additional modes are to be implemented. The mode will be saved in
        #      Redis, so that it is not modified between restarts of the manager.
        #   Loop mode: loop, True/False. If enabled, then each executed item (plan or instruction)
        #      will be placed to the back of the queue.
        self._plan_queue_mode_default = {"loop": False}
        self._plan_queue_mode = self.plan_queue_mode_default

    @property
    def plan_queue_uid(self):
        """
        Get current plan queue UID (str). Note, that the UID may be updated multiple times during
        complex queue operations, so the returned UID may not represent a valid queue state.
        The intended use: the changes of UID could be monitored to detect changes in the queue
        without accessing the queue. If the UID is different from UID returned by
        ``PlanQueueOperations.get_queue()``, then the contents of the queue changed.
        """
        return self._plan_queue_uid

    @property
    def plan_history_uid(self):
        """
        Get current plan history UID. See notes for ``PlanQueueOperations.plan_queue_uid``.
        """
        return self._plan_history_uid

    @property
    def plan_queue_mode(self):
        """
        Returns current plan queue mode. Plan queue mode is a dictionary with parameters
        used for selection of the algorithm(s) for handling queue items. Supported parameters:
        ``loop (boolean)`` enables and disables the loop mode.
        """
        return self._plan_queue_mode.copy()

    @property
    def plan_queue_mode_default(self):
        """
        Returns the default queue mode (default settings)
        """
        return self._plan_queue_mode_default.copy()

    def _validate_plan_queue_mode(self, plan_queue_mode):
        """
        Validate the dictionary 'plan_queue_mode'. Check that the dictionary contains all
        the required parameters and no unsupported parameters.

        Parameters
        ----------
        plan_queue_mode : dict
            Dictionary that contains plan queue mode. See ``self.plan_queue_mode_default``.
        """
        # It is assumed that 'plan_queue_mode' will be a single-level dictionary that contains
        #   simple types (bool, int etc), so the following code provide better error reporting
        #   than schema validation.
        expected_params = {"loop": bool}
        missing_keys = set(expected_params.keys())
        for k, v in plan_queue_mode.items():
            if k not in expected_params:
                raise ValueError(
                    f"Unsupported plan queue mode parameter '{k}': "
                    f"supported parameters {list(expected_params.keys())}"
                )
            missing_keys.remove(k)
            key_type = expected_params.get(k)  # Using [k] makes PyCharm to display annoying error
            if not isinstance(v, key_type):
                raise TypeError(
                    f"Unsupported type '{type(v)}' of the parameter '{k}': " f"expected type '{key_type}'"
                )
        if missing_keys:
            raise ValueError(
                f"Parameters {missing_keys} are missing from 'plan_queue_mode' dictionary. "
                f"The following keys are expected: {list(expected_params.keys())}"
            )

    async def _load_plan_queue_mode(self):
        """
        Load plan queue mode from Redis.
        """
        queue_mode = await self._r_pool.get(self._name_plan_queue_mode)
        self._plan_queue_mode = json.loads(queue_mode) if queue_mode else self.plan_queue_mode_default

    async def set_plan_queue_mode(self, plan_queue_mode, *, update=True):
        """
        Set plan queue mode. The plan queue mode can be a string ``default`` or a dictionary with
        parameters. See ``self.plan_queue_mode_default`` for an example of the parameter dictionary.

        Parameters
        ----------
        plan_queue_mode : dict or str
            The dictionary of parameters that define queue mode. If ``update`` is ``True``, then
            the dictionary may contain only the parameters that need to be updated. Otherwise
            ``plan_queue_mode`` dictionary must contain full valid parameter dictionary. The function
            fails if the dictionary contains unsupported parameters. Calling the function with the
            string value ``plan_queue_mode="default"`` will reset all the parameters to the default
            values.
        update : boolean (optional)
            Indicates if the dictionary ``plan_queue_mode`` should be used to update mode parameters.
            If ``True``, then the dictionary may contain only the parameters that should be changed.
        """
        if not isinstance(plan_queue_mode, dict) and plan_queue_mode != "default":
            raise TypeError(
                f"Unsupported type '{type(plan_queue_mode)}' of the parameter 'plan_queue_mode' "
                f"({plan_queue_mode}). The parameter types: ('dict', 'str'). Supported "
                f"string value: 'default'"
            )

        if plan_queue_mode == "default":
            plan_queue_mode = self.plan_queue_mode_default
        elif update:
            # Generate full parameter dictionary based on the existing and submitted parameters.
            queue_mode = self.plan_queue_mode  # Create a copy of current parameters
            queue_mode.update(plan_queue_mode)
            plan_queue_mode = queue_mode

        self._validate_plan_queue_mode(plan_queue_mode)

        # Prevent changes of the queue mode in the middle of queue operations.
        async with self._lock:
            self._plan_queue_mode = plan_queue_mode.copy()
            await self._r_pool.set(self._name_plan_queue_mode, json.dumps(self._plan_queue_mode))

    async def start(self):
        """
        Create the pool and initialize the set of UIDs from the queue if it exists in the pool.
        """
        if not self._r_pool:  # Initialize only once
            self._lock = asyncio.Lock()
            async with self._lock:
                self._r_pool = await aioredis.create_redis_pool(f"redis://{self._redis_host}", encoding="utf8")
                await self._queue_clean()
                await self._uid_dict_initialize()
                await self._load_plan_queue_mode()

                self._plan_queue_uid = self.new_item_uid()
                self._plan_history_uid = self.new_item_uid()

    async def _queue_clean(self):
        """
        Delete all the invalid queue entries (there could be some entries from failed unit tests).
        """
        pq, _ = await self._get_queue()

        def verify_item(item):
            # The criteria may be changed.
            return "item_uid" in item

        items_to_remove = []
        for item in pq:
            if not verify_item(item):
                items_to_remove.append(item)

        for item in items_to_remove:
            await self._remove_item(item, single=False)

        # Clean running plan info also (on the development computer it may contain garbage)
        item = await self._get_running_item_info()
        if item and not verify_item(item):
            await self._clear_running_item_info()

    async def _delete_pool_entries(self):
        """
        See ``self.delete_pool_entries()`` method.
        """
        await self._r_pool.delete(self._name_running_plan)
        await self._r_pool.delete(self._name_plan_queue)
        await self._r_pool.delete(self._name_plan_history)
        await self._r_pool.delete(self._name_plan_queue_mode)
        self._uid_dict_clear()

        self._plan_queue_uid = self.new_item_uid()
        self._plan_history_uid = self.new_item_uid()

    async def delete_pool_entries(self):
        """
        Delete pool entries used by RE Manager. This method is mostly intended for use in testing,
        but may be used for other purposes if needed. Deleting pool entries also resets plan queue mode.
        """
        async with self._lock:
            await self._delete_pool_entries()

    def _verify_item_type(self, item):
        """
        Check that the item (plan) is a dictionary.
        """
        if not isinstance(item, dict):
            raise TypeError(f"Parameter 'item' should be a dictionary: '{item}', (type '{type(item)}')")

    def _verify_item(self, item, *, ignore_uids=None):
        """
        Verify that item (plan) structure is valid enough to be put in the queue.
        Current checks: item is a dictionary, ``item_uid`` key is present, Plan with the UID is not in
        the queue or currently running. Ignore UIDs in the list ``ignore_uids``: those UIDs are expected
        to be in the dictionary.
        """
        ignore_uids = ignore_uids or []
        self._verify_item_type(item)
        # Verify plan UID
        if "item_uid" not in item:
            raise ValueError("Item does not have UID.")
        uid = item["item_uid"]
        if (uid not in ignore_uids) and self._is_uid_in_dict(uid):
            raise RuntimeError(f"Item with UID {uid} is already in the queue")

    @staticmethod
    def new_item_uid():
        """
        Generate UID for an item (plan).
        """
        return str(uuid.uuid4())

    def set_new_item_uuid(self, item):
        """
        Replaces Item UID with a new one or creates a new UID.

        Parameters
        ----------
        item: dict
            Dictionary of item parameters. The dictionary may or may not have the key ``item_uid``.

        Returns
        -------
        dict
            Plan with new UID.
        """
        item = copy.deepcopy(item)
        self._verify_item_type(item)
        item["item_uid"] = self.new_item_uid()
        return item

    async def _get_index_by_uid(self, *, uid):
        """
        Get index of a plan in Redis list by UID. This is inefficient operation and should
        be avoided whenever possible. Raises an exception if the plan is not found.

        Parameters
        ----------
        uid: str
            UID of the plans to find.

        Returns
        -------
        int
            Index of the plan with given UID.

        Raises
        ------
        IndexError
            No plan is found.
        """
        queue, _ = await self._get_queue()
        for n, plan in enumerate(queue):
            if plan["item_uid"] == uid:
                return n
        raise IndexError(f"No plan with UID '{uid}' was found in the list.")

    # --------------------------------------------------------------------------
    #                          Operations with UID set
    def _uid_dict_clear(self):
        """
        Clear ``self._uid_dict``.
        """
        self._plan_queue_uid = self.new_item_uid()
        self._uid_dict.clear()

    def _is_uid_in_dict(self, uid):
        """
        Checks if UID exists in ``self._uid_dict``.
        """
        return uid in self._uid_dict

    def _uid_dict_add(self, plan):
        """
        Add UID to ``self._uid_dict``.
        """
        uid = plan["item_uid"]
        if self._is_uid_in_dict(uid):
            raise RuntimeError(f"Trying to add plan with UID '{uid}', which is already in the queue")
        self._plan_queue_uid = self.new_item_uid()
        self._uid_dict.update({uid: plan})

    def _uid_dict_remove(self, uid):
        """
        Remove UID from ``self._uid_dict``.
        """
        if not self._is_uid_in_dict(uid):
            raise RuntimeError(f"Trying to remove plan with UID '{uid}', which is not in the queue")
        self._plan_queue_uid = self.new_item_uid()
        self._uid_dict.pop(uid)

    def _uid_dict_update(self, plan):
        """
        Update a plan with UID that is already in the dictionary.
        """
        uid = plan["item_uid"]
        if not self._is_uid_in_dict(uid):
            raise RuntimeError(f"Trying to update plan with UID '{uid}', which is not in the queue")
        self._plan_queue_uid = self.new_item_uid()
        self._uid_dict.update({uid: plan})

    def _uid_dict_get_item(self, uid):
        """
        Returns a plan with the given UID.
        """
        return self._uid_dict[uid]

    async def _uid_dict_initialize(self):
        """
        Initialize ``self._uid_dict`` with UIDs extracted from the plans in the queue.
        """
        pq, _ = await self._get_queue()
        self._uid_dict_clear()
        # Go over all plans in the queue
        for item in pq:
            self._uid_dict_add(item)
        # If plan is currently running
        item = await self._get_running_item_info()
        if item:
            self._uid_dict_add(item)

    # -------------------------------------------------------------
    #                   Currently Running Plan

    async def _is_item_running(self):
        """
        See ``self.is_item_running()`` method.
        """
        return bool(await self._get_running_item_info())

    async def is_item_running(self):
        """
        Check if an item is set as running. True does not indicate that the plan is actually running.

        Returns
        -------
        boolean
            True - an item is set as running, False otherwise.
        """
        async with self._lock:
            return await self._is_item_running()

    async def _get_running_item_info(self):
        """
        See ``self._get_running_item_info()`` method.
        """
        plan = await self._r_pool.get(self._name_running_plan)
        return json.loads(plan) if plan else {}

    async def get_running_item_info(self):
        """
        Read info on the currently running item (plan) from Redis.

        Returns
        -------
        dict
            Dictionary representing currently running plan. Empty dictionary if
            no plan is currently running (key value is ``{}`` or the key does not exist).
        """
        async with self._lock:
            return await self._get_running_item_info()

    async def _set_running_item_info(self, plan):
        """
        Write info on the currently running item (plan) to Redis

        Parameters
        ----------
        plan: dict
            dictionary that contains plan parameters
        """
        await self._r_pool.set(self._name_running_plan, json.dumps(plan))

    async def _clear_running_item_info(self):
        """
        Clear info on the currently running item (plan) in Redis.
        """
        await self._set_running_item_info({})

    # -------------------------------------------------------------
    #                       Plan Queue

    async def _get_queue_size(self):
        """
        See ``self.get_queue_size()`` method.
        """
        return await self._r_pool.llen(self._name_plan_queue)

    async def get_queue_size(self):
        """
        Get the number of plans in the queue.

        Returns
        -------
        int
            The number of plans in the queue.
        """
        async with self._lock:
            return await self._get_queue_size()

    async def _get_queue(self):
        """
        See ``self.get_queue()`` method.
        """
        all_plans_json = await self._r_pool.lrange(self._name_plan_queue, 0, -1)
        return [json.loads(_) for _ in all_plans_json], self._plan_queue_uid

    async def get_queue(self):
        """
        Get the list of all items in the queue. The first element of the list is the first
        item in the queue.

        Returns
        -------
        list(dict)
            The list of items in the queue. Each item is represented as a dictionary.
            Empty list is returned if the queue is empty.
        str
            Plan queue UID.
        """
        async with self._lock:
            return await self._get_queue()

    async def _get_queue_full(self):
        plan_queue, plan_queue_uid = await self._get_queue()
        running_item = await self._get_running_item_info()
        return plan_queue, running_item, plan_queue_uid

    async def get_queue_full(self):
        """
        Get the list of all items in the queue and information on currently running item.
        The first element of the list is the first item in the queue. This is 'atomic' operation,
        i.e. it guarantees that all returned data represent a valid queue state and
        the queue was not changed while the data was collected.

        Returns
        -------
        list(dict)
            The list of items in the queue. Each item is represented as a dictionary.
            Empty list is returned if the queue is empty.
        dict
            Dictionary representing currently running plan. Empty dictionary if
            no plan is currently running (key value is ``{}`` or the key does not exist).
        str
            Plan queue UID.
        """
        async with self._lock:
            return await self._get_queue_full()

    async def _get_item(self, *, pos=None, uid=None):
        """
        See ``self.get_item()`` method.
        """
        if (pos is not None) and (uid is not None):
            raise ValueError("Ambiguous parameters: plan position and UID is specified")

        if uid is not None:
            if not self._is_uid_in_dict(uid):
                raise IndexError(f"Item with UID '{uid}' is not in the queue.")
            running_item = await self._get_running_item_info()
            if running_item and (uid == running_item["item_uid"]):
                raise IndexError("The item with UID '{uid}' is currently running.")
            item = self._uid_dict_get_item(uid)

        else:
            pos = pos if pos is not None else "back"

            if pos == "back":
                index = -1
            elif pos == "front":
                index = 0
            elif isinstance(pos, int):
                index = pos
            else:
                raise TypeError(f"Parameter 'pos' has incorrect type: pos={str(pos)} (type={type(pos)})")

            item_json = await self._r_pool.lindex(self._name_plan_queue, index)
            if item_json is None:
                raise IndexError(f"Index '{index}' is out of range (parameter pos = '{pos}')")

            item = json.loads(item_json) if item_json else {}

        return item

    async def get_item(self, *, pos=None, uid=None):
        """
        Get item at a given position or with a given UID. If UID is specified, then
        the position is ignored.

        Parameters
        ----------
        pos: int, str or None
            Position of the element ``(0, ..)`` or ``(-1, ..)``, ``front`` or ``back``.

        uid: str or None
            Plan UID of the plan to be retrieved. UID always overrides position.

        Returns
        -------
        dict
            Dictionary of item parameters.

        Raises
        ------
        TypeError
            Incorrect value of ``pos`` (most likely a string different from ``front`` or ``back``)
        IndexError
            No element with position ``pos`` exists in the queue (index is out of range).
        """
        async with self._lock:
            return await self._get_item(pos=pos, uid=uid)

    async def _remove_item(self, item, single=True):
        """
        Remove an item from the queue. If ``single=True`` then the exception is
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
        n_rem_items = await self._r_pool.lrem(self._name_plan_queue, 0, json.dumps(item))
        if (n_rem_items != 1) and single:
            raise RuntimeError(f"The number of removed items is {n_rem_items}. One item is expected.")

    async def _pop_item_from_queue(self, *, pos=None, uid=None):
        """
        See ``self._pop_item_from_queue()`` method
        """

        if (pos is not None) and (uid is not None):
            raise ValueError("Ambiguous parameters: plan position and UID is specified")

        pos = pos if pos is not None else "back"

        if uid is not None:
            if not self._is_uid_in_dict(uid):
                raise IndexError(f"Plan with UID '{uid}' is not in the queue.")
            running_item = await self._get_running_item_info()
            if running_item and (uid == running_item["item_uid"]):
                raise IndexError("Can not remove an item which is currently running.")
            item = self._uid_dict_get_item(uid)
            await self._remove_item(item)
        elif pos == "back":
            item_json = await self._r_pool.rpop(self._name_plan_queue)
            if item_json is None:
                raise IndexError("Queue is empty")
            item = json.loads(item_json) if item_json else {}
        elif pos == "front":
            item_json = await self._r_pool.lpop(self._name_plan_queue)
            if item_json is None:
                raise IndexError("Queue is empty")
            item = json.loads(item_json) if item_json else {}
        elif isinstance(pos, int):
            item = await self._get_item(pos=pos)
            if item:
                await self._remove_item(item)
        else:
            raise ValueError(f"Parameter 'pos' has incorrect value: pos={str(pos)} (type={type(pos)})")

        if item:
            self._uid_dict_remove(item["item_uid"])

        qsize = await self._get_queue_size()

        return item, qsize

    async def pop_item_from_queue(self, *, pos=None, uid=None):
        """
        Pop a plan from the queue. Raises ``IndexError`` if plan with index ``pos`` is unavailable
        or if the queue is empty.

        Parameters
        ----------
        pos : int or str or None
            Integer index specified position in the queue. Available string values: "front" or "back".
            The range for the index is ``-qsize..qsize-1``: ``0, -qsize`` - front element of the queue,
            ``-1, qsize-1`` - back element of the queue. If ``pos`` is ``None``, then the plan is popped
            from the back of the queue.
        uid : str or None
            UID of the item to be removed

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
            return await self._pop_item_from_queue(pos=pos, uid=uid)

    async def _add_item_to_queue(self, item, *, pos=None, before_uid=None, after_uid=None):
        """
        See ``self.add_item_to_queue()`` method.
        """
        if (pos is not None) and (before_uid is not None or after_uid is not None):
            raise ValueError("Ambiguous parameters: plan position and UID is specified")

        if (before_uid is not None) and (after_uid is not None):
            raise ValueError(
                "Ambiguous parameters: request to insert the plan before and after the reference plan"
            )

        pos = pos if pos is not None else "back"

        if "item_uid" not in item:
            item = self.set_new_item_uuid(item)
        else:
            self._verify_item(item)

        qsize0 = await self._get_queue_size()
        if (before_uid is not None) or (after_uid is not None):
            uid = before_uid if before_uid is not None else after_uid
            before = uid == before_uid

            if not self._is_uid_in_dict(uid):
                raise IndexError(f"Plan with UID '{uid}' is not in the queue.")
            running_item = await self._get_running_item_info()
            if running_item and (uid == running_item["item_uid"]):
                if before:
                    raise IndexError("Can not insert a plan in the queue before a currently running plan.")
                else:
                    # Push to the plan front of the queue (after the running plan).
                    qsize = await self._r_pool.lpush(self._name_plan_queue, json.dumps(item))
            else:
                item_to_displace = self._uid_dict_get_item(uid)
                before = uid == before_uid
                qsize = await self._r_pool.linsert(
                    self._name_plan_queue, json.dumps(item_to_displace), json.dumps(item), before=before
                )
        elif pos == "back" or (isinstance(pos, int) and pos >= qsize0):
            qsize = await self._r_pool.rpush(self._name_plan_queue, json.dumps(item))
        elif pos == "front" or (isinstance(pos, int) and (pos == 0 or pos <= -qsize0)):
            qsize = await self._r_pool.lpush(self._name_plan_queue, json.dumps(item))
        elif isinstance(pos, int):
            # Put the position in the range
            item_to_displace = await self._get_item(pos=pos)
            if item_to_displace:
                qsize = await self._r_pool.linsert(
                    self._name_plan_queue, json.dumps(item_to_displace), json.dumps(item), before=True
                )
            else:
                raise RuntimeError(f"Could not find an existing plan at {pos}. Queue size: {qsize0}")
        else:
            raise ValueError(f"Parameter 'pos' has incorrect value: pos='{str(pos)}' (type={type(pos)})")

        self._uid_dict_add(item)
        return item, qsize

    async def add_item_to_queue(self, item, *, pos=None, before_uid=None, after_uid=None):
        """
        Add an item to the queue. By default, the item is added to the back of the queue.
        If position is integer, it is clipped to fit within the range of meaningful indices.
        For the index too large or too low, the plan is pushed to the front or the back of the queue.

        Parameters
        ----------
        item: dict
            Item (plan or instruction) represented as a dictionary of parameters
        pos: int, str or None
            Integer that specifies the position index, "front" or "back".
            If ``pos`` is in the range ``1..qsize-1`` the item is inserted
            to the specified position and items at positions ``pos..qsize-1``
            are shifted by one position to the right. If ``-qsize<pos<0`` the
            item is inserted at the positon counted from the back of the queue
            (-1 - the last element of the queue). If ``pos>=qsize``,
            the plan is added to the back of the queue. If ``pos==0`` or
            ``pos<=-qsize``, the plan is pushed to the front of the queue.
        before_uid: str or None
            If UID is specified, then the item is inserted before the plan with UID.
            ``before_uid`` has precedence over ``after_uid``.
        after_uid: str or None
            If UID is specified, then the item is inserted before the plan with UID.

        Returns
        -------
        dict, int
            The dictionary that contains the item that was added and the new size of the queue.

        Raises
        ------
        ValueError
            Incorrect value of the parameter ``pos`` (typically unrecognized string).
        TypeError
            Incorrect type of ``item`` (should be dict)
        """
        async with self._lock:
            return await self._add_item_to_queue(item, pos=pos, before_uid=before_uid, after_uid=after_uid)

    async def _replace_item(self, item, *, item_uid):
        """
        See ``self._replace_item()`` method
        """
        # We can not replace currently running item, since it is technically not in the queue
        running_item = await self._get_running_item_info()
        running_item_uid = running_item["item_uid"] if running_item else None
        if not self._is_uid_in_dict(item_uid):
            raise RuntimeError(f"Failed to replace item: Item with UID '{item_uid}' is not in the queue")
        if (running_item_uid is not None) and (running_item_uid == item_uid):
            raise RuntimeError(f"Failed to replace item: Item with UID '{item_uid}' is currently running")

        if "item_uid" not in item:
            item = self.set_new_item_uuid(item)

        item_to_replace = self._uid_dict_get_item(item_uid)
        if item == item_to_replace:
            # There is nothing to do. Consider operation as successful.
            qsize = await self._get_queue_size()
            return item, qsize

        # Item with 'item_uid' is expected to be in the queue, so ignore 'item_uid'.
        #   The concern is that the queue may contain a plan with `item["item_uid"]`, which could be
        #   different from 'item_uid'. Then inserting the item may violate integrity of the queue.
        self._verify_item(item, ignore_uids=[item_uid])

        # Insert the new item after the old one and remove the old one. At this point it is guaranteed
        #   that they are not equal.
        await self._r_pool.linsert(self._name_plan_queue, json.dumps(item_to_replace), json.dumps(item))
        await self._remove_item(item_to_replace)

        # Update self._uid_dict
        self._uid_dict_remove(item_uid)
        self._uid_dict_add(item)

        # Read the actual size of the queue.
        qsize = await self._get_queue_size()

        return item, qsize

    async def replace_item(self, item, *, item_uid):
        """
        Replace item in the queue. Item with UID ``item_uid`` is replaced by item ``item``. The new
        item may have UID which is the same or different from UID of the item being replaced.

        Parameters
        ----------
        item: dict
            Item (plan or instruction) represented as a dictionary of parameters.

        item_uid: str
            UID of existing item in the queue that will be replaced.

        Returns
        -------
        dict, int
            The dictionary that contains the item that was added and the new size of the queue.
        """
        async with self._lock:
            return await self._replace_item(item, item_uid=item_uid)

    async def _move_item(self, *, pos=None, uid=None, pos_dest=None, before_uid=None, after_uid=None):
        """
        See ``self.move_plan()`` method.
        """
        if (pos is None) and (uid is None):
            raise ValueError("Source position or UID is not specified.")
        if (pos_dest is None) and (before_uid is None) and (after_uid is None):
            raise ValueError("Destination position or UID is not specified.")

        if (pos is not None) and (uid is not None):
            raise ValueError("Ambiguous parameters: Both position and uid is specified for the source plan.")
        if (pos_dest is not None) and (before_uid is not None or after_uid is not None):
            raise ValueError("Ambiguous parameters: Both position and uid is specified for the destination plan.")
        if (before_uid is not None) and (after_uid is not None):
            raise ValueError("Ambiguous parameters: source should be moved 'before' and 'after' the destination.")

        queue_size = await self._get_queue_size()

        # Find the source plan
        src_txt = ""
        src_by_index = False  # Indicates that the source is addressed by index
        try:
            if uid is not None:
                src_txt = f"UID '{uid}'"
                item_source = await self._get_item(uid=uid)
            else:
                src_txt = f"position {pos}"
                src_by_index = True
                item_source = await self._get_item(pos=pos)
        except Exception as ex:
            raise IndexError(f"Source plan ({src_txt}) was not found: {str(ex)}.")

        uid_source = item_source["item_uid"]

        # Find the destination plan
        dest_txt, before = "", True
        try:
            if (before_uid is not None) or (after_uid is not None):
                uid_dest = before_uid if before_uid else after_uid
                before = uid_dest == before_uid
                dest_txt = f"UID '{uid_dest}'"
                item_dest = await self._get_item(uid=uid_dest)
            else:
                dest_txt = f"position {pos_dest}"
                item_dest = await self._get_item(pos=pos_dest)

                # Find the index of the source in the most efficient way
                src_index = pos if src_by_index else (await self._get_index_by_uid(uid=uid))
                if src_index == "front":
                    src_index = 0
                elif src_index == "back":
                    src_index = queue_size - 1

                # Determine if the item must be inserted before or after the destination
                if pos_dest == "front":
                    before = True
                elif pos_dest == "back":
                    # This is one case when we need to insert the plan after the 'destination' plan.
                    before = False
                else:
                    before = src_index > pos_dest

        except Exception as ex:
            raise IndexError(f"Destination plan ({dest_txt}) was not found: {str(ex)}.")

        # Copy destination UID from the plan (we need it for the case of if addressing is positional
        #   so we convert it to UID, but we can do it for the case of UID addressing as well)
        #   In case of positional addressing 'before' is True, so the source is going to be
        #   inserted in place of destination.
        uid_dest = item_dest["item_uid"]

        # If source and destination point to the same plan, then do nothing,
        #   but consider it a valid operation.
        if uid_source != uid_dest:
            item, _ = await self._pop_item_from_queue(uid=uid_source)
            kw = {"before_uid": uid_dest} if before else {"after_uid": uid_dest}
            kw.update({"item": item})
            item, qsize = await self._add_item_to_queue(**kw)
        else:
            item = item_dest
            qsize = await self._get_queue_size()
        return item, qsize

    async def move_item(self, *, pos=None, uid=None, pos_dest=None, before_uid=None, after_uid=None):
        """
        Move existing item within the queue.

        Parameters
        ----------
        pos: str or int
            Position of the source item: positive or negative integer that specifieds the index
            of the item in the queue or a string from the set {"back", "front"}.
        uid: str
            UID of the source item. UID overrides the position
        pos_dext: str or int
            Index of the new position of the item in the queue: positive or negative integer that
            specifieds the index of the item in the queue or a string from the set {"back", "front"}.
        before_uid: str
            Insert the item before the item with the given UID.
        after_uid: str
            Insert the item after the item with the given UID.

        Returns
        -------
        dict, int
            The dictionary that contains a item that was moved and the size of the queue.

        Raises
        ------
        ValueError
            Error in specification of source or destination.
        """
        async with self._lock:
            return await self._move_item(
                pos=pos, uid=uid, pos_dest=pos_dest, before_uid=before_uid, after_uid=after_uid
            )

    async def _clear_queue(self):
        """
        See ``self.clear_queue()`` method.
        """
        while await self._get_queue_size():
            await self._pop_item_from_queue()

    async def clear_queue(self):
        """
        Remove all entries from the plan queue. Does not touch the running item (plan).
        The item may be pushed back into the queue if it is stopped.
        """
        async with self._lock:
            await self._clear_queue()

    # -----------------------------------------------------------------------
    #                          Plan History

    async def _add_to_history(self, item):
        """
        Add an item to history.

        Parameters
        ----------
        item: dict
            Item (plan) represented as a dictionary of parameters. No verifications are performed
            on the plan. The function is not intended to be used outside of this class.

        Returns
        -------
        int
            The new size of the history.
        """
        self._plan_history_uid = self.new_item_uid()
        history_size = await self._r_pool.rpush(self._name_plan_history, json.dumps(item))
        return history_size

    async def _get_history_size(self):
        """
        See ``self.get_history_size()`` method.
        """
        return await self._r_pool.llen(self._name_plan_history)

    async def get_history_size(self):
        """
        Get the number of items in the plan history.

        Returns
        -------
        int
            The number of plans in the history.
        """
        async with self._lock:
            return await self._get_history_size()

    async def _get_history(self):
        """
        See ``self.get_history()`` method.
        """
        all_plans_json = await self._r_pool.lrange(self._name_plan_history, 0, -1)
        return [json.loads(_) for _ in all_plans_json], self._plan_history_uid

    async def get_history(self):
        """
        Get the list of all items in the plan history. The first element of the list is
        the oldest history entry.

        Returns
        -------
        list(dict)
            The list of items in the plan history. Each plan is represented as a dictionary.
            Empty list is returned if the queue is empty.
        str
            Plan history UID
        """
        async with self._lock:
            return await self._get_history()

    async def _clear_history(self):
        """
        See ``self.clear_history()`` method.
        """
        self._plan_history_uid = self.new_item_uid()
        while await self._get_history_size():
            await self._r_pool.rpop(self._name_plan_history)

    async def clear_history(self):
        """
        Remove all entries from the plan queue. Does not touch the running item.
        The item (plan) may be pushed back into the queue if it is stopped.
        """
        async with self._lock:
            await self._clear_history()

    # ----------------------------------------------------------------------
    #          Standard item operations during queue execution

    async def _process_next_item(self):
        """
        See ``self.process_next_item()`` method.
        """
        loop_mode = self._plan_queue_mode["loop"]

        # Read the item from the front of the queue
        item = await self._get_item(pos="front")
        item_to_return = item
        if item:
            item_type = item["item_type"]
            if item_type == "plan":
                item_to_return = await self._set_next_item_as_running()
            else:
                # Items other than plans should be pushed to the back of the queue.
                await self._pop_item_from_queue(pos="front")
                if loop_mode:
                    item_to_add = self.set_new_item_uuid(item)
                    await self._add_item_to_queue(item_to_add)
        return item_to_return

    async def process_next_item(self):
        """
        Process the next item in the queue. If the item is a plan, it is set as currently
        running (``self.set_next_item_as_running``), otherwise it is popped from the queue.
        If the queue is LOOP mode, then it the item other than plan is pushed to the back
        of the queue. (Plans are pushed to the back of the queue upon successful completion.)
        """
        async with self._lock:
            return await self._process_next_item()

    async def _set_next_item_as_running(self):
        """
        See ``self.set_next_item_as_running()`` method.
        """
        # UID remains in the `self._uid_dict` after this operation.
        if not await self._is_item_running():
            plan_json = await self._r_pool.lpop(self._name_plan_queue)
            if plan_json:
                plan = json.loads(plan_json)
                if plan["item_type"] != "plan":
                    raise RuntimeError(
                        "Function 'PlanQueueOperations.set_next_item_as_running' was called for "
                        f"an item other than plan: {plan}"
                    )
                self._plan_queue_uid = self.new_item_uid()
                await self._set_running_item_info(plan)
            else:
                plan = {}
        else:
            plan = {}
        return plan

    async def set_next_item_as_running(self):
        """
        Sets the next item from the queue as 'running'. The item MUST be a plan
        (e.g. not an instruction), otherwise an exception will be raised. The item is removed
        from the queue. UID remains in ``self._uid_dict``, i.e. item with the same UID
        may not be added to the queue while it is being executed.

        This function can only be applied to the plans. Use ``process_next_item`` that
        works correctly for any item (it calls this function if an item is a plan).

        Returns
        -------
        dict
            The item that was set as currently running. If another item is currently
            set as 'running' or the queue is empty, then ``{}`` is returned.
        """
        async with self._lock:
            return await self._set_next_item_as_running()

    async def _set_processed_item_as_completed(self, exit_status, run_uids):
        """
        See ``self.set_processed_item_as_completed`` method.
        """
        # If loop_mode is True, then add item to the back of the queue
        loop_mode = self._plan_queue_mode["loop"]

        # Note: UID remains in the `self._uid_dict` after this operation
        if await self._is_item_running():
            item = await self._get_running_item_info()
            if loop_mode:
                item_to_add = item.copy()
                item_to_add = self.set_new_item_uuid(item_to_add)
                await self._r_pool.rpush(self._name_plan_queue, json.dumps(item_to_add))
                self._uid_dict_add(item_to_add)
            item.setdefault("result", {})
            item["result"]["exit_status"] = exit_status
            item["result"]["run_uids"] = run_uids
            await self._clear_running_item_info()
            if not loop_mode:
                self._uid_dict_remove(item["item_uid"])
            await self._add_to_history(item)
        else:
            item = {}
        return item

    async def set_processed_item_as_completed(self, exit_status, run_uids):
        """
        Moves currently executed item (plan) to history and sets ``exit_status`` key.
        UID is removed from ``self._uid_dict``, so a copy of the item with
        the same UID may be added to the queue.

        Parameters
        ----------
        exit_status: str
            Completion status of the plan.
        run_uids: list(str)
            A list of uids of completed runs.

        Returns
        -------
        dict
            The item added to the history including ``exit_status``. If another item (plan)
            is currently running, then ``{}`` is returned.
        """
        async with self._lock:
            return await self._set_processed_item_as_completed(exit_status=exit_status, run_uids=run_uids)

    async def _set_processed_item_as_stopped(self, exit_status, run_uids):
        """
        See ``self.set_processed_item_as_stopped()`` method.
        """
        # Note: UID is removed from `self._uid_dict`.
        if await self._is_item_running():
            item = await self._get_running_item_info()
            item.setdefault("result", {})
            item["result"]["exit_status"] = exit_status
            item["result"]["run_uids"] = run_uids
            await self._clear_running_item_info()
            await self._r_pool.lpush(self._name_plan_queue, json.dumps(item))
            self._uid_dict_update(item)
            await self._add_to_history(item)
        else:
            item = {}
        return item

    async def set_processed_item_as_stopped(self, exit_status, run_uids):
        """
        Pushes currently executed item to the beginning of the queue and adds
        it to history with additional sets ``exit_status`` key.
        UID is remains in ``self._uid_dict``.

        Parameters
        ----------
        exit_status: str
            Completion status of the plan.
        run_uids: list(str)
            A list of uids of completed runs.

        Returns
        -------
        dict
            The item (plan) added to the history including ``exit_status``. If another item (plan)
            is currently running, then ``{}`` is returned.
        """
        async with self._lock:
            return await self._set_processed_item_as_stopped(exit_status=exit_status, run_uids=run_uids)
