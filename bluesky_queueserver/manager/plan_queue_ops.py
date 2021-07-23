import aioredis
import asyncio
import copy
import json
import uuid
import logging

logger = logging.getLogger(__name__)


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

        # The list of allowed item parameters used for parameter filtering. Filtering operation
        #   involves removing all parameters that are not in the list.
        self._allowed_item_parameters = (
            "item_uid",
            "item_type",
            "name",
            "args",
            "kwargs",
            "meta",
            "user",
            "user_group",
            "properties",
        )

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
                f"Queue mode is passed using object of unsupported type '{type(plan_queue_mode)}': "
                f"({plan_queue_mode}). Supported types: ('dict', 'str'), supported "
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
                try:
                    self._r_pool = await aioredis.create_redis_pool(f"redis://{self._redis_host}", encoding="utf8")
                except OSError as ex:
                    error_msg = (
                        f"Failed to create the Redis pool: "
                        f"Redis server may not be available at '{self._redis_host}'. "
                        f"Exception: {ex}"
                    )
                    logger.error(error_msg)
                    raise OSError(error_msg) from ex

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

    async def _get_index_by_uid_batch(self, *, uids):
        """
        Batch version of ``_get_index_by_uid``. The operation is implemented efficiently
        and should be used for finding indices of large number of items (in batch operations).
        Returns a list of indices. Index is set to -1 for items that are not found.

        Parameters
        ----------
        uids: list(str)
            List of UIDs of the items in the batch to find.

        Returns
        -------
        list(int)
            List of indices of the items. The list has the same number of items as the list ``uids``.
            Indices are set to -1 for the items that were not found in the queue.
        """
        queue, _ = await self._get_queue()

        uids_set = set(uids)  # Set should be more efficient for searching elements
        uid_to_index = {}
        for n, plan in enumerate(queue):
            uid = plan["item_uid"]
            if uid in uids_set:
                uid_to_index[uid] = n

        indices = [None] * len(uids)
        for n, uid in enumerate(uids):
            indices[n] = uid_to_index.get(uid, -1)

        return indices

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

    def _uid_dict_add(self, item):
        """
        Add UID to ``self._uid_dict``.
        """
        uid = item["item_uid"]
        if self._is_uid_in_dict(uid):
            raise RuntimeError(f"Trying to add plan with UID '{uid}', which is already in the queue")
        self._plan_queue_uid = self.new_item_uid()
        self._uid_dict.update({uid: item})

    def _uid_dict_remove(self, uid):
        """
        Remove UID from ``self._uid_dict``.
        """
        if not self._is_uid_in_dict(uid):
            raise RuntimeError(f"Trying to remove plan with UID '{uid}', which is not in the queue")
        self._plan_queue_uid = self.new_item_uid()
        self._uid_dict.pop(uid)

    def _uid_dict_update(self, item):
        """
        Update a plan with UID that is already in the dictionary.
        """
        uid = item["item_uid"]
        if not self._is_uid_in_dict(uid):
            raise RuntimeError(f"Trying to update plan with UID '{uid}', which is not in the queue")
        self._plan_queue_uid = self.new_item_uid()
        self._uid_dict.update({uid: item})

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
        See ``self.pop_item_from_queue()`` method
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
        int
            The size of the queue after completion of the operation.

        Raises
        ------
        ValueError
            Incorrect value of the parameter ``pos`` (typically unrecognized string).
        IndexError
            Position ``pos`` does not exist or the queue is empty.
        """
        async with self._lock:
            return await self._pop_item_from_queue(pos=pos, uid=uid)

    async def _pop_item_from_queue_batch(self, *, uids=None, ignore_missing=True):
        """
        See ``self.pop_item_from_queue_batch()`` method
        """

        uids = uids or []

        if not isinstance(uids, list):
            raise TypeError(f"Parameter 'uids' must be a list: type(uids) = {type(uids)}")

        if not ignore_missing:
            # Check if 'uids' contains only unique items
            uids_set = set(uids)
            if len(uids_set) != len(uids):
                raise ValueError(f"The list of contains repeated UIDs ({len(uids) - len(uids_set)} UIDs)")

            # Check if all UIDs in 'uids' exist in the queue
            uids_missing = []
            for uid in uids:
                if not self._is_uid_in_dict(uid):
                    uids_missing.append(uid)
            if uids_missing:
                raise ValueError(f"The queue does not contain items with the following UIDs: {uids_missing}")

        items = []
        for uid in uids:
            try:
                item, _ = await self._pop_item_from_queue(uid=uid)
                items.append(item)
            except Exception as ex:
                logger.debug("Failed to remove item with UID '%s' from the queue: %s", uid, str(ex))

        qsize = await self._get_queue_size()
        return items, qsize

    async def pop_item_from_queue_batch(self, *, uids=None, ignore_missing=True):
        """
        Pop a batch of items from the queue. Raises ``IndexError`` if plan with index ``pos`` is unavailable
        or if the queue is empty.

        Parameters
        ----------
        uids : list(str)
            list of UIDs of items to be removed. The list may be empty.
        ignore_missing : boolean
            if the parameter is ``True`` (default), then all items from the batch that are found in
            the queue are removed, if ``False``, then the function fails if the list contains repeated
            entries or at least one of the items is not found in the queue. No items are removed
            from the queue if the function fails.

        Returns
        -------
        list(dict)
            The list of items that were removed from the queue.
        int
            Size of the queue after completion of the operation.

        Raises
        ------
        ValueError
            Function failed due to missing or repeated items in the batch.
        """
        async with self._lock:
            return await self._pop_item_from_queue_batch(uids=uids, ignore_missing=ignore_missing)

    def filter_item_parameters(self, item):
        """
        Remove parameters that are not in the list of allowed parameters.
        Current parameter list includes parameters ``item_type``, ``item_uid``,
        ``name``, ``args``, ``kwargs``, ``meta``, ``user``, ``user_group``.

        The list does not include ``result`` parameter, i.e. ``result`` will
        be removed from the list of parameters.

        Parameters
        ----------
        item : dict
            dictionary of item parameters

        Returns
        -------
        dict
            dictionary of filtered item parameters
        """
        return {k: w for k, w in item.items() if k in self._allowed_item_parameters}

    async def _add_item_to_queue(self, item, *, pos=None, before_uid=None, after_uid=None, filter_parameters=True):
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

        if filter_parameters:
            item = self.filter_item_parameters(item)

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

    async def add_item_to_queue(self, item, *, pos=None, before_uid=None, after_uid=None, filter_parameters=True):
        """
        Add an item to the queue. By default, the item is added to the back of the queue.
        If position is integer, it is clipped to fit within the range of meaningful indices.
        For the index too large or too low, the plan is pushed to the front or the back of the queue.

        Parameters
        ----------
        item : dict
            Item (plan or instruction) represented as a dictionary of parameters
        pos : int, str or None
            Integer that specifies the position index, "front" or "back".
            If ``pos`` is in the range ``1..qsize-1`` the item is inserted
            to the specified position and items at positions ``pos..qsize-1``
            are shifted by one position to the right. If ``-qsize<pos<0`` the
            item is inserted at the positon counted from the back of the queue
            (-1 - the last element of the queue). If ``pos>=qsize``,
            the plan is added to the back of the queue. If ``pos==0`` or
            ``pos<=-qsize``, the plan is pushed to the front of the queue.
        before_uid : str or None
            If UID is specified, then the item is inserted before the plan with UID.
            ``before_uid`` has precedence over ``after_uid``.
        after_uid : str or None
            If UID is specified, then the item is inserted before the plan with UID.
        filter_parameters : boolean (optional)
            Remove all parameters that do not belong to the list of allowed parameters.
            Default: ``True``.

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
            return await self._add_item_to_queue(
                item, pos=pos, before_uid=before_uid, after_uid=after_uid, filter_parameters=filter_parameters
            )

    async def _add_batch_to_queue(
        self, items, *, pos=None, before_uid=None, after_uid=None, filter_parameters=True
    ):
        """
        See ``self.add_batch_to_queue()`` method.
        """
        items_added, results = [], []

        # Approach: attempt ot add each item to queue. In case of a failure remove all added item.
        # Operation of adding items to queue is perfectly reversible.

        # Adding a batch to a specified position in the queue:
        #   - add the first plan by passing all positional parameters to 'self._add_item_to_queue'.
        #   - add the remaining plans after the first plan (it doesn't matter how position of the first
        #     plan was defined.

        success = True
        added_item_uids = []  # List of plans with successfully added UIDs. Used for 'undo' operation.
        for item in items:
            try:
                if not added_item_uids:
                    item_added, _ = await self._add_item_to_queue(
                        item,
                        pos=pos,
                        before_uid=before_uid,
                        after_uid=after_uid,
                        filter_parameters=filter_parameters,
                    )
                else:
                    item_added, _ = await self._add_item_to_queue(
                        item, after_uid=added_item_uids[-1], filter_parameters=filter_parameters
                    )
                added_item_uids.append(item_added["item_uid"])
                items_added.append(item_added)
                results.append({"success": True, "msg": ""})
            except Exception as ex:
                success = False
                items_added.append(item)
                msg = f"Failed to add item to queue: {ex}"
                results.append({"success": False, "msg": msg})

        # 'Undo' operation: remove all inserted items
        if not success:
            for uid in added_item_uids:
                # The 'try-except' here is just in case anything goes wrong. Operation of removing
                #   items that were just added are expected to succeed.
                try:
                    await self._pop_item_from_queue(uid=uid)
                except Exception as ex:
                    logger.error(
                        "Failed to remove an item with uid='%s' after failure to add a batch of plans", str(ex)
                    )

            # Also do not return 'changed' items if adding the batch failed.
            items_added = items

        qsize = await self._get_queue_size()

        # 'items_added' and 'results' ALWAYS have the same number of elements as 'items'
        return items_added, results, qsize, success

    async def add_batch_to_queue(
        self, items, *, pos=None, before_uid=None, after_uid=None, filter_parameters=True
    ):
        """
        Add a batch of item to the queue. The behavior of the function is similar
        to ``add_item_to_queue`` except that it accepts the list of items to add.
        The function will not add any plans to the queue if at least one of the plans
        is rejected. The function returns ``success`` flag, which is ``True`` if
        the batch was added and ``False`` otherwise. Success status and error messages
        for each added plan can be found in ``results`` list. If the batch was added
        successfully, then all ``results`` element indicate success.

        The function is not expected to raise exceptions in case of failure, but instead
        report results of processing for each item in the ``results`` list.

        Parameters
        ----------
        items : list(dict)
            List of items (plans or instructions). Each element of the list is a dictionary
            of plan parameters
        pos, before_uid, after_uid, filter_parmeters
            see documentation for ``add_item_to_queue`` for details.

        Returns
        -------
        items_added : list(dict)
            List of items that were added to queue. In case the operation fails, the list
            of submitted items is returned. The list always has the same number of elements
            as ``items``.
        results : list(dict)
            List of processing results. The list always has the same number of elements as
            ``items``. Each element contains a report on processing the respective item in
            the form of a dictionary with the keys ``success`` (boolean) and ``msg`` (str).
            ``msg`` is always an empty string if ``success==True``. In case the batch was
            added successfully, all elements are ``{"success": True, "msg": ""}``.
        qsize : int
            Size of the queue after the batch was added. The size will not change if the
            batch is rejected.
        success : bool
            Indicates success of the operation of adding the batch. If ``False``, then
            the batch is rejected and error messages for each item could be found in
            the ``results`` list.
        """

        async with self._lock:
            return await self._add_batch_to_queue(
                items, pos=pos, before_uid=before_uid, after_uid=after_uid, filter_parameters=filter_parameters
            )

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

        # Parameters of the edited item should be verified against the list of the allowed parameters
        item = self.filter_item_parameters(item)

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
        See ``self.move_item()`` method.
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
            # The item is moved 'as is'. No filtering of parameters is applied.
            item, qsize = await self._add_item_to_queue(**kw, filter_parameters=False)
        else:
            item = item_dest
            qsize = await self._get_queue_size()
        return item, qsize

    async def move_item(self, *, pos=None, uid=None, pos_dest=None, before_uid=None, after_uid=None):
        """
        Move existing item within the queue. Plan is moved within the queue without modification.
        No parameter filtering is applied, so the ``result`` item parameter will not be removed if
        present.

        Parameters
        ----------
        pos: str or int
            Position of the source item: positive or negative integer that specifieds the index
            of the item in the queue or a string from the set {"back", "front"}.
        uid: str
            UID of the source item. UID overrides the position
        pos_dest: str or int
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

    async def _move_batch(self, *, uids=None, pos_dest=None, before_uid=None, after_uid=None, reorder=False):
        """
        See ``self.move_batch()`` method.
        """
        uids = uids or []

        if not isinstance(uids, list):
            raise TypeError(f"Parameter 'uids' must be a list: type(uids) = {type(uids)}")

        # Make sure only one of the mutually exclusive parameters is not None
        param_list = [pos_dest, before_uid, after_uid]
        n_params = len(param_list) - param_list.count(None)
        if n_params < 1:
            raise ValueError(
                "Destination for the batch is not specified: use parameters 'pos_dest', "
                "'before_uid' or 'after_uid'"
            )
        elif n_params > 1:
            raise ValueError(
                "The function was called with more than one mutually exclusive parameter "
                "('pos_dest', 'before_uid', 'after_uid')"
            )

        # Check if 'uids' contains only unique items
        uids_set = set(uids)
        if len(uids_set) != len(uids):
            raise ValueError(f"The list of contains repeated UIDs ({len(uids) - len(uids_set)} UIDs)")

        # Check if all UIDs in 'uids' exist in the queue
        uids_missing = []
        for uid in uids:
            if not self._is_uid_in_dict(uid):
                uids_missing.append(uid)
        if uids_missing:
            raise ValueError(f"The queue does not contain items with the following UIDs: {uids_missing}")

        # Check that 'before_uid' and 'after_uid' are not in 'uids'
        if (before_uid is not None) and (before_uid in uids):
            raise ValueError(f"Parameter 'before_uid': item with UID '{before_uid}' is in the batch")
        if (after_uid is not None) and (after_uid in uids):
            raise ValueError(f"Parameter 'after_uid': item with UID '{after_uid}' is in the batch")

        # Rearrange UIDs in the list if items need to be reordered
        if reorder:
            indices = await self._get_index_by_uid_batch(uids=uids)

            def sorting_key(element):
                return element[0]

            uids_with_indices = list(zip(indices, uids))
            uids_with_indices.sort(key=sorting_key)
            uids_prepared = [_[1] for _ in uids_with_indices]
        else:
            uids_prepared = uids

        # Perform the 'move' operation.
        last_item_uid = None
        items_moved = []
        for uid in uids_prepared:
            if last_item_uid is None:
                # First item is moved according to specified parameters
                item, _ = await self._move_item(
                    uid=uid, pos_dest=pos_dest, before_uid=before_uid, after_uid=after_uid
                )
            else:
                # Consecutive items are placed after the first item
                item, _ = await self._move_item(uid=uid, after_uid=last_item_uid)
            last_item_uid = uid
            items_moved.append(item)

        qsize = await self._get_queue_size()
        return items_moved, qsize

    async def move_batch(self, *, uids=None, pos_dest=None, before_uid=None, after_uid=None, reorder=False):
        """
        Move a batch of existing items within the queue. The items are specified as a list of uids.
        If at least one of the uids can not be found in the queue, the operation fails and no items
        are moved. The moved items can be ordered based on the order of uids in the list (``reorder=False``)
        or based on the original order in the queue (``reorder=True``). Plan is moved within the queue
        without modification. No parameter filtering is applied, so the ``result`` item parameter
        will not be removed if present.

        The items in the batch do not have to be contiguous. Destination items specified by ``after_uid``
        and ``before_uid`` may not belong to the batch. The parameters ``pos_dest``, ``before_uid`` and
        ``after_uid`` are mutually exclusive.

        The function raises an exception with error message in case the operation fails.

        Parameters
        ----------
        uids : list(str)
            List of UIDs of the items in the batch. The list may not contain repeated UIDs. All UIDs
            must be present in the queue.
        pos_dest : str
            Destination of the moved batch. Only string values ``front`` and ``back`` are allowed.
        before_uid : str
            Insert the batch before the item with the given UID.
        after_uid : str
            Insert the batch after the item with the given UID.
        reorder : boolean
            Arranged moved items according to the order of UIDs in the ``uids`` list (``False``) or
            according to the original order of items in the queue (``True``).

        Returns
        -------
        list(dict), int
            The list of items that were moved and the size of the queue. The order of the items
            matches the order of the items in the moved batch. Depending on the value of ``reorder``
            it may or may not match the order of ``uids``.

        Raises
        ------
        ValueError
            Operation could not be performed due to incorrectly specified parameters or invalid
            list of ``uids``.
        TypeError
            Incorrect type of parameter ``uids``.
        """
        async with self._lock:
            return await self._move_batch(
                uids=uids, pos_dest=pos_dest, before_uid=before_uid, after_uid=after_uid, reorder=reorder
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

    def _clean_item_properties(self, item):
        """
        The function removes unneccessary item properties before adding the item to history or
        returning it to queue.
        """
        item = copy.deepcopy(item)
        if "properties" in item:
            p = item["properties"]
            # "immediate_execution" flag is set internally by the server and should not be exposed to users
            if "immediate_execution" in p:
                del p["immediate_execution"]
            if not p:
                del item["properties"]
        return item

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
            immediate_execution = item.get("properties", {}).get("immediate_execution", False)
            item_cleaned = self._clean_item_properties(item)

            if loop_mode and not immediate_execution:
                item_to_add = item_cleaned.copy()
                item_to_add = self.set_new_item_uuid(item_to_add)
                await self._r_pool.rpush(self._name_plan_queue, json.dumps(item_to_add))
                self._uid_dict_add(item_to_add)
            item_cleaned.setdefault("result", {})
            item_cleaned["result"]["exit_status"] = exit_status
            item_cleaned["result"]["run_uids"] = run_uids
            await self._clear_running_item_info()
            if not loop_mode:
                self._uid_dict_remove(item["item_uid"])
            await self._add_to_history(item_cleaned)
        else:
            item_cleaned = {}
        return item_cleaned

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
            immediate_execution = item.get("properties", {}).get("immediate_execution", False)
            item_cleaned = self._clean_item_properties(item)

            item_cleaned.setdefault("result", {})
            item_cleaned["result"]["exit_status"] = exit_status
            item_cleaned["result"]["run_uids"] = run_uids

            await self._add_to_history(item_cleaned)
            await self._clear_running_item_info()

            # Generate new UID for the item that is pushed back into the queue.
            if not immediate_execution:
                item_pushed_to_queue = self.set_new_item_uuid(item_cleaned)
                self._uid_dict_remove(item["item_uid"])
                await self._add_item_to_queue(item_pushed_to_queue, pos="front", filter_parameters=False)
        else:
            item_cleaned = {}
        return item_cleaned

    async def set_processed_item_as_stopped(self, exit_status, run_uids):
        """
        Pushes currently executed item to the beginning of the queue and adds
        it to history with additional sets ``exit_status`` key.
        UID is remains in ``self._uid_dict``. New ``item_uid`` is generated for the item
        that is pushed back into the queue.

        Parameters
        ----------
        exit_status: str
            Completion status of the plan.
        run_uids: list(str)
            A list of uids of completed runs.

        Returns
        -------
        dict
            The item (plan) added to the history including ``exit_status``. If no item (plan)
            is running, then the function returns ``{}``. The item pushed back into the queue
            will have different ``item_uid`` than the item added to the history.
        """
        async with self._lock:
            return await self._set_processed_item_as_stopped(exit_status=exit_status, run_uids=run_uids)
