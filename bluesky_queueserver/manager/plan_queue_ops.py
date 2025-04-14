import asyncio
import copy
import json
import logging
import time as ttime
import uuid

import redis.asyncio
import aiosqlite

logger = logging.getLogger(__name__)


class PlanQueueOperations:
    """
    The class supports operations with plan queue based on Redis. The public methods
    of the class are protected with ``asyncio.Lock``.

    Parameters
    ----------
    redis_host: str
        Address of Redis host.

    name_prefix: str
        Prefix for the names of the keys used in Redis. The prefix is used to avoid conflicts
        with the keys used by other instances of Queue Server. For example, the prefix used
        for unit tests should be different from the prefix used in production. If the prefix
        is an empty string, then no prefix will be added (not recommended).

    Examples
    --------

    .. code-block:: python

        pq = PlanQueueOperations(prefix="qs_unit_test")  # Redis located at `localhost`
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

        # 'stopping' disconnects all connections. This step is not required in normal use.
        await pq.stop()
    """

    def __init__(self, redis_host="localhost", name_prefix="qs_default", backend="redis", sqlite_db_path=None):
        """
        Initialize the PlanQueueOperations class.

        Parameters
        ----------
        redis_host : str
            Address of Redis host.
        name_prefix : str
            Prefix for the names of the keys used in Redis/SQLite.
        backend : str
            Backend type ("redis" or "sqlite").
        sqlite_db_path : str or None
            Path to the SQLite database file. If None, the path is read from the
            environment variable `SQLITE_DB_PATH`. If the environment variable is not set,
            a default path `my_database.db` is used.
        """
        self._backend = backend
        self._redis_host = redis_host
        self._sqlite_db_path = sqlite_db_path or os.getenv("SQLITE_DB_PATH", "queue_database.db")
        self._uid_dict = dict()
        self._r_pool = None
        self._sqlite_conn = None

        if not isinstance(name_prefix, str):
            raise TypeError(f"Parameter 'name_prefix' should be a string: {name_prefix}")

        # The case of an empty string
        if name_prefix:
            name_prefix = name_prefix + "_"

        self._name_running_plan = name_prefix + "running_plan"
        self._name_plan_queue = name_prefix + "plan_queue"
        self._name_plan_history = name_prefix + "plan_history"
        self._name_plan_queue_mode = name_prefix + "plan_queue_mode"

        # Redis is also used for storage of some additional information not related to the queue.
        #   The class contains only the functions for saving and retrieving the data, which is
        #   not used by other functions of the class.
        self._name_user_group_permissions = name_prefix + "user_group_permissions"
        self._name_lock_info = name_prefix + "lock_info"
        self._name_autostart_mode_info = name_prefix + "autostart_mode_info"
        self._name_stop_pending_info = name_prefix + "stop_pending_info"

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
        #   Loop mode:
        #      loop, True/False. If enabled, then each executed item (plan or instruction)
        #        will be placed to the back of the queue.
        #      ignore_failures, True/False. Run all the plans in the queue to the end
        #        even if some or all of the plans fail. The queue is still stopped if the user
        #        stops/aborts/halts a plan.
        self._plan_queue_mode_default = {"loop": False, "ignore_failures": False}
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
        expected_params = {"loop": bool, "ignore_failures": bool}
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
        try:
            self._validate_plan_queue_mode(self._plan_queue_mode)
        except Exception as ex:
            logger.error("Failed to load plan queue mode from Redis. The default mode is used: %s", ex)
            self._plan_queue_mode = self.plan_queue_mode_default

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
        if self._backend == "redis":
            if not self._r_pool:  # Initialize only once
                self._lock = asyncio.Lock()
                async with self._lock:
                    try:
                        host = f"redis://{self._redis_host}"
                        self._r_pool = redis.asyncio.from_url(host, encoding="utf-8", decode_responses=True)
                        await self._r_pool.ping()
                    except Exception as ex:
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

        elif self._backend == "sqlite":
            if not self._sqlite_conn:
                self._sqlite_conn = await aiosqlite.connect(self._sqlite_db_path)
                await self._initialize_sqlite_tables()

    async def _initialize_sqlite_tables(self):
        """
        Initialize SQLite tables if they do not exist.
        """
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._sqlite_tables['plan_queue']} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_uid TEXT UNIQUE,
                    item_data TEXT
                )
            """)
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._sqlite_tables['plan_history']} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_uid TEXT UNIQUE,
                    item_data TEXT
                )
            """)
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._sqlite_tables['running_plan']} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_data TEXT
                )
            """)
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._sqlite_tables['plan_queue_mode']} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mode_data TEXT
                )
            """)
        await self._sqlite_conn.commit()

    async def stop(self):
        """
        Close all connections in the pool.
        """
        if self._backend == "redis" and self._r_pool:
            await self._r_pool.aclose()
            self._r_pool = None
        elif self._backend == "sqlite" and self._sqlite_conn:
            await self._sqlite_conn.close()
            self._sqlite_conn = None

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

    @staticmethod
    def _verify_item_type(item):
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
        Get the size of the queue.
        """
        if self._backend == "redis":
            return await self._r_pool.llen(self._name_plan_queue)
        elif self._backend == "sqlite":
            if not hasattr(self, "_queue_size_cache"):
                async with self._sqlite_conn.cursor() as cursor:
                    await cursor.execute(f"SELECT COUNT(*) FROM list_store WHERE list_key=?", (self._name_plan_queue,))
                    row = await cursor.fetchone()
                    self._queue_size_cache = row[0] if row else 0
            return self._queue_size_cache

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
        Retrieve the queue from the backend using the abstraction layer.
        """
        all_plans_json = await self._backend_list_range(self._name_plan_queue, 0, -1)
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
        Retrieve an item from the queue by position or UID using the abstraction layer.

        Parameters
        ----------
        pos : int, str or None
            Position of the element (e.g., "front", "back", or an integer index).
        uid : str or None
            UID of the item to retrieve. If UID is specified, position is ignored.

        Returns
        -------
        dict
            The item retrieved from the queue.

        Raises
        ------
        ValueError
            If both `pos` and `uid` are specified.
        IndexError
            If the item is not found or the position is out of range.
        TypeError
            If `pos` has an invalid type.
        """
        if (pos is not None) and (uid is not None):
            raise ValueError("Ambiguous parameters: both position and UID are specified.")

        if uid is not None:
            # Retrieve item by UID
            if not self._is_uid_in_dict(uid):
                raise IndexError(f"Item with UID '{uid}' is not in the queue.")
            running_item = await self._get_running_item_info()
            if running_item and (uid == running_item["item_uid"]):
                raise IndexError(f"The item with UID '{uid}' is currently running.")
            item = self._uid_dict_get_item(uid)
            
        else:
            # Retrieve item by position
            if pos == "back":
                query = f"SELECT value FROM list_store WHERE list_key=? ORDER BY id DESC LIMIT 1"
            elif pos == "front":
                query = f"SELECT value FROM list_store WHERE list_key=? ORDER BY id ASC LIMIT 1"
            elif isinstance(pos, int):
                offset = pos if pos >= 0 else pos + await self._get_queue_size()
                query = f"SELECT value FROM list_store WHERE list_key=? ORDER BY id ASC LIMIT 1 OFFSET {offset}"
            else:
                raise TypeError(f"Parameter 'pos' has incorrect type: pos={str(pos)} (type={type(pos)})")

            async with self._sqlite_conn.cursor() as cursor:
                await cursor.execute(query, (self._name_plan_queue,))
                row = await cursor.fetchone()
                if not row:
                    raise IndexError(f"Item at position '{pos}' is not found.")
                return json.loads(row[0])

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
        item: dict
            Dictionary of item parameters. Must be identical to the item that is
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
        item_json = json.dumps(item)

        if self._backend == "redis":
            n_rem_items = await self._r_pool.lrem(self._name_plan_queue, 0, item_json)
        elif self._backend == "sqlite":
            async with self._sqlite_conn.cursor() as cursor:
                await cursor.execute(
                    f"DELETE FROM list_store WHERE list_key=? AND value=?", (self._name_plan_queue, item_json)
                )
                n_rem_items = cursor.rowcount
            await self._sqlite_conn.commit()

            if single and n_rem_items != 1:
                raise RuntimeError(f"The number of removed items is {n_rem_items}. One item is expected.")

    async def _pop_item_from_queue(self, *, pos=None, uid=None):
        """
        See ``self.pop_item_from_queue()`` method
        """
        if (pos is not None) and (uid is not None):
            raise ValueError("Ambiguous parameters: plan position and UID are specified")

        pos = pos if pos is not None else "back"

        if uid is not None:
            if not self._is_uid_in_dict(uid):
                raise IndexError(f"Plan with UID '{uid}' is not in the queue.")
            running_item = await self._get_running_item_info()
            if running_item and (uid == running_item["item_uid"]):
                raise IndexError("Cannot remove an item that is currently running.")
            item = self._uid_dict_get_item(uid)
            await self._remove_item(item)
        elif pos in ["back", "front"]:
            item_json = await self._backend_list_pop(self._name_plan_queue, position=pos)
            if item_json is None:
                raise IndexError("Queue is empty")
            item = json.loads(item_json)
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
        Remove a batch of items from the queue using the abstraction layer.

        Parameters
        ----------
        uids : list(str)
            List of UIDs of items to be removed. The list may be empty.
        ignore_missing : boolean
            If True (default), all items from the batch that are found in the queue are removed.
            If False, the function fails if the list contains repeated entries or at least one
            of the items is not found in the queue. No items are removed from the queue if the
            function fails.

        Returns
        -------
        list(dict)
            The list of items that were removed from the queue.
        int
            Size of the queue after completion of the operation.

        Raises
        ------
        ValueError
            If the function fails due to missing or repeated items in the batch.
        """
        if self._backend == "sqlite":
            if not uids:
                return [], await self._get_queue_size()

            async with self._sqlite_conn.cursor() as cursor:
                # Retrieve items to be removed
                placeholders = ", ".join("?" for _ in uids)
                await cursor.execute(
                    f"SELECT value FROM list_store WHERE list_key=? AND json_extract(value, '$.item_uid') IN ({placeholders})",
                    (self._name_plan_queue, *uids)
                )
                rows = await cursor.fetchall()
                items_to_remove = [json.loads(row[0]) for row in rows]

                if not ignore_missing and len(items_to_remove) != len(uids):
                    raise ValueError("Some UIDs are missing from the queue.")

                # Remove items in a single query
                await cursor.execute(
                    f"DELETE FROM list_store WHERE list_key=? AND json_extract(value, '$.item_uid') IN ({placeholders})",
                    (self._name_plan_queue, *uids)
                )
            await self._sqlite_conn.commit()

            for item in items_to_remove:
                self._uid_dict_remove(item["item_uid"])

            qsize = await self._get_queue_size()
            return items_to_remove, qsize

        else:
            # Fallback to existing logic for Redis
            return await super()._pop_item_from_queue_batch(uids=uids, ignore_missing=ignore_missing)

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
        Add an item to the queue using the abstraction layer.

        Parameters
        ----------
        item : dict
            Item (plan or instruction) represented as a dictionary of parameters.
        pos : int, str or None
            Integer that specifies the position index, "front" or "back".
        before_uid : str or None
            If UID is specified, then the item is inserted before the plan with UID.
            ``before_uid`` has precedence over ``after_uid``.
        after_uid : str or None
            If UID is specified, then the item is inserted after the plan with UID.
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
        IndexError
            If the reference UID is not found in the queue.
        """
        if (pos is not None) and (before_uid is not None or after_uid is not None):
            raise ValueError("Ambiguous parameters: plan position and UID are specified.")

        if (before_uid is not None) and (after_uid is not None):
            raise ValueError(
                "Ambiguous parameters: request to insert the plan before and after the reference plan."
            )

        pos = pos if pos is not None else "back"

        if "item_uid" not in item:
            item = self.set_new_item_uuid(item)
        else:
            self._verify_item(item)

        if filter_parameters:
            item = self.filter_item_parameters(item)

        qsize0 = await self._get_queue_size()
        if isinstance(pos, int):
            if (pos == 0) or (pos < -qsize0):
                pos = "front"
            elif (pos == -1) or (pos >= qsize0):
                pos = "back"

        if (before_uid is not None) or (after_uid is not None):
            uid = before_uid if before_uid is not None else after_uid
            before = uid == before_uid

            if not self._is_uid_in_dict(uid):
                raise IndexError(f"Plan with UID '{uid}' is not in the queue.")
            running_item = await self._get_running_item_info()
            if running_item and (uid == running_item["item_uid"]):
                if before:
                    raise IndexError("Cannot insert a plan in the queue before a currently running plan.")
                else:
                    # Push to the front of the queue (after the running plan).
                    await self._backend_list_push(self._name_plan_queue, json.dumps(item), position="front")
            else:
                item_to_displace = self._uid_dict_get_item(uid)
                where = "BEFORE" if (uid == before_uid) else "AFTER"
                await self._backend_list_push(self._name_plan_queue, json.dumps(item_to_displace), position=where)
        elif pos == "back":
            await self._backend_list_push(self._name_plan_queue, json.dumps(item), position="back")
        elif pos == "front":
            await self._backend_list_push(self._name_plan_queue, json.dumps(item), position="front")
        elif isinstance(pos, int):
            pos_reference = pos if (pos > 0) else (pos + 1)

            item_to_displace = await self._get_item(pos=pos_reference)
            if item_to_displace:
                await self._backend_list_push(self._name_plan_queue, json.dumps(item_to_displace), position="BEFORE")
            else:
                raise RuntimeError(f"Could not find an existing plan at {pos}. Queue size: {qsize0}")
        else:
            raise ValueError(f"Parameter 'pos' has incorrect value: pos='{str(pos)}' (type={type(pos)})")

        self._uid_dict_add(item)
        qsize = await self._get_queue_size()
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
            If ``pos`` is in the range ``0..qsize`` (qsize counted before the new
            item is inserted), the item is inserted to the specified position
            and items at positions ``pos..qsize-1`` are shifted by one position
            to the right. If ``-qsize<pos<0`` the item is inserted at the positon
            counted from the back of the queue (-1 - the last element of the queue).
            If ``pos>qsize`` or ``pos==-1``, the plan is added to the back of
            the queue. If ``pos==0`` or ``pos<-qsize``, the plan is pushed to
            the front of the queue.
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
        Add a batch of items to the queue using the abstraction layer.

        Parameters
        ----------
        items : list(dict)
            List of items (plans or instructions) to be added to the queue.
        pos : int, str or None
            Position index, "front" or "back".
        before_uid : str or None
            If UID is specified, the first item is inserted before the plan with UID.
            ``before_uid`` has precedence over ``after_uid``.
        after_uid : str or None
            If UID is specified, the first item is inserted after the plan with UID.
        filter_parameters : boolean (optional)
            Remove all parameters that do not belong to the list of allowed parameters.
            Default: ``True``.

        Returns
        -------
        items_added : list(dict)
            List of items that were successfully added to the queue.
        results : list(dict)
            List of processing results for each item.
        qsize : int
            Size of the queue after the batch was added.
        success : bool
            Indicates whether the batch was successfully added.
        """
        if self._backend == "sqlite":
            async with self._sqlite_conn.cursor() as cursor:
                items_to_insert = [
                    (self._name_plan_queue, json.dumps(self.filter_item_parameters(item) if filter_parameters else item))
                    for item in items
                ]
                await cursor.executemany(
                    f"INSERT INTO list_store (list_key, value) VALUES (?, ?)", items_to_insert
                )
            await self._sqlite_conn.commit()
            for item in items:
                self._uid_dict_add(item)
            qsize = await self._get_queue_size()
            return items, [{"success": True, "msg": ""} for _ in items], qsize, True

        else:
            # Fallback to existing logic for Redis
            return await super()._add_batch_to_queue(
                items, pos=pos, before_uid=before_uid, after_uid=after_uid, filter_parameters=filter_parameters
            )

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
        Replace an item in the queue using the abstraction layer.

        Parameters
        ----------
        item : dict
            The new item (plan or instruction) to replace the existing one.
        item_uid : str
            UID of the existing item in the queue to be replaced.

        Returns
        -------
        dict, int
            The dictionary of the replaced item and the new size of the queue.

        Raises
        ------
        RuntimeError
            If the item with the given UID is not in the queue or is currently running.
        """
        # Check if the item to be replaced is currently running
        running_item = await self._get_running_item_info()
        running_item_uid = running_item["item_uid"] if running_item else None
        if not self._is_uid_in_dict(item_uid):
            raise RuntimeError(f"Failed to replace item: Item with UID '{item_uid}' is not in the queue")
        if (running_item_uid is not None) and (running_item_uid == item_uid):
            raise RuntimeError(f"Failed to replace item: Item with UID '{item_uid}' is currently running")

        # Generate a new UID for the replacement item if it doesn't already have one
        if "item_uid" not in item:
            item = self.set_new_item_uuid(item)

        # Retrieve the item to be replaced
        item_to_replace = self._uid_dict_get_item(item_uid)
        if item == item_to_replace:
            # If the new item is identical to the existing one, no action is needed
            qsize = await self._get_queue_size()
            return item, qsize

        # Verify the new item, ignoring the UID of the item being replaced
        self._verify_item(item, ignore_uids=[item_uid])

        # Filter the parameters of the new item
        item = self.filter_item_parameters(item)

        # Insert the new item after the old one and then remove the old one
        if self._backend == "redis":
            await self._r_pool.linsert(
                self._name_plan_queue, "AFTER", json.dumps(item_to_replace), json.dumps(item)
            )
        elif self._backend == "sqlite":
            async with self._sqlite_conn.cursor() as cursor:
                # Find the ID of the item to replace
                await cursor.execute(
                    f"SELECT id FROM list_store WHERE list_key=? AND value=?",
                    (self._name_plan_queue, json.dumps(item_to_replace)),
                )
                row = await cursor.fetchone()
                if not row:
                    raise RuntimeError(f"Failed to find the item with UID '{item_uid}' in the queue")
                item_to_replace_id = row[0]

                # Insert the new item after the old one
                await cursor.execute(
                    f"INSERT INTO list_store (list_key, value) VALUES (?, ?)",
                    (self._name_plan_queue, json.dumps(item)),
                )
                new_item_id = cursor.lastrowid

                # Update the order of items to place the new item after the old one
                await cursor.execute(
                    f"UPDATE list_store SET id = id + 1 WHERE list_key=? AND id > ?",
                    (self._name_plan_queue, item_to_replace_id),
                )
                await cursor.execute(
                    f"UPDATE list_store SET id = ? WHERE id = ?", (item_to_replace_id + 1, new_item_id)
                )
            await self._sqlite_conn.commit()

        # Remove the old item
        await self._remove_item(item_to_replace)

        # Update the UID dictionary
        self._uid_dict_remove(item_uid)
        self._uid_dict_add(item)

        # Get the updated queue size
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
        Move an item within the queue using the abstraction layer.

        Parameters
        ----------
        pos : int, str or None
            Position of the source item in the queue ("front", "back", or an integer index).
        uid : str or None
            UID of the source item to move. If specified, `pos` is ignored.
        pos_dest : int, str or None
            Position to move the item to ("front", "back", or an integer index).
        before_uid : str or None
            UID of the item before which the source item should be moved.
        after_uid : str or None
            UID of the item after which the source item should be moved.

        Returns
        -------
        dict, int
            The dictionary of the moved item and the new size of the queue.

        Raises
        ------
        ValueError
            If the parameters are ambiguous or invalid.
        IndexError
            If the source or destination item is not found.
        """
        if (pos is None) and (uid is None):
            raise ValueError("Source position or UID is not specified.")
        if (pos_dest is None) and (before_uid is None) and (after_uid is None):
            raise ValueError("Destination position or UID is not specified.")
        if (pos is not None) and (uid is not None):
            raise ValueError("Ambiguous parameters: Both position and UID are specified for the source item.")
        if (pos_dest is not None) and (before_uid is not None or after_uid is not None):
            raise ValueError("Ambiguous parameters: Both position and UID are specified for the destination item.")
        if (before_uid is not None) and (after_uid is not None):
            raise ValueError("Ambiguous parameters: Source should be moved 'before' and 'after' the destination.")

        # Retrieve the source item
        try:
            if uid is not None:
                item_source = await self._get_item(uid=uid)
            else:
                item_source = await self._get_item(pos=pos)
        except Exception as ex:
            raise IndexError(f"Source item not found: {str(ex)}")

        # Retrieve the destination item
        before = True
        try:
            if before_uid is not None or after_uid is not None:
                uid_dest = before_uid if before_uid else after_uid
                before = uid_dest == before_uid
                item_dest = await self._get_item(uid=uid_dest)
            else:
                item_dest = await self._get_item(pos=pos_dest)
                before = pos_dest == "front" or (isinstance(pos_dest, int) and pos_dest < 0)
        except Exception as ex:
            raise IndexError(f"Destination item not found: {str(ex)}")

        # If the source and destination are the same, do nothing
        if item_source["item_uid"] == item_dest["item_uid"]:
            qsize = await self._get_queue_size()
            return item_source, qsize

        # Remove the source item from the queue
        item, _ = await self._pop_item_from_queue(uid=item_source["item_uid"])

        # Add the source item to the new position
        if before:
            item, qsize = await self._add_item_to_queue(item, before_uid=item_dest["item_uid"], filter_parameters=False)
        else:
            item, qsize = await self._add_item_to_queue(item, after_uid=item_dest["item_uid"], filter_parameters=False)

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
        Move a batch of items within the queue using the abstraction layer.

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
            Arrange moved items according to the order of UIDs in the ``uids`` list (``False``) or
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
        uids = uids or []

        if not isinstance(uids, list):
            raise TypeError(f"Parameter 'uids' must be a list: type(uids) = {type(uids)}")

        # Ensure only one of the mutually exclusive parameters is specified
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
            raise ValueError(f"The list contains repeated UIDs ({len(uids) - len(uids_set)} UIDs)")

        # Check if all UIDs in 'uids' exist in the queue
        uids_missing = [uid for uid in uids if not self._is_uid_in_dict(uid)]
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

        # Perform the 'move' operation
        last_item_uid = None
        items_moved = []
        for uid in uids_prepared:
            if last_item_uid is None:
                # First item is moved according to specified parameters
                item, _ = await self._move_item(
                    uid=uid, pos_dest=pos_dest, before_uid=before_uid, after_uid=after_uid
                )
            else:
                # Consecutive items are placed after the last moved item
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
        Clear the queue using the abstraction layer.
        """
        self._plan_queue_uid = self.new_item_uid()

        # Clear the queue in the backend
        await self._backend_delete(self._name_plan_queue)

        # Remove all entries from 'self._uid_dict' except the running item
        running_item = await self._get_running_item_info()
        if running_item:
            uid = running_item["item_uid"]
            item = self._uid_dict_get_item(uid)
            self._uid_dict_clear()
            self._uid_dict_add(item)
        else:
            self._uid_dict_clear()

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
        Add an item to history using the abstraction layer.

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
        item_json = json.dumps(item)

        # Add the item to the history using the abstraction layer
        await self._backend_list_push(self._name_plan_history, item_json, position="back")

        # Get the updated history size
        history_size = await self._get_history_size()
        return history_size

    async def _get_history_size(self):
        """
        Get the size of the history using the abstraction layer.

        Returns
        -------
        int
            The number of items in the history.
        """
        if self._backend == "redis":
            return await self._r_pool.llen(self._name_plan_history)
        elif self._backend == "sqlite":
            async with self._sqlite_conn.cursor() as cursor:
                await cursor.execute(f"SELECT COUNT(*) FROM list_store WHERE list_key=?", (self._name_plan_history,))
                row = await cursor.fetchone()
            return row[0] if row else 0

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
        Retrieve the history from the backend using the abstraction layer.

        Returns
        -------
        list(dict)
            The list of items in the plan history. Each item is represented as a dictionary.
        str
            Plan history UID.
        """
        all_plans_json = await self._backend_list_range(self._name_plan_history, 0, -1)
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
        Clear the plan history using the abstraction layer.

        This method removes all entries from the plan history.
        """
        self._plan_history_uid = self.new_item_uid()

        # Clear the history in the backend
        await self._backend_delete(self._name_plan_history)

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
        Clean unnecessary item properties before adding the item to history or returning it to the queue.

        Parameters
        ----------
        item : dict
            The item (plan) represented as a dictionary of parameters.

        Returns
        -------
        dict
            A cleaned copy of the item with unnecessary properties removed.
        """
        item = copy.deepcopy(item)  # Create a deep copy to avoid modifying the original item
        properties = item.get("properties", {})

        # Remove unnecessary properties
        properties.pop("immediate_execution", None)  # Internal flag, not exposed to users
        properties.pop("time_start", None)  # Temporary parameter, should be removed

        # Remove the 'properties' key if it is empty
        if not properties:
            item.pop("properties", None)

        return item

    async def _process_next_item(self, *, item=None):
        """
        Process the next item in the queue using the abstraction layer.

        Parameters
        ----------
        item : dict or None
            If provided, the item is processed for immediate execution. Otherwise, the next
            item in the queue is processed.

        Returns
        -------
        dict
            The item that was processed.
        """
        loop_mode = self._plan_queue_mode["loop"]

        # Determine if the item is for immediate execution
        immediate_execution = bool(item)
        if immediate_execution:
            # Generate UID if it does not exist or create a deep copy
            item = copy.deepcopy(item) if "item_uid" in item else self.set_new_item_uuid(item)
        else:
            # Retrieve the item from the front of the queue
            item = await self._get_item(pos="front")

        item_to_return = item
        if item:
            item_type = item["item_type"]
            if item_type == "plan":
                # If the item is a plan, set it as the next running item
                kwargs = {"item": item} if immediate_execution else {}
                item_to_return = await self._set_next_item_as_running(**kwargs)
            elif not immediate_execution:
                # If the item is not a plan, pop it from the front of the queue
                await self._pop_item_from_queue(pos="front")
                if loop_mode:
                    # If loop mode is enabled, add the item back to the end of the queue
                    item_to_add = self.set_new_item_uuid(item)
                    await self._add_item_to_queue(item_to_add)

        return item_to_return

    async def process_next_item(self, *, item=None):
        """
        Process the next item in the queue. If the item is a plan, it is set as currently
        running (``self.set_next_item_as_running``), otherwise it is popped from the queue.
        If the queue is LOOP mode, then it the item other than plan is pushed to the back
        of the queue. (Plans are pushed to the back of the queue upon successful completion.)

        If ``item`` is a plan, then the plan is set for immediate execution. If ``item`` is
        an instruction, then the function does nothing.

        If an item submitted for 'immediate' execution has no UID, a new UID is created.
        The returned plan is in the exact form, which is set for execution.

        For more details on processing of queue plans, see the description for
        ``self.set_next_item_as_running`` method.
        """
        async with self._lock:
            return await self._process_next_item(item=item)

    async def _set_next_item_as_running(self, *, item=None):
        """
        Set the next item in the queue as running using the abstraction layer.

        Parameters
        ----------
        item : dict or None
            If provided, the item is set for immediate execution. Otherwise, the next
            item in the queue is set as running.

        Returns
        -------
        dict
            The item that was set as running. If no item is available or another item
            is already running, an empty dictionary is returned.

        Raises
        ------
        RuntimeError
            If the item is not a plan or another item is already running.
        """
        immediate_execution = bool(item)
        if immediate_execution:
            # Generate UID if it does not exist or create a deep copy
            item = copy.deepcopy(item) if "item_uid" in item else self.set_new_item_uuid(item)
            item.setdefault("properties", {})["immediate_execution"] = True

        try:
            # Check if another item is already running
            if await self._is_item_running():
                raise RuntimeError("Another item is already running.")

            if immediate_execution:
                plan = item
            else:
                # Retrieve the next item from the front of the queue
                plan = await self._get_item(pos="front")
                if not plan:
                    raise RuntimeError("No item available in the queue to set as running.")

            if "item_type" not in plan or plan["item_type"] != "plan":
                raise RuntimeError(
                    f"Cannot set the item as running. Expected a plan, but got: {plan}"
                )

            if not immediate_execution:
                # Remove the plan from the front of the queue
                await self._pop_item_from_queue(pos="front")

            # Record the start time for the plan
            plan.setdefault("properties", {})["time_start"] = ttime.time()

            # Set the plan as the currently running item
            await self._set_running_item_info(plan)
            self._plan_queue_uid = self.new_item_uid()

        except RuntimeError:
            raise
        except Exception:
            plan = {}

        return plan

    async def set_next_item_as_running(self, *, item=None):
        """
        Sets the next item from the queue as 'running'. The item MUST be a plan
        (e.g. not an instruction), otherwise an exception will be raised. The item is removed
        from the queue. UID remains in ``self._uid_dict``, i.e. item with the same UID
        may not be added to the queue while it is being executed. If ``item`` parameter
        represents a plan, then the plan is set for immediate execution.

        This function can only be applied to the plans. Use ``process_next_item`` that
        works correctly for any item (it calls this function if an item is a plan).

        If a plan submitted for 'immediate' execution has no UID, a new UID is created.
        The returned plan is in the exact form, which is set for execution.

        Parameters
        ----------
        item: dict or None
            The dictionary that represents a plan submitted for immediate execution.
            If ``item`` is a plan, then the queue remains intact and the plan is
            set for immediate execution. If ``item=None``, then the top queue item
            is removed from the queue and set for execution.

        Returns
        -------
        dict
            The item that was set as currently running. If another item is currently
            set as 'running' or the queue is empty, then ``{}`` is returned.

        Raises
        ------
        RuntimeError
            The function is called for an item other than plan.
        """
        async with self._lock:
            return await self._set_next_item_as_running(item=item)

    async def _set_processed_item_as_completed(self, *, exit_status, run_uids, scan_ids, err_msg, err_tb):
        """
        Mark the currently running item as completed and move it to history using the abstraction layer.

        Parameters
        ----------
        exit_status : str
            Completion status of the plan (e.g., "completed", "failed").
        run_uids : list(str)
            A list of UIDs of completed runs.
        scan_ids : list(int)
            A list of scan IDs for the completed runs.
        err_msg : str
            Error message in case of failure.
        err_tb : str
            Traceback in case of failure.

        Returns
        -------
        dict
            The item added to the history, including the `exit_status`. If no item is running, returns an empty dictionary.
        """
        # Check if loop mode is enabled
        loop_mode = self._plan_queue_mode["loop"]

        # If an item is running, process it
        if await self._is_item_running():
            item = await self._get_running_item_info()
            immediate_execution = item.get("properties", {}).get("immediate_execution", False)
            item_time_start = item["properties"].get("time_start", None)
            item_cleaned = self._clean_item_properties(item)

            # If loop mode is enabled and the item is not for immediate execution, add it back to the queue
            if loop_mode and not immediate_execution:
                item_to_add = self.set_new_item_uuid(item_cleaned.copy())
                await self._backend_list_push(self._name_plan_queue, json.dumps(item_to_add), position="back")
                self._uid_dict_add(item_to_add)

            # Add result details to the item
            item_cleaned.setdefault("result", {})
            item_cleaned["result"]["exit_status"] = exit_status
            item_cleaned["result"]["run_uids"] = run_uids
            item_cleaned["result"]["scan_ids"] = scan_ids
            item_cleaned["result"]["time_start"] = item_time_start
            item_cleaned["result"]["time_stop"] = ttime.time()
            item_cleaned["result"]["msg"] = err_msg
            item_cleaned["result"]["traceback"] = err_tb

            # Clear the running item info
            await self._clear_running_item_info()

            # If not in loop mode and not immediate execution, remove the UID from the dictionary
            if not loop_mode and not immediate_execution:
                self._uid_dict_remove(item["item_uid"])

            # Update the plan queue UID
            self._plan_queue_uid = self.new_item_uid()

            # Add the cleaned item to history
            await self._add_to_history(item_cleaned)
        else:
            # If no item is running, return an empty dictionary
            item_cleaned = {}

        return item_cleaned

    async def set_processed_item_as_completed(self, *, exit_status, run_uids, scan_ids, err_msg, err_tb):
        """
        Moves currently executed item (plan) to history and sets ``exit_status`` key.
        UID is removed from ``self._uid_dict``, so a copy of the item with
        the same UID may be added to the queue.

        Known ``exit_status`` values: ``"completed"`` and ``"unknown"`` (status
        information was lost due to restart of RE Manager, assume that the completion
        was successful and start the next plan).

        Parameters
        ----------
        exit_status: str
            Completion status of the plan.
        run_uids: list(str)
            A list of uids of completed runs.
        scan_ids: list(int)
            A list of scan IDs for the completed runs.
        err_msg: str
            Error message in case of failure.
        err_tb: str
            Traceback in case of failure.

        Returns
        -------
        dict
            The item added to the history including ``exit_status``. If another item (plan)
            is currently running, then ``{}`` is returned.
        """
        async with self._lock:
            return await self._set_processed_item_as_completed(
                exit_status=exit_status, run_uids=run_uids, scan_ids=scan_ids, err_msg=err_msg, err_tb=err_tb
            )

    async def _set_processed_item_as_stopped(self, *, exit_status, run_uids, scan_ids, err_msg, err_tb):
        """
        Mark the currently running item as stopped and move it to history using the abstraction layer.

        Parameters
        ----------
        exit_status : str
            Completion status of the plan (e.g., "stopped", "failed", "aborted", "halted").
        run_uids : list(str)
            A list of UIDs of completed runs.
        scan_ids : list(int)
            A list of scan IDs for the completed runs.
        err_msg : str
            Error message in case of failure.
        err_tb : str
            Traceback in case of failure.

        Returns
        -------
        dict
            The item added to the history, including the `exit_status`. If no item is running, returns an empty dictionary.
        """
        if await self._is_item_running():
            # Retrieve the currently running item
            item = await self._get_running_item_info()
            immediate_execution = item.get("properties", {}).get("immediate_execution", False)
            item_time_start = item["properties"].get("time_start", None)
            item_cleaned = self._clean_item_properties(item)

            # Add result details to the item
            item_cleaned.setdefault("result", {})
            item_cleaned["result"]["exit_status"] = exit_status
            item_cleaned["result"]["run_uids"] = run_uids
            item_cleaned["result"]["scan_ids"] = scan_ids
            item_cleaned["result"]["time_start"] = item_time_start
            item_cleaned["result"]["time_stop"] = ttime.time()
            item_cleaned["result"]["msg"] = err_msg
            item_cleaned["result"]["traceback"] = err_tb

            # Add the cleaned item to history
            await self._add_to_history(item_cleaned)

            # Clear the running item info
            await self._clear_running_item_info()

            # If the item is not for immediate execution, remove its UID from the dictionary
            if not immediate_execution:
                self._uid_dict_remove(item["item_uid"])

                # If the exit status is not "stopped", push the item back to the front of the queue
                if exit_status != "stopped":
                    item_pushed_to_queue = self.set_new_item_uuid(item_cleaned)
                    await self._add_item_to_queue(item_pushed_to_queue, pos="front", filter_parameters=False)

            # Update the plan queue UID
            self._plan_queue_uid = self.new_item_uid()

            return item_cleaned
        else:
            # If no item is running, return an empty dictionary
            return {}

    async def set_processed_item_as_stopped(self, *, exit_status, run_uids, scan_ids, err_msg, err_tb):
        """
        A stopped plan is considered successfully completed (if ``exit_status=="stopped"``) or
        failed (otherwise). All items are added to history with respective ``exit_status``.
        Failed items are pushed to the beginning of the queue. Item UID is removed in ``self._uid_dict``.
        A new ``item_uid`` is generated for the item that is pushed back into the queue.

        Known ``exit_status`` values: ``"failed"``, ``"stopped"`` (success), ``"aborted"``, ``"halted"``.

        Parameters
        ----------
        exit_status: str
            Completion status of the plan.
        run_uids: list(str)
            A list of uids of completed runs.
        scan_ids: list(int)
            A list of scan IDs for the completed runs.
        err_msg: str
            Error message in case of failure.
        err_tb: str
            Traceback in case of failure.

        Returns
        -------
        dict
            The item (plan) added to the history including ``exit_status``. If no item (plan)
            is running, then the function returns ``{}``. The item pushed back into the queue
            will have different ``item_uid`` than the item added to the history.
        """
        async with self._lock:
            return await self._set_processed_item_as_stopped(
                exit_status=exit_status, run_uids=run_uids, scan_ids=scan_ids, err_msg=err_msg, err_tb=err_tb
            )

    # =============================================================================================
    #         Methods for saving and retrieving user group permissions.

    async def user_group_permissions_clear(self):
        """
        Clear user group permissions saved in the backend using the abstraction layer.
        """
        await self._backend_delete(self._name_user_group_permissions)

    async def user_group_permissions_save(self, user_group_permissions):
        """
        Save user group permissions to the backend using the abstraction layer.

        Parameters
        ----------
        user_group_permissions: dict
            A dictionary containing user group permissions.
        """
        await self._backend_set(self._name_user_group_permissions, json.dumps(user_group_permissions))

    async def user_group_permissions_retrieve(self):
        """
        Retrieve saved user group permissions using the abstraction layer.

        Returns
        -------
        dict or None
            Returns a dictionary with saved user group permissions or ``None`` if no permissions are saved.
        """
        ugp_json = await self._backend_get(self._name_user_group_permissions)
        return json.loads(ugp_json) if ugp_json else None

    # =============================================================================================
    #         Methods for saving and retrieving lock info.

    async def lock_info_clear(self):
        """
        Clear lock info saved in the backend using the abstraction layer.
        """
        await self._backend_delete(self._name_lock_info)

    async def lock_info_save(self, lock_info):
        """
        Save lock info to the backend using the abstraction layer.

        Parameters
        ----------
        lock_info: dict
            A dictionary containing lock info.
        """
        await self._backend_set(self._name_lock_info, json.dumps(lock_info))

    async def lock_info_retrieve(self):
        """
        Retrieve saved lock info using the abstraction layer.

        Returns
        -------
        dict or None
            Returns a dictionary with saved lock info or ``None`` if no lock info is saved.
        """
        lock_info_json = await self._backend_get(self._name_lock_info)
        return json.loads(lock_info_json) if lock_info_json else None

    # =============================================================================================
    #         Methods for saving and retrieving 'queue_stop_pending'.

    async def stop_pending_clear(self):
        """
        Clear 'stop_pending' mode info saved in the backend using the abstraction layer.
        """
        await self._backend_delete(self._name_stop_pending_info)

    async def stop_pending_save(self, stop_pending):
        """
        Save 'stop_pending' mode info to the backend using the abstraction layer.

        Parameters
        ----------
        stop_pending: dict
            A dictionary containing 'stop_pending' mode info.
        """
        await self._backend_set(self._name_stop_pending_info, json.dumps(stop_pending))
        
    async def stop_pending_retrieve(self):
        """
        Retrieve saved 'stop_pending' mode info using the abstraction layer.

        Returns
        -------
        dict or None
            Returns a dictionary with saved 'stop_pending' mode info or ``None`` if no 'stop_pending' info is saved.
        """
        stop_pending_json = await self._backend_get(self._name_stop_pending_info)
        return json.loads(stop_pending_json) if stop_pending_json else None

    # =============================================================================================
    #         Methods for saving and retrieving 'autostart' mode.

    async def autostart_mode_clear(self):
        """
        Clear 'autostart' mode info saved in the backend using the abstraction layer.
        """
        await self._backend_delete(self._name_autostart_mode_info)

    async def autostart_mode_save(self, autostart_info):
        """
        Save 'autostart' mode info to the backend using the abstraction layer.

        Parameters
        ----------
        autostart_info: dict
            A dictionary containing 'autostart' mode info.
        """
        await self._backend_set(self._name_autostart_mode_info, json.dumps(autostart_info))

    async def autostart_mode_retrieve(self):
        """
        Retrieve saved 'autostart' mode info using the abstraction layer.

        Returns
        -------
        dict or None
            Returns a dictionary with saved 'autostart' mode info or ``None`` if no 'autostart' info is saved.
        """
        autostart_info_json = await self._backend_get(self._name_autostart_mode_info)
        return json.loads(autostart_info_json) if autostart_info_json else None

    # =============================================================================================
    #         Helper methods to handle common operations.

    async def _backend_get(self, key):
        if self._backend == "redis":
            return await self._r_pool.get(key)
        elif self._backend == "sqlite":
            async with self._sqlite_conn.cursor() as cursor:
                await cursor.execute(f"SELECT value FROM kv_store WHERE key=?", (key,))
                row = await cursor.fetchone()
            return row[0] if row else None

    async def _backend_set(self, key, value):
        if self._backend == "redis":
            await self._r_pool.set(key, value)
        elif self._backend == "sqlite":
            async with self._sqlite_conn.cursor() as cursor:
                await cursor.execute(
                    f"INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)", (key, value)
                )
            await self._sqlite_conn.commit()

    async def _backend_delete(self, key):
        if self._backend == "redis":
            await self._r_pool.delete(key)
        elif self._backend == "sqlite":
            async with self._sqlite_conn.cursor() as cursor:
                await cursor.execute(f"DELETE FROM kv_store WHERE key=?", (key,))
            await self._sqlite_conn.commit()

    async def _backend_list_push(self, key, value, position="back"):
        if self._backend == "redis":
            if position == "back":
                await self._r_pool.rpush(key, value)
            elif position == "front":
                await self._r_pool.lpush(key, value)
        if self._backend == "sqlite":
            async with self._sqlite_conn.cursor() as cursor:
                if position == "back":
                    await cursor.execute(
                        f"INSERT INTO list_store (list_key, value) VALUES (?, ?)", (key, value)
                    )
                elif position == "front":
                    # Insert at the front by decrementing IDs
                    await cursor.execute(
                        f"UPDATE list_store SET id = id + 1 WHERE list_key=?", (key,)
                    )
                    await cursor.execute(
                        f"INSERT INTO list_store (id, list_key, value) VALUES (1, ?, ?)", (key, value)
                    )
            await self._sqlite_conn.commit()

    async def _backend_list_pop(self, key, position="back"):
        if self._backend == "redis":
            if position == "back":
                return await self._r_pool.rpop(key)
            elif position == "front":
                return await self._r_pool.lpop(key)
        elif self._backend == "sqlite":
            async with self._sqlite_conn.cursor() as cursor:
                if position == "back":
                    await cursor.execute(
                        f"DELETE FROM list_store WHERE id = (SELECT id FROM list_store WHERE list_key=? ORDER BY id DESC LIMIT 1) RETURNING value",
                        (key,)
                    )
                elif position == "front":
                    await cursor.execute(
                        f"DELETE FROM list_store WHERE id = (SELECT id FROM list_store WHERE list_key=? ORDER BY id ASC LIMIT 1) RETURNING value",
                        (key,)
                    )
                row = await cursor.fetchone()
                return row[0] if row else None
            return None

    async def _backend_list_range(self, key, start=0, end=-1):
        if self._backend == "redis":
            return await self._r_pool.lrange(key, start, end)
        if self._backend == "sqlite":
            async with self._sqlite_conn.cursor() as cursor:
                limit = end - start + 1 if end != -1 else None
                offset = start
                await cursor.execute(
                    f"SELECT value FROM list_store WHERE list_key=? ORDER BY id ASC LIMIT ? OFFSET ?",
                    (key, limit, offset)
                )
                rows = await cursor.fetchall()
            return [row[0] for row in rows]
