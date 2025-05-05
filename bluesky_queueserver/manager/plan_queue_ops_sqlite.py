import asyncio                      # Necessary (used for async operations and `asyncio.Lock`)
import aiosqlite                    # Necessary (used for SQLite operations)
import os                           # Necessary (used for environment variable and file path handling)
import json                         # Necessary (used for JSON serialization/deserialization)
import logging                      # Necessary (used for logging)
import time as ttime                # Necessary (used for timestamps in `set_processed_item_as_completed` and `set_processed_item_as_stopped`)
import uuid                         # Necessary (used for generating unique UIDs)
from bluesky_queueserver.manager.plan_queue_ops_abstract import AbstractPlanQueueOperations  # Necessary (base class)
# from bluesky_queueserver.manager.plan_queue_ops_base import BasePlanQueueOperations  # Necessary (base class for plan queue operations)

logger = logging.getLogger(__name__)

class SQLitePlanQueueOperations(AbstractPlanQueueOperations): # Add a BasePlanQueueOperations class to ensure methods are implemented equivalently
    """
    The class supports operations with plan queue based on SQLite. The public methods
    of the class are protected with ``asyncio.Lock``.

    Parameters
    ----------
    sqlite_db_path: str
        Path to the SQLite database file.

    name_prefix: str
        Prefix for the names of the tables used in SQLite. The prefix is used to avoid conflicts
        with the tables used by other instances of Queue Server. For example, the prefix used
        for unit tests should be different from the prefix used in production. If the prefix
        is an empty string, then no prefix will be added (not recommended).
    """

    def __init__(self, sqlite_db_path=None, name_prefix="qs_default"):
        """
        Initialize the SQLitePlanQueueOperations class.

        Parameters
        ----------
        sqlite_db_path : str or None
            Path to the SQLite database file. If None, the path is determined by the
            environment variable `PLAN_QUEUE_SQLITE_PATH`. If the environment variable
            is not set, the default is the current working directory.
        name_prefix : str
            Prefix for the names of the tables used in SQLite.
        """

        # Determine the SQLite database path
        if sqlite_db_path is None:
            sqlite_db_path = os.getenv("PLAN_QUEUE_SQLITE_PATH", os.path.join(os.getcwd(), "plan_queue.db"))

        self._sqlite_db_path = sqlite_db_path
        self._sqlite_conn = None

        if not isinstance(name_prefix, str):
            raise TypeError(f"Parameter 'name_prefix' should be a string: {name_prefix}")

        # The case of an empty string
        if name_prefix:
            name_prefix = name_prefix + "_"

        self._table_running_plan = name_prefix + "running_plan"
        self._table_plan_queue = name_prefix + "plan_queue"
        self._table_plan_history = name_prefix + "plan_history"
        self._table_plan_queue_mode = name_prefix + "plan_queue_mode"

        # SQLite is also used for storage of some additional information not related to the queue.
        #   The class contains only the functions for saving and retrieving the data, which is
        #   not used by other functions of the class.
        self._table_user_group_permissions = name_prefix + "user_group_permissions"
        self._table_lock_info = name_prefix + "lock_info"
        self._table_autostart_mode_info = name_prefix + "autostart_mode_info"
        self._table_stop_pending_info = name_prefix + "stop_pending_info"

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
        #      SQLite, so that it is not modified between restarts of the manager.
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
        """Get the current plan queue UID."""
        return self._plan_queue_uid

    @property
    def plan_history_uid(self):
        """Get the current plan history UID."""
        return self._plan_history_uid

    @property
    def plan_queue_mode(self):
        """Get or set the current plan queue mode."""
        return self._plan_queue_mode

    async def start(self):
        """Start the backend connection."""
        if not self._sqlite_conn:
            self._sqlite_conn = await aiosqlite.connect(self._sqlite_db_path)
            await self._initialize_tables()

    async def stop(self):
        """Stop the backend connection."""
        if self._sqlite_conn:
            await self._sqlite_conn.close()
            self._sqlite_conn = None

    async def _initialize_tables(self):
        """Initialize SQLite tables if they do not exist."""
        async with self._sqlite_conn.cursor() as cursor:
            # Table for the plan queue
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table_plan_queue} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item TEXT
                )
            """)
            # Table for the plan history
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table_plan_history} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item TEXT
                )
            """)
            # Table for the running plan
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table_running_plan} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item TEXT
                )
            """)
            # Table for the plan queue mode
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table_plan_queue_mode} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mode TEXT
                )
            """)
            # Table for UIDs
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS uids (
                    id INTEGER PRIMARY KEY AUTOINCREMENT
                )
            """)
            # Insert the first UID if the table is empty
            await cursor.execute("INSERT OR IGNORE INTO uids (id) VALUES (1)")
        await self._sqlite_conn.commit()

    async def add_item_to_queue(self, item, pos=None, before_uid=None, after_uid=None):
        """Add a single item to the queue."""
        async with self._sqlite_conn.cursor() as cursor:
            # Generate a new UID for the item
            if "item_uid" not in item:
                item["item_uid"] = await self.new_item_uid()
            item_json = json.dumps(item)
            if pos == "front":
                await cursor.execute(f"INSERT INTO {self._table_plan_queue} (item) VALUES (?)", (item_json,))
            else:  # Default to adding to the back
                await cursor.execute(f"INSERT INTO {self._table_plan_queue} (item) VALUES (?)", (item_json,))
        await self._sqlite_conn.commit()

    async def get_queue_size(self):
        """Get the size of the queue."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
            row = await cursor.fetchone()
        return row[0]

    async def get_queue_full(self):
        """Retrieve the full queue."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT item FROM {self._table_plan_queue}")
            rows = await cursor.fetchall()
        return [json.loads(row[0]) for row in rows]

    async def clear_queue(self):
        """Clear the plan queue."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"DELETE FROM {self._table_plan_queue}")
        await self._sqlite_conn.commit()

    async def set_plan_queue_mode(self, plan_queue_mode, update=True):
        """Set the plan queue mode."""
        async with self._sqlite_conn.cursor() as cursor:
            mode_json = json.dumps(plan_queue_mode)
            await cursor.execute(f"DELETE FROM {self._table_plan_queue_mode}")
            await cursor.execute(f"INSERT INTO {self._table_plan_queue_mode} (mode) VALUES (?)", (mode_json,))
        await self._sqlite_conn.commit()

    async def get_running_item_info(self):
        """Retrieve information about the currently running item."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT item FROM {self._table_running_plan} LIMIT 1")
            row = await cursor.fetchone()
        return json.loads(row[0]) if row else {}

    async def set_processed_item_as_completed(self, exit_status, run_uids, scan_ids, err_msg="", err_tb=""):
        """
        Mark a processed item as completed and move it to the history table.

        Parameters
        ----------
        exit_status : str
            Completion status of the plan (e.g., "completed").
        run_uids : list
            List of run UIDs associated with the completed plan.
        scan_ids : list
            List of scan IDs associated with the completed plan.
        err_msg : str, optional
            Error message in case of failure.
        err_tb : str, optional
            Traceback in case of failure.

        Returns
        -------
        dict
            The item added to the history, including the completion status.
        """
        async with self._sqlite_conn.cursor() as cursor:
            # Retrieve the currently running item
            await cursor.execute(f"SELECT item FROM {self._table_running_plan} LIMIT 1")
            row = await cursor.fetchone()

            if not row:
                return {}  # No running item found

            # Parse the running item
            item = json.loads(row[0])

            # Add result details to the item
            item.setdefault("result", {})
            item["result"]["exit_status"] = exit_status
            item["result"]["run_uids"] = run_uids
            item["result"]["scan_ids"] = scan_ids
            item["result"]["time_start"] = item.get("properties", {}).get("time_start", None)
            item["result"]["time_stop"] = ttime.time()
            item["result"]["msg"] = err_msg
            item["result"]["traceback"] = err_tb

            # Add the item to the history table
            item_json = json.dumps(item)
            await cursor.execute(f"INSERT INTO {self._table_plan_history} (item) VALUES (?)", (item_json,))

            # Clear the running plan table
            await cursor.execute(f"DELETE FROM {self._table_running_plan}")

        # Commit the changes
        await self._sqlite_conn.commit()

        return item

    async def set_processed_item_as_stopped(self, exit_status, run_uids, scan_ids, err_msg="", err_tb=""):
        """
        Mark a processed item as stopped and move it to the history table.
        """
        async with self._sqlite_conn.cursor() as cursor:
            # Retrieve the currently running item
            await cursor.execute(f"SELECT item FROM {self._table_running_plan} LIMIT 1")
            row = await cursor.fetchone()

            if not row:
                return {}  # No running item found

            # Parse the running item
            item = json.loads(row[0])

            # Add result details to the item
            item.setdefault("result", {})
            item["result"]["exit_status"] = exit_status
            item["result"]["run_uids"] = run_uids
            item["result"]["scan_ids"] = scan_ids
            item["result"]["time_start"] = item.get("properties", {}).get("time_start", None)
            item["result"]["time_stop"] = ttime.time()
            item["result"]["msg"] = err_msg
            item["result"]["traceback"] = err_tb

            # Add the item to the history table
            item_json = json.dumps(item)
            await cursor.execute(f"INSERT INTO {self._table_plan_history} (item) VALUES (?)", (item_json,))

            # Clear the running plan table
            await cursor.execute(f"DELETE FROM {self._table_running_plan}")

            # If the plan was not stopped successfully, re-add it to the front of the queue
            if exit_status not in ["stopped", "completed"]:
                item["item_uid"] = await self.new_item_uid()  # Use new_item_uid instead of uuid.uuid4()
                item_json = json.dumps(item)
                await cursor.execute(f"INSERT INTO {self._table_plan_queue} (item) VALUES (?)", (item_json,))

        # Commit the changes
        await self._sqlite_conn.commit()

        return item

    async def user_group_permissions_save(self, user_group_permissions):
        """Save user group permissions."""
        async with self._sqlite_conn.cursor() as cursor:
            permissions_json = json.dumps(user_group_permissions)
            await cursor.execute(f"DELETE FROM {self._table_user_group_permissions}")
            await cursor.execute(f"INSERT INTO {self._table_user_group_permissions} (permissions) VALUES (?)", (permissions_json,))
        await self._sqlite_conn.commit()

    async def user_group_permissions_retrieve(self):
        """Retrieve user group permissions."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT permissions FROM {self._table_user_group_permissions} LIMIT 1")
            row = await cursor.fetchone()
        return json.loads(row[0]) if row else None
    
    async def lock_info_clear(self):
        """
        Clear lock information stored in the SQLite database.
        """
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"DELETE FROM {self._table_lock_info}")
        await self._sqlite_conn.commit()

    async def new_item_uid(self):
        """
        Generate a new item UID. This method is not applicable to the SQLite backend.
        """
        raise NotImplementedError("The method 'new_item_uid' is not applicable to the SQLite backend.")
    
    async def delete_pool_entries(self):
        """
        Delete all pool entries used by the SQLite backend.
        """
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"DELETE FROM {self._table_plan_queue}")
            await cursor.execute(f"DELETE FROM {self._table_plan_history}")
            await cursor.execute(f"DELETE FROM {self._table_running_plan}")
            await cursor.execute(f"DELETE FROM {self._table_plan_queue_mode}")
        await self._sqlite_conn.commit()

    async def user_group_permissions_clear(self):
        """
        Clear user group permissions stored in the SQLite database.
        """
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"DELETE FROM {self._table_user_group_permissions}")
        await self._sqlite_conn.commit()

    async def autostart_mode_clear(self):
        """
        Clear the autostart mode information stored in the SQLite database.
        """
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"DELETE FROM {self._table_autostart_mode_info}")
        await self._sqlite_conn.commit()

    async def stop_pending_clear(self):
        """
        Clear the stop-pending state information stored in the SQLite database.
        """
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"DELETE FROM {self._table_stop_pending_info}")
        await self._sqlite_conn.commit()

#### Non interface methods
#### UID operations

    async def new_item_uid(self):
        """
        Generate a new item UID using the SQLite backend.
        """
        async with self._sqlite_conn.cursor() as cursor:
            # Increment the UID counter and retrieve the new UID
            await cursor.execute("INSERT INTO uids DEFAULT VALUES")
            await cursor.execute("SELECT last_insert_rowid()")
            row = await cursor.fetchone()
        return str(row[0])  # Return the UID as a string
    
#### queue operations

    async def replace_item(self, item, uid):
        """Replace an item in the queue by its UID."""
        async with self._sqlite_conn.cursor() as cursor:
            item_json = json.dumps(item)
            await cursor.execute(f"""
                UPDATE {self._table_plan_queue}
                SET item = ?
                WHERE json_extract(item, '$.item_uid') = ?
            """, (item_json, uid))
        await self._sqlite_conn.commit()

    async def move_item(self, uid, pos=None, before_uid=None, after_uid=None):
        """Move an item to a new position in the queue."""
        async with self._sqlite_conn.cursor() as cursor:
            # Retrieve the item to be moved
            await cursor.execute(f"""
                SELECT item FROM {self._table_plan_queue}
                WHERE json_extract(item, '$.item_uid') = ?
            """, (uid,))
            row = await cursor.fetchone()
            if not row:
                raise ValueError(f"Item with UID {uid} not found in the queue.")
            item = json.loads(row[0])

            # Remove the item from the queue
            await cursor.execute(f"""
                DELETE FROM {self._table_plan_queue}
                WHERE json_extract(item, '$.item_uid') = ?
            """, (uid,))

            # Re-insert the item at the specified position
            item_json = json.dumps(item)
            if pos == "front":
                await cursor.execute(f"INSERT INTO {self._table_plan_queue} (item) VALUES (?)", (item_json,))
            else:  # Default to adding to the back
                await cursor.execute(f"INSERT INTO {self._table_plan_queue} (item) VALUES (?)", (item_json,))
        await self._sqlite_conn.commit()

    async def pop_item_from_queue(self):
        """Remove and return the first item from the queue."""
        async with self._sqlite_conn.cursor() as cursor:
            # Retrieve the first item
            await cursor.execute(f"SELECT item FROM {self._table_plan_queue} ORDER BY id LIMIT 1")
            row = await cursor.fetchone()
            if not row:
                return None
            item = json.loads(row[0])

            # Remove the item from the queue
            await cursor.execute(f"DELETE FROM {self._table_plan_queue} WHERE id = (SELECT id FROM {self._table_plan_queue} ORDER BY id LIMIT 1)")
        await self._sqlite_conn.commit()
        return item
    
#### history management

    async def clear_history(self):
        """Clear the plan history."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"DELETE FROM {self._table_plan_history}")
        await self._sqlite_conn.commit()

    async def get_history(self):
        """Retrieve the full history."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT item FROM {self._table_plan_history}")
            rows = await cursor.fetchall()
        return [json.loads(row[0]) for row in rows]

    async def get_history_size(self):
        """Retrieve the size of the history."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_history}")
            row = await cursor.fetchone()
        return row[0]
    
#### Lock information management

    async def lock_info_save(self, lock_info):
        """Save lock information."""
        async with self._sqlite_conn.cursor() as cursor:
            lock_info_json = json.dumps(lock_info)
            await cursor.execute(f"DELETE FROM {self._table_lock_info}")
            await cursor.execute(f"INSERT INTO {self._table_lock_info} (info) VALUES (?)", (lock_info_json,))
        await self._sqlite_conn.commit()

    async def lock_info_retrieve(self):
        """Retrieve lock information."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT info FROM {self._table_lock_info} LIMIT 1")
            row = await cursor.fetchone()
        return json.loads(row[0]) if row else None

#### Autostart mode management

    async def autostart_mode_save(self, enabled):
        """Save the autostart mode."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"DELETE FROM {self._table_autostart_mode_info}")
            await cursor.execute(f"INSERT INTO {self._table_autostart_mode_info} (enabled) VALUES (?)", (enabled,))
        await self._sqlite_conn.commit()

    async def autostart_mode_retrieve(self):
        """Retrieve the autostart mode."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT enabled FROM {self._table_autostart_mode_info} LIMIT 1")
            row = await cursor.fetchone()
        return row[0] if row else None
    
#### Stop-pending state management

    async def stop_pending_save(self, stop_pending_info):
        """Save the stop-pending state information."""
        async with self._sqlite_conn.cursor() as cursor:
            stop_pending_info_json = json.dumps(stop_pending_info)
            await cursor.execute(f"DELETE FROM {self._table_stop_pending_info}")
            await cursor.execute(f"INSERT INTO {self._table_stop_pending_info} (info) VALUES (?)", (stop_pending_info_json,))
        await self._sqlite_conn.commit()

    async def stop_pending_retrieve(self):
        """Retrieve the stop-pending state information."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT info FROM {self._table_stop_pending_info} LIMIT 1")
            row = await cursor.fetchone()
        return json.loads(row[0]) if row else None