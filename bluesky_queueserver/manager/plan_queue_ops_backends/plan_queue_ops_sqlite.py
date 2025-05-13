import asyncio
import aiosqlite
import json
import logging
import os
import time as ttime
import copy
from typing import Any, Dict, List, Optional, Tuple, Union

from bluesky_queueserver.manager.plan_queue_ops_backends.plan_queue_ops_abstract import AbstractPlanQueueOperations


logger = logging.getLogger(__name__)

class SQLitePlanQueueOperations(AbstractPlanQueueOperations):
    """
    Backend implementation for plan queue operations using SQLite database.
    
    This class provides persistent storage of queue items, history, and other state
    information using a SQLite database. It implements all the methods required
    by AbstractPlanQueueOperations and utilizes UIDOperations for consistent handling
    of unique identifiers.
    
    Parameters
    ----------
    sqlite_db_path : str, optional
        Path to the SQLite database file. Default is determined by environment variable 
        or a default path in the current working directory.
    name_prefix : str, optional
        Prefix for table names to avoid conflicts with other instances. Default is "qs_default".
    """
    def __init__(
        self, 
        sqlite_db_path: Optional[str] = None, 
        name_prefix: str = "qs_default"
    ) -> None:
        """
        Initialize SQLitePlanQueueOperations.
        
        Parameters
        ----------
        sqlite_db_path : str, optional
            Path to the SQLite database file
        name_prefix : str, default "qs_default"
            Prefix used for table names
        """
        # Initialize the parent class first
        super().__init__()
                
        # Set database path
        if not sqlite_db_path:
            sqlite_db_path = os.getenv("BACKEND_DB_PATH", os.path.join(os.getcwd(), "bluesky_queue.sqlite"))
        
        self._sqlite_db_path = sqlite_db_path
        self._sqlite_conn = None
        self._name_prefix = name_prefix
        
        # Ensure consistent naming pattern with Redis implementation
        if name_prefix:
            name_prefix = name_prefix + "_"
        
        # Table names
        self._table_queue = name_prefix + "plan_queue"
        self._table_running = name_prefix + "running_plan"
        self._table_history = name_prefix + "plan_history"
        self._table_plan_queue_mode = name_prefix + "plan_queue_mode"
        self._table_user_group_permissions = name_prefix + "user_group_permissions"
        self._table_lock_info = name_prefix + "lock_info"
        self._table_stop_pending = name_prefix + "stop_pending_info"
        self._table_autostart_mode = name_prefix + "autostart_mode_info"
        
        # Match variable names with code expectations
        self._table_plan_queue = self._table_queue
        self._table_running_plan = self._table_running
        self._table_plan_history = self._table_history
        self._table_stop_pending_info = self._table_stop_pending
        self._table_autostart_mode_info = self._table_autostart_mode
        
        # Create lock for thread safety
        self._lock = asyncio.Lock()
        
    async def _initialize_tables(self) -> None:
        """
        Initialize database tables if they don't exist.
        """
        async with self._sqlite_conn.cursor() as cursor:
            # Create plan queue table
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table_plan_queue} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    position INTEGER,
                    item TEXT
                )
            """)
            
            # Create plan history table
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table_plan_history} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create running plan table
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table_running_plan} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item TEXT
                )
            """)
            
            # Create lock info table
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table_lock_info} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    info TEXT
                )
            """)
            
            # Create autostart mode info table
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table_autostart_mode_info} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    info TEXT
                )
            """)
            
            # Create stop pending info table
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table_stop_pending_info} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    info TEXT
                )
            """)
            
            # Create user group permissions table
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table_user_group_permissions} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    info TEXT
                )
            """)
            
            # Create plan queue mode table
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table_plan_queue_mode} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    info TEXT
                )
            """)
            
            # Create indexes for efficient lookup
            await cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_position ON {self._table_plan_queue} (position)")
            
        await self._sqlite_conn.commit()
        
    # --------------------------------------------------------------------------
    # Backend Management
    async def start(self) -> None:
        """
        Start the SQLite backend connection and initialize tables.
        """
        # Only establish connection if none exists
        if self._sqlite_conn is None:
            try:
                # Connect to SQLite database
                self._sqlite_conn = await aiosqlite.connect(self._sqlite_db_path)
                
                # Create tables
                await self._initialize_tables()
                
                # Initialize UID dictionary from existing data
                await self._uid_dict_initialize()
                
                # Initialize plan queue mode if not exists
                async with self._sqlite_conn.cursor() as cursor:
                    await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue_mode}")
                    count = await cursor.fetchone()
                    if count[0] == 0:
                        # Set default mode if none exists
                        await cursor.execute(
                            f"INSERT INTO {self._table_plan_queue_mode} (info) VALUES (?)",
                            (json.dumps(self._plan_queue_mode_default),)
                        )
                        await self._sqlite_conn.commit()
                    else:
                        # Load existing mode
                        await cursor.execute(f"SELECT info FROM {self._table_plan_queue_mode} LIMIT 1")
                        row = await cursor.fetchone()
                        if row:
                            self._plan_queue_mode = json.loads(row[0])
                            
            except Exception as ex:
                logger.error(f"Failed to connect to SQLite database: {ex}")
                if self._sqlite_conn:
                    await self._sqlite_conn.close()
                    self._sqlite_conn = None
                raise

    async def stop(self) -> None:
        """
        Stop the SQLite backend connection.
        """
        if self._sqlite_conn:
            await self._sqlite_conn.close()
            self._sqlite_conn = None

    async def reset(self) -> None:
        """
        Reset the backend to its initial state by clearing all data.
        """
        async with self._lock:
            if self._sqlite_conn:
                async with self._sqlite_conn.cursor() as cursor:
                    await cursor.execute(f"DELETE FROM {self._table_plan_queue}")
                    await cursor.execute(f"DELETE FROM {self._table_plan_history}")
                    await cursor.execute(f"DELETE FROM {self._table_running_plan}")
                    await cursor.execute(f"DELETE FROM {self._table_lock_info}")
                    await cursor.execute(f"DELETE FROM {self._table_autostart_mode_info}")
                    await cursor.execute(f"DELETE FROM {self._table_stop_pending_info}")
                    await cursor.execute(f"DELETE FROM {self._table_user_group_permissions}")
                    
                    # Reset plan queue mode to default
                    await cursor.execute(f"DELETE FROM {self._table_plan_queue_mode}")
                    default_mode = {"loop": False, "ignore_failures": False}
                    await cursor.execute(
                        f"INSERT INTO {self._table_plan_queue_mode} (info) VALUES (?)",
                        (json.dumps(default_mode),)
                    )
                    
                await self._sqlite_conn.commit()
                self._uid_dict_clear()

    async def delete_pool_entries(self) -> None:
        """
        Delete all pool entries used by this backend.
        """
        await self.reset()

    # --------------------------------------------------------------------------
    # UID Operations
    async def _uid_dict_initialize(self) -> None:
        """
        Initialize the UID dictionary from the current state in SQLite.
        """
        self._uid_dict_clear()
        
        # Add all items from plan queue
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT item FROM {self._table_plan_queue} ORDER BY position")
            rows = await cursor.fetchall()
            for row in rows:
                item = json.loads(row[0])
                if "item_uid" in item:
                    self._uid_dict_add(item)
        
        # Add running item if it exists
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT item FROM {self._table_running_plan} LIMIT 1")
            row = await cursor.fetchone()
            if row:
                item = json.loads(row[0])
                if "item_uid" in item:
                    self._uid_dict_add(item)

    # --------------------------------------------------------------------------
    # Queue Operations
    async def add_item_to_queue(
        self, 
        item: Dict[str, Any], 
        *, 
        pos: Optional[Union[int, str]] = None,
        before_uid: Optional[str] = None,
        after_uid: Optional[str] = None,
        filter_parameters: bool = True
    ) -> Tuple[Dict[str, Any], int]:
        """
        Add an item to the queue.
        
        Parameters
        ----------
        item : dict
            Item to add to the queue
        pos : int or str, optional
            Position in the queue. Can be an integer (0-based index) or string ('front' or 'back')
        before_uid : str, optional
            UID of the item before which to place the new item
        after_uid : str, optional
            UID of the item after which to place the new item
        filter_parameters : bool, default True
            Whether to filter item parameters

        Returns
        -------
        tuple
            (item, queue_size) - the added item and the new queue size
        """
        async with self._lock:
            # FIXED: Add validation for required fields
            if "name" not in item or not item["name"]:
                raise ValueError("Item must have a valid name")

            if (pos is not None) and (before_uid is not None or after_uid is not None):
                raise ValueError("Ambiguous parameters: plan position and UID is specified.")

            if (before_uid is not None) and (after_uid is not None):
                raise ValueError(
                    "Ambiguous parameters: request to insert the plan before and after the reference plan."
                )

            pos = pos if pos is not None else "back"

            if "item_uid" not in item:
                item = await self.set_new_item_uuid(item)
            else:
                await self._verify_item(item)

            if filter_parameters:
                item = self.filter_item_parameters(item)

            # Get current queue size
            async with self._sqlite_conn.cursor() as cursor:
                await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
                qsize = (await cursor.fetchone())[0]
                
                # Determine the position
                position = None
                if isinstance(pos, int):
                    if pos <= 0:
                        position = 0
                    elif pos >= qsize:
                        position = qsize
                    else:
                        position = pos
                elif pos == "front":
                    position = 0
                elif pos == "back":
                    position = qsize
                elif before_uid or after_uid:
                    # Find the index of the item with the specified UID
                    ref_uid = before_uid if before_uid else after_uid
                    await cursor.execute(
                        f"SELECT position FROM {self._table_plan_queue} WHERE json_extract(item, '$.item_uid') = ?",
                        (ref_uid,)
                    )
                    row = await cursor.fetchone()
                    if not row:
                        raise IndexError(f"Item with UID '{ref_uid}' not found in the queue.")
                    
                    position = row[0] if before_uid else row[0] + 1
                else:
                    raise ValueError(f"Invalid value for 'pos': {pos}")
                
                # Shift positions for items that come after the insertion point
                await cursor.execute(
                    f"UPDATE {self._table_plan_queue} SET position = position + 1 WHERE position >= ?",
                    (position,)
                )
                
                # Insert the new item
                item_json = json.dumps(item)
                await cursor.execute(
                    f"INSERT INTO {self._table_plan_queue} (position, item) VALUES (?, ?)",
                    (position, item_json)
                )
                
                # Add item to UID dictionary
                self._uid_dict_add(item)
                
                # Get the new queue size
                await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
                new_qsize = (await cursor.fetchone())[0]

            await self._sqlite_conn.commit()
            return item, new_qsize

    async def add_batch_to_queue(
        self, 
        items: List[Dict[str, Any]], 
        *, 
        pos: Optional[Union[int, str]] = None,
        before_uid: Optional[str] = None,
        after_uid: Optional[str] = None,
        filter_parameters: bool = True
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int, bool]:
        """
        Add a batch of items to the queue.
        
        Parameters
        ----------
        items : list
            List of items to add to the queue
        pos : int or str, optional
            Position in the queue. Can be an integer (0-based index) or string ('front' or 'back')
        before_uid : str, optional
            UID of the item before which to place the new items
        after_uid : str, optional
            UID of the item after which to place the new items
        filter_parameters : bool, default True
            Whether to filter item parameters

        Returns
        -------
        tuple
            (items_added, results, queue_size, success) - added items, results of each operation,
            new queue size, and overall success flag
        """
        async with self._lock:
            items_added = []
            results = []
            success = True
            added_item_uids = []  # For rollback in case of failure
            
            try:
                # Begin transaction
                await self._sqlite_conn.execute("BEGIN TRANSACTION")
                
                for item in items:
                    try:
                        if not added_item_uids:
                            # First item is placed according to parameters
                            item_added, _ = await self.add_item_to_queue(
                                item,
                                pos=pos,
                                before_uid=before_uid,
                                after_uid=after_uid,
                                filter_parameters=filter_parameters,
                            )
                        else:
                            # Subsequent items are placed after the previous one
                            item_added, _ = await self.add_item_to_queue(
                                item, 
                                after_uid=added_item_uids[-1], 
                                filter_parameters=filter_parameters
                            )
                        
                        added_item_uids.append(item_added["item_uid"])
                        items_added.append(item_added)
                        results.append({"success": True, "msg": ""})
                    except Exception as ex:
                        success = False
                        items_added.append(item)
                        msg = f"Failed to add item to queue: {ex}"
                        results.append({"success": False, "msg": msg})
                        # No raise here - this was causing the test to hang
                        break  # Stop processing more items if one fails
                
                # Handle transaction based on overall success
                if success:
                    await self._sqlite_conn.commit()
                else:
                    await self._sqlite_conn.rollback()
                    items_added = items  # Reset to original items for consistent return value
                
                # Get the new queue size - always needs to be awaited
                qsize = 0
                async with self._sqlite_conn.cursor() as cursor:
                    await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
                    row = await cursor.fetchone()
                    if row:
                        qsize = row[0]
                
                return items_added, results, qsize, success
                
            except Exception:
                # Ensure transaction is rolled back in case of error
                await self._sqlite_conn.rollback()
                queue_size = await self.get_queue_size()  # Ensure we await this
                return items, results, queue_size, False

    async def pop_item_from_queue(
        self, 
        *, 
        pos: Optional[Union[int, str]] = None,
        uid: Optional[str] = None
    ) -> Tuple[Optional[Dict[str, Any]], int]:
        """
        Remove and return an item from the queue.
        
        Parameters
        ----------
        pos : int or str, optional
            Position in the queue. Can be an integer (0-based index) or string ('front' or 'back')
        uid : str, optional
            UID of the item to remove

        Returns
        -------
        tuple
            (item, queue_size) - the removed item and the new queue size
        """
        async with self._lock:
            if (pos is not None) and (uid is not None):
                raise ValueError("Ambiguous parameters: plan position and UID is specified")

            pos = pos if pos is not None else "front"
            
            async with self._sqlite_conn.cursor() as cursor:
                item = None
                
                if uid is not None:
                    # Check if item with this UID exists
                    if not self._is_uid_in_dict(uid):
                        raise IndexError(f"Item with UID '{uid}' is not in the queue.")
                    
                    # Check if the item is not currently running
                    await cursor.execute(
                        f"SELECT item FROM {self._table_running_plan} WHERE json_extract(item, '$.item_uid') = ?",
                        (uid,)
                    )
                    row = await cursor.fetchone()
                    if row:
                        raise IndexError(f"Cannot remove an item which is currently running.")
                    
                    # Get the item and its position
                    await cursor.execute(
                        f"SELECT position, item FROM {self._table_plan_queue} WHERE json_extract(item, '$.item_uid') = ?",
                        (uid,)
                    )
                    row = await cursor.fetchone()
                    if not row:
                        raise IndexError(f"Item with UID '{uid}' not found in the queue.")
                    
                    position = row[0]
                    item = json.loads(row[1])
                    
                    # Delete the item
                    await cursor.execute(
                        f"DELETE FROM {self._table_plan_queue} WHERE json_extract(item, '$.item_uid') = ?",
                        (uid,)
                    )
                    
                    # Remove from UID dictionary
                    self._uid_dict_remove(uid)
                    
                elif pos == "front":
                    await cursor.execute(
                        f"SELECT item FROM {self._table_plan_queue} ORDER BY position LIMIT 1"
                    )
                    row = await cursor.fetchone()
                    if not row:
                        return None, 0
                    
                    item = json.loads(row[0])
                    
                    # Delete the item
                    await cursor.execute(
                        f"DELETE FROM {self._table_plan_queue} WHERE position = 0"
                    )
                    
                    # Remove from UID dictionary
                    self._uid_dict_remove(item["item_uid"])
                    
                elif pos == "back":
                    # Find the position of the last item
                    await cursor.execute(
                        f"SELECT MAX(position) FROM {self._table_plan_queue}"
                    )
                    row = await cursor.fetchone()
                    if not row or row[0] is None:
                        return None, 0
                    
                    max_pos = row[0]
                    
                    await cursor.execute(
                        f"SELECT item FROM {self._table_plan_queue} WHERE position = ?",
                        (max_pos,)
                    )
                    row = await cursor.fetchone()
                    if not row:
                        return None, 0
                    
                    item = json.loads(row[0])
                    
                    # Delete the item
                    await cursor.execute(
                        f"DELETE FROM {self._table_plan_queue} WHERE position = ?",
                        (max_pos,)
                    )
                    
                    # Remove from UID dictionary
                    self._uid_dict_remove(item["item_uid"])
                    
                elif isinstance(pos, int):
                    # Adjust negative indices
                    if pos < 0:
                        await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
                        qsize = (await cursor.fetchone())[0]
                        pos = qsize + pos
                        
                    if pos < 0:
                        raise IndexError(f"Position {pos} out of range")
                    
                    await cursor.execute(
                        f"SELECT item FROM {self._table_plan_queue} WHERE position = ?",
                        (pos,)
                    )
                    row = await cursor.fetchone()
                    if not row:
                        raise IndexError(f"Position {pos} out of range")
                    
                    item = json.loads(row[0])
                    
                    # Delete the item
                    await cursor.execute(
                        f"DELETE FROM {self._table_plan_queue} WHERE position = ?",
                        (pos,)
                    )
                    
                    # Remove from UID dictionary
                    self._uid_dict_remove(item["item_uid"])
                else:
                    raise ValueError(f"Invalid value for 'pos': {pos}")
                
                # Reposition remaining items to eliminate gaps
                await cursor.execute(f"""
                    UPDATE {self._table_plan_queue} 
                    SET position = (
                        SELECT COUNT(*) - 1 
                        FROM {self._table_plan_queue} AS t2 
                        WHERE t2.position <= {self._table_plan_queue}.position 
                        AND t2.id != {self._table_plan_queue}.id
                    )
                """)
                
                # Get the new queue size
                await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
                qsize = (await cursor.fetchone())[0]
            
            await self._sqlite_conn.commit()
            return item, qsize

    async def pop_item_from_queue_batch(
        self, 
        *, 
        uids: Optional[List[str]] = None,
        ignore_missing: bool = True
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Remove and return multiple items from the queue.
        
        Parameters
        ----------
        uids : list, optional
            List of UIDs of the items to remove
        ignore_missing : bool, default True
            Whether to ignore missing items

        Returns
        -------
        tuple
            (items, queue_size) - the removed items and the new queue size
        """
        async with self._lock:
            uids = uids or []
            
            if not isinstance(uids, list):
                raise TypeError(f"Parameter 'uids' must be a list: type(uids) = {type(uids)}")
                
            if not ignore_missing:
                # Check if 'uids' contains only unique items
                uids_set = set(uids)
                if len(uids_set) != len(uids):
                    raise ValueError(f"The list contains repeated UIDs ({len(uids) - len(uids_set)} UIDs)")
                    
                # Check if all UIDs in 'uids' exist in the queue
                uids_missing = []
                for uid in uids:
                    if not self._is_uid_in_dict(uid):
                        uids_missing.append(uid)
                if uids_missing:
                    raise ValueError(f"The queue does not contain items with the following UIDs: {uids_missing}")
            
            # Begin transaction
            await self._sqlite_conn.execute("BEGIN TRANSACTION")
            
            items = []
            try:
                for uid in uids:
                    try:
                        item, _ = await self.pop_item_from_queue(uid=uid)
                        if item:  # Add null check
                            items.append(item)
                    except Exception as ex:
                        if not ignore_missing:
                            await self._sqlite_conn.rollback()  # Ensure rollback on exception
                            raise
                        logger.debug(f"Failed to remove item with UID '{uid}' from the queue: {ex}")
                
                await self._sqlite_conn.commit()
                
                # Get the new queue size
                qsize = 0  # Default value in case of empty result
                async with self._sqlite_conn.cursor() as cursor:
                    await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
                    row = await cursor.fetchone()
                    if row:
                        qsize = row[0]
                
                return items, qsize
            except Exception as ex:
                # Make sure the rollback is awaited
                await self._sqlite_conn.rollback()
                logger.error(f"Error in pop_item_from_queue_batch: {ex}")
                raise

    async def clear_queue(self) -> None:
        """Clear the plan queue."""
        async with self._lock:
            async with self._sqlite_conn.cursor() as cursor:
                # Get the running item's UID if it exists
                await cursor.execute(f"SELECT item FROM {self._table_running_plan} LIMIT 1")
                row = await cursor.fetchone()
                running_uid = None
                if row:
                    running_item = json.loads(row[0])
                    running_uid = running_item.get('item_uid')
                
                # FIXED: Complete reset of UID dictionary
                self._uid_dict_clear()  
                
                # Re-add just the running item if it exists
                if running_uid and row:
                    running_item = json.loads(row[0])
                    self._uid_dict_add(running_item)
                
                # Clear the queue table
                await cursor.execute(f"DELETE FROM {self._table_plan_queue}")
                
            await self._sqlite_conn.commit()

    async def get_queue_size(self) -> int:
        """
        Get the size of the queue.
        
        Returns
        -------
        int
            The number of items in the queue
        """
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
            return (await cursor.fetchone())[0]

    async def get_queue_full(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any], str]:
        """
        Retrieve the full queue.
        
        Returns
        -------
        tuple
            (queue_items, running_item, queue_uid) - all items in the queue, the currently running item,
            and a unique identifier for this queue state
        """
        async with self._lock:
            # Get all items from queue ordered by position
            async with self._sqlite_conn.cursor() as cursor:
                await cursor.execute(f"SELECT item FROM {self._table_plan_queue} ORDER BY position")
                rows = await cursor.fetchall()
                queue = [json.loads(row[0]) for row in rows]
                
                # Get running item if exists
                await cursor.execute(f"SELECT item FROM {self._table_running_plan} LIMIT 1")
                row = await cursor.fetchone()
                running_item = json.loads(row[0]) if row else {}
                
                # Generate a unique identifier for this queue state
                queue_uid = self.new_item_uid()
                
                return queue, running_item, queue_uid

    async def move_item(
        self, 
        *, 
        pos: Optional[Union[int, str]] = None,
        uid: Optional[str] = None,
        pos_dest: Optional[Union[int, str]] = None,
        before_uid: Optional[str] = None,
        after_uid: Optional[str] = None
    ) -> Tuple[Dict[str, Any], int]:
        """
        Move an item to a new position in the queue.
        
        Parameters
        ----------
        pos : int or str, optional
            Source position in the queue
        uid : str, optional
            UID of the item to move
        pos_dest : int or str, optional
            Destination position
        before_uid : str, optional
            UID of the item before which to place the moved item
        after_uid : str, optional
            UID of the item after which to place the moved item

        Returns
        -------
        tuple
            (item, queue_size) - the moved item and the new queue size
        """
        async with self._lock:
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
                
            # Begin transaction
            await self._sqlite_conn.execute("BEGIN TRANSACTION")
            
            try:
                async with self._sqlite_conn.cursor() as cursor:
                    # Find the source item and its position
                    source_pos = None
                    item = None
                    
                    if uid is not None:
                        await cursor.execute(
                            f"SELECT position, item FROM {self._table_plan_queue} WHERE json_extract(item, '$.item_uid') = ?",
                            (uid,)
                        )
                        row = await cursor.fetchone()
                        if not row:
                            raise IndexError(f"Item with UID '{uid}' not found in the queue.")
                        source_pos = row[0]
                        item = json.loads(row[1])
                    else:
                        await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
                        qsize = (await cursor.fetchone())[0]
                        if pos == "front":
                            source_pos = 0
                        elif pos == "back":
                            source_pos = qsize - 1
                        elif isinstance(pos, int):
                            if pos < 0:
                                source_pos = qsize + pos
                            else:
                                source_pos = pos
                                
                            if source_pos < 0 or source_pos >= qsize:
                                raise IndexError(f"Position {pos} is out of range.")
                        else:
                            raise ValueError(f"Invalid value for 'pos': {pos}")
                        
                        await cursor.execute(
                            f"SELECT item FROM {self._table_plan_queue} WHERE position = ?",
                            (source_pos,)
                        )
                        row = await cursor.fetchone()
                        if not row:
                            raise IndexError(f"No item found at position {source_pos}.")
                        item = json.loads(row[0])
                    
                    # Find the destination position
                    dest_pos = None
                    await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
                    qsize = (await cursor.fetchone())[0]
                    
                    if before_uid is not None or after_uid is not None:
                        dest_uid = before_uid if before_uid is not None else after_uid
                        before = dest_uid == before_uid
                        
                        await cursor.execute(
                            f"SELECT position FROM {self._table_plan_queue} WHERE json_extract(item, '$.item_uid') = ?",
                            (dest_uid,)
                        )
                        row = await cursor.fetchone()
                        if not row:
                            raise IndexError(f"Item with UID '{dest_uid}' not found in the queue.")
                        
                        dest_pos = row[0]
                        if not before:  # If after_uid, increment the dest_pos
                            dest_pos += 1
                    else:
                        if pos_dest == "front":
                            dest_pos = 0
                        elif pos_dest == "back":
                            dest_pos = qsize
                        elif isinstance(pos_dest, int):
                            if pos_dest < 0:
                                dest_pos = qsize + pos_dest
                            else:
                                dest_pos = pos_dest
                                
                            if dest_pos < 0 or dest_pos > qsize:
                                raise IndexError(f"Position {pos_dest} is out of range.")
                        else:
                            raise ValueError(f"Invalid value for 'pos_dest': {pos_dest}")
                    
                    # If source_pos is the same as dest_pos, no need to move
                    if source_pos == dest_pos:
                        await self._sqlite_conn.rollback()  # No changes needed
                        return item, qsize
                        
                    # If source is before destination, adjust dest_pos since array length will change
                    if source_pos < dest_pos:
                        dest_pos -= 1
                    
                    # Remove item from source position
                    await cursor.execute(
                        f"DELETE FROM {self._table_plan_queue} WHERE position = ?",
                        (source_pos,)
                    )
                    
                    # Reposition remaining items to eliminate gaps
                    await cursor.execute(f"""
                        UPDATE {self._table_plan_queue} 
                        SET position = (
                            SELECT COUNT(*) - 1 
                            FROM {self._table_plan_queue} AS t2 
                            WHERE t2.position <= {self._table_plan_queue}.position 
                            AND t2.id != {self._table_plan_queue}.id
                        )
                    """)
                    
                    # Make space at destination position
                    await cursor.execute(
                        f"UPDATE {self._table_plan_queue} SET position = position + 1 WHERE position >= ?",
                        (dest_pos,)
                    )
                    
                    # Insert item at destination position
                    item_json = json.dumps(item)
                    await cursor.execute(
                        f"INSERT INTO {self._table_plan_queue} (position, item) VALUES (?, ?)",
                        (dest_pos, item_json)
                    )
                    
                    # Get the new queue size
                    await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
                    qsize = (await cursor.fetchone())[0]
                
                await self._sqlite_conn.commit()
                return item, qsize
            except Exception:
                await self._sqlite_conn.rollback()
                raise

    async def move_batch(
        self, 
        *, 
        uids: Optional[List[str]] = None,
        pos_dest: Optional[Union[int, str]] = None,
        before_uid: Optional[str] = None,
        after_uid: Optional[str] = None,
        reorder: bool = False
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Move a batch of items within the queue.
        
        Parameters
        ----------
        uids : list, optional
            List of UIDs of the items to move
        pos_dest : int or str, optional
            Destination position for the first item
        before_uid : str, optional
            UID of the item before which to place the first moved item
        after_uid : str, optional
            UID of the item after which to place the first moved item
        reorder : bool, default False
            Whether to reorder items according to their current order in the queue

        Returns
        -------
        tuple
            (items, queue_size) - the moved items and the new queue size
        """
        async with self._lock:
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
                raise ValueError(f"The list contains repeated UIDs ({len(uids) - len(uids_set)} UIDs)")
                
            # Begin transaction
            await self._sqlite_conn.execute("BEGIN TRANSACTION")
            
            try:
                async with self._sqlite_conn.cursor() as cursor:
                    # Check if all UIDs in 'uids' exist in the queue
                    for uid in uids:
                        await cursor.execute(
                            f"SELECT COUNT(*) FROM {self._table_plan_queue} WHERE json_extract(item, '$.item_uid') = ?",
                            (uid,)
                        )
                        count = (await cursor.fetchone())[0]
                        if count == 0:
                            raise ValueError(f"The queue does not contain an item with UID: {uid}")
                    
                    # Check that 'before_uid' and 'after_uid' are not in 'uids'
                    if (before_uid is not None) and (before_uid in uids):
                        raise ValueError(f"Parameter 'before_uid': item with UID '{before_uid}' is in the batch")
                    if (after_uid is not None) and (after_uid in uids):
                        raise ValueError(f"Parameter 'after_uid': item with UID '{after_uid}' is in the batch")
                    
                    # If reorder is True, arrange UIDs based on their current positions in the queue
                    if reorder:
                        uids_with_positions = []
                        for uid in uids:
                            await cursor.execute(
                                f"SELECT position FROM {self._table_plan_queue} WHERE json_extract(item, '$.item_uid') = ?",
                                (uid,)
                            )
                            position = (await cursor.fetchone())[0]
                            uids_with_positions.append((position, uid))
                        
                        uids_with_positions.sort(key=lambda x: x[0])
                        uids_prepared = [pair[1] for pair in uids_with_positions]
                    else:
                        uids_prepared = uids
                
                # Perform the 'move' operation
                last_item_uid = None
                items_moved = []
                
                for uid in uids_prepared:
                    try:
                        if last_item_uid is None:
                            # First item is moved according to specified parameters
                            item, _ = await self.move_item(
                                uid=uid, pos_dest=pos_dest, before_uid=before_uid, after_uid=after_uid
                            )
                        else:
                            # Consecutive items are placed after the previous item
                            item, _ = await self.move_item(uid=uid, after_uid=last_item_uid)
                        
                        last_item_uid = uid
                        items_moved.append(item)
                    except Exception as ex:
                        await self._sqlite_conn.rollback()  # Ensure rollback is awaited
                        raise RuntimeError(f"Error moving item {uid}: {str(ex)}") from ex
                
                # Get the new queue size
                qsize = 0
                async with self._sqlite_conn.cursor() as cursor:
                    await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
                    row = await cursor.fetchone()
                    if row:
                        qsize = row[0]
                
                await self._sqlite_conn.commit()
                return items_moved, qsize
                
            except Exception as ex:
                # Ensure transaction is rolled back in case of error
                await self._sqlite_conn.rollback()
                raise

    async def replace_item(
        self, 
        item: Dict[str, Any], 
        *, 
        item_uid: str
    ) -> Tuple[Dict[str, Any], int]:
        """
        Replace an existing item in the queue with a new item.
        
        Parameters
        ----------
        item : dict
            The new item to replace the existing one
        item_uid : str
            UID of the item to replace

        Returns
        -------
        tuple
            (old_item, queue_size) - the replaced item and the new queue size
        """
        async with self._lock:
            # Verify the new item, ignoring the UID of the item being replaced
            await self._verify_item(item, ignore_uids=[item_uid])
            
            async with self._sqlite_conn.cursor() as cursor:
                # Find the item to replace
                await cursor.execute(
                    f"SELECT position, item FROM {self._table_plan_queue} WHERE json_extract(item, '$.item_uid') = ?",
                    (item_uid,)
                )
                row = await cursor.fetchone()
                if not row:
                    raise ValueError(f"Item with UID '{item_uid}' not found in the queue.")
                
                old_item = json.loads(row[1])
                position = row[0]
                
                # Replace the item
                item_json = json.dumps(item)
                await cursor.execute(
                    f"UPDATE {self._table_plan_queue} SET item = ? WHERE position = ?",
                    (item_json, position)
                )
                
                # Update UID dictionary
                self._uid_dict_remove(item_uid)
                self._uid_dict_add(item)
                
                # Get the new queue size
                await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
                qsize = (await cursor.fetchone())[0]
            
            await self._sqlite_conn.commit()
            return old_item, qsize

    # --------------------------------------------------------------------------
    # History Management
    async def clear_history(self) -> None:
        """Clear the plan history."""
        async with self._lock:
            async with self._sqlite_conn.cursor() as cursor:
                await cursor.execute(f"DELETE FROM {self._table_plan_history}")
            await self._sqlite_conn.commit()

    async def get_history(self) -> Tuple[List[Dict[str, Any]], str]:
        """
        Retrieve the full history.
        
        Returns
        -------
        tuple
            (history_items, history_uid) - all items in the history and a unique identifier for this history state
        """
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(
                f"SELECT item FROM {self._table_plan_history} ORDER BY id DESC"
            )
            rows = await cursor.fetchall()
            history = [json.loads(row[0]) for row in rows]
            history_uid = self.new_item_uid()  # Generate a unique ID for this history state
            return history, history_uid

    async def get_history_size(self) -> int:
        """
        Get the size of the history.
        
        Returns
        -------
        int
            The number of items in the history
        """
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_history}")
            return (await cursor.fetchone())[0]

    # --------------------------------------------------------------------------
    # Running Item Management
    async def get_running_item_info(self) -> Dict[str, Any]:
        """
        Get information about the currently running item.
        
        Returns
        -------
        dict
            Information about the currently running item, or an empty dict if no item is running
        """
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT item FROM {self._table_running_plan} LIMIT 1")
            row = await cursor.fetchone()
            return json.loads(row[0]) if row else {}

    async def _clean_item_properties(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and set default item properties."""
        item = copy.deepcopy(item)
        
        # Add default values for required fields if missing
        if "args" not in item:
            item["args"] = []
        if "kwargs" not in item:
            item["kwargs"] = {}
        if "item_type" not in item:
            item["item_type"] = "plan"
        if "user" not in item:
            item["user"] = None
        if "user_group" not in item:
            item["user_group"] = None
        
        # Clean existing properties if present
        if "properties" in item:
            p = item["properties"]
            # ONLY remove these specific properties
            if "immediate_execution" in p:
                del p["immediate_execution"]
            if "time_start" in p:
                del p["time_start"]
            if not p:
                del item["properties"]
                
        return item

    async def set_next_item_as_running(self, *, item: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Set the next item from the queue as running.
        
        Parameters
        ----------
        item : dict, optional
            Item for immediate execution

        Returns
        -------
        dict
            The item that is now running
        """
        async with self._lock:
            async with self._sqlite_conn.cursor() as cursor:
                # Check if an item is already running
                await cursor.execute(f"SELECT COUNT(*) FROM {self._table_running_plan}")
                count = (await cursor.fetchone())[0]
                if count > 0:
                    raise RuntimeError("An item is already running")
                
                immediate_execution = bool(item)
                plan = None
                
                if immediate_execution:
                    # Generate UID if it doesn't exist
                    plan = copy.deepcopy(item)
                    if "item_uid" not in plan:
                        plan = await self.set_new_item_uuid(plan)
                    
                    plan.setdefault("properties", {})["immediate_execution"] = True
                else:
                    # Get the first item from the queue
                    await cursor.execute(
                        f"SELECT item FROM {self._table_plan_queue} ORDER BY position LIMIT 1"
                    )
                    row = await cursor.fetchone()
                    if not row:
                        return {}  # Queue is empty
                    
                    plan = json.loads(row[0])
                    
                    # Remove the plan from the queue
                    await cursor.execute(
                        f"DELETE FROM {self._table_plan_queue} WHERE position = 0"
                    )
                    
                    # Reposition remaining items
                    await cursor.execute(f"""
                        UPDATE {self._table_plan_queue} 
                        SET position = (
                            SELECT COUNT(*) - 1 
                            FROM {self._table_plan_queue} AS t2 
                            WHERE t2.position <= {self._table_plan_queue}.position 
                            AND t2.id != {self._table_plan_queue}.id
                        )
                    """)
                
                # Verify that the item is a plan
                if "item_type" not in plan:
                    raise ValueError("Item does not have 'item_type'")
                
                if plan["item_type"] != "plan":
                    raise RuntimeError(
                        "Function 'set_next_item_as_running' was called for "
                        f"an item other than plan: {plan}"
                    )
                
                # Record start time
                plan.setdefault("properties", {})["time_start"] = ttime.time()
                
                # Set as running plan
                plan_json = json.dumps(plan)
                await cursor.execute(
                    f"INSERT INTO {self._table_running_plan} (item) VALUES (?)",
                    (plan_json,)
                )
                
                # Add to UID dictionary
                self._uid_dict_add(plan)
            
            await self._sqlite_conn.commit()
            return plan

    async def set_processed_item_as_completed(
        self, 
        *, 
        exit_status: str,
        run_uids: List[str],
        scan_ids: List[int],
        err_msg: str = "",
        err_tb: str = ""
    ) -> Dict[str, Any]:
        """
        Mark the currently running item as completed and move it to history.
        
        Parameters
        ----------
        exit_status : str
            Exit status of the item
        run_uids : list
            List of run UIDs
        scan_ids : list
            List of scan IDs
        err_msg : str, default ""
            Error message if any
        err_tb : str, default ""
            Error traceback if any

        Returns
        -------
        dict
            The completed item
        """
        async with self._lock:
            # Check if loop mode is enabled in queue mode
            async with self._sqlite_conn.cursor() as cursor:
                await cursor.execute(f"SELECT item FROM {self._table_running_plan} LIMIT 1")
                row = await cursor.fetchone()
                if not row:
                    return {}
                
                running_item = json.loads(row[0])
                immediate_execution = running_item.get("properties", {}).get("immediate_execution", False)
                item_time_start = running_item.get("properties", {}).get("time_start", ttime.time())
                
                # Check if loop mode is enabled
                loop_enabled = False
                await cursor.execute(
                    f"SELECT info FROM {self._table_plan_queue_mode} LIMIT 1"
                )
                row = await cursor.fetchone()
                if row:
                    mode = json.loads(row[0])
                    loop_enabled = mode.get("loop", False)
                
                # Clean item properties
                item_cleaned = await self._clean_item_properties(running_item)
                
                if loop_enabled and not immediate_execution:
                    # Add a copy of the item back to the queue
                    item_to_add = copy.deepcopy(item_cleaned)
                    item_to_add = await self.set_new_item_uuid(item_to_add)
                    
                    # Get the current size of the queue
                    await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
                    qsize = (await cursor.fetchone())[0]
                    
                    # Add to the end of the queue
                    item_json = json.dumps(item_to_add)
                    await cursor.execute(
                        f"INSERT INTO {self._table_plan_queue} (position, item) VALUES (?, ?)",
                        (qsize, item_json)
                    )
                    
                    # Add to UID dictionary
                    self._uid_dict_add(item_to_add)
                
                # Add result information to the item
                item_cleaned.setdefault("result", {})
                item_cleaned["result"]["exit_status"] = exit_status
                item_cleaned["result"]["run_uids"] = run_uids
                item_cleaned["result"]["scan_ids"] = scan_ids
                item_cleaned["result"]["time_start"] = item_time_start
                item_cleaned["result"]["time_stop"] = ttime.time()
                item_cleaned["result"]["msg"] = err_msg
                item_cleaned["result"]["traceback"] = err_tb
                
                # Add to history
                item_json = json.dumps(item_cleaned)
                await cursor.execute(
                    f"INSERT INTO {self._table_plan_history} (item) VALUES (?)",
                    (item_json,)
                )
                
                # Remove from running
                await cursor.execute(f"DELETE FROM {self._table_running_plan}")
                
                # Remove from UID dictionary if not in loop mode
                if not loop_enabled and not immediate_execution:
                    self._uid_dict_remove(running_item["item_uid"])
            
            await self._sqlite_conn.commit()
            return item_cleaned

    async def set_processed_item_as_stopped(
        self, 
        *, 
        exit_status: str,
        run_uids: Optional[List[str]] = None,
        scan_ids: Optional[List[int]] = None,
        err_msg: str = "",
        err_tb: str = ""
    ) -> Dict[str, Any]:
        """
        Mark the currently running item as stopped and move it to history.
        If exit_status is not "stopped", also push the item back to the queue.
        
        Parameters
        ----------
        exit_status : str
            Exit status of the item
        run_uids : list, optional
            List of run UIDs
        scan_ids : list, optional
            List of scan IDs
        err_msg : str, default ""
            Error message if any
        err_tb : str, default ""
            Error traceback if any

        Returns
        -------
        dict
            The stopped item
        """
        async with self._lock:
            run_uids = run_uids or []
            scan_ids = scan_ids or []
            
            # If the status is "stopped", treat it as a completed item
            if exit_status == "stopped":
                return await self.set_processed_item_as_completed(
                    exit_status=exit_status,
                    run_uids=run_uids,
                    scan_ids=scan_ids,
                    err_msg=err_msg,
                    err_tb=err_tb
                )
            
            async with self._sqlite_conn.cursor() as cursor:
                await cursor.execute(f"SELECT item FROM {self._table_running_plan} LIMIT 1")
                row = await cursor.fetchone()
                if not row:
                    return {}
                
                running_item = json.loads(row[0])
                immediate_execution = running_item.get("properties", {}).get("immediate_execution", False)
                item_time_start = running_item.get("properties", {}).get("time_start", ttime.time())
                
                # Clean item properties
                item_cleaned = await self._clean_item_properties(running_item)
                
                # Add result information to the item
                item_cleaned.setdefault("result", {})
                item_cleaned["result"]["exit_status"] = exit_status
                item_cleaned["result"]["run_uids"] = run_uids
                item_cleaned["result"]["scan_ids"] = scan_ids
                item_cleaned["result"]["time_start"] = item_time_start
                item_cleaned["result"]["time_stop"] = ttime.time()
                item_cleaned["result"]["msg"] = err_msg
                item_cleaned["result"]["traceback"] = err_tb
                
                # Add to history
                item_json = json.dumps(item_cleaned)
                await cursor.execute(
                    f"INSERT INTO {self._table_plan_history} (item) VALUES (?)",
                    (item_json,)
                )
                
                # Push back to the queue with new UID if not immediate execution and status != stopped
                if not immediate_execution and exit_status != "stopped":
                    # Create a copy without result information
                    item_copy = copy.deepcopy(item_cleaned)
                    if "result" in item_copy:
                        del item_copy["result"]
                    
                    # Set a new UID
                    item_copy = await self.set_new_item_uuid(item_copy)
                    
                    # Insert at the front of the queue
                    await cursor.execute(
                        f"UPDATE {self._table_plan_queue} SET position = position + 1"
                    )
                    
                    item_json = json.dumps(item_copy)
                    await cursor.execute(
                        f"INSERT INTO {self._table_plan_queue} (position, item) VALUES (0, ?)",
                        (item_json,)
                    )
                    
                    # Add to UID dictionary
                    self._uid_dict_add(item_copy)
                
                # Remove from running
                await cursor.execute(f"DELETE FROM {self._table_running_plan}")
                
                # Remove original item from UID dictionary
                self._uid_dict_remove(running_item["item_uid"])
            
            await self._sqlite_conn.commit()
            return item_cleaned

    # --------------------------------------------------------------------------
    # Lock Management
    async def lock_info_save(self, lock_info: Dict[str, Any]) -> None:
        """
        Save lock information.
        
        Parameters
        ----------
        lock_info : dict
            Lock information to save
        """
        async with self._lock:
            async with self._sqlite_conn.cursor() as cursor:
                # Clear existing lock info
                await cursor.execute(f"DELETE FROM {self._table_lock_info}")
                
                # Save new lock info
                lock_json = json.dumps(lock_info)
                await cursor.execute(
                    f"INSERT INTO {self._table_lock_info} (info) VALUES (?)",
                    (lock_json,)
                )
            await self._sqlite_conn.commit()

    async def lock_info_retrieve(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve lock information.
        
        Returns
        -------
        dict or None
            The saved lock information, or None if no information exists
        """
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT info FROM {self._table_lock_info} LIMIT 1")
            row = await cursor.fetchone()
            return json.loads(row[0]) if row else None

    async def lock_info_clear(self) -> None:
        """Clear lock information."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"DELETE FROM {self._table_lock_info}")
        await self._sqlite_conn.commit()

    # --------------------------------------------------------------------------
    # Autostart Mode Management
    async def autostart_mode_save(self, autostart_mode: Dict[str, Any]) -> None:
        """
        Save autostart mode information.
        
        Parameters
        ----------
        autostart_mode : dict
            Autostart mode information to save
        """
        async with self._lock:
            async with self._sqlite_conn.cursor() as cursor:
                # Clear existing autostart mode info
                await cursor.execute(f"DELETE FROM {self._table_autostart_mode_info}")
                
                # Save new autostart mode info
                mode_json = json.dumps(autostart_mode)
                await cursor.execute(
                    f"INSERT INTO {self._table_autostart_mode_info} (info) VALUES (?)",
                    (mode_json,)
                )
            await self._sqlite_conn.commit()

    async def autostart_mode_retrieve(self) -> bool:
        """
        Retrieve autostart mode information.
        
        Returns
        -------
        bool
            True if autostart mode is enabled, False otherwise
        """
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT info FROM {self._table_autostart_mode_info} LIMIT 1")
            row = await cursor.fetchone()
            if row:
                info = json.loads(row[0])
                return info.get("enabled", False)
            return False

    async def autostart_mode_clear(self) -> None:
        """Clear autostart mode information."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"DELETE FROM {self._table_autostart_mode_info}")
        await self._sqlite_conn.commit()

    # --------------------------------------------------------------------------
    # Stop Pending State Management
    async def stop_pending_save(self, stop_pending: Dict[str, Any]) -> None:
        """
        Save stop pending information.
        
        Parameters
        ----------
        stop_pending : dict
            Stop pending information to save
        """
        async with self._lock:
            async with self._sqlite_conn.cursor() as cursor:
                # Clear existing stop pending info
                await cursor.execute(f"DELETE FROM {self._table_stop_pending_info}")
                
                # Save new stop pending info
                pending_json = json.dumps(stop_pending)
                await cursor.execute(
                    f"INSERT INTO {self._table_stop_pending_info} (info) VALUES (?)",
                    (pending_json,)
                )
            await self._sqlite_conn.commit()

    async def stop_pending_retrieve(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve stop pending information.
        
        Returns
        -------
        dict or None
            The saved stop pending information, or None if no information exists
        """
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT info FROM {self._table_stop_pending_info} LIMIT 1")
            row = await cursor.fetchone()
            return json.loads(row[0]) if row else None

    async def stop_pending_clear(self) -> None:
        """Clear stop pending information."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"DELETE FROM {self._table_stop_pending_info}")
        await self._sqlite_conn.commit()


    # --------------------------------------------------------------------------
    # User Group Permissions
    async def user_group_permissions_save(self, user_group_permissions: Dict[str, Any]) -> None:
        """
        Save user group permissions.
        
        Parameters
        ----------
        user_group_permissions : dict
            User group permissions to save
        """
        async with self._lock:
            async with self._sqlite_conn.cursor() as cursor:
                # Clear existing permissions
                await cursor.execute(f"DELETE FROM {self._table_user_group_permissions}")
                
                # Save new permissions
                perms_json = json.dumps(user_group_permissions)
                await cursor.execute(
                    f"INSERT INTO {self._table_user_group_permissions} (info) VALUES (?)",
                    (perms_json,)
                )
            await self._sqlite_conn.commit()

    async def user_group_permissions_retrieve(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve user group permissions.
        
        Returns
        -------
        dict or None
            The saved user group permissions, or None if no permissions exist
        """
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"SELECT info FROM {self._table_user_group_permissions} LIMIT 1")
            row = await cursor.fetchone()
            return json.loads(row[0]) if row else None

    async def user_group_permissions_clear(self) -> None:
        """Clear user group permissions."""
        async with self._sqlite_conn.cursor() as cursor:
            await cursor.execute(f"DELETE FROM {self._table_user_group_permissions}")
        await self._sqlite_conn.commit()

    async def get_queue_state(self) -> Dict[str, Any]:
        """
        Get the overall queue state.
        
        Returns
        -------
        dict
            Dictionary containing the queue state, including queue size, history size,
            and information about the currently running item.
        """
        queue_size = await self.get_queue_size()
        history_size = await self.get_history_size()
        running_item = await self.get_running_item_info()
        
        return {
            "queue_size": queue_size,
            "history_size": history_size,
            "running_item": running_item,
        }
    
    async def get_item(
        self, 
        *, 
        pos: Optional[Union[int, str]] = None,
        uid: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get an item from the queue without removing it.
        
        Parameters
        ----------
        pos : int or str, optional
            Position in the queue. Can be an integer (0-based index) or string ('front' or 'back')
        uid : str, optional
            UID of the item to get

        Returns
        -------
        dict
            The requested item
        """
        if (pos is None) and (uid is None):
            raise ValueError("Position or UID must be specified")
        if (pos is not None) and (uid is not None):
            raise ValueError("Ambiguous parameters: both position and UID are specified")
            
        # Extra validation could be added here
        if uid is not None:
           await self._verify_item_exists(uid)  # If you implement this method
        
        async with self._sqlite_conn.cursor() as cursor:
            if uid is not None:
                # Get by UID
                await cursor.execute(
                    f"SELECT item FROM {self._table_plan_queue} WHERE json_extract(item, '$.item_uid') = ?", 
                    (uid,)
                )
                row = await cursor.fetchone()
                if not row:
                    raise RuntimeError(f"Item with UID '{uid}' not found in the queue")
                return json.loads(row[0])
            else:
                # Get by position
                await cursor.execute(f"SELECT COUNT(*) FROM {self._table_plan_queue}")
                qsize = (await cursor.fetchone())[0]
                
                if qsize == 0:
                    raise IndexError("The queue is empty")
                    
                if pos == "front":
                    position = 0
                elif pos == "back":
                    position = qsize - 1
                elif isinstance(pos, int):
                    if pos < 0:
                        position = qsize + pos
                    else:
                        position = pos
                        
                    if position < 0 or position >= qsize:
                        raise IndexError(f"Position {pos} is out of range")
                else:
                    raise ValueError(f"Invalid value for 'pos': {pos}")
                    
                await cursor.execute(
                    f"SELECT item FROM {self._table_plan_queue} WHERE position = ?", 
                    (position,)
                )
                row = await cursor.fetchone()
                if not row:
                    raise IndexError(f"No item found at position {position}")
                return json.loads(row[0])

    async def process_next_item(
        self,
        *,
        item: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Process the next item in the queue or a specified item.
        
        Parameters
        ----------
        item : dict, optional
            Item for immediate execution, if not provided, use the next item from the queue

        Returns
        -------
        dict
            The processed item
        """
        # This method is essentially a wrapper around set_next_item_as_running
        return await self.set_next_item_as_running(item=item)

    async def set_plan_queue_mode(
        self,
        plan_queue_mode: Union[Dict[str, Any], str],
        *,
        update: bool = False
    ) -> Dict[str, Any]:
        """
        Set the plan queue mode.
        
        Parameters
        ----------
        plan_queue_mode : dict or str
            The new plan queue mode or 'default' to reset to default mode
        update : bool, default False
            If True, update only the specified fields in the mode dictionary

        Returns
        -------
        dict
            The updated plan queue mode
        """
        async with self._lock:
            # Create a table for plan queue mode if it doesn't exist
            async with self._sqlite_conn.cursor() as cursor:
                await cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self._table_plan_queue_mode} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        info TEXT
                    )
                """)
            
            # Get current mode from database or use default
            async with self._sqlite_conn.cursor() as cursor:
                await cursor.execute(f"SELECT info FROM {self._table_plan_queue_mode} LIMIT 1")
                row = await cursor.fetchone()
                current_mode = json.loads(row[0]) if row else copy.deepcopy(self._plan_queue_mode_default)
                
            # Reset to default if requested
            if isinstance(plan_queue_mode, str) and plan_queue_mode.lower() == "default":
                new_mode = copy.deepcopy(self._plan_queue_mode_default)
            elif not isinstance(plan_queue_mode, dict):
                raise TypeError(f"Parameter 'plan_queue_mode' must be a dictionary or 'default': {plan_queue_mode}")
            else:
                # Create a new mode or update the current one
                if update:
                    new_mode = copy.deepcopy(current_mode)
                    # Verify keys in plan_queue_mode
                    for key in plan_queue_mode:
                        if key not in self._plan_queue_mode_default:
                            raise ValueError(f"Unsupported plan queue mode parameter '{key}'")
                    # Update mode
                    new_mode.update(plan_queue_mode)
                else:
                    # Verify that all required keys are present
                    missing_keys = set(self._plan_queue_mode_default.keys()) - set(plan_queue_mode.keys())
                    if missing_keys:
                        raise ValueError(f"Parameters {missing_keys} are missing")
                    
                    # Verify keys in plan_queue_mode
                    for key in plan_queue_mode:
                        if key not in self._plan_queue_mode_default:
                            raise ValueError(f"Unsupported plan queue mode parameter '{key}'")
                            
                    new_mode = copy.deepcopy(plan_queue_mode)
            
            # Verify value types
            for key, value in new_mode.items():
                if key == "loop" and not isinstance(value, bool):
                    raise TypeError(f"Unsupported type {type(value)} of the parameter 'loop'")
                elif key == "ignore_failures" and not isinstance(value, bool):
                    raise TypeError(f"Unsupported type {type(value)} of the parameter 'ignore_failures'")
            
            # Save the new mode to the database
            async with self._sqlite_conn.cursor() as cursor:
                await cursor.execute(f"DELETE FROM {self._table_plan_queue_mode}")
                mode_json = json.dumps(new_mode)
                await cursor.execute(
                    f"INSERT INTO {self._table_plan_queue_mode} (info) VALUES (?)",
                    (mode_json,)
                )
            
            await self._sqlite_conn.commit()
            
            # Update instance variable
            self._plan_queue_mode = new_mode
            
            return copy.deepcopy(self._plan_queue_mode)

    @property
    def plan_queue_mode(self) -> Dict[str, Any]:
        """
        Get the current plan queue mode.
        
        Returns
        -------
        dict
            The current plan queue mode
        """
        return copy.deepcopy(self._plan_queue_mode)

    @property
    def plan_queue_mode_default(self) -> Dict[str, Any]:
        """
        Get the default plan queue mode.
        
        Returns
        -------
        dict
            The default plan queue mode
        """
        return copy.deepcopy(self._plan_queue_mode_default)

    @property
    def plan_queue_uid(self) -> str:
        """
        Get the current plan queue UID.
        
        Returns
        -------
        str
            The current plan queue UID
        """
        return self.new_item_uid()

    @property
    def plan_history_uid(self) -> str:
        """
        Get the current plan history UID.
        
        Returns
        -------
        str
            The current plan history UID
        """
        return self.new_item_uid()

    # --------------------------------------------------------------------------
    # method not included in the abstract (interface) class, but necessary for testing  
    async def get_queue(self) -> Tuple[List[Dict[str, Any]], str]:
        """
        Retrieve the queue contents and a unique queue ID.
        
        Returns
        -------
        tuple
            (queue_items, queue_uid) - all items in the queue and a unique queue ID
        """
        queue, _, queue_uid = await self.get_queue_full()
        return queue, queue_uid