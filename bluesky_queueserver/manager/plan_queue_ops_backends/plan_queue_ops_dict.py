import asyncio
import aiofiles
import os
import json
import logging
import time as ttime
import copy
from typing import Any, Dict, List, Optional, Tuple, Union
from bluesky_queueserver.manager.plan_queue_ops_abstract import AbstractPlanQueueOperations

logger = logging.getLogger(__name__)

class DictPlanQueueOperations(AbstractPlanQueueOperations):
    """
    Backend implementation for plan queue operations using an in-memory Python dictionary.
    Data is saved and loaded to/from storage using JSON serialization with aiofiles.
    """

    def __init__(self, storage_file=None, in_memory=False, name_prefix="qs_default"):
        """
        Initialize the DictPlanQueueOperations class.
        """
        # Initialize the parent class
        super().__init__()
        
        self._in_memory = in_memory
        self._name_prefix = name_prefix  # Store name_prefix for consistency with other backends
        self._uid_dict = dict()  # Initialize UID dictionary for tracking

        if not in_memory:
            if not storage_file:
                # Use name_prefix in auto-generated filename
                storage_file = os.getenv(
                    "BACKEND_DB_PATH", 
                    os.path.join(os.getcwd(), f"{name_prefix}_plan_queue_storage.json")
                )
            self._storage_file = storage_file
        else:
            self._storage_file = None  # No file is used for in-memory mode

        self._data = {
            "plan_queue": [],
            "plan_history": [],
            "running_plan": None,
            "plan_queue_mode": {"loop": False, "ignore_failures": False},
            "user_group_permissions": None,
            "lock_info": None,
            "autostart_mode_info": None,
            "stop_pending_info": None,
        }
        self._plan_queue_uid = self.new_item_uid()
        self._plan_history_uid = self.new_item_uid()
        
        # The list of allowed item parameters used for parameter filtering
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
        
        self._lock = asyncio.Lock()

    # UID Dictionary Operations
    def _uid_dict_add(self, item):
        """Add an item to the UID dictionary."""
        if "item_uid" in item:
            self._uid_dict[item["item_uid"]] = item

    def _uid_dict_remove(self, uid):
        """Remove an item from the UID dictionary."""
        if uid in self._uid_dict:
            del self._uid_dict[uid]

    def _uid_dict_clear(self):
        """Clear the UID dictionary."""
        self._uid_dict.clear()

    def _uid_dict_get_item(self, uid):
        """Get an item from the UID dictionary."""
        return self._uid_dict.get(uid)

    def _is_uid_in_dict(self, uid):
        """Check if a UID exists in the dictionary."""
        return uid in self._uid_dict

    async def _verify_item_type(self, item):
        """Verify that item is a dictionary."""
        if not isinstance(item, dict):
            raise TypeError(f"Parameter 'item' should be a dictionary, got {type(item)}")

    async def _verify_item(self, item, *, ignore_uids=None):
        """
        Verify that item structure is valid enough to be put in the queue.
        """
        ignore_uids = ignore_uids or []
        await self._verify_item_type(item)
        
        # First check for UID (original behavior that tests expect)
        if "item_uid" not in item:
            raise ValueError("Item does not have UID.")
            
        # Then check if UID is already in use
        uid = item["item_uid"]
        if (uid not in ignore_uids) and self._is_uid_in_dict(uid):
            raise RuntimeError(f"Item with UID {uid} is already in the queue")
            
        # Only then check name (this won't be reached if UID check fails)
        if "name" not in item or not item["name"]:
            raise ValueError("Item must have a valid name")

    async def set_new_item_uuid(self, item):
        """
        Set a new UUID for an item.
        
        Parameters
        ----------
        item : dict
            Item to set UUID for
            
        Returns
        -------
        dict
            Copy of the item with a new UUID
        """
        item_copy = copy.deepcopy(item)
        item_copy["item_uid"] = self.new_item_uid()
        return item_copy

    async def _save_to_storage(self):
        """Save the data to storage file if not in memory mode."""
        if not self._in_memory and self._storage_file:
            try:
                async with aiofiles.open(self._storage_file, 'w') as f:
                    await f.write(json.dumps(self._data))
            except Exception as ex:
                logger.error(f"Failed to save data to storage file: {ex}")
    
    async def _load_from_storage(self):
        """Load the data from storage file if it exists."""
        if not self._in_memory and self._storage_file and os.path.exists(self._storage_file):
            try:
                async with aiofiles.open(self._storage_file, 'r') as f:
                    content = await f.read()
                    if content:
                        self._data = json.loads(content)
            except Exception as ex:
                logger.error(f"Failed to load data from storage file: {ex}")

    def filter_item_parameters(self, item):
        """
        Remove parameters that are not in the list of allowed parameters.
        
        Parameters
        ----------
        item : dict
            dictionary of item parameters

        Returns
        -------
        dict
            dictionary of filtered item parameters
        """
        return {k: v for k, v in item.items() if k in self._allowed_item_parameters}

    # --------------------------------------------------------------------------
    # Queue Operations
    async def add_item_to_queue(self, item, *, pos=None, before_uid=None, after_uid=None, filter_parameters=True):
        """Add a single item to the queue."""
        async with self._lock:
            if (pos is not None) and (before_uid is not None or after_uid is not None):
                raise ValueError("Ambiguous parameters: plan position and UID is specified.")

            if (before_uid is not None) and (after_uid is not None):
                raise ValueError(
                    "Ambiguous parameters: request to insert the plan before and after the reference plan."
                )

            pos = pos if pos is not None else "back"

            if "item_uid" not in item:
                item = await self.set_new_item_uuid(item)  # Added await here
            else:
                await self._verify_item(item)

            if filter_parameters:
                item = self.filter_item_parameters(item)

            qsize = len(self._data["plan_queue"])
            
            if isinstance(pos, int):
                if (pos <= 0) or (pos < -qsize):
                    pos = "front"
                elif (pos >= qsize) or (pos == -1):
                    pos = "back"
            
            if (before_uid is not None) or (after_uid is not None):
                uid = before_uid if before_uid is not None else after_uid
                before = uid == before_uid

                if not self._is_uid_in_dict(uid):
                    raise IndexError(f"Item with UID '{uid}' not found in the queue.")
                    
                # Find the item with the specified UID
                idx = next((i for i, q in enumerate(self._data["plan_queue"]) if q["item_uid"] == uid), None)
                if idx is None:
                    raise IndexError(f"Item with UID '{uid}' not found in the queue.")
                    
                insert_pos = idx if before else idx + 1
                self._data["plan_queue"].insert(insert_pos, item)
            elif pos == "front":
                self._data["plan_queue"].insert(0, item)
            elif pos == "back":
                self._data["plan_queue"].append(item)
            else:  # pos is an integer
                self._data["plan_queue"].insert(pos, item)
            
            # Add item to UID dictionary
            self._uid_dict_add(item)
            self._plan_queue_uid = self.new_item_uid()
            
            await self._save_to_storage()
            return item, len(self._data["plan_queue"])

    async def add_batch_to_queue(self, items, *, pos=None, before_uid=None, after_uid=None, filter_parameters=True):
        """Add a batch of items to the queue."""
        async with self._lock:
            items_added, results = [], []
            
            # Approach: attempt to add each item to queue. In case of a failure, remove all added items.
            success = True
            added_item_uids = []  # List of successfully added UIDs (used for rollback)
            
            for item in items:
                try:
                    if not added_item_uids:
                        item_added, _ = await self.add_item_to_queue(
                            item,
                            pos=pos,
                            before_uid=before_uid,
                            after_uid=after_uid,
                            filter_parameters=filter_parameters,
                        )
                    else:
                        item_added, _ = await self.add_item_to_queue(
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
                    break  # Stop processing more items if one fails
            
            # 'Undo' operation: remove all inserted items if there was a failure
            if not success:
                for uid in added_item_uids:
                    try:
                        await self.pop_item_from_queue(uid=uid)
                    except Exception as ex:
                        logger.error(
                            f"Failed to remove an item with UID '{uid}' after failure to add a batch of plans: {ex}"
                        )
                # Return the original items if the batch operation fails
                items_added = items
            
            qsize = len(self._data["plan_queue"])
            return items_added, results, qsize, success

    async def set_processed_item_as_completed(self, *, exit_status, run_uids=None, scan_ids=None, err_msg="", err_tb=""):
        """Mark the currently running item as completed and move it to history."""
        async with self._lock:
            # Default empty lists for run_uids and scan_ids
            run_uids = run_uids or []
            scan_ids = scan_ids or []
            
            # Check if loop mode is enabled
            loop_enabled = self._data["plan_queue_mode"].get("loop", False)
            
            running_item = self._data["running_plan"]
            if running_item:
                immediate_execution = running_item.get("properties", {}).get("immediate_execution", False)
                item_time_start = running_item.get("properties", {}).get("time_start", ttime.time())
                item_cleaned = await self._clean_item_properties(running_item)  # Added await here
                
                if loop_enabled and not immediate_execution:
                    # Add a copy of the item back to the queue
                    item_to_add = copy.deepcopy(item_cleaned)
                    item_to_add = await self.set_new_item_uuid(item_to_add)  # Added await here
                    self._data["plan_queue"].append(item_to_add)
                    self._uid_dict_add(item_to_add)
                    
                # Add result information
                item_cleaned.setdefault("result", {})
                item_cleaned["result"]["exit_status"] = exit_status
                item_cleaned["result"]["run_uids"] = run_uids
                item_cleaned["result"]["scan_ids"] = scan_ids
                item_cleaned["result"]["time_start"] = item_time_start
                item_cleaned["result"]["time_stop"] = ttime.time()
                item_cleaned["result"]["msg"] = err_msg
                item_cleaned["result"]["traceback"] = err_tb
                
                # Clear running item
                self._data["running_plan"] = None
                
                # Remove from UID dictionary if not in loop mode and not immediate execution
                if not loop_enabled and not immediate_execution:
                    self._uid_dict_remove(running_item["item_uid"])
                    
                # Add to history
                self._data["plan_history"].append(item_cleaned)
                self._plan_history_uid = self.new_item_uid()
                self._plan_queue_uid = self.new_item_uid()
                
                await self._save_to_storage()
                return item_cleaned
            else:
                return {}

    async def set_processed_item_as_stopped(self, *, exit_status, run_uids=None, scan_ids=None, err_msg="", err_tb=""):
        """
        Mark the currently running item as stopped and either move it to history
        or push it back to the front of the queue depending on exit status.
        """
        async with self._lock:
            run_uids = run_uids or []
            scan_ids = scan_ids or []
            
            if exit_status == "stopped":
                # Stopped item is considered successful, so it is not pushed back to the queue
                return await self.set_processed_item_as_completed(
                    exit_status=exit_status, run_uids=run_uids, scan_ids=scan_ids, err_msg=err_msg, err_tb=err_tb
                )
            
            running_item = self._data["running_plan"]
            if running_item:
                immediate_execution = running_item.get("properties", {}).get("immediate_execution", False)
                item_time_start = running_item.get("properties", {}).get("time_start", ttime.time())
                item_cleaned = await self._clean_item_properties(running_item)  # Added await here
                
                # Add result information
                item_cleaned.setdefault("result", {})
                item_cleaned["result"]["exit_status"] = exit_status
                item_cleaned["result"]["run_uids"] = run_uids
                item_cleaned["result"]["scan_ids"] = scan_ids
                item_cleaned["result"]["time_start"] = item_time_start
                item_cleaned["result"]["time_stop"] = ttime.time()
                item_cleaned["result"]["msg"] = err_msg
                item_cleaned["result"]["traceback"] = err_tb
                
                # Add to history
                self._data["plan_history"].append(item_cleaned)
                self._plan_history_uid = self.new_item_uid()
                
                # Clear running item
                self._data["running_plan"] = None
                
                # Generate new UID for the item that is pushed back into the queue
                if not immediate_execution:
                    self._uid_dict_remove(running_item["item_uid"])
                    # Only push back to queue for non-stopped statuses
                    if exit_status != "stopped":
                        item_copy = copy.deepcopy(item_cleaned)
                        if "result" in item_copy:
                            del item_copy["result"]
                        item_pushed_to_queue = await self.set_new_item_uuid(item_copy)  # Added await here
                        self._data["plan_queue"].insert(0, item_pushed_to_queue)
                        self._uid_dict_add(item_pushed_to_queue)
                        
                self._plan_queue_uid = self.new_item_uid()
                await self._save_to_storage()
                return item_cleaned
            else:
                return {}

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
            # Default mode for reference
            default_mode = {"loop": False, "ignore_failures": False}
            
            # Reset to default if requested
            if isinstance(plan_queue_mode, str) and plan_queue_mode.lower() == "default":
                self._data["plan_queue_mode"] = copy.deepcopy(default_mode)
            elif not isinstance(plan_queue_mode, dict):
                raise TypeError(f"Parameter 'plan_queue_mode' must be a dictionary or 'default': {plan_queue_mode}")
            else:
                # Create a new mode or update the current one
                if update:
                    # Verify keys in plan_queue_mode
                    for key in plan_queue_mode:
                        if key not in default_mode:
                            raise ValueError(f"Unsupported plan queue mode parameter '{key}'")
                    # Update mode
                    self._data["plan_queue_mode"].update(plan_queue_mode)
                else:
                    # Verify that all required keys are present
                    missing_keys = set(default_mode.keys()) - set(plan_queue_mode.keys())
                    if missing_keys:
                        raise ValueError(f"Parameters {missing_keys} are missing")
                    
                    # Verify keys in plan_queue_mode
                    for key in plan_queue_mode:
                        if key not in default_mode:
                            raise ValueError(f"Unsupported plan queue mode parameter '{key}'")
                            
                    self._data["plan_queue_mode"] = copy.deepcopy(plan_queue_mode)
            
            # Verify value types
            for key, value in self._data["plan_queue_mode"].items():
                if key == "loop" and not isinstance(value, bool):
                    raise TypeError(f"Unsupported type {type(value)} of the parameter 'loop'")
                elif key == "ignore_failures" and not isinstance(value, bool):
                    raise TypeError(f"Unsupported type {type(value)} of the parameter 'ignore_failures'")
            
            # Save to storage
            await self._save_to_storage()
                
            return copy.deepcopy(self._data["plan_queue_mode"])

    @property
    def plan_queue_mode(self) -> Dict[str, Any]:
        """
        Get the current plan queue mode.
        
        Returns
        -------
        dict
            The current plan queue mode
        """
        return copy.deepcopy(self._data["plan_queue_mode"])