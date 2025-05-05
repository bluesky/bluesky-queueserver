import asyncio
import aiofiles
import os
import json
import logging
import time as ttime
import uuid
from bluesky_queueserver.manager.plan_queue_ops_abstract import AbstractPlanQueueOperations
# from bluesky_queueserver.manager.plan_queue_ops_base import BasePlanQueueOperations

logger = logging.getLogger(__name__)

class DictPlanQueueOperations(AbstractPlanQueueOperations): # Add a BasePlanQueueOperations class to ensure methods are implemented equivalently
    """
    Backend implementation for plan queue operations using an in-memory Python dictionary.
    Data is saved and loaded to/from storage using JSON serialization with aiofiles.

    **Limitations
    The DictPlanQueueOperations backend is designed for in-memory operations with persistence via JSON files. 
    While it is suitable for lightweight or testing scenarios, it may not be ideal for production environments 
    where durability and concurrency are critical. For such cases, Redis or SQLite backends are more appropriate.
    """

    def __init__(self, storage_file=None):
        """
        Initialize the DictPlanQueueOperations class.

        Parameters
        ----------
        storage_file : str or None
            Path to the file where the data will be saved and loaded. If None, the path is determined
            by the environment variable `PLAN_QUEUE_DICT_PATH`. If the environment variable is not set,
            the default is `plan_queue_storage.json` in the current working directory.
        """
        # Determine the storage file path
        if storage_file is None:
            storage_file = os.getenv("PLAN_QUEUE_DICT_PATH", os.path.join(os.getcwd(), "plan_queue_storage.json"))

        self._storage_file = storage_file
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
        return self._data["plan_queue_mode"]

    async def _save_to_storage(self):
        """Save the current state to the storage file asynchronously."""
        async with aiofiles.open(self._storage_file, "w") as f:
            await f.write(json.dumps(self._data))

    async def _load_from_storage(self):
        """Load the state from the storage file asynchronously."""
        try:
            async with aiofiles.open(self._storage_file, "r") as f:
                content = await f.read()
                self._data = json.loads(content)
        except FileNotFoundError:
            pass  # If the file doesn't exist, keep the default data

    async def start(self):
        """Start the backend connection by loading data from storage."""
        await self._load_from_storage()

    async def stop(self):
        """Stop the backend connection by saving data to storage."""
        await self._save_to_storage()

    async def add_item_to_queue(self, item, pos=None, before_uid=None, after_uid=None):
        """Add a single item to the queue."""
        if "item_uid" not in item:
            item["item_uid"] = self.new_item_uid()
        if pos == "front":
            self._data["plan_queue"].insert(0, item)
        else:  # Default to adding to the back
            self._data["plan_queue"].append(item)
        self._plan_queue_uid = self.new_item_uid()
        await self._save_to_storage()

    async def get_queue_size(self):
        """Get the size of the queue."""
        return len(self._data["plan_queue"])

    async def get_queue_full(self):
        """Retrieve the full queue."""
        return self._data["plan_queue"]

    async def clear_queue(self):
        """Clear the plan queue."""
        self._data["plan_queue"] = []
        self._plan_queue_uid = self.new_item_uid()
        await self._save_to_storage()

    async def set_plan_queue_mode(self, plan_queue_mode, update=True):
        """Set the plan queue mode."""
        if update:
            self._data["plan_queue_mode"].update(plan_queue_mode)
        else:
            self._data["plan_queue_mode"] = plan_queue_mode
        await self._save_to_storage()

    async def get_running_item_info(self):
        """Retrieve information about the currently running item."""
        return self._data["running_plan"]

    async def set_processed_item_as_completed(self, exit_status, run_uids, scan_ids, err_msg="", err_tb=""):
        """Mark a processed item as completed and move it to the history."""
        item = self._data["running_plan"]
        if not item:
            return {}
        item["result"] = {
            "exit_status": exit_status,
            "run_uids": run_uids,
            "scan_ids": scan_ids,
            "time_start": item.get("properties", {}).get("time_start", None),
            "time_stop": ttime.time(),
            "msg": err_msg,
            "traceback": err_tb,
        }
        self._data["plan_history"].append(item)
        self._data["running_plan"] = None
        self._plan_history_uid = self.new_item_uid()
        await self._save_to_storage()
        return item
    
    async def set_processed_item_as_stopped(self, exit_status, run_uids, scan_ids, err_msg="", err_tb=""):
        """Mark a processed item as stopped and move it to the history."""
        item = self._data["running_plan"]
        if not item:
            return {}
        item["result"] = {
            "exit_status": exit_status,
            "run_uids": run_uids,
            "scan_ids": scan_ids,
            "time_start": item.get("properties", {}).get("time_start", None),
            "time_stop": ttime.time(),
            "msg": err_msg,
            "traceback": err_tb,
        }
        self._data["plan_history"].append(item)
        self._data["running_plan"] = None
        if exit_status not in ["stopped", "completed"]:
            item["item_uid"] = self.new_item_uid()
            self._data["plan_queue"].insert(0, item)
        self._plan_history_uid = self.new_item_uid()
        await self._save_to_storage()
        return item

    async def user_group_permissions_save(self, user_group_permissions):
        """Save user group permissions."""
        self._data["user_group_permissions"] = user_group_permissions
        await self._save_to_storage()

    async def user_group_permissions_retrieve(self):
        """Retrieve user group permissions."""
        return self._data["user_group_permissions"]

    async def lock_info_clear(self):
        """Clear lock information."""
        self._data["lock_info"] = None
        await self._save_to_storage()

    async def new_item_uid(self):
        """Generate a new item UID."""
        return str(uuid.uuid4())

    async def delete_pool_entries(self):
        """Delete all pool entries."""
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
        await self._save_to_storage()

    async def user_group_permissions_clear(self):
        """Clear user group permissions."""
        self._data["user_group_permissions"] = None
        await self._save_to_storage()

    async def autostart_mode_clear(self):
        """Clear the autostart mode information."""
        self._data["autostart_mode_info"] = None
        await self._save_to_storage()

    async def stop_pending_clear(self):
        """Clear the stop-pending state information."""
        self._data["stop_pending_info"] = None
        await self._save_to_storage()

#### queue operations
    async def replace_item(self, item, uid):
        """Replace an item in the queue by its UID."""
        for i, existing_item in enumerate(self._data["plan_queue"]):
            if existing_item["item_uid"] == uid:
                self._data["plan_queue"][i] = item
                break
        else:
            raise ValueError(f"Item with UID {uid} not found in the queue.")
        self._plan_queue_uid = self.new_item_uid()
        await self._save_to_storage()

    async def move_item(self, uid, pos=None, before_uid=None, after_uid=None):
        """Move an item to a new position in the queue."""
        for i, existing_item in enumerate(self._data["plan_queue"]):
            if existing_item["item_uid"] == uid:
                item = self._data["plan_queue"].pop(i)
                break
        else:
            raise ValueError(f"Item with UID {uid} not found in the queue.")

        if pos == "front":
            self._data["plan_queue"].insert(0, item)
        elif pos == "back":
            self._data["plan_queue"].append(item)
        elif before_uid:
            for i, existing_item in enumerate(self._data["plan_queue"]):
                if existing_item["item_uid"] == before_uid:
                    self._data["plan_queue"].insert(i, item)
                    break
            else:
                raise ValueError(f"Item with UID {before_uid} not found in the queue.")
        elif after_uid:
            for i, existing_item in enumerate(self._data["plan_queue"]):
                if existing_item["item_uid"] == after_uid:
                    self._data["plan_queue"].insert(i + 1, item)
                    break
            else:
                raise ValueError(f"Item with UID {after_uid} not found in the queue.")
        else:
            raise ValueError("Invalid position parameters.")

        self._plan_queue_uid = self.new_item_uid()
        await self._save_to_storage()

    async def pop_item_from_queue(self):
        """Remove and return the first item from the queue."""
        if not self._data["plan_queue"]:
            return None
        item = self._data["plan_queue"].pop(0)
        self._plan_queue_uid = self.new_item_uid()
        await self._save_to_storage()
        return item
    
#### history management
    async def clear_history(self):
        """Clear the plan history."""
        self._data["plan_history"] = []
        self._plan_history_uid = self.new_item_uid()
        await self._save_to_storage()

    async def get_history_size(self):
        """Get the size of the plan history."""
        return len(self._data["plan_history"])

    async def get_history(self):
        """Retrieve the full history."""
        return self._data["plan_history"]
    
#### Lock management
    async def lock_info_retrieve(self):
        """Retrieve lock information."""
        return self._data["lock_info"]

    async def lock_info_save(self, lock_info):
        """Save lock information."""
        self._data["lock_info"] = lock_info
        await self._save_to_storage()

#### Autostart mode management
    async def autostart_mode_retrieve(self):
        """Retrieve the autostart mode information."""
        return self._data["autostart_mode_info"]

    async def autostart_mode_save(self, enabled):
        """Save the autostart mode information."""
        self._data["autostart_mode_info"] = enabled
        await self._save_to_storage()

#### Stop-pending state management
    async def stop_pending_retrieve(self):
        """Retrieve the stop-pending state information."""
        return self._data["stop_pending_info"]

    async def stop_pending_save(self, stop_pending_info):
        """Save the stop-pending state information."""
        self._data["stop_pending_info"] = stop_pending_info
        await self._save_to_storage()