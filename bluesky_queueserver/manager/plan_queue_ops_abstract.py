
from abc import ABC, abstractmethod
import asyncio

class AbstractPlanQueueOperations(ABC):
    """
    Abstract base class for plan queue operations. Subclasses must implement these methods
    for specific backends (e.g., Redis, SQLite).
    """

    # Variables
    @property
    @abstractmethod
    def plan_queue_uid(self):
        """Get the current plan queue UID."""
        pass

    @property
    @abstractmethod
    def plan_history_uid(self):
        """Get the current plan history UID."""
        pass

    @property
    @abstractmethod
    def plan_queue_mode(self):
        """Get or set the current plan queue mode."""
        pass

    # Methods
    @abstractmethod
    async def add_batch_to_queue(self, items_prepared, pos=None, before_uid=None, after_uid=None):
        """Add a batch of items to the queue."""
        pass

    @abstractmethod
    async def add_item_to_queue(self, item, pos=None, before_uid=None, after_uid=None):
        """Add a single item to the queue."""
        pass

    @abstractmethod
    async def autostart_mode_retrieve(self):
        """Retrieve the autostart mode."""
        pass

    @abstractmethod
    async def autostart_mode_save(self, enabled):
        """Save the autostart mode."""
        pass

    @abstractmethod
    async def clear_history(self):
        """Clear the plan history."""
        pass

    @abstractmethod
    async def clear_queue(self):
        """Clear the plan queue."""
        pass

    @abstractmethod
    async def get_history_size(self):
        """Get the size of the plan history."""
        pass

    @abstractmethod
    async def get_history(self):
        """Retrieve the plan history."""
        pass

    @abstractmethod
    async def get_item(self, pos="front", uid=None):
        """Retrieve an item from the queue."""
        pass

    @abstractmethod
    async def get_queue_full(self):
        """Retrieve the full queue."""
        pass

    @abstractmethod
    async def get_queue_size(self):
        """Get the size of the queue."""
        pass

    @abstractmethod
    async def get_running_item_info(self):
        """Retrieve information about the currently running item."""
        pass

    @abstractmethod
    async def lock_info_retrieve(self):
        """Retrieve lock information."""
        pass

    @abstractmethod
    async def lock_info_save(self, lock_info):
        """Save lock information."""
        pass

    @abstractmethod
    async def move_batch(self, uids, pos_dest=None, before_uid=None, after_uid=None, reorder=False):
        """Move a batch of items in the queue."""
        pass

    @abstractmethod
    async def move_item(self, pos=None, uid=None, pos_dest=None, before_uid=None, after_uid=None):
        """Move a single item in the queue."""
        pass

    @abstractmethod
    async def pop_item_from_queue_batch(self, uids, ignore_missing=False):
        """Pop a batch of items from the queue."""
        pass

    @abstractmethod
    async def pop_item_from_queue(self, pos=None, uid=None):
        """Pop a single item from the queue."""
        pass

    @abstractmethod
    async def process_next_item(self, item):
        """Process the next item in the queue."""
        pass

    @abstractmethod
    async def replace_item(self, item, item_uid_original):
        """Replace an item in the queue."""
        pass

    @abstractmethod
    async def set_new_item_uuid(self, item):
        """Set a new UUID for an item."""
        pass

    @abstractmethod
    async def set_plan_queue_mode(self, plan_queue_mode, update=True):
        """Set the plan queue mode."""
        pass

    @abstractmethod
    async def set_processed_item_as_completed(self, exit_status, run_uids, scan_ids, err_msg="", err_tb=""):
        """Mark a processed item as completed."""
        pass

    @abstractmethod
    async def set_processed_item_as_stopped(self, exit_status, run_uids, scan_ids, err_msg="", err_tb=""):
        """Mark a processed item as stopped."""
        pass

    @abstractmethod
    async def start(self):
        """Start the backend connection."""
        pass

    @abstractmethod
    async def stop_pending_retrieve(self):
        """Retrieve the stop-pending state."""
        pass

    @abstractmethod
    async def stop_pending_save(self, enabled):
        """Save the stop-pending state."""
        pass

    @abstractmethod
    async def stop(self):
        """Stop the backend connection."""
        pass

    @abstractmethod
    async def user_group_permissions_retrieve(self):
        """Retrieve user group permissions."""
        pass

    @abstractmethod
    async def user_group_permissions_save(self, user_group_permissions):
        """Save user group permissions."""
        pass

    # method found in qserver_cli.py and tests/common.py
    @abstractmethod
    async def lock_info_clear(self):
        """
        Clear lock information. This method is not part of the public API and should
        not be used outside of the class.
        """
        pass

    # method found in test_zmq_api.py; not applicable to SQLite
    @staticmethod
    @abstractmethod
    def new_item_uid():
        """
        Generate UID for an item (plan). This method must be implemented as a static method
        in subclasses.
        """
        pass

    # method found in test/common.py
    @abstractmethod
    async def delete_pool_entries(self):
        """
        Delete all pool entries used by the backend. This method is typically used for testing
        or resetting the backend state.
        """
        pass

    # method found in test/common.py
    @abstractmethod
    async def user_group_permissions_clear(self):
        """
        Clear user group permissions stored in the backend.
        """
        pass

    # method found in test/common.py
    @abstractmethod
    async def autostart_mode_clear(self):
        """
        Clear the autostart mode information stored in the backend.
        """
        pass

    # method found in test/common.py
    @abstractmethod
    async def stop_pending_clear(self):
        """
        Clear the stop-pending state information stored in the backend.
        """
        pass

