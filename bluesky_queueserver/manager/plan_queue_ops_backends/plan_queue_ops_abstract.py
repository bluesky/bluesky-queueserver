import abc
import copy
import uuid
from typing import Dict, List, Any, Optional, Tuple, Union


class AbstractPlanQueueOperations(abc.ABC):
    """
    Abstract base class that defines the interface for all queue operations implementations.
    
    This class defines the contract that all concrete implementations (Redis, SQLite, PostgreSQL, Dict)
    must follow to be compatible with the RunEngineManager.
    
    This class also includes UID management functionality for plans in a queue.
    It provides methods to verify, add, remove, and update plans in the queue
    based on their unique identifiers (UIDs).
    """

    def __init__(self):
        """
        Initialize the AbstractPlanQueueOperations class.
        """
        self._uid_dict = {}
        self._plan_queue_uid = self.new_item_uid()
        self._allowed_item_parameters = [
        "name", "args", "kwargs", "item_type", "user", "user_group",
        "item_uid", "properties", "meta"]

        # Initialize instance variables for plan queue mode
        self._plan_queue_mode_default = {"loop": False, "ignore_failures": False}
        self._plan_queue_mode = copy.deepcopy(self._plan_queue_mode_default)
        
        self._plan_history_uid = self.new_item_uid()

    @staticmethod
    def _verify_item_type(item):
        """
        Check that the item (plan) is a dictionary.
        """
        if not isinstance(item, dict):
            raise TypeError(f"Parameter 'item' should be a dictionary: '{item}', (type '{type(item)}')")

    # method works with redis
    async def _verify_item(self, item, *, ignore_uids=None):
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

    async def set_new_item_uuid(self, item):
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
        await self._verify_item(item)  # Make sure to await this
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
        
    def filter_item_parameters(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Filter item parameters."""
        # FIXED: Make a deep copy to avoid modifying the original
        filtered_item = copy.deepcopy(item)
        # Only keep allowed parameters
        return {k: v for k, v in filtered_item.items() if k in self._allowed_item_parameters}

    # --------------------------------------------------------------------------
    # Queue Operations
    @abc.abstractmethod
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
        pass
    
    @abc.abstractmethod
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
        pass

    @abc.abstractmethod
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
        pass

    @abc.abstractmethod
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
        pass

    @abc.abstractmethod
    async def clear_queue(self) -> None:
        """Clear the plan queue."""
        pass

    @abc.abstractmethod
    async def get_queue_size(self) -> int:
        """
        Get the size of the queue.
        
        Returns
        -------
        int
            The number of items in the queue
        """
        pass

    @abc.abstractmethod
    async def get_queue_full(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any], str]:
        """
        Retrieve the full queue.

        Returns
        -------
        tuple
            (queue_items, running_item, queue_uid) - list of items in the queue, currently running item, 
            and a unique ID for the current queue state
        """
        pass

    @abc.abstractmethod
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
        pass

    @abc.abstractmethod
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
        pass

    @abc.abstractmethod
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
        pass

    @abc.abstractmethod
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
        pass

    @abc.abstractmethod
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
        pass

    # --------------------------------------------------------------------------
    # History Management
    @abc.abstractmethod
    async def clear_history(self) -> None:
        """Clear the plan history."""
        pass

    @abc.abstractmethod
    async def get_history(self) -> Tuple[List[Dict[str, Any]], str]:
        """
        Retrieve the full history.

        Returns
        -------
        tuple
            (history_items, history_uid) - list of items in history and a unique ID for the current history state
        """
        pass

    @abc.abstractmethod
    async def get_history_size(self) -> int:
        """
        Get the size of the history.
        
        Returns
        -------
        int
            The number of items in history
        """
        pass

    # --------------------------------------------------------------------------
    # Running Item Management
    @abc.abstractmethod
    async def get_running_item_info(self) -> Dict[str, Any]:
        """
        Get information about the currently running item.
        
        Returns
        -------
        dict
            Information about the currently running item
        """
        pass

    @abc.abstractmethod
    async def set_next_item_as_running(
        self,
        *,
        item: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
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
        pass

    @abc.abstractmethod
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
        pass

    @abc.abstractmethod
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
        pass

    # --------------------------------------------------------------------------
    # State Management
    @abc.abstractmethod
    async def start(self) -> None:
        """Start the queue operations backend."""
        pass

    @abc.abstractmethod
    async def stop(self) -> None:
        """Stop the queue operations backend."""
        pass

    @abc.abstractmethod
    async def delete_pool_entries(self) -> None:
        """Delete all pool entries."""
        pass

    @abc.abstractmethod
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
        pass

    # --------------------------------------------------------------------------
    # Lock Management
    @abc.abstractmethod
    async def lock_info_clear(self) -> None:
        """Clear lock info."""
        pass

    @abc.abstractmethod
    async def lock_info_save(self, lock_info: Dict[str, Any]) -> None:
        """
        Save lock info.
        
        Parameters
        ----------
        lock_info : dict
            Lock information to save
        """
        pass

    @abc.abstractmethod
    async def lock_info_retrieve(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve saved lock info.
        
        Returns
        -------
        dict or None
            The saved lock information, or None if no information exists
        """
        pass

    # --------------------------------------------------------------------------
    # Autostart Mode Management
    @abc.abstractmethod
    async def autostart_mode_clear(self) -> None:
        """Clear autostart mode info."""
        pass

    @abc.abstractmethod
    async def autostart_mode_save(self, autostart_mode: Dict[str, Any]) -> None:
        """
        Save autostart mode info.
        
        Parameters
        ----------
        autostart_mode : dict
            Autostart mode information to save
        """
        pass

    @abc.abstractmethod
    async def autostart_mode_retrieve(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve saved autostart mode info.
        
        Returns
        -------
        dict or None
            The saved autostart mode information, or None if no information exists
        """
        pass

    # --------------------------------------------------------------------------
    # Stop Pending State Management
    @abc.abstractmethod
    async def stop_pending_clear(self) -> None:
        """Clear stop pending info."""
        pass

    @abc.abstractmethod
    async def stop_pending_save(self, stop_pending: Dict[str, Any]) -> None:
        """
        Save stop pending info.
        
        Parameters
        ----------
        stop_pending : dict
            Stop pending information to save
        """
        pass

    @abc.abstractmethod
    async def stop_pending_retrieve(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve saved stop pending info.
        
        Returns
        -------
        dict or None
            The saved stop pending information, or None if no information exists
        """
        pass

    # --------------------------------------------------------------------------
    # User Group Permissions
    @abc.abstractmethod
    async def user_group_permissions_clear(self) -> None:
        """Clear user group permissions."""
        pass

    @abc.abstractmethod
    async def user_group_permissions_save(self, user_group_permissions: Dict[str, Any]) -> None:
        """
        Save user group permissions.
        
        Parameters
        ----------
        user_group_permissions : dict
            User group permissions to save
        """
        pass

    @abc.abstractmethod
    async def user_group_permissions_retrieve(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve saved user group permissions.
        
        Returns
        -------
        dict or None
            The saved user group permissions, or None if no permissions exist
        """
        pass

    # --------------------------------------------------------------------------
    # Properties
    @property
    def plan_queue_uid(self) -> str:
        """
        Get the current plan queue UID.
        
        Returns
        -------
        str
            The current plan queue UID
        """
        raise NotImplementedError()

    @property
    def plan_history_uid(self) -> str:
        """
        Get the current plan history UID.
        
        Returns
        -------
        str
            The current plan history UID
        """
        raise NotImplementedError()

    @property
    def plan_queue_mode(self) -> Dict[str, Any]:
        """
        Get the current plan queue mode.
        
        Returns
        -------
        dict
            The current plan queue mode
        """
        raise NotImplementedError()