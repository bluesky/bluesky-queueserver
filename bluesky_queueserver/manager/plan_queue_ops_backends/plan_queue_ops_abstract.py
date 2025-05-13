"""
Abstract base class for plan queue operations.

This module defines the required interface contract that all backend implementations
must follow. This ensures consistency across different storage backends (Redis, SQLite, 
PostgreSQL, Dict).
"""

import abc
import asyncio
import copy
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union


class AbstractPlanQueueOperations(abc.ABC):
    """
    Abstract base class defining the interface for plan queue operations.
    
    All backend implementations must inherit from this class and implement
    all abstract methods and properties.
    """
    
    def __init__(self):
        """
        Initialize the base class.
        """
        # Initialize UID dictionary
        self._uid_dict = {}
        self._plan_queue_uid = self.new_item_uid()
        self._plan_history_uid = self.new_item_uid()
    
    @staticmethod
    def new_item_uid() -> str:
        """
        Generate a new unique ID for an item.
        
        Returns
        -------
        str
            A new unique ID
        """
        return str(uuid.uuid4())
    
    @property
    @abc.abstractmethod
    def plan_queue_uid(self) -> str:
        """
        Get the current plan queue UID.
        
        Returns
        -------
        str
            The current plan queue UID
        """
        pass
    
    @property
    @abc.abstractmethod
    def plan_history_uid(self) -> str:
        """
        Get the current plan history UID.
        
        Returns
        -------
        str
            The current plan history UID
        """
        pass
    
    @property
    @abc.abstractmethod
    def plan_queue_mode(self) -> Dict[str, Any]:
        """
        Get the current plan queue mode.
        
        Returns
        -------
        dict
            The current plan queue mode
        """
        pass
    
    @abc.abstractmethod
    async def start(self) -> None:
        """
        Start the plan queue operations.
        
        This method should initialize any necessary connections and resources.
        """
        pass
    
    @abc.abstractmethod
    async def stop(self) -> None:
        """
        Stop the plan queue operations.
        
        This method should clean up any resources and close connections.
        """
        pass
    
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
        Add a single item to the queue.
        
        Parameters
        ----------
        item : dict
            Item to add to the queue
        pos : int or str, optional
            Position to add the item at. Can be an integer or 'front'/'back'.
            If None, the item is added to the back of the queue.
        before_uid : str, optional
            UID of the item to add the new item before
        after_uid : str, optional
            UID of the item to add the new item after
        filter_parameters : bool, default True
            Whether to filter the item parameters
            
        Returns
        -------
        tuple
            Tuple of (item, queue_size)
            
        Raises
        ------
        ValueError
            If the position specification is ambiguous
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
        items : list of dict
            Items to add to the queue
        pos : int or str, optional
            Position to add the items at. Can be an integer or 'front'/'back'.
            If None, the items are added to the back of the queue.
        before_uid : str, optional
            UID of the item to add the new items before
        after_uid : str, optional
            UID of the item to add the new items after
        filter_parameters : bool, default True
            Whether to filter the item parameters
            
        Returns
        -------
        tuple
            Tuple of (items_added, results, queue_size, success)
            
        Raises
        ------
        ValueError
            If the position specification is ambiguous
        """
        pass
    
    @abc.abstractmethod
    async def get_queue(self) -> Tuple[List[Dict[str, Any]], str]:
        """
        Get the current queue.
        
        Returns
        -------
        tuple
            Tuple of (queue, queue_uid)
        """
        pass
    
    @abc.abstractmethod
    async def get_queue_size(self) -> int:
        """
        Get the current queue size.
        
        Returns
        -------
        int
            The current queue size
        """
        pass
    
    @abc.abstractmethod
    async def clear_queue(self) -> int:
        """
        Clear the queue.
        
        Returns
        -------
        int
            The number of items removed from the queue
        """
        pass
    
    @abc.abstractmethod
    async def pop_item_from_queue(
        self, 
        *, 
        pos: Optional[int] = None, 
        uid: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Remove an item from the queue.
        
        Parameters
        ----------
        pos : int, optional
            Position of the item to remove
        uid : str, optional
            UID of the item to remove
            
        Returns
        -------
        dict
            The removed item
            
        Raises
        ------
        IndexError
            If the position is invalid
        ValueError
            If both pos and uid are specified or neither is specified
        """
        pass
    
    @abc.abstractmethod
    async def pop_item_from_queue_batch(self, *, uids: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Remove multiple items from the queue.
        
        Parameters
        ----------
        uids : list of str
            UIDs of the items to remove
            
        Returns
        -------
        tuple
            Tuple of (removed_items, remaining_queue)
        """
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
    
    @abc.abstractmethod
    async def move_item(
        self, 
        *, 
        pos: int = None, 
        uid: str = None, 
        pos_dest: int = None, 
        before_uid: str = None, 
        after_uid: str = None
    ) -> Dict[str, Any]:
        """
        Move an item in the queue.
        
        Parameters
        ----------
        pos : int, optional
            Position of the item to move
        uid : str, optional
            UID of the item to move
        pos_dest : int, optional
            Destination position
        before_uid : str, optional
            UID of the item to move the item before
        after_uid : str, optional
            UID of the item to move the item after
            
        Returns
        -------
        dict
            The moved item
            
        Raises
        ------
        ValueError
            If the source or destination specification is ambiguous
        IndexError
            If the position is invalid
        """
        pass
    
    @abc.abstractmethod
    async def swap_items(
        self, 
        *, 
        pos1: int = None, 
        uid1: str = None, 
        pos2: int = None, 
        uid2: str = None
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Swap two items in the queue.
        
        Parameters
        ----------
        pos1 : int, optional
            Position of the first item
        uid1 : str, optional
            UID of the first item
        pos2 : int, optional
            Position of the second item
        uid2 : str, optional
            UID of the second item
            
        Returns
        -------
        tuple
            Tuple of (item1, item2)
            
        Raises
        ------
        ValueError
            If the item specification is ambiguous
        IndexError
            If the position is invalid
        """
        pass
    
    @abc.abstractmethod
    async def get_item(self, *, pos: int = None, uid: str = None) -> Dict[str, Any]:
        """
        Get an item from the queue without removing it.
        
        Parameters
        ----------
        pos : int, optional
            Position of the item
        uid : str, optional
            UID of the item
            
        Returns
        -------
        dict
            The item
            
        Raises
        ------
        ValueError
            If both pos and uid are specified or neither is specified
        IndexError
            If the position is invalid
        """
        pass
    
    @abc.abstractmethod
    async def update_item(
        self, 
        item: Dict[str, Any], 
        *, 
        pos: int = None, 
        uid: str = None
    ) -> Dict[str, Any]:
        """
        Update an item in the queue.
        
        Parameters
        ----------
        item : dict
            New item data
        pos : int, optional
            Position of the item to update
        uid : str, optional
            UID of the item to update
            
        Returns
        -------
        dict
            The updated item
            
        Raises
        ------
        ValueError
            If both pos and uid are specified or neither is specified
        IndexError
            If the position is invalid
        """
        pass
    
    @abc.abstractmethod
    async def is_item_running(self) -> bool:
        """
        Check if an item is currently running.
        
        Returns
        -------
        bool
            True if an item is running, False otherwise
        """
        pass
    
    @abc.abstractmethod
    async def process_next_item(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Process the next item in the queue.
        
        This method typically pops the first item from the queue.
        
        Returns
        -------
        tuple
            Tuple of (next_item, running_item)
        """
        pass
    
    @abc.abstractmethod
    async def set_next_item_as_running(self, *, item: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Set the next item in the queue as the running item.
        
        Parameters
        ----------
        item : dict, optional
            Item to set as running. If None, the next item in the queue is used.
            
        Returns
        -------
        dict
            The item set as running
        """
        pass
    
    @abc.abstractmethod
    async def set_processed_item_as_completed(
        self, 
        *, 
        exit_status: str,
        run_uids: Optional[List[str]] = None,
        scan_ids: Optional[List[int]] = None,
        err_msg: str = "",
        err_tb: str = ""
    ) -> Dict[str, Any]:
        """
        Mark the currently running item as completed and move it to history.
        
        Parameters
        ----------
        exit_status : str
            Exit status of the processed item
        run_uids : list of str, optional
            List of run UIDs
        scan_ids : list of int, optional
            List of scan IDs
        err_msg : str, optional
            Error message
        err_tb : str, optional
            Error traceback
            
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
        Mark the currently running item as stopped and either move it to history
        or push it back to the front of the queue depending on exit status.
        
        Parameters
        ----------
        exit_status : str
            Exit status of the processed item
        run_uids : list of str, optional
            List of run UIDs
        scan_ids : list of int, optional
            List of scan IDs
        err_msg : str, optional
            Error message
        err_tb : str, optional
            Error traceback
            
        Returns
        -------
        dict
            The stopped item
        """
        pass
    
    @abc.abstractmethod
    async def clear_running_item(self) -> Dict[str, Any]:
        """
        Clear the currently running item.
        
        Returns
        -------
        dict
            The previously running item
        """
        pass
    
    @abc.abstractmethod
    async def get_running_item(self) -> Dict[str, Any]:
        """
        Get the currently running item.
        
        Returns
        -------
        dict
            The currently running item
        """
        pass
    
    @abc.abstractmethod
    async def get_history(self) -> Tuple[List[Dict[str, Any]], str]:
        """
        Get the plan history.
        
        Returns
        -------
        tuple
            Tuple of (history, history_uid)
        """
        pass
    
    @abc.abstractmethod
    async def clear_history(self) -> int:
        """
        Clear the plan history.
        
        Returns
        -------
        int
            The number of items removed from history
        """
        pass
    
    @abc.abstractmethod
    async def user_group_permissions_get(self) -> Dict[str, Any]:
        """
        Get the user group permissions.
        
        Returns
        -------
        dict
            User group permissions
        """
        pass
    
    @abc.abstractmethod
    async def user_group_permissions_set(self, user_group_permissions: Dict[str, Any]) -> Dict[str, Any]:
        """
        Set the user group permissions.
        
        Parameters
        ----------
        user_group_permissions : dict
            User group permissions
            
        Returns
        -------
        dict
            Updated user group permissions
        """
        pass
    
    @abc.abstractmethod
    async def lock_info_get(self) -> Dict[str, Any]:
        """
        Get the lock information.
        
        Returns
        -------
        dict
            Lock information
        """
        pass
    
    @abc.abstractmethod
    async def lock_info_set(self, lock_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Set the lock information.
        
        Parameters
        ----------
        lock_info : dict
            Lock information
            
        Returns
        -------
        dict
            Updated lock information
        """
        pass
    
    @abc.abstractmethod
    async def autostart_mode_get(self) -> Dict[str, Any]:
        """
        Get the autostart mode information.
        
        Returns
        -------
        dict
            Autostart mode information
        """
        pass
    
    @abc.abstractmethod
    async def autostart_mode_set(self, autostart_mode_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Set the autostart mode information.
        
        Parameters
        ----------
        autostart_mode_info : dict
            Autostart mode information
            
        Returns
        -------
        dict
            Updated autostart mode information
        """
        pass
    
    @abc.abstractmethod
    async def stop_pending_info_get(self) -> Dict[str, Any]:
        """
        Get the stop pending information.
        
        Returns
        -------
        dict
            Stop pending information
        """
        pass
    
    @abc.abstractmethod
    async def stop_pending_info_set(self, stop_pending_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Set the stop pending information.
        
        Parameters
        ----------
        stop_pending_info : dict
            Stop pending information
            
        Returns
        -------
        dict
            Updated stop pending information
        """
        pass


    # Common utility methods
    
    async def _verify_item_type(self, item: Dict[str, Any]) -> None:
        """
        Verify that item is a dictionary.
        
        Parameters
        ----------
        item : dict
            Item to verify
            
        Raises
        ------
        TypeError
            If the item is not a dictionary
        """
        if not isinstance(item, dict):
            raise TypeError(f"Parameter 'item' should be a dictionary, got {type(item)}")
    
    async def _verify_item(self, item: Dict[str, Any], *, ignore_uids: Optional[List[str]] = None) -> None:
        """
        Verify that item structure is valid enough to be put in the queue.
        
        Parameters
        ----------
        item : dict
            Item to verify
        ignore_uids : list of str, optional
            List of UIDs to ignore when checking for duplicates
            
        Raises
        ------
        ValueError
            If the item is invalid
        RuntimeError
            If the item UID is already in the queue
        """
        # Verify item is a dictionary
        await self._verify_item_type(item)
        
        # Ensure ignore_uids is a list
        ignore_uids = ignore_uids or []
        
        # Check for UID
        if "item_uid" not in item:
            raise ValueError("Item does not have UID.")
        
        # Check for valid name
        if "name" not in item or not item["name"]:
            raise ValueError("Item must have a valid name")
            
        # Check if UID is already in the queue
        uid = item["item_uid"]
        if uid not in ignore_uids and self._is_uid_in_dict(uid):
            raise RuntimeError(f"Item with UID {uid} is already in the queue")
    
    async def set_new_item_uuid(self, item: Dict[str, Any]) -> Dict[str, Any]:
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
        # Create a deep copy to avoid modifying the original
        item_copy = copy.deepcopy(item)
        item_copy["item_uid"] = self.new_item_uid()
        return item_copy
    
    async def _clean_item_properties(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean the item properties.
        
        Parameters
        ----------
        item : dict
            Item to clean
            
        Returns
        -------
        dict
            Cleaned item
        """
        # Create a deep copy to avoid modifying the original
        cleaned_item = copy.deepcopy(item)
        
        # If there's a properties section, clean it
        if "properties" in cleaned_item:
            # Create a clean properties dictionary with only permitted fields
            allowed_properties = [
                "time_submit",
                "time_start",
                "immediate_execution"
            ]
            
            # Extract only allowed properties
            permitted_properties = {
                k: v for k, v in cleaned_item["properties"].items() 
                if k in allowed_properties
            }
            
            # Replace with cleaned properties
            cleaned_item["properties"] = permitted_properties
            
        return cleaned_item
    
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
        pq, _ = await self.get_queue()  # Changed from _get_queue to get_queue
        self._uid_dict_clear()
        # Go over all plans in the queue
        for item in pq:
            self._uid_dict_add(item)
        # If plan is currently running
        item = await self.get_running_item()  # Changed from _get_running_item_info
        if item:
            self._uid_dict_add(item)
