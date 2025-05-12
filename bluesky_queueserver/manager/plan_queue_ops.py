import os
import uuid
from typing import Any

from bluesky_queueserver.manager.plan_queue_ops_backends import get_default_backend

class PlanQueueOperations:
    """
    Wrapper class for plan queue operations. Delegates method calls to the appropriate backend
    implementation (Redis, SQLite, PostgreSQL, or Dict) while keeping the interface consistent.
    
    The backend selection is controlled through the PLAN_QUEUE_BACKEND environment variable,
    which can be set to 'redis', 'sqlite', 'postgresql', or 'dict'.
    """

    def __init__(self, **kwargs):
        """
        Initialize the PlanQueueOperations class with the specified backend.

        The backend is determined by the environment variable `PLAN_QUEUE_BACKEND`.
        If the variable is not defined, the default backend is Redis.

        Parameters
        ----------
        kwargs : dict
            Additional arguments to pass to the backend implementation.
            [...]
        """
        # Get the appropriate backend class from the centralized mechanism
        BackendClass = get_default_backend()
        self._backend = BackendClass(**kwargs)
        
    def __getattr__(self, name: str) -> Any:
        """
        Delegate attribute access to the backend implementation.

        This allows method calls to be forwarded to the backend without explicitly defining
        them in this wrapper class.

        Parameters
        ----------
        name : str
            The name of the attribute or method being accessed.

        Returns
        -------
        Any
            The attribute or method from the backend implementation.
        """
        return getattr(self._backend, name)
    
    @staticmethod
    def new_item_uid() -> str:
        """
        Generate a new unique identifier for an item.

        Returns
        -------
        str
            A new unique identifier.
        """
        return str(uuid.uuid4())
    
    @classmethod
    def get_available_backends(cls) -> list:
        """
        Return a list of available backend implementations.
        
        Returns
        -------
        list
            List of available backend names
        """
        return ["redis", "sqlite", "postgresql", "dict"]
    
    @property
    def backend_name(self) -> str:
        """
        Return the name of the currently active backend.
        
        Returns
        -------
        str
            Name of the active backend implementation
        """
        return self._backend.__class__.__name__.replace("PlanQueueOperations", "").lower()