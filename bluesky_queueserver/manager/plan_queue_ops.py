import os
import uuid
from typing import Any

# Use the centralized backend selection mechanism
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

    def __getattr__(self, name):
        """
        Delegate attribute access to the backend instance.
        
        This allows us to call methods on the backend instance directly
        without needing to explicitly define them in this wrapper class.
        
        Parameters
        ----------
        name : str
            The name of the attribute or method to access.
            
        Returns
        -------
        Any
            The requested attribute or method from the backend instance.
        """
        return getattr(self._backend, name)
    
    async def start(self):
        """Initialize the backend connection."""
        await self._backend.start()

    async def stop(self):
        """Close the backend connection."""
        await self._backend.stop()