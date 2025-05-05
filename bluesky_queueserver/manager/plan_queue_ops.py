# import logging
import os

# logger = logging.getLogger(__name__)

class PlanQueueOperations:
    """
    Wrapper class for plan queue operations. Delegates method calls to the appropriate backend
    implementation (e.g., Redis, SQLite, or Dict) while keeping the interface consistent.
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
        """
        backend = os.getenv("PLAN_QUEUE_BACKEND", "redis").lower()

        if backend == "redis":
            from bluesky_queueserver.manager.plan_queue_ops_redis import RedisPlanQueueOperations
            self._backend = RedisPlanQueueOperations(**kwargs)
        elif backend == "sqlite":
            from bluesky_queueserver.manager.plan_queue_ops_sqlite import SQLitePlanQueueOperations
            self._backend = SQLitePlanQueueOperations(**kwargs)
        elif backend == "dict":
            from bluesky_queueserver.manager.plan_queue_ops_dict import DictPlanQueueOperations
            self._backend = DictPlanQueueOperations(**kwargs)
        else:
            raise ValueError(f"Unsupported backend: {backend}")

    def __getattr__(self, name):
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