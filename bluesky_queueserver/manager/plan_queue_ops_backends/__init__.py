"""
Plan Queue Operations Backend Implementations.

This package contains the various backend implementations for the Plan Queue Operations:
- Redis: Uses Redis as the backend storage
- SQLite: Uses SQLite database as the backend storage 
- PostgreSQL: Uses PostgreSQL database as the backend storage
- Dict: Uses an in-memory dictionary (optionally persisted to file) as the backend storage
"""
import os

# Dictionary mapping backend names to their import paths
_BACKENDS = {
    "abstract": ".plan_queue_ops_abstract.AbstractPlanQueueOperations",
    "redis": ".plan_queue_ops_redis.RedisPlanQueueOperations",
    "sqlite": ".plan_queue_ops_sqlite.SQLitePlanQueueOperations",
    "dict": ".plan_queue_ops_dict.DictPlanQueueOperations",
    "postgresql": ".plan_queue_ops_postgresql.PostgreSQLPlanQueueOperations",
}

def get_backend(backend_name=None):
    """
    Dynamically import and return the requested backend class.
    
    Parameters
    ----------
    backend_name : str, optional
        Name of the backend ('redis', 'sqlite', 'dict', 'postgresql')
        If None, will look for PLAN_QUEUE_BACKEND environment variable,
        and default to 'redis' if not found.
        
    Returns
    -------
    class
        The requested backend class
        
    Raises
    ------
    ValueError
        If the backend name is not recognized
    ImportError
        If the backend cannot be imported (e.g., missing dependencies)
    """
    # If no backend specified, check environment variable
    if backend_name is None:
        backend_name = os.getenv("PLAN_QUEUE_BACKEND", "redis").lower()
    
    if backend_name not in _BACKENDS:
        raise ValueError(f"Unknown backend: {backend_name}. Available backends: {list(_BACKENDS.keys())}")
        
    import importlib
    module_path, class_name = _BACKENDS[backend_name].rsplit('.', 1)
    module = importlib.import_module(module_path, package=__package__)
    return getattr(module, class_name)

def get_default_backend():
    """
    Get the default backend class based on PLAN_QUEUE_BACKEND environment variable.
    Defaults to Redis if no environment variable is set.
    
    Returns
    -------
    class
        The default backend class
    """
    return get_backend()

# For compatibility with old code that expects direct imports
def __getattr__(name):
    """
    Support for direct attribute access like:
    from plan_queue_ops_backends import RedisPlanQueueOperations
    """
    # Map class names to backend names
    class_to_backend = {
        "AbstractPlanQueueOperations": "abstract",
        "RedisPlanQueueOperations": "redis",
        "SQLitePlanQueueOperations": "sqlite",
        "DictPlanQueueOperations": "dict",
        "PostgreSQLPlanQueueOperations": "postgresql",
    }
    
    if name in class_to_backend:
        try:
            return get_backend(class_to_backend[name])
        except ImportError:
            raise ImportError(f"Could not import {name} - backend dependencies may be missing")
    
    raise AttributeError(f"module {__name__} has no attribute {name}")