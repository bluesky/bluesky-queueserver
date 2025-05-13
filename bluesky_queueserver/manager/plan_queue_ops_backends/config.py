"""
Configuration settings for Plan Queue Operation backends.

This module contains default configuration values for all supported backends.
These settings can be overridden by passing keyword arguments to the backend constructors.
"""

import os

# Common configuration shared by all backends
COMMON_CONFIG = {
    "name_prefix": "qs_default",  # Prefix for database tables/keys
}

# Redis backend configuration
REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0,
    "password": None,
    "name_prefix": COMMON_CONFIG["name_prefix"],
    "decode_responses": True,  # Essential for proper string handling
}

# SQLite backend configuration
SQLITE_CONFIG = {
    "database": os.path.join(os.getcwd(), "queue_storage.sqlite"),
    "timeout": 5.0,  # Connection timeout in seconds
    "detect_types": 0,
    "isolation_level": None,  # Autocommit mode
    "check_same_thread": False,  # Allow access from multiple threads
    "name_prefix": COMMON_CONFIG["name_prefix"],
}

# PostgreSQL backend configuration
POSTGRESQL_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "bluesky_queue",
    "user": "postgres",
    "password": "postgres",
    "min_size": 1,  # Minimum connection pool size
    "max_size": 10,  # Maximum connection pool size
    "timeout": 10.0,  # Connection timeout in seconds
    "name_prefix": COMMON_CONFIG["name_prefix"],
}

# Dict backend configuration (in-memory Python dictionary, optionally backed by file)
DICT_CONFIG = {
    "in_memory": False,  # Whether to use in-memory only or persist to file
    "storage_file": None,  # Path to storage file (if None, uses default based on name_prefix)
    "name_prefix": COMMON_CONFIG["name_prefix"],
    "autosave": True,  # Whether to automatically save changes to file
    "autosave_interval": 1.0,  # Seconds between autosaves (if autosave is enabled)
}

# Backend selection
DEFAULT_BACKEND = "redis"  # Default backend if not specified in environment

def get_backend_config(backend_name=None):
    """
    Get configuration for the specified backend.
    
    Parameters
    ----------
    backend_name : str, optional
        Name of the backend ('redis', 'sqlite', 'postgresql', 'dict')
        If None, get from PLAN_QUEUE_BACKEND environment variable or default
        
    Returns
    -------
    dict
        Configuration dictionary for the specified backend
        
    Raises
    ------
    ValueError
        If the backend name is not recognized
    """
    # Determine backend name
    if backend_name is None:
        backend_name = os.getenv("PLAN_QUEUE_BACKEND", DEFAULT_BACKEND).lower()
    
    # Return appropriate config
    if backend_name == "redis":
        return dict(REDIS_CONFIG)
    elif backend_name == "sqlite":
        return dict(SQLITE_CONFIG)
    elif backend_name == "postgresql":
        return dict(POSTGRESQL_CONFIG)
    elif backend_name == "dict":
        return dict(DICT_CONFIG)
    else:
        raise ValueError(f"Unknown backend: {backend_name}")