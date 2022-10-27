"""
This module handles server configuration.

See profiles.py for client configuration.
"""
import builtins
from collections.abc import Mapping
import os
from pathlib import Path
import jsonschema
import logging

from .config_schemas.loading import load_schema_from_yml, ConfigError


SERVICE_CONFIGURATION_FILE_NAME = "config_schema.yml"


def expand_environment_variables(config):
    """Expand environment variables in a nested config dictionary

    VENDORED FROM dask.config.

    This function will recursively search through any nested dictionaries
    and/or lists.

    Parameters
    ----------
    config : dict, iterable, or str
        Input object to search for environment variables

    Returns
    -------
    config : same type as input

    Examples
    --------
    >>> expand_environment_variables({'x': [1, 2, '$USER']})  # doctest: +SKIP
    {'x': [1, 2, 'my-username']}
    """
    if isinstance(config, Mapping):
        return {k: expand_environment_variables(v) for k, v in config.items()}
    elif isinstance(config, str):
        return os.path.expandvars(config)
    elif isinstance(config, (list, tuple, builtins.set)):
        return type(config)([expand_environment_variables(v) for v in config])
    else:
        return config


def parse(file):
    """
    Given a config file, parse it.

    This wraps YAML parsing and environment variable expansion.
    """
    import yaml

    content = yaml.safe_load(file.read())
    return expand_environment_variables(content)


def merge(configs):
    merged = {}

    # These variables are used to produce error messages that point
    # to the relevant config file(s).
    network_source = None
    startup_source = None
    operation_source = None
    run_engine_source = None

    for filepath, config in configs.items():
        if "network" in config:
            if "network" in merged:
                raise ConfigError(
                    "'network' can only be specified in one file. "
                    f"It was found in both {network_source} and "
                    f"{filepath}"
                )
            network_source = filepath
            merged["network"] = config["network"]
        if "startup" in config:
            if "startup" in merged:
                raise ConfigError(
                    "'startup' can only be specified in one file. "
                    f"It was found in both {startup_source} and "
                    f"{filepath}"
                )
            startup_source = filepath
            merged["startup"] = config["startup"]
        if "operation" in config:
            if "operation" in merged:
                raise ConfigError(
                    "'operation' can only be specified in one file. "
                    f"It was found in both {operation_source} and "
                    f"{filepath}"
                )
            operation_source = filepath
            merged["operation"] = config["operation"]
        if "run_engine" in config:
            if "run_engine" in merged:
                raise ConfigError(
                    "'run_engine' can only be specified in one file. "
                    f"It was found in both {run_engine_source} and "
                    f"{filepath}"
                )
            run_engine_source = filepath
            merged["run_engine"] = config["run_engine"]
    return merged


def parse_configs(config_path):
    """
    Parse configuration file or directory of configuration files.

    If a directory is given it is expected to contain only valid
    configuration files, except for the following which are ignored:

    * Hidden files or directories (starting with .)
    * Python scripts (ending in .py)
    * The __pycache__ directory
    """
    if isinstance(config_path, str):
        config_path = Path(config_path)
    if config_path.is_file():
        filepaths = [config_path]
    elif config_path.is_dir():
        filepaths = list(config_path.iterdir())
    elif not config_path.exists():
        raise ConfigError(f"The config path '{config_path!s}' doesn't exist.")
    else:
        assert False, "It should be impossible to reach this line."

    parsed_configs = {}
    # The sorting here is just to make the order of the results deterministic.
    # There is *not* any sorting-based precedence applied.
    for filepath in sorted(filepaths):
        # Ignore hidden files and .py files.
        if filepath.parts[-1].startswith(".") or filepath.suffix == ".py" or filepath.parts[-1] == "__pycache__":
            continue
        with open(filepath) as file:
            config = parse(file)
            try:
                jsonschema.validate(instance=config, schema=load_schema_from_yml(SERVICE_CONFIGURATION_FILE_NAME))
            except jsonschema.ValidationError as err:
                msg = err.args[0]
                raise ConfigError(f"ValidationError while parsing configuration file {filepath}: {msg}") from err
            parsed_configs[filepath] = config

    merged_config = merge(parsed_configs)
    return merged_config


_supported_keys = [
    "network/zmq_control_addr",
    "network/zmq_private_key",
    "network/zmq_info_addr",
    "network/zmq_publish_console",
    "network/redis_addr",
    "startup/keep_re",
    "startup/existing_plans_and_devices_path",
    "startup/user_group_permissions_path",
    "startup/startup_dir",
    "startup/startup_profile",
    "startup/startup_module",
    "startup/startup_script",
    "operation/print_console_output",
    "operation/console_output_level",
    "operation/existing_plans_and_devices_update",
    "operation/user_group_permissions_reload",
    "run_engine/user_persistent_metadata",
    "run_engine/kafka_server",
    "run_engine/kafka_topic",
    "run_engine/databroker_config",
]


def get_value_from_config(config, key, default=None):
    """
    Returns value from config dictionary. The keys is a sequence of keys separated with ``/``,
    e.g. ``"startup/keep_re"``. If the key is not in config, then the default value is returned.
    If the key is an empty string or does not exist, the ``ConfigError`` is raised.
    """
    if not key or key not in _supported_keys:
        raise ConfigError(f"The key {key!r} is not supported.")

    keys = key.split("/")

    try:
        value = config
        for k in keys:
            value = value[k]
    except KeyError:
        value = default

    return value


def get_log_level_from_config(config, cli_verbose, cli_quiet, cli_silent):
    """
    Select logging level based on config and CLI parameters. It is assumed that
    only one of cli parameters is true (not checked here). CLI parameters have
    precedence over config parameters.
    """
    value = None
    if cli_verbose:
        value = "VERBOSE"
    elif cli_quiet:
        value = "QUIET"
    elif cli_silent:
        value = "SILENT"

    # If 'value' is still None, then there is no CLI parameters. Use settings from config.
    value = get_value_from_config(config, "startup/keep_re", default=value)

    # If 'value' is still None, then use the default value.
    if value is None:
        value = "NORMAL"

    levels = {
        "VERBOSE": logging.DEBUG,
        "NORMAL": logging.INFO,
        "QUIET": logging.WARNING,
        "SILENT": logging.CRITICAL + 1,
    }

    if value not in levels:
        raise ConfigError(f"Unknown level: {value}. Supported levels: {list(levels.keys())}")

    return levels[value]
