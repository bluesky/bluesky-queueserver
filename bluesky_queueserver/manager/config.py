"""
This module handles server configuration.

See profiles.py for client configuration.
"""
import builtins
import copy
from collections.abc import Mapping
from importlib.util import find_spec
import os
from pathlib import Path
import jsonschema
import logging
import sys
import yaml


from .output_streaming import default_zmq_info_address_for_server
from .config_schemas.loading import load_schema_from_yml, ConfigError
from .profile_ops import get_default_startup_dir
from .comms import validate_zmq_key, default_zmq_control_address_for_server

logger = logging.getLogger(__name__)


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


_key_mapping = {
    "zmq_control_addr": "network/zmq_control_addr",
    "zmq_private_key": "network/zmq_private_key",
    "zmq_info_addr": "network/zmq_info_addr",
    "zmq_publish_console": "network/zmq_publish_console",
    "redis_addr": "network/redis_addr",
    "keep_re": "startup/keep_re",
    "existing_plans_and_devices_path": "startup/existing_plans_and_devices_path",
    "user_group_permissions_path": "startup/user_group_permissions_path",
    "startup_dir": "startup/startup_dir",
    "startup_profile": "startup/startup_profile",
    "startup_module": "startup/startup_module",
    "startup_script": "startup/startup_script",
    "print_console_output": "operation/print_console_output",
    "console_logging_level": "operation/console_logging_level",
    "update_existing_plans_devices": "operation/update_existing_plans_and_devices",
    "user_group_permissions_reload": "operation/user_group_permissions_reload",
    "emergency_lock_key": "operation/emergency_lock_key",
    "use_persistent_metadata": "run_engine/use_persistent_metadata",
    "kafka_server": "run_engine/kafka_server",
    "kafka_topic": "run_engine/kafka_topic",
    "zmq_data_proxy_addr": "run_engine/zmq_data_proxy_addr",
    "databroker_config": "run_engine/databroker_config",
}


class _ArgsExisting:
    """
    The object should be used together with ``ArugmentParser``. The call method
    returns the parameter value if the parameter was actually passed in the command
    line and the default values.

    Parameters
    ----------
    parser: ArgumentParser
        The parser object used for parsing the list of CLI parameters
    args
        Namespace returned by ``parser.parse_args()``. If ``None``, then
        the constructor call ``parse_args()`` to parse current parameters.
    """

    def __init__(self, *, parser, args=None):
        self._parser = parser
        self._args = args or parser.parse_args()
        self._existing_params = self._get_existing_cli_params()

    def _get_existing_cli_params(self):
        """
        Returns mapping: parameter_name -> True/False (exist in command line, or
        default value is used).
        """
        key_mapping = {_.dest: _.option_strings for _ in self._parser._actions}
        key_specified = {}

        # We need to recognize two cases: ``--zmq-info-addr tcp://*:60621`` and
        #   ``--zmq-info-addr=tcp://*:60621``
        sys_argsv = [_.split("=")[0] for _ in sys.argv[1:] if _.startswith("-")]
        for k, v in key_mapping.items():
            key_specified[k] = any([_ in sys_argsv for _ in v])

        return key_specified

    def __call__(self, param_name, *, default=None):
        """
        Parameters
        ----------
        param_name: str
            Parameter name. If the parameter name is non-existing, then ``KeyError`` is raised.
        default: object
            The default value, which is returned if the parameter is not set in the CLI parameters.
        """
        if param_name not in self._existing_params:
            raise KeyError(f"CLI parameter {param_name!r} does not exist")
        if self._existing_params[param_name] is False:
            return default
        else:
            return getattr(self._args, param_name)


def to_boolean(value):
    """
    Returns ``True`` or ``False`` if ``value`` is found in one of the lists of supported values.
    Otherwise returns ``None`` (typicall means that the value is not set).
    """
    v = value.lower() if isinstance(value, str) else value
    if v in (True, "y", "yes", "t", "true", "on", "1"):
        return True
    elif v in (False, "", "n", "no", "f", "false", "off", "0"):
        return False
    else:
        return None


def _profile_name_to_startup_dir(profile_name):
    """
    Finds and returns full path to startup directory based on the profile name.
    """
    if find_spec("IPython"):
        import IPython

        path_to_ipython = IPython.paths.get_ipython_dir()
    else:
        raise ConfigError("IPython is not installed. Specify directory using CLI parameters or in config file.")
        return 1
    ipython_dir = os.path.abspath(path_to_ipython)
    profile_name_full = f"profile_{profile_name}"
    return os.path.join(ipython_dir, profile_name_full, "startup")


class Settings:
    def __init__(self, *, parser, args):
        self._parser = parser
        self._args = args
        self._args_existing = _ArgsExisting(parser=parser, args=args)
        self._settings = {}

        config_path = args.config_path
        config_path = config_path or os.environ.get("QSERVER_CONFIG", None)
        self._config = parse_configs(config_path) if config_path else {}

        self._settings["zmq_control_addr"] = self._get_zmq_control_addr()
        self._settings["zmq_private_key"] = self._get_zmq_private_key()

        self._settings["zmq_info_addr"] = self._get_param(
            value_default=default_zmq_info_address_for_server,
            value_ev=os.environ.get("QSERVER_ZMQ_INFO_ADDRESS_FOR_SERVER", None),
            value_config=self._get_value_from_config("zmq_info_addr"),
            value_cli=self._args_existing("zmq_info_addr") or self._args_existing("zmq_publish_console_addr"),
        )

        self._settings["zmq_publish_console"] = self._get_param_boolean(
            value_default=args.zmq_publish_console,
            value_config=self._get_value_from_config("zmq_publish_console"),
            value_cli=self._args_existing("zmq_publish_console"),
        )

        redis_addr = self._get_param(
            value_default=self._args.redis_addr,
            value_config=self._get_value_from_config("redis_addr"),
            value_cli=self._args_existing("redis_addr"),
        )
        if redis_addr.count(":") > 1:
            raise ConfigError(f"Redis address is incorrectly formatted: {redis_addr}")
        self._settings["redis_addr"] = redis_addr

        self._settings["keep_re"] = self._get_param_boolean(
            value_default=args.keep_re,
            value_config=self._get_value_from_config("keep_re"),
            value_cli=self._args_existing("keep_re"),
        )

        existing_plans_and_devices_path = self._get_param(
            value_default=args.existing_plans_and_devices_path,
            value_config=self._get_value_from_config("existing_plans_and_devices_path"),
            value_cli=self._args_existing("existing_plans_and_devices_path"),
        )
        if isinstance(existing_plans_and_devices_path, str):
            existing_plans_and_devices_path = os.path.expanduser(existing_plans_and_devices_path)
        self._settings["existing_plans_and_devices_path"] = existing_plans_and_devices_path

        user_group_permissions_path = self._get_param(
            value_default=args.user_group_permissions_path,
            value_config=self._get_value_from_config("user_group_permissions_path"),
            value_cli=self._args_existing("user_group_permissions_path"),
        )
        if isinstance(user_group_permissions_path, str):
            user_group_permissions_path = os.path.expanduser(user_group_permissions_path)
        self._settings["user_group_permissions_path"] = user_group_permissions_path

        startup_dir, startup_module, startup_script = self._get_startup_options()
        self._settings["startup_dir"] = startup_dir
        self._settings["startup_module"] = startup_module
        self._settings["startup_script"] = startup_script

        self._settings["print_console_output"] = self._get_param_boolean(
            value_default=args.console_output,
            value_config=self._get_value_from_config("print_console_output"),
            value_cli=self._args_existing("console_output"),
        )

        self._settings["console_logging_level"] = self._get_console_logging_level()

        self._settings["update_existing_plans_devices"] = self._get_param(
            value_default=args.update_existing_plans_devices,
            value_config=self._get_value_from_config("update_existing_plans_devices"),
            value_cli=self._args_existing("update_existing_plans_devices"),
        )

        self._settings["user_group_permissions_reload"] = self._get_param(
            value_default=args.user_group_permissions_reload,
            value_config=self._get_value_from_config("user_group_permissions_reload"),
            value_cli=self._args_existing("user_group_permissions_reload"),
        )

        self._settings["emergency_lock_key"] = self._get_param(
            value_ev=os.environ.get("QSERVER_EMERGENCY_LOCK_KEY_FOR_SERVER", None),
            value_config=self._get_value_from_config("emergency_lock_key"),
        )

        self._settings["use_persistent_metadata"] = self._get_param_boolean(
            value_default=args.use_persistent_metadata,
            value_config=self._get_value_from_config("use_persistent_metadata"),
            value_cli=self._args_existing("use_persistent_metadata"),
        )

        self._settings["kafka_server"] = self._get_param(
            value_default=args.kafka_server,
            value_config=self._get_value_from_config("kafka_server"),
            value_cli=self._args_existing("kafka_server"),
        )

        self._settings["kafka_topic"] = self._get_param(
            value_default=args.kafka_topic,
            value_config=self._get_value_from_config("kafka_topic"),
            value_cli=self._args_existing("kafka_topic"),
        )

        self._settings["zmq_data_proxy_addr"] = self._get_param(
            value_default=args.zmq_data_proxy_addr,
            value_config=self._get_value_from_config("zmq_data_proxy_addr"),
            value_cli=self._args_existing("zmq_data_proxy_addr"),
        )

        self._settings["databroker_config"] = self._get_param(
            value_default=args.databroker_config,
            value_config=self._get_value_from_config("databroker_config"),
            value_cli=self._args_existing("databroker_config"),
        )

    def __getattr__(self, attr):
        if attr in self._settings:
            return self._settings[attr]
        raise AttributeError(f"{self.__class__.__name__!r} object has no attribute {attr!r}")

    def to_dict(self):
        """Return a copy of the dictionary with settings, which is safe to modify."""
        return copy.deepcopy(self._settings)

    def __str__(self):
        settings_str = ""
        for k, v in self._settings.items():
            if k == "zmq_private_key":
                v = None if (self.zmq_private_key is None) else "*******"
            settings_str += f"    {k}: {v!r}\n"
        return settings_str

    def __repr__(self):
        return self.__str__()

    def _get_value_from_config(self, key, default=None):
        """
        Returns value from config dictionary. The keys must be one of the keys defined in
        ``_key_mapping``. If the value not found in config, then the ``default`` value is returned.
        If the key is an empty string or does not exist, the ``ConfigError`` is raised.
        """
        if not key or key not in _key_mapping:
            raise ConfigError(f"The key {key!r} is not supported.")

        keys = _key_mapping[key].split("/")

        try:
            value = self._config
            for k in keys:
                value = value[k]
        except KeyError:
            value = default

        return value

    def _get_param(self, *, value_default=None, value_ev=None, value_config=None, value_cli=None):
        """
        ``None`` - the value is not set
        """
        v = value_ev if (value_ev is not None) else value_default
        v = value_config if (value_config is not None) else v
        return value_cli if (value_cli is not None) else v

    def _get_param_boolean(self, *, value_default=None, value_ev=None, value_config=None, value_cli=None):
        """
        Returns ``True/False/None`` based on the input values. ``None`` - the value is not set.
        """
        value_default = to_boolean(value_default)
        value_ev = to_boolean(value_ev)
        value_config = to_boolean(value_config)
        value_cli = to_boolean(value_cli)

        return self._get_param(
            value_default=value_default, value_ev=value_ev, value_config=value_config, value_cli=value_cli
        )

    def _get_console_logging_level(self):
        """
        Select logging level based on config and CLI parameters. It is assumed that
        only one of cli parameters is true (not checked here). CLI parameters have
        precedence over config parameters.
        """

        def get_cli_log_level():
            cli_verbose = self._args_existing("logger_verbose")
            cli_quiet = self._args_existing("logger_quiet")
            cli_silent = self._args_existing("logger_silent")

            # Value from CLI parameters
            v = None
            if cli_verbose:
                v = "VERBOSE"
            elif cli_quiet:
                v = "QUIET"
            elif cli_silent:
                v = "SILENT"

            return v

        value_default = "NORMAL"
        value_config = self._get_value_from_config("console_logging_level")
        value_cli = get_cli_log_level()
        value = self._get_param(value_default=value_default, value_config=value_config, value_cli=value_cli)

        levels = {
            "VERBOSE": logging.DEBUG,
            "NORMAL": logging.INFO,
            "QUIET": logging.WARNING,
            "SILENT": logging.CRITICAL + 1,
        }

        if value not in levels:
            raise ConfigError(f"Unknown level: {value}. Supported levels: {list(levels.keys())}")

        return levels[value]

    def _get_startup_options(self):
        """
        Returns names of startup_dir, startup_module or startup_script. Only one of the name can be not None.
        """

        # Default: startup scripts with simulated plans and devices
        startup_dir, startup_module, startup_script = get_default_startup_dir(), None, None

        # Process config parameters
        cfg_dir, cfg_module, cfg_script = None, None, None
        if self._get_value_from_config("startup_profile"):
            cfg_dir = _profile_name_to_startup_dir(self._get_value_from_config("startup_profile"))
        elif self._get_value_from_config("startup_dir"):
            cfg_dir = self._get_value_from_config("startup_dir")
            cfg_dir = os.path.abspath(os.path.expanduser(cfg_dir))
        elif self._get_value_from_config("startup_module"):
            cfg_module = self._get_value_from_config("startup_module")
        elif self._get_value_from_config("startup_script"):
            cfg_script = os.path.abspath(os.path.expanduser(self._get_value_from_config("startup_script")))

        if any([cfg_dir, cfg_module, cfg_script]):
            startup_dir, startup_module, startup_script = cfg_dir, cfg_module, cfg_script

        # Process CLI parameters
        cli_dir, cli_module, cli_script = None, None, None
        if self._args_existing("profile_name"):
            cli_dir = _profile_name_to_startup_dir(self._args_existing("profile_name"))
        elif self._args_existing("startup_dir"):
            cli_dir = self._args_existing("startup_dir")
            cli_dir = os.path.abspath(os.path.expanduser(cli_dir))
        elif self._args_existing("startup_module_name"):
            cli_module = self._args_existing("startup_module_name")
        elif self._args_existing("startup_script_path"):
            cli_script = os.path.abspath(os.path.expanduser(self._args_existing("startup_script_path")))

        if any([cli_dir, cli_module, cli_script]):
            startup_dir, startup_module, startup_script = cli_dir, cli_module, cli_script

        # Check that only one source is defined (just in case)
        if sum([_ is not None for _ in [startup_dir, startup_module, startup_script]]) != 1:
            raise ConfigError(
                f"Multiple or no startup code sources were specified: startup_dir={startup_dir!r} "
                f"startup_module={startup_module!r} startup_script={startup_script!r}"
            )

        return startup_dir, startup_module, startup_script

    def _get_zmq_control_addr(self):
        """
        Returns 0MQ control address (string).
        """
        zmq_control_addr_cli = self._args.zmq_control_addr
        if self._args.zmq_addr is not None:
            logger.warning(
                "Parameter --zmq-addr is deprecated and will be removed in future releases. "
                "Use --zmq-control-addr instead."
            )
        zmq_control_addr_cli = zmq_control_addr_cli or self._args.zmq_addr

        zmq_control_addr = self._get_param(
            value_default=default_zmq_control_address_for_server,
            value_ev=os.environ.get("QSERVER_ZMQ_CONTROL_ADDRESS_FOR_SERVER"),
            value_config=self._get_value_from_config("zmq_control_addr"),
            value_cli=zmq_control_addr_cli,
        )

        return zmq_control_addr

    def _get_zmq_private_key(self):
        """
        Returns 0MQ private key (string) or None.
        """
        # Read private key from the environment variable, then check if the CLI parameter exists
        zmq_private_key_ev = os.environ.get("QSERVER_ZMQ_PRIVATE_KEY_FOR_SERVER", None)
        if (zmq_private_key_ev is None) and ("QSERVER_ZMQ_PRIVATE_KEY" in os.environ):
            logger.warning(
                "Environment variable QSERVER_ZMQ_PRIVATE_KEY is deprecated and will be removed "
                "in future releases. Use QSERVER_ZMQ_PRIVATE_KEY_FOR_SERVER instead"
            )
        zmq_private_key_ev = zmq_private_key_ev or os.environ.get("QSERVER_ZMQ_PRIVATE_KEY", None)
        zmq_private_key_ev = zmq_private_key_ev or None  # Case of key==""

        zmq_private_key = self._get_param(
            value_ev=zmq_private_key_ev, value_config=self._get_value_from_config("zmq_private_key")
        )

        if zmq_private_key is not None:
            try:
                validate_zmq_key(zmq_private_key)
            except Exception as ex:
                raise ConfigError("ZMQ private key is improperly formatted: %s", ex) from ex

        return zmq_private_key


def save_settings_to_file(settings):
    """
    Save RE Manager current setting to a YAML file. The file path is passed using
    ``QSERVER_SETTINGS_SAVE_TO_FILE``. Error message is printed if the operation can not
    be completed. Used in automated testing.
    """
    # Save current settings to file. This feature is mostly for testing.
    save_config_path = os.environ.get("QSERVER_SETTINGS_SAVE_TO_FILE", None)
    if save_config_path and isinstance(save_config_path, str):
        try:
            save_config_path = os.path.abspath(os.path.expanduser(save_config_path))
            with open(save_config_path, "w") as f:
                yaml.dump(settings.to_dict(), f)
            logger.info("Current settings are saved to file %r", save_config_path)
        except Exception as ex:
            logger.error("Failed to save current settings to file %r: %s", save_config_path, ex)
