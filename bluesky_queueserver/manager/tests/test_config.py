import os
import pytest

from ..config import parse_configs, ConfigError

fln_config_schema = "config_schema.yml"

config_01_success = """
network:
  zmq_control_addr: tcp://*:60615
  zmq_private_key: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  zmq_info_addr: tcp://*:60625
  zmq_publish_console: true
  redis_addr: localhost:6379
startup:
  keep_re: true
  startup_dir: ~/.ipython/profile_collection/startup
  existing_plans_and_devices_path: ~/.ipython/profile_collection/startup
  user_group_permissions_path: ~/.ipython/profile_collection/startup
operation:
  print_console_output: true
  console_logging_level: NORMAL
  update_existing_plans_and_devices: ENVIRONMENT_OPEN
  user_group_permissions_reload: ON_REQUEST
  emergency_lock_key: some_lock_key
run_engine:
  use_persistent_metadata: true
  kafka_server: 127.0.0.1:9092
  kafka_topic: topic_name
  zmq_data_proxy_addr: localhost:5567
  databroker_config: TST
"""

config_01a_success = """
network:
  zmq_control_addr: tcp://*:60615
  zmq_private_key: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  zmq_info_addr: tcp://*:60625
  zmq_publish_console: true
  redis_addr: localhost:6379
"""

config_01b_success = """
startup:
  keep_re: true
  startup_dir: ~/.ipython/profile_collection/startup
  existing_plans_and_devices_path: ~/.ipython/profile_collection/startup
  user_group_permissions_path: ~/.ipython/profile_collection/startup
"""

config_01c_success = """
operation:
  print_console_output: true
  console_logging_level: NORMAL
  update_existing_plans_and_devices: ENVIRONMENT_OPEN
  user_group_permissions_reload: ON_REQUEST
  emergency_lock_key: some_lock_key
"""

config_01d_success = """
run_engine:
  use_persistent_metadata: true
  kafka_server: 127.0.0.1:9092
  kafka_topic: topic_name
  zmq_data_proxy_addr: localhost:5567
  databroker_config: TST
"""

config_01_dict = {
    "network": {
        "zmq_control_addr": "tcp://*:60615",
        "zmq_private_key": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "zmq_info_addr": "tcp://*:60625",
        "zmq_publish_console": True,
        "redis_addr": "localhost:6379",
    },
    "startup": {
        "keep_re": True,
        "startup_dir": "~/.ipython/profile_collection/startup",
        "existing_plans_and_devices_path": "~/.ipython/profile_collection/startup",
        "user_group_permissions_path": "~/.ipython/profile_collection/startup",
    },
    "operation": {
        "print_console_output": True,
        "console_logging_level": "NORMAL",
        "update_existing_plans_and_devices": "ENVIRONMENT_OPEN",
        "user_group_permissions_reload": "ON_REQUEST",
        "emergency_lock_key": "some_lock_key",
    },
    "run_engine": {
        "use_persistent_metadata": True,
        "kafka_server": "127.0.0.1:9092",
        "kafka_topic": "topic_name",
        "zmq_data_proxy_addr": "localhost:5567",
        "databroker_config": "TST",
    },
}

config_02_success = """
startup:
  keep_re: true
"""

config_02_dict = {"startup": {"keep_re": True}}

# 'startup_dir' and 'startup_script' are mutually exclusive
config_03_fail = """
startup:
  startup_dir: ~/.ipython/profile_collection/startup
  startup_script: ~/.ipython/profile_collection/startup/startup.py
"""

config_04_fail = """
startup:
  console_logging_level: INVALID
"""

config_05_fail = """
startup:
  update_existing_plans_and_devices: INVALID
"""

config_06_fail = """
startup:
  user_group_permissions_reload: INVALID
"""


# fmt: off
@pytest.mark.parametrize("config_str, config_dict, success", [
    ([config_01_success], config_01_dict, True),
    ([config_02_success], config_02_dict, True),
    ([config_03_fail], None, False),
    ([config_04_fail], None, False),
    ([config_05_fail], None, False),
    ([config_06_fail], None, False),
    ([config_01a_success, config_01b_success, config_01c_success, config_01d_success],
     config_01_dict, True),
    ([config_01a_success, config_01a_success], None, False),
])
# fmt: on
def test_config_schema_01(tmpdir, config_str, config_dict, success):
    """
    parse_configs(): basic test
    """
    for n, s in enumerate(config_str):
        fln_path = os.path.join(tmpdir, f"config{n + 1}.yml")
        with open(fln_path, "w") as f:
            f.writelines(s)

    if len(config_str) > 1:
        config_path = str(tmpdir)
    else:
        config_path = os.path.join(tmpdir, "config1.yml")

    if success:
        config = parse_configs(config_path)
        assert config == config_dict
    else:
        with pytest.raises(ConfigError):
            parse_configs(config_path)


# fmt: off
@pytest.mark.parametrize("option", ["file", "empty_dir", "non_existing_dir"])
# fmt: on
def test_config_schema_02(tmpdir, option):
    """
    parse_configs(): test the cases of missing file, missing directory and empty
    directory. If the path is missing, the exception is raised. Empty directory
    results in empty dictionary.
    """
    if option == "empty_dir":
        config_path = str(tmpdir)
    elif option == "non_existing_dir":
        config_path = os.path.join(tmpdir, "some_dir")
    elif option == "file":
        config_path = os.path.join(tmpdir, "config1.yml")
    else:
        assert False, f"Unsupported option {option!r}"

    if option in ("non_existing_dir", "file"):
        errmsg = f"The config path '{config_path!s}' doesn't exist"
        with pytest.raises(ConfigError, match=errmsg):
            parse_configs(config_path)
    else:
        config = parse_configs(config_path)
        assert config == {}
