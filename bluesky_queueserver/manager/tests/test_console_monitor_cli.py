import subprocess

import pytest

from ..comms import zmq_single_request
from .common import re_manager_cmd  # noqa: F401
from .common import condition_environment_closed, condition_environment_created, wait_for_condition

timeout_env_open = 10


# fmt: off
@pytest.mark.parametrize("test_mode", ["none", "parameter", "env_var", "both_success", "both_fail"])
# fmt: on
def test_console_monitor_cli_parameters_1(monkeypatch, re_manager_cmd, test_mode):  # noqa: F811
    """
    ``qserver-console-monitor``: Check that passing server info address as a parameter
    ``--zmq-control-addr`` and environment variable ``QSERVER_ZMQ_INFO_ADDRESS`` works as expected.
    """
    address_info_server = "tcp://*:60621"
    address_info_client = "tcp://localhost:60621"
    address_info_client_incorrect = "tcp://localhost:60622"

    params_server = ["--zmq-publish-console=ON", f"--zmq-info-addr={address_info_server}"]
    params_client = []
    if test_mode == "none":
        # Use default address, communication fails
        success = False
    elif test_mode == "parameter":
        # Pass the address as a parameter
        success = True
        params_client.append(f"--zmq-info-addr={address_info_client}")
    elif test_mode == "env_var":
        # Pass the address as an environment variable
        success = True
        monkeypatch.setenv("QSERVER_ZMQ_INFO_ADDRESS", address_info_client)
    elif test_mode == "both_success":
        # Pass the correct address as a parameter and incorrect as environment variable (ignored)
        success = True
        params_client.append(f"--zmq-info-addr={address_info_client}")
        monkeypatch.setenv("QSERVER_ZMQ_INFO_ADDRESS", address_info_client_incorrect)
    elif test_mode == "both_fail":
        # Pass incorrect address as an environment variable (ignored) and correct address as a parameter
        success = False
        params_client.append(f"--zmq-info-addr={address_info_client_incorrect}")
        monkeypatch.setenv("QSERVER_ZMQ_INFO_ADDRESS", address_info_client)
    else:
        raise RuntimeError(f"Unrecognized test mode '{test_mode}'")

    re_manager_cmd(params_server)

    # Start monitor (captures messages published to 0MQ)
    p_monitor = subprocess.Popen(
        ["qserver-console-monitor", *params_client],
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    zmq_single_request("environment_open")
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)
    zmq_single_request("environment_close")
    assert wait_for_condition(time=3, condition=condition_environment_closed)

    p_monitor.terminate()
    streamed_stdout, streamed_stderr = p_monitor.communicate()

    if success:
        # assert streamed_stdout != ""
        assert "RE Environment is ready" in streamed_stdout
    else:
        # assert streamed_stdout == ""
        assert "RE Environment is ready" not in streamed_stdout
