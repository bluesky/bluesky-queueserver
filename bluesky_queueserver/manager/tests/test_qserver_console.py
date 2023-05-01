import subprocess

import pytest

from bluesky_queueserver import generate_zmq_keys
from bluesky_queueserver.manager.comms import zmq_single_request

from ..qserver_cli import QServerExitCodes
from .common import re_manager_cmd  # noqa: F401
from .common import (
    condition_environment_closed,
    condition_environment_created,
    set_qserver_zmq_address,
    set_qserver_zmq_public_key,
    use_ipykernel_for_tests,
    wait_for_condition,
    zmq_secure_request,
)

timeout_env_open = 10


# fmt: off
@pytest.mark.parametrize("ipython_kernel_ip", ['localhost', 'auto'])
@pytest.mark.parametrize("env_open", [True, False])
# fmt: on
def test_cli_qserver_console_01(re_manager_cmd, ipython_kernel_ip, env_open):  # noqa: F811
    """
    'qserver-console' CLI tool: basic test

    This test verifies if the console is properly started when the environment is opened
    and IPython kernel is used, and fails to start with appropriate code if IPython is not used
    or the environment is not opened.

    Run the test both with and without IPython kernel.
    """
    using_ipython = bool(use_ipykernel_for_tests())

    # Start the manager
    params = []
    if ipython_kernel_ip is not None:
        params.extend([f"--ipython-kernel-ip={ipython_kernel_ip}"])
    re_manager_cmd(params)

    if env_open:
        resp2, _ = zmq_single_request("environment_open")
        assert resp2["success"] is True
        assert resp2["msg"] == ""

        assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    p = subprocess.Popen(
        ["qserver-console"],
        universal_newlines=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        p.wait(timeout=5)
        outs, errs = p.communicate()
    except subprocess.TimeoutExpired:
        outs, errs = p.communicate(input="quit(keep_kernel=True)\n")

    p.wait()
    output = outs + "\n" + errs
    return_code = p.returncode

    if not using_ipython or not env_open:
        assert return_code == QServerExitCodes.OPERATION_FAILED.value, output
        assert "Failed to start the console" in output, output
    else:
        assert return_code == QServerExitCodes.SUCCESS.value, output
        assert "Starting Jupyter Console ..." in output, output

    if env_open:
        resp9, _ = zmq_single_request("environment_close")
        assert resp9["success"] is True
        assert resp9["msg"] == ""

        assert wait_for_condition(time=3, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("zmq_control_address", [False, True])
@pytest.mark.parametrize("encryption_key", [False, True])
@pytest.mark.parametrize("use_env_var", [False, True])
# fmt: on
@pytest.mark.skipif(not use_ipykernel_for_tests(), reason="Test is run only with IPython worker")
def test_cli_qserver_console_02(
    monkeypatch, re_manager_cmd, zmq_control_address, encryption_key, use_env_var  # noqa: F811
):
    """
    'qserver-console' CLI tool: basic test

    Test --zmq-control-addr parameter of 'qserver-console'. Also test that the control address
    may be set using QSERVER_ZMQ_CONTROL_ADDRESS environment variable. Also test that
    the encryption public key is set using QSERVER_ZMQ_PUBLIC_KEY environment variable.

    Run the test only with IPython kernel.
    """
    using_ipython = bool(use_ipykernel_for_tests())
    if not using_ipython:
        assert False, "Test must be run only in IPython kernel mode"

    address_server = "tcp://*:60621"
    public_key, private_key = generate_zmq_keys()

    params_console = []

    params = []
    if zmq_control_address:
        params.append(f"--zmq-control-addr={address_server}")
        address_client = address_server.replace("*", "localhost")
        set_qserver_zmq_address(monkeypatch, zmq_server_address=address_client)

        if use_env_var:
            monkeypatch.setenv("QSERVER_ZMQ_CONTROL_ADDRESS", address_client)
        else:
            params_console.append(f"--zmq-control-addr={address_client}")

    if encryption_key:
        # # Set server public key (for 'qserver') using environment variable
        # monkeypatch.setenv("QSERVER_ZMQ_PUBLIC_KEY", public_key)
        # Set private key for RE manager
        monkeypatch.setenv("QSERVER_ZMQ_PRIVATE_KEY_FOR_SERVER", private_key)
        # Set public key used by test helper functions such as 'wait_for_condition'
        set_qserver_zmq_public_key(monkeypatch, server_public_key=public_key)

        monkeypatch.setenv("QSERVER_ZMQ_PUBLIC_KEY", public_key)

    re_manager_cmd(params)

    resp2, _ = zmq_secure_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    p = subprocess.Popen(
        ["qserver-console", *params_console],
        universal_newlines=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        p.wait(timeout=5)
        outs, errs = p.communicate()
    except subprocess.TimeoutExpired:
        outs, errs = p.communicate(input="quit(keep_kernel=True)\n")

    p.wait()
    output = outs + "\n" + errs
    return_code = p.returncode

    assert return_code == QServerExitCodes.SUCCESS.value, output
    assert "Starting Jupyter Console ..." in output, output

    resp9, _ = zmq_secure_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)
