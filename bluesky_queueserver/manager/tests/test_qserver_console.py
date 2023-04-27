import subprocess

import pytest

from ..comms import zmq_single_request
from ..qserver_cli import QServerExitCodes
from .common import re_manager_cmd  # noqa: F401
from .common import (
    condition_environment_closed,
    condition_environment_created,
    use_ipykernel_for_tests,
    wait_for_condition,
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
