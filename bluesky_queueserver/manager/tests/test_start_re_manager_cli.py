import os
import pytest
import subprocess

from ..comms import zmq_single_request
from .common import re_manager_cmd  # noqa: F401

from .common import (
    wait_for_condition,
    condition_environment_created,
    condition_environment_closed,
    condition_queue_processing_finished,
    copy_default_profile_collection,
)

from bluesky_queueserver.manager.profile_ops import gen_list_of_plans_and_devices


# Plans used in most of the tests: '_plan1' and '_plan2' are quickly executed '_plan3' runs for 5 seconds.
_plan1 = {"name": "count", "args": [["det1", "det2"]], "item_type": "plan"}
_plan2 = {"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10], "item_type": "plan"}
_plan3 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 5, "delay": 1}, "item_type": "plan"}
_instruction_stop = {"name": "queue_stop", "item_type": "instruction"}

# User name and user group name used throughout most of the tests.
_user, _user_group = "Testing Script", "admin"


# fmt: off
@pytest.mark.parametrize("option", ["--verbose", "--quiet", "--silent"])
# fmt: on
def test_start_re_manager_logging_1(re_manager_cmd, option):  # noqa: F811
    """
    Test if RE Manager is correctly started with parameters that define logging verbosity.
    The test also creates the worker environment to make sure that the program does not crash
    when worker process is created.

    This is a smoke test: it does not verify that logging works.
    """
    re_manager_cmd([option])

    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert resp1["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_created)

    # Attemtp to communicate with RE Manager
    resp2, _ = zmq_single_request("status")
    assert resp2["items_in_queue"] == 0
    assert resp2["items_in_history"] == 0

    resp3, _ = zmq_single_request("environment_close")
    assert resp3["success"] is True
    assert resp3["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("console_print, console_zmq", [
    (None, None),
    (True, True),
    (True, False),
    (False, True),
    (False, False),
])
# fmt: on
def test_start_re_manager_console_output_1(re_manager_cmd, console_print, console_zmq):  # noqa: F811
    """
    Test for printing and publishing the console output (--console-output and --zmq-publish).
    """
    params = []
    if console_print is True:
        params.extend(["--console-output", "ON"])
    elif console_print is False:
        params.extend(["--console-output", "OFF"])
    if console_zmq is True:
        params.extend(["--zmq-publish-console", "ON"])
    elif console_zmq is False:
        params.extend(["--zmq-publish-console", "OFF"])

    # Default values (if parameters are not specified)
    if console_print is None:
        console_print = True
    if console_zmq is None:
        console_zmq = False

    # Start monitor (captures messages published to 0MQ)
    p_monitor = subprocess.Popen(
        ["qserver-console-monitor"], universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    re_manager = re_manager_cmd(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert resp1["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_created)

    resp2, _ = zmq_single_request("queue_item_add", {"item": _plan1, "user": _user, "user_group": _user_group})
    assert resp2["success"] is True

    resp3, _ = zmq_single_request("queue_start")
    assert resp3["success"] is True

    assert wait_for_condition(time=10, condition=condition_queue_processing_finished)

    resp3, _ = zmq_single_request("environment_close")
    assert resp3["success"] is True
    assert resp3["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    p = re_manager._p
    re_manager.stop_manager()
    re_manager_stdout, re_manager_stderr = p.communicate()

    p_monitor.terminate()
    streamed_stdout, streamed_stderr = p_monitor.communicate()

    def check_output_contents(collected_stdout):
        assert collected_stdout != ""
        # Verify that output from all sources is present in the output
        # Logging from manager
        assert "INFO:bluesky_queueserver.manager.manager:" in collected_stdout
        assert "INFO:bluesky_queueserver.manager.profile_ops:" in collected_stdout
        # Logging from Worker
        assert "INFO:bluesky_queueserver.manager.worker:RE Environment is ready" in collected_stdout
        # Printing from live table
        assert "generator count" in collected_stdout
        assert "Run was closed:" in collected_stdout

    assert re_manager_stderr == ""
    assert "INFO:bluesky_queueserver.manager.output_streaming:" in streamed_stderr

    if console_print:
        check_output_contents(re_manager_stdout)
    else:
        assert re_manager_stdout == ""

    if console_zmq:
        check_output_contents(streamed_stdout)
    else:
        assert streamed_stdout == ""


# fmt: off
@pytest.mark.parametrize("option", ["unchanged", "add_plan", "add_device", "add_plan_device"])
@pytest.mark.parametrize("update_existing_plans_devices", ["NEVER", "ENVIRONMENT_OPEN", "ALWAYS"])
# fmt: on
def test_cli_update_existing_plans_devices_01(
    re_manager_cmd, tmp_path, update_existing_plans_devices, option  # noqa: F811
):
    # Copy the default profile collection and generate the list of existing devices
    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=True)
    gen_list_of_plans_and_devices(
        startup_dir=pc_path, file_dir=pc_path, file_name="existing_plans_and_devices.yaml", overwrite=True
    )

    # Start the manager
    params = ["--startup-dir", pc_path, "--update-existing-plans-devices", update_existing_plans_devices]
    re_manager_cmd(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    resp1, _ = zmq_single_request("status")
    devices_allowed_uid1 = resp1["devices_allowed_uid"]
    plans_allowed_uid1 = resp1["plans_allowed_uid"]

    # Add a plan ('count50') and a device ('det50') if needed
    with open(os.path.join(pc_path, "zz.py"), "w") as f:
        if option in ("add_device", "add_plan_device"):
            f.writelines("det50 = det\n")
        if option in ("add_plan", "add_plan_device"):
            f.writelines("count50 = count\n")

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_created)

    resp3, _ = zmq_single_request("environment_close")
    assert resp3["success"] is True
    assert resp3["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    resp4, _ = zmq_single_request("status")
    devices_allowed_uid2 = resp4["devices_allowed_uid"]
    plans_allowed_uid2 = resp4["plans_allowed_uid"]

    resp5a, _ = zmq_single_request("plans_allowed", params={"user_group": _user_group})
    assert resp5a["success"] is True, f"resp={resp5a}"
    plans_allowed = resp5a["plans_allowed"]
    resp5b, _ = zmq_single_request("devices_allowed", params={"user_group": _user_group})
    assert resp5b["success"] is True, f"resp={resp5b}"
    devices_allowed = resp5b["devices_allowed"]

    if option in ("add_device", "add_plan_device"):
        assert devices_allowed_uid2 != devices_allowed_uid1
        assert "det50" in devices_allowed
    else:
        assert devices_allowed_uid2 == devices_allowed_uid1
        assert "det50" not in devices_allowed
    if option in ("add_plan", "add_plan_device"):
        assert plans_allowed_uid2 != plans_allowed_uid1
        assert "count50" in plans_allowed
    else:
        assert plans_allowed_uid2 == plans_allowed_uid1
        assert "count50" not in plans_allowed
