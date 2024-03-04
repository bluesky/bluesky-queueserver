import os
import pprint
import subprocess
import time as ttime

import pytest
import yaml

from bluesky_queueserver import gen_list_of_plans_and_devices
from bluesky_queueserver.manager.comms import generate_zmq_keys

from ..qserver_cli import QServerExitCodes
from .common import (  # noqa: F401
    _test_redis_name_prefix,
    append_code_to_last_startup_file,
    condition_environment_closed,
    condition_environment_created,
    condition_ip_kernel_idle,
    condition_manager_idle,
    condition_manager_idle_or_paused,
    condition_manager_paused,
    condition_queue_processing_finished,
    get_manager_status,
    get_queue,
    get_queue_state,
    ip_kernel_simple_client,
    patch_first_startup_file,
    patch_first_startup_file_undo,
    re_manager,
    re_manager_cmd,
    re_manager_pc_copy,
    set_qserver_zmq_address,
    set_qserver_zmq_public_key,
    use_ipykernel_for_tests,
    wait_for_condition,
    zmq_single_request,
)

# Exit codes for CLI tool
SUCCESS = QServerExitCodes.SUCCESS.value
PARAM_ERROR = QServerExitCodes.PARAMETER_ERROR.value
REQ_FAILED = QServerExitCodes.REQUEST_FAILED.value
COM_ERROR = QServerExitCodes.COMMUNICATION_ERROR.value
EXCEPTION_OCCURRED = QServerExitCodes.EXCEPTION_OCCURRED.value

timeout_env_open = 10


def test_qserver_cli_and_manager(re_manager):  # noqa: F811
    """
    Long test runs a series of CLI commands.
    """
    assert wait_for_condition(
        time=3, condition=condition_manager_idle
    ), "Timeout while waiting for manager to initialize."

    # Clear queue
    assert subprocess.call(["qserver", "queue", "clear"]) == SUCCESS

    # Request the list of allowed plans and devices (we don't check what is returned)
    assert subprocess.call(["qserver", "allowed", "plans"], stdout=subprocess.DEVNULL) == SUCCESS
    assert subprocess.call(["qserver", "allowed", "devices"], stdout=subprocess.DEVNULL) == SUCCESS

    # Request the list of existing plans and devices (we don't check what is returned)
    assert subprocess.call(["qserver", "existing", "plans"], stdout=subprocess.DEVNULL) == SUCCESS
    assert subprocess.call(["qserver", "existing", "devices"], stdout=subprocess.DEVNULL) == SUCCESS

    # Add a number of plans
    plan_1 = "{'name':'count', 'args':[['det1', 'det2']]}"
    plan_2 = "{'name':'scan', 'args':[['det1', 'det2'], 'motor', -1, 1, 10]}"
    plan_3 = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':10, 'delay':1}}"
    assert subprocess.call(["qserver", "queue", "add", "plan", plan_1]) == SUCCESS
    assert subprocess.call(["qserver", "queue", "add", "plan", plan_2]) == SUCCESS
    assert subprocess.call(["qserver", "queue", "add", "plan", plan_3]) == SUCCESS

    n_plans, is_plan_running, _ = get_queue_state()
    assert n_plans == 3, "Incorrect number of plans in the queue"
    assert not is_plan_running, "Plan is executed while it shouldn't"

    assert subprocess.call(["qserver", "queue", "get"]) == SUCCESS
    assert subprocess.call(["qserver", "queue", "item", "remove"]) == SUCCESS

    n_plans, is_plan_running, _ = get_queue_state()
    assert n_plans == 2, "Incorrect number of plans in the queue"

    assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS

    assert wait_for_condition(time=60, condition=condition_queue_processing_finished)

    # Smoke test for 'history_get' and 'history_clear'
    assert subprocess.call(["qserver", "history", "get"]) == SUCCESS
    assert subprocess.call(["qserver", "history", "clear"]) == SUCCESS

    # Queue is expected to be empty (processed). Load one more plan.
    assert subprocess.call(["qserver", "queue", "add", "plan", plan_3]) == SUCCESS

    n_plans, is_plan_running, _ = get_queue_state()
    assert n_plans == 1, "Incorrect number of plans in the queue"

    assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS
    ttime.sleep(1)
    assert subprocess.call(["qserver", "re", "pause", "immediate"]) == SUCCESS
    assert wait_for_condition(
        time=60, condition=condition_manager_paused
    ), "Timeout while waiting for manager to pause"

    assert subprocess.call(["qserver", "re", "resume"]) == SUCCESS
    ttime.sleep(1)
    assert subprocess.call(["qserver", "re", "pause", "deferred"]) == SUCCESS
    assert wait_for_condition(
        time=60, condition=condition_manager_paused
    ), "Timeout while waiting for manager to pause"

    assert subprocess.call(["qserver", "re", "resume"]) == SUCCESS

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    assert subprocess.call(["qserver", "queue", "add", "plan", plan_1]) == SUCCESS
    assert subprocess.call(["qserver", "queue", "add", "plan", plan_1]) == SUCCESS

    n_plans, is_plan_running, _ = get_queue_state()
    assert n_plans == 2, "Incorrect number of plans in the queue"

    assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    # Test 'killing' the manager during running plan. Load long plan and two short ones.
    #   The tests checks if execution of the queue is continued uninterrupted after
    #   the manager restart
    assert subprocess.call(["qserver", "queue", "add", "plan", plan_3]) == SUCCESS
    assert subprocess.call(["qserver", "queue", "add", "plan", plan_3]) == SUCCESS
    assert subprocess.call(["qserver", "queue", "add", "plan", plan_3]) == SUCCESS
    n_plans, is_plan_running, _ = get_queue_state()
    assert n_plans == 3, "Incorrect number of plans in the queue"

    assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS
    ttime.sleep(1)
    assert subprocess.call(["qserver", "manager", "kill", "test"]) != SUCCESS
    ttime.sleep(6)  # Don't request the condition to avoid timeout error TODO: wait for the server
    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    assert subprocess.call(["qserver", "environment", "close"]) == SUCCESS
    assert wait_for_condition(
        time=5, condition=condition_environment_closed
    ), "Timeout while waiting for environment to be closed"


def test_qserver_environment_close(re_manager):  # noqa: F811
    """
    Test for `environment_close` command
    """
    assert wait_for_condition(
        time=3, condition=condition_manager_idle
    ), "Timeout while waiting for manager to initialize."

    # Clear queue
    assert subprocess.call(["qserver", "queue", "clear"]) == SUCCESS

    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':5, 'delay':1}}"
    assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == SUCCESS

    n_plans, is_plan_running, _ = get_queue_state()
    assert n_plans == 1, "Incorrect number of plans in the queue"
    assert is_plan_running is False

    assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS
    ttime.sleep(2)
    n_plans, is_plan_running, _ = get_queue_state()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is True

    # Call is expected to fail, because a plan is currently running
    assert subprocess.call(["qserver", "environment", "close"]) != SUCCESS

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    n_plans, is_plan_running, n_history = get_queue_state()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is False
    assert n_history == 1

    # Now we can close the environment because plan execution is complete
    assert subprocess.call(["qserver", "environment", "close"]) == SUCCESS
    assert wait_for_condition(
        time=5, condition=condition_environment_closed
    ), "Timeout while waiting for environment to be closed"


def test_qserver_environment_destroy(re_manager):  # noqa: F811
    """
    Test for `environment_destroy` command
    """
    assert wait_for_condition(
        time=3, condition=condition_manager_idle
    ), "Timeout while waiting for manager to initialize."

    # Clear queue
    assert subprocess.call(["qserver", "queue", "clear"]) == SUCCESS

    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':5, 'delay':1}}"
    assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == SUCCESS

    n_plans, is_plan_running, _ = get_queue_state()
    assert n_plans == 1, "Incorrect number of plans in the queue"
    assert is_plan_running is False

    assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS
    ttime.sleep(2)
    n_plans, is_plan_running, _ = get_queue_state()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is True

    assert subprocess.call(["qserver", "environment", "destroy"]) == SUCCESS
    assert wait_for_condition(
        time=3, condition=condition_manager_idle
    ), "Timeout while waiting for environment to be destroyed."

    n_plans, is_plan_running, _ = get_queue_state()
    assert n_plans == 1, "Incorrect number of plans in the queue"
    assert is_plan_running is False

    assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS
    ttime.sleep(2)
    n_plans, is_plan_running, _ = get_queue_state()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is True

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    n_plans, is_plan_running, n_history = get_queue_state()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is False
    assert n_history == 2

    assert subprocess.call(["qserver", "environment", "close"]) == SUCCESS
    assert wait_for_condition(
        time=5, condition=condition_environment_closed
    ), "Timeout while waiting for environment to be closed"


# fmt: off
@pytest.mark.parametrize("run_in_background", [False, True])
# fmt: on
def test_qserver_environment_update_01(re_manager, run_in_background):  # noqa: F811
    """
    Test for `environment_update` command (more of a 'smoke' test)
    """
    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':5, 'delay':1}}"
    assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == SUCCESS

    n_plans, _, _ = get_queue_state()
    assert n_plans == 1, "Incorrect number of plans in the queue"

    assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    env_update_params = ["qserver", "environment", "update"]
    if run_in_background:
        env_update_params.append("background")

    assert subprocess.call(env_update_params) == SUCCESS

    ttime.sleep(1)

    assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS
    n_plans, is_plan_running, _ = get_queue_state()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is True

    ttime.sleep(2)

    assert subprocess.call(env_update_params) == SUCCESS if run_in_background else REQ_FAILED

    assert wait_for_condition(time=20, condition=condition_queue_processing_finished)

    assert subprocess.call(["qserver", "environment", "close"]) == SUCCESS
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("option_pause, option_continue", [
    ("deferred", "resume"),
    ("immediate", "resume"),
    ("deferred", "stop"),
    ("deferred", "abort"),
    ("deferred", "halt")
])
# fmt: on
def test_qserver_re_pause_continue(re_manager, option_pause, option_continue):  # noqa: F811
    """
    Test for `re_pause`, `re_resume`, `re_stop`, `re_abort` and `re_halt` commands
    """
    assert wait_for_condition(
        time=3, condition=condition_manager_idle
    ), "Timeout while waiting for manager to initialize."

    # Out of place calls
    assert subprocess.call(["qserver", "re", option_continue]) == REQ_FAILED
    assert subprocess.call(["qserver", "re", "pause", option_pause]) == REQ_FAILED

    # Clear queue
    assert subprocess.call(["qserver", "queue", "clear"]) == SUCCESS

    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num': 10, 'delay': 1}}"
    assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == SUCCESS
    assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == SUCCESS

    n_plans, is_plan_running, _ = get_queue_state()
    assert n_plans == 2, "Incorrect number of plans in the queue"
    assert is_plan_running is False

    assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS
    ttime.sleep(2)

    # Out of place calls
    assert subprocess.call(["qserver", "re", option_continue]) == REQ_FAILED

    assert subprocess.call(["qserver", "re", "pause", option_pause]) == SUCCESS
    assert wait_for_condition(
        time=3, condition=condition_manager_paused
    ), "Timeout while waiting for manager to pause"

    status = get_manager_status()
    assert status["manager_state"] == "paused"

    n_plans, is_plan_running, n_history = get_queue_state()
    assert n_plans == 1, "Incorrect number of plans in the queue"
    assert is_plan_running is False
    assert n_history == 0

    # Out of place call
    assert subprocess.call(["qserver", "re", "pause", option_pause]) == REQ_FAILED

    assert subprocess.call(["qserver", "re", option_continue]) == SUCCESS

    if option_continue == "resume":
        n_history_expected = 2
    else:
        assert wait_for_condition(time=3, condition=condition_manager_idle)

        n_plans, is_plan_running, n_history = get_queue_state()
        assert n_plans == (1 if option_continue == "stop" else 2), "Incorrect number of plans in the queue"
        assert is_plan_running is False
        assert n_history == 1

        assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS

        # Includes entry related to 1 aborted or halted plan
        n_history_expected = 2 if option_continue == "stop" else 3

    ttime.sleep(1)

    n_plans, is_plan_running, n_history = get_queue_state()
    assert n_plans == (0 if option_continue == "stop" else 1), "Incorrect number of plans in the queue"
    assert is_plan_running is True
    assert n_history == n_history_expected - (1 if option_continue == "stop" else 2)

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    n_plans, is_plan_running, n_history = get_queue_state()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is False
    assert n_history == n_history_expected

    assert subprocess.call(["qserver", "environment", "close"]) == SUCCESS
    assert wait_for_condition(
        time=5, condition=condition_environment_closed
    ), "Timeout while waiting for environment to be closed"


# fmt: off
@pytest.mark.parametrize("time_kill", ["before", 2, 8, "paused"])
# fmt: on
def test_qserver_manager_kill(re_manager, time_kill):  # noqa: F811
    """
    Test for `test_manager_kill` command. The command is stopping the event loop of RE Manager,
    causeing RE Watchdog to restart it. RE Manager can be restarted at any time: the restart
    should not affect executed plans or the state of the queue or RE Worker. Response to this
    command is never returned, so it can also be used to test how the system handles communication
    timeouts. It takes 5 seconds of RE Manager inactivity befor it is restarted. The following cases
    are tested:
    - RE Manager is killed and restarted before queue processing is started.
    - RE Manager is killed and restarted while the 1st plan in the queue is executed.
    - RE Manager is killed while the 1st plan is still executed and is not restarted before
    the plan execution is finished. RE Manager is supposed to recognize that the plan is completed,
    process the report and start processing of the next plan.
    - RE Manager is killed and restarted while the 1st plan is in 'paused' state. RE Manager is
    supposed to switch to 'paused' state at the restart. The plan can execution can be resumed.
    """
    assert wait_for_condition(
        time=3, condition=condition_manager_idle
    ), "Timeout while waiting for manager to initialize."

    # Clear queue
    assert subprocess.call(["qserver", "queue", "clear"]) == SUCCESS

    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num': 10, 'delay': 1}}"
    assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == SUCCESS
    assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == SUCCESS

    assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    if time_kill == "before":
        # The command that kills manager always times out
        assert subprocess.call(["qserver", "manager", "kill", "test"]) == COM_ERROR
        ttime.sleep(8)  # It takes 5 seconds before the manager is restarted

        status = get_manager_status()
        assert status["manager_state"] == "idle"

    # Start queue processing
    assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS

    if isinstance(time_kill, int):
        ttime.sleep(time_kill)
        # The command that kills manager always times out
        assert subprocess.call(["qserver", "manager", "kill", "test"]) == COM_ERROR
        ttime.sleep(8)  # It takes 5 seconds before the manager is restarted

        status = get_manager_status()
        assert status["manager_state"] == "executing_queue"

    elif time_kill == "paused":
        ttime.sleep(3)
        assert subprocess.call(["qserver", "re", "pause", "deferred"]) == 0
        assert wait_for_condition(time=3, condition=condition_manager_paused)
        assert subprocess.call(["qserver", "manager", "kill", "test"]) == COM_ERROR
        ttime.sleep(8)  # It takes 5 seconds before the manager is restarted

        status = get_manager_status()
        assert status["manager_state"] == "paused"

        assert subprocess.call(["qserver", "re", "resume"]) == SUCCESS

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    n_plans, is_plan_running, n_history = get_queue_state()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is False
    assert n_history == 2

    assert subprocess.call(["qserver", "environment", "close"]) == SUCCESS
    assert wait_for_condition(
        time=5, condition=condition_environment_closed
    ), "Timeout while waiting for environment to be closed"


# fmt: off
@pytest.mark.parametrize("additional_code, success", [
    # Nothing is added. Load profiles as usual.
    ("""
""", True),

    # Simulate profile that takes long time to load.
    ("""
\n
import time as ttime
ttime.sleep(20)

""", True),

    # Raise exception while loading the profile. This should cause RE Worker to exit.
    ("""
\n
raise Exception("This exception is raised to test if error handling works correctly")

""", False),
])
# fmt: on
def test_qserver_env_open_various_cases(re_manager_pc_copy, additional_code, success):  # noqa: F811
    _, pc_path = re_manager_pc_copy

    # Patch one of the startup files.
    patch_first_startup_file(pc_path, additional_code)

    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    # Attempt to create the environment. Long timeout.
    assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=30, condition=condition_manager_idle)

    status = get_manager_status()
    assert status["worker_environment_exists"] == success

    if not success:
        # Remove the offending patch and try to start the environment again. It should work
        patch_first_startup_file_undo(pc_path)
        assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
        assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # Run a plan to make sure RE Manager is functional after the startup.
    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num': 10, 'delay': 1}}"
    assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == SUCCESS

    # Start queue processing
    assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS
    ttime.sleep(2)
    status = get_manager_status()
    assert status["manager_state"] == "executing_queue"

    assert wait_for_condition(time=60, condition=condition_queue_processing_finished)
    n_plans, is_plan_running, n_history = get_queue_state()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is False
    assert n_history == 1

    assert subprocess.call(["qserver", "environment", "close"]) == SUCCESS
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("option", [None, "on", "off"])
# fmt: on
def test_qserver_manager_stop_1(re_manager, option):  # noqa: F811
    """
    Method ``manager_stop``. Environment is in 'idle' state.
    """
    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    # Attempt to create the environment
    assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_manager_idle)

    cmd = ["qserver", "manager", "stop"]
    if option:
        cmd += ["safe", option]

    assert subprocess.call(cmd) == SUCCESS

    # Check if RE Manager was stopped.
    assert re_manager.check_if_stopped() is True


# fmt: off
@pytest.mark.parametrize("option", [None, "on", "off"])
# fmt: on
def test_qserver_manager_stop_2(re_manager, option):  # noqa: F811
    """
    Method ``manager_stop``. Environment is running a plan.
    """
    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    # Attempt to create the environment
    assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_manager_idle)

    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num': 10, 'delay': 1}}"
    assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == SUCCESS
    assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == SUCCESS

    assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS
    ttime.sleep(2)
    status = get_manager_status()
    assert status["manager_state"] == "executing_queue"

    cmd = ["qserver", "manager", "stop"]
    if option:
        cmd += ["safe", option]

    if option == "off":
        assert subprocess.call(cmd) == SUCCESS

        # Check if RE Manager was stopped.
        assert re_manager.check_if_stopped() is True

    else:
        assert subprocess.call(cmd) == REQ_FAILED

        assert wait_for_condition(time=60, condition=condition_queue_processing_finished)
        n_plans, is_plan_running, n_history = get_queue_state()
        assert n_plans == 0, "Incorrect number of plans in the queue"
        assert is_plan_running is False
        assert n_history == 2


def test_qserver_queue_mode_set_1(re_manager):  # noqa F811
    """
    Basic test for ``qserver queue mode set`` command
    """
    assert subprocess.call(["qserver", "queue", "mode", "set", "loop", "True"]) == SUCCESS
    status = get_manager_status()
    assert status["plan_queue_mode"]["loop"] is True

    assert subprocess.call(["qserver", "queue", "mode", "set", "loop", "False"]) == SUCCESS
    status = get_manager_status()
    assert status["plan_queue_mode"]["loop"] is False

    assert subprocess.call(["qserver", "queue", "mode", "set", "ignore_failures", "True"]) == SUCCESS
    status = get_manager_status()
    assert status["plan_queue_mode"]["ignore_failures"] is True

    assert subprocess.call(["qserver", "queue", "mode", "set", "ignore_failures", "False"]) == SUCCESS
    status = get_manager_status()
    assert status["plan_queue_mode"]["ignore_failures"] is False


# fmt: off
@pytest.mark.parametrize("plist, exit_code", [
    (("set", "loop", "True"), SUCCESS),
    (("set",), SUCCESS),  # This should also work (no parameters -> the mode is not changed)
    (("unknown_option",), PARAM_ERROR),
    (("set", "loop"), PARAM_ERROR),  # Incorrect number of parameters
    (("set", "unknown_param", "True"), REQ_FAILED),  # Unsupported parameter name
    (("set", "loop", "true"), REQ_FAILED),  # Invalid parameter value
    (("set", "loop", "10"), REQ_FAILED),  # Invalid parameter value
    (("set", "ignore_failures", "true"), REQ_FAILED),  # Invalid parameter value
    (("set", "ignore_failures", "10"), REQ_FAILED),  # Invalid parameter value
])
# fmt: on
def test_qserver_queue_mode_set_2_fail(re_manager, plist, exit_code):  # noqa F811
    """
    Failing cases of the ``qserver queue mode set`` command
    """
    assert subprocess.call(["qserver", "queue", "mode", *plist]) == exit_code, str(plist)


def test_qserver_queue_autostart_1(re_manager):  # noqa F811
    """
    Basic test for ``qserver queue autostart`` command
    """
    status = get_manager_status()
    assert status["queue_autostart_enabled"] is False

    assert subprocess.call(["qserver", "queue", "autostart", "enable"]) == SUCCESS
    status = get_manager_status()
    assert status["queue_autostart_enabled"] is True

    assert subprocess.call(["qserver", "queue", "autostart", "disable"]) == SUCCESS
    status = get_manager_status()
    assert status["queue_autostart_enabled"] is False


def test_qserver_queue_autostart_2_fail(re_manager):  # noqa F811
    """
    Basic test for ``qserver queue autostart`` command: failing cases
    """
    assert subprocess.call(["qserver", "queue", "autostart", "unsupported"]) == PARAM_ERROR


# fmt: off
@pytest.mark.parametrize("pos, pos_result, success", [
    (None, 2, True),
    ("back", 2, True),
    ("front", 0, True),
    ("some", None, False),
    (0, 0, True),
    (1, 1, True),
    (2, 2, True),
    (3, 2, True),
    (100, 2, True),
    (-1, 2, True),
    (-2, 1, True),
    (-3, 0, True),
    (-4, 0, True),
    (-100, 0, True),
])
# fmt: on
def test_qserver_queue_item_add_1(re_manager, pos, pos_result, success):  # noqa F811
    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    plan1 = "{'name':'count', 'args':[['det1']]}"
    plan2 = "{'name':'count', 'args':[['det1', 'det2']]}"

    # Create the queue with 2 entries
    assert subprocess.call(["qserver", "queue", "add", "plan", plan1]) == SUCCESS
    assert subprocess.call(["qserver", "queue", "add", "plan", plan1]) == SUCCESS

    # Add another entry at the specified position
    params = [plan2]
    if pos is not None:
        params.insert(0, str(pos))

    res = subprocess.call(["qserver", "queue", "add", "plan", *params])
    if success:
        assert res == SUCCESS
    else:
        assert res == PARAM_ERROR

    resp = get_queue()
    assert len(resp["items"]) == (3 if success else 2)

    if success:
        assert resp["items"][pos_result]["args"] == [["det1", "det2"]]
        assert "item_uid" in resp["items"][pos_result]


def test_qserver_queue_item_add_2(re_manager):  # noqa F811
    """
    Failing cases: adding the plans that are expected to fail validation.
    """
    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    # Unknown plan name
    plan1 = "{'name':'count_test', 'args':[['det1']]}"
    # Unknown kwarg
    plan2 = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'abc': 10}}"

    # Both calls are expected to fail
    assert subprocess.call(["qserver", "queue", "add", "plan", plan1]) == REQ_FAILED
    assert subprocess.call(["qserver", "queue", "add", "plan", plan2]) == REQ_FAILED


# fmt: off
@pytest.mark.parametrize("before, target_pos, result_order", [
    (True, 0, [2, 0, 1]),
    (False, 0, [0, 2, 1]),
    (True, 1, [0, 2, 1]),
    (False, 1, [0, 1, 2]),
])
# fmt: on
def test_qserver_queue_item_add_3(re_manager, before, target_pos, result_order):  # noqa F811
    """
    Insert an item before or after the element with a given UID
    """
    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    plan1 = "{'name':'count', 'args':[['det1']]}"
    plan2 = "{'name':'count', 'args':[['det1', 'det2']]}"
    plan3 = "{'name':'count', 'args':[['det2']]}"

    assert subprocess.call(["qserver", "queue", "add", "plan", plan1]) == SUCCESS
    assert subprocess.call(["qserver", "queue", "add", "plan", plan2]) == SUCCESS

    # Read queue.
    queue_1 = get_queue()["items"]
    assert len(queue_1) == 2
    uids_1 = [_["item_uid"] for _ in queue_1]

    params = ["before" if before else "after", uids_1[target_pos], plan3]
    assert subprocess.call(["qserver", "queue", "add", "plan", *params]) == SUCCESS

    # Check if the element was inserted in the right plance
    queue_2 = get_queue()["items"]
    assert len(queue_2) == 3
    uids_2 = [_["item_uid"] for _ in queue_2]
    for n, uid in enumerate(uids_2):
        n_res = result_order[n]
        if (n_res < 2) and (uid != uids_1[n_res]):
            assert False, f"uids_1: {uids_1}, uids_2: {uids_2}, result_order: {result_order}"


def test_qserver_queue_item_add_4(re_manager):  # noqa F811
    """
    Add instruction to the queue
    """
    plan1 = "{'name':'count', 'args':[['det1']]}"
    plan2 = "{'name':'count', 'args':[['det1', 'det2']]}"
    instruction = "queue-stop"

    assert subprocess.call(["qserver", "queue", "add", "plan", plan1]) == SUCCESS
    assert subprocess.call(["qserver", "queue", "add", "instruction", instruction]) == SUCCESS
    assert subprocess.call(["qserver", "queue", "add", "plan", plan2]) == SUCCESS

    queue_1 = get_queue()["items"]
    assert len(queue_1) == 3
    assert queue_1[0]["item_type"] == "plan", str(queue_1[0])
    assert queue_1[1]["item_type"] == "instruction", str(queue_1[1])
    assert queue_1[2]["item_type"] == "plan", str(queue_1[2])


# fmt: off
@pytest.mark.parametrize("pos", [None, "back"])
# fmt: on
def test_qserver_queue_item_add_5_fail(re_manager, pos):  # noqa F811
    """
    No plan is supplied.
    """
    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    if pos:
        assert subprocess.call(["qserver", "queue", "add", "plan", pos]) == PARAM_ERROR
    else:
        assert subprocess.call(["qserver", "queue", "add", "plan"]) == PARAM_ERROR


# fmt: off
@pytest.mark.parametrize("pos", [10, "front", "back"])
# fmt: on
def test_qserver_queue_item_add_6_fail(re_manager, pos):  # noqa F811
    """
    Incorrect order of arguments (position is specified).
    """
    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    pos, plan = 10, "{'name':'count', 'args':[['det1']]}"
    params = [plan, str(pos)]
    assert subprocess.call(["qserver", "queue", "add", "plan", *params]) == PARAM_ERROR


# fmt: off
@pytest.mark.parametrize("params, exit_code", [
    # Error while processing message by the manager
    (["before_uid", "some_uid", "plan"], PARAM_ERROR),
    # Unknown keyword
    (["unknown_keyword", "some_uid", "plan"], PARAM_ERROR),
    # Incorrect order of arguments
    (["plan", "before_uid", "some_uid"], PARAM_ERROR),
    (["some_uid", "before_uid", "plan"], PARAM_ERROR),
    (["some_uid", "plan", "before_uid"], PARAM_ERROR),
])
# fmt: on
def test_qserver_queue_item_add_7_fail(re_manager, params, exit_code):  # noqa F811
    """
    Incorrect order of arguments (position is specified).
    """
    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    plan = "{'name':'count', 'args':[['det1']]}"
    params = [_ if _ != "plan" else plan for _ in params]
    assert subprocess.call(["qserver", "queue", "add", "plan", *params]) == exit_code


# fmt: on
@pytest.mark.parametrize("replace", [False, True])
@pytest.mark.parametrize("item_type", ["plan", "instruction"])
# fmt: off
def test_qserver_queue_item_update_1(re_manager, replace, item_type):  # noqa F811
    """
    Basic test for `queue_item_update` method.
    """
    plan1 = "{'name':'count', 'args':[['det1', 'det2']]}"
    plan2 = "{'name':'count', 'args':[['det1', 'det2']]}"
    instruction = "queue-stop"

    assert subprocess.call(["qserver", "queue", "add", "plan", plan1]) == SUCCESS

    queue_1 = get_queue()["items"]
    assert len(queue_1) == 1
    item_1 = queue_1[0]
    uid_to_replace = item_1["item_uid"]

    if item_type == "plan":
        item = plan2
    elif item_type == "instruction":
        item = instruction
    else:
        assert False, f"Unsupported item type '{item_type}'"
    option = "replace" if replace else "update"

    assert subprocess.call(["qserver", "queue", option, item_type, uid_to_replace, item]) == SUCCESS

    queue_2 = get_queue()["items"]
    assert len(queue_2) == 1
    item_2 = queue_2[0]

    if replace:
        assert item_2["item_uid"] != item_1["item_uid"]
    else:
        assert item_2["item_uid"] == item_1["item_uid"]
    item_2["item_type"] == item_type


# fmt: on
@pytest.mark.parametrize("replace", [False, True])
@pytest.mark.parametrize("item_type", ["plan", "instruction"])
# fmt: off
def test_qserver_queue_item_update_2_fail(re_manager, replace, item_type):  # noqa F811
    """
    Failing cases for `queue_item_update`: no matching UID is found in the queue.
    """
    plan1 = "{'name':'count', 'args':[['det1', 'det2']]}"
    plan2 = "{'name':'count', 'args':[['det1', 'det2']]}"
    instruction = "queue-stop"

    assert subprocess.call(["qserver", "queue", "add", "plan", plan1]) == SUCCESS

    queue_1 = get_queue()["items"]
    assert len(queue_1) == 1
    uid_to_replace = "non-existent-UID"

    if item_type == "plan":
        item = plan2
    elif item_type == "instruction":
        item = instruction
    else:
        assert False, f"Unsupported item type '{item_type}'"
    option = "replace" if replace else "update"

    assert subprocess.call(["qserver", "queue", option, item_type, uid_to_replace, item]) == REQ_FAILED

    queue_2 = get_queue()["items"]
    assert queue_1 == queue_2


# fmt: off
@pytest.mark.parametrize("item_type, env_exists", [
    ("plan", True),
    ("instruction", True),
    ("plan", False),
    ("instruction", False),
])
# fmt: on
def test_qserver_item_execute_1(re_manager, item_type, env_exists):  # noqa: F811
    """
    Long test runs a series of CLI commands.
    """
    plan_1 = "{'name':'count', 'args':[['det1', 'det2']]}"

    assert wait_for_condition(
        time=3, condition=condition_manager_idle
    ), "Timeout while waiting for manager to initialize."

    if env_exists:
        assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
        assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    expected_result = SUCCESS if env_exists else REQ_FAILED
    expected_n_history = 1 if env_exists and (item_type == "plan") else 0
    if item_type == "plan":
        item = ["plan", plan_1]
    elif item_type == "instruction":
        item = ["instruction", "queue-stop"]
    else:
        raise ValueError(f"Unknown item type '{item_type}'")

    assert subprocess.call(["qserver", "queue", "execute", *item]) == expected_result

    if env_exists:
        assert wait_for_condition(
            time=10, condition=condition_queue_processing_finished
        ), "Timeout while waiting for process to finish"

        assert subprocess.call(["qserver", "environment", "close"]) == SUCCESS
        assert wait_for_condition(
            time=5, condition=condition_environment_closed
        ), "Timeout while waiting for environment to be closed"

    n_plans, is_plan_running, n_history = get_queue_state()
    assert n_plans == 0
    assert is_plan_running is False
    assert n_history == expected_n_history


# fmt: off
@pytest.mark.parametrize("pos, uid_ind, pos_result, success", [
    (None, None, 2, True),
    ("back", None, 2, True),
    ("front", None, 0, True),
    ("some", None, None, False),
    (0, None, 0, True),
    (1, None, 1, True),
    (2, None, 2, True),
    (3, None, None, False),
    (100, None, None, False),
    (-1, None, 2, True),
    (-2, None, 1, True),
    (-3, None, 0, True),
    (-4, None, 0, False),
    (-100, None, 0, False),
    (None, 0, 0, True),
    (None, 1, 1, True),
    (None, 2, 2, True),
    (None, 3, 2, False),
])
# fmt: on
def test_qserver_queue_item_get_remove(re_manager, pos, uid_ind, pos_result, success):  # noqa F811
    """
    Tests for ``queue_item_get`` and ``queue_item_remove`` requests.
    """
    plans = [
        "{'name':'count', 'args':[['det1']]}",
        "{'name':'count', 'args':[['det2']]}",
        "{'name':'count', 'args':[['det1', 'det2']]}",
    ]
    plans_args = [[["det1"]], [["det2"]], [["det1", "det2"]]]

    for plan in plans:
        assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == SUCCESS

    queue_1 = get_queue()["items"]
    assert len(queue_1) == 3
    uids_1 = [_["item_uid"] for _ in queue_1]
    uids_1.append("unknown_uid")  # Extra element (for one of the tests)

    if uid_ind is None:
        # Remove entry at the specified position
        args = [str(pos)] if (pos is not None) else []
    else:
        uid = uids_1[uid_ind]
        args = [uid]

    # Testing 'queue_item_get'. ONLY THE RETURN CODE IS TESTED.
    res = subprocess.call(["qserver", "queue", "item", "get", *args])
    if success:
        assert res == SUCCESS
    else:
        assert res == REQ_FAILED

    # Testing 'queue_item_remove'.
    res = subprocess.call(["qserver", "queue", "item", "remove", *args])
    if success:
        assert res == SUCCESS
    else:
        assert res == REQ_FAILED

    queue_2 = get_queue()["items"]
    assert len(queue_2) == (2 if success else 3)
    if success:
        ind = [0, 1, 2]
        ind.pop(pos_result)
        # Check that the right entry disappeared from the queue.
        assert queue_2[0]["args"] == plans_args[ind[0]]
        assert queue_2[1]["args"] == plans_args[ind[1]]


# fmt: off
@pytest.mark.parametrize("params, result_order, exit_code", [
    # 'params': positions are always represented as str, all int's are UIDs.
    (["0", "1"], [1, 0, 2], SUCCESS),
    (["2", "0"], [2, 0, 1], SUCCESS),
    (["2", "-3"], [2, 0, 1], SUCCESS),
    (["-1", "-3"], [2, 0, 1], SUCCESS),
    (["2", "-5"], [0, 1, 2], REQ_FAILED),  # Destination index out of range
    (["1", "3"], [0, 1, 2], REQ_FAILED),  # Destination index out of range
    (["front", "back"], [1, 2, 0], SUCCESS),
    (["back", "front"], [2, 0, 1], SUCCESS),
    ([1, "before", 0], [1, 0, 2], SUCCESS),
    ([0, "after", 1], [1, 0, 2], SUCCESS),
    (["1", "before", 0], [1, 0, 2], SUCCESS),  # Mixed pos->uid
    (["0", "after", 1], [1, 0, 2], SUCCESS),  # Mixed pos->uid
    ([1, "0"], [1, 0, 2], SUCCESS),  # Mixed uid->pos
    ([1, "2"], [0, 2, 1], SUCCESS),  # Mixed uid->pos
    (["1", "unknown_kwd", 0], [0, 1, 2], PARAM_ERROR),  # Mixed pos->uid
    (["0", "after"], [0, 1, 2], PARAM_ERROR),  # Second parameter 'after' is a keyword, UID is expected
    (["0"], [0, 1, 2], PARAM_ERROR),  # Not enough parameters
])
# fmt: on
def test_qserver_queue_item_get_move(re_manager, params, result_order, exit_code):  # noqa F811
    """
    Tests for ``queue_item_get`` and ``queue_item_remove`` requests.
    """
    plans = [
        "{'name':'count', 'args':[['det1']]}",
        "{'name':'count', 'args':[['det2']]}",
        "{'name':'count', 'args':[['det1', 'det2']]}",
    ]

    for plan in plans:
        assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == SUCCESS

    queue_1 = get_queue()["items"]
    assert len(queue_1) == 3
    uids_1 = [_["item_uid"] for _ in queue_1]
    uids_1.append("unknown_uid")  # Extra element (for one of the tests)

    # Replace ints with UIDs (positions are represented as strings)
    params = params.copy()
    for n, p in enumerate(params):
        if isinstance(p, int):
            params[n] = uids_1[p]

    # Testing 'queue_item_move'.
    assert subprocess.call(["qserver", "queue", "item", "move", *params]) == exit_code

    queue_2 = get_queue()["items"]
    assert len(queue_2) == 3
    uids_2 = [_["item_uid"] for _ in queue_2]

    # Compare the order of UIDs before and after moving the element
    uids_1_reordered = [uids_1[_] for _ in result_order]
    assert uids_1_reordered == uids_2


# fmt: off
@pytest.mark.parametrize("deactivate", [False, True])
# fmt: on
def test_qserver_queue_stop(re_manager, deactivate):  # noqa: F811
    """
    Methods ``queue_stop`` and ``queue_stop_cancel``.
    """
    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    # Attempt to create the environment
    assert subprocess.call(["qserver", "environment", "open"]) == 0
    assert wait_for_condition(time=timeout_env_open, condition=condition_manager_idle)

    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num': 10, 'delay': 1}}"
    assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == 0
    assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == 0

    # Queue is not running, so the request is expected to fail
    assert subprocess.call(["qserver", "queue", "stop"]) != 0
    status = get_manager_status()
    assert status["queue_stop_pending"] is False

    assert subprocess.call(["qserver", "queue", "start"]) == 0
    ttime.sleep(2)
    status = get_manager_status()
    assert status["manager_state"] == "executing_queue"

    assert subprocess.call(["qserver", "queue", "stop"]) == 0
    status = get_manager_status()
    assert status["queue_stop_pending"] is True

    if deactivate:
        ttime.sleep(1)
        assert subprocess.call(["qserver", "queue", "stop", "cancel"]) == 0
        status = get_manager_status()
        assert status["queue_stop_pending"] is False

    assert wait_for_condition(time=60, condition=condition_manager_idle)
    n_plans, is_plan_running, n_history = get_queue_state()
    assert n_plans == (0 if deactivate else 1)
    assert is_plan_running is False
    assert n_history == (2 if deactivate else 1)
    status = get_manager_status()
    assert status["queue_stop_pending"] is False


def test_qserver_ping(re_manager):  # noqa: F811
    """
    Methods ``ping``: basic test
    """
    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    # Send 'ping' request
    assert subprocess.call(["qserver", "ping"]) == 0


# fmt: off
@pytest.mark.parametrize("option, exit_code", [
    (None, SUCCESS),
    ("active", SUCCESS),
    ("open", SUCCESS),
    ("closed", SUCCESS),
    ("some_unknown", PARAM_ERROR)
])
# fmt: on
def test_qserver_re_runs(re_manager, option, exit_code):  # noqa: F811
    """
    Basic test for ``re_runs`` method. There is no easy way to verify if the response
    was correct, so we just check if all supported combinations of parameters are accepted.
    """
    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    params = [option] if option else []
    assert subprocess.call(["qserver", "re", "runs", *params]) == exit_code


_script_to_upload_1 = """
def count_modified(detectors, num=1, delay=None):
    yield from count(detectors=detectors, num=num, delay=delay)
"""


# fmt: off
@pytest.mark.parametrize("run_in_background", [False, True])
@pytest.mark.parametrize("update_re", [False, True])
@pytest.mark.parametrize("update_lists", [False, True])
# fmt: on
def test_qserver_script_upload_1(re_manager, tmp_path, run_in_background, update_lists, update_re):  # noqa: F811
    """
    Tests for 'qserver script upload'. The uploaded script does not change RE, so the tests
    simply checks if 'update-re' parameter is accepted.
    """
    # Create a file with the script
    script_fln = "script_to_upload.py"
    script_path = os.path.join(tmp_path, script_fln)
    with open(script_path, "w") as f:
        f.writelines(_script_to_upload_1)

    params = ["qserver", "script", "upload", script_path]
    if run_in_background:
        params.append("background")
    if update_re:
        params.append("update-re")
    if not update_lists:
        params.append("keep-lists")

    # Call is expected to fail (environment is not open)
    assert subprocess.call(params) == REQ_FAILED

    assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # Check that 'count_modified' plan does not exist before the script is uploaded
    resp1, _ = zmq_single_request("plans_existing")
    assert resp1["success"] is True
    assert "count_modified" not in resp1["plans_existing"]

    if run_in_background:
        # Start some foreground process (run a function)
        item = r"{'name': 'function_sleep', 'kwargs': {'time': 2}}"
        params_fg = ["qserver", "function", "execute", item]
        assert subprocess.call(params_fg) == SUCCESS

    assert subprocess.call(params) == SUCCESS
    if run_in_background:
        ttime.sleep(2)  # Wait for the task to be completed
    assert wait_for_condition(time=5, condition=condition_manager_idle)

    # Check that 'count_modified' function was loaded into the environment
    resp2, _ = zmq_single_request("plans_existing")
    assert resp2["success"] is True
    if update_lists:
        assert "count_modified" in resp2["plans_existing"]
    else:
        assert "count_modified" not in resp2["plans_existing"]

    assert subprocess.call(["qserver", "environment", "close"]) == SUCCESS
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_qserver_script_upload_2_fail(re_manager, tmp_path):  # noqa: F811
    """
    Tests for 'qserver script upload': failing cases.
    """
    script_path = os.path.join(tmp_path, "script.py")
    with open(script_path, "w") as f:
        f.writelines(_script_to_upload_1)

    # File does not exist (IO error)
    assert subprocess.call(["qserver", "script", "upload", os.path.join(tmp_path, "script2.py")]) == PARAM_ERROR

    # Invalid parameter
    assert subprocess.call(["qserver", "script", "upload", script_path, "invalid_param"]) == PARAM_ERROR


# fmt: off
@pytest.mark.parametrize("run_in_background", [False, True])
# fmt: on
def test_qserver_function_execute_1(re_manager, run_in_background):  # noqa: F811
    """
    Tests for 'qserver function execute'.
    """
    item = r"{'name': 'function_sleep', 'kwargs': {'time': 2}}"
    params = ["qserver", "function", "execute", item]
    params_fg = params.copy()  # Parameters for foreground task
    if run_in_background:
        params.append("background")

    # Call is expected to fail (environment is not open)
    assert subprocess.call(params) == REQ_FAILED

    assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    if run_in_background:
        # Start a foreground task (the same function) to test if a function can be run in the background
        assert subprocess.call(params_fg) == SUCCESS
        # Attempt to start another foreground process
        assert subprocess.call(params_fg) == REQ_FAILED

    assert subprocess.call(params) == SUCCESS
    if run_in_background:
        ttime.sleep(3)  # Wait for the task to be completed
    assert wait_for_condition(time=5, condition=condition_manager_idle)

    assert subprocess.call(["qserver", "environment", "close"]) == SUCCESS
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_qserver_function_execute_2_fail(re_manager):  # noqa: F811
    """
    Tests for 'qserver function execute': failing cases.
    """
    item = r"{'name': 'function_sleep', 'kwargs': {'time': 2}}"

    # Invalid parameter
    assert subprocess.call(["qserver", "function", "execute", item, "invalid_param"]) == PARAM_ERROR


def test_qserver_task_result_status_1(re_manager):  # noqa: F811
    """
    Tests for 'qserver task result' and 'qserver task status.
    """
    # The request should be successful for any 'task_uid'.
    task_uid = "01e80342-5e36-44de-bc86-9bd8d57c9885"
    assert subprocess.call(["qserver", "task", "status", task_uid]) == SUCCESS
    assert subprocess.call(["qserver", "task", "result", task_uid]) == SUCCESS

    # Some cases of invalid parameters
    assert subprocess.call(["qserver", "task", "status"]) == PARAM_ERROR
    assert subprocess.call(["qserver", "task", "result"]) == PARAM_ERROR
    assert subprocess.call(["qserver", "task", "something"]) == PARAM_ERROR
    assert subprocess.call(["qserver", "task", "status", task_uid, "extra_param"]) == PARAM_ERROR
    assert subprocess.call(["qserver", "task", "result", task_uid, "extra_param"]) == PARAM_ERROR


_sample_trivial_plan1 = """
def trivial_plan_for_unit_test():
    '''
    Trivial plan for unit test.
    '''
    yield from scan([det1, det2], motor, -1, 1, 10)
"""


# fmt: off
@pytest.mark.parametrize("restore_plans_devices", [True, False])
# fmt: on
def test_qserver_permissions_reload_1(re_manager_pc_copy, tmp_path, restore_plans_devices):  # noqa F811
    """
    Tests for ``qserver permissions reload [lists]`` API.
    """
    # pc_path = copy_default_profile_collection(tmp_path)
    _, pc_path = re_manager_pc_copy
    append_code_to_last_startup_file(pc_path, additional_code=_sample_trivial_plan1)

    # Generate the new list of allowed plans and devices and reload them
    gen_list_of_plans_and_devices(startup_dir=pc_path, file_dir=pc_path, overwrite=True)

    plan = "{'name': 'trivial_plan_for_unit_test'}"

    # Attempt to add the plan to the queue. The request is supposed to fail, because
    #   the initially loaded profile collection does not contain the plan.
    assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == REQ_FAILED

    # Reload profile collection
    params = ["permissions", "reload"]
    if restore_plans_devices:
        params.append("lists")
    assert subprocess.call(["qserver", *params]) == SUCCESS

    # Attempt to add the plan to the queue. It should be successful now.
    res_expected = SUCCESS if restore_plans_devices else REQ_FAILED
    assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == res_expected


_permissions_dict_not_allow_count = {
    "user_groups": {
        "root": {"allowed_plans": [None], "allowed_devices": [None]},
        "primary": {"allowed_plans": [None], "forbidden_plans": ["^count$"], "allowed_devices": [None]},
    }
}


def test_qserver_permissions_set_get_1(re_manager, tmp_path):  # noqa F811
    """
    Tests for ``qserver permissions set`` and ``qserver permissions get``: basic tests.
    """

    user_group_permissions = _permissions_dict_not_allow_count

    # Create yaml file with user group permissions
    fl_path = os.path.join(tmp_path, "ugp.yaml")
    with open(fl_path, "w") as f:
        f.writelines(yaml.dump(user_group_permissions))

    # Set user group permissions
    assert subprocess.call(["qserver", "permissions", "set", fl_path]) == SUCCESS

    # Check that permissions were really changed
    resp1, _ = zmq_single_request("permissions_get")
    assert resp1["success"] is True, f"resp={resp1}"
    assert resp1["user_group_permissions"] == user_group_permissions

    # Check if 'qserver permissions get' works
    assert subprocess.call(["qserver", "permissions", "get"]) == SUCCESS


def test_qserver_permissions_set_get_2_fail(re_manager, tmp_path):  # noqa F811
    """
    Tests for ``qserver permissions set`` and ``qserver permissions get``: failing cases.
    """
    # Create yaml file with user group permissions
    fl_path = os.path.join(tmp_path, "ugp.yaml")
    with open(fl_path, "w") as f:
        f.writelines("This is not a valid permissions dictionary")  # Invalid file

    # Non-existing file
    assert subprocess.call(["qserver", "permissions", "set", os.path.join(tmp_path, "some_file")]) == PARAM_ERROR
    # No file name
    assert subprocess.call(["qserver", "permissions", "set"]) == PARAM_ERROR
    # Extra parameters
    assert subprocess.call(["qserver", "permissions", "set", fl_path, "extra_parameter"]) == PARAM_ERROR
    # Request is correct, but the file does not contain valid dictionary
    assert subprocess.call(["qserver", "permissions", "set", fl_path]) == REQ_FAILED

    # Extra parameters
    assert subprocess.call(["qserver", "permissions", "get", "extra_parameter"]) == PARAM_ERROR


# fmt: off
@pytest.mark.parametrize("test_mode", ["none", "ev"])
# fmt: on
def test_qserver_secure_1(monkeypatch, re_manager_cmd, test_mode):  # noqa: F811
    """
    Test operation of `qserver` CLI tool with enabled encryption. Test options to
    set the private key used by `qserver` using the environment variable.
    """
    public_key, private_key = generate_zmq_keys()

    if test_mode == "none":
        pass
    elif test_mode == "ev":
        # Set server public key (for 'qserver') using environment variable
        monkeypatch.setenv("QSERVER_ZMQ_PUBLIC_KEY", public_key)
        # Set private key for RE manager
        monkeypatch.setenv("QSERVER_ZMQ_PRIVATE_KEY_FOR_SERVER", private_key)
        # Set public key used by test helper functions such as 'wait_for_condition'
        set_qserver_zmq_public_key(monkeypatch, server_public_key=public_key)
    else:
        raise RuntimeError(f"Unrecognized test mode '{test_mode}'")

    # Security enabled by setting
    re_manager_cmd([])

    _plan1 = '{"name": "count", "args": [["det1", "det2"]], "item_type": "plan"}'
    _plan2 = '{"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10], "item_type": "plan"}'

    # Add 2 plans
    assert subprocess.call(["qserver", "queue", "add", "plan", _plan1]) == 0
    assert subprocess.call(["qserver", "queue", "add", "plan", _plan2]) == 0

    # Request the list of allowed plans and devices (we don't check what is returned)
    assert subprocess.call(["qserver", "allowed", "plans"], stdout=subprocess.DEVNULL) == SUCCESS
    assert subprocess.call(["qserver", "allowed", "devices"], stdout=subprocess.DEVNULL) == SUCCESS

    # Request the list of existing plans and devices (we don't check what is returned)
    assert subprocess.call(["qserver", "existing", "plans"], stdout=subprocess.DEVNULL) == SUCCESS
    assert subprocess.call(["qserver", "existing", "devices"], stdout=subprocess.DEVNULL) == SUCCESS

    assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    state = get_manager_status()
    assert state["items_in_queue"] == 2
    assert state["items_in_history"] == 0

    assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS
    assert wait_for_condition(
        time=20, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    state = get_manager_status()
    assert state["items_in_queue"] == 0
    assert state["items_in_history"] == 2

    assert subprocess.call(["qserver", "environment", "close"]) == SUCCESS
    assert wait_for_condition(
        time=5, condition=condition_environment_closed
    ), "Timeout while waiting for environment to be closed"


# fmt: off
@pytest.mark.parametrize("test_mode", ["none", "parameter", "env_var", "both_success", "both_fail"])
# fmt: on
def test_qserver_parameters_1(monkeypatch, re_manager_cmd, test_mode):  # noqa: F811
    """
    Check that passing server control address as a parameter ``--zmq-control-addr`` and
    environment variable ``QSERVER_ZMQ_CONTROL_ADDRESS`` works as expected.
    """
    address_server = "tcp://*:60621"
    address_client = "tcp://localhost:60621"
    address_client_incorrect = "tcp://localhost:60620"

    params_server = [f"--zmq-control-addr={address_server}"]
    params_client = []
    if test_mode == "none":
        # Use default address, communication fails
        result = COM_ERROR
    elif test_mode == "parameter":
        # Pass the address as a parameter
        result = SUCCESS
        params_client.append(f"--zmq-control-addr={address_client}")
    elif test_mode == "env_var":
        # Pass the address as an environment variable
        result = SUCCESS
        monkeypatch.setenv("QSERVER_ZMQ_CONTROL_ADDRESS", address_client)
    elif test_mode == "both_success":
        # Pass the correct address as a parameter and incorrect as environment variable (ignored)
        result = SUCCESS
        params_client.append(f"--zmq-control-addr={address_client}")
        monkeypatch.setenv("QSERVER_ZMQ_CONTROL_ADDRESS", address_client_incorrect)
    elif test_mode == "both_fail":
        # Pass incorrect address as an environment variable (ignored) and correct address as a parameter
        result = COM_ERROR
        params_client.append(f"--zmq-control-addr={address_client_incorrect}")
        monkeypatch.setenv("QSERVER_ZMQ_CONTROL_ADDRESS", address_client)
    else:
        raise RuntimeError(f"Unrecognized test mode '{test_mode}'")

    set_qserver_zmq_address(monkeypatch, zmq_server_address=address_client)

    # Security enabled by setting
    re_manager_cmd(params_server)

    assert subprocess.call(["qserver", "status"] + params_client) == result


def test_qserver_lock_01(re_manager):  # noqa: F811
    def check_state(environment, queue):
        status = get_manager_status()
        assert status["lock"]["environment"] == environment
        assert status["lock"]["queue"] == queue

    lock_key = "custom_lock_key"

    # Test different options
    check_state(False, False)
    assert subprocess.call(["qserver", "-k", lock_key, "lock", "environment"]) == SUCCESS
    check_state(True, False)
    assert subprocess.call(["qserver", "-k", lock_key, "lock", "queue"]) == SUCCESS
    check_state(False, True)
    assert subprocess.call(["qserver", "-k", lock_key, "lock", "all", "Note ..."]) == SUCCESS
    check_state(True, True)

    # Failing calls
    assert subprocess.call(["qserver", "-k", "invalid_key", "lock", "all"]) == REQ_FAILED
    assert subprocess.call(["qserver", "lock", "environment"]) == REQ_FAILED
    assert subprocess.call(["qserver", "-k", lock_key, "lock"]) == PARAM_ERROR
    assert subprocess.call(["qserver", "-k", lock_key, "lock", "environment", "queue"]) == PARAM_ERROR
    assert subprocess.call(["qserver", "-k", lock_key, "lock", "something"]) == PARAM_ERROR
    params = ["qserver", "-k", lock_key, "lock", "environment", "Note ...", "extra_param"]
    assert subprocess.call(params) == PARAM_ERROR

    check_state(True, True)

    # Load 'lock_info'
    assert subprocess.call(["qserver", "lock", "info"]) == SUCCESS
    # Load 'lock_info' and validate the key
    assert subprocess.call(["qserver", "-k", lock_key, "lock", "info"]) == SUCCESS
    # Load 'lock_info' and validate the key - invalid key
    assert subprocess.call(["qserver", "-k", "invalid-key", "lock", "info"]) == REQ_FAILED

    # Open/close the environment
    assert subprocess.call(["qserver", "environment", "open"]) == REQ_FAILED
    assert subprocess.call(["qserver", "-k", "invalid-key", "environment", "open"]) == REQ_FAILED

    assert subprocess.call(["qserver", "-k", lock_key, "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)
    assert subprocess.call(["qserver", "-k", lock_key, "environment", "close"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_closed)

    # Add a plan
    plan_1 = "{'name':'count', 'args':[['det1', 'det2']]}"
    assert subprocess.call(["qserver", "queue", "add", "plan", plan_1]) == REQ_FAILED
    assert subprocess.call(["qserver", "-k", "invalid-key", "queue", "add", "plan", plan_1]) == REQ_FAILED
    assert subprocess.call(["qserver", "-k", lock_key, "queue", "add", "plan", plan_1]) == SUCCESS

    assert subprocess.call(["qserver", "unlock"]) == REQ_FAILED
    assert subprocess.call(["qserver", "-k", "invalid-key", "unlock"]) == REQ_FAILED
    assert subprocess.call(["qserver", "-k", lock_key, "unlock", "invalid_param"]) == PARAM_ERROR
    assert subprocess.call(["qserver", "-k", lock_key, "unlock"]) == SUCCESS
    check_state(False, False)


# fmt: off
@pytest.mark.parametrize("env_open", [False, True])
# fmt: on
def test_qserver_config_01(re_manager, env_open):  # noqa: F811
    """
    ``qserver config``: basic test.
    """
    if env_open:
        assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
        assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    status = get_manager_status()
    assert status["worker_environment_exists"] == env_open

    assert subprocess.call(["qserver", "config"]) == SUCCESS
    assert subprocess.call(["qserver", "config", "something"]) == PARAM_ERROR
    assert subprocess.call(["qserver", "config"]) == SUCCESS

    if env_open:
        assert subprocess.call(["qserver", "environment", "close"]) == SUCCESS
        assert wait_for_condition(time=timeout_env_open, condition=condition_environment_closed)

    status = get_manager_status()
    assert status["worker_environment_exists"] is False


_busy_script_01 = """
import time
for n in range(30):
    time.sleep(1)
"""


# fmt: off
@pytest.mark.parametrize("option", ["ip_client", "script", "plan"])
# fmt: on
@pytest.mark.skipif(not use_ipykernel_for_tests(), reason="Test is run only with IPython worker")
def test_qserver_kernel_interrupt_01(re_manager, ip_kernel_simple_client, option):  # noqa: F811
    """
    "qserver kernel interrupt":  basic test.
    """
    using_ipython = use_ipykernel_for_tests()
    assert using_ipython, "The test can be run only in IPython mode"

    def check_status(ip_kernel_state, ip_kernel_captured):
        # Returned status may be used to do additional checks
        status = get_manager_status()
        if isinstance(ip_kernel_state, (str, type(None))):
            ip_kernel_state = [ip_kernel_state]
        assert status["ip_kernel_state"] in ip_kernel_state
        assert status["ip_kernel_captured"] == ip_kernel_captured
        return status

    assert subprocess.call(["qserver", "environment", "open"]) == SUCCESS
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    kernel_int_params = ["qserver", "kernel", "interrupt"]

    if option == "ip_client":
        ip_kernel_simple_client.start()
        ip_kernel_simple_client.execute_with_check(_busy_script_01)
    elif option == "script":
        resp, _ = zmq_single_request("script_upload", params=dict(script=_busy_script_01))
        assert resp["success"] is True, pprint.pformat(resp)
        kernel_int_params.append("task")
    elif option == "plan":
        plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num': 5, 'delay': 1}}"
        assert subprocess.call(["qserver", "queue", "add", "plan", plan]) == SUCCESS
        assert subprocess.call(["qserver", "queue", "start"]) == SUCCESS
        kernel_int_params.append("plan")
    else:
        assert False, f"Unknown option {option!r}"

    ttime.sleep(2)

    ip_kernel_captured = (option != "ip_client")
    check_status("busy", ip_kernel_captured)

    assert subprocess.call(kernel_int_params) == SUCCESS

    if option == "ip_client":
        assert wait_for_condition(3, condition_ip_kernel_idle)
    else:
        assert wait_for_condition(3, condition_manager_idle_or_paused)

    check_status("idle", False)

    status = get_manager_status()
    if status["re_state"] == "paused":
        assert subprocess.call(["qserver", "re", "stop"]) == SUCCESS
        assert wait_for_condition(10, condition_manager_idle)

    assert subprocess.call(["qserver", "environment", "close"]) == SUCCESS
    assert wait_for_condition(time=3, condition=condition_environment_closed)


# ==================================================================================
#                             qserver-clear-lock


def test_qserver_clear_lock_01(re_manager_cmd):  # noqa: F811
    """
    Basic test for ``qserver-clear-lock`` utility.
    """

    def check_state(environment, queue):
        status = get_manager_status()
        assert status["lock"]["environment"] == environment
        assert status["lock"]["queue"] == queue

    manager = re_manager_cmd()

    # Lock the environment
    assert subprocess.call(["qserver", "-k", "some-lock-key", "lock", "environment"]) == SUCCESS
    check_state(True, False)

    # Redis name prefix is optional, but it must be used in the test to avoid changing other keys
    prefix = f"--redis-name-prefix={_test_redis_name_prefix}"

    # Clear the lock in Redis
    assert subprocess.call(["qserver-clear-lock", prefix]) == 0
    check_state(True, False)

    # Restart the manager
    manager.stop_manager(cleanup=False)
    manager.start_manager(cleanup=False)
    wait_for_condition(time=10, condition=condition_manager_idle)

    check_state(False, False)

    # Now try to clear the lock repeatedly.
    assert subprocess.call(["qserver-clear-lock", prefix]) == 0
    assert subprocess.call(["qserver-clear-lock", prefix]) == 0
    check_state(False, False)

    # Pass the Redis address (correct address, same as default)
    assert subprocess.call(["qserver-clear-lock", "--redis-addr=localhost", prefix]) == 0

    # Invalid address (incorrect port). The call fails.
    assert subprocess.call(["qserver-clear-lock", "--redis-addr=localhost:9999", prefix]) == 1

    # Unknown parameter
    assert subprocess.call(["qserver-clear-lock", "--unknown-param=value", prefix]) == 2


# ================================================================================
#                            qserver-zmq-keys


def test_qserver_zmq_keys():
    """
    Test for ``qserver-zmq-keys`` CLI
    """
    # Generate key pair
    assert subprocess.call(["qserver-zmq-keys"]) == SUCCESS

    # Generated public key based on private key - invalid key (exception)
    assert subprocess.call(["qserver-zmq-keys", "--zmq-private-key", "abc"]) == EXCEPTION_OCCURRED

    # Generated public key based on private key - success
    _, private_key = generate_zmq_keys()
    print(f"Private key used for the test: '{private_key}'")
    assert subprocess.call(["qserver-zmq-keys", f"--zmq-private-key={private_key}"]) == SUCCESS
