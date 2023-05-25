import pprint
import time as ttime

import pytest

from ..comms import zmq_single_request
from .common import ip_kernel_simple_client  # noqa: F401
from .common import re_manager  # noqa: F401
from .common import re_manager_cmd  # noqa: F401
from .common import (
    _user,
    _user_group,
    append_code_to_last_startup_file,
    condition_environment_closed,
    condition_environment_created,
    condition_ip_kernel_busy,
    condition_ip_kernel_idle,
    condition_manager_idle,
    condition_manager_paused,
    condition_queue_processing_finished,
    copy_default_profile_collection,
    get_queue_state,
    use_ipykernel_for_tests,
    wait_for_condition,
    wait_for_task_result,
)

timeout_env_open = 20

# Plans used in most of the tests: '_plan1' and '_plan2' are quickly executed '_plan3' runs for 5 seconds.
_plan1 = {"name": "count", "args": [["det1", "det2"]], "item_type": "plan"}
_plan2 = {"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10], "item_type": "plan"}
_plan3 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 5, "delay": 1}, "item_type": "plan"}
_plan4 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 10, "delay": 1}, "item_type": "plan"}
_instruction_stop = {"name": "queue_stop", "item_type": "instruction"}

_script_with_ip_features = """
from IPython.core.magic import register_line_magic, register_cell_magic

@register_line_magic
def lmagic(line):
    return line

@register_cell_magic
def cmagic(line, cell):
    return line, cell
"""


def test_ip_kernel_loading_script_01(tmp_path, re_manager_cmd):  # noqa: F811
    """
    Test that the IPython-based worker can load startup code with IPython-specific features,
    and regular worker fails.
    """
    using_ipython = use_ipykernel_for_tests()

    pc_path = copy_default_profile_collection(tmp_path)
    append_code_to_last_startup_file(pc_path, additional_code=_script_with_ip_features)

    params = ["--startup-dir", pc_path]
    re_manager_cmd(params)

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    if not using_ipython:
        assert not wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    else:
        assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

        resp9, _ = zmq_single_request("environment_close")
        assert resp9["success"] is True
        assert resp9["msg"] == ""

        assert wait_for_condition(time=3, condition=condition_environment_closed)


def test_ip_kernel_loading_script_02(re_manager):  # noqa: F811
    """
    Test that the IPython-based worker accepts uploaded scripts with IPython-specific code
    and the regular worker fails.
    """
    using_ipython = use_ipykernel_for_tests()

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp3, _ = zmq_single_request("script_upload", params={"script": _script_with_ip_features})
    assert resp3["success"] is True, pprint.pformat(resp3)

    result = wait_for_task_result(10, resp3["task_uid"])
    if not using_ipython:
        assert result["success"] is False, pprint.pformat(result)
        assert "Failed to execute stript" in result["msg"]
    else:
        assert result["success"] is True, pprint.pformat(result)
        assert result["msg"] == "", pprint.pformat(result)

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("resume_option", ["resume", "stop", "halt", "abort"])
@pytest.mark.parametrize("plan_option", ["queue", "plan"])
# fmt: on
def test_ip_kernel_run_plans_01(re_manager, plan_option, resume_option):  # noqa: F811
    """
    Test basic operations: execute a plan (as part of queue or individually), pause and
    resume/stop/halt/abort the plan. Check that ``ip_kernel_state`` and ``ip_kernel_captured``
    are properly set at every stage.
    """
    using_ipython = use_ipykernel_for_tests()

    def check_status(ip_kernel_state, ip_kernel_captured):
        # Returned status may be used to do additional checks
        status = get_queue_state()
        if isinstance(ip_kernel_state, (str, type(None))):
            ip_kernel_state = [ip_kernel_state]
        assert status["ip_kernel_state"] in ip_kernel_state
        assert status["ip_kernel_captured"] == ip_kernel_captured
        return status

    check_status(None, None)

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    if using_ipython:
        check_status("idle", False)
    else:
        check_status("disabled", True)

    if plan_option in ("queue", "plan"):
        if plan_option == "queue":
            resp, _ = zmq_single_request(
                "queue_item_add", {"item": _plan4, "user": _user, "user_group": _user_group}
            )
            assert resp["success"] is True
            resp, _ = zmq_single_request("queue_start")
            assert resp["success"] is True
        elif plan_option == "plan":
            resp, _ = zmq_single_request(
                "queue_item_execute", {"item": _plan4, "user": _user, "user_group": _user_group}
            )
            assert resp["success"] is True
        else:
            assert False, f"Unsupported option: {plan_option!r}"

        s = get_queue_state()  # Kernel may not be 'captured' at this point
        assert s["manager_state"] in ("starting_queue", "executing_queue")
        assert s["worker_environment_state"] in ("idle", "executing_plan")

        ttime.sleep(1)

        s = check_status("busy" if using_ipython else "disabled", True)
        assert s["manager_state"] == "executing_queue"
        assert s["worker_environment_state"] == "executing_plan"

        ttime.sleep(1)

        resp, _ = zmq_single_request("re_pause")
        assert resp["success"] is True, pprint.pformat(resp)

        wait_for_condition(time=10, condition=condition_manager_paused)

        s = check_status("idle" if using_ipython else "disabled", False if using_ipython else True)
        assert s["manager_state"] == "paused"
        assert s["worker_environment_state"] == "idle"

        resp, _ = zmq_single_request(f"re_{resume_option}")
        assert resp["success"] is True, pprint.pformat(resp)

        if resume_option == "resume":
            s = get_queue_state()  # Kernel may not be 'captured' at this point
            assert s["manager_state"] == "executing_queue"
            assert s["worker_environment_state"] in ("idle", "executing_plan")

            ttime.sleep(1)

            s = check_status("busy" if using_ipython else "disabled", True)
            assert s["manager_state"] == "executing_queue"
            assert s["worker_environment_state"] == "executing_plan"

        assert wait_for_condition(time=20, condition=condition_manager_idle)

        s = get_queue_state()
        n_items_in_queue = 1 if resume_option in ["halt", "abort"] and plan_option == "queue" else 0
        assert s["items_in_queue"] == n_items_in_queue
        assert s["items_in_history"] == 1

        resp, _ = zmq_single_request("history_get")
        assert resp["success"] is True, pprint.pformat(resp)
        history_items = resp["items"]
        exit_status = history_items[0]["result"]["exit_status"]

        es = {"resume": "completed", "stop": "stopped", "abort": "aborted", "halt": "halted"}
        exit_status_expected = es[resume_option]

        assert exit_status == exit_status_expected, pprint.pformat(history_items[0])

    else:
        assert False, f"Unsupported option: {plan_option!r}"

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    check_status(None, None)


# fmt: off
@pytest.mark.parametrize("resume_option", ["resume", "stop", "halt", "abort"])
@pytest.mark.parametrize("plan_option", ["queue", "plan"])
# fmt: on
@pytest.mark.skipif(not use_ipykernel_for_tests(), reason="Test is run only with IPython worker")
def test_ip_kernel_run_plans_02(re_manager, ip_kernel_simple_client, plan_option, resume_option):  # noqa: F811
    """
    Start execute a plan in the manager, pause it, then resume/stop/halt/abort using
    a client directly connected to the IPython kernel.
    """
    using_ipython = use_ipykernel_for_tests()
    assert using_ipython, "The test can be run only in IPython mode"

    def check_status(ip_kernel_state, ip_kernel_captured):
        # Returned status may be used to do additional checks
        status = get_queue_state()
        if isinstance(ip_kernel_state, (str, type(None))):
            ip_kernel_state = [ip_kernel_state]
        assert status["ip_kernel_state"] in ip_kernel_state
        assert status["ip_kernel_captured"] == ip_kernel_captured
        return status

    check_status(None, None)

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    check_status("idle", False)

    item_params = {"item": _plan4, "user": _user, "user_group": _user_group}

    if plan_option == "queue":
        resp, _ = zmq_single_request("queue_item_add", item_params)
        item_params2 = {"item": _plan1, "user": _user, "user_group": _user_group}
        resp, _ = zmq_single_request("queue_item_add", item_params2)
        assert resp["success"] is True
        resp, _ = zmq_single_request("queue_start")
        assert resp["success"] is True
    elif plan_option == "plan":
        resp, _ = zmq_single_request("queue_item_execute", item_params)
        assert resp["success"] is True
    else:
        assert False, f"Unsupported option: {plan_option!r}"

    ttime.sleep(1)

    resp, _ = zmq_single_request("re_pause")
    assert resp["success"] is True, pprint.pformat(resp)

    wait_for_condition(time=10, condition=condition_manager_paused)

    s = check_status("idle", False)
    assert s["manager_state"] == "paused"
    assert s["worker_environment_state"] == "idle"

    ip_kernel_simple_client.start()
    command = f"RE.{resume_option}()"
    ip_kernel_simple_client.execute_with_check(command)

    if resume_option == "resume":
        ttime.sleep(1)
        s = check_status("busy", False)
        assert s["manager_state"] == "paused"
        assert s["worker_environment_state"] == "idle"

    assert wait_for_condition(time=20, condition=condition_manager_idle)

    s = get_queue_state()
    n_items_in_queue = 1 if resume_option in ["halt", "abort"] and plan_option == "queue" else 0
    n_items_in_queue = n_items_in_queue if plan_option == "plan" else n_items_in_queue + 1
    assert s["items_in_queue"] == n_items_in_queue
    assert s["items_in_history"] == 1

    resp, _ = zmq_single_request("history_get")
    assert resp["success"] is True, pprint.pformat(resp)
    history_items = resp["items"]
    exit_status = history_items[0]["result"]["exit_status"]

    # Different set of exit status values (from stop documents)
    es = {"resume": "unknown", "stop": "unknown", "abort": "aborted", "halt": "aborted"}
    exit_status_expected = es[resume_option]

    assert exit_status == exit_status_expected, pprint.pformat(history_items[0])

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    check_status(None, None)


_plan_for_test1 = """
def plan_for_test_fail():
    # Failing plan
    n = 0
    def f(detectors, step, pos_cache):
        nonlocal n
        yield from bps.one_nd_step(detectors, step, pos_cache)
        if n >= 5:
            raise Exception("This plan is designed to fail")
        n += 1
        yield from bps.sleep(1)

    yield from scan([det1], motor1, 0, 10, 11, per_step=f)
"""


# fmt: off
@pytest.mark.parametrize("plan_option", ["queue", "plan"])
# fmt: on
@pytest.mark.skipif(not use_ipykernel_for_tests(), reason="Test is run only with IPython worker")
def test_ip_kernel_run_plans_03(re_manager, ip_kernel_simple_client, plan_option):  # noqa: F811
    """
    Handling of a plan that fails (a run fails). Start execute a plan in the manager, pause it,
    then resume using a client directly connected to the IPython kernel.
    """
    using_ipython = use_ipykernel_for_tests()
    assert using_ipython, "The test can be run only in IPython mode"

    def check_status(ip_kernel_state, ip_kernel_captured):
        # Returned status may be used to do additional checks
        status = get_queue_state()
        if isinstance(ip_kernel_state, (str, type(None))):
            ip_kernel_state = [ip_kernel_state]
        assert status["ip_kernel_state"] in ip_kernel_state
        assert status["ip_kernel_captured"] == ip_kernel_captured
        return status

    check_status(None, None)

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # Add failing plan to the environment
    resp, _ = zmq_single_request("script_upload", params={"script": _plan_for_test1})
    assert resp["success"] is True
    wait_for_condition(time=10, condition=condition_manager_idle)

    check_status("idle", False)

    plan_for_test = {"name": "plan_for_test_fail", "item_type": "plan"}
    item_params = {"item": plan_for_test, "user": _user, "user_group": _user_group}

    if plan_option == "queue":
        resp, _ = zmq_single_request("queue_item_add", item_params)
        assert resp["success"] is True
        resp, _ = zmq_single_request("queue_item_add", item_params)
        assert resp["success"] is True
        resp, _ = zmq_single_request("queue_start")
        assert resp["success"] is True
    elif plan_option == "plan":
        resp, _ = zmq_single_request("queue_item_execute", item_params)
        assert resp["success"] is True
    else:
        assert False, f"Unsupported option: {plan_option!r}"

    ttime.sleep(1)

    resp, _ = zmq_single_request("re_pause")
    assert resp["success"] is True, pprint.pformat(resp)

    wait_for_condition(time=10, condition=condition_manager_paused)

    s = check_status("idle", False)
    assert s["manager_state"] == "paused"
    assert s["worker_environment_state"] == "idle"

    ip_kernel_simple_client.start()
    command = "RE.resume()"
    ip_kernel_simple_client.execute_with_check(command)

    ttime.sleep(1)

    s = check_status("busy", False)
    assert s["manager_state"] == "paused"
    assert s["worker_environment_state"] == "idle"

    assert wait_for_condition(time=20, condition=condition_manager_idle)
    assert wait_for_condition(time=20, condition=condition_ip_kernel_idle)

    s = get_queue_state()
    n_items_in_queue = 0 if plan_option == "plan" else 2
    assert s["items_in_queue"] == n_items_in_queue
    assert s["items_in_history"] == 1

    resp, _ = zmq_single_request("history_get")
    assert resp["success"] is True, pprint.pformat(resp)
    history_items = resp["items"]
    exit_status = history_items[0]["result"]["exit_status"]

    exit_status_expected = "failed"

    assert exit_status == exit_status_expected, pprint.pformat(history_items[0])

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    check_status(None, None)


# fmt: off
@pytest.mark.parametrize("resume_option", ["resume", "stop", "halt", "abort"])
@pytest.mark.parametrize("plan_option", ["queue", "plan"])
# fmt: on
@pytest.mark.skipif(not use_ipykernel_for_tests(), reason="Test is run only with IPython worker")
def test_ip_kernel_run_plans_04(re_manager, ip_kernel_simple_client, plan_option, resume_option):  # noqa: F811
    """
    Start a plan (as part of queue or individually), pause and resume it using IPython client,
    then pause and resume/stop/halt/abort the plan from the manager.
    """
    using_ipython = use_ipykernel_for_tests()
    assert using_ipython, "The test can be run only in IPython mode"

    def check_status(ip_kernel_state, ip_kernel_captured):
        # Returned status may be used to do additional checks
        status = get_queue_state()
        if isinstance(ip_kernel_state, (str, type(None))):
            ip_kernel_state = [ip_kernel_state]
        assert status["ip_kernel_state"] in ip_kernel_state
        assert status["ip_kernel_captured"] == ip_kernel_captured
        return status

    check_status(None, None)

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    check_status("idle", False)

    item_params = {"item": _plan4, "user": _user, "user_group": _user_group}

    if plan_option == "queue":
        resp, _ = zmq_single_request("queue_item_add", item_params)
        item_params2 = {"item": _plan1, "user": _user, "user_group": _user_group}
        resp, _ = zmq_single_request("queue_item_add", item_params2)
        assert resp["success"] is True
        resp, _ = zmq_single_request("queue_start")
        assert resp["success"] is True
    elif plan_option == "plan":
        resp, _ = zmq_single_request("queue_item_execute", item_params)
        assert resp["success"] is True
    else:
        assert False, f"Unsupported option: {plan_option!r}"

    ttime.sleep(1)

    resp, _ = zmq_single_request("re_pause")
    assert resp["success"] is True, pprint.pformat(resp)

    wait_for_condition(time=10, condition=condition_manager_paused)

    s = check_status("idle", False)
    assert s["manager_state"] == "paused"
    assert s["worker_environment_state"] == "idle"

    ip_kernel_simple_client.start()
    command = "RE.resume()"
    ip_kernel_simple_client.execute_with_check(command)

    wait_for_condition(time=10, condition=condition_ip_kernel_busy)

    s = check_status("busy", False)
    assert s["manager_state"] == "paused"
    assert s["worker_environment_state"] == "idle"

    resp, _ = zmq_single_request("re_pause")
    assert resp["success"] is True, pprint.pformat(resp)

    wait_for_condition(time=10, condition=condition_ip_kernel_idle)

    s = check_status("idle", False)
    assert s["manager_state"] == "paused"
    assert s["worker_environment_state"] == "idle"

    resp, _ = zmq_single_request(f"re_{resume_option}")
    assert resp["success"] is True, pprint.pformat(resp)

    if resume_option == "resume":
        s = get_queue_state()  # Kernel may not be 'captured' at this point
        assert s["manager_state"] == "executing_queue"
        assert s["worker_environment_state"] in ("idle", "executing_plan")

        ttime.sleep(2)

        s = check_status("busy", True)
        assert s["manager_state"] == "executing_queue"
        assert s["worker_environment_state"] == "executing_plan"

    assert wait_for_condition(time=20, condition=condition_manager_idle)

    s = get_queue_state()

    if resume_option in ["halt", "abort"]:
        n_items_in_queue = 0 if plan_option == "plan" else 2
        n_items_in_history = 1
    elif resume_option == "resume":
        n_items_in_queue = 0
        n_items_in_history = 1 if plan_option == "plan" else 2
    elif resume_option == "stop":
        n_items_in_queue = 0 if plan_option == "plan" else 1
        n_items_in_history = 1
    else:
        assert False, f"Unknown resume option: {resume_option!r}"

    # n_items_in_queue = 1 if resume_option in ["halt", "abort"] and plan_option == "queue" else 0
    # n_items_in_queue = n_items_in_queue if plan_option == "plan" else n_items_in_queue + 1
    assert s["items_in_queue"] == n_items_in_queue
    assert s["items_in_history"] == n_items_in_history

    resp, _ = zmq_single_request("history_get")
    assert resp["success"] is True, pprint.pformat(resp)
    history_items = resp["items"]
    exit_status = history_items[0]["result"]["exit_status"]

    es = {"resume": "completed", "stop": "stopped", "abort": "aborted", "halt": "halted"}
    exit_status_expected = es[resume_option]

    assert exit_status == exit_status_expected, pprint.pformat(history_items[0])

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    check_status(None, None)


# fmt: off
@pytest.mark.parametrize("option", ["function", "script"])
@pytest.mark.parametrize("run_in_background", [False, True])
# fmt: on
def test_ip_kernel_execute_tasks_01(re_manager, option, run_in_background):  # noqa: F811
    """
    Test basic operations: execute a function or a script as a foreground or background task.
    Check that ``ip_kernel_state`` and ``ip_kernel_captured`` are properly set at every stage.
    """
    using_ipython = use_ipykernel_for_tests()

    def check_status(ip_kernel_state, ip_kernel_captured):
        # Returned status may be used to do additional checks
        status = get_queue_state()
        if isinstance(ip_kernel_state, (str, type(None))):
            ip_kernel_state = [ip_kernel_state]
        assert status["ip_kernel_state"] in ip_kernel_state
        assert status["ip_kernel_captured"] == ip_kernel_captured
        return status

    check_status(None, None)

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    check_status("idle" if using_ipython else "disabled", False if using_ipython else True)

    if option == "function":
        # Upload a script with a function function
        script = "def func_for_test():\n    import time\n    time.sleep(3)"
        resp, _ = zmq_single_request("script_upload", params={"script": script})
        assert resp["success"] is True
        wait_for_condition(time=3, condition=condition_manager_idle)

        # Make sure that RE Manager and Worker are in the correct state
        s = check_status("idle" if using_ipython else "disabled", False if using_ipython else True)
        assert s["manager_state"] == "idle"
        assert s["worker_environment_state"] == "idle"

        wait_for_condition(time=30, condition=condition_manager_idle)

        func_info = {"name": "func_for_test", "item_type": "function"}
        resp, _ = zmq_single_request(
            "function_execute",
            params={
                "item": func_info,
                "user": _user,
                "user_group": _user_group,
                "run_in_background": run_in_background,
            },
        )
        assert resp["success"] is True, pprint.pformat(resp)
        task_uid = resp["task_uid"]

    elif option == "script":
        script = "import time\ntime.sleep(3)"
        resp, _ = zmq_single_request(
            "script_upload", params={"script": script, "run_in_background": run_in_background}
        )
        assert resp["success"] is True
        task_uid = resp["task_uid"]

    else:
        assert False, f"Unsupported option: {option!r}"

    if not run_in_background:
        s = get_queue_state()  # Kernel may or may not be captured at this point
        assert s["manager_state"] == "executing_task"
        assert s["worker_environment_state"] in ("idle", "executing_task")

        ttime.sleep(1)

        s = check_status("busy" if using_ipython else "disabled", True)
        assert s["manager_state"] == "executing_task"
        assert s["worker_environment_state"] == "executing_task"
    else:
        s = check_status("idle" if using_ipython else "disabled", False if using_ipython else True)
        assert s["manager_state"] == "idle"
        assert s["worker_environment_state"] == "idle"

        ttime.sleep(1)

        s = check_status("idle" if using_ipython else "disabled", False if using_ipython else True)
        assert s["manager_state"] == "idle"
        assert s["worker_environment_state"] == "idle"

    assert wait_for_task_result(10, task_uid)

    s = check_status("idle" if using_ipython else "disabled", False if using_ipython else True)
    assert s["manager_state"] == "idle"
    assert s["worker_environment_state"] == "idle"

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    check_status(None, None)


def test_ip_kernel_execute_tasks_02(re_manager):  # noqa: F811
    """
    Test basic operations: Execute multiple foreground tasks in a row.
    Check that ``ip_kernel_state`` and ``ip_kernel_captured`` are properly set at every stage.
    """
    using_ipython = use_ipykernel_for_tests()

    def check_status(ip_kernel_state, ip_kernel_captured):
        # Returned status may be used to do additional checks
        status = get_queue_state()
        if isinstance(ip_kernel_state, (str, type(None))):
            ip_kernel_state = [ip_kernel_state]
        assert status["ip_kernel_state"] in ip_kernel_state
        assert status["ip_kernel_captured"] == ip_kernel_captured
        return status

    check_status(None, None)

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    check_status("idle" if using_ipython else "disabled", False if using_ipython else True)

    script = "test_v = 0\ndef func_for_test():\n    return test_v"
    resp, _ = zmq_single_request("script_upload", params={"script": script})
    assert resp["success"] is True
    wait_for_condition(time=3, condition=condition_manager_idle)

    for _ in range(3):
        script = "test_v += 1"
        resp, _ = zmq_single_request("script_upload", params={"script": script})
        assert resp["success"] is True
        wait_for_condition(time=3, condition=condition_manager_idle)

    func_info = {"name": "func_for_test", "item_type": "function"}
    resp, _ = zmq_single_request(
        "function_execute",
        params={"item": func_info, "user": _user, "user_group": _user_group},
    )
    assert resp["success"] is True, pprint.pformat(resp)
    task_uid1 = resp["task_uid"]
    wait_for_condition(time=3, condition=condition_manager_idle)

    for _ in range(3):
        script = "test_v += 1"
        resp, _ = zmq_single_request("script_upload", params={"script": script})
        assert resp["success"] is True
        wait_for_condition(time=3, condition=condition_manager_idle)

    resp, _ = zmq_single_request(
        "function_execute",
        params={"item": func_info, "user": _user, "user_group": _user_group},
    )
    assert resp["success"] is True, pprint.pformat(resp)
    task_uid2 = resp["task_uid"]
    wait_for_condition(time=3, condition=condition_manager_idle)

    # Make sure that the tests were executed correctly
    resp, _ = zmq_single_request("task_result", params={"task_uid": task_uid1})
    assert resp["success"] is True
    value1 = resp["result"]["return_value"]

    resp, _ = zmq_single_request("task_result", params={"task_uid": task_uid2})
    assert resp["success"] is True
    value2 = resp["result"]["return_value"]

    assert value1 == 3
    assert value2 == 6

    s = check_status("idle" if using_ipython else "disabled", False if using_ipython else True)
    assert s["manager_state"] == "idle"
    assert s["worker_environment_state"] == "idle"

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    check_status(None, None)


@pytest.mark.skipif(not use_ipykernel_for_tests(), reason="Test is run only with IPython worker")
def test_ip_kernel_direct_connection_01(re_manager, ip_kernel_simple_client):  # noqa: F811
    """
    Basic test: start a task by connecting directly to IP Kernel. Make sure that
    status reflects 'busy' state of the kernel.
    """
    using_ipython = use_ipykernel_for_tests()
    assert using_ipython, "The test can be run only in IPython mode"

    def check_status(ip_kernel_state, ip_kernel_captured):
        # Returned status may be used to do additional checks
        status = get_queue_state()
        if isinstance(ip_kernel_state, (str, type(None))):
            ip_kernel_state = [ip_kernel_state]
        assert status["ip_kernel_state"] in ip_kernel_state
        assert status["ip_kernel_captured"] == ip_kernel_captured
        return status

    check_status(None, None)

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    ip_kernel_simple_client.start()

    command = "print('Started')\nimport time\ntime.sleep(3)\nprint('Finished')"
    ip_kernel_simple_client.execute_with_check(command)

    ttime.sleep(1)

    s = check_status("busy", False)
    assert s["manager_state"] == "idle"
    assert s["worker_environment_state"] == "idle"

    wait_for_condition(15, condition_ip_kernel_idle)

    s = check_status("idle", False)
    assert s["manager_state"] == "idle"
    assert s["worker_environment_state"] == "idle"

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    check_status(None, None)


# fmt: off
@pytest.mark.parametrize("delay", [0, 1])
@pytest.mark.parametrize("plan_option", ["queue", "plan"])
# fmt: on
@pytest.mark.skipif(not use_ipykernel_for_tests(), reason="Test is run only with IPython worker")
def test_ip_kernel_direct_connection_02(re_manager, ip_kernel_simple_client, plan_option, delay):  # noqa: F811
    """
    Basic test: attempt to start a plan while the externally started task is running.
    """
    using_ipython = use_ipykernel_for_tests()
    assert using_ipython, "The test can be run only in IPython mode"

    def check_status(ip_kernel_state, ip_kernel_captured):
        # Returned status may be used to do additional checks
        status = get_queue_state()
        if isinstance(ip_kernel_state, (str, type(None))):
            ip_kernel_state = [ip_kernel_state]
        assert status["ip_kernel_state"] in ip_kernel_state
        assert status["ip_kernel_captured"] == ip_kernel_captured
        return status

    check_status(None, None)

    if plan_option == "queue":
        resp, _ = zmq_single_request(
            "queue_item_add", {"item": _plan3, "user": _user, "user_group": _user_group}
        )
        assert resp["success"] is True

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    ip_kernel_simple_client.start()

    command = "print('Start sleep')\nimport time\ntime.sleep(3)\nprint('Sleep finished')"
    ip_kernel_simple_client.execute_with_check(command)

    ttime.sleep(delay)

    if plan_option == "queue":
        resp, _ = zmq_single_request("queue_start")
    elif plan_option == "plan":
        resp, _ = zmq_single_request(
            "queue_item_execute", {"item": _plan3, "user": _user, "user_group": _user_group}
        )
    else:
        assert False, f"Unsupported option: {plan_option!r}"

    n_history_items_expected = 1

    if delay > 0.6:
        assert resp["success"] is False
        assert "IPython kernel (RE Worker) is busy" in resp["msg"]
        check_status("busy", False)

    else:
        ttime.sleep(1)  # Wait until the request is processed
        s = get_queue_state()
        request_failed = resp["success"] is False
        queue_not_started = s["manager_state"] != "executing_queue"
        assert request_failed or queue_not_started, (request_failed, queue_not_started)

        if not request_failed:
            n_history_items_expected = 2

    assert wait_for_condition(10, condition_ip_kernel_idle)

    # External tasks are finished. Now try running the plan.
    if plan_option == "queue":
        resp, _ = zmq_single_request("queue_start")
        assert resp["success"] is True, pprint.pformat(resp)
    elif plan_option == "plan":
        resp, _ = zmq_single_request(
            "queue_item_execute", {"item": _plan1, "user": _user, "user_group": _user_group}
        )
        assert resp["success"] is True, pprint.pformat(resp)
    else:
        assert False, f"Unsupported option: {plan_option!r}"

    assert wait_for_condition(time=10, condition=condition_queue_processing_finished)

    s = check_status("idle", False)
    assert s["items_in_queue"] == 0
    assert s["items_in_history"] == n_history_items_expected

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    check_status(None, None)


# fmt: off
@pytest.mark.parametrize("delay", [0, 1])
@pytest.mark.parametrize("option", ["function", "script"])
# fmt: on
@pytest.mark.skipif(not use_ipykernel_for_tests(), reason="Test is run only with IPython worker")
def test_ip_kernel_direct_connection_03(re_manager, ip_kernel_simple_client, option, delay):  # noqa: F811
    """
    Basic test: attempt to start a plan while the externally started task is running.
    """
    using_ipython = use_ipykernel_for_tests()
    assert using_ipython, "The test can be run only in IPython mode"

    def check_status(ip_kernel_state, ip_kernel_captured):
        # Returned status may be used to do additional checks
        status = get_queue_state()
        if isinstance(ip_kernel_state, (str, type(None))):
            ip_kernel_state = [ip_kernel_state]
        assert status["ip_kernel_state"] in ip_kernel_state
        assert status["ip_kernel_captured"] == ip_kernel_captured
        return status

    check_status(None, None)

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    if option == "function":
        # Upload a script with a function
        script = "def func_for_test():\n    ttime.sleep(0.5)"
        resp, _ = zmq_single_request("script_upload", params={"script": script})
        assert resp["success"] is True, pprint.pformat(resp)
        wait_for_condition(time=3, condition=condition_manager_idle)

    func_info = {"name": "func_for_test", "item_type": "function"}
    func_params = {"item": func_info, "user": _user, "user_group": _user_group}
    func_params_bckg = {"run_in_background": True}
    test_script = "ttime.sleep(0.5)"

    ip_kernel_simple_client.start()

    command = "print('Start sleep')\nimport time\ntime.sleep(3)\nprint('Sleep finished')"
    ip_kernel_simple_client.execute_with_check(command)

    ttime.sleep(delay)

    if option == "function":
        resp1, _ = zmq_single_request("function_execute", params=func_params)
        resp2, _ = zmq_single_request("function_execute", params=dict(**func_params, **func_params_bckg))
    elif option == "script":
        resp1, _ = zmq_single_request("script_upload", params={"script": test_script})
        resp2, _ = zmq_single_request("script_upload", params=dict(script=test_script, **func_params_bckg))
    else:
        assert False, f"Unsupported option: {option!r}"

    task_uid1 = None
    task_uid2 = resp2["task_uid"]
    if delay > 0.6:
        assert resp1["success"] is False
        assert "IPython kernel (RE Worker) is busy" in resp1["msg"]
        check_status("busy", False)

    else:
        s = get_queue_state()
        request_failed = resp1["success"] is False
        queue_not_started = s["manager_state"] != "executing_task"
        assert request_failed or not queue_not_started, (request_failed, queue_not_started)

        if not request_failed:
            task_uid1 = resp1["task_uid"]

    assert wait_for_task_result(10, task_uid2)

    if task_uid1:
        resp, _ = zmq_single_request("task_result", params={"task_uid": task_uid1})
        assert resp["success"] is True
        assert resp["result"]["msg"] != "", pprint.pformat(resp)

    resp, _ = zmq_single_request("task_result", params={"task_uid": task_uid2})
    assert resp["success"] is True
    assert resp["result"]["msg"] == "", pprint.pformat(resp)

    assert wait_for_condition(10, condition_ip_kernel_idle)

    # External tasks are finished. Now try running the plan.
    if option == "function":
        resp3, _ = zmq_single_request("function_execute", params=func_params)
    elif option == "script":
        resp3, _ = zmq_single_request("script_upload", params={"script": test_script})
    else:
        assert False, f"Unsupported option: {option!r}"

    assert resp3["success"] is True

    task_uid3 = resp3["task_uid"]
    assert wait_for_task_result(10, task_uid3)

    resp, _ = zmq_single_request("task_result", params={"task_uid": task_uid3})
    assert resp["success"] is True
    assert resp["result"]["msg"] == "", pprint.pformat(resp)

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    check_status(None, None)
