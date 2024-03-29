import pprint
import random
import time as ttime

import pytest

import bluesky_queueserver

from ..comms import generate_zmq_keys, zmq_single_request
from .common import (  # noqa: F401
    _user,
    _user_group,
    condition_environment_closed,
    condition_environment_created,
    condition_manager_idle,
    condition_manager_paused,
    condition_queue_processing_finished,
    get_manager_status,
    re_manager,
    re_manager_cmd,
    re_manager_pc_copy,
    set_qserver_zmq_public_key,
    wait_for_condition,
    zmq_secure_request,
)

qserver_version = bluesky_queueserver.__version__

# Plans used in most of the tests: '_plan1' and '_plan2' are quickly executed '_plan3' runs for 5 seconds.
_plan1 = {"name": "count", "args": [["det1", "det2"]], "item_type": "plan"}
_plan2 = {"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10], "item_type": "plan"}
_plan3 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 5, "delay": 1}, "item_type": "plan"}
_plan4 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 10, "delay": 1}, "item_type": "plan"}
_instruction_stop = {"name": "queue_stop", "item_type": "instruction"}

# User name and user group name used throughout most of the tests.
_test_user_group = "test_user"

timeout_env_open = 10


# =======================================================================================
#                 Tests for different scenarios of queue execution


def test_zmq_api_queue_execution_1(re_manager):  # noqa: F811
    """
    Execution of a queue that contains an instruction ('queue_stop').
    """

    # Instruction STOP
    params1a = {"item": _instruction_stop, "user": _user, "user_group": _user_group}
    resp1a, _ = zmq_single_request("queue_item_add", params1a)
    assert resp1a["success"] is True, f"resp={resp1a}"
    assert resp1a["msg"] == ""
    assert resp1a["item"]["name"] == "queue_stop"

    # Plan
    params1b = {"item": _plan1, "user": _user, "user_group": _user_group}
    resp1b, _ = zmq_single_request("queue_item_add", params1b)
    assert resp1b["success"] is True, f"resp={resp1b}"

    # Instruction STOP
    params1c = {"item": _instruction_stop, "user": _user, "user_group": _user_group}
    resp1c, _ = zmq_single_request("queue_item_add", params1c)
    assert resp1c["success"] is True, f"resp={resp1c}"
    assert resp1c["msg"] == ""
    assert resp1c["item"]["name"] == "queue_stop"

    # Plan
    params1d = {"item": _plan2, "user": _user, "user_group": _user_group}
    resp1d, _ = zmq_single_request("queue_item_add", params1d)
    assert resp1d["success"] is True, f"resp={resp1d}"

    # The queue contains only a single instruction (stop the queue).
    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp2a, _ = zmq_single_request("status")
    assert resp2a["items_in_queue"] == 4
    assert resp2a["items_in_history"] == 0

    resp3, _ = zmq_single_request("queue_start")
    assert resp3["success"] is True

    assert wait_for_condition(time=5, condition=condition_manager_idle)

    resp3a, _ = zmq_single_request("status")
    assert resp3a["items_in_queue"] == 3
    assert resp3a["items_in_history"] == 0

    resp4, _ = zmq_single_request("queue_start")
    assert resp4["success"] is True

    assert wait_for_condition(time=5, condition=condition_manager_idle)

    resp4a, _ = zmq_single_request("status")
    assert resp4a["items_in_queue"] == 1
    assert resp4a["items_in_history"] == 1

    resp5, _ = zmq_single_request("queue_start")
    assert resp5["success"] is True

    assert wait_for_condition(time=5, condition=condition_queue_processing_finished)

    resp5a, _ = zmq_single_request("status")
    assert resp5a["items_in_queue"] == 0
    assert resp5a["items_in_history"] == 2

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


class UidChecker:
    """
    The class may be used to verify if ``plan_queue_uid`` and ``plan_history_uid``
    changed by calling ``verify_uid_changes`` between operations.
    """

    def __init__(self):
        self.pq_uid, self.ph_uid = self.get_queue_uids()

    def get_queue_uids(self):
        status = get_manager_status()
        return status["plan_queue_uid"], status["plan_history_uid"]

    def verify_uid_changes(self, *, pq_changed, ph_changed):
        """
        Verify if ``plan_queue_uid`` and ``plan_history_uid`` changed
        since the last call to this function or instantiation of the class.

        Parameters
        ----------
        pq_changed : boolean
            indicates if ``plan_queue_uid`` is expected to change since last
            call to this function
        ph_changed : boolean
            indicates if ``plan_history_uid`` is expected to change since last
            call to this function
        """
        pq_uid_new, ph_uid_new = self.get_queue_uids()
        if pq_changed:
            assert pq_uid_new != self.pq_uid
        else:
            assert pq_uid_new == self.pq_uid

        if ph_changed:
            assert ph_uid_new != self.ph_uid
        else:
            assert ph_uid_new == self.ph_uid

        self.pq_uid, self.ph_uid = pq_uid_new, ph_uid_new


def test_zmq_api_queue_execution_2(re_manager):  # noqa: F811
    """
    Test if status fields ``plan_queue_uid`` and ``plan_history_uid`` are properly changed
    during execution of common queue operations.
    """
    uid_checker = UidChecker()

    # Plan
    params1b = {"item": _plan3, "user": _user, "user_group": _user_group}
    resp1b, _ = zmq_single_request("queue_item_add", params1b)
    assert resp1b["success"] is True, f"resp={resp1b}"
    uid_checker.verify_uid_changes(pq_changed=True, ph_changed=False)

    # Plan
    params1d = {"item": _plan3, "user": _user, "user_group": _user_group}
    resp1d, _ = zmq_single_request("queue_item_add", params1d)
    assert resp1d["success"] is True, f"resp={resp1d}"
    uid_checker.verify_uid_changes(pq_changed=True, ph_changed=False)

    # The queue contains only a single instruction (stop the queue).
    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp2a, _ = zmq_single_request("status")
    assert resp2a["items_in_queue"] == 2
    assert resp2a["items_in_history"] == 0

    uid_checker.verify_uid_changes(pq_changed=False, ph_changed=False)

    resp3, _ = zmq_single_request("queue_start")
    assert resp3["success"] is True
    ttime.sleep(1)
    uid_checker.verify_uid_changes(pq_changed=True, ph_changed=False)

    resp3a, _ = zmq_single_request("queue_stop")
    assert resp3a["success"] is True

    assert wait_for_condition(time=20, condition=condition_manager_idle)
    uid_checker.verify_uid_changes(pq_changed=True, ph_changed=True)

    resp3b, _ = zmq_single_request("status")
    assert resp3b["items_in_queue"] == 1
    assert resp3b["items_in_history"] == 1

    resp5, _ = zmq_single_request("queue_start")
    assert resp5["success"] is True
    ttime.sleep(1)
    uid_checker.verify_uid_changes(pq_changed=True, ph_changed=False)

    resp5a, _ = zmq_single_request("re_pause", params={"option": "immediate"})
    assert resp5a["success"] is True, str(resp5a)

    assert wait_for_condition(time=20, condition=condition_manager_paused)
    uid_checker.verify_uid_changes(pq_changed=False, ph_changed=False)

    resp5b, _ = zmq_single_request("re_abort")
    assert resp5b["success"] is True, str(resp5b)

    assert wait_for_condition(time=20, condition=condition_manager_idle)
    uid_checker.verify_uid_changes(pq_changed=True, ph_changed=True)

    resp5a, _ = zmq_single_request("status")
    assert resp5a["items_in_queue"] == 1
    assert resp5a["items_in_history"] == 2

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=30, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("test_mode", ["none", "ev"])
# fmt: on
def test_zmq_api_queue_execution_3(monkeypatch, re_manager_cmd, test_mode):  # noqa: F811
    """
    Test operation of RE Manager and 0MQ API with enabled encryption. Test options to
    set the server (RE Manager) private key using the environment variable.
    """
    public_key, private_key = generate_zmq_keys()

    if test_mode == "none":
        # No encryption
        pass
    elif test_mode == "ev":
        # Set server private key using environment variable
        monkeypatch.setenv("QSERVER_ZMQ_PRIVATE_KEY_FOR_SERVER", private_key)
        set_qserver_zmq_public_key(monkeypatch, server_public_key=public_key)
    else:
        raise RuntimeError(f"Unrecognized test mode '{test_mode}'")

    re_manager_cmd([])

    # Plan
    params1b = {"item": _plan1, "user": _user, "user_group": _user_group}
    resp1b, _ = zmq_secure_request("queue_item_add", params1b)
    assert resp1b["success"] is True, f"resp={resp1b}"

    # Plan
    params1d = {"item": _plan2, "user": _user, "user_group": _user_group}
    resp1d, _ = zmq_secure_request("queue_item_add", params1d)
    assert resp1d["success"] is True, f"resp={resp1d}"

    params = {"user_group": _user_group}
    resp1, _ = zmq_secure_request("plans_allowed", params)
    resp2, _ = zmq_secure_request("devices_allowed", params)
    assert len(resp1["plans_allowed"])
    assert len(resp2["devices_allowed"])

    # The queue contains only a single instruction (stop the queue).
    resp2, _ = zmq_secure_request("environment_open")
    assert resp2["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp2a, _ = zmq_secure_request("status")
    assert resp2a["items_in_queue"] == 2
    assert resp2a["items_in_history"] == 0

    resp3, _ = zmq_secure_request("queue_start")
    assert resp3["success"] is True

    assert wait_for_condition(time=20, condition=condition_queue_processing_finished)

    resp5a, _ = zmq_secure_request("status")
    assert resp5a["items_in_queue"] == 0
    assert resp5a["items_in_history"] == 2

    # Close the environment
    resp6, _ = zmq_secure_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("stop_queue, pause_before_kill", [
    (False, 0.5),
    (False, 9),
    (True, None),
])
# fmt: on
def test_zmq_api_queue_execution_4(re_manager, stop_queue, pause_before_kill):  # noqa: F811
    """
    Test if the manager could be successfully restarted in the following cases:
    - while running 1st plan (of 2), restart is completed before the plan is finished,
      the manager is proceding to the 2nd plan;
    - while running 1st plan, plan is finished before restart is completed, the manager
      is proceding to the 2nd plan once restart is completed;
    - after the 1st plan is completed and the queue is stopped, the manager is idle when
      it is restarted, the queue is restarted again after restart is completed and the 2nd
      plan is executed.
    """

    def _check_status(n_queue, n_hist, m_state, re_state, pause_pend):
        resp, _ = zmq_single_request("status")
        assert resp["items_in_queue"] == n_queue
        assert resp["items_in_history"] == n_hist
        assert resp["manager_state"] == m_state
        assert resp["re_state"] == re_state
        assert resp["pause_pending"] == pause_pend

    # Add 2 plans
    for _ in range(2):
        params1a = {"item": _plan4, "user": _user, "user_group": _user_group}
        resp1a, _ = zmq_single_request("queue_item_add", params1a)
        assert resp1a["success"] is True, f"resp={resp1a}"

    # The queue contains only a single instruction (stop the queue).
    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    _check_status(2, 0, "idle", "idle", False)

    resp3, _ = zmq_single_request("queue_start")
    assert resp3["success"] is True

    if stop_queue:
        ttime.sleep(1)  # Wait for the plan to start
        resp3a, _ = zmq_single_request("queue_stop")
        assert resp3a["success"] is True, f"{resp3a}"

        _check_status(1, 0, "executing_queue", "running", False)

    else:
        ttime.sleep(pause_before_kill)  # ~50% of the second (last) measurement of the 1st plan
        zmq_single_request("manager_kill")
        ttime.sleep(6)  # Wait until the manager is restarted

        if pause_before_kill < 6:
            # Still executing the 1st plan
            _check_status(1, 0, "executing_queue", "running", False)
        else:
            # Executing the 2nd plan
            _check_status(0, 1, "executing_queue", "running", False)

    assert wait_for_condition(time=20, condition=condition_manager_idle)

    if stop_queue:
        zmq_single_request("manager_kill")
        ttime.sleep(6)  # Wait until the manager is restarted
        _check_status(1, 1, "idle", "idle", False)

    # Execute the remaining plans (if any plans left)
    resp4, _ = zmq_single_request("queue_start")
    assert resp4["success"] is True

    assert wait_for_condition(time=20, condition=condition_queue_processing_finished)

    _check_status(0, 2, "idle", "idle", False)

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# =======================================================================================
#                        Execution of malfunctioning plans

script_blocking_plan = """
def blocking_plan(delay=30):
    print("Starting 'freezing_plan'")
    t_start = ttime.time()
    while True:
        # This loop will block Run Engine event loop
        if ttime.time() - t_start > delay:
            break
    yield from bps.sleep(0.1)
"""


def test_blocking_plan_01(re_manager):  # noqa: F811
    """
    Verify that plans that block Run Engine event loop are properly handled.
    The test includes the following steps:
    - open environment;
    - upload script that contains 'blocking' plan;
    - add blocking plan to the queue and start the queue;
    - attempt to pause the blocking plan (it can not be paused, but we can successfully
      submit a request, this operation was known to cause failure of communication between
      manager and worker making the following step impossible);
    - start execution of a function, wait for completion and verify the result;
    - destroy the environment.
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp2, _ = zmq_single_request("script_upload", params={"script": script_blocking_plan})
    assert resp2["success"] is True
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    resp3, _ = zmq_single_request("plans_allowed", params={"user_group": _user_group})
    assert resp3["success"] is True, resp3
    assert "blocking_plan" in resp3["plans_allowed"]

    params = {"item": {"item_type": "plan", "name": "blocking_plan"}}
    params.update({"user": _user, "user_group": _user_group})
    resp4, _ = zmq_single_request("queue_item_add", params=params)
    assert resp4["success"] is True, pprint.pformat(resp4)

    resp5, _ = zmq_single_request("queue_start")
    assert resp4["success"] is True, resp5

    ttime.sleep(1)

    resp6, _ = zmq_single_request("re_pause")
    assert resp6["success"] is True, resp6

    state = get_manager_status()
    assert state["pause_pending"] is True

    ttime.sleep(1)

    delay = 0.5
    func_item = {"name": "function_sleep", "item_type": "function", "args": [delay]}
    params = {"item": func_item, "run_in_background": True, "user": _user, "user_group": _test_user_group}
    resp7, _ = zmq_single_request("function_execute", params=params)
    assert resp7["success"] is True
    task_uid = resp7["task_uid"]

    t_start = ttime.time()
    while True:
        resp8, _ = zmq_single_request("task_status", params={"task_uid": task_uid})
        assert resp8["success"] is True
        if resp8["status"] == "completed":
            break
        assert ttime.time() - t_start < 10
        ttime.sleep(0.1)

    resp9, _ = zmq_single_request("task_result", params={"task_uid": task_uid})
    assert resp9["success"] is True
    assert resp9["status"] == "completed"
    assert resp9["result"]["return_value"] == {"success": True, "time": delay}

    resp10, _ = zmq_single_request("environment_destroy")
    assert resp10["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_closed)


def poll_for_task_completion(task_uid, *, timeout):
    # Simplified polling
    t_start = ttime.time()
    while True:
        resp, _ = zmq_single_request("task_status", params={"task_uid": task_uid})
        assert resp["success"] is True
        if resp["status"] == "completed":
            break
        assert ttime.time() - t_start < timeout
        ttime.sleep(0.5)


script_upload_download_data = """
_data = None

def unit_test_upload_data(data):
    global _data
    _data = data
    return "Data is received"


def unit_test_download_data():
    global _data
    return _data
"""


# fmt: off
@pytest.mark.parametrize("background", [False, True])
# fmt: on
def test_large_datasets_01(re_manager, background):  # noqa: F811
    """
    Submit large array as a function parameter. When running functions on background
    start execution of a plan in the main thread of the worker.
    """
    n_elements, timeout_ms = 1000000, 15000
    vlist = [random.random() for _ in range(n_elements)]

    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp2, _ = zmq_single_request("script_upload", params={"script": script_upload_download_data})
    assert resp2["success"] is True
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    if background:
        params = {"item": _plan4, "user": _user, "user_group": _user_group}
        resp, _ = zmq_single_request("queue_item_add", params)
        assert resp["success"] is True, pprint.pformat(resp)

        resp, _ = zmq_single_request("queue_start")
        assert resp["success"] is True, pprint.pformat(resp)

    func_item = {"name": "unit_test_upload_data", "item_type": "function", "kwargs": {"data": vlist}}
    params = {"item": func_item, "run_in_background": background, "user": _user, "user_group": _test_user_group}
    resp3, _ = zmq_single_request("function_execute", params=params, timeout=timeout_ms)
    assert resp3["success"] is True
    task_uid = resp3["task_uid"]

    poll_for_task_completion(task_uid, timeout=10)

    resp4, _ = zmq_single_request("task_result", params={"task_uid": task_uid})
    assert resp4["success"] is True
    assert resp4["status"] == "completed"
    assert resp4["result"]["return_value"] == "Data is received"

    func_item = {"name": "unit_test_download_data", "item_type": "function"}
    params = {"item": func_item, "run_in_background": background, "user": _user, "user_group": _test_user_group}
    resp5, _ = zmq_single_request("function_execute", params=params, timeout=timeout_ms)
    assert resp5["success"] is True, pprint.pformat(resp5)
    task_uid = resp5["task_uid"]

    poll_for_task_completion(task_uid, timeout=30)

    resp6, _ = zmq_single_request("task_result", params={"task_uid": task_uid})
    assert resp6["success"] is True
    assert resp6["status"] == "completed"
    assert resp6["result"]["return_value"] == vlist

    assert wait_for_condition(time=30, condition=condition_manager_idle)

    status = get_manager_status()
    assert status["items_in_queue"] == 0
    assert status["items_in_history"] == (1 if background else 0)

    resp10, _ = zmq_single_request("environment_destroy")
    assert resp10["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_closed)


script_plan_processing_large_array = """
def plan_large_array(vlist, n):
    assert len(vlist) == n
    yield from bps.sleep(0.1)
"""


def test_large_datasets_02(re_manager):  # noqa: F811
    """
    Submit large array as a plan parameter. Then download the queue that contains the plan.
    Then run the plan.

    NOTE ABOUT THIS TEST !!!
    This is a stress test for RE Manager. THIS WOULD BE A HORRIBLE PRACTICE IN PRODUCTION.
    Never pass large datasets with plan parameters. The plan parameters are kept in the queue
    and downloaded each time the queue is downloaded. Then they are move to plan history and
    stay there until history is cleared. This test is slow with just one such plan, it will
    not work if there are several plans like this in the queue or the history. Keep data passed
    as parameters as as small as possible and pass large datasets using some other method
    (e.g. using functions, which are not kept in the queue or history).
    """
    n_elements, timeout_ms = 1000000, 15000
    vlist = [random.random() for _ in range(n_elements)]

    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp2, _ = zmq_single_request("script_upload", params={"script": script_plan_processing_large_array})
    assert resp2["success"] is True
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    plan = {"name": "plan_large_array", "kwargs": {"vlist": vlist, "n": len(vlist)}, "item_type": "plan"}
    params = {"item": plan, "user": _user, "user_group": _user_group}
    resp, _ = zmq_single_request("queue_item_add", params, timeout=timeout_ms)
    assert resp["success"] is True, pprint.pformat(resp)

    resp4, _ = zmq_single_request("queue_get", timeout=timeout_ms)
    assert resp4["success"] is True, pprint.pformat(resp4)
    assert len(resp4["items"]) == 1
    assert resp4["items"][0]["kwargs"]["vlist"] == vlist
    assert resp4["items"][0]["kwargs"]["n"] == len(vlist)

    resp, _ = zmq_single_request("queue_start")
    assert resp["success"] is True, pprint.pformat(resp)

    assert wait_for_condition(time=30, condition=condition_manager_idle)

    status = get_manager_status()
    assert status["items_in_queue"] == 0
    assert status["items_in_history"] == 1

    resp10, _ = zmq_single_request("environment_destroy")
    assert resp10["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_closed)


# fmt: off
_plan_move_then_count = {
    "name": "move_then_count",
    "kwargs": {"motors": ["motor1", "motor2"], "detectors": ["det1"], "positions": [1, 2]},
    "item_type": "plan",
}


@pytest.mark.parametrize("plan, n_plans, timeout_ms", [
    (_plan4, 10000, 60000),
    (_plan_move_then_count, 10000, 60000),
])
# fmt: on
def test_large_datasets_03(re_manager, plan, n_plans, timeout_ms):  # noqa: F811
    """
    Submit large number of plans to the queue as a batch.

    This is a stress test. Never submit large batches in production.
    """
    plans = [plan] * n_plans
    params = {"items": plans, "user": _user, "user_group": _user_group}

    resp, _ = zmq_single_request("queue_item_add_batch", params, timeout=timeout_ms)
    assert "success" in resp, pprint.pformat(resp)
    assert resp["success"] is True, pprint.pformat(resp)

    status = get_manager_status()
    assert status["items_in_queue"] == n_plans
    assert status["items_in_history"] == 0

    resp, _ = zmq_single_request("queue_get", timeout=timeout_ms)
    assert "success" in resp, pprint.pformat(resp)
    assert resp["success"] is True, pprint.pformat(resp)
    assert len(resp["items"]) == n_plans

    resp, _ = zmq_single_request("queue_clear")
    assert "success" in resp, pprint.pformat(resp)
    assert resp["success"] is True, pprint.pformat(resp)

    status = get_manager_status()
    assert status["items_in_queue"] == 0
    assert status["items_in_history"] == 0
