import time as ttime

import requests
import pytest

from bluesky_queueserver.manager.tests._common import re_manager  # noqa F401
from bluesky_queueserver.server.tests.conftest import (  # noqa F401
    SERVER_ADDRESS,
    SERVER_PORT,
    add_plans_to_queue,
    fastapi_server,
)


def wait_for_environment_to_be_created(timeout, polling_period=0.2):
    """Wait for environment to be created with timeout."""
    time_start = ttime.time()
    while ttime.time() < time_start + timeout:
        ttime.sleep(polling_period)
        resp = _request_to_json("get", "/status")
        if resp["worker_environment_exists"]:
            return True

    return False


def _request_to_json(request_type, path, **kwargs):
    resp = getattr(requests, request_type)(f"http://{SERVER_ADDRESS}:{SERVER_PORT}{path}", **kwargs).json()
    return resp


def test_http_server_ping_handler(re_manager, fastapi_server):  # noqa F811
    resp = _request_to_json("get", "/")
    assert resp["msg"] == "RE Manager"
    assert resp["manager_state"] == "idle"
    assert resp["plans_in_queue"] == 0
    assert resp["running_plan_uid"] is None
    assert resp["worker_environment_exists"] is False


def test_http_server_status_handler(re_manager, fastapi_server):  # noqa F811
    resp = _request_to_json("get", "/status")
    assert resp["msg"] == "RE Manager"
    assert resp["manager_state"] == "idle"
    assert resp["plans_in_queue"] == 0
    assert resp["running_plan_uid"] is None
    assert resp["worker_environment_exists"] is False


def test_http_server_queue_get_handler(re_manager, fastapi_server):  # noqa F811
    resp = _request_to_json("get", "/queue/get")
    assert resp["queue"] == []
    assert resp["running_plan"] == {}


def test_http_server_plans_allowed_and_devices(re_manager, fastapi_server):  # noqa F811
    resp1 = _request_to_json("get", "/plans/allowed")
    assert isinstance(resp1["plans_allowed"], dict)
    assert len(resp1["plans_allowed"]) > 0
    resp2 = _request_to_json("get", "/devices/allowed")
    assert isinstance(resp2["devices_allowed"], dict)
    assert len(resp2["devices_allowed"]) > 0


def test_http_server_queue_plan_add_handler_1(re_manager, fastapi_server):  # noqa F811
    resp1 = _request_to_json(
        "post", "/queue/plan/add", json={"plan": {"name": "count", "args": [["det1", "det2"]]}}
    )
    assert resp1["success"] is True
    assert resp1["qsize"] == 1
    assert resp1["plan"]["name"] == "count"
    assert resp1["plan"]["args"] == [["det1", "det2"]]
    assert "plan_uid" in resp1["plan"]

    resp2 = _request_to_json("get", "/queue/get")
    assert resp2["queue"] != []
    assert len(resp2["queue"]) == 1
    assert resp2["queue"][0] == resp1["plan"]
    assert resp2["running_plan"] == {}


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
    (-1, 1, True),
    (-2, 0, True),
    (-3, 0, True),
    (-100, 0, True),
])
# fmt: on
def test_http_server_queue_plan_add_handler_2(re_manager, fastapi_server, pos, pos_result, success):  # noqa F811

    plan1 = {"name": "count", "args": [["det1"]]}
    plan2 = {"name": "count", "args": [["det1", "det2"]]}

    # Create the queue with 2 entries
    _request_to_json("post", "/queue/plan/add", json={"plan": plan1})
    _request_to_json("post", "/queue/plan/add", json={"plan": plan1})

    # Add another entry at the specified position
    params = {"plan": plan2}
    if pos is not None:
        params.update({"pos": pos})
    resp1 = _request_to_json("post", "/queue/plan/add", json=params)

    assert resp1["success"] is success
    assert resp1["qsize"] == (3 if success else None)
    assert resp1["plan"]["name"] == "count"
    assert resp1["plan"]["args"] == plan2["args"]
    assert "plan_uid" in resp1["plan"]

    resp2 = _request_to_json("get", "/queue/get")

    assert len(resp2["queue"]) == (3 if success else 2)
    assert resp2["running_plan"] == {}

    if success:
        assert resp2["queue"][pos_result]["args"] == plan2["args"]


def test_http_server_queue_plan_add_handler_3(re_manager, fastapi_server):  # noqa F811

    # Unknown plan name
    plan1 = {"plan": {"name": "count_test", "args": [["det1", "det2"]]}}
    resp1 = _request_to_json("post", "/queue/plan/add", json=plan1)
    assert resp1["success"] is False
    assert "Plan 'count_test' is not in the list of allowed plans" in resp1["msg"]

    # Unknown kwarg
    plan2 = {"plan": {"name": "count", "args": [["det1", "det2"]], "kwargs": {"abc": 10}}}
    resp2 = _request_to_json("post", "/queue/plan/add", json=plan2)
    assert resp2["success"] is False
    assert "Failed to add a plan: Plan validation failed: got an unexpected keyword argument 'abc'" in resp2["msg"]

    # Valid plan
    plan3 = {"plan": {"name": "count", "args": [["det1", "det2"]]}}
    resp3 = _request_to_json("post", "/queue/plan/add", json=plan3)
    assert resp3["success"] is True
    assert resp3["qsize"] == 1
    assert resp3["plan"]["name"] == "count"
    assert resp3["plan"]["args"] == [["det1", "det2"]]
    assert "plan_uid" in resp3["plan"]

    resp4 = _request_to_json("get", "/queue/get")
    assert resp4["queue"] != []
    assert len(resp4["queue"]) == 1
    assert resp4["queue"][0] == resp3["plan"]
    assert resp4["running_plan"] == {}


def test_http_server_queue_plan_add_handler_4_fail(re_manager, fastapi_server):  # noqa F811
    """
    Failing case: call without sending a plan.
    """
    resp1 = _request_to_json("post", "/queue/plan/add", json={})
    assert resp1["success"] is False
    assert resp1["qsize"] is None
    assert resp1["plan"] == {}
    assert "no plan is specified" in resp1["msg"]


def test_http_server_queue_plan_get_remove_handler_1(re_manager, fastapi_server, add_plans_to_queue):  # noqa F811

    resp1 = _request_to_json("get", "/queue/get")
    assert resp1["queue"] != []
    assert len(resp1["queue"]) == 3
    assert resp1["running_plan"] == {}

    resp2 = _request_to_json("post", "/queue/plan/get", json={})
    assert resp2["success"] is True
    assert resp2["plan"]["name"] == "count"
    assert resp2["plan"]["args"] == [["det1", "det2"]]
    assert "plan_uid" in resp2["plan"]

    resp3 = _request_to_json("post", "/queue/plan/remove", json={})
    assert resp3["success"] is True
    assert resp3["qsize"] == 2
    assert resp3["plan"]["name"] == "count"
    assert resp3["plan"]["args"] == [["det1", "det2"]]
    assert "plan_uid" in resp3["plan"]


# fmt: off
@pytest.mark.parametrize("pos, pos_result, success", [
    (None, 2, True),
    ("back", 2, True),
    ("front", 0, True),
    ("some", None, False),
    (0, 0, True),
    (1, 1, True),
    (2, 2, True),
    (3, None, False),
    (100, None, False),
    (-1, 2, True),
    (-2, 1, True),
    (-3, 0, True),
    (-4, 0, False),
    (-100, 0, False),
])
# fmt: on
def test_http_server_queue_plan_get_remove_handler_2(
    re_manager, fastapi_server, pos, pos_result, success  # noqa F811
):
    plans = [
        {"name": "count", "args": [["det1"]]},
        {"name": "count", "args": [["det2"]]},
        {"name": "count", "args": [["det1", "det2"]]},
    ]
    for plan in plans:
        _request_to_json("post", "/queue/plan/add", json={"plan": plan})

    # Remove entry at the specified position
    params = {} if pos is None else {"pos": pos}

    # Testing '/queue/plan/get'
    resp1 = _request_to_json("post", "/queue/plan/get", json=params)
    assert resp1["success"] is success
    if success:
        assert resp1["plan"]["args"] == plans[pos_result]["args"]
        assert "plan_uid" in resp1["plan"]
        assert resp1["msg"] == ""
    else:
        assert resp1["plan"] == {}
        assert "Failed to get a plan" in resp1["msg"]

    # Testing '/queue/plan/remove'
    resp2 = _request_to_json("post", "/queue/plan/remove", json=params)
    assert resp2["success"] is success
    assert resp2["qsize"] == (2 if success else None)
    if success:
        assert resp2["plan"]["args"] == plans[pos_result]["args"]
        assert "plan_uid" in resp2["plan"]
        assert resp2["msg"] == ""
    else:
        assert resp2["plan"] == {}
        assert "Failed to remove a plan" in resp2["msg"]

    resp3 = _request_to_json("get", "/queue/get")
    assert len(resp3["queue"]) == (2 if success else 3)
    assert resp3["running_plan"] == {}


def test_http_server_open_environment_handler(re_manager, fastapi_server):  # noqa F811
    resp1 = _request_to_json("post", "/environment/open")
    assert resp1 == {"success": True, "msg": ""}

    assert wait_for_environment_to_be_created(10), "Timeout"

    resp2 = _request_to_json("post", "/environment/open")
    assert resp2 == {"success": False, "msg": "Environment already exists."}


def test_http_server_close_environment_handler(re_manager, fastapi_server):  # noqa F811
    resp1 = _request_to_json("post", "/environment/open")
    assert resp1 == {"success": True, "msg": ""}

    assert wait_for_environment_to_be_created(10), "Timeout"

    resp2 = _request_to_json("post", "/environment/close")
    assert resp2 == {"success": True, "msg": ""}

    ttime.sleep(3)  # TODO: API needed to test if environment is closed. Use delay for now.

    resp3 = _request_to_json("post", "/environment/close")
    assert resp3 == {"success": False, "msg": "RE Worker environment does not exist."}


def test_http_server_queue_start_handler(re_manager, fastapi_server, add_plans_to_queue):  # noqa F811
    resp1 = _request_to_json("post", "/queue/start")
    assert resp1 == {"success": False, "msg": "RE Worker environment does not exist."}

    resp2 = _request_to_json("post", "/environment/open")
    assert resp2 == {"success": True, "msg": ""}
    resp2a = _request_to_json("get", "/queue/get")
    assert len(resp2a["queue"]) == 3
    assert resp2a["running_plan"] == {}

    assert wait_for_environment_to_be_created(10), "Timeout"

    resp3 = _request_to_json("post", "/queue/start")
    assert resp3 == {"success": True, "msg": ""}

    ttime.sleep(1)
    # The plan is currently being executed. 'get_queue' is expected to return currently executed plan.
    resp4 = _request_to_json("get", "/queue/get")
    assert len(resp4["queue"]) == 2
    assert resp4["running_plan"]["name"] == "count"  # Check name of the running plan

    ttime.sleep(25)  # Wait until all plans are executed

    resp4 = _request_to_json("get", "/queue/get")
    assert len(resp4["queue"]) == 0
    assert resp2a["running_plan"] == {}


# fmt: off
@pytest.mark.parametrize("option_pause, option_continue", [
    ("deferred", "resume"),
    ("immediate", "resume"),
    ("deferred", "stop"),
    ("deferred", "abort"),
    ("deferred", "halt")
])
# fmt: on
def test_http_server_re_pause_continue_handlers(
    re_manager, fastapi_server, option_pause, option_continue  # noqa F811
):
    resp1 = _request_to_json("post", "/environment/open")
    assert resp1 == {"success": True, "msg": ""}

    assert wait_for_environment_to_be_created(10), "Timeout"

    resp2 = _request_to_json(
        "post",
        "/queue/plan/add",
        json={"plan": {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 10, "delay": 1}}},
    )
    assert resp2["success"] is True
    assert resp2["qsize"] == 1
    assert resp2["plan"]["name"] == "count"
    assert resp2["plan"]["args"] == [["det1", "det2"]]
    assert "plan_uid" in resp2["plan"]

    resp3 = _request_to_json("post", "/queue/start")
    assert resp3 == {"success": True, "msg": ""}
    ttime.sleep(3.5)  # Let some time pass before pausing the plan (fractional number of seconds)
    resp3a = _request_to_json("post", "/re/pause", json={"option": option_pause})
    assert resp3a == {"msg": "", "success": True}
    ttime.sleep(2)  # TODO: API is needed
    resp3b = _request_to_json("get", "/queue/get")
    assert len(resp3b["queue"]) == 0  # The plan is paused, but it is not in the queue
    assert resp3b["running_plan"] != {}  # Running plan is set

    resp4 = _request_to_json("post", f"/re/{option_continue}")
    assert resp4 == {"msg": "", "success": True}

    ttime.sleep(15)  # TODO: we need to wait for plan completion

    resp4a = _request_to_json("get", "/queue/get")
    # The plan returns to the queue if it is stopped
    assert len(resp4a["queue"]) == 0 if option_continue == "resume" else 1
    assert resp4a["running_plan"] == {}


def test_http_server_close_print_db_uids_handler(re_manager, fastapi_server, add_plans_to_queue):  # noqa F811
    resp1 = _request_to_json("post", "/environment/open")
    assert resp1 == {"success": True, "msg": ""}

    assert wait_for_environment_to_be_created(10), "Timeout"

    resp2 = _request_to_json("post", "/queue/start")
    assert resp2 == {"success": True, "msg": ""}

    ttime.sleep(15)

    resp2a = _request_to_json("get", "/queue/get")
    assert len(resp2a["queue"]) == 0
    assert resp2a["running_plan"] == {}


def test_http_server_clear_queue_handler(re_manager, fastapi_server, add_plans_to_queue):  # noqa F811
    resp1 = _request_to_json("get", "/queue/get")
    assert len(resp1["queue"]) == 3

    resp2 = _request_to_json("post", "/queue/clear")
    assert resp2["success"] is True
    assert resp2["msg"] == "Plan queue is now empty."

    resp3 = _request_to_json("get", "/queue/get")
    assert len(resp3["queue"]) == 0


def test_http_server_plan_history(re_manager, fastapi_server):  # noqa F811
    # Select very short plan
    plan = {"plan": {"name": "count", "args": [["det1", "det2"]]}}
    _request_to_json("post", "/queue/plan/add", json=plan)
    _request_to_json("post", "/queue/plan/add", json=plan)
    _request_to_json("post", "/queue/plan/add", json=plan)

    _request_to_json("post", "/environment/open")
    assert wait_for_environment_to_be_created(10), "Timeout"

    _request_to_json("post", "/queue/start")
    ttime.sleep(5)

    resp1 = _request_to_json("get", "/history/get")
    assert len(resp1["history"]) == 3
    assert resp1["history"][0]["name"] == "count"

    resp2 = _request_to_json("post", "/history/clear")
    assert resp2["success"] is True

    resp3 = _request_to_json("get", "/history/get")
    assert resp3["history"] == []


def test_http_server_manager_kill(re_manager, fastapi_server):  # noqa F811

    _request_to_json("post", "/environment/open")
    assert wait_for_environment_to_be_created(10), "Timeout"

    resp = _request_to_json("post", "/test/manager/kill")
    assert resp["success"] is False
    assert "ZMQ communication error:" in resp["msg"]

    ttime.sleep(10)

    resp = _request_to_json("get", "/status")
    assert resp["msg"] == "RE Manager"
    assert resp["manager_state"] == "idle"
    assert resp["plans_in_queue"] == 0
    assert resp["running_plan_uid"] is None
    assert resp["worker_environment_exists"] is True


# fmt: off
@pytest.mark.parametrize("option", [None, "safe_on", "safe_off"])
# fmt: on
def test_http_server_clear_queue_handler_1(re_manager, fastapi_server, option):  # noqa F811

    _request_to_json("post", "/environment/open")
    assert wait_for_environment_to_be_created(10), "Timeout"

    kwargs = {"json": {"option": option} if option else {}}
    resp1 = _request_to_json("post", "/manager/stop", **kwargs)
    assert resp1["success"] is True

    assert re_manager.check_if_stopped() is True


# fmt: off
@pytest.mark.parametrize("option", [None, "safe_on", "safe_off"])
# fmt: on
def test_http_server_clear_queue_handler_2(re_manager, fastapi_server, add_plans_to_queue, option):  # noqa F811

    _request_to_json("post", "/environment/open")
    assert wait_for_environment_to_be_created(10), "Timeout"

    _request_to_json("post", "/queue/start")

    ttime.sleep(2)
    resp = _request_to_json("get", "/status")
    assert resp["msg"] == "RE Manager"
    assert resp["manager_state"] == "executing_queue"
    assert resp["plans_in_queue"] == 2
    assert resp["running_plan_uid"] is not None
    assert resp["plans_in_history"] == 0
    assert resp["worker_environment_exists"] is True

    # Attempt to stop
    kwargs = {"json": {"option": option} if option else {}}
    resp1 = _request_to_json("post", "/manager/stop", **kwargs)
    assert resp1["success"] == (option == "safe_off")

    if option == "safe_off":
        assert re_manager.check_if_stopped() is True

    else:
        # The queue is expected to be running
        ttime.sleep(15)
        resp = _request_to_json("get", "/status")
        assert resp["msg"] == "RE Manager"
        assert resp["manager_state"] == "idle"
        assert resp["plans_in_queue"] == 0
        assert resp["plans_in_history"] == 3
        assert resp["running_plan_uid"] is None
        assert resp["worker_environment_exists"] is True


# fmt: off
@pytest.mark.parametrize("deactivate", [False, True])
# fmt: on
def test_http_server_queue_stop(re_manager, fastapi_server, add_plans_to_queue, deactivate):  # noqa F811
    """
    Methods ``queue_stop_activate`` and ``queue_stop_deactivate``.
    """
    _request_to_json("post", "/environment/open")
    assert wait_for_environment_to_be_created(10), "Timeout"

    # Queue is not running, so the request is expected to fail
    resp1 = _request_to_json("post", "/queue/stop")
    assert resp1["success"] is False
    status = _request_to_json("get", "/status")
    assert status["queue_stop_pending"] is False

    _request_to_json("post", "/queue/start")
    ttime.sleep(2)
    status = _request_to_json("get", "/status")
    assert status["manager_state"] == "executing_queue"

    resp2 = _request_to_json("post", "/queue/stop")
    assert resp2["success"] is True
    status = _request_to_json("get", "/status")
    assert status["queue_stop_pending"] is True

    if deactivate:
        ttime.sleep(1)

        resp3 = _request_to_json("post", "/queue/stop/cancel")
        assert resp3["success"] is True
        status = _request_to_json("get", "/status")
        assert status["queue_stop_pending"] is False

    ttime.sleep(15)

    status = _request_to_json("get", "/status")
    assert status["manager_state"] == "idle"
    assert status["plans_in_queue"] == (0 if deactivate else 2)
    assert status["plans_in_history"] == (3 if deactivate else 1)
    assert status["running_plan_uid"] is None
    assert status["worker_environment_exists"] is True
    assert status["queue_stop_pending"] is False
