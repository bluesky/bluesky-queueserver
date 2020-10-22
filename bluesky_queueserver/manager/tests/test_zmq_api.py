import pytest
import os
import time as ttime

from bluesky_queueserver.manager.profile_ops import (
    get_default_profile_collection_dir,
    load_allowed_plans_and_devices,
)

from ..comms import zmq_single_request

from ._common import (
    wait_for_condition,
    condition_environment_created,
    condition_queue_processing_finished,
    get_queue_state,
    condition_environment_closed,
)
from ._common import re_manager, re_manager_pc_copy  # noqa: F401

# Plans used in most of the tests: '_plan1' and '_plan2' are quickly executed '_plan3' runs for 5 seconds.
_plan1 = {"name": "count", "args": [["det1", "det2"]]}
_plan2 = {"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10]}
_plan3 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 5, "delay": 1}}

# User name and user group name used throughout most of the tests.
_user, _user_group = "Testing Script", "admin"

_existing_plans_and_devices_fln = "existing_plans_and_devices.yaml"
_user_group_permissions_fln = "user_group_permissions.yaml"

# =======================================================================================
#                             Method 'queue_plan_add'


def test_zmq_api_queue_plan_add_1(re_manager):  # noqa F811
    """
    Basic test for `queue_plan_add` method.
    """
    resp1, _ = zmq_single_request("queue_plan_add", {"plan": _plan1, "user": _user, "user_group": _user_group})
    assert resp1["success"] is True
    assert resp1["qsize"] == 1
    assert resp1["plan"]["name"] == _plan1["name"]
    assert resp1["plan"]["args"] == _plan1["args"]
    assert resp1["plan"]["user"] == _user
    assert resp1["plan"]["user_group"] == _user_group
    assert "plan_uid" in resp1["plan"]

    resp2, _ = zmq_single_request("queue_get")
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
def test_zmq_api_queue_plan_add_2(re_manager, pos, pos_result, success):  # noqa F811

    plan1 = {"name": "count", "args": [["det1"]]}
    plan2 = {"name": "count", "args": [["det1", "det2"]]}

    # Create the queue with 2 entries
    params1 = {"plan": plan1, "user": _user, "user_group": _user_group}
    zmq_single_request("queue_plan_add", params1)
    zmq_single_request("queue_plan_add", params1)

    # Add another entry at the specified position
    params2 = {"plan": plan2, "user": _user, "user_group": _user_group}
    if pos is not None:
        params2.update({"pos": pos})

    resp1, _ = zmq_single_request("queue_plan_add", params2)

    assert resp1["success"] is success
    assert resp1["qsize"] == (3 if success else None)
    assert resp1["plan"]["name"] == "count"
    assert resp1["plan"]["args"] == plan2["args"]
    assert resp1["plan"]["user"] == _user
    assert resp1["plan"]["user_group"] == _user_group
    assert "plan_uid" in resp1["plan"]

    resp2, _ = zmq_single_request("queue_get")

    assert len(resp2["queue"]) == (3 if success else 2)
    assert resp2["running_plan"] == {}

    if success:
        assert resp2["queue"][pos_result]["args"] == plan2["args"]


def test_zmq_api_queue_plan_add_3_fail(re_manager):  # noqa F811

    # Unknown plan name
    plan1 = {"name": "count_test", "args": [["det1", "det2"]]}
    params1 = {"plan": plan1, "user": _user, "user_group": _user_group}
    resp1, _ = zmq_single_request("queue_plan_add", params1)
    assert resp1["success"] is False
    assert "Plan 'count_test' is not in the list of allowed plans" in resp1["msg"]

    # Unknown kwarg
    plan2 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"abc": 10}}
    params2 = {"plan": plan2, "user": _user, "user_group": _user_group}
    resp2, _ = zmq_single_request("queue_plan_add", params2)
    assert resp2["success"] is False
    assert "Failed to add a plan: Plan validation failed: got an unexpected keyword argument 'abc'" in resp2["msg"]

    # User name is not specified
    params3 = {"plan": plan2, "user_group": _user_group}
    resp3, _ = zmq_single_request("queue_plan_add", params3)
    assert resp3["success"] is False
    assert "user name is not specified" in resp3["msg"]

    # User group is not specified
    params4 = {"plan": plan2, "user": _user}
    resp4, _ = zmq_single_request("queue_plan_add", params4)
    assert resp4["success"] is False
    assert "user group is not specified" in resp4["msg"]

    # User group is not specified
    params5 = {"plan": plan2, "user": _user, "user_group": "no_such_group"}
    resp5, _ = zmq_single_request("queue_plan_add", params5)
    assert resp5["success"] is False
    assert "Unknown user group: 'no_such_group'" in resp5["msg"]

    # User group is not specified
    params6 = {"user": _user, "user_group": "no_such_group"}
    resp6, _ = zmq_single_request("queue_plan_add", params6)
    assert resp6["success"] is False
    assert "no plan is specified" in resp6["msg"]

    # Valid plan
    plan7 = {"name": "count", "args": [["det1", "det2"]]}
    params7 = {"plan": plan7, "user": _user, "user_group": _user_group}
    resp7, _ = zmq_single_request("queue_plan_add", params7)
    assert resp7["success"] is True
    assert resp7["qsize"] == 1
    assert resp7["plan"]["name"] == "count"
    assert resp7["plan"]["args"] == [["det1", "det2"]]
    assert resp7["plan"]["user"] == _user
    assert resp7["plan"]["user_group"] == _user_group
    assert "plan_uid" in resp7["plan"]

    resp8, _ = zmq_single_request("queue_get")
    assert resp8["queue"] != []
    assert len(resp8["queue"]) == 1
    assert resp8["queue"][0] == resp7["plan"]
    assert resp8["running_plan"] == {}


# =======================================================================================
#                      Method 'plans_allowed', 'devices_allowed'


def test_zmq_api_plans_allowed_and_devices_allowed_1(re_manager):  # noqa F811
    """
    Basic calls to 'plans_allowed', 'devices_allowed' methods.
    """
    params = {"user_group": _user_group}
    resp1, _ = zmq_single_request("plans_allowed", params)
    assert resp1["success"] is True
    assert resp1["msg"] == ""
    assert isinstance(resp1["plans_allowed"], dict)
    assert len(resp1["plans_allowed"]) > 0
    resp2, _ = zmq_single_request("devices_allowed", params)
    assert resp2["success"] is True
    assert resp2["msg"] == ""
    assert isinstance(resp2["devices_allowed"], dict)
    assert len(resp2["devices_allowed"]) > 0


def test_zmq_api_plans_allowed_and_devices_allowed_2(re_manager):  # noqa F811
    """
    Test that group names are recognized correctly. The number of returned plans and
    devices is compared to the number of plans and devices loaded from the profile
    collection. The functions for loading files and generating lists are tested
    separately somewhere else.
    """

    pc_path = get_default_profile_collection_dir()
    path_epd = os.path.join(pc_path, _existing_plans_and_devices_fln)
    path_up = os.path.join(pc_path, _user_group_permissions_fln)

    allowed_plans, allowed_devices = load_allowed_plans_and_devices(
        path_existing_plans_and_devices=path_epd, path_user_group_permissions=path_up
    )

    # Make sure that the user groups is the same. Otherwise it's a bug.
    assert set(allowed_plans.keys()) == set(allowed_devices.keys())

    group_info = {
        _: {"n_plans": len(allowed_plans[_]), "n_devices": len(allowed_devices[_])} for _ in allowed_plans.keys()
    }

    for group, info in group_info.items():
        params = {"user_group": group}
        resp1, _ = zmq_single_request("plans_allowed", params)
        resp2, _ = zmq_single_request("devices_allowed", params)
        allowed_plans = resp1["plans_allowed"]
        allowed_devices = resp2["devices_allowed"]
        assert len(allowed_plans) == info["n_plans"]
        assert len(allowed_devices) == info["n_devices"]


# fmt: off
@pytest.mark.parametrize("params, message", [
    ({}, "user group is not specified"),
    ({"user_group": "no_such_group"}, "Unknown user group: 'no_such_group'"),
])
# fmt: on
def test_zmq_api_plans_allowed_and_devices_allowed_3_fail(re_manager, params, message):  # noqa F811
    """
    Some failing cases for 'plans_allowed', 'devices_allowed' methods.
    """
    resp1, _ = zmq_single_request("plans_allowed", params)
    assert resp1["success"] is False
    assert message in resp1["msg"]
    assert isinstance(resp1["plans_allowed"], dict)
    assert len(resp1["plans_allowed"]) == 0
    resp2, _ = zmq_single_request("devices_allowed", params)
    assert resp1["success"] is False
    assert message in resp1["msg"]
    assert isinstance(resp2["devices_allowed"], dict)
    assert len(resp2["devices_allowed"]) == 0


# =======================================================================================
#                      Method 'queue_plan_get', 'queue_plan_remove'


def test_zmq_api_queue_plan_get_remove_handler_1(re_manager):  # noqa F811
    """
    Get and remove a plan from the back of the queue
    """
    zmq_single_request("queue_plan_add", {"plan": _plan1, "user": _user, "user_group": _user_group})
    zmq_single_request("queue_plan_add", {"plan": _plan2, "user": _user, "user_group": _user_group})
    zmq_single_request("queue_plan_add", {"plan": _plan3, "user": _user, "user_group": _user_group})

    resp1, _ = zmq_single_request("queue_get")
    assert resp1["queue"] != []
    assert len(resp1["queue"]) == 3
    assert resp1["running_plan"] == {}

    # Get the last plan from the queue
    resp2, _ = zmq_single_request("queue_plan_get")
    assert resp2["success"] is True
    assert resp2["plan"]["name"] == _plan3["name"]
    assert resp2["plan"]["args"] == _plan3["args"]
    assert resp2["plan"]["kwargs"] == _plan3["kwargs"]
    assert "plan_uid" in resp2["plan"]

    # Remove the last plan from the queue
    resp3, _ = zmq_single_request("queue_plan_remove")
    assert resp3["success"] is True
    assert resp3["qsize"] == 2
    assert resp3["plan"]["name"] == "count"
    assert resp3["plan"]["args"] == [["det1", "det2"]]
    assert resp2["plan"]["kwargs"] == _plan3["kwargs"]
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
def test_zmq_api_queue_plan_get_remove_handler_2(re_manager, pos, pos_result, success):  # noqa F811
    """
    Get and remove elements using element position in the queue.
    """

    plans = [
        {"plan_uid": "one", "name": "count", "args": [["det1"]]},
        {"plan_uid": "two", "name": "count", "args": [["det2"]]},
        {"plan_uid": "three", "name": "count", "args": [["det1", "det2"]]},
    ]
    for plan in plans:
        zmq_single_request("queue_plan_add", {"plan": plan, "user": _user, "user_group": _user_group})

    # Remove entry at the specified position
    params = {} if pos is None else {"pos": pos}

    # Testing '/queue/plan/get'
    resp1, _ = zmq_single_request("queue_plan_get", params)
    assert resp1["success"] is success
    if success:
        assert resp1["plan"]["args"] == plans[pos_result]["args"]
        assert "plan_uid" in resp1["plan"]
        assert resp1["msg"] == ""
    else:
        assert resp1["plan"] == {}
        assert "Failed to get a plan" in resp1["msg"]

    # Testing '/queue/plan/remove'
    resp2, _ = zmq_single_request("queue_plan_remove", params)
    assert resp2["success"] is success
    assert resp2["qsize"] == (2 if success else None)
    if success:
        assert resp2["plan"]["args"] == plans[pos_result]["args"]
        assert "plan_uid" in resp2["plan"]
        assert resp2["msg"] == ""
    else:
        assert resp2["plan"] == {}
        assert "Failed to remove a plan" in resp2["msg"]

    resp3, _ = zmq_single_request("queue_get")
    assert len(resp3["queue"]) == (2 if success else 3)
    assert resp3["running_plan"] == {}


def test_zmq_api_queue_plan_get_remove_handler_3(re_manager):  # noqa F811
    """
    Get and remove elements using plan UID. Successful and failing cases.
    """
    zmq_single_request("queue_plan_add", {"plan": _plan3, "user": _user, "user_group": _user_group})
    zmq_single_request("queue_plan_add", {"plan": _plan2, "user": _user, "user_group": _user_group})
    zmq_single_request("queue_plan_add", {"plan": _plan1, "user": _user, "user_group": _user_group})

    resp1, _ = zmq_single_request("queue_get")
    plans_in_queue = resp1["queue"]
    assert len(plans_in_queue) == 3

    # Get and then remove plan 2 from the queue
    uid = plans_in_queue[1]["plan_uid"]
    resp2a, _ = zmq_single_request("queue_plan_get", {"uid": uid})
    assert resp2a["plan"]["plan_uid"] == plans_in_queue[1]["plan_uid"]
    assert resp2a["plan"]["name"] == plans_in_queue[1]["name"]
    assert resp2a["plan"]["args"] == plans_in_queue[1]["args"]
    resp2b, _ = zmq_single_request("queue_plan_remove", {"uid": uid})
    assert resp2b["plan"]["plan_uid"] == plans_in_queue[1]["plan_uid"]
    assert resp2b["plan"]["name"] == plans_in_queue[1]["name"]
    assert resp2b["plan"]["args"] == plans_in_queue[1]["args"]

    # Start the first plan (this removes it from the queue)
    #   Also the rest of the operations will be performed on a running queue.
    resp3, _ = zmq_single_request("environment_open")
    assert resp3["success"] is True
    assert wait_for_condition(
        time=3, condition=condition_environment_created
    ), "Timeout while waiting for environment to be opened"

    resp4, _ = zmq_single_request("queue_start")
    assert resp4["success"] is True

    ttime.sleep(1)
    uid = plans_in_queue[0]["plan_uid"]
    resp5a, _ = zmq_single_request("queue_plan_get", {"uid": uid})
    assert resp5a["success"] is False
    assert "is currently running" in resp5a["msg"]
    resp5b, _ = zmq_single_request("queue_plan_remove", {"uid": uid})
    assert resp5b["success"] is False
    assert "Can not remove a plan which is currently running" in resp5b["msg"]

    uid = "nonexistent"
    resp6a, _ = zmq_single_request("queue_plan_get", {"uid": uid})
    assert resp6a["success"] is False
    assert "not in the queue" in resp6a["msg"]
    resp6b, _ = zmq_single_request("queue_plan_remove", {"uid": uid})
    assert resp6b["success"] is False
    assert "not in the queue" in resp6b["msg"]

    # Remove the last entry
    uid = plans_in_queue[2]["plan_uid"]
    resp7a, _ = zmq_single_request("queue_plan_get", {"uid": uid})
    assert resp7a["success"] is True
    resp7b, _ = zmq_single_request("queue_plan_remove", {"uid": uid})
    assert resp7b["success"] is True

    assert wait_for_condition(
        time=10, condition=condition_queue_processing_finished
    ), "Timeout while waiting for environment to be opened"

    state = get_queue_state()
    assert state["plans_in_queue"] == 0
    assert state["plans_in_history"] == 1

    # Close the environment
    resp8, _ = zmq_single_request("environment_close")
    assert resp8["success"] is True
    assert wait_for_condition(time=5, condition=condition_environment_closed)
