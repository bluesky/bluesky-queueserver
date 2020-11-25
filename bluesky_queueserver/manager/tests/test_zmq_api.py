import pytest
import os
import time as ttime

from bluesky_queueserver.manager.profile_ops import (
    get_default_profile_collection_dir,
    load_allowed_plans_and_devices,
)

from bluesky_queueserver.manager.plan_queue_ops import PlanQueueOperations

from ..comms import zmq_single_request

from ._common import (
    wait_for_condition,
    condition_environment_created,
    condition_queue_processing_finished,
    get_queue_state,
    condition_environment_closed,
    condition_manager_idle,
)
from ._common import re_manager, re_manager_pc_copy  # noqa: F401

# Plans used in most of the tests: '_plan1' and '_plan2' are quickly executed '_plan3' runs for 5 seconds.
_plan1 = {"name": "count", "args": [["det1", "det2"]]}
_plan2 = {"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10]}
_plan3 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 5, "delay": 1}}
_instruction_stop = {"action": "queue_stop"}

# User name and user group name used throughout most of the tests.
_user, _user_group = "Testing Script", "admin"

_existing_plans_and_devices_fln = "existing_plans_and_devices.yaml"
_user_group_permissions_fln = "user_group_permissions.yaml"


# =======================================================================================
#                   Methods 'environment_open', 'environment_close'
def test_zmq_api_environment_open_close_1(re_manager):  # noqa F811
    """
    Basic test for `environment_open` and `environment_close` methods.
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert resp1["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_created)

    resp2, _ = zmq_single_request("environment_close")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)


def test_zmq_api_environment_open_close_2(re_manager):  # noqa F811
    """
    Test for `environment_open` and `environment_close` methods.
    Opening/closing the environment while it is being opened/closed.
    Opening the environment that already exists.
    Closing the environment that does not exist.
    """
    resp1a, _ = zmq_single_request("environment_open")
    assert resp1a["success"] is True
    # Attempt to open the environment before the previous operation is completed
    resp1b, _ = zmq_single_request("environment_open")
    assert resp1b["success"] is False
    assert "in the process of creating the RE Worker environment" in resp1b["msg"]

    assert wait_for_condition(time=3, condition=condition_environment_created)

    # Attempt to open the environment while it already exists
    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is False
    assert "RE Worker environment already exists" in resp2["msg"]

    resp3a, _ = zmq_single_request("environment_close")
    assert resp3a["success"] is True
    # The environment is being closed.
    resp3b, _ = zmq_single_request("environment_close")
    assert resp3b["success"] is False
    assert "in the process of closing the RE Worker environment" in resp3b["msg"]

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    # The environment is already closed.
    resp4, _ = zmq_single_request("environment_close")
    assert resp4["success"] is False
    assert "RE Worker environment does not exist" in resp4["msg"]


def test_zmq_api_environment_open_close_3(re_manager):  # noqa F811
    """
    Test for `environment_open` and `environment_close` methods.
    Closing the environment while a plan is running.
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert resp1["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_created)

    # Start a plan
    resp2, _ = zmq_single_request("queue_plan_add", {"plan": _plan3, "user": _user, "user_group": _user_group})
    assert resp2["success"] is True
    resp3, _ = zmq_single_request("queue_start")
    assert resp3["success"] is True

    # Try to close the environment while the plan is running
    resp4, _ = zmq_single_request("environment_close")
    assert resp4["success"] is False
    assert "Queue execution is in progress" in resp4["msg"]

    assert wait_for_condition(time=20, condition=condition_queue_processing_finished)

    resp2, _ = zmq_single_request("environment_close")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)


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
    resp0a, _ = zmq_single_request("queue_plan_add", params1)
    assert resp0a["success"] is True
    resp0b, _ = zmq_single_request("queue_plan_add", params1)
    assert resp0b["success"] is True

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


def test_zmq_api_queue_plan_add_3(re_manager):  # noqa F811
    plan1 = {"name": "count", "args": [["det1"]]}
    plan2 = {"name": "count", "args": [["det1", "det2"]]}
    plan3 = {"name": "count", "args": [["det2"]]}

    params = {"plan": plan1, "user": _user, "user_group": _user_group}
    resp0a, _ = zmq_single_request("queue_plan_add", params)
    assert resp0a["success"] is True
    params = {"plan": plan2, "user": _user, "user_group": _user_group}
    resp0b, _ = zmq_single_request("queue_plan_add", params)
    assert resp0b["success"] is True

    base_plans = zmq_single_request("queue_get")[0]["queue"]

    params = {"plan": plan3, "after_uid": base_plans[0]["plan_uid"], "user": _user, "user_group": _user_group}
    resp1, _ = zmq_single_request("queue_plan_add", params)
    assert resp1["success"] is True
    assert resp1["qsize"] == 3
    uid1 = resp1["plan"]["plan_uid"]
    resp1a, _ = zmq_single_request("queue_get")
    assert len(resp1a["queue"]) == 3
    assert resp1a["queue"][1]["plan_uid"] == uid1
    resp1b, _ = zmq_single_request("queue_plan_remove", {"uid": uid1})
    assert resp1b["success"] is True

    params = {"plan": plan3, "before_uid": base_plans[1]["plan_uid"], "user": _user, "user_group": _user_group}
    resp2, _ = zmq_single_request("queue_plan_add", params)
    assert resp2["success"] is True
    uid2 = resp2["plan"]["plan_uid"]
    resp2a, _ = zmq_single_request("queue_get")
    assert len(resp2a["queue"]) == 3
    assert resp2a["queue"][1]["plan_uid"] == uid2
    resp2b, _ = zmq_single_request("queue_plan_remove", {"uid": uid2})
    assert resp2b["success"] is True

    # Non-existing uid
    params = {"plan": plan3, "before_uid": "non-existing-uid", "user": _user, "user_group": _user_group}
    resp2, _ = zmq_single_request("queue_plan_add", params)
    assert resp2["success"] is False
    assert "is not in the queue" in resp2["msg"]

    # Ambiguous parameters
    params = {"plan": plan3, "pos": 1, "before_uid": uid2, "user": _user, "user_group": _user_group}
    resp2, _ = zmq_single_request("queue_plan_add", params)
    assert resp2["success"] is False
    assert "Ambiguous parameters" in resp2["msg"]

    # Ambiguous parameters
    params = {"plan": plan3, "before_uid": uid2, "after_uid": uid2, "user": _user, "user_group": _user_group}
    resp2, _ = zmq_single_request("queue_plan_add", params)
    assert resp2["success"] is False
    assert "Ambiguous parameters" in resp2["msg"]


def test_zmq_api_queue_plan_add_4(re_manager):  # noqa F811
    """
    Try inserting plans before and after the running plan
    """
    params = {"plan": _plan3, "user": _user, "user_group": _user_group}
    resp0a, _ = zmq_single_request("queue_plan_add", params)
    assert resp0a["success"] is True
    params = {"plan": _plan3, "user": _user, "user_group": _user_group}
    resp0b, _ = zmq_single_request("queue_plan_add", params)
    assert resp0b["success"] is True

    base_plans = zmq_single_request("queue_get")[0]["queue"]
    uid = base_plans[0]["plan_uid"]

    # Start the first plan (this removes it from the queue)
    #   Also the rest of the operations will be performed on a running queue.
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(
        time=3, condition=condition_environment_created
    ), "Timeout while waiting for environment to be opened"

    resp2, _ = zmq_single_request("queue_start")
    assert resp2["success"] is True

    ttime.sleep(1)

    # Try to insert a plan before the running plan
    params = {"plan": _plan3, "before_uid": uid, "user": _user, "user_group": _user_group}
    resp3, _ = zmq_single_request("queue_plan_add", params)
    assert resp3["success"] is False
    assert "Can not insert a plan in the queue before a currently running plan" in resp3["msg"]

    # Insert the plan after the running plan
    params = {"plan": _plan3, "after_uid": uid, "user": _user, "user_group": _user_group}
    resp4, _ = zmq_single_request("queue_plan_add", params)
    assert resp4["success"] is True

    assert wait_for_condition(
        time=20, condition=condition_queue_processing_finished
    ), "Timeout while waiting for environment to be opened"

    state = get_queue_state()
    assert state["plans_in_queue"] == 0
    assert state["plans_in_history"] == 3

    # Close the environment
    resp5, _ = zmq_single_request("environment_close")
    assert resp5["success"] is True
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_zmq_api_queue_plan_add_5(re_manager):  # noqa: F811
    """
    Make sure that the new plan UID is generated when the plan is added
    """
    plan1 = {"name": "count", "args": [["det1", "det2"]]}

    # Set plan UID. This UID is expected to be replaced when the plan is added
    plan1["plan_uid"] = PlanQueueOperations.new_item_uid()

    params1 = {"plan": plan1, "user": _user, "user_group": _user_group}
    resp1, _ = zmq_single_request("queue_plan_add", params1)
    assert resp1["success"] is True
    assert resp1["msg"] == ""
    assert resp1["plan"]["plan_uid"] != plan1["plan_uid"]


def test_zmq_api_queue_plan_add_6(re_manager):  # noqa: F811
    """
    Add instruction ('queue_stop') to the queue.
    """

    params1a = {"plan": _plan1, "user": _user, "user_group": _user_group}
    resp1a, _ = zmq_single_request("queue_plan_add", params1a)
    assert resp1a["success"] is True, f"resp={resp1a}"

    params1 = {"instruction": _instruction_stop, "user": _user, "user_group": _user_group}
    resp1, _ = zmq_single_request("queue_plan_add", params1)
    assert resp1["success"] is True, f"resp={resp1}"
    assert resp1["msg"] == ""
    assert resp1["instruction"]["action"] == "queue_stop"

    params1c = {"plan": _plan2, "user": _user, "user_group": _user_group}
    resp1c, _ = zmq_single_request("queue_plan_add", params1c)
    assert resp1c["success"] is True, f"resp={resp1c}"

    resp2, _ = zmq_single_request("queue_get")
    assert len(resp2["queue"]) == 3
    assert resp2["queue"][0]["item_type"] == "plan"
    assert resp2["queue"][1]["item_type"] == "instruction"
    assert resp2["queue"][2]["item_type"] == "plan"


def test_zmq_api_queue_plan_add_7_fail(re_manager):  # noqa F811

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
    assert "plan" not in resp6
    assert "instruction" not in resp6
    assert "Incorrect request format: request contains no item info." in resp6["msg"]

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


def test_zmq_api_queue_plan_get_remove_1(re_manager):  # noqa F811
    """
    Get and remove a plan from the back of the queue
    """
    plans = [_plan1, _plan2, _plan3]
    for plan in plans:
        resp0, _ = zmq_single_request("queue_plan_add", {"plan": plan, "user": _user, "user_group": _user_group})
        assert resp0["success"] is True

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
def test_zmq_api_queue_plan_get_remove_2(re_manager, pos, pos_result, success):  # noqa F811
    """
    Get and remove elements using element position in the queue.
    """

    plans = [
        {"plan_uid": "one", "name": "count", "args": [["det1"]]},
        {"plan_uid": "two", "name": "count", "args": [["det2"]]},
        {"plan_uid": "three", "name": "count", "args": [["det1", "det2"]]},
    ]
    for plan in plans:
        resp0, _ = zmq_single_request("queue_plan_add", {"plan": plan, "user": _user, "user_group": _user_group})
        assert resp0["success"] is True

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


def test_zmq_api_queue_plan_get_remove_3(re_manager):  # noqa F811
    """
    Get and remove elements using plan UID. Successful and failing cases.
    """
    plans = [_plan3, _plan2, _plan1]
    for plan in plans:
        resp0, _ = zmq_single_request("queue_plan_add", {"plan": plan, "user": _user, "user_group": _user_group})
        assert resp0["success"] is True

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


def test_zmq_api_queue_plan_get_remove_4_failing(re_manager):  # noqa F811
    """
    Failing cases that are not tested in other places.
    """
    # Ambiguous parameters
    resp1, _ = zmq_single_request("queue_plan_get", {"pos": 5, "uid": "some_uid"})
    assert resp1["success"] is False
    assert "Ambiguous parameters" in resp1["msg"]


# fmt: off
@pytest.mark.parametrize("params, src, order, success, msg", [
    ({"pos": 1, "pos_dest": 1}, 1, [0, 1, 2], True, ""),
    ({"pos": 1, "pos_dest": 0}, 1, [1, 0, 2], True, ""),
    ({"pos": 1, "pos_dest": 2}, 1, [0, 2, 1], True, ""),
    ({"pos": "front", "pos_dest": "front"}, 0, [0, 1, 2], True, ""),
    ({"pos": "back", "pos_dest": "back"}, 2, [0, 1, 2], True, ""),
    ({"pos": "front", "pos_dest": "back"}, 0, [1, 2, 0], True, ""),
    ({"pos": "back", "pos_dest": "front"}, 2, [2, 0, 1], True, ""),
    ({"uid": 1, "pos_dest": 1}, 1, [0, 1, 2], True, ""),
    ({"uid": 1, "pos_dest": 0}, 1, [1, 0, 2], True, ""),
    ({"uid": 1, "pos_dest": 2}, 1, [0, 2, 1], True, ""),
    ({"uid": 1, "pos_dest": 1}, 1, [0, 1, 2], True, ""),
    ({"uid": 1, "pos_dest": "front"}, 1, [1, 0, 2], True, ""),
    ({"uid": 1, "pos_dest": "back"}, 1, [0, 2, 1], True, ""),
    ({"uid": 0, "pos_dest": "front"}, 0, [0, 1, 2], True, ""),
    ({"uid": 2, "pos_dest": "back"}, 2, [0, 1, 2], True, ""),
    ({"uid": 0, "before_uid": 0}, 0, [0, 1, 2], True, ""),
    ({"uid": 0, "after_uid": 0}, 0, [0, 1, 2], True, ""),
    ({"uid": 2, "before_uid": 2}, 2, [0, 1, 2], True, ""),
    ({"uid": 2, "after_uid": 2}, 2, [0, 1, 2], True, ""),
    ({"uid": 0, "before_uid": 2}, 0, [1, 0, 2], True, ""),
    ({"uid": 0, "after_uid": 2}, 0, [1, 2, 0], True, ""),
    ({"uid": 2, "before_uid": 0}, 2, [2, 0, 1], True, ""),
    ({"uid": 2, "after_uid": 0}, 2, [0, 2, 1], True, ""),
    ({"pos": 50, "after_uid": 0}, 2, [], False, "Source plan (position 50) was not found"),
    ({"uid": 3, "after_uid": 0}, 2, [], False, "Source plan (UID 'nonexistent') was not found"),
    ({"pos": 1, "pos_dest": 50}, 2, [], False, "Destination plan (position 50) was not found"),
    ({"uid": 1, "after_uid": 3}, 2, [], False, "Destination plan (UID 'nonexistent') was not found"),
    ({"uid": 1, "before_uid": 3}, 2, [], False, "Destination plan (UID 'nonexistent') was not found"),
    ({"after_uid": 0}, 2, [], False, "Source position or UID is not specified"),
    ({"pos": 1}, 2, [], False, "Destination position or UID is not specified"),
    ({"pos": 1, "uid": 1, "after_uid": 0}, 2, [], False, "Ambiguous parameters"),
    ({"pos": 1, "pos_dest": 1, "after_uid": 0}, 2, [], False, "Ambiguous parameters"),
    ({"pos": 1, "before_uid": 0, "after_uid": 0}, 2, [], False, "Ambiguous parameters"),
])
# fmt: on
def test_zmq_api_move_plan_1(re_manager, params, src, order, success, msg):  # noqa: F811
    plans = [_plan1, _plan2, _plan3]
    for plan in plans:
        resp0, _ = zmq_single_request("queue_plan_add", {"plan": plan, "user": _user, "user_group": _user_group})
        assert resp0["success"] is True

    resp1, _ = zmq_single_request("queue_get")
    queue = resp1["queue"]
    assert len(queue) == 3

    plan_uids = [_["plan_uid"] for _ in queue]
    # Add one more 'nonexistent' uid (that is not in the queue)
    plan_uids.append("nonexistent")

    # Replace indices with actual UIDs that will be sent to the function
    if "uid" in params:
        params["uid"] = plan_uids[params["uid"]]
    if "before_uid" in params:
        params["before_uid"] = plan_uids[params["before_uid"]]
    if "after_uid" in params:
        params["after_uid"] = plan_uids[params["after_uid"]]

    resp2, _ = zmq_single_request("queue_plan_move", params)
    if success:
        assert resp2["success"] is True
        assert resp2["plan"] == queue[src]
        assert resp2["qsize"] == len(plans)
        assert resp2["msg"] == ""

        # Compare the order of UIDs in the queue with the expected order
        plan_uids_reordered = [plan_uids[_] for _ in order]
        resp3, _ = zmq_single_request("queue_get")
        plan_uids_from_queue = [_["plan_uid"] for _ in resp3["queue"]]

        assert plan_uids_from_queue == plan_uids_reordered

    else:
        assert resp2["success"] is False
        assert msg in resp2["msg"]


# =======================================================================================
#                 Tests for different scenarios of queue execution


def test_zmq_api_queue_execution_1(re_manager):  # noqa: F811
    """
    Execution of a queue that contains an instruction ('queue_stop').
    """

    # Instruction STOP
    params1a = {"instruction": _instruction_stop, "user": _user, "user_group": _user_group}
    resp1a, _ = zmq_single_request("queue_plan_add", params1a)
    assert resp1a["success"] is True, f"resp={resp1a}"
    assert resp1a["msg"] == ""
    assert resp1a["instruction"]["action"] == "queue_stop"

    # Plan
    params1b = {"plan": _plan1, "user": _user, "user_group": _user_group}
    resp1b, _ = zmq_single_request("queue_plan_add", params1b)
    assert resp1b["success"] is True, f"resp={resp1b}"

    # Instruction STOP
    params1c = {"instruction": _instruction_stop, "user": _user, "user_group": _user_group}
    resp1c, _ = zmq_single_request("queue_plan_add", params1c)
    assert resp1c["success"] is True, f"resp={resp1c}"
    assert resp1c["msg"] == ""
    assert resp1c["instruction"]["action"] == "queue_stop"

    # Plan
    params1d = {"plan": _plan2, "user": _user, "user_group": _user_group}
    resp1d, _ = zmq_single_request("queue_plan_add", params1d)
    assert resp1d["success"] is True, f"resp={resp1d}"

    # The queue contains only a single instruction (stop the queue).
    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert wait_for_condition(time=10, condition=condition_environment_created)

    resp2a, _ = zmq_single_request("status")
    assert resp2a["plans_in_queue"] == 4
    assert resp2a["plans_in_history"] == 0

    resp3, _ = zmq_single_request("queue_start")
    assert resp3["success"] is True

    assert wait_for_condition(time=5, condition=condition_manager_idle)

    resp3a, _ = zmq_single_request("status")
    assert resp3a["plans_in_queue"] == 3
    assert resp3a["plans_in_history"] == 0

    resp4, _ = zmq_single_request("queue_start")
    assert resp4["success"] is True

    assert wait_for_condition(time=5, condition=condition_manager_idle)

    resp4a, _ = zmq_single_request("status")
    assert resp4a["plans_in_queue"] == 1
    assert resp4a["plans_in_history"] == 1

    resp5, _ = zmq_single_request("queue_start")
    assert resp5["success"] is True

    assert wait_for_condition(time=5, condition=condition_queue_processing_finished)

    resp5a, _ = zmq_single_request("status")
    assert resp5a["plans_in_queue"] == 0
    assert resp5a["plans_in_history"] == 2

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


"""
@pytest.mark.parametrize("a", [0] * 100)
def test_qserver_communication_reliability(re_manager, a):  # noqa: F811
    for i in range(10):
        print(f"i={i}")
        resp0, _ = zmq_single_request("status")
        assert resp0["manager_state"] == "idle"
        print(f"status: {resp0}")
"""
