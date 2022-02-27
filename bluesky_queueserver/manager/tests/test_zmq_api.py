import pytest
import os
import time as ttime
import asyncio
import copy
import pprint
import re
import glob
import json
import numpy as np
import yaml

import bluesky_queueserver

from bluesky_queueserver.manager.profile_ops import (
    get_default_startup_dir,
    load_allowed_plans_and_devices,
    gen_list_of_plans_and_devices,
    load_profile_collection,
    _prepare_devices,
    _prepare_plans,
    devices_from_nspace,
    plans_from_nspace,
)

from bluesky_queueserver.manager.plan_queue_ops import PlanQueueOperations

from ..comms import (
    zmq_single_request,
    ZMQCommSendThreads,
    ZMQCommSendAsync,
    CommTimeoutError,
    generate_new_zmq_key_pair,
)

from .common import (
    zmq_secure_request,
    wait_for_condition,
    condition_environment_created,
    condition_queue_processing_finished,
    condition_manager_paused,
    wait_for_task_result,
    get_queue_state,
    condition_environment_closed,
    condition_manager_idle,
    append_code_to_last_startup_file,
    set_qserver_zmq_public_key,
    copy_default_profile_collection,
    # clear_redis_pool,
)
from .common import re_manager, re_manager_pc_copy, re_manager_cmd, db_catalog  # noqa: F401

qserver_version = bluesky_queueserver.__version__

# Plans used in most of the tests: '_plan1' and '_plan2' are quickly executed '_plan3' runs for 5 seconds.
_plan1 = {"name": "count", "args": [["det1", "det2"]], "item_type": "plan"}
_plan2 = {"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10], "item_type": "plan"}
_plan3 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 5, "delay": 1}, "item_type": "plan"}
_plan4 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 10, "delay": 1}, "item_type": "plan"}
_instruction_stop = {"name": "queue_stop", "item_type": "instruction"}

# User name and user group name used throughout most of the tests.
_user, _user_group, _test_user_group = "Testing Script", "admin", "test_user"

_existing_plans_and_devices_fln = "existing_plans_and_devices.yaml"
_user_group_permissions_fln = "user_group_permissions.yaml"

timeout_env_open = 10

# =======================================================================================
#                   Thread-based ZMQ API - ZMQCommSendThreads


def test_zmq_api_thread_based(re_manager):  # noqa F811
    """
    Communicate with the server using the thread-based API. The purpose of the test is to make
    sure that the API is compatible with the client. It is sufficient to test only
    the blocking call, since it is still using callbacks mechanism.
    """
    client = ZMQCommSendThreads()

    resp1 = client.send_message(
        method="queue_item_add", params={"item": _plan1, "user": _user, "user_group": _user_group}
    )
    assert resp1["success"] is True, str(resp1)
    assert resp1["qsize"] == 1
    assert resp1["item"]["item_type"] == _plan1["item_type"]
    assert resp1["item"]["name"] == _plan1["name"]
    assert resp1["item"]["args"] == _plan1["args"]
    assert resp1["item"]["user"] == _user
    assert resp1["item"]["user_group"] == _user_group
    assert "item_uid" in resp1["item"]

    resp2 = client.send_message(method="queue_get")
    assert resp2["items"] != []
    assert len(resp2["items"]) == 1
    assert resp2["items"][0] == resp1["item"]
    assert resp2["running_item"] == {}

    with pytest.raises(CommTimeoutError, match="timeout occurred"):
        client.send_message(method="manager_kill")

    # Wait until the manager is restarted
    ttime.sleep(6)

    resp3 = client.send_message(method="status")
    assert resp3["manager_state"] == "idle"
    assert resp3["items_in_queue"] == 1
    assert resp3["items_in_history"] == 0


# =======================================================================================
#                   Thread-based ZMQ API - ZMQCommSendThreads


def test_zmq_api_asyncio_based(re_manager):  # noqa F811
    """
    Communicate with the server using asyncio-based API. The purpose of the test is make
    sure that the API is compatible with the client.
    """

    async def testing():
        client = ZMQCommSendAsync()

        resp1 = await client.send_message(
            method="queue_item_add", params={"item": _plan1, "user": _user, "user_group": _user_group}
        )
        assert resp1["success"] is True, str(resp1)
        assert resp1["qsize"] == 1
        assert resp1["item"]["item_type"] == _plan1["item_type"]
        assert resp1["item"]["name"] == _plan1["name"]
        assert resp1["item"]["args"] == _plan1["args"]
        assert resp1["item"]["user"] == _user
        assert resp1["item"]["user_group"] == _user_group
        assert "item_uid" in resp1["item"]

        resp2 = await client.send_message(method="queue_get")
        assert resp2["items"] != []
        assert len(resp2["items"]) == 1
        assert resp2["items"][0] == resp1["item"]
        assert resp2["running_item"] == {}

        with pytest.raises(CommTimeoutError, match="timeout occurred"):
            await client.send_message(method="manager_kill")

        # Wait until the manager is restarted
        await asyncio.sleep(6)

        resp3 = await client.send_message(method="status")
        assert resp3["manager_state"] == "idle"
        assert resp3["items_in_queue"] == 1
        assert resp3["items_in_history"] == 0

    asyncio.run(testing())


# =======================================================================================
#                   Methods 'ping' (currently returning status), "status"

# fmt: off
@pytest.mark.parametrize("api_name", ["ping", "status"])
# fmt: on
def test_zmq_api_ping_status_01(re_manager, api_name):  # noqa F811
    resp, _ = zmq_single_request(api_name)
    assert resp["msg"] == f"RE Manager v{qserver_version}"
    assert resp["manager_state"] == "idle"
    assert resp["items_in_queue"] == 0
    assert resp["running_item_uid"] is None
    assert resp["worker_environment_exists"] is False
    assert bool(resp["plan_queue_uid"])
    assert isinstance(resp["plan_queue_uid"], str), type(resp["plan_queue_uid"])
    assert bool(resp["plan_history_uid"])
    assert isinstance(resp["plan_history_uid"], str), type(resp["plan_history_uid"])

    # Run Engine state is None if RE environment does not exist. Otherwise it should
    #   be a string representing Run Engine state.
    assert resp["re_state"] is None
    assert resp["pause_pending"] is False

    # Worker environment is initially 'closed'
    assert resp["worker_environment_state"] == "closed"

    assert isinstance(resp["plan_queue_mode"], dict)
    assert resp["plan_queue_mode"]["loop"] is False


# fmt: off
@pytest.mark.parametrize("api_name", ["ping", "status"])
# fmt: on
def test_zmq_api_ping_status_02(re_manager, api_name):  # noqa F811
    """
    Check that extra parameters, such as 'reload' are ignored by the API.
    """
    resp, _ = zmq_single_request(api_name, params={"reload": True})
    assert resp["msg"] == f"RE Manager v{qserver_version}"


# =======================================================================================
#                   Methods 'environment_open', 'environment_close'


def test_zmq_api_environment_open_close_1(re_manager):  # noqa F811
    """
    Basic test for `environment_open` and `environment_close` methods.
    """
    state = get_queue_state()
    assert state["re_state"] is None
    assert state["worker_environment_state"] == "closed"

    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert resp1["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    state = get_queue_state()
    assert state["re_state"] == "idle"
    assert state["worker_environment_state"] == "idle"

    resp2, _ = zmq_single_request("environment_close")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    state = get_queue_state()
    assert state["re_state"] is None
    assert state["worker_environment_state"] == "closed"


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

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

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

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # Start a plan
    resp2, _ = zmq_single_request("queue_item_add", {"item": _plan3, "user": _user, "user_group": _user_group})
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
#                             Method 'queue_item_add'


def test_zmq_api_queue_item_add_1(re_manager):  # noqa F811
    """
    Basic test for `queue_item_add` method.
    """
    status0 = get_queue_state()

    resp1, _ = zmq_single_request("queue_item_add", {"item": _plan1, "user": _user, "user_group": _user_group})
    assert resp1["success"] is True
    assert resp1["qsize"] == 1
    assert resp1["item"]["name"] == _plan1["name"]
    assert resp1["item"]["args"] == _plan1["args"]
    assert resp1["item"]["user"] == _user
    assert resp1["item"]["user_group"] == _user_group
    assert "item_uid" in resp1["item"]

    status1 = get_queue_state()
    assert status1["plan_queue_uid"] != status0["plan_queue_uid"]
    assert status1["plan_history_uid"] == status0["plan_history_uid"]

    resp2, _ = zmq_single_request("queue_get")
    assert resp2["items"] != []
    assert len(resp2["items"]) == 1
    assert resp2["items"][0] == resp1["item"]
    assert resp2["running_item"] == {}
    assert resp2["plan_queue_uid"] == status1["plan_queue_uid"]


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
def test_zmq_api_queue_item_add_2(re_manager, pos, pos_result, success):  # noqa F811

    plan1 = {"name": "count", "args": [["det1"]], "item_type": "plan"}
    plan2 = {"name": "count", "args": [["det1", "det2"]], "item_type": "plan"}

    # Create the queue with 2 entries
    params1 = {"item": plan1, "user": _user, "user_group": _user_group}
    resp0a, _ = zmq_single_request("queue_item_add", params1)
    assert resp0a["success"] is True
    resp0b, _ = zmq_single_request("queue_item_add", params1)
    assert resp0b["success"] is True

    # Add another entry at the specified position
    params2 = {"item": plan2, "user": _user, "user_group": _user_group}
    if pos is not None:
        params2.update({"pos": pos})

    resp1, _ = zmq_single_request("queue_item_add", params2)

    assert resp1["success"] is success
    assert resp1["qsize"] == (3 if success else None)
    assert resp1["item"]["item_type"] == "plan"
    assert resp1["item"]["name"] == "count"
    assert resp1["item"]["args"] == plan2["args"]
    assert resp1["item"]["user"] == _user
    assert resp1["item"]["user_group"] == _user_group
    assert "item_uid" in resp1["item"]

    resp2, _ = zmq_single_request("queue_get")

    assert len(resp2["items"]) == (3 if success else 2)
    assert resp2["running_item"] == {}
    print(f"QUEUE ITEMS: {pprint.pformat(resp2['items'])}")

    if success:
        assert resp2["items"][pos_result]["args"] == plan2["args"]


def test_zmq_api_queue_item_add_3(re_manager):  # noqa F811
    plan1 = {"name": "count", "args": [["det1"]], "item_type": "plan"}
    plan2 = {"name": "count", "args": [["det1", "det2"]], "item_type": "plan"}
    plan3 = {"name": "count", "args": [["det2"]], "item_type": "plan"}

    params = {"item": plan1, "user": _user, "user_group": _user_group}
    resp0a, _ = zmq_single_request("queue_item_add", params)
    assert resp0a["success"] is True
    params = {"item": plan2, "user": _user, "user_group": _user_group}
    resp0b, _ = zmq_single_request("queue_item_add", params)
    assert resp0b["success"] is True

    base_plans = zmq_single_request("queue_get")[0]["items"]

    params = {"item": plan3, "after_uid": base_plans[0]["item_uid"], "user": _user, "user_group": _user_group}
    resp1, _ = zmq_single_request("queue_item_add", params)
    assert resp1["success"] is True
    assert resp1["qsize"] == 3
    uid1 = resp1["item"]["item_uid"]
    resp1a, _ = zmq_single_request("queue_get")
    assert len(resp1a["items"]) == 3
    assert resp1a["items"][1]["item_uid"] == uid1
    resp1b, _ = zmq_single_request("queue_item_remove", {"uid": uid1})
    assert resp1b["success"] is True

    params = {"item": plan3, "before_uid": base_plans[1]["item_uid"], "user": _user, "user_group": _user_group}
    resp2, _ = zmq_single_request("queue_item_add", params)
    assert resp2["success"] is True
    uid2 = resp2["item"]["item_uid"]
    resp2a, _ = zmq_single_request("queue_get")
    assert len(resp2a["items"]) == 3
    assert resp2a["items"][1]["item_uid"] == uid2
    resp2b, _ = zmq_single_request("queue_item_remove", {"uid": uid2})
    assert resp2b["success"] is True

    # Non-existing uid
    params = {"item": plan3, "before_uid": "non-existing-uid", "user": _user, "user_group": _user_group}
    resp2, _ = zmq_single_request("queue_item_add", params)
    assert resp2["success"] is False
    assert "is not in the queue" in resp2["msg"]

    # Ambiguous parameters
    params = {"item": plan3, "pos": 1, "before_uid": uid2, "user": _user, "user_group": _user_group}
    resp2, _ = zmq_single_request("queue_item_add", params)
    assert resp2["success"] is False
    assert "Ambiguous parameters" in resp2["msg"]

    # Ambiguous parameters
    params = {"item": plan3, "before_uid": uid2, "after_uid": uid2, "user": _user, "user_group": _user_group}
    resp2, _ = zmq_single_request("queue_item_add", params)
    assert resp2["success"] is False
    assert "Ambiguous parameters" in resp2["msg"]


def test_zmq_api_queue_item_add_4(re_manager):  # noqa F811
    """
    Try inserting plans before and after the running plan
    """
    params = {"item": _plan3, "user": _user, "user_group": _user_group}
    resp0a, _ = zmq_single_request("queue_item_add", params)
    assert resp0a["success"] is True
    params = {"item": _plan3, "user": _user, "user_group": _user_group}
    resp0b, _ = zmq_single_request("queue_item_add", params)
    assert resp0b["success"] is True

    base_plans = zmq_single_request("queue_get")[0]["items"]
    uid = base_plans[0]["item_uid"]

    # Start the first plan (this removes it from the queue)
    #   Also the rest of the operations will be performed on a running queue.
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(
        time=timeout_env_open, condition=condition_environment_created
    ), "Timeout while waiting for environment to be opened"

    resp2, _ = zmq_single_request("queue_start")
    assert resp2["success"] is True

    ttime.sleep(1)

    # Try to insert a plan before the running plan
    params = {"item": _plan3, "before_uid": uid, "user": _user, "user_group": _user_group}
    resp3, _ = zmq_single_request("queue_item_add", params)
    assert resp3["success"] is False
    assert "Can not insert a plan in the queue before a currently running plan" in resp3["msg"]

    # Insert the plan after the running plan
    params = {"item": _plan3, "after_uid": uid, "user": _user, "user_group": _user_group}
    resp4, _ = zmq_single_request("queue_item_add", params)
    assert resp4["success"] is True

    assert wait_for_condition(
        time=20, condition=condition_queue_processing_finished
    ), "Timeout while waiting for environment to be opened"

    state = get_queue_state()
    assert state["items_in_queue"] == 0
    assert state["items_in_history"] == 3

    # Close the environment
    resp5, _ = zmq_single_request("environment_close")
    assert resp5["success"] is True
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("plan_to_add, ugroup, success_submit, success_run, msg", [
    # 'count' plan does not have restrictions on the name of devices, so all
    #   the following plans can be submitted, but some of them will fail during
    #   execution.
    ({"name": "count",
      "args": [["sim_bundle_A.dets.det_A", "sim_bundle_B.dets.det_B"]],
      "kwargs": {"num": 1, "delay": 1}, "item_type": "plan"},
     _user_group, True, True, ""),
    ({"name": "count",
      "kwargs": {"detectors": ["sim_bundle_A.dets.det_A", "sim_bundle_B.dets.det_B"],
                 "num": 1, "delay": 1}, "item_type": "plan"},
     _user_group, True, True, ""),
    ({"name": "count",
      "args": [["sim_bundle_A.dets", "sim_bundle_A"]],
      "kwargs": {"num": 1, "delay": 1}, "item_type": "plan"},
     _user_group, True, True, ""),
    ({"name": "count",
      "args": [["sim_bundle_A.dets", "sim_bundle_B"]],
      "kwargs": {"num": 1, "delay": 1}, "item_type": "plan"},
     _test_user_group, True, False, ""),
    ({"name": "count",
      "args": [["sim_bundle_A.dets", "sim_bundle_B.dets"]],
      "kwargs": {"num": 1, "delay": 1}, "item_type": "plan"},
     _test_user_group, True, False, ""),
    ({"name": "count",
      "args": [["sim_bundle_A.dets", "sim_bundle_B.dets.det_A"]],
      "kwargs": {"num": 1, "delay": 1}, "item_type": "plan"},
     _test_user_group, True, False, ""),
    # Specially designed test plan with defined set of items. Plan validation
    #   fails at submission if a parameter is not in the list
    ({"name": "count_bundle_test",
      "args": [["sim_bundle_A.dets.det_A", "sim_bundle_B.dets.det_B"]],
      "kwargs": {"num": 1, "delay": 1}, "item_type": "plan"},
     _user_group, True, True, ""),
    ({"name": "count_bundle_test",
      "kwargs": {"detectors": ["sim_bundle_A.dets.det_A", "sim_bundle_B"],
                 "num": 1, "delay": 1}, "item_type": "plan"},
     _user_group, True, True, ""),
    ({"name": "count_bundle_test",
      "args": [["sim_bundle_A.dets.det_A", "sim_bundle_A.dets.det_B"]],
      "kwargs": {"num": 1, "delay": 1}, "item_type": "plan"},
     _test_user_group, True, True, ""),
    ({"name": "count_bundle_test",
      "args": [["sim_bundle_A.dets.det_A", "sim_bundle_B.dets.det_B"]],
      "kwargs": {"num": 1, "delay": 1}, "item_type": "plan"},
     _test_user_group, False, False, "Failed to add an item: Plan validation failed"),
    ({"name": "count_bundle_test",
      "kwargs": {"detectors": ["sim_bundle_A.dets.det_A", "sim_bundle_B.dets.det_B"],
                 "num": 1, "delay": 1}, "item_type": "plan"},
     _test_user_group, False, False, "Failed to add an item: Plan validation failed"),
    ({"name": "count_bundle_test",
      "args": [["sim_bundle_A.dets.det_A", "sim_bundle_B"]],
      "kwargs": {"num": 1, "delay": 1}, "item_type": "plan"},
     _test_user_group, False, False, "Failed to add an item: Plan validation failed"),
])
# fmt: on
def test_zmq_api_queue_item_add_5(re_manager, plan_to_add, ugroup, success_submit, success_run, msg):  # noqa F811
    """
    Check if subdevice names could be passed to plans
    """
    params = {"item": plan_to_add, "user": _user, "user_group": ugroup}
    resp0a, _ = zmq_single_request("queue_item_add", params)
    assert resp0a["success"] is success_submit
    response_msg = resp0a["msg"]

    state = get_queue_state()
    assert state["items_in_queue"] == (1 if success_submit else 0)
    assert state["items_in_history"] == 0

    if not success_submit:
        assert msg in response_msg, pprint.pformat(resp0a)

    else:
        # Now execute the plan
        resp1, _ = zmq_single_request("environment_open")
        assert resp1["success"] is True
        assert wait_for_condition(
            time=timeout_env_open, condition=condition_environment_created
        ), "Timeout while waiting for environment to be opened"

        resp2, _ = zmq_single_request("queue_start")
        assert resp2["success"] is True

        assert wait_for_condition(
            time=10, condition=condition_manager_idle
        ), "Timeout while waiting for environment to be opened"

        state = get_queue_state()
        assert state["items_in_queue"] == (0 if success_run else 1)
        assert state["items_in_history"] == 1

        # Close the environment
        resp5, _ = zmq_single_request("environment_close")
        assert resp5["success"] is True
        assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_zmq_api_queue_item_add_6(re_manager):  # noqa: F811
    """
    Make sure that the new plan UID is generated when the plan is added
    """
    plan1 = {"name": "count", "args": [["det1", "det2"]], "item_type": "plan"}

    # Set plan UID. This UID is expected to be replaced when the plan is added
    plan1["item_uid"] = PlanQueueOperations.new_item_uid()

    params1 = {"item": plan1, "user": _user, "user_group": _user_group}
    resp1, _ = zmq_single_request("queue_item_add", params1)
    assert resp1["success"] is True
    assert resp1["msg"] == ""
    assert resp1["item"]["item_uid"] != plan1["item_uid"]


def test_zmq_api_queue_item_add_7(re_manager):  # noqa: F811
    """
    Add instruction ('queue_stop') to the queue.
    """

    params1a = {"item": _plan1, "user": _user, "user_group": _user_group}
    resp1a, _ = zmq_single_request("queue_item_add", params1a)
    assert resp1a["success"] is True, f"resp={resp1a}"

    params1 = {"item": _instruction_stop, "user": _user, "user_group": _user_group}
    resp1, _ = zmq_single_request("queue_item_add", params1)
    assert resp1["success"] is True, f"resp={resp1}"
    assert resp1["msg"] == ""
    assert resp1["item"]["name"] == "queue_stop"

    params1c = {"item": _plan2, "user": _user, "user_group": _user_group}
    resp1c, _ = zmq_single_request("queue_item_add", params1c)
    assert resp1c["success"] is True, f"resp={resp1c}"

    resp2, _ = zmq_single_request("queue_get")
    assert len(resp2["items"]) == 3
    assert resp2["items"][0]["item_type"] == "plan"
    assert resp2["items"][1]["item_type"] == "instruction"
    assert resp2["items"][2]["item_type"] == "plan"


# fmt: off
@pytest.mark.parametrize("meta_param, meta_saved", [
    # 'meta' is dictionary, all keys are saved
    ({"test_key": "test_value"}, {"test_key": "test_value"}),
    # 'meta' - array with two elements. Merging dictionaries with distinct keys.
    ([{"test_key1": 10}, {"test_key2": 20}], {"test_key1": 10, "test_key2": 20}),
    # ' meta' - array. Merging dictionaries with identical keys.
    ([{"test_key": 10}, {"test_key": 20}], {"test_key": 10}),
])
# fmt: on
def test_zmq_api_queue_item_add_8(db_catalog, re_manager_cmd, meta_param, meta_saved):  # noqa: F811
    """
    Add plan with metadata.
    """
    re_manager_cmd(["--databroker-config", db_catalog["catalog_name"]])
    cat = db_catalog["catalog"]

    # Plan
    plan = copy.deepcopy(_plan2)
    plan["meta"] = meta_param
    params1 = {"item": plan, "user": _user, "user_group": _user_group}
    resp1, _ = zmq_single_request("queue_item_add", params1)
    assert resp1["success"] is True, f"resp={resp1}"

    resp2, _ = zmq_single_request("status")
    assert resp2["items_in_queue"] == 1
    assert resp2["items_in_history"] == 0

    # Open the environment.
    resp3, _ = zmq_single_request("environment_open")
    assert resp3["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp4, _ = zmq_single_request("queue_start")
    assert resp4["success"] is True

    assert wait_for_condition(time=5, condition=condition_manager_idle)

    resp5, _ = zmq_single_request("status")
    assert resp5["items_in_queue"] == 0
    assert resp5["items_in_history"] == 1

    resp6, _ = zmq_single_request("history_get")
    history = resp6["items"]
    assert len(history) == 1

    # Check if metadata was recorded in the start document.
    uid = history[-1]["result"]["run_uids"][0]
    start_doc = cat[uid].metadata["start"]
    for key in meta_saved:
        assert key in start_doc, str(start_doc)
        assert meta_saved[key] == start_doc[key], str(start_doc)

    # Close the environment.
    resp7, _ = zmq_single_request("environment_close")
    assert resp7["success"] is True, f"resp={resp7}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_zmq_api_queue_item_add_9_fail(re_manager):  # noqa F811

    # Unknown plan name
    plan1 = {"name": "count_test", "args": [["det1", "det2"]], "item_type": "plan"}
    params1 = {"item": plan1, "user": _user, "user_group": _user_group}
    resp1, _ = zmq_single_request("queue_item_add", params1)
    assert resp1["success"] is False
    assert "Plan 'count_test' is not in the list of allowed plans" in resp1["msg"]

    # Unknown kwarg
    plan2 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"abc": 10}, "item_type": "plan"}
    params2 = {"item": plan2, "user": _user, "user_group": _user_group}
    resp2, _ = zmq_single_request("queue_item_add", params2)
    assert resp2["success"] is False
    assert (
        "Failed to add an item: Plan validation failed: got an unexpected keyword argument 'abc'" in resp2["msg"]
    )

    # User name is not specified
    params3 = {"item": plan2, "user_group": _user_group}
    resp3, _ = zmq_single_request("queue_item_add", params3)
    assert resp3["success"] is False
    assert "user name is not specified" in resp3["msg"]

    # User group is not specified
    params4 = {"item": plan2, "user": _user}
    resp4, _ = zmq_single_request("queue_item_add", params4)
    assert resp4["success"] is False
    assert "user group is not specified" in resp4["msg"]

    # Unknown user group
    params5 = {"item": plan2, "user": _user, "user_group": "no_such_group"}
    resp5, _ = zmq_single_request("queue_item_add", params5)
    assert resp5["success"] is False
    assert "Unknown user group: 'no_such_group'" in resp5["msg"]

    # Missing item parameters
    params6 = {"user": _user, "user_group": _user_group}
    resp6, _ = zmq_single_request("queue_item_add", params6)
    assert resp6["success"] is False
    assert resp6["item"] is None
    assert "Incorrect request format: request contains no item info" in resp6["msg"]

    # Incorrect type of the item parameter (must be dict)
    params6a = {"item": [], "user": _user, "user_group": _user_group}
    resp6a, _ = zmq_single_request("queue_item_add", params6a)
    assert resp6a["success"] is False
    assert resp6a["item"] == []
    assert "item parameter must have type 'dict'" in resp6a["msg"]

    # Unsupported item type
    plan7 = {"name": "count_test", "args": [["det1", "det2"]], "item_type": "unsupported"}
    params7 = {"item": plan7, "user": _user, "user_group": _user_group}
    resp7, _ = zmq_single_request("queue_item_add", params7)
    assert resp7["success"] is False
    assert resp7["item"] == plan7
    assert "Incorrect request format: unsupported 'item_type' value: 'unsupported'" in resp7["msg"]

    # Valid plan
    plan8 = {"name": "count", "args": [["det1", "det2"]], "item_type": "plan"}
    params8 = {"item": plan8, "user": _user, "user_group": _user_group}
    resp8, _ = zmq_single_request("queue_item_add", params8)
    assert resp8["success"] is True
    assert resp8["qsize"] == 1
    assert resp8["item"]["name"] == "count"
    assert resp8["item"]["args"] == [["det1", "det2"]]
    assert resp8["item"]["user"] == _user
    assert resp8["item"]["user_group"] == _user_group
    assert "item_uid" in resp8["item"]

    resp9, _ = zmq_single_request("queue_get")
    assert resp9["items"] != []
    assert len(resp9["items"]) == 1
    assert resp9["items"][0] == resp8["item"]
    assert resp9["running_item"] == {}


# =======================================================================================
#                          Method 'queue_item_execute'


def test_zmq_api_queue_item_execute_1(re_manager):  # noqa: F811
    """
    Basic test for ``queue_item_execute`` API.
    """
    # Add plan to queue
    params1a = {"item": _plan1, "user": _user, "user_group": _user_group}
    resp1a, _ = zmq_single_request("queue_item_add", params1a)
    assert resp1a["success"] is True, f"resp={resp1a}"

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp2a, _ = zmq_single_request("status")
    assert resp2a["items_in_queue"] == 1
    assert resp2a["items_in_history"] == 0
    assert resp2a["worker_environment_state"] == "idle"

    # Execute a plan
    params3 = {"item": _plan3, "user": _user, "user_group": _user_group}
    resp3, _ = zmq_single_request("queue_item_execute", params3)
    assert resp3["success"] is True, f"resp={resp3}"
    assert resp3["msg"] == ""
    assert resp3["qsize"] == 1
    assert resp3["item"]["name"] == _plan3["name"]

    ttime.sleep(1)
    status, _ = zmq_single_request("status")
    assert status["items_in_queue"] == 1
    assert status["items_in_history"] == 0
    assert status["worker_environment_state"] == "executing_plan"

    assert wait_for_condition(time=30, condition=condition_manager_idle)

    # Execute an instruction (STOP instruction - nothing will be done)
    params3a = {"item": _instruction_stop, "user": _user, "user_group": _user_group}
    resp3a, _ = zmq_single_request("queue_item_execute", params3a)
    assert resp3a["success"] is True, f"resp={resp3a}"
    assert resp3a["msg"] == ""
    assert resp3a["qsize"] == 1
    assert resp3a["item"]["name"] == _instruction_stop["name"]

    assert wait_for_condition(time=5, condition=condition_manager_idle)

    resp3b, _ = zmq_single_request("status")
    assert resp3b["items_in_queue"] == 1
    assert resp3b["items_in_history"] == 1
    assert resp3b["worker_environment_state"] == "idle"

    resp4, _ = zmq_single_request("queue_start")
    assert resp4["success"] is True

    assert wait_for_condition(time=5, condition=condition_manager_idle)

    resp4a, _ = zmq_single_request("status")
    assert resp4a["items_in_queue"] == 0
    assert resp4a["items_in_history"] == 2

    history, _ = zmq_single_request("history_get")
    h_items = history["items"]
    assert len(h_items) == 2, pprint.pformat(h_items)
    assert h_items[0]["name"] == _plan3["name"]
    assert h_items[1]["name"] == _plan1["name"]

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_zmq_api_queue_item_execute_2(re_manager):  # noqa: F811
    """
    Test for ``queue_item_execute`` API: attempt to start execution of a plan and instruction
    while the queue is running.
    """
    # Add plan to queue
    params1a = {"item": _plan3, "user": _user, "user_group": _user_group}
    resp1a, _ = zmq_single_request("queue_item_add", params1a)
    assert resp1a["success"] is True, f"resp={resp1a}"

    # The queue contains only a single instruction (stop the queue).
    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp2a, _ = zmq_single_request("status")
    assert resp2a["items_in_queue"] == 1
    assert resp2a["items_in_history"] == 0

    # Start the queue
    resp3, _ = zmq_single_request("queue_start")
    assert resp3["success"] is True

    # While the queue is running, attempt to execute a plan
    params3a = {"item": _plan1, "user": _user, "user_group": _user_group}
    resp3a, _ = zmq_single_request("queue_item_execute", params3a)
    assert resp3a["success"] is False, f"resp={resp3a}"
    expected_error_msg = "Failed to start execution of the item: RE Manager is busy."
    assert resp3a["msg"] == expected_error_msg
    assert resp3a["qsize"] is None
    assert resp3a["item"]["name"] == _plan1["name"]

    # While the queue is running, attempt to execute a plan
    params3b = {"item": _instruction_stop, "user": _user, "user_group": _user_group}
    resp3b, _ = zmq_single_request("queue_item_execute", params3b)
    assert resp3b["success"] is False, f"resp={resp3b}"
    assert resp3b["msg"] == expected_error_msg
    assert resp3b["qsize"] is None
    assert resp3b["item"]["name"] == _instruction_stop["name"]

    # Wait for the completion of the running plan
    assert wait_for_condition(time=30, condition=condition_manager_idle)

    resp4a, _ = zmq_single_request("status")
    assert resp4a["items_in_queue"] == 0
    assert resp4a["items_in_history"] == 1

    history, _ = zmq_single_request("history_get")
    h_items = history["items"]
    assert len(h_items) == 1, pprint.pformat(h_items)
    assert h_items[0]["name"] == _plan3["name"]

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_zmq_api_queue_item_execute_3(re_manager):  # noqa: F811
    """
    Test for ``queue_item_execute`` API: attempt to start execution of a plan before
    RE Worker environment is opened.
    """
    # Attempt to start execution of a plan before the environment is open
    params1a = {"item": _plan1, "user": _user, "user_group": _user_group}
    resp1a, _ = zmq_single_request("queue_item_execute", params1a)
    assert resp1a["success"] is False, f"resp={resp1a}"
    assert resp1a["msg"] == "Failed to start execution of the item: RE Worker environment does not exist."
    assert resp1a["qsize"] is None
    assert resp1a["item"]["name"] == _plan1["name"]

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp2a, _ = zmq_single_request("status")
    assert resp2a["items_in_queue"] == 0
    assert resp2a["items_in_history"] == 0

    # Execute a plan
    params3 = {"item": _plan3, "user": _user, "user_group": _user_group}
    resp3, _ = zmq_single_request("queue_item_execute", params3)
    assert resp3["success"] is True, f"resp={resp3}"
    assert resp3["qsize"] == 0
    assert resp3["item"]["name"] == _plan3["name"]

    assert wait_for_condition(time=30, condition=condition_manager_idle)

    resp4a, _ = zmq_single_request("status")
    assert resp4a["items_in_queue"] == 0
    assert resp4a["items_in_history"] == 1

    history, _ = zmq_single_request("history_get")
    h_items = history["items"]
    assert len(h_items) == 1, pprint.pformat(h_items)
    assert h_items[0]["name"] == _plan3["name"]

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_zmq_api_queue_item_execute_4_fail(re_manager):  # noqa: F811
    """
    Test for ``queue_item_execute`` API: failing cases (validation errors)
    """
    # Incorrect item type
    plan = {"name": "count", "args": [["det1", "det2"]], "item_type": "unknown"}
    params1a = {"item": plan, "user": _user, "user_group": _user_group}
    resp1a, _ = zmq_single_request("queue_item_execute", params1a)
    assert resp1a["success"] is False, f"resp={resp1a}"
    assert "unsupported 'item_type' value: 'unknown'" in resp1a["msg"]
    assert resp1a["qsize"] is None
    assert resp1a["item"]["name"] == plan["name"]

    resp1b, _ = zmq_single_request("status")
    assert resp1b["items_in_queue"] == 0
    assert resp1b["items_in_history"] == 0

    # Incorrect plan name
    plan = {"name": "unknown", "args": [["det1", "det2"]], "item_type": "plan"}
    params2a = {"item": plan, "user": _user, "user_group": _user_group}
    resp2a, _ = zmq_single_request("queue_item_execute", params2a)
    assert resp2a["success"] is False, f"resp={resp2a}"
    assert "Plan 'unknown' is not in the list of allowed plans" in resp2a["msg"]
    assert resp2a["qsize"] is None
    assert resp2a["item"]["name"] == plan["name"]

    resp2b, _ = zmq_single_request("status")
    assert resp2b["items_in_queue"] == 0
    assert resp2b["items_in_history"] == 0

    # Incorrect user group
    plan = {"name": "count", "args": [["det1", "det2"]], "item_type": "plan"}
    params3a = {"item": plan, "user": _user, "user_group": "unknown"}
    resp3a, _ = zmq_single_request("queue_item_execute", params3a)
    assert resp3a["success"] is False, f"resp={resp3a}"
    assert "Unknown user group: 'unknown'" in resp3a["msg"]
    assert resp3a["qsize"] is None
    assert resp3a["item"]["name"] == plan["name"]

    resp3b, _ = zmq_single_request("status")
    assert resp3b["items_in_queue"] == 0
    assert resp3b["items_in_history"] == 0


# =======================================================================================
#                          Method 'queue_item_add_batch'

# fmt: off
@pytest.mark.parametrize("batch_params, queue_seq, batch_seq, expected_seq, success, msgs", [
    ({}, "", "", "", True, "" * 3),  # Add an empty batch
    ({}, "", "567", "567", True, "" * 3),
    ({"pos": "front"}, "", "567", "567", True, "" * 3),
    ({"pos": "back"}, "", "567", "567", True, "" * 3),
    ({}, "1234", "567", "1234567", True, "" * 3),
    ({"pos": "front"}, "1234", "567", "5671234", True, "" * 3),
    ({"pos": "back"}, "1234", "567", "1234567", True, "" * 3),
    ({"pos": 0}, "1234", "567", "5671234", True, "" * 3),
    ({"pos": 1}, "1234", "567", "1567234", True, "" * 3),
    ({"pos": 100}, "1234", "567", "1234567", True, "" * 3),
    ({"pos": -1}, "1234", "567", "1234567", True, "" * 3),
    ({"pos": -100}, "1234", "567", "5671234", True, "" * 3),
    ({"before_uid": "1"}, "1234", "567", "5671234", True, "" * 3),
    ({"before_uid": "2"}, "1234", "567", "1567234", True, "" * 3),
    ({"before_uid": "3"}, "1234", "567", "1256734", True, "" * 3),
    ({"after_uid": "1"}, "1234", "567", "1567234", True, "" * 3),
    ({"after_uid": "2"}, "1234", "567", "1256734", True, "" * 3),
    ({"after_uid": "4"}, "1234", "567", "1234567", True, "" * 3),
    ({"before_uid": "unknown"}, "1234", "567", "1234", False, ["Plan with UID .* is not in the queue"] * 3),
    ({"after_uid": "unknown"}, "1234", "567", "1234", False, ["Plan with UID .* is not in the queue"] * 3),
    ({"before_uid": "unknown", "after_uid": "unknown"}, "1234", "567", "1234",
     False, ["Ambiguous parameters"] * 3),
    ({"pos": "front", "after_uid": "unknown"}, "1234", "567", "1234", False, ["Ambiguous parameters"] * 3),
])
# fmt: on
def test_zmq_api_queue_item_add_batch_1(
    re_manager, batch_params, queue_seq, batch_seq, expected_seq, success, msgs  # noqa: F811
):
    """
    Basic test for ``queue_item_add_batch`` API.
    """
    plan_template = {
        "name": "count",
        "args": [["det1"]],
        "kwargs": {"num": 50, "delay": 0.01},
        "item_type": "plan",
    }

    # Fill the queue with the initial set of plans
    for item_code in queue_seq:
        item = copy.deepcopy(plan_template)
        item["kwargs"]["num"] = int(item_code)
        params = {"item": item, "user": _user, "user_group": _user_group}
        resp1a, _ = zmq_single_request("queue_item_add", params)
        assert resp1a["success"] is True

    state = get_queue_state()
    assert state["items_in_queue"] == len(queue_seq)
    assert state["items_in_history"] == 0

    resp1b, _ = zmq_single_request("queue_get")
    assert resp1b["success"] is True
    queue_initial = resp1b["items"]

    # If there are 'before_uid' or 'after_uid' parameters, then convert values of those
    #   parameters to actual item UIDs.
    def find_uid(dummy_uid):
        """If item is not found, then return ``dummy_uid``"""
        try:
            ind = queue_seq.index(dummy_uid)
            return queue_initial[ind]["item_uid"]
        except Exception:
            return dummy_uid

    if "before_uid" in batch_params:
        batch_params["before_uid"] = find_uid(batch_params["before_uid"])

    if "after_uid" in batch_params:
        batch_params["after_uid"] = find_uid(batch_params["after_uid"])

    # Create a list of items to add
    items_to_add = []
    for item_code in batch_seq:
        item = copy.deepcopy(plan_template)
        item["kwargs"]["num"] = int(item_code)
        items_to_add.append(item)

    # Add the batch
    params = {"items": items_to_add, "user": _user, "user_group": _user_group}
    params.update(batch_params)
    resp2a, _ = zmq_single_request("queue_item_add_batch", params)

    if success:
        assert resp2a["success"] is True
        assert resp2a["msg"] == ""
        assert resp2a["qsize"] == len(expected_seq)
        items_added = resp2a["items"]
        assert len(items_added) == len(batch_seq)
        added_seq = [str(_["kwargs"]["num"]) for _ in items_added]
        added_seq = "".join(added_seq)
        assert added_seq == batch_seq
    else:
        n_total = len(msgs)
        n_success = len([_ for _ in msgs if not (_)])
        n_failed = n_total - n_success
        msg = (
            f"Failed to add all items: validation of {n_failed} out of {n_total} submitted items failed"
            if n_failed
            else ""
        )

        assert resp2a["success"] is False
        assert resp2a["msg"] == msg
        assert resp2a["qsize"] == len(expected_seq)
        items_added = resp2a["items"]
        assert len(items_added) == len(batch_seq)
        added_seq = [str(_["kwargs"]["num"]) for _ in items_added]
        added_seq = "".join(added_seq)
        assert added_seq == batch_seq

    resp2b, _ = zmq_single_request("queue_get")
    assert resp2b["success"] is True
    queue_final = resp2b["items"]
    queue_final_seq = [str(_["kwargs"]["num"]) for _ in queue_final]
    queue_final_seq = "".join(queue_final_seq)
    assert queue_final_seq == expected_seq

    state = get_queue_state()
    assert state["items_in_queue"] == len(expected_seq)
    assert state["items_in_history"] == 0


def test_zmq_api_queue_item_add_batch_2(re_manager):  # noqa: F811
    """
    Test for ``queue_item_add_batch``: copy a batch of existing items from the queue.
    The items have duplicate UIDs, but they still should be added successfully, since
    new UIDs are generated for newly inserted items anyway. This is important feature of
    the API and should be tested.
    """
    plan_template = {
        "name": "count",
        "args": [["det1"]],
        "kwargs": {"num": 50, "delay": 0.01},
        "item_type": "plan",
    }

    queue_seq = "12345"
    expected_seq = "12342345"

    # Fill the queue with the initial set of plans
    for item_code in queue_seq:
        item = copy.deepcopy(plan_template)
        item["kwargs"]["num"] = int(item_code)
        params = {"item": item, "user": _user, "user_group": _user_group}
        resp1a, _ = zmq_single_request("queue_item_add", params)
        assert resp1a["success"] is True

    state = get_queue_state()
    assert state["items_in_queue"] == len(queue_seq)
    assert state["items_in_history"] == 0

    resp1b, _ = zmq_single_request("queue_get")
    assert resp1b["success"] is True
    queue_initial = resp1b["items"]

    # Copy items 1, 2, 3 to create the batch
    items_to_add = queue_initial[1:4]

    # Add the batch
    params = {
        "items": items_to_add,
        "after_uid": items_to_add[-1]["item_uid"],
        "user": _user,
        "user_group": _user_group,
    }
    resp2a, _ = zmq_single_request("queue_item_add_batch", params)
    assert resp2a["success"] is True, pprint.pformat(resp2a)
    assert resp2a["msg"] == ""
    assert resp2a["qsize"] == len(queue_initial) + len(items_to_add)

    resp2b, _ = zmq_single_request("queue_get")
    assert resp2b["success"] is True
    queue_final = resp2b["items"]
    queue_final_seq = [str(_["kwargs"]["num"]) for _ in queue_final]
    queue_final_seq = "".join(queue_final_seq)
    assert queue_final_seq == expected_seq


def test_zmq_api_queue_item_add_batch_3(re_manager):  # noqa: F811
    """
    Test for ``queue_item_add_batch``: add a batch of items (including plans and instructions)
    and execute the queue.
    """
    items = [_plan1, _plan2, _instruction_stop, _plan3]

    params = {"items": items, "user": _user, "user_group": _user_group}
    resp1a, _ = zmq_single_request("queue_item_add_batch", params)
    assert resp1a["success"] is True, f"resp={resp1a}"
    assert resp1a["msg"] == ""
    assert resp1a["qsize"] == 4
    item_list = resp1a["items"]
    item_results = resp1a["results"]
    assert len(item_list) == len(items)
    assert len(item_results) == len(items)

    for n, item in enumerate(items):
        item_res, res = item_list[n], item_results[n]
        assert res["success"] is True, str(item)
        assert res["msg"] == "", str(item)

        assert "name" in item_res, str(item_res)
        assert item_res["name"] == item["name"]
        assert isinstance(item_res["item_uid"], str)
        assert item_res["item_uid"]

        if "args" in item:
            assert item_res["args"] == item["args"]
        else:
            assert "args" not in item_res
        if "kwargs" in item:
            assert item_res["kwargs"] == item["kwargs"]
        else:
            assert "kwargs" not in item_res

    state = get_queue_state()
    assert state["items_in_queue"] == 4
    assert state["items_in_history"] == 0

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp3, _ = zmq_single_request("queue_start")
    assert resp3["success"] is True
    assert wait_for_condition(time=30, condition=condition_manager_idle)

    state = get_queue_state()
    assert state["items_in_queue"] == 1
    assert state["items_in_history"] == 2

    resp4, _ = zmq_single_request("queue_start")
    assert resp4["success"] is True
    assert wait_for_condition(time=10, condition=condition_queue_processing_finished)

    state = get_queue_state()
    assert state["items_in_queue"] == 0
    assert state["items_in_history"] == 3

    resp5, _ = zmq_single_request("environment_close")
    assert resp5["success"] is True
    assert wait_for_condition(time=5, condition=condition_manager_idle)


def test_zmq_api_queue_item_add_batch_4_fail(re_manager):  # noqa: F811
    """
    Test for ``queue_item_add_batch`` API. Attempt to add a batch that contains invalid plans.
    Make sure that the invalid plans are detected, correct error messages are returned and the
    plans from the batch are not added to the queue.
    """
    _plan2_corrupt = _plan2.copy()
    _plan2_corrupt["name"] = "nonexisting_name"
    items = [_plan1, _plan2_corrupt, _instruction_stop, {}, _plan3]
    success_expected = [True, False, True, False, True]
    msg_expected = ["", "is not in the list of allowed plans", "", "'item_type' key is not found", ""]

    params = {"items": items, "user": _user, "user_group": _user_group}
    resp1a, _ = zmq_single_request("queue_item_add_batch", params)
    assert resp1a["success"] is False, f"resp={resp1a}"
    assert resp1a["msg"] == "Failed to add all items: validation of 2 out of 5 submitted items failed"
    assert resp1a["qsize"] == 0
    item_list = resp1a["items"]
    item_results = resp1a["results"]
    assert len(item_list) == len(items)
    assert len(item_results) == len(items)

    assert item_list == items
    for n, res in enumerate(item_results):
        assert res["success"] == success_expected[n], str(res)
        assert msg_expected[n] in res["msg"], str(res)

    state = get_queue_state()
    assert state["items_in_queue"] == 0
    assert state["items_in_history"] == 0


# =======================================================================================
#                            Method 'queue_item_update'

# fmt: on
@pytest.mark.parametrize("replace", [None, False, True])
# fmt: off
def test_zmq_api_queue_item_update_1(re_manager, replace):  # noqa F811
    """
    Basic test for `queue_item_update` method.
    """

    resp1, _ = zmq_single_request("queue_item_add", {"item": _plan1, "user": _user, "user_group": _user_group})
    assert resp1["success"] is True
    assert resp1["qsize"] == 1
    assert resp1["item"]["name"] == _plan1["name"]
    assert resp1["item"]["args"] == _plan1["args"]
    assert resp1["item"]["user"] == _user
    assert resp1["item"]["user_group"] == _user_group
    assert "item_uid" in resp1["item"]

    plan = resp1["item"]
    uid = plan["item_uid"]

    plan_changed = plan.copy()
    plan_new_args = [["det1"]]
    plan_changed["args"] = plan_new_args

    user_replaced = "Different User"
    params = {"item": plan_changed, "user": user_replaced, "user_group": _user_group}
    if replace is not None:
        params["replace"] = replace

    status1 = get_queue_state()

    resp2, _ = zmq_single_request("queue_item_update", params)
    assert resp2["success"] is True
    assert resp2["qsize"] == 1
    assert resp2["item"]["name"] == _plan1["name"]
    assert resp2["item"]["args"] == plan_new_args
    assert resp2["item"]["user"] == user_replaced
    assert resp2["item"]["user_group"] == _user_group
    assert "item_uid" in resp2["item"]
    if replace:
        assert resp2["item"]["item_uid"] != uid
    else:
        assert resp2["item"]["item_uid"] == uid

    status2 = get_queue_state()
    assert status2["plan_queue_uid"] != status1["plan_queue_uid"]
    assert status2["plan_history_uid"] == status1["plan_history_uid"]

    resp3, _ = zmq_single_request("queue_get")
    assert resp3["items"] != []
    assert len(resp3["items"]) == 1
    assert resp3["items"][0] == resp2["item"]
    assert resp3["running_item"] == {}
    assert resp3["plan_queue_uid"] == status2["plan_queue_uid"]


# fmt: on
@pytest.mark.parametrize("replace", [None, False, True])
# fmt: off
def test_zmq_api_queue_item_update_2_fail(re_manager, replace):  # noqa F811
    """
    Failing cases for `queue_item_update`: submitted item UID does not match any UID in the queue.
    """
    resp1, _ = zmq_single_request("queue_item_add", {"item": _plan1, "user": _user, "user_group": _user_group})
    assert resp1["success"] is True
    assert resp1["qsize"] == 1
    assert resp1["item"]["name"] == _plan1["name"]
    assert resp1["item"]["args"] == _plan1["args"]
    assert resp1["item"]["user"] == _user
    assert resp1["item"]["user_group"] == _user_group
    assert "item_uid" in resp1["item"]

    plan = resp1["item"]

    plan_changed = plan.copy()
    plan_changed["args"] = [["det1"]]
    plan_changed["item_uid"] = "incorrect_uid"

    user_replaced = "Different User"
    params = {"item": plan_changed, "user": user_replaced, "user_group": _user_group}
    if replace is not None:
        params["replace"] = replace

    resp2, _ = zmq_single_request("queue_item_update", params)
    assert resp2["success"] is False
    assert resp2["msg"] == "Failed to add an item: Failed to replace item: " \
                           "Item with UID 'incorrect_uid' is not in the queue"

    resp3, _ = zmq_single_request("queue_get")
    assert resp3["items"] != []
    assert len(resp3["items"]) == 1
    assert resp3["items"][0] == plan
    assert resp3["running_item"] == {}


# fmt: on
@pytest.mark.parametrize("replace", [None, False, True])
# fmt: off
def test_zmq_api_queue_item_update_3_fail(re_manager, replace):  # noqa F811
    """
    Failing cases for `queue_item_update`: submitted item UID does not match any UID in the queue
    (the case of empty queue - expected to work the same as for non-empty queue)
    """
    resp1, _ = zmq_single_request("queue_get")
    assert resp1["items"] == []
    assert resp1["running_item"] == {}

    plan_changed = _plan1
    plan_changed["item_uid"] = "incorrect_uid"

    user_replaced = "Different User"
    params = {"item": plan_changed, "user": user_replaced, "user_group": _user_group}
    if replace is not None:
        params["replace"] = replace

    resp2, _ = zmq_single_request("queue_item_update", params)
    assert resp2["success"] is False
    assert resp2["msg"] == "Failed to add an item: Failed to replace item: " \
                           "Item with UID 'incorrect_uid' is not in the queue"

    resp3, _ = zmq_single_request("queue_get")
    assert resp3["items"] == []
    assert resp3["running_item"] == {}


def test_zmq_api_queue_item_update_4_fail(re_manager):  # noqa F811
    """
    Failing cases for ``queue_item_update`` API: verify that it works identically to 'queue_item_add' for
    all failing cases.
    """

    resp1, _ = zmq_single_request("queue_item_add", {"item": _plan1, "user": _user, "user_group": _user_group})
    assert resp1["success"] is True
    assert resp1["qsize"] == 1
    assert resp1["item"]["name"] == _plan1["name"]
    assert resp1["item"]["args"] == _plan1["args"]
    assert resp1["item"]["user"] == _user
    assert resp1["item"]["user_group"] == _user_group
    assert "item_uid" in resp1["item"]

    plan_to_update = resp1["item"].copy()

    # Unknown plan name
    plan2 = plan_to_update.copy()
    plan2["name"] = "count_test"
    params2 = {"item": plan2, "user": _user, "user_group": _user_group}
    resp2, _ = zmq_single_request("queue_item_update", params2)
    assert resp2["success"] is False
    assert "Plan 'count_test' is not in the list of allowed plans" in resp2["msg"]

    # Unknown kwarg
    plan3 = plan_to_update.copy()
    plan3["kwargs"] = {"abc": 10}
    params3 = {"item": plan3, "user": _user, "user_group": _user_group}
    resp3, _ = zmq_single_request("queue_item_update", params3)
    assert resp3["success"] is False
    assert (
        "Failed to add an item: Plan validation failed: got an unexpected keyword argument 'abc'" in resp3["msg"]
    )

    # User name is not specified
    params4 = {"item": plan_to_update, "user_group": _user_group}
    resp4, _ = zmq_single_request("queue_item_update", params4)
    assert resp4["success"] is False
    assert "user name is not specified" in resp4["msg"]

    # User group is not specified
    params5 = {"item": plan_to_update, "user": _user}
    resp5, _ = zmq_single_request("queue_item_update", params5)
    assert resp5["success"] is False
    assert "user group is not specified" in resp5["msg"]

    # Unknown user group
    params6 = {"item": plan_to_update, "user": _user, "user_group": "no_such_group"}
    resp6, _ = zmq_single_request("queue_item_update", params6)
    assert resp6["success"] is False
    assert "Unknown user group: 'no_such_group'" in resp6["msg"]

    # Missing item parameters
    params7 = {"user": _user, "user_group": _user_group}
    resp7, _ = zmq_single_request("queue_item_update", params7)
    assert resp7["success"] is False
    assert resp7["item"] is None
    assert "Incorrect request format: request contains no item info" in resp7["msg"]

    # Incorrect type of the item parameter (must be dict)
    params8 = {"item": [], "user": _user, "user_group": _user_group}
    resp8, _ = zmq_single_request("queue_item_update", params8)
    assert resp8["success"] is False
    assert resp8["item"] == []
    assert "item parameter must have type 'dict'" in resp8["msg"]

    # Unsupported item type
    plan9 = plan_to_update.copy()
    plan9["item_type"] = "unsupported"
    params9 = {"item": plan9, "user": _user, "user_group": _user_group}
    resp9, _ = zmq_single_request("queue_item_update", params9)
    assert resp9["success"] is False
    assert resp9["item"] == plan9
    assert "Incorrect request format: unsupported 'item_type' value: 'unsupported'" in resp9["msg"]

    # Valid plan
    plan10 = plan_to_update.copy()
    plan10["args"] = [["det1"]]
    params10 = {"item": plan10, "user": _user, "user_group": _user_group}
    resp10, _ = zmq_single_request("queue_item_update", params10)
    assert resp10["success"] is True
    assert resp10["qsize"] == 1
    assert resp10["item"]["name"] == "count"
    assert resp10["item"]["args"] == [["det1"]]
    assert resp10["item"]["user"] == _user
    assert resp10["item"]["user_group"] == _user_group
    assert "item_uid" in resp10["item"]
    assert resp10["item"]["item_uid"] == plan_to_update["item_uid"]

    resp11, _ = zmq_single_request("queue_get")
    assert resp11["items"] != []
    assert len(resp11["items"]) == 1
    assert resp11["items"][0] == resp10["item"]
    assert resp11["running_item"] == {}


# =======================================================================================
#                              Method 'script_upload'


_script_to_upload_1 = """
# Another device
from ophyd import Device

dev_test = Device(name="dev_test")

# Trivial plan
def sleep_for_a_few_sec(tt=1):
    yield from bps.sleep(tt)
"""


# fmt: off
@pytest.mark.parametrize("run_in_background", [None, False, True])
# fmt: on
def test_zmq_api_script_upload_1(re_manager, run_in_background):  # noqa: F811
    """
    Basic test for ``script_upload`` API: detailed checks of all flag at each transition.
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    status, _ = zmq_single_request("status")
    task_results_uid = status["task_results_uid"]
    plans_allowed_uid = status["plans_allowed_uid"]
    devices_allowed_uid = status["devices_allowed_uid"]
    plans_existing_uid = status["plans_existing_uid"]
    devices_existing_uid = status["devices_existing_uid"]
    assert isinstance(task_results_uid, str)
    assert status["worker_background_tasks"] == 0

    # Make the plan sleep for 1 second (emulates a script that takes 1s to load)
    script = "import time as ttime\nttime.sleep(1)\n" + _script_to_upload_1

    params = {"script": script}
    if run_in_background is not None:
        params.update({"run_in_background": run_in_background})
    else:
        run_in_background = False
    print(f"Parameters for 'script_upload': {params}")

    resp2, _ = zmq_single_request("script_upload", params=params)
    assert resp2["success"] is True, pprint.pformat(resp2)
    assert resp2["msg"] == ""
    assert resp2["task_uid"]
    task_uid = resp2["task_uid"]
    assert isinstance(task_uid, str)

    status, _ = zmq_single_request("status")
    assert status["task_results_uid"] == task_results_uid
    assert status["plans_allowed_uid"] == plans_allowed_uid
    assert status["devices_allowed_uid"] == devices_allowed_uid
    assert status["plans_existing_uid"] == plans_existing_uid
    assert status["devices_existing_uid"] == devices_existing_uid
    assert status["manager_state"] == "idle" if run_in_background else "executing_task"

    ttime.sleep(0.6)  # Wait until worker state is updated by RE Manager
    status, _ = zmq_single_request("status")
    assert status["worker_background_tasks"] == (1 if run_in_background else 0)

    resp3, _ = zmq_single_request("task_result", params={"task_uid": task_uid})
    assert resp3["success"] is True
    assert resp3["msg"] == ""
    assert resp3["task_uid"] == task_uid
    assert resp3["status"] == "running"
    result = resp3["result"]
    assert isinstance(result, dict)
    assert isinstance(result["time_start"], float)
    assert result["task_uid"] == task_uid
    assert result["run_in_background"] is run_in_background

    ttime.sleep(1.5)

    status, _ = zmq_single_request("status")
    assert status["task_results_uid"] != task_results_uid
    assert status["plans_allowed_uid"] != plans_allowed_uid
    assert status["devices_allowed_uid"] != devices_allowed_uid
    assert status["plans_existing_uid"] != plans_existing_uid
    assert status["devices_existing_uid"] != devices_existing_uid
    assert status["manager_state"] == "idle"
    assert status["worker_environment_state"] == "idle"
    assert status["items_in_queue"] == 0
    assert status["items_in_history"] == 0
    assert status["worker_background_tasks"] == 0

    resp4, _ = zmq_single_request("task_result", params={"task_uid": task_uid})
    assert resp4["success"] is True
    assert resp4["msg"] == ""
    assert resp4["task_uid"] == task_uid
    assert resp4["status"] == "completed"
    result = resp4["result"]
    assert isinstance(result, dict)
    assert isinstance(result["time_start"], float)
    assert isinstance(result["time_stop"], float)
    assert result["task_uid"] == task_uid
    assert result["success"] is True
    assert result["msg"] == ""
    assert result["return_value"] is None

    # Check that the new plan and the new device are in the new list of available plans and devices
    resp5a, _ = zmq_single_request("plans_allowed", params={"user_group": _user_group})
    assert resp5a["success"] is True, resp5a
    assert "sleep_for_a_few_sec" in resp5a["plans_allowed"]

    resp5b, _ = zmq_single_request("devices_allowed", params={"user_group": _user_group})
    assert resp5b["success"] is True, resp5b
    assert "dev_test" in resp5b["devices_allowed"]

    resp5c, _ = zmq_single_request("plans_existing")
    assert resp5c["success"] is True, resp5c
    assert "sleep_for_a_few_sec" in resp5c["plans_existing"]

    resp5d, _ = zmq_single_request("devices_existing")
    assert resp5d["success"] is True, resp5d
    assert "dev_test" in resp5d["devices_existing"]

    # Add plan to queue
    _p6 = {"name": "sleep_for_a_few_sec", "kwargs": {"tt": 1.5}, "item_type": "plan"}
    params6 = {"item": _p6, "user": _user, "user_group": _user_group}
    resp6, _ = zmq_single_request("queue_item_add", params6)
    assert resp6["success"] is True, f"resp={resp6}"

    resp7, _ = zmq_single_request("queue_start")
    assert resp7["success"] is True

    assert wait_for_condition(time=5, condition=condition_manager_idle)

    status, _ = zmq_single_request("status")
    assert status["items_in_queue"] == 0
    assert status["items_in_history"] == 1

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)

    status, _ = zmq_single_request("status")
    assert status["worker_background_tasks"] == 0


_script_to_upload_2a = """
# Device
from ophyd import Device
dev_test = Device(name="dev_test")
"""

_script_to_upload_2b = """
# Trivial plan
def sleep_for_a_few_sec(tt=1):
    yield from bps.sleep(tt)
"""


# fmt: off
@pytest.mark.parametrize("scripts, updated_devs, updated_plans", [
    ([_script_to_upload_2a], ["dev_test"], []),
    ([_script_to_upload_2b], [], ["sleep_for_a_few_sec"]),
    ([_script_to_upload_2a, _script_to_upload_2b], ["dev_test"], ["sleep_for_a_few_sec"]),
])
# fmt: on
def test_zmq_api_script_upload_2(re_manager, scripts, updated_devs, updated_plans):  # noqa: F811
    """
    'script_upload' API: load scripts that contain only devices and only plans separately
    or both. Make sure that the plan and the device are included in the lists of existing
    and allowed plans and devices when necessary. Check that list UIDs are properly updated.
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    status, _ = zmq_single_request("status")
    task_results_uid = status["task_results_uid"]
    plans_allowed_uid = status["plans_allowed_uid"]
    devices_allowed_uid = status["devices_allowed_uid"]
    plans_existing_uid = status["plans_existing_uid"]
    devices_existing_uid = status["devices_existing_uid"]

    for script in scripts:
        resp2, _ = zmq_single_request("script_upload", params={"script": script})
        assert resp2["success"] is True, pprint.pformat(resp2)
        assert resp2["msg"] == ""
        task_uid = resp2["task_uid"]

        result = wait_for_task_result(10, task_uid)
        assert result["return_value"] is None

    status, _ = zmq_single_request("status")
    assert status["task_results_uid"] != task_results_uid

    if updated_plans:
        assert status["plans_allowed_uid"] != plans_allowed_uid
        assert status["plans_existing_uid"] != plans_existing_uid
    else:
        assert status["plans_allowed_uid"] == plans_allowed_uid
        assert status["plans_existing_uid"] == plans_existing_uid

    if updated_devs:
        assert status["devices_allowed_uid"] != devices_allowed_uid
        assert status["devices_existing_uid"] != devices_existing_uid
    else:
        assert status["devices_allowed_uid"] == devices_allowed_uid
        assert status["devices_existing_uid"] == devices_existing_uid

    resp5a, _ = zmq_single_request("plans_allowed", params={"user_group": _user_group})
    assert resp5a["success"] is True, resp5a
    plans_allowed = resp5a["plans_allowed"]

    resp5b, _ = zmq_single_request("devices_allowed", params={"user_group": _user_group})
    assert resp5b["success"] is True, resp5b
    devices_allowed = resp5b["devices_allowed"]

    resp5c, _ = zmq_single_request("plans_existing")
    assert resp5c["success"] is True, resp5c
    plans_existing = resp5c["plans_existing"]

    resp5d, _ = zmq_single_request("devices_existing")
    assert resp5d["success"] is True, resp5d
    devices_existing = resp5d["devices_existing"]

    for plan_name in updated_plans:
        assert plan_name in plans_existing
        assert plan_name in plans_allowed

    for dev_name in updated_devs:
        assert dev_name in devices_existing
        assert dev_name in devices_allowed

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


_script_to_upload_3a = """
import time as tt
def sleep_for_a_few_sec_1(tt=1):
    yield from bps.sleep(tt)

tt.sleep(2)  # Emulate a script that runs for 2 seconds

def sleep_for_a_few_sec_2(tt=1):
    yield from bps.sleep(tt)
"""

_script_to_upload_3b = """
# Trivial plan
def sleep_for_a_few_sec_3(tt=1):
    yield from bps.sleep(tt)
"""


# fmt: off
@pytest.mark.parametrize("use_bg_task", [False, True])
# fmt: on
def test_zmq_api_script_upload_3(re_manager, use_bg_task):  # noqa: F811
    """
    'script_upload' API: Load two scripts in parallel. Script #1 takes 2 seconds to
    load is foreground or background task. Script #2 is loaded as a background task
    while Script #1 is being loaded. No race conditions are expected while two
    scripts are running. Check that all loaded plans are in the list of allowed plans.
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # Task 1
    resp2a, _ = zmq_single_request(
        "script_upload",
        params={"script": _script_to_upload_3a, "run_in_background": use_bg_task},
    )
    assert resp2a["success"] is True, pprint.pformat(resp2a)
    task_uid_1 = resp2a["task_uid"]

    ttime.sleep(1)

    # Task 2
    resp2b, _ = zmq_single_request(
        "script_upload",
        params={"script": _script_to_upload_3b, "run_in_background": True},
    )
    assert resp2b["success"] is True, pprint.pformat(resp2a)
    task_uid_2 = resp2b["task_uid"]

    # Wait for each task to complete
    result_1 = wait_for_task_result(10, task_uid_1)
    result_2 = wait_for_task_result(10, task_uid_2)

    # Make sure that execution of Task 2 is completed while Task1 is running
    assert result_1["time_start"] < result_2["time_start"]
    assert result_1["time_stop"] > result_2["time_stop"]

    resp5a, _ = zmq_single_request("plans_allowed", params={"user_group": _user_group})
    assert resp5a["success"] is True, resp5a
    plans_allowed = resp5a["plans_allowed"]

    # Check that all plans are imported
    plan_names = ["sleep_for_a_few_sec_1", "sleep_for_a_few_sec_2", "sleep_for_a_few_sec_3"]
    for plan_name in plan_names:
        assert plan_name in plans_allowed

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_zmq_api_script_upload_4(tmp_path, re_manager_cmd):  # noqa: F811
    """
    'script_upload' API: Open the environent with 'empty' startup file and then
    load full collection of built-in startup files using the API. Compare the lists
    of existing plans and devices with the lists obtained from built-in profile
    collection. Start a simple 'count' plan and make sure it completes correctly.
    """
    pc_path = copy_default_profile_collection(tmp_path=tmp_path, copy_py=False)
    os.remove(os.path.join(pc_path, "existing_plans_and_devices.yaml"))
    # Only 'user_group_permissions.yaml' is left in the directory

    # Create an empty startup script
    with open(os.path.join(pc_path, "00-startup.py"), "w"):
        pass

    re_manager_cmd(["--startup-dir", pc_path])

    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # At this point the lists of allowed plans and devices are expected to be empty.
    resp2a, _ = zmq_single_request("plans_existing")
    assert resp2a["success"] is True, pprint.pformat(resp2a)
    assert resp2a["plans_existing"] == {}
    resp2b, _ = zmq_single_request("devices_existing")
    assert resp2b["success"] is True, pprint.pformat(resp2a)
    assert resp2b["devices_existing"] == {}

    # Now send built-in startup script files one by one over 0MQ
    default_pc_path = get_default_startup_dir()
    default_files = glob.glob(os.path.join(default_pc_path, "*.py"))
    default_files.sort()
    for fn in default_files:
        with open(fn, "r") as f:
            script = f.read()
            resp3, _ = zmq_single_request("script_upload", params={"script": script})
            assert resp3["success"] is True
            wait_for_task_result(10, resp3["task_uid"])
            resp3a, _ = zmq_single_request("task_result", params={"task_uid": resp3["task_uid"]})
            assert resp3a["success"] is True
            assert resp3a["result"]["success"] is True, resp3a["result"]["return_value"]

    # At this point the list of existing plans and devices must be identical to the default
    nspace = load_profile_collection(default_pc_path)
    default_devices = _prepare_devices(devices_from_nspace(nspace))
    default_plans = _prepare_plans(plans_from_nspace(nspace), existing_devices=default_devices)
    # Converting to JSON and back gets the same representation as we get by downloading the list
    default_devices = json.loads(json.dumps(default_devices))
    default_plans = json.loads(json.dumps(default_plans))

    resp4a, _ = zmq_single_request("plans_existing")
    assert resp4a["success"] is True, pprint.pformat(resp4a)
    # Keys are easier to compare, so first compare keys
    assert set(resp4a["plans_existing"].keys()) == set(default_plans.keys())
    # Now compare the plan descriptions one by one (easier to read error messages)
    for k in default_plans.keys():
        assert resp4a["plans_existing"][k] == default_plans[k]

    resp4b, _ = zmq_single_request("devices_existing")
    assert resp4b["success"] is True, pprint.pformat(resp4a)
    assert resp4b["devices_existing"] == default_devices

    # Now try to run a simple plan and make sure it works
    resp5a, _ = zmq_single_request("queue_item_add", {"item": _plan1, "user": _user, "user_group": _user_group})
    assert resp5a["success"] is True

    resp5b, _ = zmq_single_request("queue_start")
    assert resp5b["success"] is True

    assert wait_for_condition(20, condition_queue_processing_finished)

    status, _ = zmq_single_request("status")
    assert status["items_in_queue"] == 0
    assert status["items_in_history"] == 1

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_zmq_api_script_upload_5(tmp_path, re_manager_cmd):  # noqa: F811
    """
    'script_upload' API: Check that local imports work.
    """
    pc_path = copy_default_profile_collection(tmp_path=tmp_path, copy_py=False)
    os.remove(os.path.join(pc_path, "existing_plans_and_devices.yaml"))
    # Only 'user_group_permissions.yaml' is left in the directory

    mod_dir = os.path.join(pc_path, "mod")
    os.makedirs(os.path.join(pc_path, "mod"))

    # Create an empty startup script
    with open(os.path.join(pc_path, "00-startup.py"), "w"):
        pass

    # Create a module
    mod_fln = os.path.join(mod_dir, "mod_file.py")
    with open(os.path.join(mod_fln), "w") as f:
        f.writelines(_script_to_upload_1)

    re_manager_cmd(["--startup-dir", pc_path])

    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # Upload the module that uses local imports
    script = "from mod.mod_file import *\n"
    resp2, _ = zmq_single_request("script_upload", params={"script": script})
    result = wait_for_task_result(10, resp2["task_uid"])
    assert result["success"] is True, pprint.pformat(result)
    assert result["msg"] == "", pprint.pformat(result)

    # Check that the plan and the device was imported from the module
    resp4a, _ = zmq_single_request("plans_existing")
    assert resp4a["success"] is True, pprint.pformat(resp4a)
    assert "sleep_for_a_few_sec" in resp4a["plans_existing"]
    resp4b, _ = zmq_single_request("devices_existing")
    assert resp4b["success"] is True, pprint.pformat(resp4a)
    assert "dev_test" in resp4b["devices_existing"]

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


_script_save_instances_re_db = """
RE_backup = RE
db_backup = db
"""


# fmt: off
@pytest.mark.parametrize("update_re_param", [False, True])
@pytest.mark.parametrize("replace_re", [False, True])
@pytest.mark.parametrize("replace_db", [False, True])
# fmt: on
def test_zmq_api_script_upload_6(re_manager_cmd, update_re_param, replace_re, replace_db):  # noqa: F811
    """
    'script_upload' API: Test that instances 'RE' and 'db' could be replaced in
    the RE Worker namespace. The test does not check if references kept internally by RE Worker
    are updated, but the update happens in the same branches where the namespace is updated.
    """

    # Make sure that the environment contains databroker instance
    re_manager_cmd(["--databroker-config", "temp"])

    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # Upload script that saves instances of 'RE' and 'db' in the namespace
    resp2, _ = zmq_single_request("script_upload", params={"script": _script_save_instances_re_db})
    result = wait_for_task_result(10, resp2["task_uid"])
    assert result["success"] is True, pprint.pformat(result)

    script_replace_re_and_db = ""
    if replace_re:
        script_replace_re_and_db += "from bluesky import RunEngine\nRE = RunEngine()\n"
    if replace_db:
        script_replace_re_and_db += 'from databroker import Broker\ndb = Broker.named("temp")\n'

    resp3, _ = zmq_single_request(
        "script_upload", params={"script": script_replace_re_and_db, "update_re": update_re_param}
    )
    result = wait_for_task_result(10, resp3["task_uid"])
    assert result["success"] is True, pprint.pformat(result)

    # Upload the script the verifies that the environment has new instances of RE and db.
    #     The script fails to load if the RE or db is not updated properly when required.
    script_verify_re_and_db = ""
    if replace_re and update_re_param:
        script_verify_re_and_db += "assert RE != RE_backup\n"
    else:
        script_verify_re_and_db += "assert RE == RE_backup\n"
    if replace_db and update_re_param:
        script_verify_re_and_db += "assert db != db_backup\n"
    else:
        script_verify_re_and_db += "assert db == db_backup\n"

    resp4, _ = zmq_single_request("script_upload", params={"script": script_verify_re_and_db})
    result = wait_for_task_result(10, resp4["task_uid"])
    assert result["success"] is True, pprint.pformat(result)

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_zmq_api_script_upload_7(re_manager):  # noqa: F811
    """
    'script_upload' API: Check that the environment can be destroyed while a script is
    being loaded. It could be necessary to destroy the environment to terminate execution
    of a script (e.g. a script with infinite loop).
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # Run the script in foreground
    long_script = "import time as tt\ntt.sleep(20)\n"
    resp2, _ = zmq_single_request("script_upload", params={"script": long_script})
    assert resp2["success"] is True

    # Attempt to close the environment
    resp3, _ = zmq_single_request("environment_close")
    assert resp3["success"] is False, f"resp={resp3}"

    ttime.sleep(1)  # Make sure the script is already running

    # Attempt to close the environment
    resp4, _ = zmq_single_request("environment_close")
    assert resp4["success"] is False, f"resp={resp4}"

    status, _ = zmq_single_request("status")
    assert status["worker_background_tasks"] == 0  # Not a background task

    # Destroy the environment
    resp5, _ = zmq_single_request("environment_destroy")
    assert resp5["success"] is True, f"resp={resp5}"
    assert wait_for_condition(time=10, condition=condition_environment_closed)

    # Open and close the environment to make sure everything works
    resp6a, _ = zmq_single_request("environment_open")
    assert resp6a["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp6b, _ = zmq_single_request("environment_close")
    assert resp6b["success"] is True, f"resp={resp6b}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_zmq_api_script_upload_8_fail(re_manager):  # noqa: F811
    """
    'script_upload' API: Check if call fails if the environment is not open.
    """
    resp2, _ = zmq_single_request("script_upload", params={"script": _script_to_upload_1})
    assert resp2["success"] is False
    assert "RE Worker environment is not open" in resp2["msg"]


# fmt: off
@pytest.mark.parametrize("test_with_plan", [True, False])
# fmt: on
def test_zmq_api_script_upload_9_fail(re_manager, test_with_plan):  # noqa: F811
    """
    'script_upload' API: Check if script upload request fails if another script or
    a plan is running.
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    if test_with_plan:
        # Now try to run a simple plan and make sure it works
        resp2a, _ = zmq_single_request(
            "queue_item_add", {"item": _plan3, "user": _user, "user_group": _user_group}
        )
        assert resp2a["success"] is True
        resp2b, _ = zmq_single_request("queue_start")
        assert resp2b["success"] is True
    else:
        script = "import time as tt\ntt.sleep(3)\n"
        resp3, _ = zmq_single_request("script_upload", params={"script": script})
        assert resp3["success"] is True

    resp4a, _ = zmq_single_request("script_upload", params={"script": _script_to_upload_1})
    assert resp4a["success"] is False
    assert "RE Manager must be in idle state" in resp4a["msg"], resp4a["msg"]

    ttime.sleep(1)

    resp4b, _ = zmq_single_request("script_upload", params={"script": _script_to_upload_1})
    assert resp4b["success"] is False
    assert "RE Manager must be in idle state" in resp4b["msg"], resp4b["msg"]

    assert wait_for_condition(time=10, condition=condition_manager_idle)

    # Now try again, it should work
    resp5, _ = zmq_single_request("script_upload", params={"script": _script_to_upload_1})
    assert resp5["success"] is True
    wait_for_task_result(time=10, task_uid=resp5["task_uid"])

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# ===========================================================================================
#                                'task_status' API


def test_zmq_api_task_status_1(re_manager):  # noqa: F811
    """
    ``task_status``: basic test.
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    status, _ = zmq_single_request("status")
    assert status["worker_background_tasks"] == 0

    func_item = {"name": "function_sleep", "item_type": "function", "args": [2.0]}
    params = {"item": func_item, "run_in_background": True, "user": _user, "user_group": _test_user_group}

    task_uids = []
    for _ in range(3):
        resp1, _ = zmq_single_request("function_execute", params=params)
        assert resp1["success"] is True
        task_uids.append(resp1["task_uid"])

    assert len(task_uids) == 3

    resp2, _ = zmq_single_request("task_status", params={"task_uid": task_uids[0]})
    assert resp2["success"] is True
    assert resp2["msg"] == ""
    assert resp2["status"] == "running"

    resp3, _ = zmq_single_request("task_status", params={"task_uid": [task_uids[0]]})
    assert resp3["success"] is True
    assert resp3["msg"] == ""
    assert resp3["status"] == {task_uids[0]: "running"}

    resp4, _ = zmq_single_request("task_status", params={"task_uid": (task_uids[0], task_uids[1])})
    assert resp4["success"] is True
    assert resp4["msg"] == ""
    assert resp4["status"] == {task_uids[0]: "running", task_uids[1]: "running"}

    resp5, _ = zmq_single_request("task_status", params={"task_uid": task_uids})
    assert resp5["success"] is True
    assert resp5["msg"] == ""
    assert resp5["status"] == {_: "running" for _ in task_uids}

    result = wait_for_task_result(10, task_uids[-1])
    assert result["success"] is True, pprint.pformat(result)

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_zmq_api_task_status_2(re_manager):  # noqa: F811
    """
    ``task_status``: some special successfull cases.
    """
    resp1, _ = zmq_single_request("task_status", params={"task_uid": "some_uid"})
    assert resp1["success"] is True
    assert resp1["msg"] == ""
    assert resp1["status"] == "not_found"

    resp2, _ = zmq_single_request("task_status", params={"task_uid": ["uid1", "uid2"]})
    assert resp2["success"] is True
    assert resp2["msg"] == ""
    assert resp2["status"] == {"uid1": "not_found", "uid2": "not_found"}

    resp3, _ = zmq_single_request("task_status", params={"task_uid": ["uid1", "uid1"]})
    assert resp3["success"] is True
    assert resp3["msg"] == ""
    assert resp3["status"] == {"uid1": "not_found"}


# fmt: off
@pytest.mark.parametrize("params, err_msg", [
    ({}, "Required 'task_uid' parameter is missing in the API call"),
    ({"some_param": 10}, "API request contains unsupported parameters: 'some_param'"),
    ({"task_uid": 10}, "'task_uid' must be a string or a list of strings"),
    ({"task_uid": "uid1", "some_param": 10},
     "API request contains unsupported parameters: 'some_param'"),
])
# fmt: on
def test_zmq_api_task_status_3_fail(re_manager, params, err_msg):  # noqa: F811
    """
    ``task_status``: failing cases.
    """
    resp1, _ = zmq_single_request("task_status", params=params)
    assert resp1["success"] is False
    assert err_msg in resp1["msg"], pprint.pformat(resp1)
    assert resp1["status"] is None


# ===========================================================================================
#                             'function_execute' API


# fmt: off
@pytest.mark.parametrize("run_in_background, wait_for_idle", [
    (False, False),
    (False, True),
    (True, False),
])
@pytest.mark.parametrize("test_with_args", [True, False])
# fmt: on
def test_zmq_api_function_execute_1(re_manager, run_in_background, wait_for_idle, test_with_args):  # noqa: F811
    """
    ``function_execute`` 0MQ API: basic tests.
    """
    status, _ = zmq_single_request("status")
    assert status["worker_background_tasks"] == 0

    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    func_item = {"name": "function_sleep", "item_type": "function"}
    if test_with_args:
        func_item.update({"args": [1.0]})
    else:
        func_item.update({"kwargs": {"time": 1.0}})

    params = {
        "item": func_item,
        "run_in_background": run_in_background,
        "user": _user,
        "user_group": _test_user_group,
    }
    resp1, _ = zmq_single_request("function_execute", params=params)
    assert resp1["success"] is True
    # Detailed check of the return values
    assert resp1["msg"] == ""
    task_uid = resp1["task_uid"]
    assert isinstance(task_uid, str)
    returned_item = resp1["item"]
    assert returned_item["name"] == func_item["name"]
    assert returned_item["user"] == params["user"]
    assert returned_item["user_group"] == params["user_group"]
    assert returned_item["item_uid"] == task_uid

    ttime.sleep(0.5)
    status, _ = zmq_single_request("status")
    assert status["worker_background_tasks"] == (1 if run_in_background else 0)
    assert status["worker_environment_state"] == ("idle" if run_in_background else "executing_task")
    assert status["manager_state"] == ("idle" if run_in_background else "executing_task")

    if wait_for_idle:
        # Check that RE Manager state is managed correctly, i.e. we can wait for
        #   manager state to switch to idle. This only makes sense when function is
        #   executed on the foreground.
        assert wait_for_condition(time=10, condition=condition_manager_idle)
        resp2, _ = zmq_single_request("task_result", params={"task_uid": task_uid})
        assert resp2["success"] is True, pprint.pformat(resp2)
        assert resp2["result"]["success"] is True, pprint.pformat(resp2["result"])
    else:
        # Just wait for the result to be ready.
        result = wait_for_task_result(10, task_uid)
        assert result["success"] is True, pprint.pformat(result)

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)

    status, _ = zmq_single_request("status")
    assert status["worker_background_tasks"] == 0


# fmt: off
@pytest.mark.parametrize("run_in_background, option", [
    (False, "standalone"),
    (True, "standalone"),
    (True, "with_plan"),
    (True, "with_function"),
])
# fmt: on
def test_zmq_api_function_execute_2(re_manager, run_in_background, option):  # noqa: F811
    """
    ``function_execute`` 0MQ API: test if the executed functions can successfully manipulate
    global objects in the namespace (use test functions from simulated 'profile collection').
    Perform the test while a plan or a function is running in foreground.
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    if option == "with_plan":
        resp2a, _ = zmq_single_request(
            "queue_item_add", {"item": _plan3, "user": _user, "user_group": _user_group}
        )
        assert resp2a["success"] is True
        resp2b, _ = zmq_single_request("queue_start")
        assert resp2b["success"] is True
        ttime.sleep(1)
    elif option == "with_function":
        # Start a function in foreground
        fg_func_info = {"name": "function_sleep", "args": [4], "item_type": "function"}
        fg_params = {"user": _user, "user_group": _test_user_group, "run_in_background": False}
        resp2c, _ = zmq_single_request("function_execute", params={"item": fg_func_info, **fg_params})
        assert resp2c["success"] is True, pprint.pformat(resp2c)
        ttime.sleep(1)
    elif option == "standalone":
        pass
    else:
        assert False, f"Unknown option: {option}"

    # Push value to FIFO buffer
    value = {"some_key": "some_value"}
    func_info1 = {"name": "push_buffer_element", "args": [value], "item_type": "function"}
    params = {"user": _user, "user_group": _test_user_group, "run_in_background": run_in_background}

    resp3, _ = zmq_single_request("function_execute", params={"item": func_info1, **params})
    assert resp3["success"] is True, pprint.pformat(resp3)
    task_uid = resp3["task_uid"]

    result = wait_for_task_result(10, task_uid)
    assert result["success"] is True, pprint.pformat(result)

    # Pop value from FIFO buffer
    func_info2 = {"name": "pop_buffer_element", "item_type": "function"}

    resp4, _ = zmq_single_request("function_execute", params={"item": func_info2, **params})
    assert resp4["success"] is True, pprint.pformat(resp4)
    task_uid = resp4["task_uid"]

    result = wait_for_task_result(10, task_uid)
    assert result["success"] is True, pprint.pformat(result)
    assert result["return_value"] == value, pprint.pformat(result)

    # Now wait for the function or the plan to complete
    if option in ("with_plan", "with_function"):
        assert wait_for_condition(time=20, condition=condition_manager_idle)

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("start_fg_func", [False, True])
# fmt: on
def test_zmq_api_function_execute_3(re_manager, start_fg_func):  # noqa: F811
    """
    ``function_execute`` 0MQ API: test if multiple background functions can be
    started at once and run in parallel.
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    if start_fg_func:
        fg_func_info = {"name": "function_sleep", "args": [5], "item_type": "function"}
        fg_params = {"user": _user, "user_group": _test_user_group, "run_in_background": False}
        resp2, _ = zmq_single_request("function_execute", params={"item": fg_func_info, **fg_params})
        assert resp2["success"] is True, pprint.pformat(resp2)
        ttime.sleep(1)

    bg_func_info = {"name": "function_sleep", "args": [2], "item_type": "function"}
    bg_params = {"user": _user, "user_group": _test_user_group, "run_in_background": True}

    # Start 5 background functions (identical)
    task_uids = []
    for _ in range(5):
        resp3, _ = zmq_single_request("function_execute", params={"item": bg_func_info, **bg_params})
        assert resp3["success"] is True, pprint.pformat(resp3)
        task_uids.append(resp3["task_uid"])

    ttime.sleep(1)
    status, _ = zmq_single_request("status")
    assert status["worker_background_tasks"] == 5

    for task_uid in task_uids:
        result = wait_for_task_result(10, task_uid)
        assert result["success"] is True, pprint.pformat(result)

    status, _ = zmq_single_request("status")
    assert status["worker_background_tasks"] == 0

    if start_fg_func:
        assert wait_for_condition(time=20, condition=condition_manager_idle)

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


_script_func_1 = """
def push_two_elements(element1, element2):
    _fifo_buffer.append(element1)
    _fifo_buffer.append(element2)
"""


def test_zmq_api_function_execute_4(re_manager):  # noqa: F811
    """
    ``function_execute`` 0MQ API: test if a function could be uploaded as part
    of the script and used to manipulate global objects in the namespace.
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp2, _ = zmq_single_request("script_upload", params={"script": _script_func_1})
    assert resp2["success"] is True
    result = wait_for_task_result(time=10, task_uid=resp2["task_uid"])
    assert result["success"] is True, pprint.pformat(result)

    elements = [[10, 20, 30], {"element_name": "element2"}]
    kwargs = {"element1": elements[0], "element2": elements[1]}
    func_info = {"name": "push_two_elements", "kwargs": kwargs, "item_type": "function"}
    params = {"user": _user, "user_group": _test_user_group, "run_in_background": True}
    resp3, _ = zmq_single_request("function_execute", params={"item": func_info, **params})
    assert resp3["success"] is True, pprint.pformat(resp3)
    result = wait_for_task_result(10, resp3["task_uid"])
    assert result["success"] is True, pprint.pformat(result)

    for el in elements:
        func_info = {"name": "pop_buffer_element", "item_type": "function"}
        params = {"user": _user, "user_group": _test_user_group, "run_in_background": True}
        resp4, _ = zmq_single_request("function_execute", params={"item": func_info, **params})
        assert resp4["success"] is True, pprint.pformat(resp4)
        result = wait_for_task_result(10, resp4["task_uid"])
        assert result["success"] is True, pprint.pformat(result)
        assert result["return_value"] == el

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


_script_func_2 = """
def func_elements(val):
    return val
"""

_script_func_3 = """
import numpy as np

def func_elements():
    return np.array([1,2,3])
"""

_script_func_4 = """
def func_elements():
    raise Exception(f"-- Exception was raised!!! --")
"""


# fmt: off
@pytest.mark.parametrize("script, args, return_value, success_snd, success_rcv, msg", [
    (_script_func_2, [10], 10, True, True, ""),
    (_script_func_2, ["test"], "test", True, True, ""),
    (_script_func_2, [[1, 2, 3]], [1, 2, 3], True, True, ""),
    (_script_func_2, [(1, 2, 3)], [1, 2, 3], True, True, ""),
    (_script_func_2, [{"key": 1.0}], {"key": 1.0}, True, True, ""),
    (_script_func_2, [None], None, True, True, ""),
    (_script_func_2, [np.array([1, 2, 3])], None, False, None, "Object of type ndarray is not JSON serializable"),
    (_script_func_3, [], None, True, False, "Object of type ndarray is not JSON serializable"),
    (_script_func_4, [], None, True, False, "-- Exception was raised!!! --"),
])
# fmt: on
def test_zmq_api_function_execute_5(
    re_manager, script, args, return_value, success_snd, success_rcv, msg  # noqa: F811
):
    """
    ``function_execute`` 0MQ API: test if data of different types could be passed to and
    from a function. Check that error is reported if function parameter or return value is not serializable.
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp2, _ = zmq_single_request("script_upload", params={"script": script})
    assert resp2["success"] is True

    result = wait_for_task_result(time=10, task_uid=resp2["task_uid"])
    assert result["success"] is True, pprint.pformat(result)

    func_info = {"name": "func_elements", "args": args, "item_type": "function"}
    params = {"user": _user, "user_group": _test_user_group, "run_in_background": True}
    resp4, msg_err = zmq_single_request("function_execute", params={"item": func_info, **params})
    if not success_snd:
        # Communication error due to unserializable parameter type (not JSON serializable)
        assert msg in msg_err
    else:
        assert resp4["success"] is True, pprint.pformat(resp4)
        result = wait_for_task_result(10, resp4["task_uid"])
        assert result["success"] == success_rcv, pprint.pformat(result)
        if success_rcv:
            assert result["return_value"] == return_value
        else:
            assert msg in result["return_value"]

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_zmq_api_function_execute_6(re_manager):  # noqa: F811
    """
    ``function_execute`` 0MQ API: test if 'item_uid' in function item is ignored (new 'item_uid'
    is generated and this new 'item_uid' becomes 'task_uid').
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    func_info = {"name": "clear_buffer", "item_type": "function"}

    # Add 'item_uid' to 'func_info'
    item_uid = "abc"
    func_info.update({"item_uid": "abc"})

    params = {"user": _user, "user_group": _test_user_group, "run_in_background": True}
    resp4, _ = zmq_single_request("function_execute", params={"item": func_info, **params})
    assert resp4["success"] is True
    task_uid = resp4["task_uid"]
    assert task_uid != item_uid
    assert resp4["item"]["item_uid"] != item_uid
    assert resp4["item"]["item_uid"] == task_uid

    result = wait_for_task_result(10, task_uid)
    assert result["success"] is True, pprint.pformat(result)

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("option, item_type, msg", [
    ("missing_user", "function", "user name is not specified"),
    ("missing_group", "function", "user group is not specified"),
    ("unknown_group", "function", "Unknown user group: 'unknown_group'"),
    ("missing_name", "function", "Function name is not specified"),
    ("not_allowed_name", "function", "Function 'not_allowed' is not allowed"),
    ("args_not_list", "function", "Parameter 'args' is not a tuple or a list"),
    ("kwargs_not_dict", "function", "Parameter 'kwargs' is not a dictionary"),
    ("unsupported_type", "plan", "unsupported 'item_type' value: 'plan'"),
    ("unsupported_type", "instruction", "unsupported 'item_type' value: 'instruction'"),
    ("unsupported_type", "unsupported", "unsupported 'item_type' value: 'unsupported'"),
])
# fmt: on
def test_zmq_api_function_execute_7_fail(re_manager, option, item_type, msg):  # noqa: F811
    """
    ``function_execute`` 0MQ API: failing cases.
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    func_info = {"name": "function_sleep", "args": [2], "item_type": item_type}
    params = {"user": _user, "user_group": _test_user_group, "run_in_background": True}
    if option == "missing_user":
        del params["user"]
    elif option == "missing_group":
        del params["user_group"]
    elif option == "unknown_group":
        params["user_group"] = "unknown_group"
    elif option == "missing_name":
        del func_info["name"]
    elif option == "not_allowed_name":
        func_info["name"] = "not_allowed"
    elif option == "args_not_list":
        func_info["args"] = 50
    elif option == "kwargs_not_dict":
        func_info["kwargs"] = 50
    elif option == "unsupported_type":
        pass
    else:
        assert False, f"Unknown option: {option!r}"

    resp4, _ = zmq_single_request("function_execute", params={"item": func_info, **params})
    assert resp4["success"] is False
    assert msg in resp4["msg"]

    status, _ = zmq_single_request("status")
    assert status["manager_state"] == "idle"

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_zmq_api_function_execute_8_fail(re_manager):  # noqa: F811
    """
    ``function_execute`` 0MQ API: execute a function that does not exist in the worker
    namespace: 'function_execute' succeeds, but the returned task result is expected
    to contain error message.
    """
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # The function name is in the list of permitted functions
    non_existing_function_name = "non_existing_element"
    func_info = {"name": non_existing_function_name, "item_type": "function"}
    params = {"user": _user, "user_group": _test_user_group, "run_in_background": True}

    resp4, _ = zmq_single_request("function_execute", params={"item": func_info, **params})
    # The call still succeeds, but function execution fails
    assert resp4["success"] is True

    task_uid = resp4["task_uid"]
    result = wait_for_task_result(10, task_uid)
    assert result["success"] is False, pprint.pformat(result)
    msg = "Function 'non_existing_element' is not found in the worker namespace"
    assert msg in result["msg"]
    assert "not found in the worker namespace" in result["return_value"]

    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


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
    assert isinstance(resp1["plans_allowed_uid"], str)
    resp2, _ = zmq_single_request("devices_allowed", params)
    assert resp2["success"] is True
    assert resp2["msg"] == ""
    assert isinstance(resp2["devices_allowed"], dict)
    assert len(resp2["devices_allowed"]) > 0
    assert isinstance(resp2["devices_allowed_uid"], str)


def test_zmq_api_plans_allowed_and_devices_allowed_2(re_manager):  # noqa F811
    """
    Test that group names are recognized correctly. The number of returned plans and
    devices is compared to the number of plans and devices loaded from the profile
    collection. The functions for loading files and generating lists are tested
    separately somewhere else.
    """

    pc_path = get_default_startup_dir()
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
    assert resp1["plans_allowed_uid"] is None
    resp2, _ = zmq_single_request("devices_allowed", params)
    assert resp1["success"] is False
    assert message in resp1["msg"]
    assert isinstance(resp2["devices_allowed"], dict)
    assert len(resp2["devices_allowed"]) == 0
    assert resp2["devices_allowed_uid"] is None


# =======================================================================================
#                      Method 'plans_existing', 'devices_existing'


def test_zmq_api_plans_existing_and_devices_existing_1(re_manager):  # noqa F811
    """
    Basic calls to 'plans_existing', 'devices_existing' methods.
    """
    resp1, _ = zmq_single_request("plans_existing")
    assert resp1["success"] is True
    assert resp1["msg"] == ""
    assert isinstance(resp1["plans_existing"], dict)
    assert len(resp1["plans_existing"]) > 0
    assert isinstance(resp1["plans_existing_uid"], str)
    resp2, _ = zmq_single_request("devices_existing")
    assert resp2["success"] is True
    assert resp2["msg"] == ""
    assert isinstance(resp2["devices_existing"], dict)
    assert len(resp2["devices_existing"]) > 0
    assert isinstance(resp2["devices_existing_uid"], str)


def test_zmq_api_plans_existing_and_devices_existing_2_fail(re_manager):  # noqa F811
    """
    Test that 'plans_existing', 'devices_existing' methods fail if extra parameters are passed.
    """
    params = {"user_group": _user_group}  # 'user_group' is not supported by the methods

    resp1, _ = zmq_single_request("plans_existing", params=params)
    assert resp1["success"] is False
    assert "API request contains unsupported parameters: 'user_group'." in resp1["msg"]
    assert resp1["plans_existing"] == {}
    assert resp1["plans_existing_uid"] is None

    resp2, _ = zmq_single_request("devices_existing", params=params)
    assert resp2["success"] is False
    assert "API request contains unsupported parameters: 'user_group'." in resp2["msg"]
    assert resp2["devices_existing"] == {}
    assert resp2["devices_existing_uid"] is None


# =======================================================================================
#                      Method 'queue_item_get', 'queue_item_remove'


def test_zmq_api_queue_item_get_remove_1(re_manager):  # noqa F811
    """
    Get and remove a plan from the back of the queue
    """
    plans = [_plan1, _plan2, _plan3]
    for plan in plans:
        resp0, _ = zmq_single_request("queue_item_add", {"item": plan, "user": _user, "user_group": _user_group})
        assert resp0["success"] is True

    status0 = get_queue_state()

    resp1, _ = zmq_single_request("queue_get")
    assert resp1["items"] != []
    assert len(resp1["items"]) == 3
    assert resp1["running_item"] == {}
    assert resp1["plan_queue_uid"] == status0["plan_queue_uid"]

    # Get the last plan from the queue
    resp2, _ = zmq_single_request("queue_item_get")
    assert resp2["success"] is True
    assert resp2["item"]["name"] == _plan3["name"]
    assert resp2["item"]["args"] == _plan3["args"]
    assert resp2["item"]["kwargs"] == _plan3["kwargs"]
    assert "item_uid" in resp2["item"]

    # Remove the last plan from the queue
    resp3, _ = zmq_single_request("queue_item_remove")
    assert resp3["success"] is True
    assert resp3["qsize"] == 2
    assert resp3["item"]["name"] == "count"
    assert resp3["item"]["args"] == [["det1", "det2"]]
    assert resp2["item"]["kwargs"] == _plan3["kwargs"]
    assert "item_uid" in resp3["item"]

    status1 = get_queue_state()
    assert status1["plan_queue_uid"] != status0["plan_queue_uid"]


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
def test_zmq_api_queue_item_get_remove_2(re_manager, pos, pos_result, success):  # noqa F811
    """
    Get and remove elements using element position in the queue.
    """

    plans = [
        {"item_uid": "one", "name": "count", "args": [["det1"]], "item_type": "plan"},
        {"item_uid": "two", "name": "count", "args": [["det2"]], "item_type": "plan"},
        {"item_uid": "three", "name": "count", "args": [["det1", "det2"]], "item_type": "plan"},
    ]
    for plan in plans:
        resp0, _ = zmq_single_request("queue_item_add", {"item": plan, "user": _user, "user_group": _user_group})
        assert resp0["success"] is True

    # Remove entry at the specified position
    params = {} if pos is None else {"pos": pos}

    # Testing 'queue_item_get'
    resp1, _ = zmq_single_request("queue_item_get", params)
    assert resp1["success"] is success
    if success:
        assert resp1["item"]["args"] == plans[pos_result]["args"]
        assert "item_uid" in resp1["item"]
        assert resp1["msg"] == ""
    else:
        assert resp1["item"] == {}
        assert "Failed to get an item" in resp1["msg"]

    # Testing 'queue_item_remove'
    resp2, _ = zmq_single_request("queue_item_remove", params)
    assert resp2["success"] is success
    assert resp2["qsize"] == (2 if success else None)
    if success:
        assert resp2["item"]["args"] == plans[pos_result]["args"]
        assert "item_uid" in resp2["item"]
        assert resp2["msg"] == ""
    else:
        assert resp2["item"] == {}
        assert "Failed to remove an item" in resp2["msg"]

    resp3, _ = zmq_single_request("queue_get")
    assert len(resp3["items"]) == (2 if success else 3)
    assert resp3["running_item"] == {}


def test_zmq_api_queue_item_get_remove_3(re_manager):  # noqa F811
    """
    Get and remove elements using plan UID. Successful and failing cases.
    """
    plans = [_plan3, _plan2, _plan1]
    for plan in plans:
        resp0, _ = zmq_single_request("queue_item_add", {"item": plan, "user": _user, "user_group": _user_group})
        assert resp0["success"] is True

    resp1, _ = zmq_single_request("queue_get")
    plans_in_queue = resp1["items"]
    assert len(plans_in_queue) == 3

    # Get and then remove plan 2 from the queue
    uid = plans_in_queue[1]["item_uid"]
    resp2a, _ = zmq_single_request("queue_item_get", {"uid": uid})
    assert resp2a["item"]["item_uid"] == plans_in_queue[1]["item_uid"]
    assert resp2a["item"]["name"] == plans_in_queue[1]["name"]
    assert resp2a["item"]["args"] == plans_in_queue[1]["args"]
    resp2b, _ = zmq_single_request("queue_item_remove", {"uid": uid})
    assert resp2b["item"]["item_uid"] == plans_in_queue[1]["item_uid"]
    assert resp2b["item"]["name"] == plans_in_queue[1]["name"]
    assert resp2b["item"]["args"] == plans_in_queue[1]["args"]

    # Start the first plan (this removes it from the queue)
    #   Also the rest of the operations will be performed on a running queue.
    resp3, _ = zmq_single_request("environment_open")
    assert resp3["success"] is True
    assert wait_for_condition(
        time=timeout_env_open, condition=condition_environment_created
    ), "Timeout while waiting for environment to be opened"

    resp4, _ = zmq_single_request("queue_start")
    assert resp4["success"] is True

    ttime.sleep(1)
    uid = plans_in_queue[0]["item_uid"]
    resp5a, _ = zmq_single_request("queue_item_get", {"uid": uid})
    assert resp5a["success"] is False
    assert "is currently running" in resp5a["msg"]
    resp5b, _ = zmq_single_request("queue_item_remove", {"uid": uid})
    assert resp5b["success"] is False
    assert "Can not remove an item which is currently running" in resp5b["msg"]

    uid = "nonexistent"
    resp6a, _ = zmq_single_request("queue_item_get", {"uid": uid})
    assert resp6a["success"] is False
    assert "not in the queue" in resp6a["msg"]
    resp6b, _ = zmq_single_request("queue_item_remove", {"uid": uid})
    assert resp6b["success"] is False
    assert "not in the queue" in resp6b["msg"]

    # Remove the last entry
    uid = plans_in_queue[2]["item_uid"]
    resp7a, _ = zmq_single_request("queue_item_get", {"uid": uid})
    assert resp7a["success"] is True
    resp7b, _ = zmq_single_request("queue_item_remove", {"uid": uid})
    assert resp7b["success"] is True

    assert wait_for_condition(
        time=10, condition=condition_queue_processing_finished
    ), "Timeout while waiting for environment to be opened"

    state = get_queue_state()
    assert state["items_in_queue"] == 0
    assert state["items_in_history"] == 1

    # Close the environment
    resp8, _ = zmq_single_request("environment_close")
    assert resp8["success"] is True
    assert wait_for_condition(time=5, condition=condition_environment_closed)


def test_zmq_api_queue_item_get_remove_4_failing(re_manager):  # noqa F811
    """
    Failing cases that are not tested in other places.
    """
    # Ambiguous parameters
    resp1, _ = zmq_single_request("queue_item_get", {"pos": 5, "uid": "some_uid"})
    assert resp1["success"] is False
    assert "Ambiguous parameters" in resp1["msg"]


# fmt: off
@pytest.mark.parametrize("batch_params, queue_seq, selection_seq, batch_seq, expected_seq, success, msg", [
    ({}, "0123456", "", "", "0123456", True, ""),
    ({}, "0123456", "23", "23", "01456", True, ""),
    ({}, "0123456", "32", "32", "01456", True, ""),
    ({}, "0123456", "06", "06", "12345", True, ""),
    ({}, "0123456", "283", "23", "01456", True, ""),
    ({}, "0123456", "2893", "23", "01456", True, ""),
    ({}, "0123456", "2443", "243", "0156", True, ""),
    ({"ignore_missing": True}, "0123456", "2443", "243", "0156", True, ""),
    ({"ignore_missing": True}, "0123456", "283", "23", "01456", True, ""),
    ({"ignore_missing": False}, "0123456", "2443", "", "0123456", False, "The list of contains repeated UIDs"),
    ({"ignore_missing": False}, "0123456", "283", "", "0123456", False,
     "The queue does not contain items with the following UIDs"),
    ({"ignore_missing": False}, "0123456", "2883", "", "0123456", False, "The list of contains repeated UIDs"),
    ({}, "0123456", "", "", "0123456", True, ""),
    ({}, "", "", "", "", True, ""),
    ({}, "", "23", "", "", True, ""),
    ({"ignore_missing": False}, "", "", "", "", True, ""),
    ({"ignore_missing": False}, "", "23", "", "", False,
     "The queue does not contain items with the following UIDs"),
])
# fmt: on
def test_zmq_api_item_remove_batch_1(
    re_manager, batch_params, queue_seq, selection_seq, batch_seq, expected_seq, success, msg  # noqa: F811
):
    """
    Tests for ``queue_item_remove_batch`` API.
    """
    plan_template = {
        "name": "count",
        "args": [["det1"]],
        "kwargs": {"num": 50, "delay": 0.01},
        "item_type": "plan",
    }

    # Fill the queue with the initial set of plans
    for item_code in queue_seq:
        item = copy.deepcopy(plan_template)
        item["kwargs"]["num"] = int(item_code)
        params = {"item": item, "user": _user, "user_group": _user_group}
        resp1a, _ = zmq_single_request("queue_item_add", params)
        assert resp1a["success"] is True

    state = get_queue_state()
    assert state["items_in_queue"] == len(queue_seq)
    assert state["items_in_history"] == 0

    resp1b, _ = zmq_single_request("queue_get")
    assert resp1b["success"] is True
    queue_initial = resp1b["items"]

    def find_uid(dummy_uid):
        """If item is not found, then return ``dummy_uid``"""
        try:
            ind = queue_seq.index(dummy_uid)
            return queue_initial[ind]["item_uid"]
        except Exception:
            return dummy_uid

    # Create a list of UIDs of items to be moved
    uids_of_items_to_remove = []
    for item_code in selection_seq:
        uids_of_items_to_remove.append(find_uid(item_code))

    # Move the batch
    params = {"uids": uids_of_items_to_remove}
    params.update(batch_params)
    resp2a, _ = zmq_single_request("queue_item_remove_batch", params)

    if success:
        assert resp2a["success"] is True, pprint.pformat(resp2a)
        assert resp2a["msg"] == ""
        assert resp2a["qsize"] == len(expected_seq)
        items_moved = resp2a["items"]
        assert len(items_moved) == len(batch_seq)
        added_seq = [str(_["kwargs"]["num"]) for _ in items_moved]
        added_seq = "".join(added_seq)
        assert added_seq == batch_seq
    else:
        assert resp2a["success"] is False, pprint.pformat(resp2a)
        assert re.search(msg, resp2a["msg"]), pprint.pformat(resp2a)
        assert resp2a["qsize"] is None
        assert resp2a["items"] == []

    resp2b, _ = zmq_single_request("queue_get")
    assert resp2b["success"] is True
    queue_final = resp2b["items"]
    queue_final_seq = [str(_["kwargs"]["num"]) for _ in queue_final]
    queue_final_seq = "".join(queue_final_seq)
    assert queue_final_seq == expected_seq

    state = get_queue_state()
    assert state["items_in_queue"] == len(expected_seq)
    assert state["items_in_history"] == 0


def test_zmq_api_item_remove_batch_2_fail(re_manager):  # noqa: F811
    """
    Test for ``queue_item_remove_batch`` API
    """
    resp1, _ = zmq_single_request("queue_item_remove_batch", params={})
    assert resp1["success"] is False
    assert "Request does not contain the list of UIDs" in resp1["msg"]
    assert resp1["qsize"] is None
    assert resp1["items"] == []


# =======================================================================================
#                              Method `queue_item_move`

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
def test_zmq_api_item_move_1(re_manager, params, src, order, success, msg):  # noqa: F811
    plans = [_plan1, _plan2, _plan3]
    for plan in plans:
        resp0, _ = zmq_single_request("queue_item_add", {"item": plan, "user": _user, "user_group": _user_group})
        assert resp0["success"] is True

    resp1, _ = zmq_single_request("queue_get")
    queue = resp1["items"]
    pq_uid = resp1["plan_queue_uid"]
    assert len(queue) == 3

    item_uids = [_["item_uid"] for _ in queue]
    # Add one more 'nonexistent' uid (that is not in the queue)
    item_uids.append("nonexistent")

    # Replace indices with actual UIDs that will be sent to the function
    if "uid" in params:
        params["uid"] = item_uids[params["uid"]]
    if "before_uid" in params:
        params["before_uid"] = item_uids[params["before_uid"]]
    if "after_uid" in params:
        params["after_uid"] = item_uids[params["after_uid"]]

    resp2, _ = zmq_single_request("queue_item_move", params)
    if success:
        assert resp2["success"] is True
        assert resp2["item"] == queue[src]
        assert resp2["qsize"] == len(plans)
        assert resp2["msg"] == ""

        # Compare the order of UIDs in the queue with the expected order
        item_uids_reordered = [item_uids[_] for _ in order]
        resp3, _ = zmq_single_request("queue_get")
        item_uids_from_queue = [_["item_uid"] for _ in resp3["items"]]

        assert item_uids_from_queue == item_uids_reordered

        status = get_queue_state()
        if order != [0, 1, 2]:
            # The queue actually changed, so UID is expected to change
            assert status["plan_queue_uid"] != pq_uid
        else:
            # The queue did not change, so UID is expected to remain the same
            assert status["plan_queue_uid"] == pq_uid

    else:
        assert resp2["success"] is False
        assert resp2["qsize"] is None
        assert resp2["item"] == {}
        assert msg in resp2["msg"]

        status = get_queue_state()
        # Queue did not change, so UID should remain the same
        assert status["plan_queue_uid"] == pq_uid


# fmt: off
@pytest.mark.parametrize("batch_params, queue_seq, selection_seq, batch_seq, expected_seq, success, msg", [
    ({"pos_dest": "front"}, "0123456", "23", "23", "2301456", True, ""),
    ({"before_uid": "0"}, "0123456", "23", "23", "2301456", True, ""),
    ({"pos_dest": "back"}, "0123456", "23", "23", "0145623", True, ""),
    ({"after_uid": "6"}, "0123456", "23", "23", "0145623", True, ""),
    ({"before_uid": "5"}, "0123456", "23", "23", "0142356", True, ""),
    ({"after_uid": "5"}, "0123456", "23", "23", "0145236", True, ""),
    # Controlling the order of moved items
    ({"after_uid": "5"}, "0123456", "023", "023", "1450236", True, ""),
    ({"after_uid": "5"}, "0123456", "203", "203", "1452036", True, ""),
    ({"after_uid": "5"}, "0123456", "302", "302", "1453026", True, ""),
    ({"after_uid": "5", "reorder": False}, "0123456", "302", "302", "1453026", True, ""),
    ({"after_uid": "5", "reorder": True}, "0123456", "023", "023", "1450236", True, ""),
    ({"after_uid": "5", "reorder": True}, "0123456", "203", "023", "1450236", True, ""),
    ({"after_uid": "5", "reorder": True}, "0123456", "302", "023", "1450236", True, ""),
    # Empty list of UIDS
    ({"pos_dest": "front"}, "0123456", "", "", "0123456", True, ""),
    ({"pos_dest": "front"}, "", "", "", "", True, ""),
    # Move the batch which is already in the front or back to front or back of the queue
    #   (nothing is done, but operation is still successful)
    ({"pos_dest": "front"}, "0123456", "01", "01", "0123456", True, ""),
    ({"pos_dest": "back"}, "0123456", "56", "56", "0123456", True, ""),
    # Same, but change the order of moved items
    ({"pos_dest": "front"}, "0123456", "10", "10", "1023456", True, ""),
    ({"pos_dest": "back"}, "0123456", "65", "65", "0123465", True, ""),
    # Failing cases
    ({}, "0123456", "23", "23", "0123456", False, "Destination for the batch is not specified"),
    ({"pos_dest": "front", "before_uid": "5"}, "0123456", "23", "23", "0123456", False,
     "more than one mutually exclusive parameter"),
    ({"after_uid": "3"}, "0123456", "023", "023", "0123456", False, "item with UID '.*' is in the batch"),
    ({"before_uid": "3"}, "0123456", "023", "023", "0123456", False, "item with UID '.*' is in the batch"),
    ({"after_uid": "5"}, "0123456", "093", "093", "0123456", False,
     re.escape("The queue does not contain items with the following UIDs: ['9']")),
    ({"after_uid": "5"}, "0123456", "07893", "07893", "0123456", False,
     re.escape("The queue does not contain items with the following UIDs: ['7', '8', '9']")),
    ({"after_uid": "5"}, "0123456", "0223", "0223", "0123456", False,
     re.escape("The list of contains repeated UIDs (1 UIDs)")),
])
# fmt: on
def test_zmq_api_item_move_batch_1(
    re_manager, batch_params, queue_seq, selection_seq, batch_seq, expected_seq, success, msg  # noqa: F811
):
    """
    Tests for ``queue_item_move_batch`` API.
    """
    plan_template = {
        "name": "count",
        "args": [["det1"]],
        "kwargs": {"num": 50, "delay": 0.01},
        "item_type": "plan",
    }

    # Fill the queue with the initial set of plans
    for item_code in queue_seq:
        item = copy.deepcopy(plan_template)
        item["kwargs"]["num"] = int(item_code)
        params = {"item": item, "user": _user, "user_group": _user_group}
        resp1a, _ = zmq_single_request("queue_item_add", params)
        assert resp1a["success"] is True

    state = get_queue_state()
    assert state["items_in_queue"] == len(queue_seq)
    assert state["items_in_history"] == 0

    resp1b, _ = zmq_single_request("queue_get")
    assert resp1b["success"] is True
    queue_initial = resp1b["items"]

    # If there are 'before_uid' or 'after_uid' parameters, then convert values of those
    #   parameters to actual item UIDs.
    def find_uid(dummy_uid):
        """If item is not found, then return ``dummy_uid``"""
        try:
            ind = queue_seq.index(dummy_uid)
            return queue_initial[ind]["item_uid"]
        except Exception:
            return dummy_uid

    if "before_uid" in batch_params:
        batch_params["before_uid"] = find_uid(batch_params["before_uid"])

    if "after_uid" in batch_params:
        batch_params["after_uid"] = find_uid(batch_params["after_uid"])

    # Create a list of UIDs of items to be moved
    uids_of_items_to_move = []
    for item_code in selection_seq:
        uids_of_items_to_move.append(find_uid(item_code))

    # Move the batch
    params = {"uids": uids_of_items_to_move}
    params.update(batch_params)
    resp2a, _ = zmq_single_request("queue_item_move_batch", params)

    if success:
        assert resp2a["success"] is True, pprint.pformat(resp2a)
        assert resp2a["msg"] == ""
        assert resp2a["qsize"] == len(expected_seq)
        items_moved = resp2a["items"]
        assert len(items_moved) == len(batch_seq)
        added_seq = [str(_["kwargs"]["num"]) for _ in items_moved]
        added_seq = "".join(added_seq)
        assert added_seq == batch_seq
    else:
        assert resp2a["success"] is False, pprint.pformat(resp2a)
        assert re.search(msg, resp2a["msg"]), pprint.pformat(resp2a)
        assert resp2a["qsize"] is None
        assert resp2a["items"] == []

    resp2b, _ = zmq_single_request("queue_get")
    assert resp2b["success"] is True
    queue_final = resp2b["items"]
    queue_final_seq = [str(_["kwargs"]["num"]) for _ in queue_final]
    queue_final_seq = "".join(queue_final_seq)
    assert queue_final_seq == expected_seq

    state = get_queue_state()
    assert state["items_in_queue"] == len(expected_seq)
    assert state["items_in_history"] == 0


def test_zmq_api_item_move_batch_2_fail(re_manager):  # noqa: F811
    """
    Test for ``queue_item_move_batch`` API
    """
    resp1, _ = zmq_single_request("queue_item_move_batch", params={})
    assert resp1["success"] is False
    assert "Request does not contain the list of UIDs" in resp1["msg"]
    assert resp1["qsize"] is None
    assert resp1["items"] == []


def test_zmq_api_queue_mode_set_1(re_manager):  # noqa: F811
    """
    Basic tests for ``queue_mode_set`` API
    """
    status = get_queue_state()
    queue_mode_default = status["plan_queue_mode"]

    # Send empty dictionary, this should not change the mode
    resp1, _ = zmq_single_request("queue_mode_set", params={"mode": {}})
    assert resp1["success"] is True, str(resp1)
    assert resp1["msg"] == ""
    status = get_queue_state()
    assert status["plan_queue_mode"] == queue_mode_default

    # Meaningful change: enable the LOOP mode
    resp2, _ = zmq_single_request("queue_mode_set", params={"mode": {"loop": True}})
    assert resp2["success"] is True, str(resp2)
    status = get_queue_state()
    assert status["plan_queue_mode"] != queue_mode_default
    queue_mode_expected = queue_mode_default.copy()
    queue_mode_expected["loop"] = True
    assert status["plan_queue_mode"] == queue_mode_expected

    # Reset to default
    resp3, _ = zmq_single_request("queue_mode_set", params={"mode": "default"})
    assert resp3["success"] is True, str(resp3)
    status = get_queue_state()
    assert status["plan_queue_mode"] == queue_mode_default


# fmt: off
@pytest.mark.parametrize("mode, msg_expected", [
    ("unknown_str", "Queue mode is passed using object of unsupported type '<class 'str'>'"),
    (["a", "b"], "Queue mode is passed using object of unsupported type '<class 'list'>'"),
    ({"unsupported_key": 10}, "Unsupported plan queue mode parameter 'unsupported_key'"),
    ({"loop": 10}, "Unsupported type '<class 'int'>' of the parameter 'loop'"),
])
# fmt: on
def test_zmq_api_queue_mode_set_2_fail(re_manager, mode, msg_expected):  # noqa: F811
    """
    Failing cases for ``queue_mode_set`` API
    """
    status = get_queue_state()
    queue_mode_default = status["plan_queue_mode"]

    resp, _ = zmq_single_request("queue_mode_set", params={"mode": mode})
    assert resp["success"] is False
    assert msg_expected in resp["msg"]

    status = get_queue_state()
    assert status["plan_queue_mode"] == queue_mode_default


def test_zmq_api_queue_mode_set_3_loop_mode(re_manager):  # noqa: F811
    """
    More sophisticated test for ``queue_mode_set`` API. Run the queue with enabled and
    disabled loop mode.
    """

    items = (_plan1, _plan2, _instruction_stop)
    for item in items:
        resp, _ = zmq_single_request(
            "queue_item_add", params={"item": item, "user": _user, "user_group": _user_group}
        )
        assert resp["success"] is True

    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # Continuously test execution of the queue in both modes. The queue contains 2 plans and
    #   'stop' instruction. In the loop mode the queue stops after the instructions, but
    #   all the items remain in the queue. After execution of the queue with disable loop
    #   mode the queue is empty.
    for loop_mode in (True, False):
        resp2, _ = zmq_single_request("queue_mode_set", params={"mode": {"loop": loop_mode}})
        assert resp2["success"] is True

        status = get_queue_state()
        assert status["items_in_queue"] == 3, f"loop_mode={loop_mode}"
        assert status["items_in_history"] == (0 if loop_mode else 4), f"loop_mode={loop_mode}"

        resp3, _ = zmq_single_request("queue_start")
        assert resp3["success"] is True

        assert wait_for_condition(time=10, condition=condition_manager_idle)

        status = get_queue_state()
        assert status["items_in_queue"] == (3 if loop_mode else 0), f"loop_mode={loop_mode}"
        assert status["items_in_history"] == (2 if loop_mode else 6), f"loop_mode={loop_mode}"

        resp3, _ = zmq_single_request("queue_start")
        assert resp3["success"] is True

        assert wait_for_condition(time=10, condition=condition_manager_idle)

        status = get_queue_state()
        assert status["items_in_queue"] == (3 if loop_mode else 0), f"loop_mode={loop_mode}"
        assert status["items_in_history"] == (4 if loop_mode else 6), f"loop_mode={loop_mode}"

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# =======================================================================================
#                              Method 'permissions_reload'

# fmt: off
@pytest.mark.parametrize("reload_plans_devices", [False, True])
# fmt: on
def test_permissions_reload_1(re_manager_pc_copy, tmp_path, reload_plans_devices):  # noqa: F811
    """
    Comprehensive test for ``permission_reload`` API: create a copy of startup files,
    generate the lists of existing plans and devices, start RE Manager (loads the list
    of existing plans and devices from disk), add a device and a plan to startup scripts,
    generate the new lists of existing plans and devices, call ``permissions_reload``,
    check if the new device and the new plan was added to the lists of allowed plans
    and devices (if ``reload_plans_devices`` is ``True``, then the device and the plan
    are expected to be in the list).
    """

    _, pc_path = re_manager_pc_copy
    # Generate the list of existing devices
    gen_list_of_plans_and_devices(
        startup_dir=pc_path, file_dir=pc_path, file_name="existing_plans_and_devices.yaml", overwrite=True
    )

    # Add a plan ('count50') and a device ('det50'). Generate the new disk file
    with open(os.path.join(pc_path, "zz.py"), "w") as f:
        f.writelines(["det50 = det\n", "count50 = count\n"])
    gen_list_of_plans_and_devices(
        startup_dir=pc_path, file_dir=pc_path, file_name="existing_plans_and_devices.yaml", overwrite=True
    )

    # 'allowed_plans_uid' and 'allowed_devices_uid'
    status, _ = zmq_single_request("status")
    plans_allowed_uid = status["plans_allowed_uid"]
    devices_allowed_uid = status["devices_allowed_uid"]
    assert plans_allowed_uid != devices_allowed_uid

    resp1a, _ = zmq_single_request("plans_allowed", params={"user_group": _user_group})
    assert resp1a["success"] is True, f"resp={resp1a}"
    assert resp1a["plans_allowed_uid"] == plans_allowed_uid
    plans_allowed = resp1a["plans_allowed"]

    resp1b, _ = zmq_single_request("devices_allowed", params={"user_group": _user_group})
    assert resp1b["success"] is True, f"resp={resp1b}"
    assert resp1b["devices_allowed_uid"] == devices_allowed_uid
    devices_allowed = resp1b["devices_allowed"]

    assert isinstance(plans_allowed_uid, str)
    assert isinstance(devices_allowed_uid, str)
    assert "count50" not in plans_allowed
    assert "det50" not in devices_allowed

    resp2, _ = zmq_single_request("permissions_reload", {"reload_plans_devices": reload_plans_devices})
    assert resp2["success"] is True, f"resp={resp2}"

    # Check that 'plans_allowed_uid' and 'devices_allowed_uid' changed while permissions were reloaded
    status, _ = zmq_single_request("status")
    plans_allowed_uid2 = status["plans_allowed_uid"]
    devices_allowed_uid2 = status["devices_allowed_uid"]
    assert plans_allowed_uid2 != devices_allowed_uid2

    resp3a, _ = zmq_single_request("plans_allowed", params={"user_group": _user_group})
    assert resp3a["success"] is True, f"resp={resp3a}"
    assert resp3a["plans_allowed_uid"] == plans_allowed_uid2
    plans_allowed2 = resp3a["plans_allowed"]

    resp3b, _ = zmq_single_request("devices_allowed", params={"user_group": _user_group})
    assert resp3b["success"] is True, f"resp={resp3b}"
    assert resp3b["devices_allowed_uid"] == devices_allowed_uid2
    devices_allowed2 = resp3b["devices_allowed"]

    assert isinstance(plans_allowed_uid2, str)
    assert isinstance(devices_allowed_uid2, str)

    assert plans_allowed_uid2 != plans_allowed_uid
    assert devices_allowed_uid2 != devices_allowed_uid

    if reload_plans_devices:
        assert "count50" in plans_allowed2
        assert "det50" in devices_allowed2
    else:
        assert "count50" not in plans_allowed2
        assert "det50" not in devices_allowed2


permissions_allow_count = """
user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - null  # Allow all
    forbidden_devices:
      - null  # Nothing is forbidden
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ":^count$"  # 'count plan'
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - null  # A different way to allow all
    forbidden_devices:
      - null  # Nothing is forbidden
"""


permissions_not_allow_count = """
user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - null  # Allow all
    forbidden_devices:
      - null  # Nothing is forbidden
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - null  # A different way to allow all
    forbidden_plans:
      - ":^count$"  # 'count' plan
    allowed_devices:
      - null  # A different way to allow all
    forbidden_devices:
      - null  # Nothing is forbidden
"""


# fmt: off
@pytest.mark.parametrize("reload_permissions_from_disk", [None, True, False])
# fmt: on
def test_permissions_reload_2(re_manager_pc_copy, reload_permissions_from_disk):  # noqa: F811
    """
    Tests for ``permissions_reload`` API: check if parameter ``reload_permissions`` works correctly.
    """
    _, pc_path = re_manager_pc_copy

    # Now create a new list of user permissions, which may allow/disallow the 'count' plan
    permissions_text = permissions_not_allow_count
    permissions_dict = yaml.load(permissions_text, Loader=yaml.FullLoader)
    with open(os.path.join(pc_path, "user_group_permissions.yaml"), "w") as f:
        f.writelines(permissions_text)

    # Now reload permissions. The new lists of allowed plans and devices must be generated
    params = {}
    if reload_permissions_from_disk in (True, False):
        params["reload_permissions"] = reload_permissions_from_disk

    resp1, _ = zmq_single_request("permissions_reload", params=params)
    assert resp1["success"] is True, f"resp={resp1}"

    resp2, _ = zmq_single_request("permissions_get")
    assert resp2["success"] is True, f"resp={resp2}"
    user_group_permissions = resp2["user_group_permissions"]

    if reload_permissions_from_disk in (None, True):
        assert user_group_permissions == permissions_dict
    else:
        assert user_group_permissions != permissions_dict


# fmt: off
@pytest.mark.parametrize("allow_count_plan", [True, False])
# fmt: on
def test_permissions_reload_3(re_manager_pc_copy, allow_count_plan):  # noqa: F811
    """
    Test if permissions are correctly loaded from disk by the Manager process and propagated to the
    worker process if the environment is open. The test includes the following steps:
    - Start RE Manager with the copy of startup and config files in temporary location (files could be modified).
    - Open RE Worker environment (loads startup files and updates the list of existing plans and devices).
    - Add one ``count`` plan to the queue (should always work).
    - Replace ``user_group_permissions.yaml`` to modify permissions.
    - Test if the plan can still be added to the queue (shows if permissions were correctly loaded by
      the manager process).
    - Try to execute the queue (shows if reloaded permissions correctly propagated to the worker process).
    """
    _, pc_path = re_manager_pc_copy

    resp1a, _ = zmq_single_request("environment_open")
    assert resp1a["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # Add the first 'count' plan
    resp1b, _ = zmq_single_request("queue_item_add", {"item": _plan1, "user": _user, "user_group": _user_group})
    assert resp1b["success"] is True

    # Now create a new list of user permissions, which may allow/disallow the 'count' plan
    up_text = permissions_allow_count if allow_count_plan else permissions_not_allow_count
    with open(os.path.join(pc_path, "user_group_permissions.yaml"), "w") as f:
        f.writelines(up_text)

    # Now reload permissions. The new lists of allowed plans and devices must be generated
    resp2, _ = zmq_single_request("permissions_reload")
    assert resp2["success"] is True, f"resp={resp2}"

    resp3, _ = zmq_single_request("plans_allowed", params={"user_group": _user_group})
    assert resp3["success"] is True, f"resp={resp3}"
    plans_allowed = resp3["plans_allowed"]

    # Attempt to add the second plan
    resp4, _ = zmq_single_request("queue_item_add", {"item": _plan1, "user": _user, "user_group": _user_group})
    status = get_queue_state()

    # The following checks verify that the permissions are properly reloaded by the manager process
    if allow_count_plan:
        assert "count" in plans_allowed
        assert resp4["success"] is True
        assert status["items_in_queue"] == 2
    else:
        assert "count" not in plans_allowed
        assert resp4["success"] is False
        assert status["items_in_queue"] == 1

    # Now verify that permissions are reloaded by the worker process. The way to do it is to is to start
    #   the queue and see if 'count' plans are executed.
    resp5, _ = zmq_single_request("queue_start")
    assert resp5["success"] is True, f"resp={resp5}"

    assert wait_for_condition(time=30, condition=condition_manager_idle)

    status = get_queue_state()

    if allow_count_plan:
        assert status["items_in_queue"] == 0
        assert status["items_in_history"] == 2
    else:
        assert status["items_in_queue"] == 1  # The plan is pushed back in the queue
        assert status["items_in_history"] == 1  # The plan failed, but it is still added to history

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# =======================================================================================
#                Methods 'permissions_set' and 'permissions_get

_permissions_dict_not_allow_count = {
    "user_groups": {
        "root": {"allowed_plans": [None], "allowed_devices": [None]},
        "admin": {"allowed_plans": [None], "forbidden_plans": [":^count$"], "allowed_devices": [None]},
    }
}

_permissions_dict_allow_all = {
    "user_groups": {
        "root": {"allowed_plans": [None], "allowed_devices": [None]},
        "admin": {"allowed_plans": [None], "allowed_devices": [None]},
    }
}


# fmt: off
@pytest.mark.parametrize("restart_manager", [False, True])
# fmt: on
def test_permissions_set_get_1(re_manager, restart_manager):  # noqa: F811
    """
    Basic test for 'permissions_set' and 'permissions_get' API: check that both API work,
    check that the permissions are updated and new list of allowed plans is generated
    (expected to work for devices as well), check that if the manager process is reloaded,
    the last set of permissions is loaded from Redis and the correct list of available plans
    is generated.
    """
    status = get_queue_state()
    plans_allowed_uid_1 = status["plans_allowed_uid"]

    resp1, _ = zmq_single_request("plans_allowed", params={"user_group": "admin"})
    assert resp1["success"] is True
    assert "count" in resp1["plans_allowed"]

    resp2, _ = zmq_single_request(
        "permissions_set", params={"user_group_permissions": _permissions_dict_not_allow_count}
    )
    assert resp2["success"] is True, pprint.pformat(resp2)
    assert resp2["msg"] == ""

    if restart_manager:
        _, err_msg = zmq_single_request("manager_kill")
        assert err_msg != ""
        ttime.sleep(6)

    status = get_queue_state()
    plans_allowed_uid2 = status["plans_allowed_uid"]
    assert plans_allowed_uid2 != plans_allowed_uid_1

    resp3, _ = zmq_single_request("plans_allowed", params={"user_group": "admin"})
    assert resp3["success"] is True
    print(f"plans: {list(resp3['plans_allowed'].keys())}")

    assert "count" not in resp3["plans_allowed"]

    resp4, _ = zmq_single_request("permissions_get")
    assert resp4["success"] is True
    assert resp4["msg"] == ""
    assert resp4["user_group_permissions"] == _permissions_dict_not_allow_count


def test_permissions_set_get_2(re_manager):  # noqa: F811
    """
    Test that repeatedly uploading the same permissions dictionary does not update
    the lists of allowed plans and devices (UIDs)
    """
    status = get_queue_state()
    plans_allowed_uid_0 = status["plans_allowed_uid"]
    devices_allowed_uid_0 = status["devices_allowed_uid"]

    # Upload permissions the first time
    resp1, _ = zmq_single_request(
        "permissions_set", params={"user_group_permissions": _permissions_dict_not_allow_count}
    )
    assert resp1["success"] is True, pprint.pformat(resp1)

    status = get_queue_state()
    plans_allowed_uid_1 = status["plans_allowed_uid"]
    devices_allowed_uid_1 = status["devices_allowed_uid"]

    resp2a, _ = zmq_single_request("plans_allowed", params={"user_group": "admin"})
    assert resp2a["success"] is True
    plans_allowed_1 = resp2a["plans_allowed"]
    resp2b, _ = zmq_single_request("devices_allowed", params={"user_group": "admin"})
    assert resp2b["success"] is True
    devices_allowed_1 = resp2b["devices_allowed"]

    # Test that the new permissions were applied
    assert "count" not in plans_allowed_1

    # Upload permissions the second time
    resp3, _ = zmq_single_request(
        "permissions_set", params={"user_group_permissions": _permissions_dict_not_allow_count}
    )
    assert resp3["success"] is True, pprint.pformat(resp3)

    status = get_queue_state()
    plans_allowed_uid_2 = status["plans_allowed_uid"]
    devices_allowed_uid_2 = status["devices_allowed_uid"]

    resp4a, _ = zmq_single_request("plans_allowed", params={"user_group": "admin"})
    assert resp4a["success"] is True
    plans_allowed_2 = resp4a["plans_allowed"]
    resp4b, _ = zmq_single_request("devices_allowed", params={"user_group": "admin"})
    assert resp4b["success"] is True
    devices_allowed_2 = resp4b["devices_allowed"]

    assert plans_allowed_uid_1 != plans_allowed_uid_0
    assert devices_allowed_uid_1 != devices_allowed_uid_0

    # UIDs or lists should not change if the same permisions are repeatedly uploaded
    assert plans_allowed_uid_2 == plans_allowed_uid_1
    assert devices_allowed_uid_2 == devices_allowed_uid_1
    assert plans_allowed_2 == plans_allowed_1
    assert devices_allowed_2 == devices_allowed_1


# fmt: off
@pytest.mark.parametrize("params, err_msg", [
    ({"unsupported_param": 10}, "API request contains unsupported parameters: 'unsupported_param'"),
    ({}, "Parameter 'user_group_permissions' is missing"),
    # Following are cases of failing schema validations of permissions
    ({"user_group_permissions": {}}, "'user_groups' is a required property"),
    ({"user_group_permissions": {"user_groups": {}}}, "Missing required user group: 'root'"),
])
# fmt: on
def test_permissions_set_get_3_fail(re_manager, params, err_msg):  # noqa: F811
    """
    Tests for ``permissions_set`` 0MQ API: failing cases
    """
    resp1, _ = zmq_single_request("permissions_set", params=params)
    assert resp1["success"] is False, pprint.pformat(resp1)
    assert err_msg in resp1["msg"], resp1["msg"]


# =======================================================================================
#                              Method `environment_destroy`


def test_zmq_api_environment_destroy(re_manager):  # noqa: F811
    """
    Test for `environment_destroy` API. The test also checks if valid values of
    ``re_status`` are returned at for each step.
    """
    resp0, _ = zmq_single_request("queue_item_add", {"item": _plan3, "user": _user, "user_group": _user_group})

    status = get_queue_state()
    assert status["items_in_queue"] == 1, "Incorrect number of plans in the queue"
    assert status["running_item_uid"] is None
    assert status["re_state"] is None

    # Open environment, start a plan and then destroy the environment in the middle of
    #  the plan execution.

    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    status = get_queue_state()
    assert status["re_state"] == "idle"

    resp2, _ = zmq_single_request("queue_start")
    assert resp2["success"] is True

    ttime.sleep(2)
    status = get_queue_state()
    assert status["items_in_queue"] == 0, "Incorrect number of plans in the queue"
    assert status["running_item_uid"] is not None
    assert status["re_state"] == "running"

    resp3, _ = zmq_single_request("environment_destroy")
    assert resp1["success"] is True
    assert wait_for_condition(time=10, condition=condition_environment_closed)

    status = get_queue_state()
    assert status["items_in_queue"] == 1, "Incorrect number of plans in the queue"
    assert status["items_in_history"] == 1, "Incorrect number of plans in the history"
    assert status["running_item_uid"] is None
    assert status["re_state"] is None

    # Make sure that RE Manager is fully functional: open environment, start the plan,
    # wait for the completion and close the environment.

    resp4, _ = zmq_single_request("environment_open")
    assert resp4["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    status = get_queue_state()
    assert status["re_state"] == "idle"

    resp5, _ = zmq_single_request("queue_start")
    assert resp5["success"] is True

    ttime.sleep(2)
    status = get_queue_state()
    assert status["items_in_queue"] == 0, "Incorrect number of plans in the queue"
    assert status["running_item_uid"] is not None
    assert status["re_state"] == "running"

    assert wait_for_condition(time=60, condition=condition_queue_processing_finished)

    status = get_queue_state()
    assert status["items_in_queue"] == 0, "Incorrect number of plans in the queue"
    assert status["items_in_history"] == 2, "Incorrect number of plans in the history"
    assert status["running_item_uid"] is None
    assert status["re_state"] == "idle"

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# ======================================================================================
#                           Method 're_pause'


_plan_3steps = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 3, "delay": 2}, "item_type": "plan"}


# fmt: off
@pytest.mark.parametrize("kill_manager, pause_option, pause_before_kill", [
    (False, "immediate", 0),
    (False, "deferred", 0),
    (False, None, 0),        # None - default (immediate) option
    (True, "immediate", 0),    # Pausing is completed while the manager is being restarted
    (True, "deferred", 0),
    (True, "immediate", 3.0),  # Pausing is completed before the manager is restarted
    (True, "deferred", 3.0),
])
# fmt: on
def test_zmq_api_re_pause_1(re_manager, pause_option, kill_manager, pause_before_kill):  # noqa: F811
    """
    Test the simple case when deferred pause is requested before the last checkpoint so that
    the plan could be paused correctly using deferred and immediate options. Verify that
    ``pause_pending`` flag is correctly set.
    """

    def _check_status(n_queue, n_hist, m_state, re_state, env_state, pause_pend):
        resp, _ = zmq_single_request("status")
        assert resp["items_in_queue"] == n_queue
        assert resp["items_in_history"] == n_hist
        assert resp["manager_state"] == m_state
        if isinstance(re_state, list):
            assert resp["re_state"] in re_state, str(resp["re_state"])
        else:
            assert resp["re_state"] == re_state
        assert resp["worker_environment_state"] == env_state
        assert resp["pause_pending"] == pause_pend

    params1a = {"item": _plan_3steps, "user": _user, "user_group": _user_group}
    resp1a, _ = zmq_single_request("queue_item_add", params1a)
    assert resp1a["success"] is True, f"resp={resp1a}"

    # The queue contains only a single instruction (stop the queue).
    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    _check_status(1, 0, "idle", "idle", "idle", False)

    resp3, _ = zmq_single_request("queue_start")
    assert resp3["success"] is True

    ttime.sleep(0.9)  # ~50% of the first measurement in the plan
    _check_status(0, 0, "executing_queue", "running", "executing_plan", False)

    params = {} if pause_option is None else {"option": pause_option}
    resp3, _ = zmq_single_request("re_pause", params=params)
    assert resp3["success"] is True, f"resp={resp3}"

    if pause_option in ("deferred", None):
        # In case of immediate pause, the state will depend on how fast the request is processed
        _check_status(0, 0, "executing_queue", "running", "executing_plan", True)

    if kill_manager:
        if pause_before_kill:
            ttime.sleep(pause_before_kill)
        zmq_single_request("manager_kill")
        ttime.sleep(6)  # Wait until the manager is restarted

    assert wait_for_condition(time=20, condition=condition_manager_paused)

    _check_status(0, 0, "paused", "paused", "idle", False)

    # Execute the remaining plans (if any plans left)
    resp4, _ = zmq_single_request("re_resume")
    assert resp4["success"] is True

    ttime.sleep(1)
    _check_status(0, 0, "executing_queue", "running", "executing_plan", False)

    assert wait_for_condition(time=20, condition=condition_manager_idle)

    _check_status(0, 1, "idle", "idle", "idle", False)

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("kill_manager, pause_before_kill", [
    (False, 0),
    (True, 0),    # Pausing is completed while the manager is being restarted
    (True, 3.0),  # Pausing is completed before the manager is restarted
])
@pytest.mark.parametrize("n_plans", [1, 2])
# fmt: on
def test_zmq_api_re_pause_2(re_manager, n_plans, kill_manager, pause_before_kill):  # noqa: F811
    """
    Test the case when deferred pause is requested after the plan passes the last checkpoint.
    The plan is expected to run to completion and the queue is expected to be stopped.
    ``pause_pending`` status flag is expected to return True while the pause is
    pending, but switch to False once pause is processed (by pausing the plan or stopping
    the queue).
    """

    def _check_status(n_queue, n_hist, m_state, re_state, pause_pend):
        resp, _ = zmq_single_request("status")
        assert resp["items_in_queue"] == n_queue
        assert resp["items_in_history"] == n_hist
        assert resp["manager_state"] == m_state
        assert resp["re_state"] == re_state
        assert resp["pause_pending"] == pause_pend

    for _ in range(n_plans):
        params1a = {"item": _plan_3steps, "user": _user, "user_group": _user_group}
        resp1a, _ = zmq_single_request("queue_item_add", params1a)
        assert resp1a["success"] is True, f"resp={resp1a}"

    # The queue contains only a single instruction (stop the queue).
    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    _check_status(n_plans, 0, "idle", "idle", False)

    resp3, _ = zmq_single_request("queue_start")
    assert resp3["success"] is True

    ttime.sleep(5)  # ~50% of the third (last) measurement of the 1st plan

    resp3, _ = zmq_single_request("re_pause", params={"option": "deferred"})
    assert resp3["success"] is True

    _check_status(n_plans - 1, 0, "executing_queue", "running", True)

    if kill_manager:
        if pause_before_kill:
            ttime.sleep(pause_before_kill)
        zmq_single_request("manager_kill")
        ttime.sleep(6)  # Wait until the manager is restarted

    assert wait_for_condition(time=20, condition=condition_manager_idle)

    _check_status(n_plans - 1, 1, "idle", "idle", False)

    # Execute the remaining plans (if any plans left)
    resp4, _ = zmq_single_request("queue_start")
    assert resp4["success"] is True

    assert wait_for_condition(time=20, condition=condition_manager_idle)

    _check_status(0, n_plans, "idle", "idle", False)

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# =======================================================================================
#                              Method 're_runs'


_sample_multirun_plan1 = """
import bluesky.preprocessors as bpp
import bluesky.plan_stubs as bps


@bpp.set_run_key_decorator("run_2")
@bpp.run_decorator(md={})
def _multirun_plan_inner():
    npts, delay = 5, 1.0
    for j in range(npts):
        yield from bps.mov(motor1, j * 0.1 + 1, motor2, j * 0.2 - 2)
        yield from bps.trigger_and_read([motor1, motor2, det2])
        yield from bps.sleep(delay)


@bpp.set_run_key_decorator("run_1")
@bpp.run_decorator(md={})
def multirun_plan_nested():
    '''
    Multirun plan that is expected to produce 3 runs: 2 sequential runs nested in 1 outer run.
    '''
    npts, delay = 6, 1.0
    for j in range(int(npts / 2)):
        yield from bps.mov(motor, j * 0.2)
        yield from bps.trigger_and_read([motor, det])
        yield from bps.sleep(delay)

    yield from _multirun_plan_inner()

    yield from _multirun_plan_inner()

    for j in range(int(npts / 2), npts):
        yield from bps.mov(motor, j * 0.2)
        yield from bps.trigger_and_read([motor, det])
        yield from bps.sleep(delay)
"""


# fmt: off
@pytest.mark.parametrize("test_with_manager_restart", [False, True])
# fmt: on
def test_re_runs_1(re_manager_pc_copy, tmp_path, test_with_manager_restart):  # noqa: F811
    """
    Relatively complicated test for ``re_runs`` ZMQ API with multirun test. The same test
    is run with and without manager restart (API ``manager_kill``). Additionally
    the ``permissions_reload`` API was tested.
    """
    _, pc_path = re_manager_pc_copy
    append_code_to_last_startup_file(pc_path, additional_code=_sample_multirun_plan1)

    # Generate the new list of allowed plans and devices and reload them
    gen_list_of_plans_and_devices(startup_dir=pc_path, file_dir=pc_path, overwrite=True)

    # 'plans_allowed_uid' and 'devices_allowed_uid'
    status, _ = zmq_single_request("status")
    plans_allowed_uid = status["plans_allowed_uid"]
    devices_allowed_uid = status["devices_allowed_uid"]

    resp1, _ = zmq_single_request("permissions_reload", {"reload_plans_devices": True})
    assert resp1["success"] is True, f"resp={resp1}"

    # Check that 'plans_allowed_uid' and 'devices_allowed_uid' changed while permissions were reloaded
    status, _ = zmq_single_request("status")
    assert status["plans_allowed_uid"] != plans_allowed_uid
    assert status["devices_allowed_uid"] != devices_allowed_uid

    # Add plan to the queue
    params = {
        "item": {"name": "multirun_plan_nested", "item_type": "plan"},
        "user": _user,
        "user_group": _user_group,
    }
    resp2, _ = zmq_single_request("queue_item_add", params)
    assert resp2["success"] is True, f"resp={resp2}"

    # Open the environment
    resp3, _ = zmq_single_request("environment_open")
    assert resp3["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # Get initial value of (empty) run list to capture changes in the run list
    resp, _ = zmq_single_request("status")
    run_list_uid = resp["run_list_uid"]

    # Start the queue
    resp4, _ = zmq_single_request("queue_start")
    assert resp4["success"] is True

    # The plan consists of 3 runs: runs #2 and #3 are sequential and enclosed in run #1.
    #   As the plan is executed we are going to look at the states of the executed runs.
    #   The sequence of possible states is known and we will capture all the states we can
    #   (by monitoring 'run_list_uid' in RE Monitor status). The states may occur only in
    #   the listed sequence, but some of the states are very unlikely to be hit, so they
    #   are marked as not required.
    run_list_states = [
        {"is_open": [True], "required": True},
        {"is_open": [True, True], "required": True},
        {"is_open": [True, False], "required": False},
        {"is_open": [True, False, True], "required": True},
        {"is_open": [True, False, False], "required": True},
        {"is_open": [False, False, False], "required": False},
        {"is_open": [], "required": False},
    ]
    # We will count the number of times the state was detected.
    states_found = [0] * len(run_list_states)

    # The index of the last state.
    n_last_state = -1

    # If test includes manager restart, then do the restart.
    if test_with_manager_restart:
        ttime.sleep(4)  # Let the plan work for a little bit.
        zmq_single_request("manager_kill")

    # Wait for the end of execution of the plan with timeout (60 seconds)
    time_finish = ttime.time() + 60
    while ttime.time() < time_finish:

        # If the manager was restarted, then wait for the manager to restart.
        #   All requests will time out until the manager is restarted.
        resp, _ = zmq_single_request("status")
        if test_with_manager_restart and not resp:
            # Wait until RE Manager is restarted
            continue
        else:
            # This is an error, raise the exception
            assert resp

        # Exit if the plan execution is completed
        if resp["manager_state"] == "idle":
            break

        # Check if 'run_list_uid' changed. If yes, then read and analyze the new 'run_list_uid'.
        if run_list_uid != resp["run_list_uid"]:
            run_list_uid = resp["run_list_uid"]
            # Use all supported combinations of options to load the 'run_list_uid'.
            resp_run_list1, _ = zmq_single_request("re_runs")
            resp_run_list2, _ = zmq_single_request("re_runs", params={"option": "active"})
            resp_run_list3, _ = zmq_single_request("re_runs", params={"option": "open"})
            resp_run_list4, _ = zmq_single_request("re_runs", params={"option": "closed"})
            full_list = resp_run_list1["run_list"]
            assert resp_run_list2["run_list"] == full_list
            assert resp_run_list3["run_list"] == [_ for _ in full_list if _["is_open"]]
            assert resp_run_list4["run_list"] == [_ for _ in full_list if not _["is_open"]]

            # Save full UID list (for all runs)
            if len(full_list) == 3:
                full_uid_list = [_["uid"] for _ in full_list]

            is_open_list = [_["is_open"] for _ in full_list]
            for n, state in enumerate(run_list_states):
                if state["is_open"] == is_open_list:
                    states_found[n] += 1
                    assert n > n_last_state, f"The Run List state #{n} was visited after state #{n_last_state}"
                    n_last_state = n
                    break

        ttime.sleep(0.1)

    # Since some states could be missed if RE Manager is restarted, we don't do the following check.
    if not test_with_manager_restart:
        for n_hits, state in zip(states_found, run_list_states):
            if state["required"]:
                assert n_hits == 1

    # Finally check the status (to ensure the plan was executed correctly).
    resp5a, _ = zmq_single_request("status")
    assert resp5a["items_in_queue"] == 0
    assert resp5a["items_in_history"] == 1
    # Also check if 'run_list_uid' was updated when the run list was cleared.
    if states_found[-1]:
        # The last state in the list is an empty run list. So UID is expected to remain the same.
        assert resp5a["run_list_uid"] == run_list_uid
    else:
        # UID is expected to change, because the run list is cleared at the end of plan execution.
        assert resp5a["run_list_uid"] != run_list_uid

    # Make sure that the run list is empty.
    resp5b, _ = zmq_single_request("re_runs")
    assert resp5b["success"] is True
    assert resp5b["msg"] == ""
    assert resp5b["run_list"] == []

    # Make sure that history contains correct data.
    resp5b, _ = zmq_single_request("history_get")
    assert resp5b["success"] is True
    history = resp5b["items"]
    assert len(history) == 1, str(resp5b)
    # Check that correct number of UIDs are saved in the history
    history_run_uids = history[0]["result"]["run_uids"]
    assert len(history_run_uids) == 3, str(resp5b)
    # Make sure that the list of UID in history matches the list of UIDs in the run list
    assert history_run_uids == full_uid_list

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# =======================================================================================
#            Tests that involve restarting the manager process


def test_manager_kill_1(re_manager_pc_copy):  # noqa: F811
    """
    Test if the lists of existing plans and devices and user group permissions are
    downloading from RE worker when the manager process is restarted. The test
    includes the following steps:
    - Copy profile collection to a temporary location and start RE Manager.
    - Delete .yaml files (RE manager should not attempt to load them during the test).
    - Add 'count' plan to the queue (trivial part of the test).
    - Restart (kill) the manager and wait until it is back online.
    - Add 'count' plan to the queue and make sure it is successfully added. If the queue
      has the right size, then RE Manager has correct set of permissions that were
      loaded from RE Worker (the disk files were deleted at the beginning of the test).
    """
    _, pc_path = re_manager_pc_copy

    # Delete all .yaml files ('existing_plans_and_devices.yaml' and 'user_group_permissions.yaml')
    #   Queue Server is not expected to attempt to load those files during this test.
    files = glob.glob(os.path.join(pc_path, "*.yaml"))
    for f in files:
        os.remove(f)

    # Open the environment
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # Add 'count' plan
    params2 = {"item": _plan1, "user": _user, "user_group": _user_group}
    resp2, _ = zmq_single_request("queue_item_add", params2)
    assert resp2["success"] is True, f"resp={resp2}"

    # Now restart the manager process
    zmq_single_request("manager_kill")
    ttime.sleep(1)
    # Verify that the manager is not responsive
    with pytest.raises(TimeoutError):
        get_queue_state()
    ttime.sleep(7)  # Wait until the manager is back online

    # Attempt to add 'count' plan. RE Manager is expected to use user group permissions and
    #   the lists of existing plans and devices download from the running environment.
    resp3, _ = zmq_single_request("queue_item_add", params2)
    assert resp3["success"] is True, f"resp={resp3}"

    status = get_queue_state()
    assert status["items_in_queue"] == 2

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# =======================================================================================
#            Test if API requests with unsupported parameters are rejected
def test_zmq_api_unsupported_parameters(re_manager):  # noqa: F811
    api_names = (
        "plans_allowed",
        "devices_allowed",
        "permissions_reload",
        "queue_get",
        "queue_mode_set",
        "queue_item_add",
        "queue_item_add_batch",
        "queue_item_update",
        "queue_item_get",
        "queue_item_remove",
        "queue_item_remove_batch",
        "queue_item_move",
        "queue_item_move_batch",
        "queue_item_execute",
        "queue_clear",
        "history_get",
        "history_clear",
        "environment_open",
        "environment_close",
        "environment_destroy",
        "queue_start",
        "queue_stop",
        "queue_stop_cancel",
        "re_pause",
        "re_resume",
        "re_stop",
        "re_abort",
        "re_halt",
        "re_runs",
        "manager_stop",
        "manager_kill",
    )
    unsupported_param = {"unsupported_param": 10}

    for api_name in api_names:
        resp, _ = zmq_single_request(api_name, params=unsupported_param)
        assert resp["success"] is False, f"API name: {api_name}"
        assert "unsupported parameters: 'unsupported_param'" in resp["msg"], f"API name: {api_name}"


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
        status = get_queue_state()
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

    resp5b, _ = zmq_single_request("re_stop")
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
    public_key, private_key = generate_new_zmq_key_pair()

    if test_mode == "none":
        # No encryption
        pass
    elif test_mode == "ev":
        # Set server private key using environment variable
        monkeypatch.setenv("QSERVER_ZMQ_PRIVATE_KEY", private_key)
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


"""
@pytest.mark.parametrize("a", [0] * 100)
def test_qserver_communication_reliability(re_manager, a):  # noqa: F811
    for i in range(10):
        print(f"i={i}")
        resp0, _ = zmq_single_request("status")
        assert resp0["manager_state"] == "idle"
        print(f"status: {resp0}")
"""
