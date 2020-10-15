import pytest

from ._common import (
    zmq_communicate,
)

from ._common import re_manager, re_manager_pc_copy  # noqa: F401

# Plans used in most of the tests: '_plan1' and '_plan2' are quickly executed '_plan3' runs for 5 seconds.
_plan1 = {"name": "count", "args": [["det1", "det2"]]}
_plan2 = {"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10]}
_plan3 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 5, "delay": 1}}

# User name and user group name used throughout most of the tests.
_user, _user_group = "Testing Script", "admin"

# =======================================================================================
#                             Method 'queue_plan_add'


def test_zmq_api_queue_plan_add_1(re_manager):  # noqa F811
    """
    Basic test for `queue_plan_add` method.
    """
    resp1, _ = zmq_communicate("queue_plan_add", {"plan": _plan1, "user": _user, "user_group": _user_group})
    assert resp1["success"] is True
    assert resp1["qsize"] == 1
    assert resp1["plan"]["name"] == _plan1["name"]
    assert resp1["plan"]["args"] == _plan1["args"]
    assert resp1["plan"]["user"] == _user
    assert resp1["plan"]["user_group"] == _user_group
    assert "plan_uid" in resp1["plan"]

    resp2, _ = zmq_communicate("queue_get")
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
    zmq_communicate("queue_plan_add", params1)
    zmq_communicate("queue_plan_add", params1)

    # Add another entry at the specified position
    params2 = {"plan": plan2, "user": _user, "user_group": _user_group}
    if pos is not None:
        params2.update({"pos": pos})

    resp1, _ = zmq_communicate("queue_plan_add", params2)

    assert resp1["success"] is success
    assert resp1["qsize"] == (3 if success else None)
    assert resp1["plan"]["name"] == "count"
    assert resp1["plan"]["args"] == plan2["args"]
    assert resp1["plan"]["user"] == _user
    assert resp1["plan"]["user_group"] == _user_group
    assert "plan_uid" in resp1["plan"]

    resp2, _ = zmq_communicate("queue_get")

    assert len(resp2["queue"]) == (3 if success else 2)
    assert resp2["running_plan"] == {}

    if success:
        assert resp2["queue"][pos_result]["args"] == plan2["args"]


def test_zmq_api_queue_plan_add_3_fail(re_manager):  # noqa F811

    # Unknown plan name
    plan1 = {"name": "count_test", "args": [["det1", "det2"]]}
    params1 = {"plan": plan1, "user": _user, "user_group": _user_group}
    resp1, _ = zmq_communicate("queue_plan_add", params1)
    assert resp1["success"] is False
    assert "Plan 'count_test' is not in the list of allowed plans" in resp1["msg"]

    # Unknown kwarg
    plan2 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"abc": 10}}
    params2 = {"plan": plan2, "user": _user, "user_group": _user_group}
    resp2, _ = zmq_communicate("queue_plan_add", params2)
    assert resp2["success"] is False
    assert "Unexpected kwargs ['abc'] in the plan parameters" in resp2["msg"]

    # User name is not specified
    params3 = {"plan": plan2, "user_group": _user_group}
    resp3, _ = zmq_communicate("queue_plan_add", params3)
    assert resp3["success"] is False
    assert "user name is not specified" in resp3["msg"]

    # User group is not specified
    params4 = {"plan": plan2, "user": _user}
    resp4, _ = zmq_communicate("queue_plan_add", params4)
    assert resp4["success"] is False
    assert "user group is not specified" in resp4["msg"]

    # User group is not specified
    params5 = {"plan": plan2, "user": _user, "user_group": "no_such_group"}
    resp5, _ = zmq_communicate("queue_plan_add", params5)
    assert resp5["success"] is False
    assert "Unknown user group: 'no_such_group'" in resp5["msg"]

    # User group is not specified
    params6 = {"user": _user, "user_group": "no_such_group"}
    resp6, _ = zmq_communicate("queue_plan_add", params6)
    assert resp6["success"] is False
    assert "no plan is specified" in resp6["msg"]

    # Valid plan
    plan7 = {"name": "count", "args": [["det1", "det2"]]}
    params7 = {"plan": plan7, "user": _user, "user_group": _user_group}
    resp7, _ = zmq_communicate("queue_plan_add", params7)
    assert resp7["success"] is True
    assert resp7["qsize"] == 1
    assert resp7["plan"]["name"] == "count"
    assert resp7["plan"]["args"] == [["det1", "det2"]]
    assert resp7["plan"]["user"] == _user
    assert resp7["plan"]["user_group"] == _user_group
    assert "plan_uid" in resp7["plan"]

    resp8, _ = zmq_communicate("queue_get")
    assert resp8["queue"] != []
    assert len(resp8["queue"]) == 1
    assert resp8["queue"][0] == resp7["plan"]
    assert resp8["running_plan"] == {}
