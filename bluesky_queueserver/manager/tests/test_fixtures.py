from bluesky_queueserver.manager.comms import zmq_single_request

from .common import (  # noqa: F401
    _user,
    _user_group,
    condition_environment_closed,
    condition_environment_created,
    condition_manager_idle,
    re_manager_cmd,
    wait_for_condition,
)


def test_fixture_re_manager_cmd_1(re_manager_cmd):  # noqa F811
    """
    Basic test for ``re_manager_cmd``.
    """
    # Create RE Manager with no parameters
    re_manager_cmd()
    # The second call is supposed to close the existing RE Manager and create a new one.
    re_manager_cmd([])


def test_fixture_re_manager_cmd_2(re_manager_cmd):  # noqa F811
    """
    Test for the fixture ``re_manager_cmd``: test if the plans are properly executed.
    """
    re_manager_cmd()

    plan = {"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10], "item_type": "plan"}

    # Plan
    params1 = {"item": plan, "user": _user, "user_group": _user_group}
    resp1, _ = zmq_single_request("queue_item_add", params1)
    assert resp1["success"] is True, f"resp={resp1}"

    resp2, _ = zmq_single_request("status")
    assert resp2["items_in_queue"] == 1
    assert resp2["items_in_history"] == 0

    # Open the environment.
    resp3, _ = zmq_single_request("environment_open")
    assert resp3["success"] is True
    assert wait_for_condition(time=10, condition=condition_environment_created)

    resp4, _ = zmq_single_request("queue_start")
    assert resp4["success"] is True

    assert wait_for_condition(time=5, condition=condition_manager_idle)

    resp5, _ = zmq_single_request("status")
    assert resp5["items_in_queue"] == 0
    assert resp5["items_in_history"] == 1

    resp6, _ = zmq_single_request("history_get")
    history = resp6["items"]
    assert len(history) == 1

    # Close the environment.
    resp7, _ = zmq_single_request("environment_close")
    assert resp7["success"] is True, f"resp={resp7}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)
