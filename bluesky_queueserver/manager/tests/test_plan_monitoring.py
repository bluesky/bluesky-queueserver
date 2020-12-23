import pytest
import uuid

from bluesky_queueserver.manager.plan_monitoring import RunList, CallbackRegisterRun


def test_RunList_1():
    """
    Full functionality test: ``RunList`` class.
    """
    uids = [str(uuid.uuid4()) for _ in range(3)]
    is_open = [True] * 3
    exit_code = [None] * 3
    expected_run_list = [
        {"uid": _[0], "is_open": _[1], "exit_status": _[2]} for _ in zip(uids, is_open, exit_code)
    ]

    # Create run list
    run_list = RunList()
    assert run_list.is_changed() is False
    assert run_list.get_run_list() == []

    # Add one object
    run_list.add_run(uid=uids[0])
    assert run_list.is_changed() is True
    assert run_list.get_run_list() == expected_run_list[0:1]
    assert run_list.is_changed() is True
    assert run_list.get_run_list(clear_state=True) == expected_run_list[0:1]
    assert run_list.is_changed() is False

    # Add two more objects
    run_list.add_run(uid=uids[1])
    run_list.add_run(uid=uids[2])
    assert run_list.is_changed() is True
    assert run_list.get_run_list(clear_state=True) == expected_run_list
    assert run_list.is_changed() is False

    # Set the second object as completed
    expected_run_list[1]["is_open"] = False
    expected_run_list[1]["exit_status"] = "success"
    run_list.set_run_closed(uid=uids[1], exit_status="success")
    assert run_list.is_changed() is True
    assert run_list.get_run_list(clear_state=True) == expected_run_list
    assert run_list.is_changed() is False

    # Fail case: non-existing UID
    with pytest.raises(Exception, match="Run with UID .* was not found in the list"):
        run_list.set_run_closed(uid="non-existing-uid", exit_status="success")
    assert run_list.is_changed() is False

    run_list.clear()
    assert run_list.is_changed() is True
    assert run_list.get_run_list(clear_state=True) == []
    assert run_list.is_changed() is False


def test_CallbackRegisterRun_1():
    """
    Basic test: ``CallbackRegisterRun`` class.
    """
    run_list = RunList()
    cb = CallbackRegisterRun(run_list=run_list)
    uid = str(uuid.uuid4())

    cb("start", {"uid": uid})
    assert run_list.get_run_list() == [{"uid": uid, "is_open": True, "exit_status": None}]
    cb("stop", {"run_start": uid, "exit_status": "success"})
    assert run_list.get_run_list() == [{"uid": uid, "is_open": False, "exit_status": "success"}]
