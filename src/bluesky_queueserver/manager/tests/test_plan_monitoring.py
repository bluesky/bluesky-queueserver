import uuid

import pytest

from bluesky_queueserver.manager.plan_monitoring import (
    CallbackRegisterRun,
    MsgHookStreamManager,
    RunList,
    WatcherStreamManager,
    _serialize_msg_component,
)
from bluesky_queueserver.manager.profile_ops import (
    get_default_startup_dir,
    load_profile_collection,
    load_script_into_existing_nspace,
)


def test_RunList_1():
    """
    Full functionality test: ``RunList`` class.
    """
    uids = [str(uuid.uuid4()) for _ in range(3)]
    scan_ids = list(range(1, 4))
    is_open = [True] * 3
    exit_code = [None] * 3
    expected_run_list = [
        {"uid": _[0], "scan_id": _[1], "is_open": _[2], "exit_status": _[3]}
        for _ in zip(uids, scan_ids, is_open, exit_code)
    ]

    # Create run list
    run_list = RunList()
    assert run_list.is_changed() is False
    assert run_list.get_run_list() == []

    run_list.enable()

    # Add one object
    run_list.add_run(uid=uids[0], scan_id=scan_ids[0])
    assert run_list.is_changed() is True
    assert run_list.get_run_list() == expected_run_list[0:1]
    assert run_list.is_changed() is True
    assert run_list.get_run_list(clear_state=True) == expected_run_list[0:1]
    assert run_list.is_changed() is False

    # Add two more objects
    run_list.add_run(uid=uids[1], scan_id=scan_ids[1])
    run_list.add_run(uid=uids[2], scan_id=scan_ids[2])
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

    assert run_list.get_uids() == uids
    assert run_list.get_scan_ids() == scan_ids

    run_list.clear()
    assert run_list.is_changed() is True
    assert run_list.get_run_list(clear_state=True) == []
    assert run_list.is_changed() is False


def test_RunList_2():
    """
    Additional tests: ``RunList`` class.
    """

    uids = [str(uuid.uuid4()) for _ in range(2)]
    scan_ids = list(range(1, 3))

    # Create run list
    run_list = RunList()
    assert run_list.is_changed() is False
    assert run_list.get_run_list() == []

    assert run_list.is_enabled() is False
    run_list.enable()
    assert run_list.is_enabled() is True

    # List is enabled, add one run
    run_list.add_run(uid=uids[0], scan_id=scan_ids[0])
    assert run_list.is_changed() is True
    assert run_list.nruns == 1
    run_list.get_run_list(clear_state=True)
    assert run_list.is_changed() is False

    run_list.set_run_closed(uid=uids[0], exit_status="success")
    assert run_list.is_changed() is True
    assert run_list.nruns == 1

    # Disable the list
    run_list.disable()
    assert run_list.is_enabled() is False

    # The state can still be cleared
    assert run_list.is_changed() is True
    run_list.get_run_list(clear_state=True)
    assert run_list.is_changed() is False

    # But no plans can be added to the list
    run_list.add_run(uid=uids[1], scan_id=scan_ids[1])
    assert run_list.is_changed() is False
    assert run_list.nruns == 1
    run_list.get_run_list(clear_state=True)
    assert run_list.is_changed() is False
    run_list.set_run_closed(uid=uids[1], exit_status="success")
    assert run_list.is_changed() is False
    assert run_list.nruns == 1

    assert run_list.get_uids() == uids[:1]
    assert run_list.get_scan_ids() == scan_ids[:1]

    # The disabled list can be cleared
    run_list.clear()
    assert run_list.nruns == 0


# fmt: off
@pytest.mark.parametrize("scan_id", [101, None, []])
# fmt: on
def test_CallbackRegisterRun_1(scan_id):
    """
    Basic test: ``CallbackRegisterRun`` class.
    """
    run_list = RunList()
    run_list.enable()
    cb = CallbackRegisterRun(run_list=run_list)
    uid = str(uuid.uuid4())

    param = {"scan_id": scan_id} if scan_id is not None else {}
    param_expected = {"scan_id": scan_id} if isinstance(scan_id, int) else {"scan_id": None}
    cb("start", {"uid": uid, **param})
    assert run_list.get_run_list() == [{"uid": uid, "is_open": True, "exit_status": None, **param_expected}]
    cb("stop", {"run_start": uid, "exit_status": "success"})
    assert run_list.get_run_list() == [{"uid": uid, "is_open": False, "exit_status": "success", **param_expected}]



class _MockQueue:
    """A simple list-backed mock for multiprocessing.Queue."""

    def __init__(self):
        self.messages = []

    def put(self, msg):
        self.messages.append(msg)


class _MockStatus:
    """A mock Status object that supports watch()."""

    def __init__(self, *, name="motor1", done=False, supports_watch=True):
        self._name = name
        self.done = done
        self._supports_watch = supports_watch
        self._watchers = []

    def watch(self, func):
        if not self._supports_watch:
            raise AttributeError("watch not supported")
        self._watchers.append(func)

    def simulate_update(self, **kwargs):
        """Simulate a watcher callback from the status object."""
        defaults = dict(
            name=self._name,
            current=None,
            initial=None,
            target=None,
            unit=None,
            precision=None,
            fraction=None,
            time_elapsed=None,
            time_remaining=None,
        )
        defaults.update(kwargs)
        for w in self._watchers:
            w(**defaults)


def _get_progress_messages(mock_queue):
    """Extract only progress payloads from the mock queue (info channel, 'device_progress' key)."""
    results = []
    for msg in mock_queue.messages:
        if msg.get("channel") == "info" and isinstance(msg.get("msg"), dict):
            payload = msg["msg"].get("device_progress")
            if isinstance(payload, dict):
                results.append(payload)
    return results


def test_WatcherStreamManager_basic():
    """
    WatcherStreamManager subscribes to status objects and publishes progress updates.
    """
    mq = _MockQueue()
    wsm = WatcherStreamManager(msg_queue=mq, min_update_period=0)

    st = _MockStatus(name="motor1")
    wsm({st})

    # Simulate an update
    st.simulate_update(current=1.0, initial=0.0, target=5.0, unit="mm", precision=3, fraction=0.2)

    msgs = _get_progress_messages(mq)
    assert len(msgs) == 1
    assert msgs[0]["name"] == "motor1"
    assert msgs[0]["current"] == 1.0
    assert msgs[0]["target"] == 5.0
    assert msgs[0]["unit"] == "mm"
    assert msgs[0]["done"] is False

    # Simulate completion
    st.done = True
    st.simulate_update(current=5.0, initial=0.0, target=5.0, fraction=1.0)

    msgs = _get_progress_messages(mq)
    assert len(msgs) == 2
    assert msgs[1]["done"] is True
    assert msgs[1]["current"] == 5.0


def test_WatcherStreamManager_none_clears():
    """
    Calling with None sends a completed message and resets state.
    """
    mq = _MockQueue()
    wsm = WatcherStreamManager(msg_queue=mq, min_update_period=0)

    st = _MockStatus(name="motor1")
    wsm({st})
    st.simulate_update(current=1.0)

    # Signal end of waiting
    wsm(None)

    msgs = _get_progress_messages(mq)
    # Last message should be the completion indicator
    assert msgs[-1] == {"completed": True}


def test_WatcherStreamManager_no_resubscribe():
    """
    Calling with the same status object multiple times does not re-subscribe.
    """
    mq = _MockQueue()
    wsm = WatcherStreamManager(msg_queue=mq, min_update_period=0)

    st = _MockStatus(name="motor1")
    wsm({st})
    wsm({st})
    wsm({st})

    # Should have exactly one watcher registered
    assert len(st._watchers) == 1


def test_WatcherStreamManager_throttling():
    """
    Updates that arrive faster than min_update_period are suppressed,
    except for the final (done) update.
    """
    mq = _MockQueue()
    # Set a very long throttle period so all non-final updates are suppressed
    wsm = WatcherStreamManager(msg_queue=mq, min_update_period=9999)

    st = _MockStatus(name="motor1")
    wsm({st})

    # First update always goes through (last_sent starts at 0)
    st.simulate_update(current=1.0)
    # Second update should be throttled
    st.simulate_update(current=2.0)
    # Third update should be throttled
    st.simulate_update(current=3.0)

    msgs = _get_progress_messages(mq)
    assert len(msgs) == 1  # Only the first one

    # Final update (done=True) must always go through
    st.done = True
    st.simulate_update(current=5.0)

    msgs = _get_progress_messages(mq)
    assert len(msgs) == 2
    assert msgs[1]["done"] is True


def test_WatcherStreamManager_no_watch_support():
    """
    Status objects without watch() are silently ignored.
    """
    mq = _MockQueue()
    wsm = WatcherStreamManager(msg_queue=mq, min_update_period=0)

    st = _MockStatus(supports_watch=False)
    # Should not raise
    wsm({st})

    msgs = _get_progress_messages(mq)
    assert len(msgs) == 0


def test_WatcherStreamManager_already_done():
    """
    Status objects that are already done are not subscribed to.
    """
    mq = _MockQueue()
    wsm = WatcherStreamManager(msg_queue=mq, min_update_period=0)

    st = _MockStatus(done=True)
    wsm({st})

    assert len(st._watchers) == 0


def test_WatcherStreamManager_no_watch_attr():
    """
    Objects without a watch attribute at all are ignored.
    """
    mq = _MockQueue()
    wsm = WatcherStreamManager(msg_queue=mq, min_update_period=0)

    class _BareStatus:
        done = False

    wsm({_BareStatus()})
    msgs = _get_progress_messages(mq)
    assert len(msgs) == 0


def test_WatcherStreamManager_multiple_statuses():
    """
    Multiple concurrent status objects each get their own progress stream.
    """
    mq = _MockQueue()
    wsm = WatcherStreamManager(msg_queue=mq, min_update_period=0)

    st1 = _MockStatus(name="motor1")
    st2 = _MockStatus(name="motor2")
    wsm({st1, st2})

    st1.simulate_update(current=1.0, target=5.0)
    st2.simulate_update(current=10.0, target=20.0)

    msgs = _get_progress_messages(mq)
    assert len(msgs) == 2
    names = {m["name"] for m in msgs}
    assert names == {"motor1", "motor2"}


def test_WatcherStreamManager_name_none():
    """
    If the watch callback receives name=None, a generated label is used.
    """
    mq = _MockQueue()
    wsm = WatcherStreamManager(msg_queue=mq, min_update_period=0)

    st = _MockStatus(name=None)
    wsm({st})
    st.simulate_update(name=None, current=1.0)

    msgs = _get_progress_messages(mq)
    assert len(msgs) == 1
    assert msgs[0]["name"].startswith("Status ")


def test_WatcherStreamManager_json_safe_coercion():
    """
    Non-JSON-safe values for current/initial/target are coerced.
    """
    import numpy as np

    mq = _MockQueue()
    wsm = WatcherStreamManager(msg_queue=mq, min_update_period=0)

    st = _MockStatus(name="motor1")
    wsm({st})
    st.simulate_update(current=np.float64(3.14), initial=np.int32(0), target=np.float64(10.0))

    msgs = _get_progress_messages(mq)
    assert len(msgs) == 1
    assert isinstance(msgs[0]["current"], float)
    assert isinstance(msgs[0]["initial"], float)
    assert isinstance(msgs[0]["target"], float)


def test_WatcherStreamManager_sim_motor_move():
    """
    Integration test: load the simulated profile collection, append a script
    that creates a slow motor, attach WatcherStreamManager to the RunEngine,
    move the motor, and verify that progress updates were published to the queue.
    """

    # Load the simulated profile collection
    startup_dir = get_default_startup_dir()
    nspace = load_profile_collection(startup_dir, patch_profiles=True)

    # Append a script that creates a slow motor (non-zero delay so watch fires)
    script = """
from ophyd.sim import SynAxis
slow_motor = SynAxis(name="slow_motor", labels={"motors"})
slow_motor.delay = 0.3
"""
    load_script_into_existing_nspace(
        script=script,
        nspace=nspace,
        script_root_path=startup_dir,
    )

    RE = nspace["RE"]
    slow_motor = nspace["slow_motor"]

    # Attach WatcherStreamManager
    mq = _MockQueue()
    wsm = WatcherStreamManager(msg_queue=mq, min_update_period=0)
    RE.waiting_hook = wsm

    # Move the motor from 0 to 1 — this triggers waiting_hook with a MoveStatus
    RE(nspace["mv"](slow_motor, 1))

    msgs = _get_progress_messages(mq)

    # We should have received at least one progress update and one completed message
    progress_updates = [m for m in msgs if "completed" not in m]
    completed_msgs = [m for m in msgs if m.get("completed") is True]

    assert len(progress_updates) > 0, "Expected at least one progress update during motor move"
    assert len(completed_msgs) >= 1, "Expected a 'completed' message after motor move"

    # Verify the structure of a progress update
    update = progress_updates[0]
    assert "name" in update
    assert "current" in update
    assert "target" in update
    assert update["done"] in (True, False)
    assert update["name"] == "slow_motor"

    # The target should be 1 (where we moved the motor)
    assert update["target"] == 1


def _get_message_payloads(mock_queue):
    """Extract only message payloads from the mock queue (info channel, 're_message' key)."""
    results = []
    for msg in mock_queue.messages:
        if msg.get("channel") == "info" and isinstance(msg.get("msg"), dict):
            payload = msg["msg"].get("re_message")
            if isinstance(payload, dict):
                results.append(payload)
    return results


class _MockMsg:
    """A simple mock for a bluesky ``Msg`` namedtuple."""

    def __init__(self, command, obj=None, args=(), kwargs=None, run=None):
        self.command = command
        self.obj = obj
        self.args = args
        self.kwargs = kwargs or {}
        self.run = run


class _MockDevice:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"{type(self).__name__}(name={self.name!r})"


def test_MsgHookStreamManager_basic():
    """
    MsgHookStreamManager serializes messages and publishes them to the queue.
    """
    mq = _MockQueue()
    mhsm = MsgHookStreamManager(msg_queue=mq)

    msg = _MockMsg("set", obj=_MockDevice("motor1"), args=(5,), kwargs={"group": "A"}, run=None)
    mhsm(msg)

    payloads = _get_message_payloads(mq)
    assert len(payloads) == 1
    p = payloads[0]
    assert p["command"] == "set"
    assert p["obj"] == "_MockDevice(name='motor1')"
    assert p["args"] == [5]
    assert p["kwargs"] == {"group": "A"}
    assert p["run"] is None
    assert isinstance(p["time"], float)


def test_MsgHookStreamManager_serializes_nonjson():
    """
    Non-JSON-safe components are coerced to strings (or reprs for devices).
    """
    mq = _MockQueue()
    mhsm = MsgHookStreamManager(msg_queue=mq)

    obj = object()
    msg = _MockMsg("read", obj=_MockDevice("det"), args=(_MockDevice("m2"), obj), kwargs={"k": _MockDevice("m3")})
    mhsm(msg)

    p = _get_message_payloads(mq)[0]
    assert p["command"] == "read"
    assert p["obj"] == "_MockDevice(name='det')"
    assert p["args"][0] == "_MockDevice(name='m2')"
    assert isinstance(p["args"][1], str)  # arbitrary object coerced to str
    assert p["kwargs"] == {"k": "_MockDevice(name='m3')"}


def test_serialize_msg_component_enum():
    """
    Enum members are serialized by their value, regardless of the enum base class.
    """
    import enum

    class Color(enum.Enum):
        RED = "red_value"

    class Speed(enum.IntEnum):
        FAST = 3

    class SColor(str, enum.Enum):
        BLUE = "blue_value"

    assert _serialize_msg_component(Color.RED) == "red_value"
    assert _serialize_msg_component(Speed.FAST) == 3
    assert type(_serialize_msg_component(Speed.FAST)) is int
    assert _serialize_msg_component(SColor.BLUE) == "blue_value"
    assert type(_serialize_msg_component(SColor.BLUE)) is str


def test_serialize_msg_component_nonfinite_floats():
    """
    NaN is serialized to None and infinities to strings (JSON-safe).
    """
    assert _serialize_msg_component(float("nan")) is None
    assert _serialize_msg_component(float("inf")) == "Infinity"
    assert _serialize_msg_component(float("-inf")) == "-Infinity"
    assert _serialize_msg_component(1.5) == 1.5


def test_serialize_msg_component_numpy():
    """
    Numpy scalars and arrays are converted to native Python types (including non-finite
    coercion inside arrays).
    """
    import numpy as np

    assert _serialize_msg_component(np.float64(1.5)) == 1.5
    assert type(_serialize_msg_component(np.float64(1.5))) is float
    assert _serialize_msg_component(np.float64("nan")) is None
    assert _serialize_msg_component(np.int64(7)) == 7
    assert type(_serialize_msg_component(np.int64(7))) is int
    assert _serialize_msg_component(np.bool_(True)) is True
    assert _serialize_msg_component(np.array([1.0, float("nan"), 3.0])) == [1.0, None, 3.0]
    assert _serialize_msg_component(np.array([[1, 2], [3, 4]])) == [[1, 2], [3, 4]]


def test_serialize_msg_component_bytes_and_datetime():
    """
    Bytes and datetimes fall back to their string representation.
    """
    import datetime

    assert _serialize_msg_component(b"abc") == "b'abc'"
    assert _serialize_msg_component(datetime.datetime(2020, 1, 1)) == "2020-01-01 00:00:00"


def test_serialize_msg_component_nested():
    """
    Enum, numpy and non-finite values are coerced recursively inside containers.
    """
    import enum

    import numpy as np

    class Color(enum.Enum):
        RED = "red_value"

    value = {"a": [np.int64(1), float("inf")], "b": Color.RED}
    assert _serialize_msg_component(value) == {"a": [1, "Infinity"], "b": "red_value"}


def test_MsgHookStreamManager_chains_existing_hook():
    """
    An existing ``msg_hook`` is preserved and called after streaming.
    """
    mq = _MockQueue()
    mhsm = MsgHookStreamManager(msg_queue=mq)

    received = []
    mhsm.msg_hook = lambda m: received.append(m)

    msg = _MockMsg("checkpoint")
    mhsm(msg)

    assert len(_get_message_payloads(mq)) == 1
    assert received == [msg]


def test_MsgHookStreamManager_sim_motor_move():
    """
    Integration test: attach MsgHookStreamManager to the RunEngine, run a plan,
    and verify that serialized messages were published to the queue.
    """
    startup_dir = get_default_startup_dir()
    nspace = load_profile_collection(startup_dir, patch_profiles=True)

    RE = nspace["RE"]
    motor = nspace["motor"]

    mq = _MockQueue()
    mhsm = MsgHookStreamManager(msg_queue=mq)
    RE.msg_hook = mhsm

    RE(nspace["mv"](motor, 1))

    payloads = _get_message_payloads(mq)

    # Every payload carries a float timestamp; strip it before exact comparison.
    for p in payloads:
        assert isinstance(p["time"], float)
    payloads_no_time = [{k: v for k, v in p.items() if k != "time"} for p in payloads]

    # The 'set' and 'wait' messages share the same auto-generated group id.
    group = payloads_no_time[0]["kwargs"]["group"]

    # The device is serialized via repr(), which includes its type and name.
    obj_repr = payloads_no_time[0]["obj"]
    assert isinstance(obj_repr, str) and "motor" in obj_repr

    assert payloads_no_time == [
        {"command": "set", "obj": obj_repr, "args": [1], "kwargs": {"group": group}, "run": None},
        {"command": "wait", "obj": None, "args": [], "kwargs": {"group": group, "timeout": None}, "run": None},
    ]
