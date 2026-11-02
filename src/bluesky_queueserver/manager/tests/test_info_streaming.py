import pprint
import re
import threading
import time as ttime

import pytest

from bluesky_queueserver.manager.output_streaming import (
    ReceiveSystemInfo,
    _default_zmq_console_topic,
    _default_zmq_info_topic,
)

from .common import (
    _user,
    _user_group,
    append_code_to_last_startup_file,
    condition_environment_closed,
    condition_environment_created,
    condition_queue_processing_finished,
    copy_default_profile_collection,
    re_manager_cmd,  # noqa: F401
    use_zmq_encoding_for_tests,
    wait_for_condition,
    wait_for_task_result,
    zmq_request,
)

timeout_env_open = 10


class ReceiveMessages(threading.Thread):
    def __init__(self, *, receiver_class, zmq_subscribe_addr, zmq_topic, encoding, timeout=0.1):
        super().__init__()
        self._rco = receiver_class(zmq_subscribe_addr=zmq_subscribe_addr, zmq_topic=zmq_topic, encoding=encoding)
        self._exit = False
        self.received_msgs = []
        self._timeout = timeout
        self.n_timeouts = 0

    def run(self):
        while True:
            try:
                _ = {} if (self._timeout is None) else {"timeout": self._timeout}
                msg = self._rco.recv(**_)
                self.received_msgs.append(msg)
            except TimeoutError:
                self.n_timeouts += 1
            if self._exit:
                break

    def stop(self):
        self._exit = True

    def subscribe(self):
        self._rco.subscribe()

    def unsubscribe(self):
        self._rco.unsubscribe()

    def clear(self):
        # Clear the messages
        self.received_msgs.clear()
        self.n_timeouts = 0

    def filter_msgs(self, key):
        """
        Returns a list of messages of the specific type (with matching key).
        """
        return [_ for _ in self.received_msgs if key in _["msg"].keys()]


# fmt: off
@pytest.mark.parametrize("stream_enabled", [True, False, None])
# fmt: on
def test_zmq_info_streaming_1(monkeypatch, re_manager_cmd, stream_enabled):  # noqa: F811
    """
    Test 0MQ streaming functionality: streaming of status messages.
    Test periodic streaming (once per second).
    Test that streamed status reflect current state of RE Manager.
    Test that streaming can be disabled.
    """
    address_info_server = "tcp://*:60621"
    address_info_client = "tcp://localhost:60621"

    params_server = [f"--zmq-info-addr={address_info_server}"]
    if stream_enabled is not None:
        params_server.append(f"--zmq-publish-info={'ON' if stream_enabled else 'OFF'}")


    zmq_encoding = use_zmq_encoding_for_tests()

    rm_info = ReceiveMessages(
        receiver_class=ReceiveSystemInfo,
        zmq_subscribe_addr=address_info_client,
        zmq_topic=_default_zmq_info_topic,
        encoding=zmq_encoding,
    )

    try:
        rm_info.start()

        re_manager_cmd(params_server)

        # Test periodic streaming of status messages (once per second)
        if stream_enabled is True:
            ttime.sleep(6)
            assert len(rm_info.received_msgs) > 5

            msg_prev = rm_info.received_msgs[-2]
            msg_last = rm_info.received_msgs[-1]
            assert msg_last["time"] > msg_prev["time"]
            status_prev = msg_prev["msg"]["status"]
            status_last = msg_last["msg"]["status"]
            assert status_last["status_uid"] == status_prev["status_uid"]
            assert status_last["manager_state"] == "idle"
            assert status_last["worker_environment_exists"] is False

        zmq_request("environment_open")
        assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

        # The assumption is that only 'status' messages are streamed.
        # If other messages are streamed, then the test needs to be adjusted.
        if stream_enabled is True:
            status_1 = rm_info.received_msgs[-1]["msg"]["status"]
            assert status_1["worker_environment_exists"] is True

        zmq_request("environment_close")
        assert wait_for_condition(time=3, condition=condition_environment_closed)

        if stream_enabled is True:
            status_2 = rm_info.received_msgs[-1]["msg"]["status"]
            assert status_2["worker_environment_exists"] is False
            assert status_2["status_uid"] != status_1["status_uid"]

        if stream_enabled is not True:
            assert len(rm_info.received_msgs) == 0

    finally:
        rm_info.stop()
        rm_info.join()


_script_device_progress = """

from ophyd_async import sim
from ophyd_async.core import init_devices

with init_devices():
    sim_motor = sim.SimMotor(name="sim_motor", instant=False)


# Check waiting_hook and WatcherStreamManager state
def unit_test_waiting_hook(device_progress_enabled):
    from bluesky_queueserver.manager.plan_monitoring import WatcherStreamManager
    msg = ""
    if device_progress_enabled:
        if not isinstance(RE.waiting_hook, WatcherStreamManager):
            msg = "RE.waiting_hook is not pointing to WatcherStreamManager object"
        elif isinstance(RE.waiting_hook.waiting_hook, WatcherStreamManager):
            msg = "RE.waiting_hook.waiting_hook is pointing to WatcherStreamManager object"
        elif RE.waiting_hook == RE.waiting_hook.waiting_hook:
            msg = "RE.waiting_hook and RE.waiting_hook.waiting_hook are pointing to the same object"
    else:
        if isinstance(RE.waiting_hook, WatcherStreamManager):
            msg = "RE.waiting_hook is pointing to WatcherStreamManager object"

    return msg == "", msg
"""


_script_enable_progress_bar = """

from bluesky.utils import ProgressBarManager
RE.waiting_hook = ProgressBarManager()
"""

_script_disable_progress_bar = """

RE.waiting_hook = None
"""


# fmt: off
@pytest.mark.parametrize("enable_progress_bar", [True, False])
@pytest.mark.parametrize("stream_dev_progress_enabled", [True, False, None])
# fmt: on
def test_zmq_info_streaming_2(
    tmp_path, monkeypatch, re_manager_cmd, enable_progress_bar, stream_dev_progress_enabled  # noqa: F811
    ):
    """
    Test 0MQ streaming functionality: streaming of device progress.
    Test that streaming of device progress can be disabled.
    """

    pc_path = copy_default_profile_collection(tmp_path)

    # Add extra plan. The original set of startup files will not contain this plan.
    append_code_to_last_startup_file(pc_path, additional_code=_script_device_progress)

    if enable_progress_bar:
        append_code_to_last_startup_file(pc_path, additional_code=_script_enable_progress_bar)
    else:
        append_code_to_last_startup_file(pc_path, additional_code=_script_disable_progress_bar)

    address_info_server = "tcp://*:60621"
    address_info_client = "tcp://localhost:60621"

    params_server = [
        f"--zmq-info-addr={address_info_server}",
        "--zmq-publish-console=ON",
        "--zmq-publish-info=ON",
        f"--startup-dir={pc_path}"
    ]
    if stream_dev_progress_enabled is not None:
        params_server.append(f"--zmq-stream-device-progress={'ON' if stream_dev_progress_enabled else 'OFF'}")

    zmq_encoding = use_zmq_encoding_for_tests()

    rm_console = ReceiveMessages(
        receiver_class=ReceiveSystemInfo,
        zmq_subscribe_addr=address_info_client,
        zmq_topic=_default_zmq_console_topic,
        encoding=zmq_encoding,

    )
    rm_info = ReceiveMessages(
        receiver_class=ReceiveSystemInfo,
        zmq_subscribe_addr=address_info_client,
        zmq_topic=_default_zmq_info_topic,
        encoding=zmq_encoding,
    )
    msg_key = "device_progress"

    try:
        rm_console.start()
        rm_info.start()

        re_manager_cmd(params_server)

        zmq_request("environment_open")
        assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

        for _ntest in range(2):
            rm_console.clear()
            rm_info.clear()

            msgs = rm_info.filter_msgs(msg_key)
            assert len(msgs) == 0

            pos_start = _ntest * 5.0
            pos_finish = pos_start + 5.0

            _plan_mv = {"name": "mv", "args": ["sim_motor", pos_finish], "item_type": "plan"}
            params = {"item": _plan_mv, "user": _user, "user_group": _user_group}
            resp2, _ = zmq_request("queue_item_add", params)
            assert resp2["success"] is True, f"resp={resp2}"

            resp3, _ = zmq_request("queue_start")
            assert resp3["success"] is True

            assert wait_for_condition(time=60, condition=condition_queue_processing_finished)
            ##ttime.sleep(2)

            msgs = rm_info.filter_msgs(msg_key)
            print(f"msgs = {pprint.pformat(msgs)}")
            if stream_dev_progress_enabled is True:
                assert len(msgs) > 3
                assert msgs[0]["msg"]["device_progress"]["name"] == "sim_motor"
                assert msgs[0]["msg"]["device_progress"]["current"] == pos_start
                assert msgs[-2]["msg"]["device_progress"]["current"] == pos_finish
                assert msgs[-1]["msg"]["device_progress"]["completed"] is True
            else:
                assert len(msgs) == 0

            # Check that the progress bar was also sent to the console output
            progress_console_msgs = [_ for _ in rm_console.received_msgs if re.search("sim_motor:.+%.*", _["msg"])]
            if enable_progress_bar:
                assert len(progress_console_msgs) > 0
            else:
                assert len(progress_console_msgs) == 0

            # --------  Check that the waiting hook is pointing to the WatcherStreamManager object
            func_info = {"name": "unit_test_waiting_hook",
                         "item_type": "function",
                         "kwargs": {"device_progress_enabled": bool(stream_dev_progress_enabled)}}
            resp, _ = zmq_request(
                "function_execute",
                params={
                    "item": func_info,
                    "user": _user,
                    "user_group": _user_group,
                    "run_in_background": False,
                },
            )
            assert resp["success"] is True, pprint.pformat(resp)
            task_uid = resp["task_uid"]

            assert wait_for_task_result(10, task_uid)

            resp, _ = zmq_request("task_result", params={"task_uid": task_uid})
            result = resp["result"]["return_value"]
            assert result[0], result[1]
            # ---------------------------------------------------------------------------------------

        zmq_request("environment_close")
        assert wait_for_condition(time=3, condition=condition_environment_closed)

    finally:
        rm_console.stop()
        rm_info.stop()
        rm_console.join()
        rm_info.join()


_script_emit_log_messages = """

import logging

logging.getLogger("qs_log_test").setLevel(logging.INFO)
logging.getLogger("qs_log_test.child").setLevel(logging.DEBUG)

def unit_test_emit_log_messages():
    logging.getLogger("qs_log_test").debug("msg-1")
    logging.getLogger("qs_log_test").info("msg-2")
    logging.getLogger("qs_log_test.child").debug("msg-3")
    logging.getLogger("qs_log_test.child.sub").warning("msg-4")
    logging.getLogger("qs_log_test_other").error("msg-5")
"""


# fmt: off
@pytest.mark.parametrize("stream_loggers", [True, False])
# fmt: on
def test_zmq_info_streaming_loggers(tmp_path, re_manager_cmd, stream_loggers):  # noqa: F811
    """
    Test 0MQ streaming functionality: streaming of log messages from selected loggers
    of the worker environment.
    """
    pc_path = copy_default_profile_collection(tmp_path)
    append_code_to_last_startup_file(pc_path, additional_code=_script_emit_log_messages)

    address_info_server = "tcp://*:60621"
    address_info_client = "tcp://localhost:60621"

    params_server = [
        f"--zmq-info-addr={address_info_server}",
        "--zmq-publish-info=ON",
        f"--startup-dir={pc_path}",
    ]
    if stream_loggers:
        params_server.extend(["--zmq-stream-loggers", "qs_log_test", "qs_log_test.child"])

    rm_info = ReceiveMessages(
        receiver_class=ReceiveSystemInfo,
        zmq_subscribe_addr=address_info_client,
        zmq_topic=_default_zmq_info_topic,
        encoding=use_zmq_encoding_for_tests(),
    )
    msg_key = "log_message"

    try:
        rm_info.start()

        re_manager_cmd(params_server)

        zmq_request("environment_open")
        assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

        rm_info.clear()

        func_info = {"name": "unit_test_emit_log_messages", "item_type": "function"}
        resp, _ = zmq_request(
            "function_execute",
            params={"item": func_info, "user": _user, "user_group": _user_group, "run_in_background": False},
        )
        assert resp["success"] is True, pprint.pformat(resp)
        assert wait_for_task_result(10, resp["task_uid"])
        ttime.sleep(1)  # Allow the remaining messages to be delivered

        records = [_["msg"][msg_key] for _ in rm_info.filter_msgs(msg_key)]
        records = [_ for _ in records if _["name"].startswith("qs_log_test")]
        if stream_loggers:
            assert [(_["name"], _["level"], _["message"]) for _ in records] == [
                ("qs_log_test", "INFO", "msg-2"),
                ("qs_log_test.child", "DEBUG", "msg-3"),
                ("qs_log_test.child.sub", "WARNING", "msg-4"),
            ]
        else:
            assert records == []

        zmq_request("environment_close")
        assert wait_for_condition(time=3, condition=condition_environment_closed)

    finally:
        rm_info.stop()
        rm_info.join()


_script_bluesky_logger_info = """

import logging

logging.getLogger("bluesky").setLevel(logging.INFO)
"""


# fmt: off
@pytest.mark.parametrize("stream_loggers", [True, False])
# fmt: on
def test_zmq_info_streaming_loggers_bluesky(tmp_path, re_manager_cmd, stream_loggers):  # noqa: F811
    """
    Test 0MQ streaming functionality: the 'bluesky' logger is set to INFO in startup code and
    the messages generated by Run Engine while executing a plan are streamed.
    """
    pc_path = copy_default_profile_collection(tmp_path)
    append_code_to_last_startup_file(pc_path, additional_code=_script_bluesky_logger_info)

    address_info_server = "tcp://*:60621"
    address_info_client = "tcp://localhost:60621"

    params_server = [
        f"--zmq-info-addr={address_info_server}",
        "--zmq-publish-info=ON",
        f"--startup-dir={pc_path}",
    ]
    if stream_loggers:
        params_server.extend(["--zmq-stream-loggers", "bluesky"])

    rm_info = ReceiveMessages(
        receiver_class=ReceiveSystemInfo,
        zmq_subscribe_addr=address_info_client,
        zmq_topic=_default_zmq_info_topic,
        encoding=use_zmq_encoding_for_tests(),
    )
    msg_key = "log_message"

    try:
        rm_info.start()

        re_manager_cmd(params_server)

        zmq_request("environment_open")
        assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

        rm_info.clear()

        _plan = {"name": "count", "args": [["det1", "det2"]], "item_type": "plan"}
        resp1, _ = zmq_request("queue_item_add", {"item": _plan, "user": _user, "user_group": _user_group})
        assert resp1["success"] is True, f"resp={resp1}"

        resp2, _ = zmq_request("queue_start")
        assert resp2["success"] is True

        assert wait_for_condition(time=30, condition=condition_queue_processing_finished)
        ttime.sleep(1)  # Allow the remaining messages to be delivered

        records = [_["msg"][msg_key] for _ in rm_info.filter_msgs(msg_key)]
        if stream_loggers:
            print(f"records = {pprint.pformat(records)}")
            assert all(_["name"] == "bluesky" or _["name"].startswith("bluesky.") for _ in records)
            assert all(_["level"] in ("INFO", "WARNING", "ERROR", "CRITICAL") for _ in records)
            assert any(_["name"] == "bluesky" and _["message"].startswith("Executing plan") for _ in records)
            assert any(_["name"] == "bluesky.RE.state" for _ in records)
        else:
            assert records == []

        zmq_request("environment_close")
        assert wait_for_condition(time=3, condition=condition_environment_closed)

    finally:
        rm_info.stop()
        rm_info.join()
