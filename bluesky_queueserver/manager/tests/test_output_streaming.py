import multiprocessing
import pytest
import sys
import time as ttime
import threading

from bluesky_queueserver.manager.output_streaming import (
    ConsoleOutputStream,
    setup_console_output_redirection,
    PublishConsoleOutput,
    ReceiveConsoleOutput,
)


def test_ConsoleOutputStream_1():
    """
    Basic test for ``ConsoleOutputStream`` class
    """
    queue = multiprocessing.Queue()
    cos = ConsoleOutputStream(msg_queue=queue)

    msgs = ["message-one\n", "message-two\n", "message-three\n"]
    for msg in msgs:
        cos.write(msg)

    for msg in msgs:
        msg_in_queue = queue.get(timeout=1)  # Timeout is just in case
        assert isinstance(msg_in_queue["time"], float)
        assert msg_in_queue["msg"] == msg


@pytest.fixture
def sys_stdout_stderr_restore():
    """
    Saves and restores ``sys.stdout`` and ``sys.stderr`` in case they are overridden in the test.
    """
    # Save references to stdout and stderr
    _stdout = sys.stdout
    _stderr = sys.stderr

    # Now 'sys.stdout' and 'sys.stderr' can be overridden
    yield

    # Restore to the original
    sys.stdout = _stdout
    sys.stderr = _stderr


def test_setup_console_output_redirection_1(sys_stdout_stderr_restore):
    """
    Test for ``setup_console_output_redirection``.
    """
    queue = multiprocessing.Queue()
    setup_console_output_redirection(msg_queue=queue)

    msgs = ["message-one", "message-two", "message-three"]

    # Printing to redirected stream
    for msg in msgs:
        print(msg)

    # Note: print statement results in two messages:
    #      (1) printed text
    #      (2) automatically inserted "\n"
    for msg in msgs:
        # Message itself
        msg_in_queue = queue.get(timeout=1)  # Timeout is just in case
        assert isinstance(msg_in_queue["time"], float)
        assert msg_in_queue["msg"] == msg

        # "\n"
        msg_in_queue = queue.get(timeout=1)  # Timeout is just in case
        assert isinstance(msg_in_queue["time"], float)
        assert msg_in_queue["msg"] == "\n"


# fmt: off
@pytest.mark.parametrize("console_output_on, zmq_publish_on, period, timeout, n_timeouts", [
    (True, True, 0, None, 0),
    (True, False, 0, None, 0),
    (False, True, 0, None, 0),
    (False, False, 0, None, 0),
    (True, True, 0.5, None, 0),
    (True, True, 1.2, None, 3),
    (True, True, 1.2, 500, 6),  # Timeout is in ms (500 == 0.5 s)
    (True, True, 0.55, 500, 3),
])
# fmt: on
# TODO: this test may need to be changed to run more reliably on CI
@pytest.mark.xfail(reason="Test often fails when run on CI, but expected to pass locally")
def test_ReceiveConsoleOutput_1(capfd, console_output_on, zmq_publish_on, period, timeout, n_timeouts):
    """
    Tests for ``ReceiveConsoleOutput`` and ``PublishConsoleOutput``.
    """
    zmq_port = 61223  # Arbitrary port
    zmq_topic = "testing_topic"

    zmq_publish_addr = f"tcp://*:{zmq_port}"
    zmq_subscribe_addr = f"tcp://localhost:{zmq_port}"

    queue = multiprocessing.Queue()

    pco = PublishConsoleOutput(
        msg_queue=queue,
        console_output_on=console_output_on,
        zmq_publish_on=zmq_publish_on,
        zmq_publish_addr=zmq_publish_addr,
        zmq_topic=zmq_topic,
    )

    class ReceiveMessages(threading.Thread):
        def __init__(self, *, zmq_subscribe_addr, zmq_topic):
            super().__init__()
            self._rco = ReceiveConsoleOutput(zmq_subscribe_addr=zmq_subscribe_addr, zmq_topic=zmq_topic)
            self._exit = False
            self.received_msgs = []
            self.n_timeouts = 0

        def run(self):
            while True:
                try:
                    _ = {} if (timeout is None) else {"timeout": timeout}
                    msg = self._rco.recv(**_)
                    self.received_msgs.append(msg)
                except TimeoutError:
                    self.n_timeouts += 1
                if self._exit:
                    break

        def stop(self):
            self._exit = True

    rm = ReceiveMessages(zmq_subscribe_addr=zmq_subscribe_addr, zmq_topic=zmq_topic)

    pco.start()
    rm.start()

    msgs = ["message-one\n", "message-two\n", "message-three\n"]
    for msg in msgs:
        queue.put({"time": ttime.time(), "msg": msg})
        ttime.sleep(period)

    # Wait for all messages to be sent
    ttime.sleep(0.1)
    pco.stop()
    # Wait for all message to be received and for the publishing thread to exit.
    ttime.sleep(pco._polling_timeout * 2)

    rm.stop()
    rm.join()

    if zmq_publish_on:
        assert len(rm.received_msgs) == 3
        for msg_received, msg in zip(rm.received_msgs, msgs):
            assert isinstance(msg_received["time"], float)
            assert msg_received["msg"] == msg
    else:
        assert len(rm.received_msgs) == 0

    captured = capfd.readouterr()
    if console_output_on:
        for msg in msgs:
            assert msg in captured.out
    else:
        assert captured.out == ""

    # There is one extra timeout needed to exit the loop
    assert rm.n_timeouts == n_timeouts + 1
