import asyncio
import multiprocessing
import sys
import threading
import time as ttime

import pytest

from bluesky_queueserver.manager.output_streaming import (
    ConsoleOutputStream,
    PublishConsoleOutput,
    ReceiveConsoleOutput,
    ReceiveConsoleOutputAsync,
    setup_console_output_redirection,
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
@pytest.mark.parametrize("console_output_on, zmq_publish_on, sub, unsub, period, timeout, n_timeouts", [
    (True, True, False, False, 0, None, 0),
    (True, False, False, False, 0, None, 0),
    (False, True, False, False, 0, None, 0),
    (False, False, False, False, 0, None, 0),
    (True, True, False, False, 0.5, None, 0),
    (True, True, False, False, 1.2, None, 3),
    (True, True, False, False, 1.2, 500, 6),  # Timeout is in ms (500 == 0.5 s)
    (True, True, False, False, 0.55, 500, 3),
    (True, True, True, False, 0, None, 0),  # Explicitly subscribe
    (True, True, True, True, 0, None, 0),   # Explicitly subscribe, then unsubscribe
])
# fmt: on
# TODO: this test may need to be changed to run more reliably on CI
@pytest.mark.xfail(reason="Test often fails when run on CI, but expected to pass locally")
def test_ReceiveConsoleOutput_1(capfd, console_output_on, zmq_publish_on, sub, unsub, period, timeout, n_timeouts):
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

        def subscribe(self):
            self._rco.subscribe()

        def unsubscribe(self):
            self._rco.unsubscribe()

    rm = ReceiveMessages(zmq_subscribe_addr=zmq_subscribe_addr, zmq_topic=zmq_topic)

    pco.start()

    if sub:
        rm.subscribe()
    if unsub:
        rm.unsubscribe()
    if not sub and not unsub:
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

    if sub or unsub:
        rm.start()
        ttime.sleep(pco._polling_timeout * 2)

    rm.stop()
    rm.join()

    if zmq_publish_on and not unsub:
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


# fmt: off
@pytest.mark.parametrize("period", [0.5, 1, 1.5])
@pytest.mark.parametrize("cb_type", ["func", "coro"])
# fmt: on
# TODO: this test may need to be changed to run more reliably on CI
@pytest.mark.xfail(reason="Test often fails when run on CI, but expected to pass locally")
def test_ReceiveConsoleOutputAsync_1(period, cb_type):
    """
    Basic test for ``ReceiveConsoleOutputAsync``: send and receive 3 messages over 0MQ.
    Send messages with different period (to check if timeout is handled correctly) and
    tests with callbacks in the form of plain function and coroutine.
    """
    timeout = 1000  # Timeout used for waiting for incoming messages

    zmq_port = 61223  # Arbitrary port
    zmq_topic = "testing_topic"

    zmq_publish_addr = f"tcp://*:{zmq_port}"
    zmq_subscribe_addr = f"tcp://localhost:{zmq_port}"

    queue = multiprocessing.Queue()

    pco = PublishConsoleOutput(
        msg_queue=queue,
        console_output_on=True,
        zmq_publish_on=True,
        zmq_publish_addr=zmq_publish_addr,
        zmq_topic=zmq_topic,
    )

    pco.start()

    msgs = ["message-one\n", "message-two\n", "message-three\n"]

    rm = ReceiveConsoleOutputAsync(zmq_subscribe_addr=zmq_subscribe_addr, zmq_topic=zmq_topic, timeout=timeout)
    ttime.sleep(1)  # Important when executed on CI

    async def testing():
        nonlocal msgs
        msgs_received = []

        def cb_func(msg):
            nonlocal msgs_received
            msgs_received.append(msg)

        async def cb_coro(msg):
            nonlocal msgs_received
            msgs_received.append(msg)

        if cb_type == "func":
            cb = cb_func
        elif cb_type == "coro":
            cb = cb_coro
        else:
            raise RuntimeError(f"Unknown callback type: {cb_type!r}")

        rm.set_callback(cb=cb)

        for n in range(2):
            # ReceiveConsoleOutputAsync is expected to allow to start and stop acquisition multiple times.
            msgs_received.clear()
            rm.start()
            rm.start()  # Repeated attempts to start should have no effect

            for msg in msgs:
                queue.put({"time": ttime.time(), "msg": msg})
                await asyncio.sleep(1)

            rm.stop()

            # Wait for all messages to be sent. It happens almost instantly when tests are run
            #   locally, but there could be delays when running on CI.
            for _ in range(10):  # 10 seconds should be sufficient in the worst case
                await asyncio.sleep(1)
                if len(msgs_received) == 3:
                    break

            assert len(msgs_received) == 3, f"Attempt #{n + 1}"
            for msg_received, msg in zip(msgs_received, msgs):
                assert isinstance(msg_received["time"], float)
                assert msg_received["msg"] == msg

            # Send an extra message. Acquisition is stopped at this point
            queue.put({"time": ttime.time(), "msg": "Message is expected to be discarded"})
            await asyncio.sleep(1)

    asyncio.run(testing())

    pco.stop()


def test_ReceiveConsoleOutputAsync_2():
    """
    ``ReceiveConsoleOutputAsync``: test various options to subscribe and unsubscribe
    """

    zmq_port = 61223  # Arbitrary port
    zmq_topic = "testing_topic"
    zmq_subscribe_addr = f"tcp://localhost:{zmq_port}"

    rm = ReceiveConsoleOutputAsync(zmq_subscribe_addr=zmq_subscribe_addr, zmq_topic=zmq_topic, timeout=100)

    assert rm._socket_subscribed is False

    # Test 'subscribe()' and 'unsubscribe()'
    rm.subscribe()
    assert rm._socket_subscribed is True
    rm.unsubscribe()
    assert rm._socket_subscribed is False

    async def testing_1():
        rm.start()
        assert rm._socket_subscribed is True
        rm.stop()
        await asyncio.sleep(1)
        assert rm._socket_subscribed is False

    asyncio.run(testing_1())

    async def testing_2():
        rm.start()
        assert rm._socket_subscribed is True
        rm.stop(unsubscribe=False)
        await asyncio.sleep(1)
        assert rm._socket_subscribed is True
        rm.unsubscribe()
        assert rm._socket_subscribed is False

    asyncio.run(testing_2())

    async def testing_3():
        with pytest.raises(TimeoutError):
            await rm.recv()
        assert rm._socket_subscribed is True
        rm.unsubscribe()
        assert rm._socket_subscribed is False

    asyncio.run(testing_3())
