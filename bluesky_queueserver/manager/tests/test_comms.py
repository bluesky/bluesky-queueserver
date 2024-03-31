import asyncio
import json
import logging
import multiprocessing
import pprint
import threading
import time as ttime

import pytest
import zmq

from bluesky_queueserver.manager.comms import (
    CommJsonRpcError,
    CommTimeoutError,
    PipeJsonRpcReceive,
    PipeJsonRpcSendAsync,
    ZMQCommSendAsync,
    ZMQCommSendThreads,
    generate_zmq_keys,
    generate_zmq_public_key,
    validate_zmq_key,
)
from bluesky_queueserver.tests.common import format_jsonrpc_msg


def count_threads_with_name(name):
    """
    Returns the number of currently existing threads with the given name
    """
    n_count = 0
    for th in threading.enumerate():
        if th.name.startswith(name):
            n_count += 1
    return n_count


# =======================================================================
#                       Class CommJsonRpcError


def test_CommJsonRpcError_1():
    """
    Basic test for `CommJsonRpcError` class.
    """
    err_msg, err_code, err_type = "Some error occured", 25, "RuntimeError"
    ex = CommJsonRpcError(err_msg, err_code, err_type)

    assert ex.message == err_msg, "Message set incorrectly"
    assert ex.error_code == err_code, "Error code set incorrectly"
    assert ex.error_type == err_type, "Error type set incorrectly"


def test_CommJsonRpcError_2():
    """
    Basic test for `CommJsonRpcError` class.
    """
    err_msg, err_code, err_type = "Some error occured", 25, "RuntimeError"
    ex = CommJsonRpcError(err_msg, err_code, err_type)

    s = str(ex)
    assert err_msg in s, "Error message is not found in printed error message"
    assert str(err_code) in s, "Error code is not found in printed error message"
    assert err_type in s, "Error type is not found in printed error message"

    repr = f"CommJsonRpcError('{err_msg}', {err_code}, '{err_type}')"
    assert ex.__repr__() == repr, "Error representation is printed incorrectly"


def test_CommJsonRpcError_3_fail():
    """
    `CommJsonRpcError` class. Failing cases
    """
    err_msg, err_code, err_type = "Some error occured", 25, "RuntimeError"
    ex = CommJsonRpcError(err_msg, err_code, err_type)

    # The pattern 'can't set attribute': Python 3.10 and older
    # The pattern 'object has no setter': Python 3.11
    with pytest.raises(AttributeError, match="can't set attribute|object has no setter"):
        ex.message = err_msg
    with pytest.raises(AttributeError, match="can't set attribute|object has no setter"):
        ex.error_code = err_code
    with pytest.raises(AttributeError, match="can't set attribute|object has no setter"):
        ex.error_type = err_type


# =======================================================================
#                       Class PipeJsonRpcReceive


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
# fmt: on
def test_PipeJsonRpcReceive_1(use_json):
    """
    Create, start and stop `PipeJsonRpcReceive` object
    """
    conn1, conn2 = multiprocessing.Pipe()
    new_name = f"Unusual Thread Name ({use_json})"

    assert count_threads_with_name(new_name) == 0, "No threads are expected to exist"

    pc = PipeJsonRpcReceive(conn=conn2, name=new_name, use_json=use_json)
    pc.start()
    assert count_threads_with_name(new_name) == 2, "Two threads are expected to exist"

    pc.start()  # Expected to do nothing

    pc.stop()
    ttime.sleep(0.15)  # Wait until the thread stops (wait more than the 0.1s polling period)
    assert count_threads_with_name(new_name) == 0, "No threads are expected to exist"

    pc.start()  # Restart
    assert count_threads_with_name(new_name) == 2, "Two thread are expected to exist"
    pc.stop()


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
@pytest.mark.parametrize(
    "method, params, result, notification",
    [
        ("method1", [], 5, False),
        ("method1", [], 5, True),
        ("method1", {}, 5, False),
        ("method1", {}, 5, True),
        ("method2", [5], 15, False),
        ("method2", {"value": 5}, 15, False),
        ("method2", {}, 12, False),
        ("method3", {"value": 5}, 20, False),
        ("method3", {}, 18, False),
        ("method4", {"value": 5}, 20, False),
        ("method4", {}, 19, False),
    ],
)
# fmt: on
def test_PipeJsonRpcReceive_2(method, params, result, notification, use_json):
    """
    The case of single requests.
    """
    value_nonlocal = None

    def method_handler1():
        nonlocal value_nonlocal
        value_nonlocal = "function_was_called"
        return 5

    def method_handler2(value=2):
        nonlocal value_nonlocal
        value_nonlocal = "function_was_called"
        return value + 10

    def method_handler3(*, value=3):
        nonlocal value_nonlocal
        value_nonlocal = "function_was_called"
        return value + 15

    class SomeClass:
        def method_handler4(self, *, value=4):
            nonlocal value_nonlocal
            value_nonlocal = "function_was_called"
            return value + 15

    some_class = SomeClass()

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, use_json=use_json)
    pc.add_method(method_handler1, "method1")
    pc.add_method(method_handler2, "method2")
    pc.add_method(method_handler3, "method3")
    pc.add_method(some_class.method_handler4, "method4")
    pc.start()

    for n in range(3):
        value_nonlocal = None

        request = format_jsonrpc_msg(method, params, notification=notification)
        request_json = json.dumps(request) if use_json else request
        conn1.send(request_json)
        if conn1.poll(timeout=0.5):  # Set timeout large enough
            if not notification:
                response = conn1.recv()
                response = json.loads(response) if use_json else response
                assert response["id"] == request["id"], f"Response ID does not match message ID: {response}"
                assert "result" in response, f"Key 'result' is not contained in response: {response}"
                assert response["result"] == result, f"Result does not match the expected: {response}"
                assert value_nonlocal == "function_was_called", "Non-local variable has incorrect value"
            else:
                assert False, "Notification was sent but response was received."
        else:
            # If request is a notification, there should be no response.
            if not notification:
                assert False, "Timeout occurred while waiting for response."
            else:
                assert value_nonlocal == "function_was_called", "Non-local variable has incorrect value"

    pc.stop()


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
# fmt: on
def test_PipeJsonRpcReceive_3(use_json):
    """
    Test sending multiple requests
    """

    def method_handler3(*, value=3):
        return value + 15

    class SomeClass:
        def method_handler4(self, *, value=4):
            return value + 7

    some_class = SomeClass()

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, use_json=use_json)
    pc.add_method(method_handler3, "method3")
    pc.add_method(some_class.method_handler4, "method4")
    pc.start()

    # Non-existing method ('Method not found' error)
    request = [
        format_jsonrpc_msg("method3", {"value": 7}),
        format_jsonrpc_msg("method4", {"value": 3}, notification=True),
        format_jsonrpc_msg("method4", {"value": 9}),
    ]
    request_json = json.dumps(request) if use_json else request
    conn1.send(request_json)

    if conn1.poll(timeout=0.5):  # Set timeout large enough
        response = conn1.recv()
        response = json.loads(response) if use_json else response
        assert len(response) == 2, "Unexpected number of response messages"
        assert response[0]["id"] == request[0]["id"], "Response ID does not match message ID."
        assert response[1]["id"] == request[2]["id"], "Response ID does not match message ID."
        assert response[0]["result"] == 22, "Response ID does not match message ID."
        assert response[1]["result"] == 16, "Response ID does not match message ID."
    else:
        assert False, "Timeout occurred while waiting for response."

    pc.stop()


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
# fmt: on
def test_PipeJsonRpcReceive_4(use_json):
    """
    Test if all outdated unprocessed messages are deleted from the pipe
    as the processing thread is started.
    """

    def method_handler3(*, value=3):
        return value + 15

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, use_json=use_json)
    pc.add_method(method_handler3, "method3")

    # The thread is not started yet, but we still send a message through the pipe.
    #   This message is expected to be deleted once the thread starts.
    request1a = [
        format_jsonrpc_msg("method3", {"value": 5}),
    ]
    request1b = [
        format_jsonrpc_msg("method3", {"value": 6}),
    ]
    request1a_json = json.dumps(request1a) if use_json else request1a
    request1b_json = json.dumps(request1b) if use_json else request1b
    conn1.send(request1a_json)
    conn1.send(request1b_json)

    # Start the processing thread. The messages that were already sent are expected to be ignored.
    pc.start()

    # Send another message. This message is expected to be processed.
    request2 = [
        format_jsonrpc_msg("method3", {"value": 7}),
    ]
    request2_json = json.dumps(request2) if use_json else request2
    conn1.send(request2_json)

    if conn1.poll(timeout=0.5):  # Set timeout large enough
        response = conn1.recv()
        response = json.loads(response) if use_json else response
        assert len(response) == 1, "Unexpected number of response messages"
        assert response[0]["id"] == request2[0]["id"], "Response ID does not match message ID."
        assert response[0]["result"] == 22, "Response ID does not match message ID."
    else:
        assert False, "Timeout occurred while waiting for response."

    pc.stop()


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
@pytest.mark.parametrize("clear_buffer", [False, True])
# fmt: on
def test_PipeJsonRpcReceive_5(clear_buffer, use_json):
    """
    Checking that the buffer overflow does not overflow the pipe.
    """
    n_calls = 0
    start_processing = False

    def method_handler3():
        nonlocal n_calls
        while not start_processing:
            ttime.sleep(0.05)
        n_calls += 1

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, use_json=use_json)
    pc.add_method(method_handler3, "method3")
    pc.start()

    # Non-existing method ('Method not found' error)
    request = format_jsonrpc_msg("method3")

    n_buf = pc._msg_recv_buffer_size

    request_json = json.dumps(request) if use_json else request
    conn1.send(request_json)
    ttime.sleep(1)
    for _ in range(n_buf * 2):
        conn1.send(request_json)

    ttime.sleep(1)

    if clear_buffer:
        ttime.sleep(1)
        pc.clear_buffer()

    start_processing = True

    ttime.sleep(1)
    assert n_calls == (1 if clear_buffer else (n_buf + 1))

    pc.stop()


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
# fmt: on
def test_PipeJsonRpcReceive_6_failing(use_json):
    """
    Those tests are a result of exploration of how `json-rpc` error processing works.
    """

    def method_handler3(*, value=3):
        return value + 15

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, use_json=use_json)
    pc.add_method(method_handler3, "method3")
    pc.start()

    # ------- Incorrect argument (arg list instead of required kwargs) -------
    #   Returns 'Server Error' (-32000)
    request = format_jsonrpc_msg("method3", [5])
    request_json = json.dumps(request) if use_json else request
    conn1.send(request_json)

    if conn1.poll(timeout=0.5):  # Set timeout large enough
        response = conn1.recv()
        response = json.loads(response) if use_json else response
        assert response["error"]["code"] == -32602, f"Incorrect error reported: {response}"
        assert response["error"]["data"]["type"] == "TypeError", "Incorrect error type."
    else:
        assert False, "Timeout occurred while waiting for response."

    # ------- Incorrect argument (incorrect argument type) -------
    # 'json-prc' doesn't check parameter types. Instead the handler will crash if the argument
    #   type is not suitable.
    request = format_jsonrpc_msg("method3", {"value": "abc"})
    request_json = json.dumps(request) if use_json else request
    conn1.send(request_json)

    if conn1.poll(timeout=0.5):
        response = conn1.recv()
        response = json.loads(response) if use_json else response
        assert response["error"]["code"] == -32000, f"Incorrect error reported: {response}"
        assert response["error"]["data"]["type"] == "TypeError", "Incorrect error type."
    else:
        assert False, "Timeout occurred while waiting for response."

    # ------- Incorrect argument (extra argument) -------
    request = format_jsonrpc_msg("method3", {"value": 5, "unknown": 10})
    request_json = json.dumps(request) if use_json else request
    conn1.send(request_json)

    if conn1.poll(timeout=0.5):
        response = conn1.recv()
        response = json.loads(response) if use_json else response
        assert response["error"]["code"] == -32602, f"Incorrect error reported: {response}"
    else:
        assert False, "Timeout occurred while waiting for response."

    # ------- Non-existing method ('Method not found' error) -------
    request = format_jsonrpc_msg("method_handler3", {"value": 5})
    request_json = json.dumps(request) if use_json else request
    conn1.send(request_json)

    if conn1.poll(timeout=0.5):
        response = conn1.recv()
        response = json.loads(response) if use_json else response
        assert response["error"]["code"] == -32601, f"Incorrect error reported: {response}"
    else:
        assert False, "Timeout occurred while waiting for response."

    pc.stop()


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
# fmt: on
def test_PipeJsonRpcReceive_7_failing(use_json):
    """
    Exception is raised inside the handler is causing 'Server Error' -32000.
    Returns error type (Exception type) and message. It is questionable whether
    built-in exception processing should be used to process exceptions within handlers.
    It may be better to catch and process all exceptions produced by the custom code and
    leave built-in processing for exceptions that happen while calling the function.

    EXCEPTIONS SHOULD BE PROCESSED INSIDE THE HANDLER!!!
    """

    def method_handler5():
        raise RuntimeError("Function crashed ...")

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, use_json=use_json)
    pc.add_method(method_handler5, "method5")
    pc.start()

    request = format_jsonrpc_msg("method5")
    request_json = json.dumps(request) if use_json else request
    conn1.send(request_json)

    if conn1.poll(timeout=0.5):  # Set timeout large enough
        response = conn1.recv()
        response = json.loads(response) if use_json else response
        assert response["error"]["code"] == -32000, f"Incorrect error reported: {response}"
        assert response["error"]["data"]["type"] == "RuntimeError", "Incorrect error type."
        assert response["error"]["data"]["message"] == "Function crashed ...", "Incorrect message."
    else:
        assert False, "Timeout occurred while waiting for response."

    pc.stop()


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
# fmt: on
def test_PipeJsonRpcReceive_8_failing(use_json):
    """
    This is an example of handler to timeout. 'json-rpc' package can not handle timeouts.
    Care must be taken to write handles that execute quickly. Timeouts must be handled
    explicitly within the handlers.

    ONLY INSTANTLY EXECUTED HANDLERS MAY BE USED!!!
    """

    def method_handler6():
        ttime.sleep(3)  # Longer than 'poll' timeout

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, use_json=use_json)
    pc.add_method(method_handler6, "method6")
    pc.start()

    # Non-existing method ('Method not found' error)
    request = format_jsonrpc_msg("method6")
    request_json = json.dumps(request) if use_json else request
    conn1.send(request_json)

    if conn1.poll(timeout=0.5):  # Set timeout large enough
        assert False, "The test is expected to timeout."
    else:
        pass

    pc.stop()


# =======================================================================
#                       Class PipeJsonRpcSendAsync


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
# fmt: on
def test_PipeJsonRpcSendAsync_1(use_json):
    """
    Create, start and stop `PipeJsonRpcReceive` object
    """
    conn1, conn2 = multiprocessing.Pipe()
    new_name = f"Unusual Thread Name ({use_json})"

    async def object_start_stop():
        assert count_threads_with_name(new_name) == 0, "No threads are expected to exist"

        pc = PipeJsonRpcSendAsync(conn=conn1, name=new_name)
        pc.start()
        assert count_threads_with_name(new_name) == 2, "One thread is expected to exist"

        pc.start()  # Expected to do nothing

        pc.stop()
        ttime.sleep(0.15)  # Wait until the thread stops (0.1s polling period)
        assert count_threads_with_name(new_name) == 0, "No threads are expected to exist"

        pc.start()  # Restart
        assert count_threads_with_name(new_name) == 2, "One thread is expected to exist"
        pc.stop()

    asyncio.run(object_start_stop())


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
@pytest.mark.parametrize(
    "method, params, result, notification",
    [
        ("method1", [], 5, False),
        ("method1", [], 5, True),
        ("method1", {}, 5, False),
        ("method1", {}, 5, True),
        ("method2", [5], 15, False),
        ("method2", {"value": 5}, 15, False),
        ("method2", {}, 12, False),
        ("method3", {"value": 5}, 20, False),
        ("method3", {}, 18, False),
        ("method4", {"value": 5}, 20, False),
        ("method4", {}, 19, False),
    ],
)
# fmt: on
def test_PipeJsonRpcSendAsync_2(method, params, result, notification, use_json):
    """
    Test of basic functionality. Here we don't test for timeout case (it raises an exception).
    """
    value_nonlocal = None

    def method_handler1():
        nonlocal value_nonlocal
        value_nonlocal = "function_was_called"
        return 5

    def method_handler2(value=2):
        nonlocal value_nonlocal
        value_nonlocal = "function_was_called"
        return value + 10

    def method_handler3(*, value=3):
        nonlocal value_nonlocal
        value_nonlocal = "function_was_called"
        return value + 15

    class SomeClass:
        def method_handler4(self, *, value=4):
            nonlocal value_nonlocal
            value_nonlocal = "function_was_called"
            return value + 15

    some_class = SomeClass()

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, name="comm-server", use_json=use_json)
    pc.add_method(method_handler1, "method1")
    pc.add_method(method_handler2, "method2")
    pc.add_method(method_handler3, "method3")
    pc.add_method(some_class.method_handler4, "method4")
    pc.start()

    async def send_messages():
        nonlocal value_nonlocal

        p_send = PipeJsonRpcSendAsync(conn=conn1, name="comm-client", use_json=use_json)
        p_send.start()

        for n in range(3):
            value_nonlocal = None

            response = await p_send.send_msg(method, params, notification=notification)
            if not notification:
                assert response == result, f"Result does not match the expected: {response}"
                assert value_nonlocal == "function_was_called", "Non-local variable has incorrect value"
            elif response is not None:
                assert False, "Response was received for notification."

        p_send.stop()

    asyncio.run(send_messages())
    pc.stop()


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
# fmt: on
def test_PipeJsonRpcSendAsync_3(use_json):
    """
    Put multiple messages to the loop at once. The should be processed one by one.
    """
    n_calls = 0
    lock = threading.Lock()

    def method_handler1():
        nonlocal n_calls
        with lock:
            n_calls += 1
            n_return = n_calls
        ttime.sleep(0.1)
        return n_return

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, name="comm-server", use_json=use_json)
    pc.add_method(method_handler1, "method1")
    pc.start()

    async def send_messages():
        p_send = PipeJsonRpcSendAsync(conn=conn1, name="comm-client", use_json=use_json)
        p_send.start()

        # Submit multiple messages at once. Messages should stay at the event loop
        #   and be processed one by one.
        futs = []
        for n in range(5):
            futs.append(asyncio.ensure_future(p_send.send_msg("method1")))

        for n, fut in enumerate(futs):
            await asyncio.wait_for(fut, timeout=5.0)  # Timeout is in case of failure
            result = fut.result()
            assert result == n + 1, "Incorrect returned value"

        p_send.stop()

    asyncio.run(send_messages())
    pc.stop()


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
# fmt: on
def test_PipeJsonRpcSendAsync_4(use_json):
    """
    Message timeout.
    """

    def method_handler1():
        ttime.sleep(1)

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, name="comm-server", use_json=use_json)
    pc.add_method(method_handler1, "method1")
    pc.start()

    async def send_messages():
        p_send = PipeJsonRpcSendAsync(conn=conn1, name="comm-client", use_json=use_json)
        p_send.start()

        # Submit multiple messages at once. Messages should stay at the event loop
        #   and be processed one by one.
        with pytest.raises(CommTimeoutError, match="Timeout while waiting for response to message"):
            await p_send.send_msg("method1", timeout=0.5)

        p_send.stop()

    asyncio.run(send_messages())
    pc.stop()


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
# fmt: on
def test_PipeJsonRpcSendAsync_5(use_json):
    """
    Special test case.
    Two messages: the first message times out, the second message is send before the response
    from the first message is received. Verify that the result returned in response to the
    second message is received. (We discard the result of the message that is timed out.)
    """

    def method_handler1():
        ttime.sleep(0.7)
        return 39

    def method_handler2():
        ttime.sleep(0.2)
        return 56

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, name="comm-server", use_json=use_json)
    pc.add_method(method_handler1, "method1")
    pc.add_method(method_handler2, "method2")
    pc.start()

    async def send_messages():
        p_send = PipeJsonRpcSendAsync(conn=conn1, name="comm-client", use_json=use_json)
        p_send.start()

        # Submit multiple messages at once. Messages should stay at the event loop
        #   and be processed one by one.
        with pytest.raises(CommTimeoutError):
            await p_send.send_msg("method1", timeout=0.5)

        result = await p_send.send_msg("method2", timeout=0.5)
        assert result == 56, "Incorrect result received"

        p_send.stop()

    asyncio.run(send_messages())
    pc.stop()


class _PipeJsonRpcReceiveTest(PipeJsonRpcReceive):
    """
    Object that responds to a single request with multiple duplicates of the message.
    The test emulates possible issues with the process receiving messages.
    """

    def _handle_msg(self, msg):
        response = self._response_manager.handle(msg)
        if response:
            self._conn.send(response)  # Send the response 3 times !!!
            self._conn.send(response)
            self._conn.send(response)


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
# fmt: on
def test_PipeJsonRpcSendAsync_6(caplog, use_json):
    """
    Special test case.
    The receiving process responds with multiple replies (3) to a single request. Check that
    only one (the first) message is processed and the following messages are ignored.
    """

    caplog.set_level(logging.INFO)

    def method_handler1():
        return 39

    conn1, conn2 = multiprocessing.Pipe()
    pc = _PipeJsonRpcReceiveTest(conn=conn2, name="comm-server", use_json=use_json)
    pc.add_method(method_handler1, "method1")
    pc.start()

    async def send_messages():
        p_send = PipeJsonRpcSendAsync(conn=conn1, name="comm-client", use_json=use_json)
        p_send.start()

        result = await p_send.send_msg("method1", timeout=0.5)
        assert result == 39, "Incorrect result received"

        await asyncio.sleep(1)  # Wait until additional messages are sent and received

        p_send.stop()

    asyncio.run(send_messages())
    pc.stop()

    txt = "Unexpected message received"
    assert txt in caplog.text
    assert caplog.text.count(txt) == 2, caplog.text


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
# fmt: on
def test_PipeJsonRpcSendAsync_7_fail(use_json):
    """
    Exception raised inside the method.
    """

    def method_handler1():
        raise RuntimeError("Function crashed ...")

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, name="comm-server", use_json=use_json)
    pc.add_method(method_handler1, "method1")
    pc.start()

    async def send_messages():
        p_send = PipeJsonRpcSendAsync(conn=conn1, name="comm-client", use_json=use_json)
        p_send.start()

        # Submit multiple messages at once. Messages should stay at the event loop
        #   and be processed one by one.
        with pytest.raises(CommJsonRpcError, match="Function crashed ..."):
            await p_send.send_msg("method1", timeout=0.5)

        p_send.stop()

    asyncio.run(send_messages())
    pc.stop()


# fmt: off
@pytest.mark.parametrize("use_json", [False, True])
# fmt: on
def test_PipeJsonRpcSendAsync_8_fail(use_json):
    """
    Method not found (other `json-rpc` errors will raise the same exception).
    """

    def method_handler1():
        pass

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, name="comm-server", use_json=use_json)
    pc.add_method(method_handler1, "method1")
    pc.start()

    async def send_messages():
        p_send = PipeJsonRpcSendAsync(conn=conn1, name="comm-client", use_json=use_json)
        p_send.start()

        # Submit multiple messages at once. Messages should stay at the event loop
        #   and be processed one by one.
        with pytest.raises(CommJsonRpcError):
            await p_send.send_msg("nonexisting_method", timeout=0.5)

        p_send.stop()

    asyncio.run(send_messages())
    pc.stop()


# =======================================================================
#                               ZMQ keys


def test_generate_zmq_keys():
    """
    Functions ``generate_zmq_keys()``, ``generate_zmq_public_key()`` and ``validate_zmq_key()``.
    """
    key_public, key_private = generate_zmq_keys()
    assert isinstance(key_public, str)
    assert len(key_public) == 40
    assert isinstance(key_private, str)
    assert len(key_private) == 40

    validate_zmq_key(key_public)
    validate_zmq_key(key_private)

    key_public_gen = generate_zmq_public_key(key_private)
    assert key_public_gen == key_public


# fmt: off
@pytest.mark.parametrize("key", [
    None,
    10,
    "",
    "abc",
    "wt8[6a8eoXFRVL<l2JBbOzs(hcI%kRBIr0Do/eL'",  # 40 characters, but contains invalid character "'"
])
# fmt: on
def test_validate_zmq_key(key):
    """
    Function ``validate_zmq_key()``: cases of failing validation.
    """
    with pytest.raises(ValueError, match="the key must be a 40 byte z85 encoded string"):
        validate_zmq_key(key)


# =======================================================================
#                       Class ZMQCommSendThreads


def _gen_server_keys(*, encryption_enabled):
    """Generate server keys and a set of kwargs."""
    if encryption_enabled:
        public_key, private_key = generate_zmq_keys()
        server_kwargs = {"private_key": private_key}
    else:
        public_key, private_key = None, None
        server_kwargs = {}
    return public_key, private_key, server_kwargs


def _zmq_server_1msg(*, private_key=None):
    """
    ZMQ server that provides single response.
    ``private_key`` - server private key (for tests with enabled encryption)
    """
    ctx = zmq.Context()
    zmq_socket = ctx.socket(zmq.REP)

    if private_key is not None:
        zmq_socket.set(zmq.CURVE_SERVER, 1)
        zmq_socket.set(zmq.CURVE_SECRETKEY, private_key.encode("utf-8"))

    zmq_socket.bind("tcp://*:60615")
    msg_in = zmq_socket.recv_json()
    msg_out = {"success": True, "some_data": 10, "msg_in": msg_in}
    zmq_socket.send_json(msg_out)
    zmq_socket.close(linger=10)


# fmt: off
@pytest.mark.parametrize("encryption_enabled", [False, True])
@pytest.mark.parametrize("is_blocking", [True, False])
# fmt: on
def test_ZMQCommSendThreads_1(is_blocking, encryption_enabled):
    """
    Basic test of ZMQCommSendThreads class: single communication with the
    server both in blocking and non-blocking mode.
    """
    public_key, _, server_kwargs = _gen_server_keys(encryption_enabled=encryption_enabled)

    thread = threading.Thread(target=_zmq_server_1msg, kwargs=server_kwargs)
    thread.start()

    zmq_comm = ZMQCommSendThreads(server_public_key=public_key if encryption_enabled else None)
    method, params = "testing", {"p1": 10, "p2": "abc"}

    msg_recv, msg_recv_err = {}, ""

    if is_blocking:
        msg_recv = zmq_comm.send_message(method=method, params=params)
    else:
        done = False

        def cb(msg, msg_err):
            nonlocal msg_recv, msg_recv_err, done
            msg_recv = msg
            msg_recv_err = msg_err
            done = True

        zmq_comm.send_message(method=method, params=params, cb=cb)
        # Implement primitive polling of 'done' flag
        while not done:
            ttime.sleep(0.1)

    assert msg_recv["success"] is True, str(msg_recv)
    assert msg_recv["some_data"] == 10, str(msg_recv)
    assert msg_recv["msg_in"] == {"method": method, "params": params}, str(msg_recv)
    assert msg_recv_err == ""

    thread.join()


def _zmq_server_2msg(*, private_key=None):
    """
    ZMQ server: provides ability to communicate twice.
    ``private_key`` - server private key (for tests with enabled encryption)
    """
    ctx = zmq.Context()
    zmq_socket = ctx.socket(zmq.REP)

    if private_key is not None:
        zmq_socket.set(zmq.CURVE_SERVER, 1)
        zmq_socket.set(zmq.CURVE_SECRETKEY, private_key.encode("utf-8"))

    zmq_socket.bind("tcp://*:60615")
    msg_in = zmq_socket.recv_json()
    msg_out = {"success": True, "some_data": 10, "msg_in": msg_in}
    zmq_socket.send_json(msg_out)
    msg_in = zmq_socket.recv_json()
    msg_out = {"success": True, "some_data": 20, "msg_in": msg_in}
    zmq_socket.send_json(msg_out)
    zmq_socket.close(linger=10)


# fmt: off
@pytest.mark.parametrize("encryption_enabled", [False, True])
@pytest.mark.parametrize("is_blocking", [True, False])
# fmt: on
def test_ZMQCommSendThreads_2(is_blocking, encryption_enabled):
    """
    Basic test of ZMQCommSendThreads class: two consecutive communications with the
    server both in blocking and non-blocking mode.
    """
    public_key, _, server_kwargs = _gen_server_keys(encryption_enabled=encryption_enabled)

    thread = threading.Thread(target=_zmq_server_2msg, kwargs=server_kwargs)
    thread.start()

    zmq_comm = ZMQCommSendThreads(server_public_key=public_key if encryption_enabled else None)
    method, params = "testing", {"p1": 10, "p2": "abc"}

    msg_recv, msg_recv_err = {}, ""

    for val in (10, 20):
        if is_blocking:
            msg_recv = zmq_comm.send_message(method=method, params=params)
        else:
            done = False

            def cb(msg, msg_err):
                nonlocal msg_recv, msg_recv_err, done
                msg_recv = msg
                msg_recv_err = msg_err
                done = True

            zmq_comm.send_message(method=method, params=params, cb=cb)
            # Implement primitive polling of 'done' flag
            while not done:
                ttime.sleep(0.1)

        assert msg_recv["success"] is True, str(msg_recv)
        assert msg_recv["some_data"] == val, str(msg_recv)
        assert msg_recv["msg_in"] == {"method": method, "params": params}, str(msg_recv)
        assert msg_recv_err == ""

    thread.join()


def _zmq_server_2msg_delay1(*, private_key=None):
    """
    ZMQ server: provides ability to communicate twice, short delay before sending the 1st response
    ``private_key`` - server private key (for tests with enabled encryption)
    """
    ctx = zmq.Context()
    zmq_socket = ctx.socket(zmq.REP)

    if private_key is not None:
        zmq_socket.set(zmq.CURVE_SERVER, 1)
        zmq_socket.set(zmq.CURVE_SECRETKEY, private_key.encode("utf-8"))

    zmq_socket.bind("tcp://*:60615")
    msg_in = zmq_socket.recv_json()
    ttime.sleep(0.1)  # Delay before the 1st response
    msg_out = {"success": True, "some_data": 10, "msg_in": msg_in}
    zmq_socket.send_json(msg_out)
    msg_in = zmq_socket.recv_json()
    msg_out = {"success": True, "some_data": 20, "msg_in": msg_in}
    zmq_socket.send_json(msg_out)
    zmq_socket.close(linger=10)


# fmt: off
@pytest.mark.parametrize("encryption_enabled", [False, True])
@pytest.mark.parametrize("is_blocking", [True, False])
# fmt: on
def test_ZMQCommSendThreads_3(is_blocking, encryption_enabled):
    """
    Testing protection of '_zmq_communicate` with lock. In this test the function
    ``send_message` is called twice so that the second call is submitted before
    the response to the first message is received. The server waits for 0.1 seconds
    before responding to the 1st message to emulate delay in processing. Since
    ``_zmq_communicate`` is protected by a lock, the second request will not
    be sent until the first message is processed.
    """
    public_key, _, server_kwargs = _gen_server_keys(encryption_enabled=encryption_enabled)

    thread = threading.Thread(target=_zmq_server_2msg_delay1, kwargs=server_kwargs)
    thread.start()

    zmq_comm = ZMQCommSendThreads(server_public_key=public_key if encryption_enabled else None)
    method, params = "testing", {"p1": 10, "p2": "abc"}

    msg_recv, msg_recv_err = [], []
    n_done = 0

    def cb(msg, msg_err):
        nonlocal msg_recv, msg_recv_err, n_done
        msg_recv.append(msg)
        msg_recv_err.append(msg_err)
        n_done += 1

    vals = (10, 20)

    if is_blocking:
        # In case of blocking call the lock can only be tested using threads
        def thread_request():
            nonlocal msg_recv, msg_recv_err, n_done
            _ = zmq_comm.send_message(method=method, params=params)
            msg_recv.append(_)
            msg_recv_err.append("")
            n_done += 1

        th_request = threading.Thread(target=thread_request)
        th_request.start()

        # Call the same function in the main thread (send 2nd message to the server)
        thread_request()

        th_request.join()

    else:
        for n in range(len(vals)):
            zmq_comm.send_message(method=method, params=params, cb=cb)

    while n_done < 2:
        ttime.sleep(0.1)

    for n, val in enumerate(vals):
        assert msg_recv[n]["success"] is True, str(msg_recv)
        assert msg_recv[n]["some_data"] == val, str(msg_recv)
        assert msg_recv[n]["msg_in"] == {"method": method, "params": params}, str(msg_recv)
        assert msg_recv_err[n] == ""

    thread.join()


def _zmq_server_delay2(*, private_key=None):
    """
    ZMQ server: provides ability to communicate twice, long delay before sending the 1st response
    ``private_key`` - server private key (for tests with enabled encryption)
    """
    ctx = zmq.Context()
    zmq_socket = ctx.socket(zmq.REP)

    if private_key is not None:
        zmq_socket.set(zmq.CURVE_SERVER, 1)
        zmq_socket.set(zmq.CURVE_SECRETKEY, private_key.encode("utf-8"))

    zmq_socket.bind("tcp://*:60615")
    msg_in = zmq_socket.recv_json()
    ttime.sleep(3)  # Generate timeout at the client
    msg_out = {"success": True, "some_data": 10, "msg_in": msg_in}
    zmq_socket.send_json(msg_out)
    msg_in = zmq_socket.recv_json()
    msg_out = {"success": True, "some_data": 20, "msg_in": msg_in}
    zmq_socket.send_json(msg_out)
    zmq_socket.close(linger=10)


# fmt: off
@pytest.mark.parametrize("encryption_enabled", [False, True])
@pytest.mark.parametrize(
    "is_blocking, raise_exception",
    [(True, False),  # Repeated test are intentional
     (True, False),
     (True, None),
     (True, True),
     (False, False),
     (False, False)]
)
@pytest.mark.parametrize("delay_between_reads", [2, 0.1])
# fmt: on
def test_ZMQCommSendThreads_4(is_blocking, raise_exception, delay_between_reads, encryption_enabled):
    """
    ZMQCommSendThreads: Timeout at the server.
    """
    public_key, _, server_kwargs = _gen_server_keys(encryption_enabled=encryption_enabled)

    thread = threading.Thread(target=_zmq_server_delay2, kwargs=server_kwargs)
    thread.start()

    zmq_comm = ZMQCommSendThreads(server_public_key=public_key if encryption_enabled else None)
    method, params = "testing", {"p1": 10, "p2": "abc"}

    msg_recv, msg_recv_err = {}, ""

    for val in (10, 20):
        if is_blocking:
            if (raise_exception in (True, None)) and (val == 10):
                # The case when timeout is expected for blocking operation
                with pytest.raises(CommTimeoutError, match="timeout occurred"):
                    zmq_comm.send_message(method=method, params=params, raise_exceptions=raise_exception)
            else:
                msg_recv = zmq_comm.send_message(method=method, params=params, raise_exceptions=raise_exception)
        else:
            done = False

            def cb(msg, msg_err):
                nonlocal msg_recv, msg_recv_err, done
                msg_recv = msg
                msg_recv_err = msg_err
                done = True

            zmq_comm.send_message(method=method, params=params, cb=cb, raise_exceptions=raise_exception)

            # Implement primitive polling of 'done' flag
            while not done:
                ttime.sleep(0.1)

        if val == 10:
            if is_blocking:
                if raise_exception not in (True, None):
                    assert msg_recv["success"] is False, str(msg_recv)
                    assert "timeout occurred" in msg_recv["msg"], str(msg_recv)
            else:
                assert msg_recv == {}
                assert "timeout occurred" in msg_recv_err

            # Delay between consecutive reads. Test cases when read is initiated before and
            #   after the server restored operation and sent the response.
            ttime.sleep(delay_between_reads)

        else:
            assert msg_recv["success"] is True, str(msg_recv)
            assert msg_recv["some_data"] == val, str(msg_recv)
            assert msg_recv["msg_in"] == {"method": method, "params": params}, str(msg_recv)
            assert msg_recv_err == ""

    thread.join()


def _zmq_server_delay3(delay):
    """
    ZMQ server: delay of 3 seconds before the response
    """
    ctx = zmq.Context()
    zmq_socket = ctx.socket(zmq.REP)
    zmq_socket.bind("tcp://*:60615")
    msg_in = zmq_socket.recv_json()
    ttime.sleep(delay)  # Generate timeout at the client
    msg_out = {"success": True, "msg_in": msg_in}
    zmq_socket.send_json(msg_out)
    zmq_socket.close(linger=10)


# fmt: off
@pytest.mark.parametrize("timeout", [None, 2000, 5000])
# fmt: on
def test_ZMQCommSendThreads_5(timeout):
    """
    ZMQCommSendThreads: Pass timeout as 'send_message' parameter.
    """
    delay = 3000
    thread = threading.Thread(target=_zmq_server_delay3, kwargs={"delay": delay / 1000})
    thread.start()

    timeout_default = 2000
    zmq_comm = ZMQCommSendThreads(timeout_recv=timeout_default)
    method, params = "testing", {"p1": 10, "p2": "abc"}

    timeout_used = timeout or timeout_default

    if timeout_used < delay:
        with pytest.raises(CommTimeoutError, match="timeout occurred"):
            zmq_comm.send_message(method=method, params=params, timeout=timeout, raise_exceptions=True)
    else:
        msg_recv = zmq_comm.send_message(method=method, params=params, timeout=timeout, raise_exceptions=True)
        assert msg_recv["success"] is True, pprint.pformat(msg_recv)

    thread.join()


def test_ZMQCommSendThreads_6_fail():
    """
    Invalid public key
    """
    with pytest.raises(ValueError):
        ZMQCommSendThreads(server_public_key="abc")


# =======================================================================
#                       Class ZMQCommSendAsync


# fmt: off
@pytest.mark.parametrize("encryption_enabled", [False, True])
# fmt: on
def test_ZMQCommSendAsync_1(encryption_enabled):
    """
    Basic test of ZMQCommSendAsync class: send one message to the server and receive response.
    """
    public_key, _, server_kwargs = _gen_server_keys(encryption_enabled=encryption_enabled)

    thread = threading.Thread(target=_zmq_server_1msg, kwargs=server_kwargs)
    thread.start()

    async def testing():
        zmq_comm = ZMQCommSendAsync(server_public_key=public_key if encryption_enabled else None)
        method, params = "testing", {"p1": 10, "p2": "abc"}

        msg_recv = await zmq_comm.send_message(method=method, params=params)

        assert msg_recv["success"] is True, str(msg_recv)
        assert msg_recv["some_data"] == 10, str(msg_recv)
        assert msg_recv["msg_in"] == {"method": method, "params": params}, str(msg_recv)

    asyncio.run(testing())

    thread.join()


# fmt: off
@pytest.mark.parametrize("encryption_enabled", [False, True])
# fmt: on
def test_ZMQCommSendAsync_2(encryption_enabled):
    """
    Basic test of ZMQCommSendAsync class: two consecutive communications with the
    server both in blocking and non-blocking mode.
    """
    public_key, _, server_kwargs = _gen_server_keys(encryption_enabled=encryption_enabled)

    thread = threading.Thread(target=_zmq_server_2msg, kwargs=server_kwargs)
    thread.start()

    async def testing():
        zmq_comm = ZMQCommSendAsync(server_public_key=public_key if encryption_enabled else None)
        method, params = "testing", {"p1": 10, "p2": "abc"}

        for val in (10, 20):
            msg_recv = await zmq_comm.send_message(method=method, params=params)

            assert msg_recv["success"] is True, str(msg_recv)
            assert msg_recv["some_data"] == val, str(msg_recv)
            assert msg_recv["msg_in"] == {"method": method, "params": params}, str(msg_recv)

    asyncio.run(testing())

    thread.join()


# fmt: off
@pytest.mark.parametrize("encryption_enabled", [False, True])
# fmt: on
def test_ZMQCommSendAsync_3(encryption_enabled):
    """
    Testing protection of '_zmq_communicate` with lock. In this test the function
    ``send_message` is called twice so that the second call is submitted before
    the response to the first message is received. The server waits for 0.1 seconds
    before responding to the 1st message to emulate delay in processing. Since
    ``_zmq_communicate`` is protected by a lock, the second request will not
    be sent until the first message is processed.
    """
    public_key, _, server_kwargs = _gen_server_keys(encryption_enabled=encryption_enabled)

    thread = threading.Thread(target=_zmq_server_2msg_delay1, kwargs=server_kwargs)
    thread.start()

    async def testing():
        zmq_comm = ZMQCommSendAsync(server_public_key=public_key if encryption_enabled else None)
        method, params = "testing", {"p1": 10, "p2": "abc"}

        vals = (10, 20)

        tasks = []
        for n in range(len(vals)):
            tsk = asyncio.ensure_future(zmq_comm.send_message(method=method, params=params))
            tasks.append(tsk)

        for tsk in tasks:
            await tsk

        for n, val in enumerate(vals):
            res = tasks[n].result()
            assert res["success"] is True, str(res)
            assert res["some_data"] == val, str(res)
            assert res["msg_in"] == {"method": method, "params": params}, str(res)

    asyncio.run(testing())

    thread.join()


# fmt: off
@pytest.mark.parametrize("encryption_enabled", [False, True])
@pytest.mark.parametrize(
    "raise_exception",
    [False,  # Repeated tests are intentional
     False,
     None,
     True,
     False,
     False]
)
@pytest.mark.parametrize("delay_between_reads", [2, 0.1])
# fmt: on
def test_ZMQCommSendAsync_4(raise_exception, delay_between_reads, encryption_enabled):
    """
    ZMQCommSendAsync: Timeout at the server.
    """
    public_key, _, server_kwargs = _gen_server_keys(encryption_enabled=encryption_enabled)

    thread = threading.Thread(target=_zmq_server_delay2, kwargs=server_kwargs)
    thread.start()

    async def testing():
        zmq_comm = ZMQCommSendAsync(server_public_key=public_key if encryption_enabled else None)
        method, params = "testing", {"p1": 10, "p2": "abc"}

        msg_recv, msg_recv_err = {}, ""

        for val in (10, 20):
            if (raise_exception in (True, None)) and (val == 10):
                # The case when timeout is expected for blocking operation
                with pytest.raises(CommTimeoutError, match="timeout occurred"):
                    await zmq_comm.send_message(method=method, params=params, raise_exceptions=raise_exception)
            else:
                msg_recv = await zmq_comm.send_message(
                    method=method, params=params, raise_exceptions=raise_exception
                )

            if val == 10:
                if raise_exception not in (True, None):
                    assert msg_recv["success"] is False, str(msg_recv)
                    assert "timeout occurred" in msg_recv["msg"], str(msg_recv)

                # Delay between consecutive reads. Test cases when read is initiated before and
                #   after the server restored operation and sent the response.
                await asyncio.sleep(delay_between_reads)

            else:
                assert msg_recv["success"] is True, str(msg_recv)
                assert msg_recv["some_data"] == val, str(msg_recv)
                assert msg_recv["msg_in"] == {"method": method, "params": params}, str(msg_recv)
                assert msg_recv_err == ""

    asyncio.run(testing())

    thread.join()


# fmt: off
@pytest.mark.parametrize("timeout", [None, 2000, 5000])
# fmt: on
def test_ZMQCommSendAsync_5(timeout):
    """
    ZMQCommSendThreads: Pass timeout as 'send_message' parameter.
    """
    delay = 3000
    thread = threading.Thread(target=_zmq_server_delay3, kwargs={"delay": delay / 1000})
    thread.start()

    async def testing():
        timeout_default = 2000
        zmq_comm = ZMQCommSendAsync(timeout_recv=timeout_default)
        method, params = "testing", {"p1": 10, "p2": "abc"}

        timeout_used = timeout or timeout_default

        if timeout_used < delay:
            with pytest.raises(CommTimeoutError, match="timeout occurred"):
                await zmq_comm.send_message(method=method, params=params, timeout=timeout, raise_exceptions=True)
        else:
            msg_recv = await zmq_comm.send_message(
                method=method, params=params, timeout=timeout, raise_exceptions=True
            )
            assert msg_recv["success"] is True, pprint.pformat(msg_recv)

    asyncio.run(testing())

    thread.join()


def test_ZMQCommSendAsync_6_fail():
    """
    Invalid public key
    """

    async def testing():
        with pytest.raises(ValueError):
            ZMQCommSendAsync(server_public_key="abc")

    asyncio.run(testing())
