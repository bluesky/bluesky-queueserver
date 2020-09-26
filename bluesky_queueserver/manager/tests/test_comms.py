import pytest
import time as ttime
import json
import multiprocessing
import threading
import asyncio
from bluesky_queueserver.manager.comms import (
    PipeJsonRpcReceive,
    PipeJsonRpcSendAsync,
    CommTimeoutError,
    CommJsonRpcError,
)
from bluesky_queueserver.tests.common import format_jsonrpc_msg


def count_threads_with_name(name):
    """
    Returns the number of currently existing threads with the given name
    """
    n_count = 0
    for th in threading.enumerate():
        if th.name == name:
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

    with pytest.raises(AttributeError, match="can't set attribute"):
        ex.message = err_msg
    with pytest.raises(AttributeError, match="can't set attribute"):
        ex.error_code = err_code
    with pytest.raises(AttributeError, match="can't set attribute"):
        ex.error_type = err_type


# =======================================================================
#                       Class PipeJsonRpcReceive


def test_PipeJsonRpcReceive_1():
    """
    Create, start and stop `PipeJsonRpcReceive` object
    """
    conn1, conn2 = multiprocessing.Pipe()
    new_name = "Unusual Thread Name"

    assert count_threads_with_name(new_name) == 0, "No threads are expected to exist"

    pc = PipeJsonRpcReceive(conn=conn2, name=new_name)
    pc.start()
    assert count_threads_with_name(new_name) == 1, "One thread is expected to exist"

    pc.start()  # Expected to do nothing

    pc.stop()
    ttime.sleep(0.15)  # Wait until the thread stops (wait more than the 0.1s polling period)
    assert count_threads_with_name(new_name) == 0, "No threads are expected to exist"

    pc.start()  # Restart
    assert count_threads_with_name(new_name) == 1, "One thread is expected to exist"
    pc.stop()


@pytest.mark.parametrize(
    "method, params, result, notification",
    [
        ("method_handler1", [], 5, False),
        ("method_handler1", [], 5, True),
        ("method1", [], 5, False),
        ("method2", [5], 15, False),
        ("method2", {"value": 5}, 15, False),
        ("method2", {}, 12, False),
        ("method3", {"value": 5}, 20, False),
        ("method3", {}, 18, False),
        ("method4", {"value": 5}, 20, False),
        ("method4", {}, 19, False),
    ],
)
def test_PipeJsonRpcReceive_2(method, params, result, notification):
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
    pc = PipeJsonRpcReceive(conn=conn2)
    pc.add_method(method_handler1)  # No name is specified, default name is "method_handler1"
    pc.add_method(method_handler1, "method1")
    pc.add_method(method_handler2, "method2")
    pc.add_method(method_handler3, "method3")
    pc.add_method(some_class.method_handler4, "method4")
    pc.start()

    for n in range(3):
        value_nonlocal = None

        request = format_jsonrpc_msg(method, params, notification=notification)
        conn1.send(json.dumps(request))
        if conn1.poll(timeout=0.5):  # Set timeout large enough
            if not notification:
                response = conn1.recv()
                response = json.loads(response)
                assert response["id"] == request["id"], "Response ID does not match message ID."
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


def test_PipeJsonRpcReceive_3():
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
    pc = PipeJsonRpcReceive(conn=conn2)
    pc.add_method(method_handler3, "method3")
    pc.add_method(some_class.method_handler4, "method4")
    pc.start()

    # Non-existing method ('Method not found' error)
    request = [
        format_jsonrpc_msg("method3", {"value": 7}),
        format_jsonrpc_msg("method4", {"value": 3}, notification=True),
        format_jsonrpc_msg("method4", {"value": 9}),
    ]
    conn1.send(json.dumps(request))

    if conn1.poll(timeout=0.5):  # Set timeout large enough
        response = conn1.recv()
        response = json.loads(response)
        assert len(response) == 2, "Unexpected number of response messages"
        assert response[0]["id"] == request[0]["id"], "Response ID does not match message ID."
        assert response[1]["id"] == request[2]["id"], "Response ID does not match message ID."
        assert response[0]["result"] == 22, "Response ID does not match message ID."
        assert response[1]["result"] == 16, "Response ID does not match message ID."
    else:
        assert False, "Timeout occurred while waiting for response."

    pc.stop()


def test_PipeJsonRpcReceive_4_failing():
    """
    Those tests are a result of exploration of how `json-rpc` error processing works.
    """

    def method_handler3(*, value=3):
        return value + 15

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2)
    pc.add_method(method_handler3, "method3")
    pc.start()

    # ------- Incorrect argument (arg list instead of required kwargs) -------
    #   Returns 'Server Error' (-32000)
    request = format_jsonrpc_msg("method3", [5])
    conn1.send(json.dumps(request))

    if conn1.poll(timeout=0.5):  # Set timeout large enough
        response = conn1.recv()
        response = json.loads(response)
        assert response["error"]["code"] == -32000, f"Incorrect error reported: {response}"
        assert response["error"]["data"]["type"] == "TypeError", "Incorrect error type."
    else:
        assert False, "Timeout occurred while waiting for response."

    # ------- Incorrect argument (incorrect argument type) -------
    # 'json-prc' doesn't check parameter types. Instead the handler will crash if the argument
    #   type is not suitable.
    request = format_jsonrpc_msg("method3", {"value": "abc"})
    conn1.send(json.dumps(request))

    if conn1.poll(timeout=0.5):
        response = conn1.recv()
        response = json.loads(response)
        assert response["error"]["code"] == -32000, f"Incorrect error reported: {response}"
        assert response["error"]["data"]["type"] == "TypeError", "Incorrect error type."
    else:
        assert False, "Timeout occurred while waiting for response."

    # ------- Incorrect argument (extra argument) -------
    request = format_jsonrpc_msg("method3", {"value": 5, "unknown": 10})
    conn1.send(json.dumps(request))

    if conn1.poll(timeout=0.5):
        response = conn1.recv()
        response = json.loads(response)
        assert response["error"]["code"] == -32602, f"Incorrect error reported: {response}"
    else:
        assert False, "Timeout occurred while waiting for response."

    # ------- Non-existing method ('Method not found' error) -------
    request = format_jsonrpc_msg("method_handler3", {"value": 5})
    conn1.send(json.dumps(request))

    if conn1.poll(timeout=0.5):
        response = conn1.recv()
        response = json.loads(response)
        assert response["error"]["code"] == -32601, f"Incorrect error reported: {response}"
    else:
        assert False, "Timeout occurred while waiting for response."

    pc.stop()


def test_PipeJsonRpcReceive_5_failing():
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
    pc = PipeJsonRpcReceive(conn=conn2)
    pc.add_method(method_handler5, "method5")
    pc.start()

    request = format_jsonrpc_msg("method5")
    conn1.send(json.dumps(request))

    if conn1.poll(timeout=0.5):  # Set timeout large enough
        response = conn1.recv()
        response = json.loads(response)
        assert response["error"]["code"] == -32000, f"Incorrect error reported: {response}"
        assert response["error"]["data"]["type"] == "RuntimeError", "Incorrect error type."
        assert response["error"]["data"]["message"] == "Function crashed ...", "Incorrect message."
    else:
        assert False, "Timeout occurred while waiting for response."

    pc.stop()


def test_PipeJsonRpcReceive_6_failing():
    """
    This is an example of handler to timeout. 'json-rpc' package can not handle timeouts.
    Care must be taken to write handles that execute quickly. Timeouts must be handled
    explicitly within the handlers.

    ONLY INSTANTLY EXECUTED HANDLERS MAY BE USED!!!
    """

    def method_handler6():
        ttime.sleep(3)  # Longer than 'poll' timeout

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2)
    pc.add_method(method_handler6, "method6")
    pc.start()

    # Non-existing method ('Method not found' error)
    request = format_jsonrpc_msg("method6")

    conn1.send(json.dumps(request))

    if conn1.poll(timeout=0.5):  # Set timeout large enough
        assert False, "The test is expected to timeout."
    else:
        pass

    pc.stop()


# =======================================================================
#                       Class PipeJsonRpcSendAsync


def test_PipeJsonRpcSendAsync_1():
    """
    Create, start and stop `PipeJsonRpcReceive` object
    """
    conn1, conn2 = multiprocessing.Pipe()
    new_name = "Unusual Thread Name"

    async def object_start_stop():
        assert count_threads_with_name(new_name) == 0, "No threads are expected to exist"

        pc = PipeJsonRpcSendAsync(conn=conn1, name=new_name)
        pc.start()
        assert count_threads_with_name(new_name) == 1, "One thread is expected to exist"

        pc.start()  # Expected to do nothing

        pc.stop()
        ttime.sleep(0.15)  # Wait until the thread stops (0.1s polling period)
        assert count_threads_with_name(new_name) == 0, "No threads are expected to exist"

        pc.start()  # Restart
        assert count_threads_with_name(new_name) == 1, "One thread is expected to exist"
        pc.stop()

    asyncio.run(object_start_stop())


@pytest.mark.parametrize(
    "method, params, result, notification",
    [
        ("method_handler1", [], 5, False),
        ("method_handler1", [], 5, True),
        ("method1", [], 5, False),
        ("method2", [5], 15, False),
        ("method2", {"value": 5}, 15, False),
        ("method2", {}, 12, False),
        ("method3", {"value": 5}, 20, False),
        ("method3", {}, 18, False),
        ("method4", {"value": 5}, 20, False),
        ("method4", {}, 19, False),
    ],
)
def test_PipeJsonRpcSendAsync_2(method, params, result, notification):
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
    pc = PipeJsonRpcReceive(conn=conn2, name="comm-server")
    pc.add_method(method_handler1)  # No name is specified, default name is "method_handler1"
    pc.add_method(method_handler1, "method1")
    pc.add_method(method_handler2, "method2")
    pc.add_method(method_handler3, "method3")
    pc.add_method(some_class.method_handler4, "method4")
    pc.start()

    async def send_messages():
        nonlocal value_nonlocal

        p_send = PipeJsonRpcSendAsync(conn=conn1, name="comm-client")
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


def test_PipeJsonRpcSendAsync_3():
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
    pc = PipeJsonRpcReceive(conn=conn2, name="comm-server")
    pc.add_method(method_handler1, "method1")
    pc.start()

    async def send_messages():
        p_send = PipeJsonRpcSendAsync(conn=conn1, name="comm-client")
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


def test_PipeJsonRpcSendAsync_4():
    """
    Message timeout.
    """

    def method_handler1():
        ttime.sleep(1)

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, name="comm-server")
    pc.add_method(method_handler1, "method1")
    pc.start()

    async def send_messages():
        p_send = PipeJsonRpcSendAsync(conn=conn1, name="comm-client")
        p_send.start()

        # Submit multiple messages at once. Messages should stay at the event loop
        #   and be processed one by one.
        with pytest.raises(CommTimeoutError, match="Timeout while waiting for response to message"):
            await p_send.send_msg("method1", timeout=0.5)

        p_send.stop()

    asyncio.run(send_messages())
    pc.stop()


def test_PipeJsonRpcSendAsync_5():
    """
    Specia test case.
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
    pc = PipeJsonRpcReceive(conn=conn2, name="comm-server")
    pc.add_method(method_handler1, "method1")
    pc.add_method(method_handler2, "method2")
    pc.start()

    async def send_messages():
        p_send = PipeJsonRpcSendAsync(conn=conn1, name="comm-client")
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


def test_PipeJsonRpcSendAsync_6_fail():
    """
    Exception raised inside the method.
    """

    def method_handler1():
        raise RuntimeError("Function crashed ...")

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, name="comm-server")
    pc.add_method(method_handler1, "method1")
    pc.start()

    async def send_messages():
        p_send = PipeJsonRpcSendAsync(conn=conn1, name="comm-client")
        p_send.start()

        # Submit multiple messages at once. Messages should stay at the event loop
        #   and be processed one by one.
        with pytest.raises(CommJsonRpcError, match="Function crashed ..."):
            await p_send.send_msg("method1", timeout=0.5)

        p_send.stop()

    asyncio.run(send_messages())
    pc.stop()


def test_PipeJsonRpcSendAsync_7_fail():
    """
    Method not found (other `json-rpc` errors will raise the same exception).
    """

    def method_handler1():
        pass

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2, name="comm-server")
    pc.add_method(method_handler1, "method1")
    pc.start()

    async def send_messages():
        p_send = PipeJsonRpcSendAsync(conn=conn1, name="comm-client")
        p_send.start()

        # Submit multiple messages at once. Messages should stay at the event loop
        #   and be processed one by one.
        with pytest.raises(CommJsonRpcError):
            await p_send.send_msg("nonexisting_method", timeout=0.5)

        p_send.stop()

    asyncio.run(send_messages())
    pc.stop()
