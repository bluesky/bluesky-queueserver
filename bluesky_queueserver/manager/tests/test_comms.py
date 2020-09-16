import pytest
import random
import time as ttime
import json
import multiprocessing
from bluesky_queueserver.manager.comms import PipeJsonRpcReceive


def test_PipeJsonRpcReceive_1():
    """
    Create, start and stop `PipeJsonRpcReceive` object
    """
    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2)
    pc.start()
    pc.stop()


@pytest.mark.parametrize("method, params, result, notification", [
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
])
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

    request = {"method": method, "jsonrpc": "2.0"}
    if params:
        request["params"] = params

    for n in range(3):
        value_nonlocal = None

        if not notification:
            msg_id = random.randint(0, 1000)  # Generate some message ID
            request["id"] = msg_id

        conn1.send(json.dumps(request))
        if conn1.poll(timeout=0.5):  # Set timeout large enough
            if not notification:
                response = conn1.recv()
                response = json.loads(response)
                assert response["id"] == msg_id, "Response ID does not match message ID."
                assert "result" in response, \
                    f"Key 'result' is not contained in response: {response}"
                assert response["result"] == result, \
                    f"Result does not match the expected: {response}"
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
    request = [{"id": 2, "jsonrpc": "2.0", "method": "method3", "params": {"value": 7}},
               {"jsonrpc": "2.0", "method": "method4", "params": {"value": 3}},  # Notification
               {"id": 3, "jsonrpc": "2.0", "method": "method4", "params": {"value": 9}}]
    conn1.send(json.dumps(request))

    if conn1.poll(timeout=0.5):  # Set timeout large enough
        response = conn1.recv()
        response = json.loads(response)
        assert len(response) == 2, "Unexpected number of response messages"
        assert response[0]["id"] == 2, "Response ID does not match message ID."
        assert response[1]["id"] == 3, "Response ID does not match message ID."
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
    request = {"id": 0, "jsonrpc": "2.0", "method": "method3", "params": [5]}
    conn1.send(json.dumps(request))

    if conn1.poll(timeout=0.5):  # Set timeout large enough
        response = conn1.recv()
        response = json.loads(response)
        assert response["error"]["code"] == -32000, f"Incorrect error reported: {response}"
        assert response["error"]["data"]["type"] == "TypeError", "Incorrect error type."
    else:
        assert False, "Timeout occurred while waiting for response."

    # ------- Incorrect argument (incorrect argument type) -------
    request = {"id": 1, "jsonrpc": "2.0", "method": "method3", "params": {"value": "abc"}}
    conn1.send(json.dumps(request))

    if conn1.poll(timeout=0.5):
        response = conn1.recv()
        response = json.loads(response)
        assert response["error"]["code"] == -32000, f"Incorrect error reported: {response}"
        assert response["error"]["data"]["type"] == "TypeError", "Incorrect error type."
    else:
        assert False, "Timeout occurred while waiting for response."

    # ------- Incorrect argument (extra argument) -------
    request = {"id": 1, "jsonrpc": "2.0", "method": "method3", "params": {"value": 5, "unknown": 10}}
    conn1.send(json.dumps(request))

    if conn1.poll(timeout=0.5):
        response = conn1.recv()
        response = json.loads(response)
        assert response["error"]["code"] == -32602, f"Incorrect error reported: {response}"
    else:
        assert False, "Timeout occurred while waiting for response."

    # ------- Non-existing method ('Method not found' error) -------
    request = {"id": 1, "jsonrpc": "2.0", "method": "method_handler3", "params": {"value": 5}}
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
    """
    def method_handler5():
        raise RuntimeError("Function crashed ...")

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2)
    pc.add_method(method_handler5, "method5")
    pc.start()

    # Non-existing method ('Method not found' error)
    request = {"id": 2, "jsonrpc": "2.0", "method": "method5"}
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

    ONLY QUICKLY EXECUTED HANDLERS MAY BE USED!!!
    """
    def method_handler6():
        ttime.sleep(3)  # Longer than 'poll' timeout

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2)
    pc.add_method(method_handler6, "method6")
    pc.start()

    # Non-existing method ('Method not found' error)
    request = {"id": 2, "jsonrpc": "2.0", "method": "method6"}
    conn1.send(json.dumps(request))

    if conn1.poll(timeout=0.5):  # Set timeout large enough
        assert False, "The test is expected to timeout."
    else:
        pass

    pc.stop()
