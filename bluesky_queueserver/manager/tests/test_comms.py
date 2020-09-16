import pytest
import random
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
    The case of simple function handlers
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


def test_PipeJsonRpcReceive_3_failing():

    def method_handler3(*, value=3):
        return value + 15

    conn1, conn2 = multiprocessing.Pipe()
    pc = PipeJsonRpcReceive(conn=conn2)
    pc.add_method(method_handler3, "method3")

    request = {"id": 0, "jsonrpc": "2.0", "method": "method3", "params": {"value": 5}}
    conn1.send(json.dumps(request))

    if conn1.poll(timeout=0.5):  # Set timeout large enough
        response = conn1.recv()
        response = json.loads(response)
        assert False, f"response={response}"
    else:
        assert False, "Timeout occurred while waiting for response."


