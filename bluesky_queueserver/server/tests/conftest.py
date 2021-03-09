import pytest
from xprocess import ProcessStarter

import bluesky_queueserver.server.server as bqss
from bluesky_queueserver.manager.comms import zmq_single_request

SERVER_ADDRESS = "localhost"
SERVER_PORT = "60610"


@pytest.fixture(scope="module")
def fastapi_server(xprocess):
    class Starter(ProcessStarter):
        pattern = "Connected to ZeroMQ server"
        args = f"uvicorn --host={SERVER_ADDRESS} --port {SERVER_PORT} {bqss.__name__}:app".split()

    xprocess.ensure("fastapi_server", Starter)

    yield

    xprocess.getinfo("fastapi_server").terminate()


def add_plans_to_queue():
    """
    Clear the queue and add 3 fixed plans to the queue.
    Raises an exception if clearing the queue or adding plans fails.
    """
    resp1, _ = zmq_single_request("queue_clear")
    assert resp1["success"] is True, str(resp1)

    user_group = "admin"
    user = "HTTP unit test setup"
    plan1 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 10, "delay": 1}}
    plan2 = {"name": "count", "args": [["det1", "det2"]]}
    for plan in (plan1, plan2, plan2):
        resp2, _ = zmq_single_request("queue_item_add", {"plan": plan, "user": user, "user_group": user_group})
        assert resp2["success"] is True, str(resp2)
