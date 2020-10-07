import subprocess

import pytest
from xprocess import ProcessStarter

import bluesky_queueserver.server.server as bqss

SERVER_ADDRESS = "localhost"
SERVER_PORT = "60610"


@pytest.fixture
def fastapi_server(xprocess):
    class Starter(ProcessStarter):
        pattern = "Connected to ZeroMQ server"
        args = f"uvicorn --host={SERVER_ADDRESS} --port {SERVER_PORT} {bqss.__name__}:app".split()

    xprocess.ensure("fastapi_server", Starter)
    # Clear the queue before the run:
    subprocess.run("qserver -c queue_clear".split())

    yield

    # Clear the queue after the run:
    subprocess.run("qserver -c queue_clear".split())
    xprocess.getinfo("fastapi_server").terminate()


@pytest.fixture
def add_plans_to_queue():
    subprocess.run("qserver -c queue_clear".split())
    subprocess.call(
        [
            "qserver",
            "-c",
            "queue_plan_add",
            "-p",
            "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':10, 'delay':1}}",
        ]
    )
    subprocess.call(["qserver", "-c", "queue_plan_add", "-p", "{'name':'count', 'args':[['det1', 'det2']]}"])
    subprocess.call(["qserver", "-c", "queue_plan_add", "-p", "{'name':'count', 'args':[['det1', 'det2']]}"])

    yield

    subprocess.run("qserver -c queue_clear".split())
