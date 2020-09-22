import subprocess
import sys

import pytest
import requests
from xprocess import ProcessStarter

import bluesky_queueserver.server.server as bqss
from bluesky_queueserver.manager.tests.test_general import \
    re_manager  # noqa F401
from bluesky_queueserver.server.server import init_func

SERVER_IP = '0.0.0.0'
SERVER_PORT = '8080'


@pytest.fixture
def aiohttp_server(xprocess):
    class Starter(ProcessStarter):
        pattern = "Connected to ZeroMQ"
        args = (f"{sys.executable} -m aiohttp.web -H {SERVER_IP} -P {SERVER_PORT} "
                f"{bqss.__name__}:{init_func.__name__}").split()

    xprocess.ensure("aiohttp_server", Starter)
    # Clear the queue before the run:
    subprocess.run('qserver -c clear_queue'.split())

    yield

    # Clear the queue after the run:
    subprocess.run('qserver -c clear_queue'.split())
    xprocess.getinfo("aiohttp_server").terminate()


@pytest.fixture
def add_plans_to_queue():
    subprocess.run('qserver -c clear_queue'.split())
    subprocess.call(["qserver", "-c", "add_to_queue", "-v",
                     "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':10, 'delay':1}}"])
    subprocess.call(["qserver", "-c", "add_to_queue", "-v",
                     "{'name':'count', 'args':[['det1', 'det2']]}"])
    subprocess.call(["qserver", "-c", "add_to_queue", "-v",
                     "{'name':'count', 'args':[['det1', 'det2']]}"])

    yield

    subprocess.run('qserver -c clear_queue'.split())


def _request_to_json(request_type, path, **kwargs):
    resp = getattr(requests, request_type)(f'http://{SERVER_IP}:{SERVER_PORT}{path}', **kwargs).json()
    return resp


def test_http_server_hello_handler(re_manager, aiohttp_server):  # noqa F811
    resp = _request_to_json('get', '/')
    assert resp['msg'] == 'RE Manager'
    assert resp['n_plans'] == 0
    assert resp['is_plan_running'] is False


def test_http_server_queue_view_handler(re_manager, aiohttp_server):  # noqa F811
    resp = _request_to_json('get', '/queue_view')
    assert resp['queue'] == []


def test_http_server_add_to_queue_handler(re_manager, aiohttp_server):  # noqa F811
    resp1 = _request_to_json('post', '/add_to_queue',
                             json={"plan": {"name": "count",
                                            "args": [["det1", "det2"]]}})
    assert resp1['name'] == 'count'
    assert resp1['args'] == [['det1', 'det2']]
    assert 'plan_uid' in resp1

    resp2 = _request_to_json('get', '/queue_view')
    assert resp2['queue'] != []
    assert len(resp2['queue']) == 1
    assert resp2['queue'][0] == resp1


def test_http_server_pop_from_queue_handler(re_manager, aiohttp_server, add_plans_to_queue):  # noqa F811

    resp1 = _request_to_json('get', '/queue_view')
    assert resp1['queue'] != []
    assert len(resp1['queue']) == 3

    resp2 = _request_to_json('post', '/pop_from_queue',
                             json={})
    assert resp2['name'] == 'count'
    assert resp2['args'] == [['det1', 'det2']]
    assert 'plan_uid' in resp2