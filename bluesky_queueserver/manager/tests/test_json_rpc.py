import json

import pytest

from ..json_rpc import JSONRPCResponseManager


# fmt: off
@pytest.mark.parametrize("use_json", [None, True, False])
# fmt: on
def test_json_rpc_init(use_json):
    if use_json is None:
        params = {}
    elif use_json in (True, False):
        params = {"use_json": use_json}
    else:
        assert False, f"Unknown value of 'use_json': {use_json}"

    rm = JSONRPCResponseManager(**params)
    assert rm._use_json == (use_json if use_json is not None else True)


def _method1():
    return 1


def _method2(a):
    return a


def _method3():
    pass


def _method4():
    raise RuntimeError("Error in 'method4'")


methods = {
    "method1": _method1,
    "method2": _method2,
    "method3": _method3,
    "method4": _method4,
}


# fmt: off
@pytest.mark.parametrize("use_json", [True, False])
@pytest.mark.parametrize("msg_in, msg_resp", [
    ([], None),  # Empty batch
    ({"jsonrpc": "2.0", "method": "method1", "id": 1}, {'id': 1, 'jsonrpc': '2.0', 'result': 1}),
    ({"jsonrpc": "2.0", "method": "method1"}, None),  # Notification
    ({"jsonrpc": "2.0", "method": "method1", "params": [], "id": 1}, {'id': 1, 'jsonrpc': '2.0', 'result': 1}),
    ({"jsonrpc": "2.0", "method": "method1", "params": {}, "id": 1}, {'id': 1, 'jsonrpc': '2.0', 'result': 1}),
    ({"jsonrpc": "2.0", "method": "method2", "params": [10], "id": 1},
     {'id': 1, 'jsonrpc': '2.0', 'result': 10}),
    ({"jsonrpc": "2.0", "method": "method2", "params": {"a": 10}, "id": 1},
     {'id': 1, 'jsonrpc': '2.0', 'result': 10}),

    # Batch
    ([{"jsonrpc": "2.0", "method": "method2", "params": [10], "id": 1}],
     [{'id': 1, 'jsonrpc': '2.0', 'result': 10}]),
    ([{"jsonrpc": "2.0", "method": "method2", "params": [10]}], None),  # Notification
    ([{"jsonrpc": "2.0", "method": "method2", "params": [10], "id": 1},
      {"jsonrpc": "2.0", "method": "method2", "params": [5]}],  # Notification
     [{'id': 1, 'jsonrpc': '2.0', 'result': 10}]),

    # Method returns None
    ({"jsonrpc": "2.0", "method": "method3", "id": 1}, {'id': 1, 'jsonrpc': '2.0', 'result': None}),

    # Invalid request
    ({"method": "method1", "id": 1},  # Missing 'jsonrpc'
     {"jsonrpc": "2.0", "id": None, "error":  # ID is None
      {"code": -32600, "message": "Invalid request", "data":
       {"type": "TypeError", "message": "Invalid message format: {'method': 'method1', 'id': 1}"}}}),
    ({"json_rpc": "1.0", "method": "method1", "id": 1},  # Incorrect version of 'jsonrpc'
     {"jsonrpc": "2.0", "id": None, "error":  # ID is None
      {"code": -32600, "message": "Invalid request", "data":
       {"type": "TypeError",
        "message": "Invalid message format: {'json_rpc': '1.0', 'method': 'method1', 'id': 1}"}}}),
    ({"json_rpc": "2.0", "id": 1},  # 'method' is missing
     {"jsonrpc": "2.0", "id": None, "error":  # ID is None
      {"code": -32600, "message": "Invalid request", "data":
       {"type": "TypeError", "message": "Invalid message format: {'json_rpc': '2.0', 'id': 1}"}}}),

    # Unknown method
    ({"jsonrpc": "2.0", "method": "unknown", "id": 1},
     {'jsonrpc': '2.0', 'id': 1, 'error':
      {'code': -32601, 'message': 'Method not found', 'data':
       {'type': 'TypeError', 'message': 'Unknown method: unknown'}}}),

    # Invalid params
    ({"jsonrpc": "2.0", "method": "method2", "params": 10, "id": 1},  # Params should be list or dict
     {"jsonrpc": "2.0", "id": 1, "error":
      {"code": -32602, "message": "Invalid params", "data":
       {"type": "TypeError",
        "message": "Invalid params in the message {'jsonrpc': '2.0', 'method': 'method2', 'params': 10, 'id': 1}"
        }}}),
    ({"jsonrpc": "2.0", "method": "method2", "params": {"b": 10}, "id": 1},  # No param 'b'
     {"jsonrpc": "2.0", "id": 1, "error":
      {"code": -32602, "message": "Invalid params", "data":
       {"type": "TypeError",
        "message": (
            "Invalid params in the message {'jsonrpc': '2.0', 'method': 'method2', 'params': "
            "{'b': 10}, 'id': 1}: _method2() got an unexpected keyword argument 'b'")
        }}}),

    # Server error
    ({"jsonrpc": "2.0", "method": "method4", "id": 1},
     {"jsonrpc": "2.0", "id": 1, "error":
      {"code": -32000, "message": "Server error", "data":
       {"type": "RuntimeError", "message": "Error in 'method4'"}}}),

    # Errors in batches of messages
    ([{"jsonrpc": "2.0", "method": "unknown", "id": 2},
      {"jsonrpc": "2.0", "method": "method2", "params": [10], "id": 1},
      {"jsonrpc": "2.0", "method": "method4", "id": 3}],
     [{'id': 2, 'jsonrpc': '2.0', 'error':
       {'code': -32601, 'message': 'Method not found', 'data':
        {'type': 'TypeError', 'message': 'Unknown method: unknown'}}},
      {'id': 1, 'jsonrpc': '2.0', 'result': 10},
      {"jsonrpc": "2.0", "id": 3, "error":
       {"code": -32000, "message": "Server error", "data":
        {"type": "RuntimeError", "message": "Error in 'method4'"}}}
      ]),
])
# fmt: on
def test_json_rpc_handle(use_json, msg_in, msg_resp):
    rm = JSONRPCResponseManager(use_json=use_json)
    for k, v in methods.items():
        rm.add_method(v, k)

    if use_json:
        msg_in = json.dumps(msg_in)
    resp = rm.handle(msg_in)
    if use_json and resp is not None:
        resp = json.loads(resp)
    assert resp == msg_resp


def test_json_rpc_corrupt_message():
    """
    Test the case when the handler fails to decode JSON message.
    """
    rm = JSONRPCResponseManager(use_json=True)
    msg = "{'json_rpc}"
    resp = rm.handle(msg)
    resp = json.loads(resp)
    assert resp == {
        "jsonrpc": "2.0",
        "id": None,
        "error": {
            "code": -32700,
            "message": "Parse error",
            "data": {
                "type": "TypeError",
                "message": (
                    "Failed to parse the message '{'json_rpc}': Expecting property name "
                    "enclosed in double quotes: line 1 column 2 (char 1)"
                ),
            },
        },
    }
