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
