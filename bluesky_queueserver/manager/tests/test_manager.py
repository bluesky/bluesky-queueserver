import pytest

from bluesky_queueserver.manager.manager import RunEngineManager


# fmt: off
@pytest.mark.parametrize("request_dict, param_names, success, msg", [
    ({"ab": 10, "cd": 50}, ["ab", "cd", "ef"], True, ""),
    ({}, [], True, ""),
    ({"ab": 10, "cd": 50}, [], False, r"unsupported parameters: \['ab', 'cd'\]. Supported parameters: \[\]"),
    ({"ab": 10}, [], False, r"unsupported parameters: 'ab'. Supported parameters: \[\]"),
    ({"ab": 10}, ["de"], False, r"unsupported parameters: 'ab'. Supported parameters: \['de'\]"),
])
# fmt: on
def test_check_request_for_unsupported_params_1(request_dict, param_names, success, msg):
    """
    Basic test for ``RunEngineManager._check_request_for_unsupported_params``.
    """
    if success:
        RunEngineManager._check_request_for_unsupported_params(request=request_dict, param_names=param_names)
    else:
        with pytest.raises(ValueError, match=msg):
            RunEngineManager._check_request_for_unsupported_params(request=request_dict, param_names=param_names)
