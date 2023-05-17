import pprint

from ..comms import zmq_single_request
from .common import re_manager  # noqa: F401
from .common import re_manager_cmd  # noqa: F401
from .common import (
    append_code_to_last_startup_file,
    condition_environment_closed,
    condition_environment_created,
    copy_default_profile_collection,
    use_ipykernel_for_tests,
    wait_for_condition,
    wait_for_task_result,
)

timeout_env_open = 10

_script_with_ip_features = """
from IPython.core.magic import register_line_magic, register_cell_magic

@register_line_magic
def lmagic(line):
    return line

@register_cell_magic
def cmagic(line, cell):
    return line, cell
"""


def test_ip_kernel_loading_script_01(tmp_path, re_manager_cmd):  # noqa: F811
    """
    Test that the IPython-based worker can load startup code with IPython-specific features,
    and regular worker fails.
    """
    using_ipython = use_ipykernel_for_tests()

    pc_path = copy_default_profile_collection(tmp_path)
    append_code_to_last_startup_file(pc_path, additional_code=_script_with_ip_features)

    params = ["--startup-dir", pc_path]
    re_manager_cmd(params)

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    if not using_ipython:
        assert not wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    else:
        assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

        resp9, _ = zmq_single_request("environment_close")
        assert resp9["success"] is True
        assert resp9["msg"] == ""

        assert wait_for_condition(time=3, condition=condition_environment_closed)


def test_ip_kernel_loading_script_02(tmp_path, re_manager):  # noqa: F811
    """
    Test that the IPython-based worker accepts uploaded scripts with IPython-specific code
    and the regular worker fails.
    """
    using_ipython = use_ipykernel_for_tests()

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp3, _ = zmq_single_request("script_upload", params={"script": _script_with_ip_features})
    assert resp3["success"] is True, pprint.pformat(resp3)

    result = wait_for_task_result(10, resp3["task_uid"])
    if not using_ipython:
        assert result["success"] is False, pprint.pformat(result)
        assert "Failed to execute stript" in result["msg"]
    else:
        assert result["success"] is True, pprint.pformat(result)
        assert result["msg"] == "", pprint.pformat(result)

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)
