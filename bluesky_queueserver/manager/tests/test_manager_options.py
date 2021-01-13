import pytest
import shutil
import os
import glob

from bluesky_queueserver.manager.profile_ops import gen_list_of_plans_and_devices
from bluesky_queueserver.manager.comms import zmq_single_request

from ._common import (
    copy_default_profile_collection,
    append_code_to_last_startup_file,
    wait_for_condition,
    condition_environment_created,
    condition_queue_processing_finished,
    condition_environment_closed,
)

from ._common import re_manager_cmd  # noqa: F401

# User name and user group name used throughout most of the tests.
_user, _user_group = "Testing Script", "admin"


_sample_plan1 = """
def simple_sample_plan():
    '''
    Simple plan for tests.
    '''
    yield from count([det1, det2])
"""


# fmt: off
@pytest.mark.parametrize("option", ["startup_dir", "profile", "multiple"])
# fmt: on
def test_manager_options_startup_profile(re_manager_cmd, tmp_path, monkeypatch, option):  # noqa: F811
    pc_path = copy_default_profile_collection(tmp_path)

    # Add extra plan. The original set of startup files will not contain this plan.
    append_code_to_last_startup_file(pc_path, additional_code=_sample_plan1)

    # Generate the new list of allowed plans and devices and reload them
    gen_list_of_plans_and_devices(startup_dir=pc_path, file_dir=pc_path, overwrite=True)

    # Start manager
    if option == "startup_dir":
        re_manager_cmd(["--startup-dir", pc_path])
    elif option == "profile":
        # This option is more complicated: we want to recreate the structure of IPython startup
        #   directory: <some root dir>/profile_<profile_name>/startup.
        root_dir = os.path.split(pc_path)[0]
        monkeypatch.setenv("IPYTHONDIR", root_dir)
        profile_name = "testing"
        startup_path = os.path.join(root_dir, f"profile_{profile_name}", "startup")
        os.makedirs(startup_path)

        file_pattern = os.path.join(pc_path, "*")
        for fl_path in glob.glob(file_pattern):
            shutil.move(fl_path, startup_path)

        os.rmdir(pc_path)

        # We pass only profile name as a parameter.
        re_manager_cmd(["--startup-profile", profile_name])
    elif option == "multiple":
        # Expected to fail if multiple options are selected.
        with pytest.raises(TimeoutError, match="RE Manager failed to start"):
            re_manager_cmd(["--startup-dir", pc_path, "--startup-profile", "some_name"])
        return
    else:
        assert False, f"Unknown option '{option}'"

    # Open the environment (make sure that the environment loads)
    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert wait_for_condition(time=10, condition=condition_environment_created)

    # Add the plan to the queue (will fail if incorrect environment is loaded)
    plan = {"name": "simple_sample_plan"}
    params = {"plan": plan, "user": _user, "user_group": _user_group}
    resp2, _ = zmq_single_request("queue_item_add", params)
    assert resp2["success"] is True, f"resp={resp2}"

    # Start the queue
    resp3, _ = zmq_single_request("queue_start")
    assert resp3["success"] is True
    assert wait_for_condition(time=10, condition=condition_queue_processing_finished)

    # Make sure that the plan was executed
    resp4, _ = zmq_single_request("status")
    assert resp4["items_in_queue"] == 0
    assert resp4["items_in_history"] == 1

    # Close the environment
    resp5, _ = zmq_single_request("environment_close")
    assert resp5["success"] is True, f"resp={resp5}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)

    monkeypatch.setenv("IPYTHONDIR", "abc")
