# This file contains tests for 0MQ API that require RE Manager started with custom set of parameters

import os
import pytest
import subprocess

from ..comms import zmq_single_request
from .common import re_manager_cmd  # noqa: F401
from .common import copy_default_profile_collection

from bluesky_queueserver.manager.profile_ops import gen_list_of_plans_and_devices

# User name and user group name used throughout most of the tests.
_user, _user_group = "Testing Script", "admin"


# =======================================================================================
#                              Method 'permissions_reload'

# fmt: off
@pytest.mark.parametrize("reload_plans_devices", [False, True])
# fmt: on
def test_permissions_reload_1(re_manager_cmd, tmp_path, reload_plans_devices):  # noqa: F811
    """
    Comprehensive test for ``permission_reload`` API: create a copy of startup files,
    generate the lists of existing plans and devices, start RE Manager (loads the list
    of existing plans and devices from disk), add a device and a plan to startup scripts,
    generate the new lists of existing plans and devices, call ``permissions_reload``,
    check if the new device and the new plan was added to the lists of allowed plans
    and devices (if ``reload_plans_devices`` is ``True``, then the device and the plan
    are expected to be in the list).
    """

    # Copy the default profile collection and generate the list of existing devices
    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=True)
    gen_list_of_plans_and_devices(
        startup_dir=pc_path, file_dir=pc_path, file_name="existing_plans_and_devices.yaml", overwrite=True
    )

    # Start the manager. This also loads the list of existing plans and devices from disk
    params = ["--startup-dir", pc_path]
    re_manager_cmd(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Add a plan ('count50') and a device ('det50'). Generate the new disk file
    with open(os.path.join(pc_path, "zz.py"), "w") as f:
        f.writelines(["det50 = det\n", "count50 = count\n"])
    gen_list_of_plans_and_devices(
        startup_dir=pc_path, file_dir=pc_path, file_name="existing_plans_and_devices.yaml", overwrite=True
    )

    # 'allowed_plans_uid' and 'allowed_devices_uid'
    status, _ = zmq_single_request("status")
    plans_allowed_uid = status["plans_allowed_uid"]
    devices_allowed_uid = status["devices_allowed_uid"]
    assert plans_allowed_uid != devices_allowed_uid

    resp1a, _ = zmq_single_request("plans_allowed", params={"user_group": _user_group})
    assert resp1a["success"] is True, f"resp={resp1a}"
    assert resp1a["plans_allowed_uid"] == plans_allowed_uid
    plans_allowed = resp1a["plans_allowed"]

    resp1b, _ = zmq_single_request("devices_allowed", params={"user_group": _user_group})
    assert resp1b["success"] is True, f"resp={resp1b}"
    assert resp1b["devices_allowed_uid"] == devices_allowed_uid
    devices_allowed = resp1b["devices_allowed"]

    assert isinstance(plans_allowed_uid, str)
    assert isinstance(devices_allowed_uid, str)
    assert "count50" not in plans_allowed
    assert "det50" not in devices_allowed

    resp2, _ = zmq_single_request("permissions_reload", {"reload_plans_devices": reload_plans_devices})
    assert resp2["success"] is True, f"resp={resp2}"

    # Check that 'plans_allowed_uid' and 'devices_allowed_uid' changed while permissions were reloaded
    status, _ = zmq_single_request("status")
    plans_allowed_uid2 = status["plans_allowed_uid"]
    devices_allowed_uid2 = status["devices_allowed_uid"]
    assert plans_allowed_uid2 != devices_allowed_uid2

    resp3a, _ = zmq_single_request("plans_allowed", params={"user_group": _user_group})
    assert resp3a["success"] is True, f"resp={resp3a}"
    assert resp3a["plans_allowed_uid"] == plans_allowed_uid2
    plans_allowed2 = resp3a["plans_allowed"]

    resp3b, _ = zmq_single_request("devices_allowed", params={"user_group": _user_group})
    assert resp3b["success"] is True, f"resp={resp3b}"
    assert resp3b["devices_allowed_uid"] == devices_allowed_uid2
    devices_allowed2 = resp3b["devices_allowed"]

    assert isinstance(plans_allowed_uid2, str)
    assert isinstance(devices_allowed_uid2, str)

    assert plans_allowed_uid2 != plans_allowed_uid
    assert devices_allowed_uid2 != devices_allowed_uid

    if reload_plans_devices:
        assert "count50" in plans_allowed2
        assert "det50" in devices_allowed2
    else:
        assert "count50" not in plans_allowed2
        assert "det50" not in devices_allowed2
