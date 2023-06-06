import inspect
import os
import sys

import pytest

from bluesky_queueserver import gen_list_of_plans_and_devices
from bluesky_queueserver.manager.profile_ops import load_profile_collection
from bluesky_queueserver.manager.profile_tools import (
    clear_ipython_mode,
    clear_re_worker_active,
    global_user_namespace,
    is_ipython_mode,
    is_re_worker_active,
    load_devices_from_happi,
    set_ipython_mode,
    set_re_worker_active,
)

from ..comms import zmq_single_request
from .common import re_manager_cmd  # noqa: F401
from .common import (
    _user,
    _user_group,
    condition_environment_closed,
    condition_environment_created,
    condition_manager_idle,
    copy_default_profile_collection,
    patch_first_startup_file,
    use_ipykernel_for_tests,
    wait_for_condition,
)


def create_local_imports_files(tmp_path):
    path_dir = os.path.join(tmp_path, "dir_local_imports")
    fln_func = os.path.join(path_dir, "file_func.py")
    fln_gen = os.path.join(path_dir, "file_gen.py")

    os.makedirs(path_dir, exist_ok=True)

    # Create file1
    code1 = """
from bluesky_queueserver.manager.profile_tools import set_user_ns

# Function that has the parameter 'ipython'
@set_user_ns
def f1(some_value, user_ns, ipython):
    user_ns["func_was_called"] = "func_was_called"
    return (some_value, user_ns["v_from_namespace"], bool(ipython))

# Function that has no parameter 'ipython'
@set_user_ns
def f1a(some_value, user_ns):
    user_ns["func_A_was_called"] = "func_was_called"
    return (some_value, user_ns["v_from_namespace"])

"""
    with open(fln_func, "w") as f:
        f.writelines(code1)

    # Create file2
    code2 = """
from bluesky_queueserver.manager.profile_tools import set_user_ns

# Function that has the parameter 'ipython'
@set_user_ns
def f2(some_value, user_ns, ipython):
    user_ns["gen_was_called"] = "gen_was_called"
    yield (some_value, user_ns["v_from_namespace"], bool(ipython))

# Function that has no parameter 'ipython'
@set_user_ns
def f2a(some_value, user_ns):
    user_ns["gen_A_was_called"] = "gen_was_called"
    yield (some_value, user_ns["v_from_namespace"])

@set_user_ns
def f3(some_value, user_ns, ipython):
    user_ns["value_f3"] = some_value

f3(91)

"""
    with open(fln_gen, "w") as f:
        f.writelines(code2)


patch_code = """
from dir_local_imports.file_func import f1, f1a
from dir_local_imports.file_gen import f2, f2a

from bluesky_queueserver.manager.profile_tools import set_user_ns

@set_user_ns
def f4(some_value, user_ns, ipython):
    user_ns["value_f4"] = some_value

f4(90)

"""


def test_set_user_ns_1(tmp_path):
    """
    Tests for ``set_user_ns`` decorator. The functionality of the decorator
    is fully tested (only without IPython):
    - using ``global_user_namespace`` to pass values in and out of the function
      defined in the imported module (emulation of ``get_ipython().user_ns``).
    - checking if the function is executed from IPython (only for the function
      defined in the imported module).
    """
    pc_path = copy_default_profile_collection(tmp_path)

    create_local_imports_files(pc_path)
    patch_first_startup_file(pc_path, patch_code)

    nspace = load_profile_collection(pc_path)
    assert len(nspace) > 0, "Failed to load the profile collection"
    assert "f1" in nspace, "Test for local imports failed"
    assert "f2" in nspace, "Test for local imports failed"

    # Test if the decorator `set_user_ns` does not change function type
    assert inspect.isgeneratorfunction(nspace["f1"]) is False
    assert inspect.isgeneratorfunction(nspace["f2"]) is True

    # Check if the extra arguments are removed from the function signature
    def check_signature(func):
        params = inspect.signature(func).parameters
        assert "user_ns" not in params
        assert "ipython" not in params

    check_signature(nspace["f1"])
    check_signature(nspace["f1a"])
    check_signature(nspace["f2"])
    check_signature(nspace["f2a"])

    assert nspace["value_f3"] == 91
    assert nspace["value_f4"] == 90

    # Test function
    global_user_namespace.set_user_namespace(user_ns=nspace, use_ipython=False)
    global_user_namespace.user_ns["v_from_namespace"] = "value-sent-to-func"
    assert nspace["v_from_namespace"] == "value-sent-to-func"

    result_func = nspace["f1"](60)
    assert nspace["func_was_called"] == "func_was_called"
    assert result_func[0] == 60
    assert result_func[1] == "value-sent-to-func"
    assert result_func[2] is False

    result_func = nspace["f1a"](65)
    assert nspace["func_A_was_called"] == "func_was_called"
    assert result_func[0] == 65
    assert result_func[1] == "value-sent-to-func"

    # Test generator
    global_user_namespace.user_ns["v_from_namespace"] = "value-sent-to-gen"
    result_func = list(nspace["f2"](110))[0]
    assert nspace["gen_was_called"] == "gen_was_called"
    assert result_func[0] == 110
    assert result_func[1] == "value-sent-to-gen"
    assert result_func[2] is False

    result_func = list(nspace["f2a"](115))[0]
    assert nspace["gen_A_was_called"] == "gen_was_called"
    assert result_func[0] == 115
    assert result_func[1] == "value-sent-to-gen"


def test_global_user_namespace():
    """
    Basic test for ``global_user_namespace``.
    """
    ns = {"ab": 1, "cd": 2}
    global_user_namespace.set_user_namespace(user_ns=ns)
    assert global_user_namespace.user_ns == ns
    assert global_user_namespace.use_ipython is False

    global_user_namespace.set_user_namespace(user_ns={}, use_ipython=True)
    assert global_user_namespace.user_ns == {}
    assert global_user_namespace.use_ipython is True

    global_user_namespace.set_user_namespace(user_ns=ns, use_ipython=False)
    assert global_user_namespace.user_ns == ns
    assert global_user_namespace.use_ipython is False


_happi_json_db_1 = """
{
  "det": {
    "_id": "det",
    "active": true,
    "args": [],
    "device_class": "ophyd.sim.DetWithCountTime",
    "documentation": null,
    "kwargs": {
      "name": "{{name}}"
    },
    "name": "det",
    "type": "OphydItem"
  },
  "motor": {
    "_id": "motor",
    "active": true,
    "args": [],
    "device_class": "ophyd.sim.SynAxisNoPosition",
    "documentation": null,
    "kwargs": {
      "name": "{{name}}"
    },
    "name": "motor",
    "type": "OphydItem"
  },
  "motor1": {
    "_id": "motor1",
    "active": true,
    "args": [],
    "device_class": "ophyd.sim.SynAxisNoHints",
    "documentation": null,
    "kwargs": {
      "name": "{{name}}"
    },
    "name": "motor1",
    "type": "OphydItem"
  },
  "tst_motor2": {
    "_id": "tst_motor2",
    "active": true,
    "args": [],
    "device_class": "ophyd.sim.SynAxisNoHints",
    "documentation": null,
    "kwargs": {
      "name": "{{name}}"
    },
    "name": "tst_motor2",
    "type": "OphydItem"
  },
  "motor3": {
    "_id": "motor3",
    "active": true,
    "args": [],
    "device_class": "ophyd.sim.SynAxis",
    "documentation": null,
    "kwargs": {
      "name": "{{name}}"
    },
    "name": "motor3",
    "type": "OphydItem"
  },
  "motor3_duplicate_error": {
    "_id": "motor3",
    "active": false,
    "args": [],
    "device_class": "ophyd.sim.SynAxis",
    "documentation": null,
    "kwargs": {
      "name": "{{name}}"
    },
    "name": "motor3",
    "type": "OphydItem"
  }
}
"""


def _configure_happi(tmp_path, monkeypatch, json_devices):
    path_json = os.path.join(tmp_path, "sim_devices.json")
    path_ini = os.path.join(tmp_path, "happi.ini")

    happi_ini_text = f"[DEFAULT]\nbackend=json\npath={path_json}"

    with open(path_ini, "w") as f:
        f.write(happi_ini_text)

    with open(path_json, "w") as f:
        f.write(json_devices)

    monkeypatch.setenv("HAPPI_CFG", path_ini)


# fmt: off
@pytest.mark.parametrize("device_names, loaded_names, kw_args, success, errmsg", [
    ([], [], {}, True, ""),  # No devices are loaded if the list of devices is empty
    (("det", "motor"), ("det", "motor"), {}, True, ""),
    (["det", "motor"], ("det", "motor"), {}, True, ""),
    ((("det", ""), ["motor", ""]), ("det", "motor"), {}, True, ""),
    (("det", ["motor", ""]), ("det", "motor"), {}, True, ""),
    (("det", ("motor", ""), ("tst_motor2", "motor2")), ("det", "motor", "motor2"), {}, True, ""),
    # This is not typical use case, but the same device may be loaded multiple times
    #   with different names if needed.
    ((("motor1", "motor1_copy1"), ("motor1", "motor1_copy2")), ("motor1_copy1", "motor1_copy2"), {}, True, ""),
    # Incorrect type of the device list
    (10, ("det", "motor"), {}, False, "Parameter 'device_names' value must be a tuple or a list"),
    ("string", ("det", "motor"), {}, False, "Parameter 'device_names' value must be a tuple or a list"),
    # Incorrecty type or form of a device list element
    (("det", 10), ("det", "motor"), {}, False, "Parameter 'device_names': element .* must be str, tuple or list"),
    ((10, "motor"), ("det", "motor"), {}, False,
     "Parameter 'device_names': element .* must be str, tuple or list"),
    (("det", (10, "motor2")), ("det", "motor"), {}, False, "element .* is expected to be in the form"),
    (("det", ("tst_motor2", 10)), ("det", "motor"), {}, False, "element .* is expected to be in the form"),
    (("det", ("tst_motor2", "motor2", 10)), ("det", "motor"), {}, False,
     "element .* is expected to be in the form"),
    # No device found
    (("det", "motor10"), ("det", "motor10"), {}, False, "No devices with name"),
    # Multiple devices found (search for "motor3" yields multile devices, this is database issue)
    (("det", "motor3"), ("det", "motor3"), {}, False, "Multiple devices with name"),
    # Use additional search parameters. (Two entries for "motor3" differ in the value of `active` field.
    #   A single entry for `det` has `active==True`.)
    (("det", "motor3"), ("det", "motor3"), {"active": True}, True, ""),
    (("det", "motor3"), ("det", "motor3"), {"active": False}, False,
     "No devices with name 'det' were found in Happi database."),
    (("motor3",), ("motor3",), {"active": False}, True, ""),
    # Verify that valid device names are accepted
    (("det", ["motor", "motor3_new"]), ("det", "motor3_new"), {}, True, ""),
    # Invalid new device name
    (("det", ["motor", "Motor"]), ("det", "motor"), {}, False, "may consist of lowercase letters, numbers"),
    (("det", ["motor", "moTor"]), ("det", "motor"), {}, False, "may consist of lowercase letters, numbers"),
    (("det", ["motor", "_motor"]), ("det", "motor"), {}, False, "may consist of lowercase letters, numbers"),
    (("det", ["motor", " motor"]), ("det", "motor"), {}, False, "may consist of lowercase letters, numbers"),
    (("det", ["motor", "motor "]), ("det", "motor"), {}, False, "may consist of lowercase letters, numbers"),
    (("det", ["motor", "motor new"]), ("det", "motor"), {}, False, "may consist of lowercase letters, numbers"),
    (("det", ["motor", "motor_$new"]), ("det", "motor"), {}, False, "may consist of lowercase letters, numbers"),
    (("det", ["motor", "2motor_$new"]), ("det", "motor"), {}, False, "may consist of lowercase letters, numbers"),
])
# fmt: on
def test_load_devices_from_happi_1(tmp_path, monkeypatch, device_names, loaded_names, kw_args, success, errmsg):
    """
    Tests for ``load_devices_from_happi``.
    """
    _configure_happi(tmp_path, monkeypatch, json_devices=_happi_json_db_1)

    # Load as a dictionary
    if success:
        ns = {}
        dlist = load_devices_from_happi(device_names, namespace=ns, **kw_args)
        assert len(ns) == len(loaded_names), str(ns)
        for d in loaded_names:
            assert d in ns
        assert set(dlist) == set(loaded_names)
    else:
        with pytest.raises(Exception, match=errmsg):
            ns = {}
            load_devices_from_happi(device_names, namespace=ns, **kw_args)

    # Load in local namespace
    def _test_loading(device_names, loaded_names):
        if success:
            load_devices_from_happi(device_names, namespace=locals(), **kw_args)
            for d in loaded_names:
                assert d in locals()
        else:
            with pytest.raises(Exception, match=errmsg):
                load_devices_from_happi(device_names, namespace=locals(), **kw_args)

    _test_loading(device_names=device_names, loaded_names=loaded_names)


def test_load_devices_from_happi_2_fail(tmp_path, monkeypatch):
    """
    Function ``load_devices_from_happi``: parameter ``namespace`` is required and must be of type ``dict``.
    """
    _configure_happi(tmp_path, monkeypatch, json_devices=_happi_json_db_1)

    # Missing 'namespace' parameter
    with pytest.raises(TypeError, match="missing 1 required keyword-only argument: 'namespace'"):
        load_devices_from_happi(["det", "motor"])

    # Incorrect type of 'namespace' parameter
    with pytest.raises(TypeError, match="Parameter 'namespace' must be a dictionary"):
        load_devices_from_happi(["det", "motor"], namespace=[1, 2, 3])


def test_is_re_worker_active_1(monkeypatch):  # noqa: F811
    """
    Basic tests for ``set_re_worker_active``, ``clear_re_worker_active`` and ``is_re_worker_active`` functions.
    """
    monkeypatch.setattr(os, "environ", os.environ.copy())
    assert is_re_worker_active() is False
    set_re_worker_active()
    assert is_re_worker_active() is True
    clear_re_worker_active()
    assert is_re_worker_active() is False


def test_is_ipython_mode_1(monkeypatch):  # noqa: F811
    """
    Basic tests for ``set_ipython_mode``, ``clear_ipython_mode`` and ``is_ipython_mode`` functions.
    If the code is not executed in the worker (worker is not active), then the function checks if
    it is called from IPython kernel, otherwise the returned value depends on the value of
    the respective environment variable.
    """
    monkeypatch.setattr(os, "environ", os.environ.copy())
    assert is_ipython_mode() is False

    set_re_worker_active()
    set_ipython_mode(True)
    assert is_ipython_mode() is True
    set_ipython_mode(False)
    assert is_ipython_mode() is False
    set_ipython_mode(True)
    assert is_ipython_mode() is True
    clear_ipython_mode()
    assert is_ipython_mode() is False

    clear_re_worker_active()
    assert is_ipython_mode() is False


# Minimalistic user permissions sufficient to start RE Manager
_user_groups_text = r"""user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - null  # Allow all
    forbidden_devices:
      - null  # Nothing is forbidden
  primary:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - null  # Allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - null  # Allow all
    forbidden_devices:
      - null  # Nothing is forbidden
"""


def _get_startup_script_1(use_relative_imports):
    startup_script = """
from ophyd.sim import det1, det2
from bluesky.plans import count

from bluesky_queueserver.manager.profile_tools import is_re_worker_active
from {}dir1.file1 import f1

# Executed during import
if not is_re_worker_active():
    raise Exception("Importing startup script: RE Worker is not detected as active")

def sim_plan_1():
    '''
    Simple plan for tests.
    '''
    if f1() is not True:
        raise Exception("sim_plan_1: Function 'f1' did not return correct result.")

    if not is_re_worker_active():
        raise Exception("sim_plan_1: RE Worker is not detected as active")

    yield from count([det1, det2])
"""
    return startup_script.format("." if use_relative_imports else "")


def create_local_imports_dir(pc_path):
    path1 = os.path.join(pc_path, "dir1")
    fln1 = os.path.join(path1, "file1.py")

    # If the following modules were already imported during tests, then they
    # will interfere with the new imports. So just delete them.
    modules_to_delete = ["dir1", "dir1.file1"]
    for m in modules_to_delete:
        if m in sys.modules:
            del sys.modules[m]

    os.makedirs(path1, exist_ok=True)

    # Create file1
    code1 = """
from bluesky_queueserver.manager.profile_tools import is_re_worker_active

# Executed during import
if not is_re_worker_active():
    raise Exception("Importing module 'file1': RE Worker is not detected as active")

def f1():
    if not is_re_worker_active():
        raise Exception("Function f1: RE Worker is not detected as active")
    return True
"""
    with open(fln1, "w") as f:
        f.writelines(code1)


# fmt: off
@pytest.mark.parametrize("option", ["startup_dir", "script", "module"])
# fmt: on
def test_is_re_worker_active_2(re_manager_cmd, tmp_path, monkeypatch, option):  # noqa: F811
    """
    Test that ``is_re_worker_active()`` API is working as expected in startup scripts.
    The sample startup scripts are modified to call ``is_re_worker_active()`` during import
    and during execution of a plan. The script is also importing the local module, which
    is calling the API during import and in the body of a function that is called during
    the plan execution. Two modes of operation are tested: (1) loading the profile collection
    in the current process (``gen_list_of_plans_and_devices`` function); (2) loading
    of startup profile collection and execution of the plan in RE Worker environment.
    """
    monkeypatch.setattr(os, "environ", os.environ.copy())

    # Load first script
    ipython_dir = os.path.join(tmp_path, "ipython")
    startup_profile = "collection_sim"
    startup_dir = os.path.join(ipython_dir, f"profile_{startup_profile}", "startup")
    script_dir = os.path.join(tmp_path, "script_dir", "script")
    script_name = "startup_script.py"
    script_path = os.path.join(script_dir, script_name)
    module_dir = os.path.join(tmp_path, "script_dir")

    fdir = startup_dir if option == "startup_dir" else script_dir
    fpath = os.path.join(fdir, script_name)

    os.makedirs(startup_dir, exist_ok=True)
    os.makedirs(script_dir, exist_ok=True)

    with open(fpath, "w") as f:
        f.write(_get_startup_script_1(option == "module"))

    create_local_imports_dir(fdir)

    path_user_permissions = os.path.join(startup_dir, "user_group_permissions.yaml")
    with open(path_user_permissions, "w") as f:
        f.writelines(_user_groups_text)

    path_existing_plans_and_devices = os.path.join(startup_dir, "existing_plans_and_devices.yaml")

    # Make sure that 'is_re_worker_active()' works as expected when the list of plans and devices is
    #   created (profile collection is loaded in the current process).
    assert is_re_worker_active() is False
    if option == "startup_dir":
        gen_list_of_plans_and_devices(startup_dir=startup_dir, file_dir=startup_dir, overwrite=True)
    elif option == "script":
        gen_list_of_plans_and_devices(startup_script_path=script_path, file_dir=startup_dir, overwrite=True)
    elif option == "module":
        # Temporarily add module to the search path
        sys_path = sys.path
        monkeypatch.setattr(sys, "path", [str(module_dir)] + sys_path)
        # monkeypatch.setenv("PYTHONPATH", str(module_dir))
        gen_list_of_plans_and_devices(
            startup_module_name="script.startup_script", file_dir=startup_dir, overwrite=True
        )
    else:
        assert False, f"Unknown option '{option}'"
    assert is_re_worker_active() is False  # Make sure that that 'active' state is cleared

    # When IPython kernel is used, point it to an empty profile
    extra_params = []
    if use_ipykernel_for_tests():
        extra_params.extend(["--ipython-dir", ipython_dir, "--startup-profile", startup_profile])

    if option == "startup_dir":
        re_manager_cmd(["--startup-dir", startup_dir])
    elif option == "script":
        re_manager_cmd(
            [
                "--startup-script",
                script_path,
                "--user-group-permissions",
                path_user_permissions,
                "--existing-plans-devices",
                path_existing_plans_and_devices,
                *extra_params,
            ]
        )
    elif option == "module":
        # The manager will still start, but the environment can not load, because
        #   the module 'script_dir1.startup_script' is not installed.
        re_manager_cmd(
            [
                "--startup-module",
                "script.startup_script",
                "--user-group-permissions",
                path_user_permissions,
                "--existing-plans-devices",
                path_existing_plans_and_devices,
                *extra_params,
            ]
        )
        # Since the environment can not be loaded, the rest of the test will not work.
        #   We successfully loaded the module in `gen_list_of_plans_and_devices()`, so
        #   let's consider it success and interrupt the test.
        return
    else:
        assert False, f"Unknown option '{option}'"

    # Open environment and execute plan 'sim_plan_1'
    resp1, _ = zmq_single_request("permissions_reload", {"restore_plans_devices": True})
    assert resp1["success"] is True, f"resp={resp1}"

    # Add plan to the queue
    params = {"item": {"name": "sim_plan_1", "item_type": "plan"}, "user": _user, "user_group": _user_group}
    resp2, _ = zmq_single_request("queue_item_add", params)
    assert resp2["success"] is True, f"resp={resp2}"

    # Open the environment
    resp3, _ = zmq_single_request("environment_open")
    assert resp3["success"] is True
    assert wait_for_condition(time=10, condition=condition_environment_created)

    # Make sure that the status is not set in the current process
    assert is_re_worker_active() is False

    # Make sure that the run is in the queue and the history is empty
    status, _ = zmq_single_request("status")
    assert status["items_in_queue"] == 1
    assert status["items_in_history"] == 0

    # Start the queue
    resp4, _ = zmq_single_request("queue_start")
    assert resp4["success"] is True

    assert wait_for_condition(time=5, condition=condition_manager_idle)

    # Make sure that the run was executed successfully
    status, _ = zmq_single_request("status")
    assert status["items_in_queue"] == 0
    assert status["items_in_history"] == 1

    # Make sure that plan was completed successfully
    resp5, _ = zmq_single_request("history_get")
    assert resp5["success"] is True
    history = resp5["items"]
    assert history[-1]["result"]["exit_status"] == "completed"

    # Close the environment
    resp6, _ = zmq_single_request("environment_close")
    assert resp6["success"] is True, f"resp={resp6}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)
