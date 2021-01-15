import os
import inspect
import pytest

from ._common import copy_default_profile_collection, patch_first_startup_file
from bluesky_queueserver.manager.profile_tools import global_user_namespace, load_devices_from_happi
from bluesky_queueserver.manager.profile_ops import load_profile_collection


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
])
# fmt: on
def test_load_devices_from_happi_1(tmp_path, monkeypatch, device_names, loaded_names, kw_args, success, errmsg):
    """
    Tests for ``load_devices_from_happi``.
    """
    _configure_happi(tmp_path, monkeypatch, json_devices=_happi_json_db_1)

    # Load as a dictionary
    if success:
        ns = load_devices_from_happi(device_names, **kw_args)
        assert len(ns) == len(loaded_names)
        for d in loaded_names:
            assert d in ns
    else:
        with pytest.raises(Exception, match=errmsg):
            load_devices_from_happi(device_names, **kw_args)

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
