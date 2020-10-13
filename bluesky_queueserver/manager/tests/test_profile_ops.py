import os
import pytest
import copy
import yaml

import ophyd

from ._common import copy_default_profile_collection, patch_first_startup_file


from bluesky_queueserver.manager.profile_ops import (
    get_default_profile_collection_dir,
    load_profile_collection,
    plans_from_nspace,
    devices_from_nspace,
    parse_plan,
    gen_list_of_plans_and_devices,
    load_list_of_plans_and_devices,
    load_user_group_permissions,
    _process_plan,
    validate_plan,
)


def test_get_default_profile_collection_dir():
    """
    Function `get_default_profile_collection_dir`
    """
    pc_path = get_default_profile_collection_dir()
    assert os.path.exists(pc_path), "Directory with default profile collection deos not exist."


def test_load_profile_collection_1():
    """
    Loading default profile collection
    """
    pc_path = get_default_profile_collection_dir()
    nspace = load_profile_collection(pc_path)
    assert len(nspace) > 0, "Failed to load the profile collection"


def test_load_profile_collection_2(tmp_path):
    """
    Loading a copy of the default profile collection
    """
    pc_path = copy_default_profile_collection(tmp_path)
    nspace = load_profile_collection(pc_path)
    assert len(nspace) > 0, "Failed to load the profile collection"


code_local_import = """
from dir1.dir2.file2 import *
"""


def create_local_imports_dirs(tmp_path):
    path1 = os.path.join(tmp_path, "dir1")
    path2 = os.path.join(path1, "dir2")
    fln1 = os.path.join(path1, "file1.py")
    fln2 = os.path.join(path2, "file2.py")

    os.makedirs(path2, exist_ok=True)

    # Create file1
    code1 = """
def f1():
    pass
"""
    with open(fln1, "w") as f:
        f.writelines(code1)

        # Create file2
        code1 = """
from ..file1 import *

def f2():
    pass
"""
    with open(fln2, "w") as f:
        f.writelines(code1)


# fmt: off
@pytest.mark.parametrize("local_imports", [False, True])
@pytest.mark.parametrize("additional_code, success, errmsg", [
    # Patched as expected
    ("""
\n
from IPython import get_ipython

get_ipython().user_ns

""", True, ""),

    # Not patched ('get_ipython' is commented)
    ("""
\n
from IPython # import get_ipython

get_ipython().user_ns
""", False, "Profile calls 'get_ipython' before the patch was applied"),

    # using 'get_ipython' before it is patched
    ("""
\n
get_ipython().user_ns

""", False, "Profile calls 'get_ipython' before the patch was applied"),

    # Commented 'get_ipython' -> OK
    ("""
\n
a = 10  # get_ipython().user_ns
""", True, ""),

    # Raise exception in the profile
    ("""
\n
raise Exception("Manually raised exception.")

""", False, "Manually raised exception."),

])
# fmt: on
def test_load_profile_collection_3(tmp_path, local_imports, additional_code, success, errmsg):
    """
    Loading a copy of the default profile collection
    """
    pc_path = copy_default_profile_collection(tmp_path)

    create_local_imports_dirs(pc_path)

    patch_first_startup_file(pc_path, additional_code)
    if local_imports:
        # Note: local imports go above the additional code.
        patch_first_startup_file(pc_path, code_local_import)

    if success:
        nspace = load_profile_collection(pc_path)
        assert len(nspace) > 0, "Failed to load the profile collection"
        if local_imports:
            assert "f1" in nspace, "Test for local imports failed"
            assert "f2" in nspace, "Test for local imports failed"
    else:
        with pytest.raises(Exception, match=errmsg):
            load_profile_collection(pc_path)


def test_load_profile_collection_4_fail(tmp_path):
    """
    Failing cases
    """
    # Non-existing path
    pc_path = os.path.join(tmp_path, "abc")
    with pytest.raises(IOError, match="Path .+ does not exist"):
        load_profile_collection(pc_path)

    pc_path = os.path.join(tmp_path, "test.txt")
    # Create a file
    with open(pc_path, "w"):
        pass
    with pytest.raises(IOError, match="Path .+ is not a directory"):
        load_profile_collection(pc_path)


def test_plans_from_nspace():
    """
    Function 'plans_from_nspace' is extracting a subset of callable items from the namespace
    """
    pc_path = get_default_profile_collection_dir()
    nspace = load_profile_collection(pc_path)
    plans = plans_from_nspace(nspace)
    for name, plan in plans.items():
        assert callable(plan), f"Plan '{name}' is not callable"


def test_devices_from_nspace():
    """
    Function 'plans_from_nspace' is extracting a subset of callable items from the namespace
    """
    pc_path = get_default_profile_collection_dir()
    nspace = load_profile_collection(pc_path)
    devices = devices_from_nspace(nspace)
    for name, device in devices.items():
        assert isinstance(device, ophyd.Device), f"The object '{device}' is not an Ophyd device"


@pytest.mark.parametrize(
    "plan, success, err_msg",
    [
        ({"name": "count", "args": [["det1", "det2"]]}, True, ""),
        ({"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10]}, True, ""),
        ({"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 10, "delay": 1}}, True, ""),
        (
            {"name": "countABC", "args": [["det1", "det2"]]},
            False,
            "Plan 'countABC' is not allowed or does not exist.",
        ),
    ],
)
def test_parse_plan(plan, success, err_msg):

    pc_path = get_default_profile_collection_dir()
    nspace = load_profile_collection(pc_path)
    plans = plans_from_nspace(nspace)
    devices = devices_from_nspace(nspace)

    if success:
        plan_parsed = parse_plan(plan, allowed_plans=plans, allowed_devices=devices)
        expected_keys = ("name", "args", "kwargs")
        for k in expected_keys:
            assert k in plan_parsed, f"Key '{k}' does not exist: {plan_parsed.keys()}"
    else:
        with pytest.raises(RuntimeError, match=err_msg):
            parse_plan(plan, allowed_plans=plans, allowed_devices=devices)


def test_gen_list_of_plans_and_devices(tmp_path):
    """
    Copy simulated profile collection and generate the list of allowed (in this case available)
    plans and devices based on the profile collection
    """
    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=False)

    fln_yaml = "list.yaml"
    gen_list_of_plans_and_devices(pc_path, file_name=fln_yaml)
    assert os.path.isfile(os.path.join(pc_path, fln_yaml)), "List of plans and devices was not created"

    # Attempt to overwrite the file
    with pytest.raises(RuntimeError, match="already exists. File overwriting is disabled."):
        gen_list_of_plans_and_devices(pc_path, file_name=fln_yaml)

    # Allow file overwrite
    gen_list_of_plans_and_devices(pc_path, file_name=fln_yaml, overwrite=True)


def test_load_list_of_plans_and_devices():
    """
    Loads the list of allowed plans and devices from simulated profile collection.
    """
    pc_path = get_default_profile_collection_dir()
    file_path = os.path.join(pc_path, "existing_plans_and_devices.yaml")

    allowed_plans, allowed_devices = load_list_of_plans_and_devices(file_path)

    assert isinstance(allowed_plans, dict), "Incorrect type of 'allowed_plans'"
    assert len(allowed_plans) > 0, "List of allowed plans was not loaded"
    assert isinstance(allowed_devices, dict), "Incorrect type of 'allowed_devices'"
    assert len(allowed_devices) > 0, "List of allowed devices was not loaded"


_user_groups_text = """user_groups:
  root:  # The group includes all available plan and devices
    allowed:
      - null  # Allow all
    forbidden:
      - null  # Nothing is forbidden
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed:
      - ".*"  # A different way to allow all
    forbidden:
      - null  # Nothing is forbidden
  test_user:  # Users with limited access capabilities
    allowed:
      - "^count$"  # Use regular expression patterns
      - "scan$"
    forbidden:
      - "^adaptive_scan$" # Use regular expression patterns
      - "^inner_product"
"""

_user_groups_dict = {
    "user_groups": {
        "root": {"allowed": [None], "forbidden": [None]},
        "admin": {"allowed": [".*"], "forbidden": [None]},
        "test_user": {
            "allowed": ["^count$", "scan$"],
            "forbidden": ["^adaptive_scan$", "^inner_product"],
        },
    }
}


def test_load_user_group_permissions_1(tmp_path):
    """
    Create YAML file (with comments), load it and compare with the expected results.
    """
    path_to_file = os.path.join(tmp_path, "some_dir")
    os.makedirs(path_to_file, exist_ok=True)
    path_to_file = os.path.join(path_to_file, "user_permissions.yaml")
    with open(path_to_file, "w") as f:
        f.writelines(_user_groups_text)

    user_group_permissions = load_user_group_permissions(path_to_file)
    assert user_group_permissions == _user_groups_dict


def test_load_user_group_permissions_2_fail(tmp_path):
    """
    Function ``load_user_group_permissions``. Failed schema validation.
    """
    path_to_file = os.path.join(tmp_path, "some_dir")
    os.makedirs(path_to_file, exist_ok=True)
    path_to_file = os.path.join(path_to_file, "user_permissions.yaml")

    with pytest.raises(IOError, match=f"File '{path_to_file}' does not exist"):
        load_user_group_permissions(path_to_file)


def test_load_user_group_permissions_3_fail(tmp_path):
    """
    Function ``load_user_group_permissions``. Failed schema validation.
    """
    path_to_file = os.path.join(tmp_path, "some_dir")
    os.makedirs(path_to_file, exist_ok=True)
    path_to_file = os.path.join(path_to_file, "user_permissions.yaml")

    ug_dict = copy.deepcopy(_user_groups_dict)
    ug_dict["user_groups"]["test_user"]["something"] = ["a", "b"]

    with open(path_to_file, "w") as f:
        yaml.dump(ug_dict, f)

    with pytest.raises(IOError, match="Additional properties are not allowed"):
        load_user_group_permissions(path_to_file)


def test_load_user_group_permissions_4_fail(tmp_path):
    """
    Function ``load_user_group_permissions``. Failed schema validation.
    """
    path_to_file = os.path.join(tmp_path, "some_dir")
    os.makedirs(path_to_file, exist_ok=True)
    path_to_file = os.path.join(path_to_file, "user_permissions.yaml")

    ug_dict = copy.deepcopy(_user_groups_dict)
    ug_dict["unknown_key"] = ["a", "b"]

    with open(path_to_file, "w") as f:
        yaml.dump(ug_dict, f)

    with pytest.raises(IOError, match="Additional properties are not allowed"):
        load_user_group_permissions(path_to_file)


def test_load_user_group_permissions_5_fail(tmp_path):
    """
    Function ``load_user_group_permissions``. Failed schema validation.
    """
    path_to_file = os.path.join(tmp_path, "some_dir")
    os.makedirs(path_to_file, exist_ok=True)
    path_to_file = os.path.join(path_to_file, "user_permissions.yaml")

    ug_dict = copy.deepcopy(_user_groups_dict)
    ug_dict["user_groups"]["test_user"]["allowed"].append(50)

    with open(path_to_file, "w") as f:
        yaml.dump(ug_dict, f)

    with pytest.raises(IOError, match="is not of type 'string'"):
        load_user_group_permissions(path_to_file)


def _f1(a, b, c):
    pass


def _f2(*args, **kwargs):
    pass


def _f3(a, b, *args, c, d):
    pass


def _f4(a, b, *args, c, d=4):
    pass


def _f5(a, b=5, *args, c, d=4):
    pass


# fmt: off
@pytest.mark.parametrize("func, plan, success, errmsg", [
    (_f1, {"name": "nonexistent", "args": [1, 4, 5], "kwargs": {}}, False,
     "Plan 'nonexistent' is not in the list of allowed plans"),

    (_f1, {"name": "existing", "args": [1, 4, 5], "kwargs": {}}, True, ""),
    (_f1, {"name": "existing", "args": [1, 4], "kwargs": {"c": 5}}, True, ""),
    (_f1, {"name": "existing", "args": [], "kwargs": {"a": 1, "b": 4, "c": 5}}, True, ""),
    (_f1, {"name": "existing", "args": [], "kwargs": {"c": 1, "b": 4, "a": 5}}, True, ""),
    (_f1, {"name": "existing", "args": [1, 4], "kwargs": {}}, False,
     "Plan parameters do not contain required args or kwargs"),
    (_f1, {"name": "existing", "args": [], "kwargs": {}}, False,
     "Plan parameters do not contain required args or kwargs"),
    (_f1, {"name": "existing", "args": [1, 4, 6, 7], "kwargs": {}}, False,
     "The number of args exceeds the number of allowed parameters"),
    (_f1, {"name": "existing", "args": [1, 4, 6, 7], "kwargs": {"kw": 10}}, False,
     "The number of args exceeds the number of allowed parameters"),
    (_f1, {"name": "existing", "args": [1, 4], "kwargs": {"b": 10}}, False,
     "Unexpected kwargs ['b'] in the plan parameters"),

    (_f2, {"name": "existing", "args": [1, 4, 5], "kwargs": {}}, True, ""),
    (_f2, {"name": "existing", "args": [], "kwargs": {"a": 1, "b": 4, "c": 5}}, True, ""),
    (_f2, {"name": "existing", "args": [1, 4, 5], "kwargs": {"a": 1, "b": 4, "c": 5}}, True, ""),

    (_f2, {"name": "existing", "args": [1, 4, 5]}, True, ""),
    (_f2, {"name": "existing", "kwargs": {"a": 1, "b": 4, "c": 5}}, True, ""),

    (_f3, {"name": "existing", "args": [1, 4], "kwargs": {"c": 5, "d": 10}}, True, ""),
    (_f3, {"name": "existing", "args": [], "kwargs": {"a": 1, "b": 4, "c": 5, "d": 10}}, True, ""),
    (_f3, {"name": "existing", "args": [], "kwargs": {"a": 1, "b": 4, "d": 10}}, False,
     "Plan parameters do not contain required args or kwargs ['c']"),
    (_f3, {"name": "existing", "args": [6, 8], "kwargs": {"a": 1, "c": 4, "d": 10}}, False,
     "Unexpected kwargs ['a'] in the plan parameters"),

    (_f4, {"name": "existing", "args": [1, 4], "kwargs": {"c": 5, "d": 10}}, True, ""),
    (_f4, {"name": "existing", "args": [1, 4], "kwargs": {"c": 5}}, True, ""),
    (_f4, {"name": "existing", "args": [1, 4], "kwargs": {"d": 10}}, False,
     "Plan parameters do not contain required args or kwargs ['c']"),

    (_f5, {"name": "existing", "args": [1, 4], "kwargs": {"c": 5, "d": 10}}, True, ""),
    (_f5, {"name": "existing", "args": [1], "kwargs": {"c": 5, "d": 10}}, True, ""),
    (_f5, {"name": "existing", "args": [], "kwargs": {"c": 5, "d": 10}}, False,
     "Plan parameters do not contain required args or kwargs ['a']"),

])
# fmt: on
def test_validate_plan_1(func, plan, success, errmsg):
    """
    Tests for the plan validation algorithm.
    """
    allowed_plans = {"existing": _process_plan(func)}
    success_out, errmsg_out = validate_plan(plan, allowed_plans=allowed_plans)

    assert success_out == success, f"errmsg: {errmsg_out}"
    if success:
        assert errmsg_out == errmsg
    else:
        assert errmsg in errmsg_out


@pytest.mark.parametrize("allowed_plans", [None, {}])
def test_validate_plan_2(allowed_plans):
    """
    At this point all plans are considered valid if there is not list of allowed plans.
    """
    success_out, errmsg_out = validate_plan({}, allowed_plans=allowed_plans)
    assert success_out is True
    assert errmsg_out == ""
