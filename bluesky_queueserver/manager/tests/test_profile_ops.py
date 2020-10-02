import os
import pytest
import shutil
import glob

import ophyd

from bluesky_queueserver.manager.profile_ops import (
    get_default_profile_collection_dir,
    load_profile_collection,
    plans_from_nspace,
    devices_from_nspace,
    parse_plan,
    gen_list_of_plans_and_devices,
    load_list_of_plans_and_devices,
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


def _copy_default_profile_collection(tmp_path):
    """
    Copy default profile collections (only .py files) to temporary directory.
    Returns the new temporary directory.
    """
    # Default path
    pc_path = get_default_profile_collection_dir()
    # New path
    new_pc_path = os.path.join(tmp_path, "startup")

    os.makedirs(new_pc_path, exist_ok=True)

    # Copy simulated profile collection (only .py files)
    file_pattern = os.path.join(pc_path, "[0-9][0-9]*.py")
    file_list = glob.glob(file_pattern)
    for fln in file_list:
        shutil.copy(fln, new_pc_path)

    return new_pc_path


def test_load_profile_collection_2(tmp_path):
    """
    Loading a copy of the default profile collection
    """
    pc_path = _copy_default_profile_collection(tmp_path)
    nspace = load_profile_collection(pc_path)
    assert len(nspace) > 0, "Failed to load the profile collection"


code_rel_import = """
from dir1.dir2.file2 import *
"""

def create_rel_imports_dirs(tmp_path):
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
@pytest.mark.parametrize("rel_imports", [False, True])
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
def test_load_profile_collection_3(tmp_path, rel_imports, additional_code, success, errmsg):
    """
    Loading a copy of the default profile collection
    """
    pc_path = _copy_default_profile_collection(tmp_path)

    create_rel_imports_dirs(pc_path)

    # Path to the first file (starts with 00)
    file_pattern = os.path.join(pc_path, "[0-9][0-9]*.py")
    file_list = glob.glob(file_pattern)
    file_list.sort()
    fln = file_list[0]

    with open(fln, "r") as file_in:
        code = file_in.readlines()

    with open(fln, "w") as file_out:
        if rel_imports:
            file_out.writelines(code_rel_import)
        file_out.writelines(additional_code)
        file_out.writelines(code)

    if success:
        nspace = load_profile_collection(pc_path)
        assert len(nspace) > 0, "Failed to load the profile collection"
        if rel_imports:
            assert "f1" in nspace, "Test for relative imports failed"
            assert "f2" in nspace, "Test for relative imports failed"
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
    pc_path = get_default_profile_collection_dir()

    # Copy simulated profile collection (only .py files)
    file_pattern = os.path.join(pc_path, "[0-9][0-9]*.py")
    file_list = glob.glob(file_pattern)
    for fln in file_list:
        shutil.copy(fln, tmp_path)

    # Check if profile collection was moved
    file_pattern = os.path.join(tmp_path, "[0-9][0-9]*.py")
    file_list = glob.glob(file_pattern)
    assert len(file_list) > 0, "Profile collection was not copied"

    fln_yaml = "list.yaml"
    gen_list_of_plans_and_devices(tmp_path, file_name=fln_yaml)
    assert os.path.isfile(os.path.join(tmp_path, fln_yaml)), "List of plans and devices was not created"

    # Attempt to overwrite the file
    with pytest.raises(RuntimeError, match="already exists. File overwriting is disabled."):
        gen_list_of_plans_and_devices(tmp_path, file_name=fln_yaml)

    # Allow file overwrite
    gen_list_of_plans_and_devices(tmp_path, file_name=fln_yaml, overwrite=True)


def test_load_list_of_plans_and_devices():
    """
    Loads the list of allowed plans and devices from simulated profile collection.
    """
    pc_path = get_default_profile_collection_dir()
    file_path = os.path.join(pc_path, "allowed_plans_and_devices.yaml")

    allowed_plans, allowed_devices = load_list_of_plans_and_devices(file_path)

    assert isinstance(allowed_plans, dict), "Incorrect type of 'allowed_plans'"
    assert len(allowed_plans) > 0, "List of allowed plans was not loaded"
    assert isinstance(allowed_devices, dict), "Incorrect type of 'allowed_devices'"
    assert len(allowed_devices) > 0, "List of allowed devices was not loaded"
