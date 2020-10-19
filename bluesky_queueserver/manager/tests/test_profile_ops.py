import os
import pytest
import copy
import yaml
import pickle
import typing

import ophyd

from ._common import copy_default_profile_collection, patch_first_startup_file

from bluesky_queueserver.manager.annotation_decorator import parameter_annotation_decorator

from bluesky_queueserver.manager.profile_ops import (
    get_default_profile_collection_dir,
    load_profile_collection,
    plans_from_nspace,
    devices_from_nspace,
    parse_plan,
    gen_list_of_plans_and_devices,
    load_existing_plans_and_devices,
    load_user_group_permissions,
    _process_plan,
    validate_plan,
    _select_allowed_items,
    load_allowed_plans_and_devices,
    hex2bytes,
    bytes2hex,
    _prepare_plans,
    _prepare_devices,
    _unpickle_types,
)


def test_hex2bytes_bytes2hex():
    """
    Basic test for the functions ``hex2bytes`` and ``bytes2hex``.
    """
    dict_initial = {"abc": 50, "def": {"some_key": "some_value"}}

    # Check if pickling/unpickling a dictionary works.
    b_in = pickle.dumps(dict_initial)
    s = bytes2hex(b_in)
    assert isinstance(s, str)
    assert len(s) == 3 * len(b_in) - 1
    b_out = hex2bytes(s)
    assert b_out == b_in
    dict_result = pickle.loads(b_out)
    assert dict_result == dict_initial


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


def test_load_existing_plans_and_devices():
    """
    Loads the list of allowed plans and devices from simulated profile collection.
    """
    pc_path = get_default_profile_collection_dir()
    file_path = os.path.join(pc_path, "existing_plans_and_devices.yaml")

    existing_plans, existing_devices = load_existing_plans_and_devices(file_path)

    assert isinstance(existing_plans, dict), "Incorrect type of 'allowed_plans'"
    assert len(existing_plans) > 0, "List of allowed plans was not loaded"
    assert isinstance(existing_devices, dict), "Incorrect type of 'allowed_devices'"
    assert len(existing_devices) > 0, "List of allowed devices was not loaded"

    existing_plans, existing_devices = load_existing_plans_and_devices(None)
    assert existing_plans == {}
    assert existing_devices == {}


def test_unpickle_items():
    """
    Simple test for ``_unpickle_items()``.
    """
    # Dictionary that contains pickled values. The dictionary may contain lists (tuples)
    #   of dictionaries, so the conversion function must be able to handle the lists.
    item_dict_pickled = {
        "a": "abc",
        "b": typing.Any,
        "b_pickled": bytes2hex(pickle.dumps(typing.Any)),
        "e": {
            "f": {
                "a": "abc",
                "b": typing.List[typing.Any],
                "b_pickled": bytes2hex(pickle.dumps(typing.List[typing.Any])),
            },
            "g": [
                {
                    "d": {
                        "p_pickled": bytes2hex(pickle.dumps(typing.Union[float, str])),
                    }
                }
            ],
        },
    }

    # The dictionary with raw binary items.
    item_dict = copy.deepcopy(item_dict_pickled)
    item_dict["b_pickled"] = typing.Any
    item_dict["e"]["f"]["b_pickled"] = typing.List[typing.Any]
    item_dict["e"]["g"][0]["d"]["p_pickled"] = typing.Union[float, str]

    _unpickle_types(item_dict_pickled)
    assert item_dict_pickled == item_dict


def test_verify_default_profile_collection():
    """
    Verify if the list of existing plans and devices matches current default profile collection.
    This test may fail if the the algorithm for generating the lists, the set of built-in
    bluesky plans or simulated Ophyd devices was changed. Generate the new list to fix the
    issue.
    """
    # Create dictionaries of existing plans and devices. Apply all preprocessing steps.
    pc_path = get_default_profile_collection_dir()
    nspace = load_profile_collection(pc_path)

    plans = plans_from_nspace(nspace)
    plans = _prepare_plans(plans)

    devices = devices_from_nspace(nspace)
    devices = _prepare_devices(devices)

    # Read the list of the existing plans of devices
    file_path = os.path.join(pc_path, "existing_plans_and_devices.yaml")
    existing_plans, existing_devices = load_existing_plans_and_devices(file_path)

    # The types must be unpicked before they could be compared (pickling format may
    #   differ depending on Python version)
    _unpickle_types(plans)
    _unpickle_types(devices)
    _unpickle_types(existing_plans)
    _unpickle_types(existing_devices)

    # Compare
    assert set(plans.keys()) == set(existing_plans.keys())
    assert set(devices) == set(existing_devices)

    assert plans == existing_plans
    assert devices == existing_devices


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
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ".*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ".*"  # A different way to allow all
    forbidden_devices:
      - null  # Nothing is forbidden
  test_user:  # Users with limited access capabilities
    allowed_plans:
      - "^count$"  # Use regular expression patterns
      - "scan$"
    forbidden_plans:
      - "^adaptive_scan$" # Use regular expression patterns
      - "^inner_product"
    allowed_devices:
      - "^det"  # Use regular expression patterns
      - "^motor"
    forbidden_devices:
      - "^det[3-5]$" # Use regular expression patterns
      - "^motor\\d+$"
"""

_user_groups_dict = {
    "user_groups": {
        "root": {
            "allowed_plans": [None],
            "forbidden_plans": [None],
            "allowed_devices": [None],
            "forbidden_devices": [None],
        },
        "admin": {
            "allowed_plans": [".*"],
            "forbidden_plans": [None],
            "allowed_devices": [".*"],
            "forbidden_devices": [None],
        },
        "test_user": {
            "allowed_plans": ["^count$", "scan$"],
            "forbidden_plans": ["^adaptive_scan$", "^inner_product"],
            "allowed_devices": ["^det", "^motor"],
            "forbidden_devices": ["^det[3-5]$", r"^motor\d+$"],
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


@pytest.mark.parametrize("group_to_delete", ["root", "admin"])
def test_load_user_group_permissions_5_fail(tmp_path, group_to_delete):
    """
    Function ``load_user_group_permissions``. Failed schema validation.
    """
    path_to_file = os.path.join(tmp_path, "some_dir")
    os.makedirs(path_to_file, exist_ok=True)
    path_to_file = os.path.join(path_to_file, "user_permissions.yaml")

    ug_dict = copy.deepcopy(_user_groups_dict)
    ug_dict["user_groups"].pop(group_to_delete)

    with open(path_to_file, "w") as f:
        yaml.dump(ug_dict, f)

    with pytest.raises(IOError, match="Missing required user group"):
        load_user_group_permissions(path_to_file)


def test_load_user_group_permissions_6_fail(tmp_path):
    """
    Function ``load_user_group_permissions``. Failed schema validation.
    """
    path_to_file = os.path.join(tmp_path, "some_dir")
    os.makedirs(path_to_file, exist_ok=True)
    path_to_file = os.path.join(path_to_file, "user_permissions.yaml")

    ug_dict = copy.deepcopy(_user_groups_dict)
    ug_dict["user_groups"]["test_user"]["allowed_plans"].append(50)

    with open(path_to_file, "w") as f:
        yaml.dump(ug_dict, f)

    with pytest.raises(IOError, match="is not of type 'string'"):
        load_user_group_permissions(path_to_file)


# fmt: off
@pytest.mark.parametrize("item_dict, allow_patterns, disallow_patterns, result", [
    ({"abc34": 1, "abcd": 2}, [r"^abc"], [r"^abc\d+$"], {"abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [r"^abc"], [r"^abc.*$"], {}),
    ({"abc34": 1, "abcd": 2}, [r"^abc"], [r"^abcde$", r"^abc.*$"], {}),
    ({"abc34": 1, "abcd": 2}, [r"^abc"], [r"^abcde$", r"^a.2$"], {"abc34": 1, "abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [r"d$", r"4$"], [r"^abcde$", r"^a.2$"], {"abc34": 1, "abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [None], [r"^abc\d+$"], {"abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [r"^abc"], [None], {"abc34": 1, "abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [None], [None], {"abc34": 1, "abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [], [None], {}),
    ({"abc34": 1, "abcd": 2}, [None], [], {"abc34": 1, "abcd": 2}),
    ({}, [r"^abc"], [r"^abc\d+$"], {}),
])
# fmt: on
def test_select_allowed_items(item_dict, allow_patterns, disallow_patterns, result):
    """
    Tests for ``_select_allowed_items``.
    """
    r = _select_allowed_items(item_dict, allow_patterns, disallow_patterns)
    assert r == result


# fmt: off
@pytest.mark.parametrize("fln_existing_items, fln_user_groups, empty_dict, all_users", [
    ("existing_plans_and_devices.yaml", "user_group_permissions.yaml", False, True),
    ("existing_plans_and_devices.yaml", None, False, False),
    (None, "user_group_permissions.yaml", True, True),
    (None, None, True, False),
])
# fmt: on
def test_load_allowed_plans_and_devices(fln_existing_items, fln_user_groups, empty_dict, all_users):
    """"""
    pc_path = get_default_profile_collection_dir()

    fln_existing_items = None if (fln_existing_items is None) else os.path.join(pc_path, fln_existing_items)
    fln_user_groups = None if (fln_user_groups is None) else os.path.join(pc_path, fln_user_groups)

    allowed_plans, allowed_devices = load_allowed_plans_and_devices(
        path_existing_plans_and_devices=fln_existing_items, path_user_group_permissions=fln_user_groups
    )

    if empty_dict:
        assert allowed_plans == {}
        assert allowed_devices == {}
    else:
        assert "root" in allowed_plans
        assert "root" in allowed_devices
        assert allowed_plans["root"]
        assert allowed_devices["root"]

        if all_users:
            assert "admin" in allowed_plans
            assert "admin" in allowed_devices
            assert allowed_plans["admin"]
            assert allowed_devices["admin"]
            assert "test_user" in allowed_plans
            assert "test_user" in allowed_devices
            assert allowed_plans["test_user"]
            assert allowed_devices["test_user"]
        else:
            assert "admin" not in allowed_plans
            assert "admin" not in allowed_devices
            assert "test_user" not in allowed_plans
            assert "test_user" not in allowed_devices


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
     "Plan validation failed: missing a required argument: 'c'"),
    (_f1, {"name": "existing", "args": [], "kwargs": {}}, False,
     "Plan validation failed: missing a required argument: 'a'"),
    (_f1, {"name": "existing", "args": [1, 4, 6, 7], "kwargs": {}}, False,
     "Plan validation failed: too many positional arguments"),
    (_f1, {"name": "existing", "args": [1, 4, 6, 7], "kwargs": {"kw": 10}}, False,
     "Plan validation failed: too many positional arguments"),
    (_f1, {"name": "existing", "args": [1, 4], "kwargs": {"b": 10}}, False,
     "Plan validation failed: multiple values for argument 'b'"),

    (_f2, {"name": "existing", "args": [1, 4, 5], "kwargs": {}}, True, ""),
    (_f2, {"name": "existing", "args": [], "kwargs": {"a": 1, "b": 4, "c": 5}}, True, ""),
    (_f2, {"name": "existing", "args": [1, 4, 5], "kwargs": {"a": 1, "b": 4, "c": 5}}, True, ""),

    (_f2, {"name": "existing", "args": [1, 4, 5]}, True, ""),
    (_f2, {"name": "existing", "kwargs": {"a": 1, "b": 4, "c": 5}}, True, ""),

    (_f3, {"name": "existing", "args": [1, 4], "kwargs": {"c": 5, "d": 10}}, True, ""),
    (_f3, {"name": "existing", "args": [], "kwargs": {"a": 1, "b": 4, "c": 5, "d": 10}}, True, ""),
    (_f3, {"name": "existing", "args": [], "kwargs": {"a": 1, "b": 4, "d": 10}}, False,
     "Plan validation failed: missing a required argument: 'c'"),
    (_f3, {"name": "existing", "args": [6, 8], "kwargs": {"a": 1, "c": 4, "d": 10}}, False,
     "Plan validation failed: multiple values for argument 'a'"),

    (_f4, {"name": "existing", "args": [1, 4], "kwargs": {"c": 5, "d": 10}}, True, ""),
    (_f4, {"name": "existing", "args": [1, 4], "kwargs": {"c": 5}}, True, ""),
    (_f4, {"name": "existing", "args": [1, 4], "kwargs": {"d": 10}}, False,
     "Plan validation failed: missing a required argument: 'c'"),

    (_f5, {"name": "existing", "args": [1, 4], "kwargs": {"c": 5, "d": 10}}, True, ""),
    (_f5, {"name": "existing", "args": [1], "kwargs": {"c": 5, "d": 10}}, True, ""),
    (_f5, {"name": "existing", "args": [], "kwargs": {"c": 5, "d": 10}}, False,
     "Plan validation failed: missing a required argument: 'a"),

])
# fmt: on
def test_validate_plan_1(func, plan, success, errmsg):
    """
    Tests for the plan validation algorithm.
    """
    allowed_plans = {"existing": _process_plan(func)}
    success_out, errmsg_out = validate_plan(plan, allowed_plans=allowed_plans, allowed_devices=None)

    assert success_out == success, f"errmsg: {errmsg_out}"
    if success:
        assert errmsg_out == errmsg
    else:
        assert errmsg in errmsg_out


@pytest.mark.parametrize("allowed_plans, success", [(None, True), ({}, False)])
def test_validate_plan_2(allowed_plans, success):
    """
    At this point all plans are considered valid if there is not list of allowed plans.
    """
    success_out, errmsg_out = validate_plan({}, allowed_plans=allowed_plans, allowed_devices=None)
    assert success_out is success


@parameter_annotation_decorator(
    {
        "description": "Move motors into positions; then count dets.",
        "parameters": {
            "motors": {
                "description": "List of motors to be moved into specified positions before the measurement",
                "annotation": "typing.List[Motors]",
                "devices": {"Motors": ("m1", "m2", "m3")},
            },
            "detectors": {
                "description": "Detectors to use for measurement.",
                "annotation": "typing.Union[typing.List[Detectors1], Detectors2]",
                "devices": {
                    "Detectors1": ("d1", "d2", "d3"),
                    "Detectors2": ("d4", "d5"),
                },
            },
            "positions": {
                "description": "Motor positions.",
            },
            "plans_to_run": {
                "description": "Plan to execute for measurement.",
                "annotation": "typing.Union[typing.List[Plans], Plans]",
                "plans": {"Plans": ("p1", "p3")},
            },
        },
        "returns": {
            "description": "Yields a sequence of plan messages.",
            "annotation": "typing.Generator[tuple, None, None]",
        },
    }
)
def _some_strange_plan(
    motors: typing.List[typing.Any],  # The actual type should be a list of 'ophyd.device.Device'
    detectors: typing.List[typing.Any],  # The actual type should be a list of 'ophyd.device.Device'
    plans_to_run: typing.Union[typing.List[callable], callable],
    positions: typing.Union[typing.List[float], float, None] = 10,  # TYPE IS ACTUALLY USED FOR VALIDATION
) -> typing.Generator[str, None, None]:  # Type should be 'bluesky.utils.Msg', not 'str'
    yield from ["one", "two", "three"]


# fmt: off
@pytest.mark.parametrize("plan, allowed_devices, success, errmsg", [
    # Basic use of the function.
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # The same as the previous call, but all parameters are passed as kwargs.
    ({"args": [], "kwargs": {"motors": ("m1", "m2"), "detectors": ("d1", "d2"), "plans_to_run": ("p1",),
                             "positions": (10.0, 20.0)}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # Positions are int (instead of float). Should be converted to float.
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",), (10, 20)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # Position is a single value (part of type description).
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",), 10], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # Position is None (part of type description).
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",), None], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # Position is not specified (default value is used).
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),

    # Use motor that is not listed in the annotation (but exists in the list of allowed devices).
    ({"args": [("m2", "m4"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m2", "m4", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'm2'"),
    # The motor is not in the list of allowed devices.
    ({"args": [("m2", "m3"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m2", "m4", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'm2'"),
    # Both motors are not in the list of allowed devices.
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m4", "m5", "d1", "d2"), False, "value is not a valid enumeration member; permitted:"),
    # Empty list of allowed devices (should be the same result as above).
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     (), False, "value is not a valid enumeration member; permitted:"),
    # Single motor is passed as a scalar (instead of a list element)
    ({"args": ["m2", ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m2", "m4", "d1", "d2"), False, "value is not a valid list"),

    # Pass single detector (allowed).
    ({"args": [("m1", "m2"), "d4", ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2", "d4"), True, ""),
    # Pass single detector from 'Detectors2' group, which is not in the list of allowed devices.
    ({"args": [("m1", "m2"), "d4", ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "value is not a valid enumeration member; permitted:"),
    # Pass single detector from 'Detectors1' group (not allowed).
    ({"args": [("m1", "m2"), "d2", ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2", "d4"), False, " value is not a valid list"),
    # Pass a detector from a group 'Detector2' as a list element.
    ({"args": [("m1", "m2"), ("d4",), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2", "d4"), False, "value is not a valid enumeration member; permitted: 'd1', 'd2'"),

    # Plan 'p3' is not in the list of allowed plans
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p3",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'p1'"),
    # Plan 'p2' is in the list of allowed plans, but not listed in the annotation.
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p2",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'p1'"),
    # Plan 'p2' is in the list of allowed plans, but not listed in the annotation.
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1", "p2"), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'p1'"),
    # Single plan is passed as a scalar (allowed in the annotation).
    ({"args": [("m1", "m2"), ("d1", "d2"), "p1", (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),

])
# fmt: on
def test_validate_plan_3(plan, allowed_devices, success, errmsg):
    """
    Test ``validate_plan`` on a function with more complicated signature and custom annotation.
    Mostly testing verification of types and use of the list of available devices.
    """
    plan["name"] = "_some_strange_plan"
    allowed_plans = {
        "_some_strange_plan": _process_plan(_some_strange_plan),
        "p1": {},  # The plan is used only as a parameter value
        "p2": {},  # The plan is used only as a parameter value
    }
    # 'allowed_devices' must be a dictionary
    allowed_devices = {_: None for _ in allowed_devices}

    success_out, errmsg_out = validate_plan(plan, allowed_plans=allowed_plans, allowed_devices=allowed_devices)

    assert success_out == success, f"errmsg: {errmsg_out}"
    if success:
        assert errmsg_out == errmsg
    else:
        assert errmsg in errmsg_out
