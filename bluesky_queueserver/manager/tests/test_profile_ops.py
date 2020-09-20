import os
import pytest

import ophyd

from bluesky_queueserver.manager.profile_ops import \
    (get_default_profile_collection_dir, load_profile_collection, plans_from_nspace,
     devices_from_nspace, parse_plan)


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


def test_load_profile_collection_2_fail(tmp_path):
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


@pytest.mark.parametrize("plan, success, err_msg", [
    ({"name": "count", "args": [["det1", "det2"]]}, True, ""),
    ({"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10]}, True, ""),
    ({"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 10, "delay": 1}}, True, ""),
    ({"name": "countABC", "args": [["det1", "det2"]]}, False,
     "Plan 'countABC' is not allowed or does not exist."),
])
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
