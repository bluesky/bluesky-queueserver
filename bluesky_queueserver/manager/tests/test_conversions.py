import copy
import os
import pprint

import numpy as np
import pytest

from bluesky_queueserver.manager.conversions import (
    _read_cell_parameter,
    simplify_plan_descriptions,
    spreadsheet_to_plan_list,
)
from bluesky_queueserver.manager.tests.plan_lists import create_excel_file_from_plan_list, plan_list_sample


# fmt: off
@pytest.mark.parametrize("plans_in, plans_out_expected", [
    # Two trivial plans (only name and no parameters). Checks if multiple plans are processed
    ({"plan1": {"name": "plan1"},
      "plan2": {"name": "plan1"}},

     {"plan1": {"name": "plan1"},
      "plan2": {"name": "plan1"}}),

    # Check recognition of 'int' scalar type
    ({"plan1": {"name": "plan1",
                "description": "Description of Plan 1.",
                "parameters": [
                    {"name": "p1",
                     "kind": "POSITIONAL_OR_KEYWORD",
                     "annotation": {"type": "int"}}
                ]}},
     {"plan1": {"name": "plan1",
                "description": "Description of Plan 1.",
                "parameters": [
                    {"name": "p1",
                     "kind": "POSITIONAL_OR_KEYWORD",
                     "type": "int",
                     "is_list": False,
                     "is_optional": False}
                ]}},
     ),

    # Check recognition of list of 'floats'
    ({"plan1": {"name": "plan1",
                "description": "Description of Plan 1.",
                "parameters": [
                    {"name": "p1",
                     "description": "Description for parameter 'p1'.",
                     "kind": "POSITIONAL_OR_KEYWORD",
                     "annotation": {"type": "typing.List[float]"},
                     "default": "[10, 20]",
                     "min": "5",
                     "max": "50",
                     "step": "0.01"
                     },
                    {"name": "p2",
                     "description": "Description for parameter 'p2'.",
                     "kind": "POSITIONAL_OR_KEYWORD",
                     "annotation": {"type": "List[float]"},
                     "default": "[30, 40]",
                     },
                    {"name": "p3",
                     "description": "Description for parameter 'p3'.",
                     "kind": "POSITIONAL_OR_KEYWORD",
                     "default": "10",
                     },
                ]},
      },
     {"plan1": {"name": "plan1",
                "description": "Description of Plan 1.",
                "parameters": [
                    {"name": "p1",
                     "description": "Description for parameter 'p1'.",
                     "kind": "POSITIONAL_OR_KEYWORD",
                     "type": "float",
                     "default": [10, 20],
                     "is_list": True,
                     "is_optional": True,
                     "min": 5,
                     "max": 50,
                     "step": 0.01},
                    {"name": "p2",
                     "description": "Description for parameter 'p2'.",
                     "kind": "POSITIONAL_OR_KEYWORD",
                     "type": "float",
                     "default": [30, 40],
                     "is_list": True,
                     "is_optional": True},
                    {"name": "p3",
                     "description": "Description for parameter 'p3'.",
                     "kind": "POSITIONAL_OR_KEYWORD",
                     "default": 10,
                     "is_optional": True},
                ]},
      },
     ),

    # Check enum
    ({"plan1": {"name": "plan1",
                "description": "Description of Plan 1.",
                "parameters": [
                    {"name": "p1",
                     "kind": "POSITIONAL_OR_KEYWORD",
                     "description": "Description for parameter 'p1'.",
                     "annotation": {
                         "type": "typing.List[typing.Union[Motor1, Motor2, Plan, Enum1, Enum2]]",
                         "devices": {"Motor1": ("m1", "m2"), "Motor2": ("m3")},
                         "plans": {"Plan": ("p1", "p2")},
                         "enums": {"Enum1": ("e1"), "Enum2": ("e2", "e3")},
                     }}
                ]}},
     {"plan1": {"name": "plan1",
                "description": "Description of Plan 1.",
                "parameters": [
                    {"name": "p1",
                     "description": "Description for parameter 'p1'.",
                     "kind": "POSITIONAL_OR_KEYWORD",
                     "type": "str",
                     "enum": ["m1", "m2", "m3", "p1", "p2", "e1", "e2", "e3"],
                     "is_list": True,
                     "is_optional": False}
                ]}},
     ),
])
# fmt: on
def test_simplify_plan_descriptions_1(plans_in, plans_out_expected):
    """
    Function ``filter_plan_descriptions``. Basic test.
    """
    plans_out = simplify_plan_descriptions(plans_in)
    assert plans_out == plans_out_expected, pprint.pformat(plans_out)


# fmt: off
@pytest.mark.parametrize("val_in, val_out, val_type, success, errmsg", [
    (10, 10, int, True, ""),
    (np.int64(10), 10, int, True, ""),
    (10.0, 10, int, True, ""),  # The number is expected to be rounded to int and represented as int
    (np.float64(10.0), 10, int, True, ""),
    (10.5, 10.5, float, True, ""),
    (np.float64(10.5), 10.5, float, True, ""),
    ("10", 10, int, True, ""),  # Number representation is more straightforward if they are strings
    ("10.0", 10, float, True, ""),
    ("10.5", 10.5, float, True, ""),
    ("10, 20", (10, 20), None, True, ""),
    ("10.3, 20", (10.3, 20), None, True, ""),
    ("'det1'", 'det1', str, True, ""),
    ("'det1', 'det2'", ('det1', 'det2'), None, True, ""),
    ("det1", 'det1', None, False, "name 'det1' is not defined"),  # detector names must be quoted
    ("det1, det2", ('det1', 'det2'), None, False, "name 'det1' is not defined"),  # detector names must be quoted
    ([10, 20], None, None, False, "Cell value .* has unsupported type"),
    ("[10, 20, 'det']", [10, 20, "det"], list, True, ""),  # List represented as a string
    # Test for a complicated expression
    ("{'a':'10','b':'20.5','c':'det','d':{'e':[50, 60]}}",
     {'a': '10', 'b': '20.5', 'c': 'det', 'd': {'e': [50, 60]}}, dict, True, ""),

])
# fmt: on
def test_read_cell_parameter(val_in, val_out, val_type, success, errmsg):
    if success:
        val_result = _read_cell_parameter(val_in)
        assert val_result == val_out
        if val_type:
            assert isinstance(val_result, val_type)
    else:
        with pytest.raises(Exception, match=errmsg):
            _read_cell_parameter(val_in)


# fmt: on
@pytest.mark.parametrize("ext", [".xlsx", ".csv"])
# fmt: off
def test_spreadsheet_to_plan_list_1(tmp_path, ext):
    """
    Basic test for ``spreadsheet_to_plan_list()`` function. Check that the list of
    plans could be saved and then restored from the spreadsheet without change.
    Check that the restored parameters have correct types.
    """
    # Add plans with a kwarg that accepts values of different types (int, float and str)
    #   Those values are going to be placed in the same column of the spreadsheet.
    #   Check if types will be restored correctly (pandas write/read functions don't like columns
    #   with mixed types). Also check if all possible types are handled correctly.
    extra_plans = [
        {"name": "count", "args": [["det2"]], "kwargs": {"num": 2, "extra_param": 50}, "item_type": "plan"},
        {"name": "count", "args": [["det2"]], "kwargs": {
            "num": 2, "extra_param": "some_str"}, "item_type": "plan"},
        {"name": "count", "args": [["det2"]], "kwargs": {"num": 2, "extra_param": 50.256}, "item_type": "plan"},
        {"name": "count", "args": [["det2"]], "kwargs": {"num": 2, "extra_param": None}, "item_type": "plan"},
        {"name": "count", "args": [["det2"]], "kwargs": {"num": 2, "extra_param": ""}, "item_type": "plan"},
        {"name": "count", "args": [["det2"]], "kwargs": {
            "num": 2, "extra_param": [10, 20, 30]}, "item_type": "plan"},
        {"name": "count", "args": [["det2"]], "kwargs": {"num": 2, "extra_param": {
            "p1": 10, "p2": "10", "p3": 50}}, "item_type": "plan"},
    ]

    plan_list = copy.deepcopy(plan_list_sample)  # We are going to change the plans
    for plan in extra_plans:
        plan_list.append(plan)

    for plan in plan_list:
        if isinstance(plan["name"], str):
            plan["item_type"] = "plan"

    ss_path = create_excel_file_from_plan_list(tmp_path, plan_list=plan_list, ss_ext=ext)
    with open(ss_path, "rb") as f:
        plans = spreadsheet_to_plan_list(spreadsheet_file=f, file_name=os.path.split(ss_path)[-1])

    # Remove the case when name == None before comparing dictionaries
    assert plans == [_ for _ in plan_list if isinstance(_["name"], str)]


# fmt: on
@pytest.mark.parametrize("ext", [".xlsx", ".csv"])
@pytest.mark.parametrize(
    "extra_plan, errmsg",
    [
        # Invalid plan name (wrong type - not str)
        (
            {"name": 10, "args": [["det1", "det2"]], "kwargs": {"num": 10}},
            "Plan name .*row 9.* has incorrect type",
        ),
        (
            {"name": [10, 20], "args": [["det1", "det2"]], "kwargs": {"num": 10}},
            "Plan name .*row 9.* has incorrect type",
        ),
        # Invalid plan name (can not be a Python identifier)
        (
            {"name": "10", "args": [["det1", "det2"]], "kwargs": {"num": 10}},
            "Plan name .*10.*row 9.* is not a valid plan name",
        ),
        (
            {"name": "co unt", "args": [["det1", "det2"]], "kwargs": {"num": 10}},
            "Plan name .*co unt.*row 9.* is not a valid plan name",
        ),
    ],
)
# fmt: off
def test_spreadsheet_to_plan_list_2_fail(tmp_path, ext, extra_plan, errmsg):
    """
    Test if ``spreadsheet_to_plan_list()`` function is correctly processing the main issues
    with the spreadsheet and raises exception with correct error messages.
    """
    _plan_list_modified = plan_list_sample.copy()
    _plan_list_modified.append(extra_plan)
    ss_path = create_excel_file_from_plan_list(tmp_path, plan_list=_plan_list_modified, ss_ext=ext)
    with open(ss_path, "rb") as f:
        with pytest.raises(Exception, match=errmsg):
            spreadsheet_to_plan_list(spreadsheet_file=f, file_name=os.path.split(ss_path)[-1])
