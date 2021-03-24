import math
import numpy as np
import os
import pandas as pd
import pytest

from bluesky_queueserver.server.conversions import (
    filter_plan_descriptions,
    _read_cell_parameter,
    spreadsheet_to_plan_list,
)


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
                     "annotation": "int"}
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
                     "kind": "POSITIONAL_OR_KEYWORD",
                     "annotation": "int",  # This is ignored, because custom annotation overrides it.
                     "default": 10,
                     "custom": {
                         "description": "Description for parameter 'p1'.",
                         "annotation": "typing.List[float]",
                     }},
                    {"name": "p2",
                     "kind": "POSITIONAL_OR_KEYWORD",
                     "annotation": "int",  # This is ignored, because custom annotation overrides it.
                     "default": 10,
                     "custom": {
                         "description": "Description for parameter 'p2'.",
                         "annotation": "List[float]",
                     }},
                    {"name": "p3",
                     "default": 10,
                     "custom": {
                         "description": "Description for parameter 'p3'.",
                     }},
                ]},
      },
     {"plan1": {"name": "plan1",
                "description": "Description of Plan 1.",
                "parameters": [
                    {"name": "p1",
                     "description": "Description for parameter 'p1'.",
                     "kind": "POSITIONAL_OR_KEYWORD",
                     "type": "float",
                     "default": 10,
                     "is_list": True,
                     "is_optional": True},
                    {"name": "p2",
                     "description": "Description for parameter 'p2'.",
                     "kind": "POSITIONAL_OR_KEYWORD",
                     "type": "float",
                     "default": 10,
                     "is_list": True,
                     "is_optional": True},
                    {"name": "p3",
                     "description": "Description for parameter 'p3'.",
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
                     "custom": {
                         "description": "Description for parameter 'p1'.",
                         # This example is artificial and used only to test the filtering function.
                         "annotation": "typing.List[typing.Union[Motor1, Motor2, Plan, Enum1, Enum2]]",
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
def test_filter_plan_descriptions_1(plans_in, plans_out_expected):
    """
    Function ``filter_plan_descriptions``. Basic test.
    """
    plans_out = filter_plan_descriptions((plans_in))
    assert plans_out == plans_out_expected


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
            assert type(val_result) == val_type
    else:
        with pytest.raises(Exception, match=errmsg):
            _read_cell_parameter(val_in)


def create_excel_file_from_plan_list(tmp_path, *, plan_list, ss_filename="spreadsheet", ss_ext=".xlsx"):
    """
    Create test spreadsheet file in temporary directory. Return full path to the spreadsheet
    and the expected list of plans with parameters. Supporting '.xlsx' and '.csv' extensions.
    The idea was to also write tests for '.xls' file support, but Pandas writer does not seem to
    support writing ".xls" files.

    Parameters
    ----------
    tmp_path : str
        temporary path
    plan_list : list(dict)
        list of plan parameters
    ss_filename : str
        spreadsheet file name without extension
    ss_ext : str
        spreadsheet extension ('.xlsx' and '.csv' extensions are supported)

    Returns
    -------
    str
        full path to spreadsheet file
    """
    # Create sample Excel file
    ss_filename = ss_filename + ss_ext
    ss_path = os.path.join(tmp_path, ss_filename)

    def format_cell(val):
        if isinstance(val, (np.integer, int)):
            return int(val)
        elif isinstance(val, (np.floating, float)):
            return float(val)
        else:
            return str(val)

    plan_params = []
    kwarg_names = []
    for plan in plan_list:
        pp = [plan["name"]]
        if "args" in plan:
            pp.append(format_cell(plan["args"]))
        else:
            pp.append(math.nan)
        if "kwargs" in plan:
            plan_kwargs = plan["kwargs"].copy()
            for kw in kwarg_names:
                if kw in plan_kwargs:
                    pp.append(format_cell(plan_kwargs[kw]))
                    del plan_kwargs[kw]
                else:
                    pp.append(math.nan)
            for kw in plan_kwargs.keys():
                kwarg_names.append(kw)
                pp.append(format_cell(plan_kwargs[kw]))
        plan_params.append(pp)

    # Make all parameter lists equal length
    max_params = max([len(_) for _ in plan_params])
    for pp in plan_params:
        for _ in range(len(pp), max_params):
            pp.append(math.nan)

    col_names = ["plan_name", "args"] + kwarg_names

    def create_excel(ss_path, plan_params, col_names):
        df = pd.DataFrame(plan_params)
        df = df.set_axis(col_names, axis=1)

        _, ext = os.path.splitext(ss_path)
        if ext == ".xlsx":
            df.to_excel(ss_path, index=False, engine="openpyxl")
        elif ext == ".csv":
            df.to_csv(ss_path, index=False)
        return df

    def verify_excel(ss_path, df):
        _, ext = os.path.splitext(ss_path)
        if ext == ".xlsx":
            df_read = pd.read_excel(ss_path, engine="openpyxl")
        elif ext == ".csv":
            df_read = pd.read_csv(ss_path)

        assert df_read.equals(df), str(df_read)

    df = create_excel(ss_path, plan_params, col_names)
    verify_excel(ss_path, df)

    return ss_path


# fmt: on
@pytest.mark.parametrize("ext", [".xlsx", ".csv"])
# fmt: off
def test_spreadsheet_to_plan_list_1(tmp_path, ext):

    plan_list = [
        {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 10}},
        {"name": "count", "args": [["det1"]], "kwargs": {"delay": 0.5}},
        {"name": "count", "kwargs": {"detectors": ["det1"], "delay": 0.5}},
        {"name": math.nan},
        {"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10], "kwargs": {"num": 3}},
        {"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10]},
        {"name": "count", "args": [["det2"]], "kwargs": {"num": 2, "delay": 0.7}},
        {"name": "count", "args": [["det2"]], "kwargs": {"num": 2}},
    ]

    ss_path = create_excel_file_from_plan_list(tmp_path, plan_list=plan_list, ss_ext=ext)
    with open(ss_path, "rb") as f:
        plans = spreadsheet_to_plan_list(spreadsheet_file=f, file_name=os.path.split(ss_path)[-1])

    # Remove the case when name == None before comparing dictionaries
    assert plans == [{"plan": _} for _ in plan_list if isinstance(_["name"], str)]
