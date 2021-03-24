import numpy as np
import pytest

from bluesky_queueserver.server.conversions import filter_plan_descriptions, _read_cell_parameter


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
