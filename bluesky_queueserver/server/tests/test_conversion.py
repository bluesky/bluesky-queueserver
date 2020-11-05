import pytest

from bluesky_queueserver.server.conversion import filter_plan_descriptions


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

    plans_out = filter_plan_descriptions((plans_in))
    assert plans_out == plans_out_expected
