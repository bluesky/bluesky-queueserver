import typing
import pytest
import jsonschema
from bluesky_queueserver.manager.annotation_decorator import (
    parameter_annotation_decorator,
    _convert_annotation_to_type,
    _extract_yield_type,
)


# fmt: off
@pytest.mark.parametrize("original, converted", [
    ("<class 'int'>", "int"),
    ("<enum 'SomeEnum'>", "SomeEnum"),
])
# fmt: on
def test_convert_annotation_to_type(original, converted):
    assert _convert_annotation_to_type(original) == converted


# fmt: off
@pytest.mark.parametrize("original, converted", [
    ("typing.Iterator[int]", "int"),
    ("typing.Iterable[int]", "int"),
    ("typing.Generator[int, None, None]", "int"),
])
# fmt: on
def test_extract_yield_type(original, converted):
    assert _extract_yield_type(original) == converted


_simple_annotation = {
    "description": "Simple generator function",
    "parameters": {
        "val_arg": {"description": "Parameter 'val_arg'"},
        "val_kwarg": {"description": "Parameter 'val_kwarg'"},
    },
}

_simple_annotation_doc = """Simple generator function

Parameters
----------
val_arg
    Parameter 'val_arg'
    Kind: positional or keyword.
val_kwarg
    Parameter 'val_kwarg'
    Kind: keyword only.
    Default: 10.
"""

_simple_annotation_with_return = {
    "description": "Simple function with return value",
    "parameters": {
        "val_arg": {"description": "Parameter 'val_arg'"},
        "val_kwarg": {"description": "Parameter 'val_kwarg'"},
    },
    "returns": {"description": "Return value"},
}

_simple_annotation_with_return_doc = """Simple function with return value

Parameters
----------
val_arg
    Parameter 'val_arg'
    Kind: positional or keyword.
val_kwarg
    Parameter 'val_kwarg'
    Kind: keyword only.
    Default: 10.

Returns
-------
Return value
"""

# Note: the types in the following annotation don't match the types in the function
# parameter annotation on purpose. It allows to verify which types are actually placed
# in the docstring.
_simple_annotation_with_types = {
    "description": "Simple function with type annotations",
    "parameters": {
        "val_arg": {"description": "Parameter 'val_arg'", "annotation": "float"},
        "val_kwarg": {"description": "Parameter 'val_kwarg'", "annotation": "int"},
    },
    "returns": {"description": "Return value", "annotation": "float"},
}


_simple_annotation_with_types_doc = """Simple function with type annotations

Parameters
----------
val_arg: float
    Parameter 'val_arg'
    Kind: positional or keyword.
val_kwarg: int
    Parameter 'val_kwarg'
    Kind: keyword only.
    Default: 10.

Returns
-------
float
    Return value
"""


_simple_annotation_long_descriptions = {
    "description": "Simple function with type annotations and long description. " * 10,
    "parameters": {
        "val_arg": {"description": "Long description of the parameter 'val_arg'. " * 10, "annotation": "float"},
        "val_kwarg": {
            "description": "Long description of the parameter 'val_kwarg'. " * 10,
            "annotation": "int",
        },
    },
    "returns": {"description": "Very long description of the return value. " * 10, "annotation": "float"},
}


_simple_annotation_long_descriptions_doc = """Simple function with type annotations and long description. Simple function with
type annotations and long description. Simple function with type annotations and
long description. Simple function with type annotations and long description.
Simple function with type annotations and long description. Simple function with
type annotations and long description. Simple function with type annotations and
long description. Simple function with type annotations and long description.
Simple function with type annotations and long description. Simple function with
type annotations and long description.

Parameters
----------
val_arg: float
    Long description of the parameter 'val_arg'. Long description of the
    parameter 'val_arg'. Long description of the parameter 'val_arg'. Long
    description of the parameter 'val_arg'. Long description of the parameter
    'val_arg'. Long description of the parameter 'val_arg'. Long description of
    the parameter 'val_arg'. Long description of the parameter 'val_arg'. Long
    description of the parameter 'val_arg'. Long description of the parameter
    'val_arg'.
    Kind: positional or keyword.
val_kwarg: int
    Long description of the parameter 'val_kwarg'. Long description of the
    parameter 'val_kwarg'. Long description of the parameter 'val_kwarg'. Long
    description of the parameter 'val_kwarg'. Long description of the parameter
    'val_kwarg'. Long description of the parameter 'val_kwarg'. Long description
    of the parameter 'val_kwarg'. Long description of the parameter 'val_kwarg'.
    Long description of the parameter 'val_kwarg'. Long description of the
    parameter 'val_kwarg'.
    Kind: keyword only.
    Default: 10.

Returns
-------
float
    Very long description of the return value. Very long description of the
    return value. Very long description of the return value. Very long
    description of the return value. Very long description of the return value.
    Very long description of the return value. Very long description of the
    return value. Very long description of the return value. Very long
    description of the return value. Very long description of the return value.
"""


# fmt: off
@pytest.mark.parametrize("custom_annotation, expected_docstring", [
    (_simple_annotation, _simple_annotation_doc),
    (_simple_annotation_with_return, _simple_annotation_with_return_doc),
    (_simple_annotation_with_types, _simple_annotation_with_types_doc),
    (_simple_annotation_long_descriptions, _simple_annotation_long_descriptions_doc),

])
# fmt: on
def test_annotation_dectorator_1(custom_annotation, expected_docstring):
    """
    Basic test: decorator is applied to a simple function
    """

    @parameter_annotation_decorator(custom_annotation)
    def func(val_arg, *, val_kwarg=10):
        return int(val_arg + val_kwarg)

    assert func._custom_parameter_annotation_ == custom_annotation
    assert func(10, val_kwarg=20) == 30
    assert func.__name__ == "func"
    assert func.__doc__ == expected_docstring


# fmt: off
@pytest.mark.parametrize("custom_annotation, expected_docstring", [
    (_simple_annotation_with_types, _simple_annotation_with_types_doc),
    (_simple_annotation_long_descriptions, _simple_annotation_long_descriptions_doc),

])
# fmt: on
def test_annotation_dectorator_2(custom_annotation, expected_docstring):
    """
    Types are specified in the custom annotation and in Python parameter annotations.
    Python annotations are not used.
    """

    @parameter_annotation_decorator(custom_annotation)
    def func(val_arg: int, *, val_kwarg: float = 10):
        return int(val_arg + val_kwarg)

    assert func._custom_parameter_annotation_ == custom_annotation
    assert func(10, val_kwarg=20) == 30
    assert func.__name__ == "func"
    assert func.__doc__ == expected_docstring


_simple_annotation_no_types = {
    "description": "Simple function with type annotations",
    "parameters": {
        "val_arg": {"description": "Parameter 'val_arg'"},
        "val_kwarg": {"description": "Parameter 'val_kwarg'"},
    },
    "returns": {"description": "Return value"},
}


_simple_annotation_no_types_doc = """Simple function with type annotations

Parameters
----------
val_arg: int
    Parameter 'val_arg'
    Kind: positional or keyword.
val_kwarg: float
    Parameter 'val_kwarg'
    Kind: keyword only.
    Default: 10.

Returns
-------
int
    Return value
"""


# fmt: off
@pytest.mark.parametrize("custom_annotation, expected_docstring", [
    (_simple_annotation_no_types, _simple_annotation_no_types_doc),
])
# fmt: on
def test_annotation_dectorator_3(custom_annotation, expected_docstring):
    """
    Types are specified in the custom annotation and in Python parameter annotations.
    Python annotations are not used.
    """

    @parameter_annotation_decorator(custom_annotation)
    def func(val_arg: int, *, val_kwarg: float = 10) -> int:
        return int(val_arg + val_kwarg)

    assert func._custom_parameter_annotation_ == custom_annotation
    assert func(10, val_kwarg=20) == 30
    assert func.__name__ == "func"
    assert func.__doc__ == expected_docstring


# fmt: off
@pytest.mark.parametrize("custom_annotation, expected_docstring", [
    (_simple_annotation, _simple_annotation_doc),
])
# fmt: on
def test_annotation_dectorator_4(custom_annotation, expected_docstring):
    """
    Basic test: decorator is applied to a generator function.
    """

    @parameter_annotation_decorator(custom_annotation)
    def func(val_arg, *, val_kwarg=10):
        n_elements = val_arg + val_kwarg
        for n in range(n_elements):
            yield n

    assert func._custom_parameter_annotation_ == custom_annotation
    assert list(func(10, val_kwarg=20)) == list(range(30))
    assert func.__name__ == "func"
    assert func.__doc__ == expected_docstring


_custom_annotation_plans_and_devices = {
    "description": "Custom annotation with plans and devices.",
    "parameters": {
        "val_arg": {
            "description": "Parameter that accepts a single plan.",
            "annotation": "typing.Union(Plan1, Plan2)",
            "plans": {"Plan1": ("count", "scan", "gridscan"), "Plan2": ("some", "other", "plans")},
        },
        "val_kwarg": {
            "description": "Parameter that accepts the list of devices.",
            "annotation": "typing.List(Device)",
            "devices": {"Device": ("det1", "det2", "det3")},
        },
    },
    "returns": {"description": "Sequence of numbers"},
}


_custom_annotation_plans_and_devices_doc = """Custom annotation with plans and devices.

Parameters
----------
val_arg: typing.Union(Plan1, Plan2)
    Parameter that accepts a single plan.
    Allowed plan names:
        'Plan1': ('count', 'scan', 'gridscan')
        'Plan2': ('some', 'other', 'plans')
    Kind: positional or keyword.
val_kwarg: typing.List(Device)
    Parameter that accepts the list of devices.
    Allowed device names:
        'Device': ('det1', 'det2', 'det3')
    Kind: keyword only.

Yields
------
int
    Sequence of numbers
"""


# fmt: off
@pytest.mark.parametrize("custom_annotation, expected_docstring", [
    (_custom_annotation_plans_and_devices, _custom_annotation_plans_and_devices_doc),
])
# fmt: on
def test_annotation_dectorator_5(custom_annotation, expected_docstring):
    """
    Test if the list of plans and devices is displayed correctly in the docstring.
    """

    @parameter_annotation_decorator(custom_annotation)
    def func(val_arg, *, val_kwarg) -> typing.Iterable[int]:
        # The following code has no purpose except to see if the generator is still working.
        added_strings = val_arg + val_kwarg
        for n in range(len(added_strings)):
            yield n

    assert func._custom_parameter_annotation_ == custom_annotation
    assert list(func("gridscan", val_kwarg="det2")) == list(range(len("gridscan") + len("det2")))
    assert func.__name__ == "func"
    assert func.__doc__ == expected_docstring


_more_complicated_annotation = {
    "description": "Example of annotation with varargs and varkwargs.",
    "parameters": {
        "detector": {
            "devices": {"Device": ("det1", "det2", "det3")},
            "annotation": "Device",
            "description": "Device name",
        },
        "detectors": {
            "devices": {"Device": ("det1", "det4"), "Motor": ("motor10", "motor12")},
            "annotation": "typing.List[typing.Union[Device, Motor]]",
            "description": "Device names",
        },
        "args": {
            "devices": {"Motor": ("motor1", "motor2", "motor3"), "Detector": ("det30", "det31")},
            "annotation": "typing.Union[Detector, Motor, int]",
            "description": "Motors or ints",
        },
        "kwargs": {
            "devices": {"Detector": ("det50", "det51")},
            "annotation": "typing.Union[float, Detector]",
            "description": "Detectors or floats",
        },
    },
    "returns": {"description": "Yields strings from the list of three strings."},
}

_more_complicated_annotation_doc = """Example of annotation with varargs and varkwargs.

Parameters
----------
detector: Device
    Device name
    Allowed device names:
        'Device': ('det1', 'det2', 'det3')
    Kind: positional or keyword.
detectors: typing.List[typing.Union[Device, Motor]]
    Device names
    Allowed device names:
        'Device': ('det1', 'det4')
        'Motor': ('motor10', 'motor12')
    Kind: positional or keyword.
val1: float
    THE ITEM IS NOT DOCUMENTED YET ...
    Kind: positional or keyword.
    Default: 10.
args: typing.Union[Detector, Motor, int]
    Motors or ints
    Allowed device names:
        'Motor': ('motor1', 'motor2', 'motor3')
        'Detector': ('det30', 'det31')
    Kind: var positional.
msg: str
    THE ITEM IS NOT DOCUMENTED YET ...
    Kind: keyword only.
    Default: 'default_string'.
val2: typing.Union[int, float]
    THE ITEM IS NOT DOCUMENTED YET ...
    Kind: keyword only.
    Default: 6.
kwargs: typing.Union[float, Detector]
    Detectors or floats
    Allowed device names:
        'Detector': ('det50', 'det51')
    Kind: var keyword.

Yields
------
str
    Yields strings from the list of three strings.
"""


# fmt: off
@pytest.mark.parametrize("custom_annotation, expected_docstring", [
    (_more_complicated_annotation, _more_complicated_annotation_doc),
])
# fmt: on
def test_annotation_dectorator_6(custom_annotation, expected_docstring):
    """
    An example of more complicated docstring with varargs and varkwargs. Some of the
    args and kwargs are not documented.
    """

    @parameter_annotation_decorator(custom_annotation)
    def func(
        detector: typing.Any,
        detectors: typing.List[typing.Any],
        val1: float = 10,
        *args: int,
        msg: str = "default_string",
        val2: typing.Union[int, float] = 6,
        **kwargs: int,
    ) -> typing.Generator[str, None, None]:
        str_list = ["str1", "str2", "str3"]
        for s in str_list:
            yield s

    assert func._custom_parameter_annotation_ == custom_annotation
    assert list(func("det1", ["det1", "det2"])) == ["str1", "str2", "str3"]
    assert func.__name__ == "func"
    assert func.__doc__ == expected_docstring


_trivial_annotation = {
    "description": "Example of annotation with varargs and varkwargs.",
}

_trivial_annotation_doc = """Example of annotation with varargs and varkwargs.
"""


# fmt: off
@pytest.mark.parametrize("custom_annotation, expected_docstring", [
    (_trivial_annotation, _trivial_annotation_doc),
])
# fmt: on
def test_annotation_dectorator_7(custom_annotation, expected_docstring):
    """
    The trivial case of the decorator that only specifieds function description
    and the function has no parameters.
    """

    @parameter_annotation_decorator(custom_annotation)
    def func():
        str_list = ["str1", "str2", "str3"]
        for s in str_list:
            yield s

    assert func._custom_parameter_annotation_ == custom_annotation
    assert list(func()) == ["str1", "str2", "str3"]
    assert func.__name__ == "func"
    assert func.__doc__ == expected_docstring


_trivial_annotation_error1 = {
    "descriptions": "Example of annotation with varargs and varkwargs.",
}

_trivial_annotation_error2 = {}

_trivial_annotation_error3 = {
    "description": "Example of annotation with varargs and varkwargs.",
    "parameters": {"no_such_parameter": {"description": "The function has no such parameter"}},
}

_trivial_annotation_error4 = {
    "description": "Example of annotation with varargs and varkwargs.",
    "parameters": {
        "no_such_parameter4": {"descriptions": "Required key is 'discription'. Schema validation should fail."}
    },
}


# fmt: off
@pytest.mark.parametrize("custom_annotation, ex_type, err_msg", [
    (_trivial_annotation_error1, jsonschema.ValidationError,  "'description' is a required property"),
    (_trivial_annotation_error2, jsonschema.ValidationError, "'description' is a required property"),
    (_trivial_annotation_error3, ValueError, r"\['no_such_parameter'] are not in the signature of function"),
    (_trivial_annotation_error4, jsonschema.ValidationError,
     r"'description' is a required property"),
])
# fmt: on
def test_annotation_dectorator_8_fail(custom_annotation, ex_type, err_msg):
    """
    The trivial case of the decorator that only specifieds function description
    and the function has no parameters.
    """
    with pytest.raises(ex_type, match=err_msg):

        @parameter_annotation_decorator(custom_annotation)
        def func():
            pass
