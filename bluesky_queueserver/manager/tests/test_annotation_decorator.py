import inspect
import typing

import jsonschema
import pytest

from bluesky_queueserver.manager.annotation_decorator import parameter_annotation_decorator

_simple_annotation = {
    "description": "Simple generator function",
    "parameters": {
        "val_arg": {"description": "Parameter 'val_arg'"},
        "val_kwarg": {"description": "Parameter 'val_kwarg'"},
    },
}


_simple_annotation_no_fdesc = {
    "parameters": {
        "val_arg": {"description": "Parameter 'val_arg'"},
        "val_kwarg": {"description": "Parameter 'val_kwarg'"},
    },
}


_simple_annotation_no_pdesc = {
    "description": "Simple generator function",
    "parameters": {
        "val_arg": {},
        "val_kwarg": {},
    },
}


# Note: the types in the following annotation don't match the types in the function
# parameter annotation on purpose. It allows to verify which types are actually placed
# in the docstring.
_simple_annotation_with_types = {
    "description": "Simple function with type annotations",
    "parameters": {
        "val_arg": {"description": "Parameter 'val_arg'", "annotation": "float"},
        "val_kwarg": {"description": "Parameter 'val_kwarg'", "annotation": "int"},
    },
}


_simple_annotation_list_devices_and_plans = {
    "description": "Simple function with type annotations",
    "parameters": {
        "val_arg": {
            "description": "Parameter 'val_arg'",
            "annotation": "Motor",
            "devices": {
                "Motor": ("m1", "m2", "m3"),
            },
        },
        "val_kwarg": {
            "description": "Parameter 'val_kwarg'",
            "annotation": "Plan",
            "plans": {
                "Plan": ("p1", "p2", "p3"),
            },
        },
    },
}


_simple_annotation_enums = {
    "description": "Simple function with type annotations",
    "parameters": {
        "val_arg": {
            "description": "Parameter 'val_arg'",
            "annotation": "typing.List[CustomEnum]",
            "enums": {
                "CustomEnum": ("m1", "m2", "m3"),
            },
        },
        "val_kwarg": {
            "description": "Parameter 'val_kwarg'",
            "annotation": "CustomEnum",
            "enums": {
                "CustomEnum": ("p1", "p2", "p3"),
            },
        },
    },
}


_simple_annotation_with_default = {
    "description": "Simple function with type annotations",
    "parameters": {
        "val_arg": {
            "description": "Parameter 'val_arg'",
            "annotation": "int",
            "default": 10,
        },
        "val_kwarg": {
            "description": "Parameter 'val_kwarg'",
            "annotation": "CustomEnum",
            "enums": {
                "CustomEnum": ("p1", "p2", "p3"),
            },
            "default": "p1",  # String value should be quoted
        },
    },
}

_simple_annotation_with_min_max_step = {
    "description": "Simple function with type annotations",
    "parameters": {
        "val_arg": {
            "description": "Parameter 'val_arg'",
            "default": 10,
            "min": 1,
            "max": 100,
            "step": 0.1,
        },
    },
}


_simple_annotation_long_descriptions = {
    "description": "Simple function with type annotations and long description. " * 10,
    "parameters": {
        "val_arg": {"description": "Long description of the parameter 'val_arg'. " * 10, "annotation": "float"},
        "val_kwarg": {
            "description": "Long description of the parameter 'val_kwarg'. " * 10,
            "annotation": "int",
        },
    },
}


# fmt: off
@pytest.mark.parametrize("custom_annotation", [
    {},  # Empty custom annotation is allowed
    _simple_annotation,
    _simple_annotation_no_fdesc,
    _simple_annotation_no_pdesc,
    _simple_annotation_with_types,
    _simple_annotation_list_devices_and_plans,
    _simple_annotation_enums,
    _simple_annotation_with_default,
    _simple_annotation_with_min_max_step,
    _simple_annotation_long_descriptions,
])
# fmt: on
def test_annotation_dectorator_01(custom_annotation):
    """
    Basic test: decorator is applied to a simple function
    """

    @parameter_annotation_decorator(custom_annotation)
    def func(val_arg, *, val_kwarg=10):
        return int(val_arg + val_kwarg)

    assert func._custom_parameter_annotation_ == custom_annotation
    assert func(10, val_kwarg=20) == 30
    assert func.__name__ == "func"


# fmt: off
@pytest.mark.parametrize("custom_annotation", [
    _simple_annotation_with_types,
    _simple_annotation_long_descriptions,

])
# fmt: on
def test_annotation_dectorator_02(custom_annotation):
    """
    Types are specified in the custom annotation and in Python parameter annotations.
    Python annotations are not used. Verify that the signature was not broken.
    """

    @parameter_annotation_decorator(custom_annotation)
    def func(val_arg: int, *, val_kwarg: float = 10):
        return int(val_arg + val_kwarg)

    assert func._custom_parameter_annotation_ == custom_annotation
    assert func(10, val_kwarg=20) == 30
    assert func.__name__ == "func"


_simple_annotation_no_types = {
    "description": "Simple function with type annotations",
    "parameters": {
        "val_arg": {"description": "Parameter 'val_arg'"},
        "val_kwarg": {"description": "Parameter 'val_kwarg'"},
    },
}


# fmt: off
@pytest.mark.parametrize("custom_annotation", [
    _simple_annotation_no_types,
])
# fmt: on
def test_annotation_dectorator_03(custom_annotation):
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


# fmt: off
@pytest.mark.parametrize("custom_annotation", [
    _simple_annotation,
])
# fmt: on
def test_annotation_dectorator_04(custom_annotation):
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
}


# fmt: off
@pytest.mark.parametrize("custom_annotation", [
    _custom_annotation_plans_and_devices,
])
# fmt: on
def test_annotation_dectorator_05(custom_annotation):
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
}


# fmt: off
@pytest.mark.parametrize("custom_annotation", [
    _more_complicated_annotation,
])
# fmt: on
def test_annotation_dectorator_06(custom_annotation):
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


_trivial_annotation = {
    "description": "Example of annotation with varargs and varkwargs.",
}

# _trivial_annotation_doc = """Example of annotation with varargs and varkwargs.
# """


# fmt: off
@pytest.mark.parametrize("custom_annotation", [
    _trivial_annotation,
])
# fmt: on
def test_annotation_dectorator_07(custom_annotation):
    """
    The trivial case of the decorator that only specifies function description
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


_annotation_with_convert_plan_and_device_names = {
    "description": "Example of using 'convert_strings_to_objects'.",
    "parameters": {
        "existing_param": {
            "description": "The list of strings that should be converted to plans or devices.",
            "annotation": "typing.List[str]",
            "convert_plan_names": True,
            "convert_device_names": True,
        }
    },
}


def test_annotation_dectorator_08():
    """
    The trivial case of the decorator that only specified function description
    and the function has no parameters.
    """

    @parameter_annotation_decorator(_annotation_with_convert_plan_and_device_names)
    def func(existing_param):
        pass

    assert func._custom_parameter_annotation_ == _annotation_with_convert_plan_and_device_names


_trivial_annotation_error1 = {
    "descriptions": "Example of annotation with varargs and varkwargs.",
}

_trivial_annotation_error2 = {
    "description": "Example of annotation with varargs and varkwargs.",
    "parameters": {"no_such_parameter": {"description": "The function has no such parameter"}},
}

_trivial_annotation_error3 = {
    "description": "Example of annotation with varargs and varkwargs.",
    "parameters": {
        "existing_param": {"descriptions": "Required key is 'discription'. Schema validation should fail."}
    },
}

_trivial_annotation_error4 = {
    "description": "Example of annotation with varargs and varkwargs.",
    "parameters": {
        "existing_param": {
            "description": "Required key is 'discription'. Schema validation should fail.",
            "annotation": "Motor",
            "some_devices": {  # No such keyword
                "Motor": ("a", "b", "c"),
            },
        }
    },
}

_trivial_annotation_error5 = {
    "description": "Example of annotation with varargs and varkwargs.",
    "parameters": {
        "existing_param": {
            "description": "Required key is 'discription'. Schema validation should fail.",
            "annotation": "Motor",
            "devices": {
                "Motor": (1, 2, 3),  # Type must be 'str'
            },
        }
    },
}

_trivial_annotation_error6 = {
    "description": "Example of annotation with varargs and varkwargs.",
    "parameters": {
        "existing_param": {
            "description": "Required key is 'discription'. Schema validation should fail.",
            "annotation": "Motor",
            "devices": {
                "Motor": "unsupported_list",  # Incorrect name for built-in list
            },
        }
    },
}

_trivial_annotation_error7 = {
    "description": "Example of annotation with varargs and varkwargs.",
    "parameters": {
        "existing_param": {
            "description": "Required key is 'discription'. Schema validation should fail.",
            "annotation": "Motor",
            "devices": {
                "Motor": None,  # Incorrect name for built-in list
            },
        }
    },
}

_trivial_annotation_error8 = {
    "description": "Example of annotation with varargs and varkwargs.",
    "parameters": {
        "existing_param": {
            "description": "Required key is 'discription'. Schema validation should fail.",
            "annotation": "Plans",
            "plans": {
                "Plans.": "someplan",  # Plan name 'Plans.' contains character '.' which is not allowed
            },
        }
    },
}

_trivial_annotation_error9 = {
    "description": "Example of annotation with varargs and varkwargs.",
    "parameters": {
        "existing_param": {
            "description": "Required key is 'discription'. Schema validation should fail.",
            "annotation": "str",
            "convert_plan_names": "abc",  # Must be boolean
        }
    },
}

_trivial_annotation_error10 = {
    "description": "Example of annotation with varargs and varkwargs.",
    "parameters": {
        "existing_param": {
            "description": "Required key is 'discription'. Schema validation should fail.",
            "annotation": "str",
            "convert_device_names": 10,  # Must be boolean
        }
    },
}


# fmt: off
@pytest.mark.parametrize("custom_annotation, ex_type, err_msg", [
    (_trivial_annotation_error1, jsonschema.ValidationError,
     r"Additional properties are not allowed \('descriptions' was unexpected\)"),
    (_trivial_annotation_error2, ValueError, r"\['no_such_parameter'] are not in the signature of function"),
    (_trivial_annotation_error3, jsonschema.ValidationError,
     r"Additional properties are not allowed \('descriptions' was unexpected\)"),
    (_trivial_annotation_error4, jsonschema.ValidationError,
     r"Additional properties are not allowed \('some_devices' was unexpected\)"),
    (_trivial_annotation_error5, jsonschema.ValidationError, r"\(1, 2, 3\) is not valid"),
    (_trivial_annotation_error6, jsonschema.ValidationError, "'unsupported_list' is not valid"),
    (_trivial_annotation_error7, jsonschema.ValidationError, "None is not valid under any of the given schemas"),
    (_trivial_annotation_error8, jsonschema.ValidationError, "'Plans.' does not match any of the regexes"),
    (_trivial_annotation_error9, jsonschema.ValidationError, "'abc' is not of type 'boolean'"),
    (_trivial_annotation_error10, jsonschema.ValidationError, "10 is not of type 'boolean'"),
])
# fmt: on
def test_annotation_dectorator_09_fail(custom_annotation, ex_type, err_msg):
    """
    The trivial case of the decorator that only specified function description
    and the function has no parameters.
    """
    with pytest.raises(ex_type, match=err_msg):

        @parameter_annotation_decorator(custom_annotation)
        def func(existing_param):
            pass


_trivial_annotation = {
    "description": "Trivial annotation.",
}


def test_annotation_dectorator_10():
    """
    Test if decorated generator function is recognized as a generator function by ``inspect``.
    """

    # Apply decorator to a function
    @parameter_annotation_decorator(_trivial_annotation)
    def func():
        return None

    assert inspect.isgeneratorfunction(func) is False

    # Apply decorator to a generator function
    @parameter_annotation_decorator(_trivial_annotation)
    def func():
        yield from [1, 2, 3]

    assert inspect.isgeneratorfunction(func) is True
