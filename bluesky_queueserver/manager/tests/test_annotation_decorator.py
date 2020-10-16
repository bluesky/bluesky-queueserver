# import typing
from bluesky_queueserver.manager.annotation_decorator import parameter_annotation_decorator

"""
_annotation_dict_1 = {
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
        "description": "Detectors and ints",
    },
}


@parameter_annotation_decorator(_annotation_dict_1)
def _plan_for_testing(
    detector: typing.Any,
    detectors: typing.List[typing.Any],
    val1: float = 10,
    *args: int,
    msg: str = "default_string",
    val2: typing.Union[int, float] = 6,
    **kwargs: int,
) -> str:
    pass
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
    Kind: positional or keyword
val_kwarg
    Parameter 'val_kwarg'
    Kind: keyword only
    Default: 10

Returns
-------
Return value
"""


def test_annotation_dectorator_1():
    """
    Basic test: decorator is applied to a simple function
    """

    @parameter_annotation_decorator(_simple_annotation_with_return)
    def func(val_arg, *, val_kwarg=10):
        return val_arg + val_kwarg

    assert func._custom_parameter_annotation_ == _simple_annotation_with_return
    assert func(10, val_kwarg=20) == 30
    assert func.__doc__ == _simple_annotation_with_return_doc


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
    Kind: positional or keyword
val_kwarg
    Parameter 'val_kwarg'
    Kind: keyword only
    Default: 10
"""


def test_annotation_dectorator_2():
    """
    Basic test: decorator is applied to a generator function.
    """

    @parameter_annotation_decorator(_simple_annotation)
    def func(val_arg, *, val_kwarg=10):
        n_elements = val_arg + val_kwarg
        for n in range(n_elements):
            yield n

    assert func._custom_parameter_annotation_ == _simple_annotation
    assert list(func(10, val_kwarg=20)) == list(range(30))
    assert func.__doc__ == _simple_annotation_doc
