import typing
from bluesky_queueserver.manager.plan_annotation_decorator import parameter_annotation_decorator


@parameter_annotation_decorator(
    {
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
)
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


