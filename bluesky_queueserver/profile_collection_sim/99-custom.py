# flake8: noqa
import typing
import ophyd
import bluesky
from bluesky_queueserver.manager.annotation_decorator import parameter_annotation_decorator


@parameter_annotation_decorator(
    {
        "description": "Move motors into positions; then count dets.",
        "parameters": {
            "motors": {
                "description": "List of motors to be moved into specified positions before the measurement",
                "annotation": "typing.List[Motors]",
                "devices": {"Motors": ("motor1", "motor2")},
            },
            "detectors": {
                "description": "Detectors to use for measurement.",
                "annotation": "typing.List[Detectors]",
                "devices": {"Detectors": ("det1", "det2", "det3")},
            },
            "positions": {
                "description": "Motor positions. The number of positions must be equal "
                "to the number of the motors.",
                "annotation": "typing.List[float]",
            },
        },
        "returns": {
            "description": "Yields a sequence of plan messages.",
            "annotation": "typing.Generator[tuple, None, None]",
        },
    }
)
def move_then_count(
    motors: typing.List[ophyd.device.Device],
    detectors: typing.List[ophyd.device.Device],
    positions: typing.List[float],
) -> typing.Generator[bluesky.utils.Msg, None, None]:
    if not isinstance(motors, (list, tuple)):
        raise TypeError(f"Parameter 'motors' should be a list or a tuple: type(motors) = {type(motors)}")
    if not isinstance(detectors, (list, tuple)):
        raise TypeError(f"Parameter 'detectors' should be a list or a tuple: type(detectors) = {type(detectors)}")
    if not isinstance(positions, (list, tuple)):
        raise TypeError(f"Parameter 'positions' should be a list or a tuple: type(positions) = {type(positions)}")
    if len(motors) != len(positions):
        raise TypeError(
            f"The lists of 'motors' and 'positions' should have the same number of elements: "
            f"len(motors) = {len(motors)}, len(positions) = {len(positions)}"
        )

    mv_args = [val for tup in zip(motors, positions) for val in tup]
    yield from mv(*mv_args)
    yield from count(*detectors)
