# flake8: noqa
import typing
import ophyd
import bluesky
import bluesky.preprocessors as bpp
import bluesky.plan_stubs as bps
from bluesky_queueserver.manager.annotation_decorator import parameter_annotation_decorator


# Some useless devices for unit tests.
custom_test_device = ophyd.Device(name="custom_test_device")
custom_test_signal = ophyd.Signal(name="custom_test_signal")
custom_test_flyer = ophyd.sim.MockFlyer("custom_test_flyer", ophyd.sim.det, ophyd.sim.motor, 1, 5, 20)


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
                "default": ["det1", "det2"],
            },
            "positions": {
                "description": "Motor positions. The number of positions must be equal "
                "to the number of the motors.",
                "annotation": "typing.List[float]",
                "min": -10,
                "max": 10,
                "step": 0.01,
            },
        },
    }
)
def move_then_count(
    motors: typing.List[ophyd.device.Device],
    detectors: typing.Optional[typing.List[ophyd.device.Device]] = None,
    positions: typing.Optional[typing.List[float]] = None,
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
    yield from bps.mv(*mv_args)
    yield from count(detectors)


@bpp.set_run_key_decorator("run_2")
@bpp.run_decorator(md={})
def _sim_plan_inner(npts: int, delay: float = 1.0):
    for j in range(npts):
        yield from bps.mov(motor1, j * 0.1 + 1, motor2, j * 0.2 - 2)
        yield from bps.trigger_and_read([motor1, motor2, det2])
        yield from bps.sleep(delay)


@bpp.set_run_key_decorator("run_1")
@bpp.run_decorator(md={})
@parameter_annotation_decorator(
    {
        "description": "Simulated multi-run plan: two nested runs. "
        "The plan is included for testing purposes only.",
        "parameters": {
            "npts": {
                "description": "The number of measurements in the outer run. "
                "Inner run will contain 'npts+1' measurements.",
            },
            "delay": {
                "description": "Delay between measurements.",
            },
        },
    }
)
def sim_multirun_plan_nested(npts: int, delay: float = 1.0):
    for j in range(int(npts / 2)):
        yield from bps.mov(motor, j * 0.2)
        yield from bps.trigger_and_read([motor, det])
        yield from bps.sleep(delay)

    yield from _sim_plan_inner(npts + 1, delay)

    for j in range(int(npts / 2), npts):
        yield from bps.mov(motor, j * 0.2)
        yield from bps.trigger_and_read([motor, det])
        yield from bps.sleep(delay)
