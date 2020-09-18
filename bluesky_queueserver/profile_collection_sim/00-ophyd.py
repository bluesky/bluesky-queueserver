# flake8: noqa

from ophyd.sim import hw

# Import ALL simulated Ophyd objects in global namespace (borrowed from ophyd.sim)
globals().update(hw().__dict__)
del hw
