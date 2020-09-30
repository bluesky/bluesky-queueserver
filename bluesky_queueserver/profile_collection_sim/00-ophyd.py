# flake8: noqa

from ophyd.sim import hw
import nslsii

# Import ALL simulated Ophyd objects in global namespace (borrowed from ophyd.sim)
globals().update(hw().__dict__)
nslsii.configure_base(get_ipython().user_ns, 'MAD')
del hw
