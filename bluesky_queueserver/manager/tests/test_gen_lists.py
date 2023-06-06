import os
import pprint
import subprocess
import sys

import pytest

from bluesky_queueserver.manager.gen_lists import gen_list_of_plans_and_devices
from bluesky_queueserver.manager.profile_ops import load_existing_plans_and_devices

from .common import append_code_to_last_startup_file, copy_default_profile_collection, use_ipykernel_for_tests


def test_gen_list_of_plans_and_devices_01(tmp_path):
    """
    Copy simulated profile collection and generate the list of allowed (in this case available)
    plans and devices based on the profile collection
    """
    using_ipython = use_ipykernel_for_tests()
    pp = dict(use_ipython_kernel=using_ipython) if using_ipython else {}

    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=False)

    fln_yaml = "list.yaml"
    gen_list_of_plans_and_devices(startup_dir=pc_path, file_dir=pc_path, file_name=fln_yaml, **pp)
    assert os.path.isfile(os.path.join(pc_path, fln_yaml)), "List of plans and devices was not created"

    # Attempt to overwrite the file
    with pytest.raises(RuntimeError, match="already exists. File overwriting is disabled."):
        gen_list_of_plans_and_devices(startup_dir=pc_path, file_dir=pc_path, file_name=fln_yaml, **pp)

    # Allow file overwrite
    gen_list_of_plans_and_devices(startup_dir=pc_path, file_dir=pc_path, file_name=fln_yaml, overwrite=True, **pp)


_script_test_plan_invalid_1 = """
def test_plan_failing(dets=det1):
    # The default value is a detector, which can not be included in the plan description.
    yield from []
"""


def test_gen_list_of_plans_and_devices_02(tmp_path):
    """
    ``gen_list_of_plans_and_devices``: parameter ``ignore_invalid_plans``
    """
    using_ipython = use_ipykernel_for_tests()
    pp = dict(use_ipython_kernel=using_ipython) if using_ipython else {}

    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=False)
    append_code_to_last_startup_file(pc_path, _script_test_plan_invalid_1)

    fln_yaml = "list.yaml"
    fln_yaml_path = os.path.join(pc_path, fln_yaml)
    with pytest.raises(RuntimeError, match="The expression .* can not be evaluated"):
        gen_list_of_plans_and_devices(startup_dir=pc_path, file_dir=pc_path, file_name=fln_yaml, **pp)
    assert not os.path.exists(fln_yaml_path)

    with pytest.raises(RuntimeError, match="The expression .* can not be evaluated"):
        gen_list_of_plans_and_devices(
            startup_dir=pc_path, file_dir=pc_path, file_name=fln_yaml, ignore_invalid_plans=False, **pp
        )
    assert not os.path.exists(fln_yaml_path)

    gen_list_of_plans_and_devices(
        startup_dir=pc_path, file_dir=pc_path, file_name=fln_yaml, ignore_invalid_plans=True, **pp
    )
    assert os.path.isfile(os.path.join(pc_path, fln_yaml)), "List of plans and devices was not created"


_sim_bundle_A_depth_0 = {
    "dets": {
        "det_A": {"Imax": {}, "center": {}, "noise": {}, "noise_multiplier": {}, "sigma": {}, "val": {}},
        "det_B": {"Imax": {}, "center": {}, "noise": {}, "noise_multiplier": {}, "sigma": {}, "val": {}},
    },
    "mtrs": {
        "x": {"acceleration": {}, "readback": {}, "setpoint": {}, "unused": {}, "velocity": {}},
        "y": {"acceleration": {}, "readback": {}, "setpoint": {}, "unused": {}, "velocity": {}},
        "z": {"acceleration": {}, "readback": {}, "setpoint": {}, "unused": {}, "velocity": {}},
    },
}

_sim_bundle_A_depth_1 = {}

_sim_bundle_A_depth_2 = {"dets": {}, "mtrs": {}}


# fmt: off
@pytest.mark.parametrize("device_max_depth, det_structure", [
    (None, _sim_bundle_A_depth_0),
    (0, _sim_bundle_A_depth_0),
    (1, _sim_bundle_A_depth_1),
    (2, _sim_bundle_A_depth_2),
])
# fmt: on
def test_gen_list_of_plans_and_devices_03(tmp_path, device_max_depth, det_structure):
    """
    ``gen_list_of_plans_and_devices``: parameter ``device_max_depth``.
    """
    using_ipython = use_ipykernel_for_tests()
    pp = dict(use_ipython_kernel=using_ipython) if using_ipython else {}

    if device_max_depth is not None:
        pp.update(dict(device_max_depth=device_max_depth))

    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=False)

    fln_yaml = "list.yaml"
    fln_yaml_path = os.path.join(pc_path, fln_yaml)
    gen_list_of_plans_and_devices(startup_dir=pc_path, file_dir=pc_path, file_name=fln_yaml, **pp)
    assert os.path.isfile(fln_yaml_path)

    _, devices = load_existing_plans_and_devices(fln_yaml_path)

    def reduce_device_description(description):
        def inner(dev, dev_out):
            if "components" in dev:
                for name in dev["components"]:
                    dev_out[name] = {}
                    inner(dev["components"][name], dev_out[name])

        dev_out = {}
        inner(description, dev_out)
        return dev_out

    desc = reduce_device_description(devices["sim_bundle_A"])
    assert desc == det_structure, pprint.pformat(desc)


_script_test_gen_list_04 = """
from bluesky_queueserver import is_re_worker_active, is_ipython_mode
assert is_ipython_mode() is {mode:s}, "Unexpected IPython mode"
"""


# fmt: off
@pytest.mark.parametrize("use_ip_kernel", [False, True, None])
# fmt: on
def test_gen_list_of_plans_and_devices_04(tmp_path, use_ip_kernel):
    """
    ``gen_list_of_plans_and_devices``: parameter ``use_ipython_kernel``
    """
    pp1 = dict(use_ipython_kernel=use_ip_kernel) if use_ip_kernel is not None else {}
    pp2 = dict(use_ipython_kernel=not use_ip_kernel)

    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=False)

    script = _script_test_gen_list_04.format(mode=str(bool(use_ip_kernel)))
    append_code_to_last_startup_file(pc_path, script)

    fln_yaml = "list.yaml"
    fln_yaml_path = os.path.join(pc_path, fln_yaml)

    pp = dict(startup_dir=pc_path, file_dir=pc_path, file_name=fln_yaml)

    with pytest.raises(RuntimeError, match="AssertionError: Unexpected IPython mode"):
        gen_list_of_plans_and_devices(**pp, **pp2)

    assert not os.path.exists(fln_yaml_path)
    gen_list_of_plans_and_devices(**pp, **pp1)
    assert os.path.isfile(fln_yaml_path)


_script_test_gen_list_05 = """
from IPython.core.magic import register_line_magic, register_cell_magic

@register_line_magic
def hello():
    print("Hello world!")
"""


# fmt: off
@pytest.mark.parametrize("use_ip_kernel", [False, True, None])
# fmt: on
def test_gen_list_of_plans_and_devices_05(tmp_path, use_ip_kernel):
    """
    ``gen_list_of_plans_and_devices``: parameter ``use_ipython_kernel``. Check that
    IPython-specific code is loaded only if ``use_ipython_kernel=True``.
    """
    pp1 = dict(use_ipython_kernel=use_ip_kernel) if use_ip_kernel is not None else {}

    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=False)
    append_code_to_last_startup_file(pc_path, _script_test_gen_list_05)

    fln_yaml = "list.yaml"
    fln_yaml_path = os.path.join(pc_path, fln_yaml)

    pp = dict(startup_dir=pc_path, file_dir=pc_path, file_name=fln_yaml)
    assert not os.path.exists(fln_yaml_path)

    if use_ip_kernel:
        gen_list_of_plans_and_devices(**pp, **pp1)
        assert os.path.isfile(fln_yaml_path)
    else:
        with pytest.raises(RuntimeError, match="object has no attribute"):
            gen_list_of_plans_and_devices(**pp, **pp1)
        assert not os.path.exists(fln_yaml_path)


# fmt: off
@pytest.mark.parametrize("option", ["profile", "startup_dir", "script", "module"])
@pytest.mark.parametrize("use_ip_kernel", [True])
# fmt: on
def test_gen_list_of_plans_and_devices_06(monkeypatch, tmp_path, option, use_ip_kernel):
    """
    ``gen_list_of_plans_and_devices``: parameters ``startup_profile``, ``startup_dir``,
    ``startup_module_name``, ``startup_script_path``. The test does not cover edge cases.
    """
    # pc_path = copy_default_profile_collection(tmp_path, copy_yaml=False)
    ip_dir = os.path.join(tmp_path, "ipdir")
    profile_name = "sim"
    startup_dir = os.path.join(ip_dir, f"profile_{profile_name}", "startup")
    startup_path = os.path.join(startup_dir, "startup_script.py")
    script_dir = os.path.join(tmp_path, "script_dir", "script")
    script_fln = "startup_script.py"
    script_path = os.path.join(script_dir, script_fln)

    os.makedirs(startup_dir, exist_ok=True)
    os.makedirs(script_dir, exist_ok=True)

    fln_yaml = "list.yaml"

    if option == "profile":
        with open(startup_path, "w") as f:
            f.write(_startup_script_1)

        fln_yaml_path = os.path.join(startup_dir, fln_yaml)
        pp = dict(
            startup_profile=profile_name,
            ipython_dir=ip_dir,
            file_dir=startup_dir,
            file_name=fln_yaml,
            use_ipython_kernel=use_ip_kernel,
        )
    elif option == "startup_dir":
        with open(startup_path, "w") as f:
            f.write(_startup_script_1)

        fln_yaml_path = os.path.join(startup_dir, fln_yaml)
        pp = dict(
            startup_dir=startup_dir,
            file_dir=startup_dir,
            file_name=fln_yaml,
            use_ipython_kernel=use_ip_kernel,
        )
    elif option == "script":
        with open(script_path, "w") as f:
            f.write(_startup_script_1)

        fln_yaml_path = os.path.join(script_dir, fln_yaml)
        pp = dict(
            startup_script_path=script_path,
            file_dir=script_dir,
            file_name=fln_yaml,
            startup_profile=profile_name,
            ipython_dir=ip_dir,
            use_ipython_kernel=use_ip_kernel,
        )
    elif option == "module":
        module_dir, _ = os.path.split(script_dir)
        module_name = ".".join([_, os.path.splitext(script_fln)[0]])

        # Temporarily add module to the search path
        sys_path = sys.path
        monkeypatch.setattr(sys, "path", [str(module_dir)] + sys_path)

        with open(script_path, "w") as f:
            f.write(_startup_script_1)

        fln_yaml_path = os.path.join(script_dir, fln_yaml)

        pp = dict(
            startup_module_name=module_name,
            file_dir=script_dir,
            file_name=fln_yaml,
            startup_profile=profile_name,
            ipython_dir=ip_dir,
            use_ipython_kernel=use_ip_kernel,
        )

    assert not os.path.exists(fln_yaml_path)
    gen_list_of_plans_and_devices(**pp)
    assert os.path.isfile(fln_yaml_path)

    plans, devices = load_existing_plans_and_devices(fln_yaml_path)
    assert len(plans), pprint.pformat(plans)
    assert len(devices), pprint.pformat(devices)


_startup_script_1 = """
from ophyd.sim import det1, det2
from bluesky.plans import count

def simple_sample_plan_1():
    '''
    Simple plan for tests.
    '''
    yield from count([det1, det2])


def simple_sample_plan_2():
    '''
    Simple plan for tests.
    '''
    yield from count([det1, det2])

from bluesky import RunEngine
RE = RunEngine({})

from databroker.v0 import Broker
db = Broker.named('temp')
"""


# fmt: off
@pytest.mark.parametrize("test, exit_code", [
    ("startup_collection_at_current_dir", 0),
    ("startup_collection_dir", 0),
    ("startup_collection_incorrect_path_A", 1),
    ("startup_collection_incorrect_path_B", 1),
    ("startup_script_path", 0),
    ("startup_script_path_incorrect", 1),
    ("startup_module_name", 0),
    ("startup_module_name_incorrect", 1),
    ("file_incorrect_path", 1),
])
# fmt: on
def test_gen_list_of_plans_and_devices_cli_01(tmp_path, monkeypatch, test, exit_code):
    """
    Test for ``qserver-list-plans-devices`` CLI tool for generating list of plans and devices.
    Copy simulated profile collection and generate the list of allowed (in this case available)
    plans and devices based on the profile collection.
    """
    using_ipython = use_ipykernel_for_tests()

    ip_dir = os.path.join(tmp_path, "ipdir")
    profile_name = "sim"
    startup_dir = os.path.join(ip_dir, f"profile_{profile_name}", "startup")
    startup_path = os.path.join(startup_dir, "startup_script.py")

    module_name = "startup.startup_script"

    profile_name_empty = "empty"
    startup_dir_empty = os.path.join(ip_dir, f"profile_{profile_name_empty}", "startup")

    os.makedirs(startup_dir, exist_ok=True)
    with open(startup_path, "w") as f:
        f.write(_startup_script_1)

    os.makedirs(startup_dir_empty, exist_ok=True)

    fln_yaml = "existing_plans_and_devices.yaml"

    # Make sure that .yaml file does not exist
    assert not os.path.isfile(os.path.join(startup_dir, fln_yaml))

    os.chdir(tmp_path)

    if test == "startup_collection_at_current_dir":
        os.chdir(startup_dir)
        params = ["qserver-list-plans-devices", "--startup-dir", "."]

    elif test == "startup_collection_dir":
        params = ["qserver-list-plans-devices", "--startup-dir", startup_dir, "--file-dir", startup_dir]

    elif test == "startup_collection_incorrect_path_A":
        # Path exists (default path is used), but there are no startup files (fails)
        params = ["qserver-list-plans-devices", "--startup-dir", "."]

    elif test == "startup_collection_incorrect_path_B":
        # Path does not exist
        path_nonexisting = os.path.join(tmp_path, "abcde")
        params = ["qserver-list-plans-devices", "--startup-dir", path_nonexisting, "--file-dir", startup_dir]

    elif test == "startup_script_path":
        params = ["qserver-list-plans-devices", "--startup-script", startup_path, "--file-dir", startup_dir]
        if using_ipython:
            params.extend(["--ipython-dir", ip_dir, "--startup-profile", profile_name_empty])

    elif test == "startup_script_path_incorrect":
        params = [
            "qserver-list-plans-devices",
            "--startup-script",
            "non_existing_path",
            "--file-dir",
            startup_dir,
        ]
        if using_ipython:
            params.extend(["--ipython-dir", ip_dir, "--startup-profile", profile_name_empty])

    elif test == "startup_module_name":
        monkeypatch.setenv("PYTHONPATH", os.path.split(startup_dir)[0])
        params = ["qserver-list-plans-devices", "--startup-module", module_name, "--file-dir", startup_dir]
        if using_ipython:
            params.extend(["--ipython-dir", ip_dir, "--startup-profile", profile_name_empty])

    elif test == "startup_module_name_incorrect":
        monkeypatch.setenv("PYTHONPATH", os.path.split(startup_dir)[0])
        s_name = "incorrect.module.name"
        params = ["qserver-list-plans-devices", "--startup-module", s_name, "--file-dir", startup_dir]
        if using_ipython:
            params.extend(["--ipython-dir", ip_dir, "--startup-profile", profile_name_empty])

    elif test == "file_incorrect_path":
        # Path does not exist
        path_nonexisting = os.path.join(tmp_path, "abcde")
        params = ["qserver-list-plans-devices", "--startup-dir", startup_dir, "--file-dir", path_nonexisting]

    else:
        assert False, f"Unknown test '{test}'"

    if using_ipython:
        params.append("--use-ipython-kernel=ON")

    assert subprocess.call(params) == exit_code

    if exit_code == 0:
        assert os.path.isfile(os.path.join(startup_dir, fln_yaml))
    else:
        assert not os.path.isfile(os.path.join(startup_dir, fln_yaml))


# fmt: off
@pytest.mark.parametrize("ignore_invalid_plans", [False, True, None])
# fmt: on
def test_gen_list_of_plans_and_devices_cli_02(tmp_path, ignore_invalid_plans):
    """
    ``qserver-list-plans-devices``: tests for '--ignore-invalid-plans' parameter.
    """
    using_ipython = use_ipykernel_for_tests()

    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=False)
    append_code_to_last_startup_file(pc_path, _script_test_plan_invalid_1)

    params = ["qserver-list-plans-devices", "--startup-dir", pc_path, "--file-dir", pc_path]
    if ignore_invalid_plans is not None:
        _ = "ON" if ignore_invalid_plans else "OFF"
        params.append(f"--ignore-invalid-plans={_}")

    fln_yaml = "existing_plans_and_devices.yaml"

    # Make sure that .yaml file does not exist
    assert not os.path.isfile(os.path.join(pc_path, fln_yaml))

    os.chdir(tmp_path)

    if using_ipython:
        params.append("--use-ipython-kernel=ON")

    exit_code = 0 if ignore_invalid_plans else 1
    assert subprocess.call(params) == exit_code

    if exit_code == 0:
        assert os.path.isfile(os.path.join(pc_path, fln_yaml))
    else:
        assert not os.path.isfile(os.path.join(pc_path, fln_yaml))


# fmt: off
@pytest.mark.parametrize("device_max_depth, det_structure", [
    (None, _sim_bundle_A_depth_0),
    (0, _sim_bundle_A_depth_0),
    (1, _sim_bundle_A_depth_1),
    (2, _sim_bundle_A_depth_2),
])
# fmt: on
def test_gen_list_of_plans_and_devices_cli_03(tmp_path, device_max_depth, det_structure):
    """
    ``qserver-list-plans-devices``: tests for '--device-max-depth' parameter.
    """
    using_ipython = use_ipykernel_for_tests()

    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=False)

    params = ["qserver-list-plans-devices", "--startup-dir", pc_path, "--file-dir", pc_path]
    if device_max_depth is not None:
        params.append(f"--device-max-depth={device_max_depth}")

    fln_yaml = "existing_plans_and_devices.yaml"
    fln_yaml_path = os.path.join(pc_path, fln_yaml)

    # Make sure that .yaml file does not exist
    assert not os.path.isfile(os.path.join(pc_path, fln_yaml))

    os.chdir(tmp_path)

    if using_ipython:
        params.append("--use-ipython-kernel=ON")

    exit_code = 0
    assert subprocess.call(params) == exit_code
    assert os.path.isfile(os.path.join(pc_path, fln_yaml))

    _, devices = load_existing_plans_and_devices(fln_yaml_path)

    def reduce_device_description(description):
        def inner(dev, dev_out):
            if "components" in dev:
                for name in dev["components"]:
                    dev_out[name] = {}
                    inner(dev["components"][name], dev_out[name])

        dev_out = {}
        inner(description, dev_out)
        return dev_out

    desc = reduce_device_description(devices["sim_bundle_A"])
    assert desc == det_structure, pprint.pformat(desc)


# fmt: off
@pytest.mark.parametrize("use_ip_kernel", [False, True, None])
# fmt: on
def test_gen_list_of_plans_and_devices_cli_04(tmp_path, use_ip_kernel):
    """
    ``qserver-list-plans-devices``: parameter ``--use-ipython-kernel``. Check that
    IPython-specific code is loaded only if ``--use-ipython-kernel=ON``.
    """
    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=False)
    append_code_to_last_startup_file(pc_path, _script_test_gen_list_05)

    fln_yaml = "list.yaml"
    fln_yaml_path = os.path.join(pc_path, fln_yaml)

    params = [
        "qserver-list-plans-devices",
        f"--startup-dir={pc_path}",
        f"--file-dir={pc_path}",
        f"--file-name={fln_yaml}",
    ]
    if use_ip_kernel is not None:
        _ = "ON" if use_ip_kernel else "OFF"
        params.append(f"--use-ipython-kernel={_}")

    assert not os.path.exists(fln_yaml_path)
    exit_code = 0 if use_ip_kernel else 1
    assert subprocess.call(params) == exit_code

    if use_ip_kernel:
        assert os.path.isfile(fln_yaml_path)
    else:
        assert not os.path.exists(fln_yaml_path)
