import os
import pytest
import copy
import yaml
import typing
from typing import Dict, Optional
import subprocess
import pprint
import sys
import enum
import inspect
from collections.abc import Callable
import re
import shutil
import time as ttime


try:
    from bluesky import protocols
except ImportError:
    import bluesky_queueserver.manager._protocols as protocols

import ophyd
import ophyd.sim

from .common import copy_default_profile_collection, patch_first_startup_file, append_code_to_last_startup_file

from .common import reset_sys_modules  # noqa: F401

from bluesky_queueserver.manager.annotation_decorator import parameter_annotation_decorator

from bluesky_queueserver.manager.profile_ops import (
    get_default_startup_dir,
    load_profile_collection,
    load_startup_script,
    load_startup_module,
    load_worker_startup_code,
    load_script_into_existing_nspace,
    extract_script_root_path,
    plans_from_nspace,
    devices_from_nspace,
    prepare_plan,
    gen_list_of_plans_and_devices,
    load_existing_plans_and_devices,
    update_existing_plans_and_devices,
    load_user_group_permissions,
    _process_plan,
    validate_plan,
    bind_plan_arguments,
    _select_allowed_plans,
    load_allowed_plans_and_devices,
    _prepare_plans,
    _prepare_devices,
    StartupLoadingError,
    ScriptLoadingError,
    _process_annotation,
    _decode_parameter_types_and_defaults,
    _process_default_value,
    construct_parameters,
    _check_ranges,
    format_text_descriptions,
    check_if_function_allowed,
    _validate_user_group_permissions_schema,
    prepare_function,
    _split_name_pattern,
    _build_device_name_list,
    _build_plan_name_list,
    _find_and_replace_built_in_types,
    _is_object_name_in_list,
    _get_nspace_object,
    _filter_allowed_plans,
    _filter_device_tree,
)

# User name and user group name used throughout most of the tests.
_user, _user_group = "Testing Script", "admin"

python_version = sys.version_info  # Can be compare to tuples (such as 'python_version >= (3, 9)')


def test_get_default_startup_dir():
    """
    Function `get_default_startup_dir`
    """
    pc_path = get_default_startup_dir()
    assert os.path.exists(pc_path), "Directory with default profile collection deos not exist."


def test_load_profile_collection_1():
    """
    Loading default profile collection
    """
    pc_path = get_default_startup_dir()
    nspace = load_profile_collection(pc_path)
    assert len(nspace) > 0, "Failed to load the profile collection"


def test_load_profile_collection_2(tmp_path):
    """
    Loading a copy of the default profile collection
    """
    pc_path = copy_default_profile_collection(tmp_path)
    nspace = load_profile_collection(pc_path)
    assert len(nspace) > 0, "Failed to load the profile collection"


code_local_import = """
from dir1.dir2.file2 import *
"""


def create_local_imports_dirs(tmp_path):
    path1 = os.path.join(tmp_path, "dir1")
    path2 = os.path.join(path1, "dir2")
    fln1 = os.path.join(path1, "file1.py")
    fln2 = os.path.join(path2, "file2.py")

    os.makedirs(path2, exist_ok=True)

    # Create file1
    code1 = """
def f1():
    pass
"""
    with open(fln1, "w") as f:
        f.writelines(code1)

        # Create file2
        code1 = """
from ..file1 import *

def f2():
    pass
"""
    with open(fln2, "w") as f:
        f.writelines(code1)


# fmt: off
@pytest.mark.parametrize("local_imports", [False, True])
@pytest.mark.parametrize("additional_code, success, errmsg", [
    # Patched as expected
    ("""
\n
from IPython import get_ipython

get_ipython().user_ns

""", True, ""),

    # Patching an indented block (make sure that indentation is treated correctly)
    ("""
\n
if True:
    from IPython import get_ipython

    get_ipython().user_ns

""", True, ""),

    # Patched as expected ('get_ipython()' is not imported)
    ("""
\n
get_ipython().user_ns
""", True, ""),

    # Patched as expected ('get_ipython' is commented in the import statement)
    ("""
\n
from IPython import version_info #, get_ipython

get_ipython().user_ns
""", True, ""),

    # Commented 'get_ipython' -> OK
    ("""
\n
a = 10  # get_ipython().user_ns
""", True, ""),

    # Patched multiiple times
    ("""
\n
get_ipython().user_ns
from IPython import get_ipython
get_ipython().user_ns
from IPython import get_ipython
get_ipython().user_ns
from IPython import get_ipython
get_ipython().user_ns

""", True, ""),

    # Raise exception in the profile
    ("""
\n
raise Exception("Manually raised exception.")

""", False, "Manually raised exception."),

])
# fmt: on
def test_load_profile_collection_3(tmp_path, local_imports, additional_code, success, errmsg):
    """
    Loading a copy of the default profile collection
    """
    pc_path = copy_default_profile_collection(tmp_path)

    create_local_imports_dirs(pc_path)

    patch_first_startup_file(pc_path, additional_code)
    if local_imports:
        # Note: local imports go above the additional code.
        patch_first_startup_file(pc_path, code_local_import)

    if success:
        nspace = load_profile_collection(pc_path)
        assert len(nspace) > 0, "Failed to load the profile collection"
        if local_imports:
            assert "f1" in nspace, "Test for local imports failed"
            assert "f2" in nspace, "Test for local imports failed"
    else:
        with pytest.raises(Exception, match=errmsg):
            load_profile_collection(pc_path)


def test_load_profile_collection_4_fail(tmp_path):
    """
    Failing cases
    """
    # Non-existing path
    pc_path = os.path.join(tmp_path, "abc")
    with pytest.raises(IOError, match="Path .+ does not exist"):
        load_profile_collection(pc_path)

    # 'Empty' profile collection (no startup files)
    with pytest.raises(IOError, match="The directory .+ contains no startup files"):
        load_profile_collection(tmp_path)

    pc_path = os.path.join(tmp_path, "test.txt")
    # Create a file
    with open(pc_path, "w"):
        pass
    with pytest.raises(IOError, match="Path .+ is not a directory"):
        load_profile_collection(pc_path)


@pytest.mark.parametrize("keep_re", [True, False])
def test_load_profile_collection_5(tmp_path, keep_re):
    """
    Loading a copy of the default profile collection
    """
    pc_path = copy_default_profile_collection(tmp_path)

    patch = """
from bluesky import RunEngine
RE = RunEngine({})
from databroker import Broker
db = Broker.named('temp')
RE.subscribe(db.insert)
"""
    patch_first_startup_file(pc_path, patch)

    nspace = load_profile_collection(pc_path, keep_re=keep_re)
    if keep_re:
        assert "RE" in nspace
        assert "db" in nspace
    else:
        assert "RE" not in nspace
        assert "db" not in nspace


_happi_json_db = """
{
  "det": {
    "_id": "det",
    "active": true,
    "args": [],
    "device_class": "ophyd.sim.DetWithCountTime",
    "documentation": null,
    "kwargs": {
      "name": "{{name}}"
    },
    "name": "det",
    "type": "OphydItem"
  },
  "motor": {
    "_id": "motor",
    "active": true,
    "args": [],
    "device_class": "ophyd.sim.SynAxisNoPosition",
    "documentation": null,
    "kwargs": {
      "name": "{{name}}"
    },
    "name": "motor",
    "type": "OphydItem"
  },
  "tst_motor1": {
    "_id": "tst_motor1",
    "active": true,
    "args": [],
    "device_class": "ophyd.sim.SynAxisNoHints",
    "documentation": null,
    "kwargs": {
      "name": "{{name}}"
    },
    "name": "tst_motor1",
    "type": "OphydItem"
  },
  "tst_motor2": {
    "_id": "tst_motor2",
    "active": true,
    "args": [],
    "device_class": "ophyd.sim.SynAxisNoHints",
    "documentation": null,
    "kwargs": {
      "name": "{{name}}"
    },
    "name": "tst_motor2",
    "type": "OphydItem"
  },
  "tst_motor3": {
    "_id": "tst_motor3",
    "active": true,
    "args": [],
    "device_class": "ophyd.sim.SynAxis",
    "documentation": null,
    "kwargs": {
      "name": "{{name}}"
    },
    "name": "tst_motor3",
    "type": "OphydItem"
  }
}
"""


def _configure_happi(tmp_path, monkeypatch):
    path_json = os.path.join(tmp_path, "sim_devices.json")
    path_ini = os.path.join(tmp_path, "happi.ini")

    happi_ini_text = f"[DEFAULT]\nbackend=json\npath={path_json}"

    with open(path_ini, "w") as f:
        f.write(happi_ini_text)

    with open(path_json, "w") as f:
        f.write(_happi_json_db)

    monkeypatch.setenv("HAPPI_CFG", path_ini)


_startup_script_happi_1 = """
from bluesky.plans import count
from bluesky_queueserver.manager.profile_tools import load_devices_from_happi

# Specify the list of devices to load
device_list = [
    "det",  # Search for the device 'det' and load it as 'det.
    ("motor", ""),  #  Search for the device 'motor' and loaded it as 'motor'
    ("tst_motor2", "motor2"),  # Search for 'tst_motor2' and rename it to 'motor2'
]

# Load the devices in the script namespace. It is assumed that Happi is configured
#   properly and there is no need to specify the backend and the path.
load_devices_from_happi(device_list, namespace=locals())

def simple_sample_plan_1():
    '''
    Simple plan for tests. Calling standard 'count' plan.
    '''
    yield from count([det], num=5, delay=1)
"""


def _verify_happi_namespace(nspace):
    """
    Check contents of the namespace created by loading `_startup_script_happi_1`.
    """
    assert "det" in nspace, pprint.pformat(nspace)
    assert isinstance(nspace["det"], ophyd.sim.DetWithCountTime)
    assert "motor" in nspace
    assert isinstance(nspace["motor"], ophyd.sim.SynAxisNoPosition)
    assert "motor2" in nspace
    assert isinstance(nspace["motor2"], ophyd.sim.SynAxisNoHints)
    assert "count" in nspace
    assert "simple_sample_plan_1" in nspace


def test_load_profile_collection_6(tmp_path, monkeypatch):
    """
    Load profile collection: instantiation of devices using Happi.
    """
    _configure_happi(tmp_path, monkeypatch)

    pc_path = os.path.join(tmp_path, "script_dir1")
    script_path = os.path.join(pc_path, "startup_script.py")

    os.makedirs(pc_path, exist_ok=True)
    with open(script_path, "w") as f:
        f.write(_startup_script_happi_1)

    nspace = load_profile_collection(pc_path)

    _verify_happi_namespace(nspace)


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

from databroker import Broker
db = Broker.named('temp')
"""


_startup_script_2 = """
from ophyd.sim import det1, det2
from bluesky.plans import count


def simple_sample_plan_3():
    '''
    Simple plan for tests.
    '''
    yield from count([det1, det2])


def simple_sample_plan_4():
    '''
    Simple plan for tests.
    '''
    yield from count([det1, det2])

"""


@pytest.mark.parametrize("keep_re", [True, False])
@pytest.mark.parametrize("enable_local_imports", [True, False])
def test_load_startup_script_1(tmp_path, keep_re, enable_local_imports, reset_sys_modules):  # noqa: F811
    """
    Basic test for `load_startup_script` function. Load two scripts in sequence from two
    different locations and make sure that all the plans are loaded.
    There are NO LOCAL IMPORTS in the scripts, so the script should work with/without local
    imports.
    """
    # Load first script
    script_dir = os.path.join(tmp_path, "script_dir1")
    script_path = os.path.join(script_dir, "startup_script.py")

    os.makedirs(script_dir, exist_ok=True)
    with open(script_path, "w") as f:
        f.write(_startup_script_1)

    nspace = load_startup_script(script_path, keep_re=keep_re, enable_local_imports=enable_local_imports)

    assert nspace
    assert "simple_sample_plan_1" in nspace, pprint.pformat(nspace)
    assert "simple_sample_plan_2" in nspace, pprint.pformat(nspace)
    if keep_re:
        assert "RE" in nspace, pprint.pformat(nspace)
        assert "db" in nspace, pprint.pformat(nspace)
    else:
        assert "RE" not in nspace, pprint.pformat(nspace)
        assert "db" not in nspace, pprint.pformat(nspace)

    # Load script #2 (same name, but different path)
    script_dir = os.path.join(tmp_path, "script_dir2")
    script_path = os.path.join(script_dir, "startup_script.py")

    os.makedirs(script_dir, exist_ok=True)
    with open(script_path, "w") as f:
        f.write(_startup_script_2)

    nspace = load_startup_script(script_path, keep_re=keep_re, enable_local_imports=enable_local_imports)

    assert nspace
    assert "simple_sample_plan_3" in nspace, pprint.pformat(nspace)
    assert "simple_sample_plan_4" in nspace, pprint.pformat(nspace)
    assert "RE" not in nspace, pprint.pformat(nspace)
    assert "db" not in nspace, pprint.pformat(nspace)


_imported_module_1 = """
from ophyd.sim import det1, det2
from bluesky.plans import count

def plan_in_module_1():
    '''
    Simple plan for tests.
    '''
    yield from count([det1, det2])
"""

_imported_module_1_modified = """
from ophyd.sim import det1, det2
from bluesky.plans import count

def plan_in_module_1_modified():
    '''
    Simple plan for tests.
    '''
    yield from count([det1, det2])
"""

_imported_module_2 = """
from ophyd.sim import det1, det2
from bluesky.plans import count

def plan_in_module_2():
    '''
    Simple plan for tests.
    '''
    yield from count([det1, det2])
"""


@pytest.mark.parametrize("keep_re", [True, False])
@pytest.mark.parametrize("enable_local_imports", [True, False])
def test_load_startup_script_2(tmp_path, keep_re, enable_local_imports, reset_sys_modules):  # noqa: F811
    """
    Tests for `load_startup_script` function. Loading scripts WITH LOCAL IMPORTS.
    Loading is expected to fail if local imports are disabled.

    The test contains the following steps:
    - Load the script that contains local import statement, make sure that the imported contents
      is in the namespace.
    - Change the code in the imported module and reload the script. Make sure that the changed
      code was imported.
    - Load a script located in a different directory that is importing module with the same name
      (same relative path to the script), but containing different code. Make sure that correct
      module is imported.
    """
    # Load first script
    script_dir = os.path.join(tmp_path, "script_dir1")
    script_path = os.path.join(script_dir, "startup_script.py")
    module_dir = os.path.join(script_dir, "mod")
    module_path = os.path.join(module_dir, "imported_module.py")

    script_patch = "from mod.imported_module import *\n"

    os.makedirs(script_dir, exist_ok=True)
    os.makedirs(module_dir, exist_ok=True)
    with open(script_path, "w") as f:
        f.write(script_patch + _startup_script_1)
    with open(module_path, "w") as f:
        f.write(_imported_module_1)

    if enable_local_imports:
        nspace = load_startup_script(script_path, keep_re=keep_re, enable_local_imports=enable_local_imports)

        assert nspace
        assert "simple_sample_plan_1" in nspace, pprint.pformat(nspace)
        assert "simple_sample_plan_2" in nspace, pprint.pformat(nspace)
        assert "plan_in_module_1" in nspace, pprint.pformat(nspace)
        if keep_re:
            assert "RE" in nspace, pprint.pformat(nspace)
            assert "db" in nspace, pprint.pformat(nspace)
        else:
            assert "RE" not in nspace, pprint.pformat(nspace)
            assert "db" not in nspace, pprint.pformat(nspace)
    else:
        # Expected to fail if local imports are not enaabled
        with pytest.raises(StartupLoadingError):
            load_startup_script(script_path, keep_re=keep_re, enable_local_imports=enable_local_imports)

    # Reload the same script, but replace the code in the module (emulate the process of code editing).
    #   Check that the new code is loaded when the module is imported.
    with open(module_path, "w") as f:
        f.write(_imported_module_1_modified)

    if enable_local_imports:
        nspace = load_startup_script(script_path, keep_re=keep_re, enable_local_imports=enable_local_imports)
        assert "plan_in_module_1" not in nspace, pprint.pformat(nspace)
        assert "plan_in_module_1_modified" in nspace, pprint.pformat(nspace)

    else:
        # Expected to fail if local imports are not enaabled
        with pytest.raises(StartupLoadingError):
            load_startup_script(script_path, keep_re=keep_re, enable_local_imports=enable_local_imports)

    # Load different script (same name, but different path). The script imports module with the same name
    #   (with the same relative path). Check that the correct version of the module is loaded.
    script_dir = os.path.join(tmp_path, "script_dir2")
    script_path = os.path.join(script_dir, "startup_script.py")
    module_dir = os.path.join(script_dir, "mod")
    module_path = os.path.join(module_dir, "imported_module.py")

    script_patch = "from mod.imported_module import *\n"

    os.makedirs(script_dir, exist_ok=True)
    os.makedirs(module_dir, exist_ok=True)
    with open(script_path, "w") as f:
        f.write(script_patch + _startup_script_2)
    with open(module_path, "w") as f:
        f.write(_imported_module_2)

    if enable_local_imports:
        nspace = load_startup_script(script_path, keep_re=keep_re, enable_local_imports=enable_local_imports)

        assert nspace
        assert "simple_sample_plan_3" in nspace, pprint.pformat(nspace)
        assert "simple_sample_plan_4" in nspace, pprint.pformat(nspace)
        assert "RE" not in nspace, pprint.pformat(nspace)
        assert "db" not in nspace, pprint.pformat(nspace)
    else:
        # Expected to fail if local imports are not enaabled
        with pytest.raises(StartupLoadingError):
            load_startup_script(script_path, keep_re=keep_re, enable_local_imports=enable_local_imports)


_startup_script_3 = """
a = 10
locals()['b'] = 20
globals()['c'] = 50
"""


def test_load_startup_script_3(tmp_path, reset_sys_modules):  # noqa: F811
    """
    Test for ``load_startup_script`` function.
    Verifies if variables defined in global and local scope in the script are handled correctly.
    """
    # Load first script
    script_dir = os.path.join(tmp_path, "script_dir1")
    script_path = os.path.join(script_dir, "startup_script.py")

    os.makedirs(script_dir, exist_ok=True)
    with open(script_path, "w") as f:
        f.write(_startup_script_3)

    nspace = load_startup_script(script_path)

    expected_results = {"a": 10, "b": 20, "c": 50}
    for k, v in expected_results.items():
        assert k in nspace
        assert nspace[k] == v


_startup_script_4 = """
# Script with more sophisticated imports

import ophyd
from ophyd import Device, Component as Cpt

class SimStage(Device):
    x = Cpt(ophyd.sim.SynAxis, name="y", labels={"motors"})
    y = Cpt(ophyd.sim.SynAxis, name="y", labels={"motors"})
    z = Cpt(ophyd.sim.SynAxis, name="z", labels={"motors"})

    def set(self, x, y, z):
        self.x.set(x)
        self.y.set(y)
        self.z.set(z)

sim_stage = SimStage(name="sim_stage")
"""


@pytest.mark.parametrize("keep_re", [True, False])
@pytest.mark.parametrize("enable_local_imports", [True, False])
def test_load_startup_script_4(tmp_path, keep_re, enable_local_imports, reset_sys_modules):  # noqa: F811
    """
    Load a startup script with more sophisticated imports
    """
    # Load script script #2a (same name, but different path)
    script_dir = os.path.join(tmp_path, "script_dir3")
    script_path = os.path.join(script_dir, "startup_script.py")

    os.makedirs(script_dir, exist_ok=True)
    with open(script_path, "w") as f:
        f.write(_startup_script_4)

    nspace = load_startup_script(script_path, keep_re=keep_re, enable_local_imports=enable_local_imports)

    assert nspace
    assert "SimStage" in nspace, pprint.pformat(nspace)
    assert "sim_stage" in nspace, pprint.pformat(nspace)
    assert "RE" not in nspace, pprint.pformat(nspace)
    assert "db" not in nspace, pprint.pformat(nspace)


def test_load_startup_script_5(tmp_path, monkeypatch):
    """
    Load startup script: instantiation of devices using Happi.
    """
    _configure_happi(tmp_path, monkeypatch)

    pc_path = os.path.join(tmp_path, "script_dir1")
    script_path = os.path.join(pc_path, "startup_script.py")

    os.makedirs(pc_path, exist_ok=True)
    with open(script_path, "w") as f:
        f.write(_startup_script_happi_1)

    nspace = load_startup_script(script_path)

    _verify_happi_namespace(nspace)


# fmt: off
@pytest.mark.parametrize("params, result", [
    ({"startup_dir": "/abc/def"}, "/abc/def"),
    ({"startup_module_name": "some.module"}, None),
    ({"startup_script_path": "/abc/def/script.py"}, "/abc/def"),
    ({}, None),
])
# fmt: on
def test_extract_script_root_path_1(params, result):
    assert extract_script_root_path(**params) == result


# fmt: off
@pytest.mark.parametrize("update_re", [False, True])
@pytest.mark.parametrize("scripts", [(_startup_script_1,), (_startup_script_1, _startup_script_2)])
# fmt: on
def test_load_script_into_existing_nspace_1(scripts, update_re):
    """
    Basic test for ``load_script_into_existing_nspace``.
    """
    pc_path = get_default_startup_dir()
    nspace = load_profile_collection(pc_path)
    assert len(nspace) > 0, "Failed to load the profile collection"

    for s in scripts:
        load_script_into_existing_nspace(script=s, nspace=nspace, update_re=update_re)

    name_list = []
    if _startup_script_1 in scripts:
        name_list.extend(["simple_sample_plan_1", "simple_sample_plan_2"])
        if update_re:
            name_list.extend(["RE", "db"])

    if _startup_script_2 in scripts:
        name_list.extend(["simple_sample_plan_3", "simple_sample_plan_4"])

    for name in name_list:
        assert name in nspace


# fmt: off
@pytest.mark.parametrize("update_re", [True, False])
@pytest.mark.parametrize("enable_local_imports", [True, False])
# fmt: on
def test_load_script_into_existing_nspace_2(
    tmp_path, update_re, enable_local_imports, reset_sys_modules  # noqa: F811
):
    """
    Tests for `load_script_into_existing_nspace` function. Loading scripts WITH LOCAL IMPORTS.
    Loading is expected to fail if local imports are disabled.

    The test contains the following steps:
    - Load the script that contains local import statement, make sure that the imported contents
      is in the namespace.
    - Change the code in the imported module and reload the script. Make sure that the changed
      code was imported. Note, that the new function is added to the namespace!!! Removing function
      from the script does not automatically remove it from the namespace.
    - Load a script located in a different directory that is importing module with the same name
      (same relative path to the script), but containing different code. Make sure that functions
      from the module are added to the namespace (items are never removed from the namespace
      automatically).
    """
    # Load first script
    script_dir = os.path.join(tmp_path, "script_dir1")
    # script_path = os.path.join(script_dir, "startup_script.py")
    module_dir = os.path.join(script_dir, "mod")
    module_path = os.path.join(module_dir, "imported_module.py")

    script_patch = "from mod.imported_module import *\n"

    os.makedirs(script_dir, exist_ok=True)
    os.makedirs(module_dir, exist_ok=True)
    with open(module_path, "w") as f:
        f.write(_imported_module_1)

    script = script_patch + _startup_script_1

    nspace = {}

    if enable_local_imports:
        load_script_into_existing_nspace(
            script=script,
            nspace=nspace,
            enable_local_imports=enable_local_imports,
            script_root_path=script_dir,
            update_re=update_re,
        )

        assert nspace
        assert "simple_sample_plan_1" in nspace, pprint.pformat(nspace)
        assert "simple_sample_plan_2" in nspace, pprint.pformat(nspace)
        assert "plan_in_module_1" in nspace, pprint.pformat(nspace)
        if update_re:
            assert "RE" in nspace, pprint.pformat(nspace)
            assert "db" in nspace, pprint.pformat(nspace)
        else:
            assert "RE" not in nspace, pprint.pformat(nspace)
            assert "db" not in nspace, pprint.pformat(nspace)
    else:
        # Expected to fail if local imports are not enaabled
        with pytest.raises(ScriptLoadingError):
            load_script_into_existing_nspace(
                script=script,
                nspace=nspace,
                enable_local_imports=enable_local_imports,
                script_root_path=script_dir,
                update_re=update_re,
            )

    # Reload the same script, but replace the code in the module (emulate the process of code editing).
    #   Check that the new code is loaded when the module is imported.
    with open(module_path, "w") as f:
        f.write(_imported_module_1_modified)

    if enable_local_imports:
        load_script_into_existing_nspace(
            script=script,
            nspace=nspace,
            enable_local_imports=enable_local_imports,
            script_root_path=script_dir,
            update_re=update_re,
        )
        assert "plan_in_module_1_modified" in nspace, pprint.pformat(nspace)

    else:
        # Expected to fail if local imports are not enaabled
        with pytest.raises(ScriptLoadingError):
            load_script_into_existing_nspace(
                script=script,
                nspace=nspace,
                enable_local_imports=enable_local_imports,
                script_root_path=script_dir,
                update_re=update_re,
            )

    # Load different script (same name, but different path). The script imports module with the same name
    #   (with the same relative path). Check that the correct version of the module is loaded.
    script_dir = os.path.join(tmp_path, "script_dir2")
    module_dir = os.path.join(script_dir, "mod")
    module_path = os.path.join(module_dir, "imported_module.py")

    script_patch = "from mod.imported_module import *\n"

    os.makedirs(script_dir, exist_ok=True)
    os.makedirs(module_dir, exist_ok=True)
    with open(module_path, "w") as f:
        f.write(_imported_module_2)

    script = script_patch + _startup_script_2

    if enable_local_imports:
        load_script_into_existing_nspace(
            script=script,
            nspace=nspace,
            enable_local_imports=enable_local_imports,
            script_root_path=script_dir,
            update_re=update_re,
        )

        assert nspace
        assert "simple_sample_plan_1" in nspace, pprint.pformat(nspace)
        assert "simple_sample_plan_2" in nspace, pprint.pformat(nspace)
        assert "simple_sample_plan_3" in nspace, pprint.pformat(nspace)
        assert "simple_sample_plan_4" in nspace, pprint.pformat(nspace)
        assert "plan_in_module_1" in nspace, pprint.pformat(nspace)
        assert "plan_in_module_2" in nspace, pprint.pformat(nspace)
        if update_re:
            assert "RE" in nspace, pprint.pformat(nspace)
            assert "db" in nspace, pprint.pformat(nspace)
        else:
            assert "RE" not in nspace, pprint.pformat(nspace)
            assert "db" not in nspace, pprint.pformat(nspace)
    else:
        # Expected to fail if local imports are not enaabled
        with pytest.raises(ScriptLoadingError):
            load_script_into_existing_nspace(
                script=script,
                nspace=nspace,
                enable_local_imports=enable_local_imports,
                script_root_path=script_dir,
                update_re=update_re,
            )


def test_load_script_into_existing_nspace_3():  # noqa: F811
    """
    Test for ``load_script_into_existing_nspace``. Verifies if variables defined in global and
    local scope in the script are handled correctly.
    """
    pc_path = get_default_startup_dir()
    nspace = load_profile_collection(pc_path)
    assert len(nspace) > 0, "Failed to load the profile collection"

    load_script_into_existing_nspace(script=_startup_script_3, nspace=nspace)

    expected_results = {"a": 10, "b": 20, "c": 50}
    for k, v in expected_results.items():
        assert k in nspace
        assert nspace[k] == v


_startup_script_failing_1 = """
a = 50
b = c  # Undefined variable
"""

_startup_script_failing_2 = """
def func():
    a = 10
     b = 20  # Indentation
"""


# fmt: off
@pytest.mark.parametrize("script, ex_type, error_msg", [
    (_startup_script_failing_1, NameError, "name 'c' is not defined"),
    (_startup_script_failing_2, IndentationError, "unexpected indent"),
])
# fmt: on
def test_load_script_into_existing_nspace_4(script, ex_type, error_msg):  # noqa: F811
    """
    Test for ``load_script_into_existing_nspace``. Errors in executed script.
    """
    pc_path = get_default_startup_dir()
    nspace = load_profile_collection(pc_path)
    assert len(nspace) > 0, "Failed to load the profile collection"

    with pytest.raises(ScriptLoadingError, match=error_msg):
        load_script_into_existing_nspace(script=script, nspace=nspace)


def test_load_script_into_existing_nspace_5():  # noqa: F811
    """
    Test for ``load_script_into_existing_nspace``. Errors in executed script.
    """
    pc_path = get_default_startup_dir()
    nspace = load_profile_collection(pc_path)
    assert len(nspace) > 0, "Failed to load the profile collection"

    try:
        load_script_into_existing_nspace(script=_startup_script_failing_1, nspace=nspace)
    except ScriptLoadingError:
        pass

    # Script fails after variable 'a' is defined, so it is expected to be loaded to the environment.
    assert "a" in nspace


_startup_script_5 = """
a = 50

def modify_a():
    global a
    a = 90

def get_a():
    return a
"""


def test_load_script_into_existing_nspace_6():  # noqa: F811
    """
    Test for ``load_script_into_existing_nspace``. Modify global variable from function
    and externally. This test is mostly to make sure the environment works as expected.
    """
    pc_path = get_default_startup_dir()
    nspace = load_profile_collection(pc_path)
    assert len(nspace) > 0, "Failed to load the profile collection"

    load_script_into_existing_nspace(script=_startup_script_5, nspace=nspace)

    assert nspace["a"] == 50
    nspace["modify_a"]()
    assert nspace["a"] == 90

    assert nspace["get_a"]() == 90
    nspace["a"] = 100
    assert nspace["get_a"]() == 100


def test_load_script_into_existing_nspace_7():  # noqa: F811
    """
    Test for ``load_script_into_existing_nspace``. Modify global variable from function.
    """
    pc_path = get_default_startup_dir()
    nspace = load_profile_collection(pc_path)
    assert len(nspace) > 0, "Failed to load the profile collection"

    load_script_into_existing_nspace(script=_startup_script_5, nspace=nspace)

    assert nspace["a"] == 50
    nspace["modify_a"]()
    assert nspace["a"] == 90


# fmt: off
@pytest.mark.parametrize("update_re", [False, True])
# fmt: on
def test_load_script_into_existing_nspace_8(update_re):  # noqa: F811
    """
    Test for ``load_script_into_existing_nspace``. Modify global variable from function.
    """
    nspace = {}

    # Load script that contains RE and db. Set 'update_re' as True.
    load_script_into_existing_nspace(script=_startup_script_1, nspace=nspace, update_re=True)

    assert "RE" in nspace
    assert nspace["RE"]
    assert "db" in nspace
    assert nspace["db"]

    script = """RE=None\ndb=None"""
    load_script_into_existing_nspace(script=script, nspace=nspace, update_re=update_re)
    if update_re:
        assert nspace["RE"] is None
        assert nspace["db"] is None
    else:
        assert nspace["RE"]
        assert nspace["db"]


def test_load_script_into_existing_nspace_9():  # noqa: F811
    """
    Load script with more sophisticated use of imported types.
    """
    nspace = {}

    # Load script that contains RE and db. Set 'update_re' as True.
    load_script_into_existing_nspace(script=_startup_script_4, nspace=nspace, update_re=True)

    assert "SimStage" in nspace
    assert "sim_stage" in nspace


@pytest.mark.parametrize("keep_re", [True, False])
def test_load_startup_module_1(tmp_path, monkeypatch, keep_re, reset_sys_modules):  # noqa: F811
    """
    Test for `load_startup_module` function: import module that is in the module search path.
    The test also demonstrates that if the code of the module or any module imported by the module
    is changed, loading of the module again does not load the new code, i.e. application needs to
    be restarted if the code is edited.
    """
    # Load first script
    script_dir = os.path.join(tmp_path, "script_dir1")
    script_path = os.path.join(script_dir, "startup_script.py")
    module_dir = os.path.join(script_dir, "mod")
    module_path = os.path.join(module_dir, "imported_module.py")

    script_patch = "from .mod.imported_module import *\n"

    os.makedirs(script_dir, exist_ok=True)
    os.makedirs(module_dir, exist_ok=True)
    with open(script_path, "w") as f:
        f.write(script_patch + _startup_script_1)

    with open(module_path, "w") as f:
        f.write(_imported_module_1)

    # Temporarily add module to the search path
    sys_path = sys.path
    monkeypatch.setattr(sys, "path", [str(tmp_path)] + sys_path)

    nspace = load_startup_module("script_dir1.startup_script", keep_re=keep_re)

    assert nspace
    assert "simple_sample_plan_1" in nspace, pprint.pformat(nspace)
    assert "simple_sample_plan_2" in nspace, pprint.pformat(nspace)
    assert "plan_in_module_1" in nspace, pprint.pformat(nspace)
    if keep_re:
        assert "RE" in nspace, pprint.pformat(nspace)
        assert "db" in nspace, pprint.pformat(nspace)
    else:
        assert "RE" not in nspace, pprint.pformat(nspace)
        assert "db" not in nspace, pprint.pformat(nspace)

    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    # The rest of the test demonstrates faulty behavior of the Python import system.
    # Reload the same script, but replace the code in the module (emulate the process of code editing).
    #   NOTE: current implementation will not load the new code!!! Application has to be restarted if
    #         to import a module after code is modified.

    # Replace the 'main' module code
    with open(script_path, "w") as f:
        f.write(script_patch + _startup_script_2)

    nspace = load_startup_module("script_dir1.startup_script", keep_re=keep_re)
    # Expect the functions from 'old' code to be in the namespace!!!
    assert "simple_sample_plan_1" in nspace, pprint.pformat(nspace)
    assert "simple_sample_plan_2" in nspace, pprint.pformat(nspace)

    # Replace the code of the module which is imported from the 'main' module.
    with open(module_path, "w") as f:
        f.write(_imported_module_1_modified)

    nspace = load_startup_module("script_dir1.startup_script", keep_re=keep_re)
    # Expect the functions from 'old' code to be in the namespace!!!
    assert "plan_in_module_1" in nspace, pprint.pformat(nspace)
    assert "plan_in_module_1_modified" not in nspace, pprint.pformat(nspace)


def test_load_startup_module_2(tmp_path, monkeypatch, reset_sys_modules):  # noqa: F811
    """
    Import module with more sophisticated imports.
    """
    # Load first script
    script_dir = os.path.join(tmp_path, "script_dir1")
    script_path = os.path.join(script_dir, "startup_script.py")

    os.makedirs(script_dir, exist_ok=True)
    with open(script_path, "w") as f:
        f.write(_startup_script_4)

    # Temporarily add module to the search path
    sys_path = sys.path
    monkeypatch.setattr(sys, "path", [str(tmp_path)] + sys_path)

    nspace = load_startup_module("script_dir1.startup_script", keep_re=True)

    assert nspace
    assert "SimStage" in nspace, pprint.pformat(nspace)
    assert "sim_stage" in nspace, pprint.pformat(nspace)


def test_load_startup_module_3(tmp_path, monkeypatch, reset_sys_modules):  # noqa: F811
    """
    Load startup module: instantiation of devices using Happi.
    """
    _configure_happi(tmp_path, monkeypatch)

    pc_path = os.path.join(tmp_path, "script_dir1")
    script_path = os.path.join(pc_path, "startup_script.py")

    os.makedirs(pc_path, exist_ok=True)
    with open(script_path, "w") as f:
        f.write(_startup_script_happi_1)

    # Temporarily add module to the search path
    sys_path = sys.path
    monkeypatch.setattr(sys, "path", [str(tmp_path)] + sys_path)

    nspace = load_startup_module("script_dir1.startup_script")

    _verify_happi_namespace(nspace)


# fmt: off
@pytest.mark.parametrize("option", ["startup_dir", "script", "module"])
@pytest.mark.parametrize("keep_re", [True, False])
# fmt: on
def test_load_worker_startup_code_1(tmp_path, monkeypatch, keep_re, option, reset_sys_modules):  # noqa: F811
    """
    Test for `load_worker_startup_code` function.
    """
    script_dir = os.path.join(tmp_path, "script_dir1")
    script_path = os.path.join(script_dir, "startup_script.py")

    os.makedirs(script_dir, exist_ok=True)
    with open(script_path, "w") as f:
        f.write(_startup_script_1)

    if option == "startup_dir":
        nspace = load_worker_startup_code(startup_dir=script_dir, keep_re=keep_re)

    elif option == "script":
        nspace = load_worker_startup_code(startup_script_path=script_path, keep_re=keep_re)

    elif option == "module":
        # Temporarily add module to the search path
        sys_path = sys.path
        monkeypatch.setattr(sys, "path", [str(tmp_path)] + sys_path)

        nspace = load_worker_startup_code(startup_module_name="script_dir1.startup_script", keep_re=keep_re)

    else:
        assert False, f"Unknown option '{option}'"

    assert isinstance(nspace, dict), str(type(nspace))
    assert len(nspace) > 0

    if keep_re:
        assert "RE" in nspace, pprint.pformat(nspace)
        assert "db" in nspace, pprint.pformat(nspace)
    else:
        assert "RE" not in nspace, pprint.pformat(nspace)
        assert "db" not in nspace, pprint.pformat(nspace)


@pytest.mark.parametrize("option", ["no_sources", "multiple_sources"])
def test_load_worker_startup_code_2_failing(option, reset_sys_modules):  # noqa: F811
    with pytest.raises(ValueError, match="multiple sources were specified"):
        if option == "no_sources":
            load_worker_startup_code(startup_dir="abc", startup_module_name="script_dir1.startup_script")
        elif option == "multiple_sources":
            load_worker_startup_code()
        else:
            assert False, f"Unknown option '{option}'"


# ---------------------------------------------------------------------------------
#                          Tests for '_process_plan'


def _pf1a(val1, val2):
    """
    Some function description.

    Parameters
    ----------
    val1 : float
        Description of the parameter Value 1.
    val2 : list(str)
        Description of the parameter Value 2.

    Returns
    -------
    v : int
        Description for the return statement
    """
    return int(val1 + int(val2[0]))


def _pf1a1(val1, val2):
    """Some function description.

    Parameters
    ----------
    val1 : float
        Description of the parameter Value 1.
    val2 : list(str)
        Description of the parameter Value 2.

    Returns
    -------
    v : int
        Description for the return statement
    """
    return int(val1 + int(val2[0]))


# Docstring is incorrectly indented
# fmt: off
def _pf1a2(val1, val2):
        """
        Some function description.

        Parameters
        ----------
        val1 : float
            Description of the parameter Value 1.
        val2 : list(str)
            Description of the parameter Value 2.

        Returns
        -------
        v : int
            Description for the return statement
        """  # noqa E117
        return int(val1 + int(val2[0]))
# fmt: on


# This test is 'artificial'. Make sure that the names preceded by '*' are still recognized.
def _pf1a3(val1, val2):
    """
    Some function description.

    Parameters
    ----------
    *val1 : float
        Description of the parameter Value 1.
    **val2 : list(str)
        Description of the parameter Value 2.

    Returns
    -------
    v : int
        Description for the return statement
    """
    return int(val1 + int(val2[0]))


_pf1a_processed = {
    "description": "Some function description.",
    "parameters": [
        {
            "name": "val1",
            "description": "Description of the parameter Value 1.",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        },
        {
            "name": "val2",
            "description": "Description of the parameter Value 2.",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        },
    ],
    "properties": {"is_generator": False},
}


def _pf1b(val1, val2):
    """
    Returns
    -------
    int
        Description for the return statement
    """
    return int(val1 + int(val2[0]))


_pf1b_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        },
    ],
    "properties": {"is_generator": False},
}


def _pf1c(val1, val2):
    """
    Parameters
    ----------
    val1 : float
        Description of the parameter Value 1 - Line 1.
        Description of the parameter Value 1 - Line 2.

        Description of the parameter Value 1 - Line 3.
    val2 : list(str)
        Description of the parameter Value 2 - Line 1.
        Description of the parameter Value 2 - Line 2.

        Description of the parameter Value 2 - Line 3.

    """
    return int(val1 + int(val2[0]))


_pf1c_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "description": "Description of the parameter Value 1 - Line 1.\n"
            "Description of the parameter Value 1 - Line 2.\n\n"
            "Description of the parameter Value 1 - Line 3.",
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "description": "Description of the parameter Value 2 - Line 1.\n"
            "Description of the parameter Value 2 - Line 2.\n\n"
            "Description of the parameter Value 2 - Line 3.",
        },
    ],
    "properties": {"is_generator": False},
}


def _pf1d(val1, val2):
    return int(val1 + int(val2[0]))


# fmt: off
def _pf1d1(val1, val2):
    """
    """
    return int(val1 + int(val2[0]))
# fmt: on


# fmt: off
def _pf1d2(val1, val2):
    """

    """
    return int(val1 + int(val2[0]))
# fmt: on


_pf1d_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        },
    ],
    "properties": {"is_generator": False},
}


# fmt: off
@pytest.mark.parametrize("plan_func, plan_info_expected", [
    (_pf1a, _pf1a_processed),
    (_pf1a1, _pf1a_processed),
    (_pf1a2, _pf1a_processed),
    (_pf1a3, _pf1a_processed),
    (_pf1b, _pf1b_processed),
    (_pf1c, _pf1c_processed),
    (_pf1d, _pf1d_processed),
    (_pf1d1, _pf1d_processed),
    (_pf1d2, _pf1d_processed),
])
# fmt: on
def test_process_plan_1(plan_func, plan_info_expected):
    """
    Function '_process_plan': loading descriptions from a docstring
    """

    plan_info_expected = plan_info_expected.copy()
    plan_info_expected["name"] = plan_func.__name__
    plan_info_expected["module"] = plan_func.__module__
    pf_info = _process_plan(plan_func, existing_devices={}, existing_plans={})

    assert pf_info == plan_info_expected


def _pf2a(val1, val2):
    pass


_pf2a_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        },
    ],
    "properties": {"is_generator": False},
}


def _pf2b(val1, val2):
    yield from [val1, val2]


_pf2b_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        },
    ],
    "properties": {"is_generator": True},
}


def _pf2c(val1=10.5, val2="some_str", val3=None):
    yield from [val1, val2, val3]


_pf2c_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "10.5",
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "'some_str'",
        },
        {
            "name": "val3",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "None",
        },
    ],
    "properties": {"is_generator": True},
}


def _pf2d(val1, *, val2, val3=None):
    yield from [val1, val2, val3]


_pf2d_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        },
        {
            "name": "val2",
            "kind": {"name": "KEYWORD_ONLY", "value": 3},
        },
        {
            "name": "val3",
            "kind": {"name": "KEYWORD_ONLY", "value": 3},
            "default": "None",
        },
    ],
    "properties": {"is_generator": True},
}


def _pf2e(val1, *args, val2=None, **kwargs):
    yield from [val1, *args, val2, *kwargs.values()]


_pf2e_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        },
        {
            "name": "args",
            "kind": {"name": "VAR_POSITIONAL", "value": 2},
        },
        {
            "name": "val2",
            "kind": {"name": "KEYWORD_ONLY", "value": 3},
            "default": "None",
        },
        {
            "name": "kwargs",
            "kind": {"name": "VAR_KEYWORD", "value": 4},
        },
    ],
    "properties": {"is_generator": True},
}


def _pf2f(val1: float = 10.5, val2: str = "some_str", val3: None = None):
    yield from [val1, val2, val3]


_pf2f_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "10.5",
            "annotation": {"type": "float"},
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "'some_str'",
            "annotation": {"type": "str"},
        },
        {
            "name": "val3",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "None",
            "annotation": {"type": "None"},
        },
    ],
    "properties": {"is_generator": True},
}


def _pf2g(
    val1: typing.Tuple[typing.Union[float, int]] = (50,),
    val2: typing.Union[typing.List[str], str] = "some_str",
    val3: typing.Dict[str, int] = {"ab": 10, "cd": 50},
    val4: Dict[str, int] = {"ab": 10, "cd": 50},  # No module name 'typing'
    val5: Optional[float] = None,  # typing.Optional[float] == typing.Union[float, NoneType]
):
    yield from [val1, val2, val3, val4, val5]


_pf2g_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "(50,)",
            "annotation": {"type": "typing.Tuple[typing.Union[float, int]]"},
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "'some_str'",
            "annotation": {"type": "typing.Union[typing.List[str], str]"},
        },
        {
            "name": "val3",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "{'ab': 10, 'cd': 50}",
            "annotation": {"type": "typing.Dict[str, int]"},
        },
        {
            "name": "val4",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "{'ab': 10, 'cd': 50}",
            "annotation": {"type": "typing.Dict[str, int]"},
        },
        {
            "name": "val5",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "None",
            "annotation": {
                "type": "typing.Optional[float]" if python_version >= (3, 9) else "typing.Union[float, NoneType]"
            },
        },
    ],
    "properties": {"is_generator": True},
}


# fmt: off
@pytest.mark.parametrize("plan_func, plan_info_expected", [
    (_pf2a, _pf2a_processed),
    (_pf2b, _pf2b_processed),
    (_pf2c, _pf2c_processed),
    (_pf2d, _pf2d_processed),
    (_pf2e, _pf2e_processed),
    (_pf2f, _pf2f_processed),
    (_pf2g, _pf2g_processed),
])
# fmt: on
def test_process_plan_2(plan_func, plan_info_expected):
    """
    Function '_process_plan': parameter annotations from the signature
    """

    plan_info_expected = plan_info_expected.copy()
    plan_info_expected["name"] = plan_func.__name__
    plan_info_expected["module"] = plan_func.__module__

    pf_info = _process_plan(plan_func, existing_devices={}, existing_plans={})

    assert pf_info == plan_info_expected, pprint.pformat(pf_info)


@parameter_annotation_decorator(
    {
        "description": "This is a sample plan",
        "parameters": {
            "val1": {"description": "Parameter 'val1'"},
            "val2": {"description": "Parameter 'val2'"},
            "val3": {"description": "Parameter 'val3'"},
        },
    }
)
def _pf3a(val1: float = 10.5, val2: str = "some_str", val3: None = None):
    yield from [val1, val2, val3]


_pf3a_processed = {
    "description": "This is a sample plan",
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "10.5",
            "annotation": {"type": "float"},
            "description": "Parameter 'val1'",
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "'some_str'",
            "annotation": {"type": "str"},
            "description": "Parameter 'val2'",
        },
        {
            "name": "val3",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "None",
            "annotation": {"type": "None"},
            "description": "Parameter 'val3'",
        },
    ],
    "properties": {"is_generator": True},
}


@parameter_annotation_decorator(
    {
        "description": "This is a sample plan",
        "parameters": {
            "val1": {"description": "Parameter 'val1'"},
            "val2": {"description": "Parameter 'val2'"},
        },
    }
)
def _pf3b(val1: float = 10.5, val2: str = "some_str", val3: None = None):
    """
    Plan description will be overwritten by the description
    in the decorator.

    Parameters
    ----------
    val1 : float
        Will be overwritten
    val2
        Will be overwritten
    val3 : str
        The description for 'val3' from the docstring
    """
    yield from [val1, val2, val3]


_pf3b_processed = {
    "description": "This is a sample plan",
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "10.5",
            "annotation": {"type": "float"},
            "description": "Parameter 'val1'",
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "'some_str'",
            "annotation": {"type": "str"},
            "description": "Parameter 'val2'",
        },
        {
            "name": "val3",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "None",
            "annotation": {"type": "None"},
            "description": "The description for 'val3' from the docstring",
        },
    ],
    "properties": {"is_generator": True},
}


@parameter_annotation_decorator(
    {
        "parameters": {"val3": {"annotation": "typing.Union[typing.List[str], str]"}},
    }
)
def _pf3c(val1: float = 10.5, val2: str = "some_str", val3: None = None):
    """
    Visible description.

    Parameters
    ----------
    val1 : float
        The description for 'val1' from the docstring
    val2
        The description for 'val2' from the docstring
    val3 : str
        The description for 'val3' from the docstring
    """
    yield from [val1, val2, val3]


_pf3c_processed = {
    "description": "Visible description.",
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "10.5",
            "annotation": {"type": "float"},
            "description": "The description for 'val1' from the docstring",
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "'some_str'",
            "annotation": {"type": "str"},
            "description": "The description for 'val2' from the docstring",
        },
        {
            "name": "val3",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "None",
            "annotation": {"type": "typing.Union[typing.List[str], str]"},
            "description": "The description for 'val3' from the docstring",
        },
    ],
    "properties": {"is_generator": True},
}


@parameter_annotation_decorator(
    {
        "parameters": {
            "val3": {
                "annotation": "typing.List[typing.Union[Devices1, Plans1, Enums1]]",
                "devices": {"Devices1": ("dev_det1", "dev_det2", "dev3")},
                "plans": {"Plans1": ("plan1", "plan2", "plan3")},
                "enums": {"Enums1": ("enum1", "enum2", "enum3")},
            }
        },
    }
)
def _pf3d(val1, val2: str = "some_str", val3: None = None):
    """
    Visible description.

    Parameters
    ----------
    val1 : float
        The description for 'val1' from the docstring
    val2
        The description for 'val2' from the docstring
    val3 : str
        The description for 'val3' from the docstring
    """
    yield from [val1, val2, val3]


_pf3d_processed = {
    "description": "Visible description.",
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "description": "The description for 'val1' from the docstring",
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "'some_str'",
            "annotation": {"type": "str"},
            "description": "The description for 'val2' from the docstring",
        },
        {
            "name": "val3",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "None",
            "annotation": {
                "type": "typing.List[typing.Union[Devices1, Plans1, Enums1]]",
                "devices": {"Devices1": ["dev3", "dev_det1", "dev_det2"]},
                "plans": {"Plans1": ["plan1", "plan2", "plan3"]},
                "enums": {"Enums1": ["enum1", "enum2", "enum3"]},
            },
            "description": "The description for 'val3' from the docstring",
        },
    ],
    "properties": {"is_generator": True},
}


@parameter_annotation_decorator(
    {
        "parameters": {
            "val1": {"default": 1000},
            "val2": {"default": "replacement_str"},
            "val3": {"default": False},
        },
    }
)
def _pf3e(val1=None, val2: str = "some_str", val3: typing.Any = None):
    yield from [val1, val2, val3]


_pf3e_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "1000",
            "default_defined_in_decorator": True,
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {"type": "str"},
            "default": "'replacement_str'",
            "default_defined_in_decorator": True,
        },
        {
            "name": "val3",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "False",
            "annotation": {"type": "typing.Any"},
            "default_defined_in_decorator": True,
        },
    ],
    "properties": {"is_generator": True},
}


class _Pf3f_val1:
    ...


@parameter_annotation_decorator(
    {
        "parameters": {
            "val1": {"default": "device_name"},  # Replace unsupported value
        },
    }
)
def _pf3f(val1=ophyd.Device(name="some_device")):  # Default value has unsupported type
    # This test case would fail because the default value has type that is not supported.
    #   Replacing the default value in the signature by a different value in the decorator
    #   allows to use the plan without change.
    yield from [val1]


_pf3f_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "default": "'device_name'",
            "default_defined_in_decorator": True,
        },
    ],
    "properties": {"is_generator": True},
}


# Check that built-in types are handled correctly when they are used as stand-alone type
#   and when they are overridden.
@parameter_annotation_decorator(
    {
        "parameters": {
            "val1": {
                "annotation": "typing.Union[__PLAN__, __DEVICE__]",
                "devices": {"__DEVICE__": ("det1", "det2")},
            },
            "val2": {
                "annotation": "typing.Union[__PLAN__, __DEVICE__]",
                "plans": {"__PLAN__": ("plan1", "plan2")},
            },
            "val3": {
                "annotation": "typing.Union[__PLAN_OR_DEVICE__, __DEVICE__]",
                "devices": {"__DEVICE__": ("det1", "det2")},
            },
            "val4": {
                "annotation": "typing.Union[__PLAN_OR_DEVICE__, __DEVICE__]",
                "devices": {"__PLAN_OR_DEVICE__": ("det1", "det2")},
            },
        }
    }
)
def _pf3g(val1, val2, val3, val4):
    yield from [val1, val2, val3, val4]


_pf3g_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "typing.Union[__PLAN__, __DEVICE__]",
                "devices": {"__DEVICE__": ["det1", "det2"]},
            },
            "convert_plan_names": True,
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "typing.Union[__PLAN__, __DEVICE__]",
                "plans": {"__PLAN__": ["plan1", "plan2"]},
            },
            "convert_device_names": True,
        },
        {
            "name": "val3",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "typing.Union[__PLAN_OR_DEVICE__, __DEVICE__]",
                "devices": {"__DEVICE__": ["det1", "det2"]},
            },
            "convert_plan_names": True,
            "convert_device_names": True,
        },
        {
            "name": "val4",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "typing.Union[__PLAN_OR_DEVICE__, __DEVICE__]",
                "devices": {"__PLAN_OR_DEVICE__": ["det1", "det2"]},
            },
            "convert_device_names": True,
        },
    ],
    "properties": {"is_generator": True},
}


# Check that built-in types are handled correctly when they are used as stand-alone type
#   and when they are overridden.
@parameter_annotation_decorator(
    {
        "parameters": {
            "val1": {
                "annotation": "__PLAN__",
            },
            "val2": {
                "annotation": "__PLAN__",
                "convert_plan_names": False,
            },
            "val3": {
                "annotation": "__PLAN__",
                "convert_plan_names": True,
            },
            "val4": {
                "annotation": "__DEVICE__",
            },
            "val5": {
                "annotation": "__DEVICE__",
                "convert_device_names": False,
            },
            "val6": {
                "annotation": "__DEVICE__",
                "convert_device_names": True,
            },
            "val7": {
                "annotation": "__PLAN_OR_DEVICE__",
            },
            "val8": {
                "annotation": "__PLAN_OR_DEVICE__",
                "convert_plan_names": False,
                "convert_device_names": False,
            },
            "val9": {
                "annotation": "__PLAN_OR_DEVICE__",
                "convert_plan_names": True,
                "convert_device_names": True,
            },
        }
    }
)
def _pf3h(val1, val2, val3, val4, val5, val6, val7, val8, val9):
    yield from [val1, val2, val3, val4, val5, val6, val7, val8, val9]


_pf3h_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {"type": "__PLAN__"},
            "convert_plan_names": True,
        },
        {
            "name": "val2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {"type": "__PLAN__"},
            "convert_plan_names": False,
        },
        {
            "name": "val3",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {"type": "__PLAN__"},
            "convert_plan_names": True,
        },
        {
            "name": "val4",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {"type": "__DEVICE__"},
            "convert_device_names": True,
        },
        {
            "name": "val5",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {"type": "__DEVICE__"},
            "convert_device_names": False,
        },
        {
            "name": "val6",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {"type": "__DEVICE__"},
            "convert_device_names": True,
        },
        {
            "name": "val7",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {"type": "__PLAN_OR_DEVICE__"},
            "convert_plan_names": True,
            "convert_device_names": True,
        },
        {
            "name": "val8",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {"type": "__PLAN_OR_DEVICE__"},
            "convert_plan_names": False,
            "convert_device_names": False,
        },
        {
            "name": "val9",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {"type": "__PLAN_OR_DEVICE__"},
            "convert_plan_names": True,
            "convert_device_names": True,
        },
    ],
    "properties": {"is_generator": True},
}


@parameter_annotation_decorator(
    {
        "parameters": {"val1": {"min": 0.1, "max": 100, "step": 0.02}},
    }
)
def _pf3i(val1):
    yield from [val1]


_pf3i_processed = {
    "parameters": [
        {
            "name": "val1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "min": "0.1",
            "max": "100",
            "step": "0.02",
        },
    ],
    "properties": {"is_generator": True},
}


_pf3_existing_devices = {
    "dev_det1": {"is_readable": True, "is_movable": False, "is_flyable": False},
    "dev_det2": {"is_readable": True, "is_movable": False, "is_flyable": False},
    "dev_m1": {"is_readable": True, "is_movable": True, "is_flyable": False},
    "dev_fly1": {"is_readable": False, "is_movable": False, "is_flyable": True},
}


# fmt: off
@pytest.mark.parametrize("plan_func, existing_devices, plan_info_expected", [
    (_pf3a, {}, _pf3a_processed),
    (_pf3b, {}, _pf3b_processed),
    (_pf3c, {}, _pf3c_processed),
    (_pf3d, _pf3_existing_devices, _pf3d_processed),
    (_pf3e, {}, _pf3e_processed),
    (_pf3f, {}, _pf3f_processed),
    (_pf3g, {}, _pf3g_processed),
    (_pf3h, {}, _pf3h_processed),
    (_pf3i, {}, _pf3i_processed),
])
# fmt: on
def test_process_plan_3(plan_func, existing_devices, plan_info_expected):
    """
    Function '_process_plan': parameter annotations from the annotation
    """

    plan_info_expected = plan_info_expected.copy()
    plan_info_expected["name"] = plan_func.__name__
    plan_info_expected["module"] = plan_func.__module__

    pf_info = _process_plan(plan_func, existing_devices=existing_devices, existing_plans={})

    assert pf_info == plan_info_expected, pprint.pformat(pf_info)


@parameter_annotation_decorator(
    {
        "parameters": {
            "p1": {
                "annotation": "all_devices",
                "devices": {"all_devices": (":.*",)},
            },
            "p2": {
                "annotation": "all_detectors",
                "devices": {"all_detectors": ("__DETECTOR__:.*",)},
            },
            "p3": {
                "annotation": "all_motors",
                "devices": {"all_motors": ("__MOTOR__:.*",)},
            },
            "p4": {
                "annotation": "all_flyers",
                "devices": {"all_flyers": ("__FLYABLE__:.*",)},
            },
            "p5": {
                "annotation": "all_readable",
                "devices": {"all_readable": ("__READABLE__:.*",)},
            },
        }
    }
)
def _pf4a(p1, p2, p3, p4, p5):
    yield from [p1, p2, p3, p4, p5]


_pf4a_processed = {
    "parameters": [
        {
            "name": "p1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "all_devices",
                "devices": {
                    "all_devices": ["da0_detector", "da0_flyer", "da0_motor", "da0_motor2"],
                },
            },
        },
        {
            "name": "p2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {"type": "all_detectors", "devices": {"all_detectors": ["da0_detector"]}},
        },
        {
            "name": "p3",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {"type": "all_motors", "devices": {"all_motors": ["da0_motor", "da0_motor2"]}},
        },
        {
            "name": "p4",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {"type": "all_flyers", "devices": {"all_flyers": ["da0_flyer"]}},
        },
        {
            "name": "p5",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "all_readable",
                "devices": {"all_readable": ["da0_detector", "da0_motor", "da0_motor2"]},
            },
        },
    ],
    "properties": {"is_generator": True},
}


@parameter_annotation_decorator(
    {
        "parameters": {
            "p1": {
                "annotation": "devices1",
                "devices": {"devices1": ("some_dev", ":-motor$:+motor$:det$")},
            },
            "p2": {
                "annotation": "devices2",
                "devices": {"devices2": ("some_dev", "__MOTOR__:-motor$:+motor$:det$")},
            },
            "p3": {
                "annotation": "devices3",
                "devices": {"devices3": ("some_dev", "__READABLE__:motor$:-motor$:det$")},
            },
            "p4": {
                "annotation": "devices4",
                "devices": {"devices4": ("__MOTOR__:-motor$:motor$:det$", "__READABLE__:motor$:-motor$:det$")},
            },
            "p5": {
                "annotation": "devices5",
                "devices": {"devices5": ("__MOTOR__:-motor$:motor$:det$", "__FLYABLE__:.*")},
            },
        }
    }
)
def _pf4b(p1, p2, p3, p4, p5):
    yield from [p1, p2, p3, p4, p5]


_pf4b_processed = {
    "parameters": [
        {
            "name": "p1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "devices1",
                "devices": {
                    "devices1": [
                        "da0_motor.db0_motor",
                        "da0_motor.db0_motor.dc0_det",
                        "da0_motor.db0_motor.dc1_det",
                        "da0_motor.db0_motor.dc2_det",
                        "some_dev",
                    ],
                },
            },
        },
        {
            "name": "p2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {"type": "devices2", "devices": {"devices2": ["da0_motor.db0_motor", "some_dev"]}},
        },
        {
            "name": "p3",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "devices3",
                "devices": {
                    "devices3": [
                        "da0_motor",
                        "da0_motor.db0_motor.dc0_det",
                        "da0_motor.db0_motor.dc1_det",
                        "da0_motor.db0_motor.dc2_det",
                        "some_dev",
                    ]
                },
            },
        },
        {
            "name": "p4",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "devices4",
                "devices": {
                    "devices4": [
                        "da0_motor",
                        "da0_motor.db0_motor",
                        "da0_motor.db0_motor.dc0_det",
                        "da0_motor.db0_motor.dc1_det",
                        "da0_motor.db0_motor.dc2_det",
                    ]
                },
            },
        },
        {
            "name": "p5",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "devices5",
                "devices": {"devices5": ["da0_flyer", "da0_motor.db0_motor"]},
            },
        },
    ],
    "properties": {"is_generator": True},
}


@parameter_annotation_decorator(
    {
        "parameters": {
            "p1": {
                "annotation": "devices1",
                "devices": {"devices1": ("some_dev", ":?.*motor$")},
            },
            "p2": {
                "annotation": "devices2",
                "devices": {"devices2": ("some_dev", "__FLYABLE__:?.*$")},
            },
            "p3": {
                "annotation": "devices3",
                "devices": {
                    "devices3": (
                        "some_dev",
                        "__READABLE__:?.*db0_motor.*:depth=3",
                    )
                },
            },
            "p4": {
                "annotation": "devices4",
                "devices": {"devices4": ("__MOTOR__:?.*db0_motor.*:depth=3",)},
            },
            "p5": {
                "annotation": "devices5",
                "devices": {"devices5": ("__FLYABLE__:motor$:?.*db.*$",)},
            },
        }
    }
)
def _pf4c(p1, p2, p3, p4, p5):
    yield from [p1, p2, p3, p4, p5]


_pf4c_processed = {
    "parameters": [
        {
            "name": "p1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "devices1",
                "devices": {
                    "devices1": [
                        "da0_motor",
                        "da0_motor.db0_motor",
                        "da0_motor.db0_motor.dc3_motor",
                        "da0_motor.db0_motor.dc3_motor.dd1_motor",
                        "da0_motor.db1_det.dc1_motor",
                        "some_dev",
                    ],
                },
            },
        },
        {
            "name": "p2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "devices2",
                "devices": {"devices2": ["da0_flyer", "da0_motor.db2_flyer", "some_dev"]},
            },
        },
        {
            "name": "p3",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "devices3",
                "devices": {
                    "devices3": [
                        "da0_motor.db0_motor",
                        "da0_motor.db0_motor.dc0_det",
                        "da0_motor.db0_motor.dc1_det",
                        "da0_motor.db0_motor.dc2_det",
                        "da0_motor.db0_motor.dc3_motor",
                        "some_dev",
                    ]
                },
            },
        },
        {
            "name": "p4",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "devices4",
                "devices": {
                    "devices4": [
                        "da0_motor.db0_motor",
                        "da0_motor.db0_motor.dc3_motor",
                    ]
                },
            },
        },
        {
            "name": "p5",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "devices5",
                "devices": {"devices5": ["da0_motor.db2_flyer"]},
            },
        },
    ],
    "properties": {"is_generator": True},
}


@parameter_annotation_decorator(
    {
        "parameters": {
            "p1": {
                "annotation": "plans1",
                "plans": {"plans1": ("some_plan", ":.*$")},
            },
            "p2": {
                "annotation": "plans2",
                "plans": {"plans2": ("some_plan", ":motor$")},
            },
            "p3": {
                "annotation": "plans3",
                "plans": {"plans3": (":count",)},
            },
        }
    }
)
def _pf4d(p1, p2, p3):
    yield from [p1, p2, p3]


_pf4d_processed = {
    "parameters": [
        {
            "name": "p1",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "plans1",
                "plans": {"plans1": ["count", "count2", "count_modified", "plan1", "some_plan"]},
            },
        },
        {
            "name": "p2",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "plans2",
                "plans": {"plans2": ["some_plan"]},
            },
        },
        {
            "name": "p3",
            "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
            "annotation": {
                "type": "plans3",
                "plans": {
                    "plans3": [
                        "count",
                        "count2",
                        "count_modified",
                    ]
                },
            },
        },
    ],
    "properties": {"is_generator": True},
}


# fmt: off
_pp4_allowed_devices_dict_1 = {
    "da0_motor": {
        "is_readable": True, "is_movable": True, "is_flyable": False,
        "components": {
            "db0_motor": {
                "is_readable": True, "is_movable": True, "is_flyable": False,
                "components": {
                    "dc0_det": {"is_readable": True, "is_movable": False, "is_flyable": False},
                    "dc1_det": {"is_readable": True, "is_movable": False, "is_flyable": False},
                    "dc2_det": {"is_readable": True, "is_movable": False, "is_flyable": False},
                    "dc3_motor": {
                        "is_readable": True, "is_movable": True, "is_flyable": False,
                        "components": {
                            "dd0_det": {"is_readable": True, "is_movable": False, "is_flyable": False},
                            "dd1_motor": {"is_readable": True, "is_movable": True, "is_flyable": False},
                        }
                    },
                }
            },
            "db1_det": {
                "is_readable": True, "is_movable": False, "is_flyable": False,
                "components": {
                    "dc0_det": {"is_readable": True, "is_movable": False, "is_flyable": False},
                    "dc1_motor": {"is_readable": True, "is_movable": True, "is_flyable": False},
                }
            },
            "db2_flyer": {
                "is_readable": False, "is_movable": False, "is_flyable": True,
            },
        }
    },
    "da0_motor2": {
        "is_readable": True, "is_movable": True, "is_flyable": False,
    },
    "da0_detector": {
        "is_readable": True, "is_movable": False, "is_flyable": False,
    },
    "da0_flyer": {
        "is_readable": False, "is_movable": False, "is_flyable": True,
    },
}
# fmt: on

_pp4_allowed_plans_set_1 = {"plan1", "count", "count_modified", "count2"}


# fmt: off
@pytest.mark.parametrize("plan_func, existing_devices, plan_info_expected", [
    (_pf4a, _pp4_allowed_devices_dict_1, _pf4a_processed),
    (_pf4b, _pp4_allowed_devices_dict_1, _pf4b_processed),
    (_pf4c, _pp4_allowed_devices_dict_1, _pf4c_processed),
    (_pf4d, _pp4_allowed_devices_dict_1, _pf4d_processed),
])
# fmt: on
def test_process_plan_4(plan_func, existing_devices, plan_info_expected):
    """
    Function '_process_plan': Using regular expressions for selecting plans and
    devices from the lists of existing plans and devices.
    """

    plan_info_expected = plan_info_expected.copy()
    plan_info_expected["name"] = plan_func.__name__
    plan_info_expected["module"] = plan_func.__module__

    pf_info = _process_plan(plan_func, existing_devices=existing_devices, existing_plans=_pp4_allowed_plans_set_1)

    assert pf_info == plan_info_expected, pprint.pformat(pf_info)


def _pf5a_factory():
    """Arbitrary classes are not supported"""

    class SomeClass:
        ...

    def f(val1, *, val2, val3=SomeClass()):
        yield from [val1, val2, val3]

    return f


# Failure to process custom types with dynamically generated enums
@parameter_annotation_decorator(
    {
        "parameters": {
            "val3": {
                "annotation": "typing.List[typing.Union[Devices1, Plans1, Enums1]]",
                "devices": {"Devices1": ("dev1", "dev2", "dev3")},
                # 'plans' is missing, so 'Plans1' is undefined
                "enums": {"Enums1": ("enum1", "enum2", "enum3")},
            }
        },
    }
)
def _pf5b(val1, val2: str = "some_str", val3: None = None):
    yield from [val1, val2, val3]


@parameter_annotation_decorator(
    {
        "parameters": {
            "detector": {
                "annotation": "Detectors1]",
                "devices": {"Detectors1": ("d1", "d2", "d3")},
                "default": "d1",
            },
        },
    }
)
def _pf5c(detector: Optional[ophyd.Device]):
    # Expected to fail: the default value is in the decorator, but not in the header
    yield from [detector]


def _pf5d_factory():
    """Arbitrary classes are not supported"""

    class SomeClass:
        ...

    @parameter_annotation_decorator({"parameters": {"val1": {"default": SomeClass()}}})
    def f(val1):
        yield from [val1]

    return f


def _pf5e_factory():
    """Invalid regular expression in plan type"""

    @parameter_annotation_decorator({"parameters": {"val1": {"annotation": "Plan1", "plans": {"Plan1": [":*"]}}}})
    def f(val1):
        yield from [val1]

    return f


def _pf5f_factory():
    """Invalid regular expression in device type"""

    @parameter_annotation_decorator({"parameters": {"val1": {"annotation": "Dev1", "devices": {"Dev1": [":*"]}}}})
    def f(val1):
        yield from [val1]

    return f


def _pf5g_factory():
    """Exlicitly listed device contains invalid symbols"""

    @parameter_annotation_decorator(
        {"parameters": {"val1": {"annotation": "Dev1", "devices": {"Dev1": ["*dev"]}}}}
    )
    def f(val1):
        yield from [val1]

    return f


def _pf5h_factory():
    """Exlicitly listed plan contains invalid symbols"""

    @parameter_annotation_decorator(
        {"parameters": {"val1": {"annotation": "Plan1", "plans": {"Plan1": ["*plan"]}}}}
    )
    def f(val1):
        yield from [val1]

    return f


# fmt: off
@pytest.mark.parametrize("plan_func, err_msg", [
    (_pf5a_factory(), "unsupported type of default value"),
    (_pf5b, "name 'Plans1' is not defined'"),
    (_pf5c, "Missing default value for the parameter 'detector' in the plan signature"),
    (_pf5d_factory(), "unsupported type of default value in decorator"),
    (_pf5e_factory(), r"Name pattern ':\*' contains invalid regular expression '\*'"),
    (_pf5f_factory(), r"Name pattern ':\*' contains invalid regular expression '\*'"),
    (_pf5g_factory(), r"Name pattern '\*dev' contains invalid characters"),
    (_pf5h_factory(), r"Name pattern '\*plan' contains invalid characters"),
])
# fmt: on
def test_process_plan_5_fail(plan_func, err_msg):
    """
    Failing cases for 'process_plan' function. Some plans are expected to be rejected.
    """
    with pytest.raises(ValueError, match=err_msg):
        _process_plan(plan_func, existing_devices={}, existing_plans={})


# ---------------------------------------------------------------------------------
#                    _find_and_replace_built_in_types()

# fmt: off
@pytest.mark.parametrize("type_str_in, plans, devices, enums, type_str_out, convert_plans, convert_devices", [
    ("some_type", None, None, None, "some_type", False, False),
    ("some_type", {}, {}, {}, "some_type", False, False),
    ("__PLAN__", {}, {}, {}, "str", True, False),
    ("__DEVICE__", {}, {}, {}, "str", False, True),
    ("__PLAN_OR_DEVICE__", {}, {}, {}, "str", True, True),
    ("typing.List[__PLAN__]", {}, {}, {}, "typing.List[str]", True, False),
    ("typing.Union[typing.List[__PLAN__], typing.List[__DEVICE__]]", {}, {}, {},
     "typing.Union[typing.List[str], typing.List[str]]", True, True),
    ("__PLAN__", {"__PLAN__": {}}, {}, {}, "__PLAN__", False, False),
    ("__DEVICE__", {}, {"__DEVICE__": {}}, {}, "__DEVICE__", False, False),
    ("__PLAN_OR_DEVICE__", {}, {}, {"__PLAN_OR_DEVICE__": {}}, "__PLAN_OR_DEVICE__", False, False),
])
# fmt: on
def test_find_and_replace_built_in_types_1(
    type_str_in, plans, devices, enums, type_str_out, convert_plans, convert_devices
):
    """
    ``_find_and_replace_built_in_types``: basic tests
    """
    annotation_type_str, convert_plan_names, convert_device_names = _find_and_replace_built_in_types(
        type_str_in, plans=plans, devices=devices, enums=enums
    )
    assert annotation_type_str == type_str_out
    assert convert_plan_names == convert_plans
    assert convert_device_names == convert_devices


# ---------------------------------------------------------------------------------
#                      _process_custom_annotation()


def _create_schema_for_testing(annotation_type):
    import pydantic

    model_kwargs = {"par": (annotation_type, ...)}
    func_model = pydantic.create_model("func_model", **model_kwargs)
    schema = func_model.schema()
    return schema


# fmt: off
@pytest.mark.parametrize("encoded_annotation, type_expected, built_in_plans, built_in_devices, success, errmsg", [
    ({"type": "int"}, int, False, False, True, ""),
    ({"type": "str"}, str, False, False, True, ""),
    ({"type": "typing.List[int]"}, typing.List[int], False, False, True, ""),
    ({"type": "typing.List[typing.Union[int, float]]"},
     typing.List[typing.Union[int, float]], False, False, True, ""),
    ({"type": "List[int]"}, typing.List[int], False, False, False, "name 'List' is not defined"),

    #  Built-in types: allow any value to pass
    ({"type": "__PLAN__"}, str, True, False, True, ""),
    ({"type": "typing.List[__PLAN__]"}, typing.List[str], True, False, True, ""),
    ({"type": "__DEVICE__"}, str, False, True, True, ""),
    ({"type": "typing.List[__DEVICE__]"}, typing.List[str], False, True, True, ""),
    ({"type": "__PLAN_OR_DEVICE__"}, str, True, True, True, ""),
    ({"type": "typing.List[__PLAN_OR_DEVICE__]"}, typing.List[str], True, True, True, ""),
    ({"type": "typing.Union[typing.List[__PLAN__], __DEVICE__]"},
     typing.Union[typing.List[str], str], True, True, True, ""),

    # Errors
    ({"type": "typing.Union[typing.List[Device1], Device2]", "devices": {"Device1": []}},
     typing.Union[typing.List[str], str], False, False, False, "name 'Device2' is not defined"),
    ({"type": "Enum1", "unknown": {"Enum1": []}}, str, False, False, False,
     r"Annotation contains unsupported keys: \['unknown'\]"),
    ({"type": "str", "devices": {"Device1": []}}, str, False, False, False,
     r"Type 'Device1' is defined in the annotation, but not used"),
    ({"type": "Device1", "devices": {"Device1": None}}, str, False, False, False,
     r"The list of items \('Device1': None\) must be a list of a tuple"),
])
# fmt: on
def test_process_annotation_1(
    encoded_annotation, type_expected, built_in_plans, built_in_devices, success, errmsg
):
    """
    Function ``_process_annotation``: generate type based on annotation and compare it with the expected type.
    Also verify that JSON schema can be created from the class.
    """
    if success:
        # Compare types directly
        type_recovered, conv_plan_nms, conv_dev_nms, ns = _process_annotation(encoded_annotation)
        assert type_recovered == type_expected
        assert conv_plan_nms == built_in_plans
        assert conv_dev_nms == built_in_devices

        # Compare generated JSON schemas
        schema_recovered = _create_schema_for_testing(type_recovered)
        schema_expected = _create_schema_for_testing(type_expected)
        assert schema_recovered == schema_expected
    else:
        with pytest.raises(TypeError, match=errmsg):
            _process_annotation(encoded_annotation)


pa2_Device1 = enum.Enum("pa2_Device1", {"dev1": "dev1", "dev2": "dev2", "dev3": "dev3"})
pa2_Device2 = enum.Enum("pa2_Device2", {"dev4": "dev4", "dev5": "dev5"})
pa2_Plan1 = enum.Enum("pa2_Plan1", {"plan1": "plan1", "plan2": "plan2"})
pa2_Enum1 = enum.Enum("pa2_Enum1", {"enum1": "enum1", "enum2": "enum2"})

pa2__DEVICE__ = enum.Enum("__DEVICE__", {"dev1": "dev1", "dev2": "dev2", "dev3": "dev3"})
pa2__PLAN__ = enum.Enum("__PLAN__", {"plan1": "plan1", "plan2": "plan2"})
pa2__PLAN_OR_DEVICE__ = enum.Enum("__PLAN_OR_DEVICE__", {})


# fmt: off
@pytest.mark.parametrize("encoded_annotation, type_expected, success, errmsg", [
    # Use custom type specifications
    ({"type": "pa2_Device1", "devices": {"pa2_Device1": ("dev1", "dev2", "dev3")}}, pa2_Device1, True, ""),
    ({"type": "typing.List[pa2_Device1]", "devices": {"pa2_Device1": ("dev1", "dev2", "dev3")}},
     typing.List[pa2_Device1], True, ""),
    ({"type": "typing.List[typing.Union[pa2_Device1, pa2_Device2]]", "devices":
        {"pa2_Device1": ("dev1", "dev2", "dev3"),
         "pa2_Device2": ("dev4", "dev5")}}, typing.List[typing.Union[pa2_Device1, pa2_Device2]], True, ""),
    ({"type": "typing.Union[typing.List[pa2_Device1], typing.List[pa2_Plan1]]", "devices":
        {"pa2_Device1": ("dev1", "dev2", "dev3"),
         "pa2_Plan1": ("plan1", "plan2")}},
     typing.Union[typing.List[pa2_Device1], typing.List[pa2_Plan1]], True, ""),
    ({"type": "typing.Union[typing.List[pa2_Device1], typing.List[pa2_Enum1]]", "devices":
        {"pa2_Device1": ("dev1", "dev2", "dev3"),
         "pa2_Enum1": ("enum1", "enum2")}},
     typing.Union[typing.List[pa2_Device1], typing.List[pa2_Enum1]], True, ""),
    # Use Tuple instead of List (produces different JSON schema)
    ({"type": "typing.Union[typing.Tuple[pa2_Device1], typing.List[pa2_Enum1]]", "devices":
        {"pa2_Device1": ("dev1", "dev2", "dev3"),
         "pa2_Enum1": ("enum1", "enum2")}},
     typing.Union[typing.Tuple[pa2_Device1], typing.List[pa2_Enum1]], True, ""),
    # Redefine built-in types.
    ({"type": "typing.Union[__PLAN__, __DEVICE__, __PLAN_OR_DEVICE__]",
      "devices": {"__DEVICE__": ("dev1", "dev2", "dev3"), "__PLAN_OR_DEVICE__": []},
      "plans": {"__PLAN__": ("plan1", "plan2")}},
     typing.Union[pa2__PLAN__, pa2__DEVICE__, pa2__PLAN_OR_DEVICE__], True, ""),
    # Failing case: unknown 'custom' type in the annotation
    ({"type": "typing.Union[typing.List[unknown_type], typing.List[pa2_Enum1]]", "devices":
        {"pa2_Enum1": ("enum1", "enum2")}},
     typing.Union[typing.List[pa2_Device1], typing.List[pa2_Enum1]], False, "name 'unknown_type' is not defined"),
    # Name for custom type is not a valid Python name
    ({"type": "typing.Union[typing.List[unknown-type], typing.List[pa2_Enum1]]", "devices":
        {"pa2_Enum1": ("enum1", "enum2")}},
     typing.Union[typing.List[pa2_Device1], typing.List[pa2_Enum1]], False, "name 'unknown' is not defined"),
    # Non-existing type 'typing.list'
    ({"type": "typing.Union[typing.list[pa2_Device1], typing.List[pa2_Enum1]]", "devices":
        {"pa2_Device1": ("dev1", "dev2", "dev3"),
         "pa2_Enum1": ("enum1", "enum2")}},
     typing.Union[typing.List[pa2_Device1], typing.List[pa2_Enum1]], False,
     "module 'typing' has no attribute 'list'"),
    # Non-existing type 'List'
    ({"type": "typing.Union[List[pa2_Device1], List[pa2_Enum1]]", "devices":
        {"pa2_Device1": ("dev1", "dev2", "dev3"),
         "pa2_Enum1": ("enum1", "enum2")}},
     typing.Union[typing.List[pa2_Device1], typing.List[pa2_Enum1]], False, "name 'List' is not defined'"),
])
# fmt: on
def test_process_annotation_2(encoded_annotation, type_expected, success, errmsg):
    """
    Function ``_process_annotation``: cases where types can not be compared directly (types are based
    on independently created ``enum.Enum`` classes are always not equal), but generated JSON schemas
    can be compared. The types are used to create Pydantic model classes and JSON schemas, so  this is
    a meaningful test.
    """
    if success:
        type_recovered, _, _, ns = _process_annotation(encoded_annotation)

        schema_recovered = _create_schema_for_testing(type_recovered)
        schema_expected = _create_schema_for_testing(type_expected)
        assert schema_recovered == schema_expected
    else:
        with pytest.raises(TypeError, match=errmsg):
            _process_annotation(encoded_annotation)


# fmt: off
@pytest.mark.parametrize("encoded_annotation, type_expected, success, errmsg", [
    # Missing 'dev3'
    ({"type": "typing.Union[typing.List[pa2_Device1], typing.List[pa2_Enum1]]", "devices":
        {"pa2_Device1": ("dev1", "dev2"),
         "pa2_Enum1": ("enum1", "enum2")}},
     typing.Union[typing.List[pa2_Device1], typing.List[pa2_Enum1]], True, ""),
    # Extra 'enum3'
    ({"type": "typing.Union[typing.List[pa2_Device1], typing.List[pa2_Enum1]]", "devices":
        {"pa2_Device1": ("dev1", "dev2", "dev3"),
         "pa2_Enum1": ("enum1", "enum2", "enum3")}},
     typing.Union[typing.List[pa2_Device1], typing.List[pa2_Enum1]], True, ""),
    # Changed device name 'dev2x'
    ({"type": "typing.Union[typing.List[pa2_Device1], typing.List[pa2_Enum1]]", "devices":
        {"pa2_Device1": ("dev1", "dev2x", "dev3"),
         "pa2_Enum1": ("enum1", "enum2")}},
     typing.Union[typing.List[pa2_Device1], typing.List[pa2_Enum1]], True, ""),
])
# fmt: on
def test_process_annotation_3(encoded_annotation, type_expected, success, errmsg):
    """
    Function ``_process_annotation``: make sure that different JSON schemas are generated if
    type definitions are different.
    """
    if success:
        type_recovered, _, _, ns = _process_annotation(encoded_annotation)

        schema_recovered = _create_schema_for_testing(type_recovered)
        schema_expected = _create_schema_for_testing(type_expected)
        assert schema_recovered != schema_expected  # NOT EQUAL !!!
    else:
        with pytest.raises(TypeError, match=errmsg):
            _process_annotation(encoded_annotation)


# -----------------------------------------------------------------------------------------------------
#                                 _process_default_value

# fmt: off
@pytest.mark.parametrize("default_encoded, default_expected, success, errmsg", [
    ("10", 10, True, ""),
    ("10.453", 10.453, True, ""),
    ("'some-str'", "some-str", True, ""),
    ("int(5.6)", 5, False, r"Failed to decode the default value 'int\(5.6\)'"),
    ("some-str", "some-str", False, "Failed to decode the default value 'some-str'"),

])
# fmt: on
def test_process_default_value_1(default_encoded, default_expected, success, errmsg):

    if success:
        default = _process_default_value(default_encoded)
        assert default == default_expected
    else:
        with pytest.raises(Exception, match=errmsg):
            _process_default_value(default_encoded)


# -----------------------------------------------------------------------------------------------------
#                          _decode_parameter_types_and_defaults

_ipt1 = [
    {"name": "param1", "annotation": {"type": "int"}, "default": "10"},
    {"name": "param2", "annotation": {"type": "str"}, "default": "'some-string'"},
    {"name": "param3", "annotation": {"type": "typing.Union[typing.List[int], int, None]"}, "default": "50"},
]

_ipt1_result = {
    "param1": {"type": int, "default": 10},
    "param2": {"type": str, "default": "some-string"},
    "param3": {"type": typing.Union[typing.List[int], int, None], "default": 50},
}

_ipt2 = [
    {
        "name": "param1",
        "annotation": {"type": "_ipt2_Detectors1", "devices": {"_ipt2_Detectors1": ("det1", "det2")}},
        "default": "'det1'",
    },
    {
        "name": "param2",
        "annotation": {"type": "typing.List[_ipt2_Detectors1]", "devices": {"_ipt2_Detectors1": ("det1", "det2")}},
        "default": "'det2'",
    },
]

_ipt2_Detectors1 = enum.Enum("_ipt2_Detectors1", {"det1": "det1", "det2": "det2"})

_ipt2_result = {
    "param1": {"type": _ipt2_Detectors1, "default": "det1"},
    "param2": {"type": typing.List[_ipt2_Detectors1], "default": "det2"},
}

_ipt3 = [
    {"name": "param1", "annotation": {"type": "int"}},
    {"name": "param2", "default": "'some-string'"},
    {"name": "param3"},
]

_ipt3_result = {
    "param1": {"type": int, "default": inspect.Parameter.empty},
    "param2": {"type": typing.Any, "default": "some-string"},
    "param3": {"type": typing.Any, "default": inspect.Parameter.empty},
}


_ipt4_fail = [  # 'name' is missing
    {},
]


_ipt5_fail = [  # Failed to decode the default value
    {"name": "param1", "annotation": {"type": "int"}, "default": "det"},
]


_ipt6_fail = [  # Failed to decode the type (just one simple case)
    {"name": "param1", "annotation": {"type": "some-type"}, "default": "det"},
]


# fmt: off
@pytest.mark.parametrize("parameters, expected_types, compare_types, success, errmsg", [
    (_ipt1, _ipt1_result, True, True, ""),
    (_ipt2, _ipt2_result, False, True, ""),
    (_ipt3, _ipt3_result, True, True, ""),
    (_ipt4_fail, None, True, False, "No 'name' key in the parameter description"),
    (_ipt5_fail, None, True, False, "Failed to decode the default value 'det'"),
    (_ipt6_fail, None, True, False, "Failed to process annotation 'some-type'"),
])
# fmt: on
def test_decode_parameter_types_and_defaults_1(parameters, expected_types, compare_types, success, errmsg):
    """
    Basic test for ``_decode_parameter_types_and_defaults``
    """

    if success:
        inst_types = _decode_parameter_types_and_defaults(parameters)
        if compare_types:
            assert inst_types == expected_types

        # Compare types using JSON schema
        for p in parameters:
            name = p["name"]
            schema_created = _create_schema_for_testing(inst_types[name]["type"])
            schema_expected = _create_schema_for_testing(expected_types[name]["type"])
            assert schema_created == schema_expected

        # Compare default values (important if 'compare_types == False')
        for p in parameters:
            name = p["name"]
            assert inst_types[name]["default"] == expected_types[name]["default"]

    else:
        with pytest.raises(Exception, match=errmsg):
            _decode_parameter_types_and_defaults(parameters)


# ---------------------------------------------------------------------------------
#                                construct_parameters

_cparam1 = [
    {
        "name": "val1",
        "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        "description": "The description for 'val1' from the docstring",
    },
    {
        "name": "val2",
        "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        "default": "'some_str'",
        "annotation": {"type": "str"},
        "description": "The description for 'val2' from the docstring",
    },
    {
        "name": "val3",
        "kind": {"name": "POSITIONAL_OR_KEYWORD", "value": 1},
        "default": "'dev1'",
        "annotation": {
            "type": "typing.List[typing.Union[Devices1, Plans1, Enums1]]",
            "devices": {"Devices1": ("dev1", "dev2", "dev3")},
            "plans": {"Plans1": ("plan1", "plan2", "plan3")},
            "enums": {"Enums1": ("enum1", "enum2", "enum3")},
        },
        "description": "The description for 'val3' from the docstring",
    },
]


# fmt: off
@pytest.mark.parametrize("testmode, success, errmsg", [
    ("external_decode", True, ""),
    ("internal_decode", True, ""),
    ("name_missing", False, "Description for parameter contains no key 'name'"),
    ("kind_missing", False, "Description for parameter contains no key 'kind'"),
])
# fmt: on
def test_construct_parameters_1(testmode, success, errmsg):
    """
    Smoke test for ``construct_parameters``. Tests that the function runs, but no detailed
    validation of results.
    """
    param_list = _cparam1
    if testmode == "external_decode":
        # Decode types using separate call to the function
        param_inst = _decode_parameter_types_and_defaults(param_list)
        parameters = construct_parameters(param_list, params_decoded=param_inst)
    elif testmode == "internal_decode":
        # Decode types internally
        parameters = construct_parameters(param_list)
    elif testmode == "name_missing":
        # Remove 'name' key
        param_inst = _decode_parameter_types_and_defaults(param_list)
        param_list2 = copy.deepcopy(param_list)
        del param_list2[0]["name"]
        with pytest.raises(ValueError, match=errmsg):
            construct_parameters(param_list2, params_decoded=param_inst)
    elif testmode == "kind_missing":
        # Remove 'kind' key
        param_inst = _decode_parameter_types_and_defaults(param_list)
        param_list2 = copy.deepcopy(param_list)
        del param_list2[0]["kind"]
        with pytest.raises(ValueError, match=errmsg):
            construct_parameters(param_list2, params_decoded=param_inst)
    else:
        assert False, f"Unsupported mode {testmode}"

    if success:
        sig_param_names = [_.name for _ in parameters]
        expected_names = [_["name"] for _ in param_list]
        assert sig_param_names == expected_names


# ---------------------------------------------------------------------------------


def test_plans_from_nspace():
    """
    Function 'plans_from_nspace' is extracting a subset of callable items from the namespace
    """
    pc_path = get_default_startup_dir()
    nspace = load_profile_collection(pc_path)
    plans = plans_from_nspace(nspace)
    for name, plan in plans.items():
        assert callable(plan), f"Plan '{name}' is not callable"


def test_devices_from_nspace():
    """
    Function 'plans_from_nspace' is extracting a subset of callable items from the namespace
    """
    pc_path = get_default_startup_dir()
    nspace = load_profile_collection(pc_path)
    devices = devices_from_nspace(nspace)
    for name, device in devices.items():
        assert isinstance(
            device, (protocols.Readable, protocols.Flyable)
        ), f"The object '{device}' is not a device"

    # Check that both devices and signals are recognized by the function
    device_names = ("custom_test_device", "custom_test_signal", "custom_test_flyer", "sim_bundle_A")
    for d in device_names:
        assert d in devices
    # Device classes should not be included
    class_names = ("Device", "SimStage", "SimDetectors", "SimBundle")
    for c in class_names:
        assert c not in devices


# fmt: off
@pytest.mark.parametrize("element_def, components, uses_re, device_type", [
    # Device/subdevice names
    ("det1", [("det1", True, False, None)], False, ""),
    ("det1 ", [("det1", True, False, None)], False, ""),  # Spaces are removed
    ("det1.val", [("det1", True, False, None), ("val", True, False, None)], False, ""),
    ("sim_stage.det1.val", [("sim_stage", True, False, None), ("det1", True, False, None),
     ("val", True, False, None)], False, ""),
    # Regular expressions
    (":^det", [("^det", True, False, None)], True, ""),
    (":^det:^val$", [("^det", True, False, None), ("^val$", True, False, None)], True, ""),
    (":+^det:+^val$", [("^det", True, False, None), ("^val$", True, False, None)], True, ""),
    (":-^det:^val$", [("^det", False, False, None), ("^val$", True, False, None)], True, ""),
    (":-^det:-^val$", [("^det", False, False, None), ("^val$", True, False, None)], True, ""),
    (":sim_stage:-^det:^val$", [("sim_stage", True, False, None), ("^det", False, False, None),
     ("^val$", True, False, None)], True, ""),
    ("__READABLE__:.*", [(".*", True, False, None)], True, "__READABLE__"),
    ("__FLYABLE__:.*", [(".*", True, False, None)], True, "__FLYABLE__"),
    ("__DETECTOR__:-.*:.*", [(".*", False, False, None), (".*", True, False, None)], True, "__DETECTOR__"),
    ("__MOTOR__:.*:.*", [(".*", True, False, None), (".*", True, False, None)], True, "__MOTOR__"),
    # Full-name regular expressions
    (":?det1", [("det1", True, True, None)], True, ""),
    (":?^det1$:depth=5", [("^det1$", True, True, 5)], True, ""),
    (r":?.*\.^val$", [(r".*\.^val$", True, True, None)], True, ""),
    (r"__READABLE__:?.*\.^val$", [(r".*\.^val$", True, True, None)], True, "__READABLE__"),
    (":-^det:?^val$", [("^det", False, False, None), ("^val$", True, True, None)], True, ""),
    (":^det:?^val$:depth=1", [("^det", True, False, None), ("^val$", True, True, 1)], True, ""),
])
# fmt: on
def test_split_name_pattern_1(element_def, components, uses_re, device_type):
    """
    ``_split_list_element_definition``: basic tests
    """
    _components, _uses_re, _device_type = _split_name_pattern(element_def)
    assert _components == components
    assert _uses_re == uses_re
    assert _device_type == device_type


# fmt: off
@pytest.mark.parametrize("element_def, exception_type, msg", [
    (10, TypeError, "Name pattern 10 has incorrect type"),
    ("", ValueError, "Name pattern '' is an empty string"),
    (":", ValueError, "Name pattern ':' contains empty components"),
    (":^det:", ValueError, "Name pattern ':^det:' contains empty components"),
    (":^det::val", ValueError, "Name pattern ':^det::val' contains empty components"),
    (":*det:val", ValueError, "':*det:val' contains invalid regular expression '*det'"),
    ("__UNSUPPORTED_TYPE__:^det", ValueError, "Device type '__UNSUPPORTED_TYPE__' is not supported."),
    (":?^det:depth=0", ValueError, "Depth (0) must be positive integer greater or equal to 1"),
    (":?^det:depth=a", ValueError, "Depth specification 'depth=a' has incorrect format"),
    (":?^det:^val$", ValueError, "'?^det' can be only followed by the depth specification"),
    (":?^det:?^val$", ValueError, "'?^det' can be only followed by the depth specification"),
    (":?^det:^val:^val$", ValueError, "'?^det' must be the last"),
    ("det..val", ValueError, "Plan, device or subdevice name in the description 'det..val' is an empty string"),
    ("det.", ValueError, "Name pattern 'det.' contains invalid characters"),
    (".det", ValueError, "Name pattern '.det' contains invalid characters"),
    ("d$et", ValueError, "Name pattern 'd$et' contains invalid characters"),
    ("d$et.val", ValueError, "Name pattern 'd$et.val' contains invalid characters"),
    ("det.v$al", ValueError, "Name pattern 'det.v$al' contains invalid characters"),
])
# fmt: on
def test_split_name_pattern_2_fail(element_def, exception_type, msg):
    """
    ``_split_list_element_definition``: failing cases
    """
    with pytest.raises(exception_type, match=re.escape(msg)):
        _split_name_pattern(element_def)


# fmt: off
_allowed_devices_dict_1 = {
    "da0_motor": {
        "is_readable": True, "is_movable": True, "is_flyable": False,
        "components": {
            "db0_motor": {
                "is_readable": True, "is_movable": True, "is_flyable": False,
                "components": {
                    "dc0_det": {"is_readable": True, "is_movable": False, "is_flyable": False},
                    "dc1_det": {"is_readable": True, "is_movable": False, "is_flyable": False},
                    "dc2_det": {"is_readable": True, "is_movable": False, "is_flyable": False},
                    "dc3_motor": {
                        "excluded": False,
                        "is_readable": True, "is_movable": True, "is_flyable": False,
                        "components": {
                            "dd0_det": {"is_readable": True, "is_movable": False, "is_flyable": False},
                            "dd1_motor": {"is_readable": True, "is_movable": True, "is_flyable": False},
                        }
                    },
                    "dc4_motor": {
                        "excluded": True, "is_readable": True, "is_movable": True, "is_flyable": False,
                    }
                }
            },
            "db1_det": {
                "is_readable": True, "is_movable": False, "is_flyable": False,
                "components": {
                    "dc0_det": {"is_readable": True, "is_movable": False, "is_flyable": False},
                    "dc1_motor": {"is_readable": True, "is_movable": True, "is_flyable": False},
                }
            },
            "db2_flyer": {
                "is_readable": False, "is_movable": False, "is_flyable": True,
            },
        }
    },
    "da1_det": {
        "excluded": False,
        "is_readable": True, "is_movable": False, "is_flyable": False,
        "components": {
            "db0_det": {"is_readable": True, "is_movable": False, "is_flyable": False},
            "db1_motor": {"is_readable": True, "is_movable": True, "is_flyable": False},
        }
    },
    "da2_det": {
        "excluded": True,
        "is_readable": True, "is_movable": False, "is_flyable": False,
    }
}
# fmt: on


# fmt: off
@pytest.mark.parametrize("device_name, in_list, success, error_type, msg", [
    ("da0_motor", True, True, None, ""),
    ("not_exist", False, True, None, ""),
    ("da0_motor.db0_motor", True, True, None, ""),
    ("da0_motor.db0_motor.dc3_motor.dd1_motor", True, True, None, ""),
    ("da0_motor.not_exist.dc3_motor.dd1_motor", False, True, None, ""),
    ("da0_motor.db0_motor.not_exist.dd1_motor", False, True, None, ""),
    ("da0_motor.db0_motor.dc3_motor.not_exist", False, True, None, ""),
    ("da2_det", False, True, None, ""),  # excluded
    ("da0_motor.db0_motor.dc4_motor", False, True, None, ""),  # excluded
    (":da0_motor", True, False, ValueError,
     "Device name ':da0_motor' can not contain regular expressions"),
])
# fmt: on
def test_is_object_name_in_list_1(device_name, in_list, success, error_type, msg):
    """
    ``_is_object_name_in_list``: basic test (test on devices, but expected to work on plans)
    """
    if success:
        res = _is_object_name_in_list(device_name, allowed_objects=_allowed_devices_dict_1)
        assert res == in_list
    else:
        with pytest.raises(error_type, match=msg):
            _is_object_name_in_list(device_name, allowed_objects=_allowed_devices_dict_1)


# fmt: off
@pytest.mark.parametrize("name_pattern, expected_name_list", [
    # Device names
    ("da0_motor", ["da0_motor"]),
    ("da0_motor.db0_motor", ["da0_motor.db0_motor"]),
    ("da0_motor.db0_motor.dc2_det", ["da0_motor.db0_motor.dc2_det"]),
    # Patterns
    (":.+", ["da0_motor", "da1_det"]),
    (":.*", ["da0_motor", "da1_det"]),
    (":+.*", ["da0_motor", "da1_det"]),
    (":-.*", ["da0_motor", "da1_det"]),  # "-" is ignored
    (":-.+:^db0", ["da0_motor.db0_motor", "da1_det.db0_det"]),
    (":.+:^db0", ["da0_motor", "da0_motor.db0_motor", "da1_det", "da1_det.db0_det"]),
    (":+.+:^db0", ["da0_motor", "da0_motor.db0_motor", "da1_det", "da1_det.db0_det"]),  # "+" is not needed
    (":det$:^db0", ["da1_det", "da1_det.db0_det"]),
    (":-.+:^db0:^dc", ["da0_motor.db0_motor", "da0_motor.db0_motor.dc0_det", "da0_motor.db0_motor.dc1_det",
     "da0_motor.db0_motor.dc2_det", "da0_motor.db0_motor.dc3_motor", "da1_det.db0_det"]),
    ("__MOTOR__:-.+:^db0:^dc", ["da0_motor.db0_motor", "da0_motor.db0_motor.dc3_motor"]),
    ("__DETECTOR__:-.+:^db0:^dc", ["da0_motor.db0_motor.dc0_det", "da0_motor.db0_motor.dc1_det",
     "da0_motor.db0_motor.dc2_det", "da1_det.db0_det"]),
    ("__READABLE__:-.+:^(db0)|(db2):^dc", [
        "da0_motor.db0_motor", "da0_motor.db0_motor.dc0_det", "da0_motor.db0_motor.dc1_det",
        "da0_motor.db0_motor.dc2_det", "da0_motor.db0_motor.dc3_motor", "da1_det.db0_det"]),
    ("__FLYABLE__:-.+:^(db0)|(db2):^dc", ["da0_motor.db2_flyer"]),
    ("__FLYABLE__:-.+:^db0:^dc", []),
    # Full-name patterns
    (":?motor$", ["da0_motor", "da0_motor.db0_motor", "da0_motor.db0_motor.dc3_motor",
     "da0_motor.db0_motor.dc3_motor.dd1_motor", "da0_motor.db1_det.dc1_motor", "da1_det.db1_motor"]),
    (":?motor$:depth=1", ["da0_motor"]),
    (":?motor$:depth=2", ["da0_motor", "da0_motor.db0_motor", "da1_det.db1_motor"]),
    (":^da:?motor$:depth=1", ["da0_motor", "da0_motor.db0_motor", "da1_det", "da1_det.db1_motor"]),
    (":-^da:?motor$:depth=2", ["da0_motor.db0_motor", "da0_motor.db0_motor.dc3_motor",
     "da0_motor.db1_det.dc1_motor", "da1_det.db1_motor"]),
    (":-^da:?^da:depth=2", []),
    ("__MOTOR__:^da:?motor$:depth=1", ["da0_motor", "da0_motor.db0_motor", "da1_det.db1_motor"]),
    ("__READABLE__:?.*db0_motor.*:depth=3", [
        "da0_motor.db0_motor", "da0_motor.db0_motor.dc0_det", "da0_motor.db0_motor.dc1_det",
        "da0_motor.db0_motor.dc2_det", "da0_motor.db0_motor.dc3_motor"]),
    ("__MOTOR__:?.*db0_motor.*:depth=3", ["da0_motor.db0_motor", "da0_motor.db0_motor.dc3_motor"]),
])
# fmt: on
def test_build_device_name_list_1(name_pattern, expected_name_list):
    """
    ``_build_device_name_list``: basic tests
    """
    components, uses_re, device_type = _split_name_pattern(name_pattern)
    name_list = _build_device_name_list(
        components=components, uses_re=uses_re, device_type=device_type, existing_devices=_allowed_devices_dict_1
    )
    assert name_list == expected_name_list, pprint.pformat(name_list)


def test_build_device_name_list_2_fail():
    """
    ``_build_device_name_list``: failing cases
    """
    components, uses_re, device_type = _split_name_pattern("def")
    with pytest.raises(ValueError, match="Unsupported device type: 'unknown'"):
        _build_device_name_list(
            components=components, uses_re=uses_re, device_type="unknown", existing_devices=_allowed_devices_dict_1
        )


# fmt: off
@pytest.mark.parametrize("allow_patterns, disallow_patterns, expected_name_list", [
    # Allow all
    ([None], [None], ['da0_motor', 'da0_motor.db0_motor', 'da0_motor.db0_motor.dc0_det',
                      'da0_motor.db0_motor.dc1_det', 'da0_motor.db0_motor.dc2_det',
                      'da0_motor.db0_motor.dc3_motor', 'da0_motor.db0_motor.dc3_motor.dd0_det',
                      'da0_motor.db0_motor.dc3_motor.dd1_motor', 'da0_motor.db1_det',
                      'da0_motor.db1_det.dc0_det', 'da0_motor.db1_det.dc1_motor',
                      'da0_motor.db2_flyer', 'da1_det', 'da1_det.db0_det', 'da1_det.db1_motor']),
    ([":?.*"], [None], ['da0_motor', 'da0_motor.db0_motor', 'da0_motor.db0_motor.dc0_det',
                        'da0_motor.db0_motor.dc1_det', 'da0_motor.db0_motor.dc2_det',
                        'da0_motor.db0_motor.dc3_motor', 'da0_motor.db0_motor.dc3_motor.dd0_det',
                        'da0_motor.db0_motor.dc3_motor.dd1_motor', 'da0_motor.db1_det',
                        'da0_motor.db1_det.dc0_det', 'da0_motor.db1_det.dc1_motor',
                        'da0_motor.db2_flyer', 'da1_det', 'da1_det.db0_det', 'da1_det.db1_motor']),
    ([":?.*"], [], ['da0_motor', 'da0_motor.db0_motor', 'da0_motor.db0_motor.dc0_det',
                    'da0_motor.db0_motor.dc1_det', 'da0_motor.db0_motor.dc2_det',
                    'da0_motor.db0_motor.dc3_motor', 'da0_motor.db0_motor.dc3_motor.dd0_det',
                    'da0_motor.db0_motor.dc3_motor.dd1_motor', 'da0_motor.db1_det',
                    'da0_motor.db1_det.dc0_det', 'da0_motor.db1_det.dc1_motor',
                    'da0_motor.db2_flyer', 'da1_det', 'da1_det.db0_det', 'da1_det.db1_motor']),
    # Disallow all
    ([None], [":?.*"], []),
    ([], [None], []),
    ([], [], []),
    # Test different combinations
    ([":^da1", ":-^da0:^db1"], [None], ['da0_motor.db1_det', 'da1_det']),
    ([":?.*"], [":^da1", ":-^da0:^db1"],
     ['da0_motor', 'da0_motor.db0_motor', 'da0_motor.db0_motor.dc0_det',
      'da0_motor.db0_motor.dc1_det', 'da0_motor.db0_motor.dc2_det',
      'da0_motor.db0_motor.dc3_motor', 'da0_motor.db0_motor.dc3_motor.dd0_det',
      'da0_motor.db0_motor.dc3_motor.dd1_motor',
      'da0_motor.db1_det.dc0_det', 'da0_motor.db1_det.dc1_motor',
      'da0_motor.db2_flyer', 'da1_det.db0_det', 'da1_det.db1_motor']),
    ([":-^da1:.*", ":^da0:-^db1:.*"], [None],
     ['da0_motor', 'da0_motor.db1_det.dc0_det', 'da0_motor.db1_det.dc1_motor',
      'da1_det.db0_det', 'da1_det.db1_motor']),
    ([":-^da1:.*", ":^da0:-^db1:.*"], [":-^da0:-^db1:det$"],
     ['da0_motor', 'da0_motor.db1_det.dc1_motor', 'da1_det.db0_det', 'da1_det.db1_motor']),
    ([":-^da1:.*", ":^da0:-^db1:.*"], [":^da0:-^db1:det$"],
     ['da0_motor.db1_det.dc1_motor', 'da1_det.db0_det', 'da1_det.db1_motor']),
    ([":-^da1:?motor$", ":-^da0:-^db0:?motor$"], [None],
     ['da0_motor.db0_motor.dc3_motor', 'da0_motor.db0_motor.dc3_motor.dd1_motor', 'da1_det.db1_motor']),
    ([":-^da1:?motor$", ":-^da0:-^db0:?motor$"], [":^da0:?dd1"],
     ['da0_motor.db0_motor.dc3_motor', 'da1_det.db1_motor']),
    ([":-^da1:?motor$", ":-^da0:-^db0:?motor$"], [":^da0:?dd1:depth=2"],
     ['da0_motor.db0_motor.dc3_motor', 'da0_motor.db0_motor.dc3_motor.dd1_motor', 'da1_det.db1_motor']),
    ([":-^da1:?motor$", ":-^da0:-^db0:?motor$"], [":^da0:?dd1:depth=3"],
     ['da0_motor.db0_motor.dc3_motor', 'da1_det.db1_motor']),
    ([":-^da1:?motor$", ":-^da0:-^db0:?motor$"], [":^da0:?dd1:depth=4"],
     ['da0_motor.db0_motor.dc3_motor', 'da1_det.db1_motor']),
    ([":-^da1:?motor$", ":-^da0:?motor$:depth=2"], [None],
     ['da0_motor.db0_motor', 'da0_motor.db0_motor.dc3_motor',
      'da0_motor.db1_det.dc1_motor', 'da1_det.db1_motor'])
])
# fmt: on
def test_filter_device_name_list_1(allow_patterns, disallow_patterns, expected_name_list):
    """
    ``_filter_device_name_list``: basic tests
    """

    allowed_devices = _filter_device_tree(
        item_dict=_allowed_devices_dict_1, allow_patterns=allow_patterns, disallow_patterns=disallow_patterns
    )

    components, uses_re, device_type = _split_name_pattern(":?.*")
    name_list = _build_device_name_list(
        components=components, uses_re=uses_re, device_type=device_type, existing_devices=allowed_devices
    )

    assert name_list == expected_name_list, pprint.pformat(name_list)


_allowed_plans_set_1 = {"count", "count_modified", "mycount", "other_plan"}


# fmt: off
@pytest.mark.parametrize("plan_def, expected_name_list", [
    ("count", ["count"]),  # Plan in the list
    ("some_plan", ["some_plan"]),  # Plan is not in the list
    (":count", ["count", "count_modified", "mycount"]),
    (":^count$", ["count"]),
    (":^count", ["count", "count_modified"]),
    (":count$", ["count", "mycount"]),
    (":+count$", ["count", "mycount"]),
    (":-count$", ["count", "mycount"]),
    (":?count$", ["count", "mycount"]),
])
# fmt: on
def test_build_plan_name_list_1(plan_def, expected_name_list):
    """
    ``_build_plan_name_list``: basic tests
    """
    components, uses_re, device_type = _split_name_pattern(plan_def)
    name_list = _build_plan_name_list(
        components=components, uses_re=uses_re, device_type=device_type, existing_plans=_allowed_plans_set_1
    )
    assert name_list.sort() == expected_name_list.sort(), pprint.pformat(name_list)


# fmt: off
@pytest.mark.parametrize("plan_def, exception_type, msg", [
    ("abc.def", ValueError, "may contain only one component. Components: ['abc', 'def']"),
    (":?abc:depth=5", ValueError, "Depth specification can not be part of the name pattern for a plan"),
    ("__READABLE__:abc", ValueError,
     "Device type can not be included in the name pattern for a plan: '__READABLE__:'"),
])
# fmt: on
def test_build_plan_name_list_2_fail(plan_def, exception_type, msg):
    """
    ``_build_plan_name_list``: failing cases
    """
    components, uses_re, device_type = _split_name_pattern(plan_def)

    with pytest.raises(exception_type, match=re.escape(msg)):
        _build_plan_name_list(
            components=components, uses_re=uses_re, device_type=device_type, existing_plans=_allowed_plans_set_1
        )


# fmt: off
@pytest.mark.parametrize("object_name, exists_in_plans, exists_in_devices, exists_in_all", [
    ("count", True, False, True),
    ("unknown", False, False, False),
    ("det", False, True, True),
    ("det.val", False, True, True),
    ("sim_bundle_A.mtrs.z", False, True, True),
    ("sim_bundle_A.mtrs.a", False, False, False),
    ("sim_bundle_A.unknown.z", False, False, False),
])
# fmt: on
def test_get_nspace_object_1(object_name, exists_in_plans, exists_in_devices, exists_in_all):
    pc_path = get_default_startup_dir()
    nspace = load_profile_collection(pc_path)
    plans = plans_from_nspace(nspace)
    devices = devices_from_nspace(nspace)

    all_objects = plans.copy()
    all_objects.update(devices)

    object_ref = _get_nspace_object(object_name, objects_in_nspace=all_objects)
    if exists_in_all:
        assert not isinstance(object_ref, str)
    else:
        assert isinstance(object_ref, str)

    object_ref = _get_nspace_object(object_name, objects_in_nspace=plans)
    if exists_in_plans:
        assert not isinstance(object_ref, str)
    else:
        assert isinstance(object_ref, str)

    object_ref = _get_nspace_object(object_name, objects_in_nspace=devices)
    if exists_in_devices:
        assert not isinstance(object_ref, str)
    else:
        assert isinstance(object_ref, str)


# fmt: off
@pytest.mark.parametrize(
    "plan, exp_args, exp_kwargs, success, err_msg",
    [
        ({"name": "count", "user_group": _user_group, "args": [["det1", "det2"]]},
         [[ophyd.sim.det1, ophyd.sim.det2]], {}, True, ""),
        ({"name": "scan", "user_group": _user_group, "args": [["det1", "det2"], "motor", -1, 1, 10]},
         [[ophyd.sim.det1, ophyd.sim.det2], ophyd.sim.motor, -1, 1, 10], {}, True, ""),
        ({"name": "count", "user_group": _user_group, "args": [["det1", "det2"]],
         "kwargs": {"num": 10, "delay": 1}},
         [[ophyd.sim.det1, ophyd.sim.det2], 10, 1], {}, True, ""),
        # Explicitly specify the value for 'detectors'.
        ({"name": "move_then_count", "user_group": _user_group,
          "args": [["motor1"], ["det1", "det3"]],
          "kwargs": {"positions": [5, 7]}},
         [[ophyd.sim.motor1], [ophyd.sim.det1, ophyd.sim.det3], [5, 7]], {}, True, ""),
        # Explicitly specify the value for 'detectors': 'det4' is not in the list of allowed values.
        ({"name": "move_then_count", "user_group": _user_group,
          "args": [["motor1"], ["det1", "det4"]],
          "kwargs": {"positions": [5, 7]}},
         [[ophyd.sim.motor1], [ophyd.sim.det1], [5, 7]], {}, False,
         "value is not a valid enumeration member; permitted: 'det1', 'det2', 'det3'"),
        # Use default value for 'detectors' defined in parameter annotation
        ({"name": "move_then_count", "user_group": _user_group, "args": [["motor1"]],
          "kwargs": {"positions": [5, 7]}},
         [[ophyd.sim.motor1]], {"positions": [5, 7], "detectors": [ophyd.sim.det1, ophyd.sim.det2]}, True, ""),
        ({"name": "count", "args": [["det1", "det2"]]}, [], {}, False,
         "No user group is specified in parameters for the plan 'count'"),
        ({"name": "countABC", "user_group": _user_group, "args": [["det1", "det2"]]}, [], {}, False,
         "Plan 'countABC' is not in the list of allowed plans"),
    ],
)
# fmt: on
def test_prepare_plan_1(plan, exp_args, exp_kwargs, success, err_msg):
    """
    Basic test for ``prepare_plan``: test main features using the simulated profile collection.
    """
    pc_path = get_default_startup_dir()
    nspace = load_profile_collection(pc_path)
    plans = plans_from_nspace(nspace)
    devices = devices_from_nspace(nspace)

    path_allowed_plans = os.path.join(pc_path, "existing_plans_and_devices.yaml")
    path_permissions = os.path.join(pc_path, "user_group_permissions.yaml")
    allowed_plans, allowed_devices = load_allowed_plans_and_devices(
        path_existing_plans_and_devices=path_allowed_plans, path_user_group_permissions=path_permissions
    )

    if success:
        plan_parsed = prepare_plan(
            plan,
            plans_in_nspace=plans,
            devices_in_nspace=devices,
            allowed_plans=allowed_plans,
            allowed_devices=allowed_devices,
        )
        expected_keys = ("callable", "args", "kwargs")
        for k in expected_keys:
            assert k in plan_parsed, f"Key '{k}' does not exist: {plan_parsed.keys()}"
        assert f"{plan_parsed['args']}" == f"{exp_args}"  # Compare as strings
        assert f"{plan_parsed['kwargs']}" == f"{exp_kwargs}"
    else:
        with pytest.raises(Exception, match=err_msg):
            prepare_plan(
                plan,
                plans_in_nspace=plans,
                devices_in_nspace=devices,
                allowed_plans=allowed_plans,
                allowed_devices=allowed_devices,
            )


_pp_dev1 = ophyd.Device(name="_pp_dev1")
_pp_dev2 = ophyd.Device(name="_pp_dev2")
_pp_dev3 = ophyd.Device(name="_pp_dev3")


def _pp_p1():
    yield from []


def _pp_p2():
    yield from []


def _pp_p3():
    yield from []


def _pp_generate_stage_devs():
    """
    Created compound devices for testing 'prepare_plan' function.
    """

    from ophyd.sim import motor1 as _pp_motor1, motor2 as _pp_motor2

    class SimStage(ophyd.Device):
        x = ophyd.Component(ophyd.sim.SynAxis, name="y", labels={"motors"})
        y = ophyd.Component(ophyd.sim.SynAxis, name="y", labels={"motors"})
        z = ophyd.Component(ophyd.sim.SynAxis, name="z", labels={"motors"})

        def set(self, x, y, z):
            """Makes the device Movable"""
            self.x.set(x)
            self.y.set(y)
            self.z.set(z)

    class SimDetectors(ophyd.Device):
        """
        The detectors are controlled by simulated 'motor1' and 'motor2'
        defined on the global scale.
        """

        det_A = ophyd.Component(
            ophyd.sim.SynGauss,
            name="det_A",
            motor=_pp_motor1,
            motor_field="motor1",
            center=0,
            Imax=5,
            sigma=0.5,
            labels={"detectors"},
        )
        det_B = ophyd.Component(
            ophyd.sim.SynGauss,
            name="det_B",
            motor=_pp_motor2,
            motor_field="motor2",
            center=0,
            Imax=5,
            sigma=0.5,
            labels={"detectors"},
        )

    class SimBundle(ophyd.Device):
        mtrs = ophyd.Component(SimStage, name="mtrs")
        dets = ophyd.Component(SimDetectors, name="dets")

    sim_bundle_A = SimBundle(name="sim_bundle_A")
    sim_bundle_B = SimBundle(name="sim_bundle_B")  # Used for tests

    return sim_bundle_A, sim_bundle_B


_pp_stg_A, _pp_stg_B = _pp_generate_stage_devs()


def _gen_environment_pp2():
    def plan1(a, b, c):
        yield from [a, b, c]

    def plan2(a: int = 5, b: float = 0.5, s: str = "abc"):
        yield from [a, b, s]

    @parameter_annotation_decorator(
        {
            "parameters": {
                "a": {"annotation": "float", "default": 0.5},
                "b": {"annotation": "int", "default": 5},
                "s": {"annotation": "int", "default": 50},
            }
        }
    )
    def plan3(a: int = 5, b: float = 0.5, s: str = "abc"):
        # Override all the types and defaults (just the test of features)
        yield from [a, b, s]

    @parameter_annotation_decorator(
        {
            "parameters": {
                "detectors": {
                    "annotation": "typing.List[Detectors]",
                    "devices": {"Detectors": ["_pp_dev1", "_pp_dev2", "_pp_dev3"]},
                    # Default list of the detectors
                    "default": ["_pp_dev2", "_pp_dev3"],
                }
            }
        }
    )
    def plan4(detectors: typing.Optional[typing.List[ophyd.Device]] = None):
        # Default for 'detectors' is None, which is converted to the default list of detectors
        detectors = detectors or [_pp_dev2, _pp_dev3]
        yield from detectors

    # Explicitly specifying subdevices in the list
    @parameter_annotation_decorator(
        {
            "parameters": {
                "detectors": {
                    "annotation": "typing.List[Detectors]",
                    "devices": {"Detectors": ["_pp_dev1", "_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]},
                    # Default list of the detectors
                    "default": ["_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"],
                }
            }
        }
    )
    def plan4a(detectors: typing.Optional[typing.List[ophyd.Device]] = None):
        # Default for 'detectors' is None, which is converted to the default list of detectors
        detectors = detectors or [_pp_dev2, _pp_dev3]
        yield from detectors

    # Enable converting all devices (using __DEVICE__ built-in type)
    @parameter_annotation_decorator(
        {
            "parameters": {
                "detectors": {
                    "annotation": "typing.List[__DEVICE__]",
                    # Default list of the detectors
                    "default": ["_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"],
                }
            }
        }
    )
    def plan4b(detectors: typing.Optional[typing.List[ophyd.Device]] = None):
        # Default for 'detectors' is None, which is converted to the default list of detectors
        detectors = detectors or [_pp_dev2, _pp_dev3]
        yield from detectors

    # Enable converting all devices (using 'convert_device_names')
    @parameter_annotation_decorator(
        {
            "parameters": {
                "detectors": {
                    "annotation": "typing.List[str]",
                    "convert_device_names": True,
                    # Default list of the detectors
                    "default": ["_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"],
                }
            }
        }
    )
    def plan4c(detectors: typing.Optional[typing.List[ophyd.Device]] = None):
        # Default for 'detectors' is None, which is converted to the default list of detectors
        detectors = detectors or [_pp_dev2, _pp_dev3]
        yield from detectors

    # Enable converting all devices (using __PLAN_OR_DEVICE__ built-in type)
    @parameter_annotation_decorator(
        {
            "parameters": {
                "detectors": {
                    "annotation": "typing.List[__PLAN_OR_DEVICE__]",
                    # Default list of the detectors
                    "default": ["_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"],
                }
            }
        }
    )
    def plan4d(detectors: typing.Optional[typing.List[ophyd.Device]] = None):
        # Default for 'detectors' is None, which is converted to the default list of detectors
        detectors = detectors or [_pp_dev2, _pp_dev3]
        yield from detectors

    @parameter_annotation_decorator(
        {
            "parameters": {
                "plan_to_execute": {
                    "annotation": "Plans",
                    "plans": {"Plans": ["_pp_p1", "_pp_p2", "_pp_p3"]},
                    # Default list of plans
                    "default": "_pp_p2",
                }
            }
        }
    )
    def plan5(plan_to_execute: typing.Callable = _pp_p2):
        yield from plan_to_execute

    @parameter_annotation_decorator(
        {
            "parameters": {
                "plan_to_execute": {
                    "annotation": "__PLAN__",
                    # Default list of plans
                    "default": "_pp_p2",
                }
            }
        }
    )
    def plan5b(plan_to_execute: typing.Callable = _pp_p2):
        yield from plan_to_execute

    @parameter_annotation_decorator(
        {
            "parameters": {
                "plan_to_execute": {
                    "annotation": "str",
                    "convert_plan_names": True,
                    # Default list of plans
                    "default": "_pp_p2",
                }
            }
        }
    )
    def plan5c(plan_to_execute: typing.Callable = _pp_p2):
        yield from plan_to_execute

    @parameter_annotation_decorator(
        {
            "parameters": {
                "plan_to_execute": {
                    "annotation": "__PLAN_OR_DEVICE__",
                    # Default list of plans
                    "default": "_pp_p2",
                }
            }
        }
    )
    def plan5d(plan_to_execute: typing.Callable = _pp_p2):
        yield from plan_to_execute

    @parameter_annotation_decorator(
        {
            "parameters": {
                "strings": {
                    "annotation": "typing.Union[typing.List[Selection], typing.Tuple[Selection]]",
                    "enums": {"Selection": ["one", "two", "three"]},
                    "default": ("one", "three"),
                }
            }
        }
    )
    def plan6(strings: typing.Union[typing.List[str], typing.Tuple[str]] = ("one", "three")):
        yield from strings

    def plan7(a, b: str):
        # All strings passed as 'a' should be converted to devices/plans when possible.
        #   Strings passed to 'b' should not be converted.
        yield from [a, b]

    # Create namespace
    nspace = {"_pp_dev1": _pp_dev1, "_pp_dev2": _pp_dev2, "_pp_dev3": _pp_dev3}
    nspace.update({"_pp_stg_A": _pp_stg_A, "_pp_stg_B": _pp_stg_B})
    nspace.update({"_pp_p1": _pp_p1, "_pp_p2": _pp_p2, "_pp_p3": _pp_p3})
    nspace.update({"plan1": plan1})
    nspace.update({"plan2": plan2})
    nspace.update({"plan3": plan3})
    nspace.update({"plan4": plan4})
    nspace.update({"plan4a": plan4a})
    nspace.update({"plan4b": plan4b})
    nspace.update({"plan4c": plan4c})
    nspace.update({"plan4d": plan4d})
    nspace.update({"plan5": plan5})
    nspace.update({"plan5b": plan5b})
    nspace.update({"plan5c": plan5c})
    nspace.update({"plan5d": plan5d})
    nspace.update({"plan6": plan6})
    nspace.update({"plan7": plan7})

    plans_in_nspace = plans_from_nspace(nspace)
    devices_in_nspace = devices_from_nspace(nspace)

    existing_devices = _prepare_devices(devices_in_nspace)
    existing_plans = _prepare_plans(plans_in_nspace, existing_devices=existing_devices)
    allowed_plans, allowed_devices = {}, {}
    allowed_plans["root"], allowed_devices["root"] = existing_plans.copy(), existing_devices.copy()
    allowed_plans["admin"], allowed_devices["admin"] = existing_plans.copy(), existing_devices.copy()

    return plans_in_nspace, devices_in_nspace, allowed_plans, allowed_devices


# fmt: off
@pytest.mark.parametrize(
    "plan_name, plan, remove_objs, exp_args, exp_kwargs, exp_meta, success, err_msg",
    [
        # Passing simple set of parameters as args, kwargs or combination. No default values.
        ("plan1", {"user_group": "admin", "args": [3, 4, 5]}, [],
         [3, 4, 5], {}, {}, True, ""),
        ("plan1", {"user_group": "admin", "args": [3, 4], "kwargs": {"c": 5}}, [],
         [3, 4, 5], {}, {}, True, ""),
        ("plan1", {"user_group": "admin", "args": [3, 4], "kwargs": {"b": 5}}, [],
         [3, 4, 5], {}, {}, False, "Plan validation failed: multiple values for argument 'b'"),
        ("plan1", {"user_group": "admin", "args": [3, 4, 5], "meta": {"p1": 10, "p2": "abc"}}, [],
         [3, 4, 5], {}, {"p1": 10, "p2": "abc"}, True, ""),
        ("plan1", {"user_group": "admin", "args": [3, 4, 5], "meta": [{"p1": 10}, {"p2": "abc"}]}, [],
         [3, 4, 5], {}, {"p1": 10, "p2": "abc"}, True, ""),
        ("plan1", {"user_group": "admin", "args": [3, 4, 5], "meta": [{"p1": 10}, {"p1": 5, "p2": "abc"}]}, [],
         [3, 4, 5], {}, {"p1": 10, "p2": "abc"}, True, ""),

        # Passing simple set of parameters as args and kwargs with default values.
        ("plan2", {"user_group": "admin"}, [],
         [], {}, {}, True, ""),
        ("plan2", {"user_group": "admin", "args": [3, 2.6]}, [],
         [3, 2.6], {}, {}, True, ""),
        ("plan2", {"user_group": "admin", "args": [2.6, 3]}, [],
         [2.6, 3], {}, {}, False, " Incorrect parameter type: key='a', value='2.6'"),
        ("plan2", {"user_group": "admin", "kwargs": {"b": 9.9, "s": "def"}}, [],
         [], {"b": 9.9, "s": "def"}, {}, True, ""),

        # Plan with parameters of numerical type. Parameter types and default values are specified
        #   in 'parameter_annotation_decorator'.
        ("plan3", {"user_group": "admin"}, [],
         [], {'a': 0.5, 'b': 5, 's': 50}, {}, True, ""),
        ("plan3", {"user_group": "admin", "args": [2.8]}, [],
         [2.8], {'b': 5, 's': 50}, {}, True, ""),
        ("plan3", {"user_group": "admin", "args": [2.8], "kwargs": {"a": 2.8}}, [],
         [2.8], {'b': 5, 's': 50}, {}, False, "multiple values for argument 'a'"),
        ("plan3", {"user_group": "admin", "args": [], "kwargs": {"b": 30}}, [],
         [], {'a': 0.5, 'b': 30, 's': 50}, {}, True, ""),
        ("plan3", {"user_group": "admin", "args": [], "kwargs": {"b": 2.8}}, [],
         [], {'a': 0.5, 'b': 2.8, 's': 50}, {}, False, "Incorrect parameter type: key='b', value='2.8'"),

        # Plan with a single parameter, which is a list of detector. The list of detector names (enum)
        #   and default value (list of detectors) is specified in 'parameter_annotation_decorator'.
        #   Test if the detector names are properly converted to objects when passed as args, kwargs
        #   or if the default value is used (function is called without an argument).
        ("plan4", {"user_group": "admin"}, [],
         [], {"detectors": [_pp_dev2, _pp_dev3]}, {}, True, ""),
        ("plan4", {"user_group": "admin", "args": [["_pp_dev1", "_pp_dev3"]]}, [],
         [[_pp_dev1, _pp_dev3]], {}, {}, True, ""),
        ("plan4", {"user_group": "admin", "kwargs": {"detectors": ["_pp_dev1", "_pp_dev3"]}}, [],
         [[_pp_dev1, _pp_dev3]], {}, {}, True, ""),
        ("plan4", {"user_group": "admin", "kwargs": {"detectors": ["nonexisting_dev", "_pp_dev3"]}}, [],
         [[_pp_dev1, _pp_dev3]], {}, {}, False, "value is not a valid enumeration member"),

        # Passing subdevice names to plans. Parameter annotation contains fixed lists of device names,
        #   so the passed devices should be converted to objects or parameter validation should fail
        ("plan4a", {"user_group": "admin"}, [],
         [], {"detectors": [_pp_stg_A.mtrs, _pp_stg_A.mtrs.y]}, {}, True, ""),
        # If some of the default values are not in the list of allowed devices, they are still passed
        #   to the plan, but not converted to the device objects
        ("plan4a", {"user_group": "admin"}, ["_pp_stg_A.mtrs"],  # Remove '_pp_stg_A' from the list
         [], {"detectors": ["_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}, {}, True, ""),
        ("plan4a", {"user_group": "admin"}, ["_pp_stg_A", True],  # Set '_pp_stg_A' as excluded
         [], {"detectors": [_pp_stg_A.mtrs, _pp_stg_A.mtrs.y]}, {}, True, ""),
        ("plan4a", {"user_group": "admin"}, ["_pp_stg_A.mtrs", True],  # Set '_pp_stg_A.mtrs' as excluded
         [], {"detectors": ["_pp_stg_A.mtrs", _pp_stg_A.mtrs.y]}, {}, True, ""),
        # Passing a list of devices as args
        ("plan4a", {"user_group": "admin", "args": [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]]}, [],
         [[_pp_stg_A, _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, True, ""),
        # Passing a list of devices as kwargs
        ("plan4a", {"user_group": "admin",
         "kwargs": {"detectors": ["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}}, [],
         [[_pp_stg_A, _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, True, ""),
        # Passing a list of devices as args, remove a device from the list, or set a device as excluded
        ("plan4a", {"user_group": "admin",
         "args": [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]]}, ["_pp_stg_A"],
         [[_pp_stg_A, _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, False, "Validation of plan parameters failed"),
        ("plan4a", {"user_group": "admin",
         "args": [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]]}, ["_pp_stg_A", True],
         [[_pp_stg_A, _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, False, "Validation of plan parameters failed"),
        ("plan4a", {"user_group": "admin",
         "args": [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]]}, ["_pp_stg_A.mtrs.y", True],
         [[_pp_stg_A, _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, False, "Validation of plan parameters failed"),
        # Passing a list of devices as kwargs, remove a device from the list, or set a device as excluded
        ("plan4a", {"user_group": "admin",
         "kwargs": {"detectors": ["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}}, ["_pp_stg_A"],
         [[_pp_stg_A, _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, False, "Validation of plan parameters failed"),
        ("plan4a", {"user_group": "admin",
         "kwargs": {"detectors": ["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}}, ["_pp_stg_A", True],
         [[_pp_stg_A, _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, False, "Validation of plan parameters failed"),
        ("plan4a", {"user_group": "admin",
         "kwargs": {"detectors": ["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}}, ["_pp_stg_A.mtrs.y", True],
         [[_pp_stg_A, _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, False, "Validation of plan parameters failed"),

        # Passing subdevice names to plans. Parameter contains '__DEVICE__' built-in type in the annotation.
        # Processing of detault types is employing a different mechanism, so the tests should be repeated even
        #   if they work differently.
        ("plan4b", {"user_group": "admin"}, [],
         [], {"detectors": [_pp_stg_A.mtrs, _pp_stg_A.mtrs.y]}, {}, True, ""),
        # If some of the default values are not in the list of allowed devices, they are still passed
        #   to the plan, but not converted to the device objects
        ("plan4b", {"user_group": "admin"}, ["_pp_stg_A.mtrs"],  # Remove '_pp_stg_A' from the list
         [], {"detectors": ["_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}, {}, True, ""),
        ("plan4b", {"user_group": "admin"}, ["_pp_stg_A", True],  # Set '_pp_stg_A' as excluded
         [], {"detectors": [_pp_stg_A.mtrs, _pp_stg_A.mtrs.y]}, {}, True, ""),
        ("plan4b", {"user_group": "admin"}, ["_pp_stg_A.mtrs", True],  # Set '_pp_stg_A.mtrs' as excluded
         [], {"detectors": ["_pp_stg_A.mtrs", _pp_stg_A.mtrs.y]}, {}, True, ""),
        # Passing a list of devices as args
        ("plan4b", {"user_group": "admin", "args": [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]]}, [],
         [[_pp_stg_A, _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, True, ""),
        # Passing a list of devices as kwargs
        ("plan4b", {"user_group": "admin",
         "kwargs": {"detectors": ["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}}, [],
         [[_pp_stg_A, _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, True, ""),
        # Passing a list of devices as args, remove a device from the list, or set a device as excluded
        ("plan4b", {"user_group": "admin",
         "args": [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]]}, ["_pp_stg_A"],
         [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]], {}, {}, True, ""),
        ("plan4b", {"user_group": "admin",
         "args": [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]]}, ["_pp_stg_A", True],
         [["_pp_stg_A", _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, True, ""),
        ("plan4b", {"user_group": "admin",
         "args": [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]]}, ["_pp_stg_A.mtrs.y", True],
         [[_pp_stg_A, _pp_stg_A.mtrs, "_pp_stg_A.mtrs.y"]], {}, {}, True, ""),
        # Passing a list of devices as kwargs, remove a device from the list, or set a device as excluded
        ("plan4b", {"user_group": "admin",
         "kwargs": {"detectors": ["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}}, ["_pp_stg_A"],
         [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]], {}, {}, True, ""),
        ("plan4b", {"user_group": "admin",
         "kwargs": {"detectors": ["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}}, ["_pp_stg_A", True],
         [["_pp_stg_A", _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, True, ""),
        ("plan4b", {"user_group": "admin",
         "kwargs": {"detectors": ["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}}, ["_pp_stg_A.mtrs.y", True],
         [[_pp_stg_A, _pp_stg_A.mtrs, "_pp_stg_A.mtrs.y"]], {}, {}, True, ""),

        # Passing subdevice names to plans. Use "convert_device_name": True to enable conversion
        # Processing is employing a different mechanism, so the tests should be repeated even
        #   if they work differently.
        ("plan4c", {"user_group": "admin"}, [],
         [], {"detectors": [_pp_stg_A.mtrs, _pp_stg_A.mtrs.y]}, {}, True, ""),
        # If some of the default values are not in the list of allowed devices, they are still passed
        #   to the plan, but not converted to the device objects
        ("plan4c", {"user_group": "admin"}, ["_pp_stg_A.mtrs"],  # Remove '_pp_stg_A' from the list
         [], {"detectors": ["_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}, {}, True, ""),
        ("plan4c", {"user_group": "admin"}, ["_pp_stg_A", True],  # Set '_pp_stg_A' as excluded
         [], {"detectors": [_pp_stg_A.mtrs, _pp_stg_A.mtrs.y]}, {}, True, ""),
        ("plan4c", {"user_group": "admin"}, ["_pp_stg_A.mtrs", True],  # Set '_pp_stg_A.mtrs' as excluded
         [], {"detectors": ["_pp_stg_A.mtrs", _pp_stg_A.mtrs.y]}, {}, True, ""),
        # Passing a list of devices as args
        ("plan4c", {"user_group": "admin", "args": [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]]}, [],
         [[_pp_stg_A, _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, True, ""),
        # Passing a list of devices as kwargs
        ("plan4c", {"user_group": "admin",
         "kwargs": {"detectors": ["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}}, [],
         [[_pp_stg_A, _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, True, ""),
        # Passing a list of devices as args, remove a device from the list, or set a device as excluded
        ("plan4c", {"user_group": "admin",
         "args": [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]]}, ["_pp_stg_A"],
         [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]], {}, {}, True, ""),
        ("plan4c", {"user_group": "admin",
         "args": [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]]}, ["_pp_stg_A", True],
         [["_pp_stg_A", _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, True, ""),
        ("plan4c", {"user_group": "admin",
         "args": [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]]}, ["_pp_stg_A.mtrs.y", True],
         [[_pp_stg_A, _pp_stg_A.mtrs, "_pp_stg_A.mtrs.y"]], {}, {}, True, ""),
        # Passing a list of devices as kwargs, remove a device from the list, or set a device as excluded
        ("plan4c", {"user_group": "admin",
         "kwargs": {"detectors": ["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}}, ["_pp_stg_A"],
         [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]], {}, {}, True, ""),
        ("plan4c", {"user_group": "admin",
         "kwargs": {"detectors": ["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}}, ["_pp_stg_A", True],
         [["_pp_stg_A", _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, True, ""),
        ("plan4c", {"user_group": "admin",
         "kwargs": {"detectors": ["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}}, ["_pp_stg_A.mtrs.y", True],
         [[_pp_stg_A, _pp_stg_A.mtrs, "_pp_stg_A.mtrs.y"]], {}, {}, True, ""),

        # Passing subdevice names to plans. Parameter contains '__PLAN_OR_DEVICE__' built-in type
        #   in the annotation. The mechanism is similar to the one used with '__PLAN__" type, so
        #   just run a few tests to see if it works.
        ("plan4d", {"user_group": "admin"}, [],
         [], {"detectors": [_pp_stg_A.mtrs, _pp_stg_A.mtrs.y]}, {}, True, ""),
        # Passing a list of devices as args
        ("plan4d", {"user_group": "admin", "args": [["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]]}, [],
         [[_pp_stg_A, _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, True, ""),
        # Passing a list of devices as kwargs
        ("plan4d", {"user_group": "admin",
         "kwargs": {"detectors": ["_pp_stg_A", "_pp_stg_A.mtrs", "_pp_stg_A.mtrs.y"]}}, [],
         [[_pp_stg_A, _pp_stg_A.mtrs, _pp_stg_A.mtrs.y]], {}, {}, True, ""),



        # Check if a plan name passed as arg, kwarg or using default value is properly converted to an object.
        ("plan5", {"user_group": "admin"}, [],
         [], {"plan_to_execute": _pp_p2}, {}, True, ""),
        ("plan5", {"user_group": "admin", "args": ["_pp_p3"]}, [],
         [_pp_p3], {}, {}, True, ""),
        ("plan5", {"user_group": "admin", "kwargs": {"plan_to_execute": "_pp_p3"}}, [],
         [_pp_p3], {}, {}, True, ""),
        ("plan5", {"user_group": "admin", "args": ["nonexisting_plan"]}, [],
         [_pp_p3], {}, {}, False, "value is not a valid enumeration member"),
        # Remove plan from the list of allowed plans
        ("plan5", {"user_group": "admin"}, ["_pp_p2"],
         [], {"plan_to_execute": "_pp_p2"}, {}, True, ""),
        ("plan5", {"user_group": "admin", "kwargs": {"plan_to_execute": "_pp_p3"}}, ["_pp_p3"],  # remove
         [_pp_p3], {}, {}, False, "Validation of plan parameters failed"),
        ("plan5", {"user_group": "admin", "kwargs": {"plan_to_execute": "_pp_p3"}}, ["_pp_p3", True],  # exclude
         [_pp_p3], {}, {}, False, "Validation of plan parameters failed"),

        # Passing plan names. Parameter contains '__PLAN__' built-in type in the annotation.
        ("plan5b", {"user_group": "admin"}, [],
         [], {"plan_to_execute": _pp_p2}, {}, True, ""),
        ("plan5b", {"user_group": "admin", "args": ["_pp_p3"]}, [],
         [_pp_p3], {}, {}, True, ""),
        ("plan5b", {"user_group": "admin", "kwargs": {"plan_to_execute": "_pp_p3"}}, [],
         [_pp_p3], {}, {}, True, ""),
        ("plan5b", {"user_group": "admin"}, ["_pp_p2"],
         [], {"plan_to_execute": "_pp_p2"}, {}, True, ""),
        ("plan5b", {"user_group": "admin", "kwargs": {"plan_to_execute": "_pp_p3"}}, ["_pp_p3"],  # remove
         ["_pp_p3"], {}, {}, True, ""),
        ("plan5b", {"user_group": "admin", "kwargs": {"plan_to_execute": "_pp_p3"}}, ["_pp_p3", True],  # exclude
         ["_pp_p3"], {}, {}, True, ""),

        # Passing plan names. Use Parameter 'convert_plan_names'.
        ("plan5c", {"user_group": "admin"}, [],
         [], {"plan_to_execute": _pp_p2}, {}, True, ""),
        ("plan5c", {"user_group": "admin", "args": ["_pp_p3"]}, [],
         [_pp_p3], {}, {}, True, ""),
        ("plan5c", {"user_group": "admin", "kwargs": {"plan_to_execute": "_pp_p3"}}, [],
         [_pp_p3], {}, {}, True, ""),
        ("plan5c", {"user_group": "admin"}, ["_pp_p2"],
         [], {"plan_to_execute": "_pp_p2"}, {}, True, ""),
        ("plan5c", {"user_group": "admin", "kwargs": {"plan_to_execute": "_pp_p3"}}, ["_pp_p3"],  # remove
         ["_pp_p3"], {}, {}, True, ""),
        ("plan5c", {"user_group": "admin", "kwargs": {"plan_to_execute": "_pp_p3"}}, ["_pp_p3", True],  # exclude
         ["_pp_p3"], {}, {}, True, ""),

        # Passing plan names. Parameter contains '__PLAN_OR_DEVICE__' built-in type in the annotation.
        ("plan5d", {"user_group": "admin"}, [],
         [], {"plan_to_execute": _pp_p2}, {}, True, ""),
        ("plan5d", {"user_group": "admin", "args": ["_pp_p3"]}, [],
         [_pp_p3], {}, {}, True, ""),
        ("plan5d", {"user_group": "admin", "kwargs": {"plan_to_execute": "_pp_p3"}}, [],
         [_pp_p3], {}, {}, True, ""),
        ("plan5d", {"user_group": "admin"}, ["_pp_p2"],
         [], {"plan_to_execute": "_pp_p2"}, {}, True, ""),
        ("plan5d", {"user_group": "admin", "kwargs": {"plan_to_execute": "_pp_p3"}}, ["_pp_p3"],  # remove
         ["_pp_p3"], {}, {}, True, ""),
        ("plan5d", {"user_group": "admin", "kwargs": {"plan_to_execute": "_pp_p3"}}, ["_pp_p3", True],  # exclude
         ["_pp_p3"], {}, {}, True, ""),


        # Pass values of custom enum type. The values are not converted to objects.
        ("plan6", {"user_group": "admin"}, [],
         [], {"strings": ["one", "three"]}, {}, True, ""),
        ("plan6", {"user_group": "admin", "args": [["one", "two"]]}, [],
         [["one", "two"]], {}, {}, True, ""),
        ("plan6", {"user_group": "admin", "args": [("one", "two")]}, [],
         [("one", "two")], {}, {}, True, ""),
        ("plan6", {"user_group": "admin", "args": [("one", "nonexisting")]}, [],
         [("one", "two")], {}, {}, False, "value is not a valid enumeration member"),

        # Plan has no custom annotation. All strings must be converted to objects whenever possible.
        ("plan7", {"user_group": "admin", "args": [["_pp_dev1", "_pp_dev3"], "_pp_dev2"]}, [],
         [[_pp_dev1, _pp_dev3], "_pp_dev2"], {}, {}, True, ""),
        ("plan7", {"user_group": "admin", "args": [["_pp_dev1", "_pp_dev3", "some_str"], "_pp_dev2"]}, [],
         [[_pp_dev1, _pp_dev3, "some_str"], "_pp_dev2"], {}, {}, True, ""),
        ("plan7", {"user_group": "admin", "args": [["_pp_dev1", "_pp_dev3", "some_str"], 50]}, [],
         [[_pp_dev1, _pp_dev3, "some_str"], "_pp_dev2"], {}, {}, False,
         "Incorrect parameter type: key='b', value='50'"),

        # General failing cases
        ("nonexisting_plan", {"user_group": "admin"}, [],
         [], {}, {}, False, "Plan 'nonexisting_plan' is not in the list of allowed plans"),
        ("plan2", {"user_group": "nonexisting_group"}, [],
         [], {}, {}, False,
         "Lists of allowed plans and devices is not defined for the user group 'nonexisting_group'"),
    ],
)
# fmt: on
def test_prepare_plan_2(plan_name, plan, remove_objs, exp_args, exp_kwargs, exp_meta, success, err_msg):
    """
    Detailed tests for ``prepare_plan``. Preparation of plan parameters before execution
    is one of the key features, so unit tests are needed for all use cases.
    """
    plans_in_nspace, devices_in_nspace, allowed_plans, allowed_devices = _gen_environment_pp2()

    if remove_objs and isinstance(remove_objs[-1], bool):
        use_exclude = remove_objs.pop()
    else:
        use_exclude = False

    # Remove some objects from 'allowed_plans' and 'allowed_devices'
    for obj_name in remove_objs:
        if use_exclude:
            print(f"Set the following objects as excluded: {remove_objs}")
            # Exclude individual nodes. Assume that the device is in the list.
            components = obj_name.split(".")
            for allowed_objs in (allowed_plans["admin"], allowed_devices["admin"]):
                try:
                    root = allowed_objs[components[0]]
                    for c in components[1:]:
                        root = root["components"][c]
                    print(f"Excluding {obj_name!r} ...")
                    root["excluded"] = True
                except Exception:
                    pass
        else:
            # Remove elements from the list based on the root name for the device
            print(f"Removing object {obj_name.split('.')[0]!r}")
            allowed_plans["admin"].pop(obj_name.split(".")[0], None)
            allowed_devices["admin"].pop(obj_name.split(".")[0], None)

    # assert False, list(allowed_plans.keys())
    allowed_plans["admin"] = _filter_allowed_plans(
        allowed_plans=allowed_plans["admin"], allowed_devices=allowed_devices["admin"]
    )
    # assert False, allowed_plans["plan4a"]

    plan["name"] = plan_name

    if success:
        plan_parsed = prepare_plan(
            plan,
            plans_in_nspace=plans_in_nspace,
            devices_in_nspace=devices_in_nspace,
            allowed_plans=allowed_plans,
            allowed_devices=allowed_devices,
        )
        expected_keys = ("callable", "args", "kwargs", "meta")
        for k in expected_keys:
            assert k in plan_parsed, f"Key '{k}' does not exist: {plan_parsed.keys()}"
        assert isinstance(plan_parsed["callable"], Callable)
        assert plan_parsed["args"] == exp_args, pprint.pformat(plan_parsed["args"])
        assert plan_parsed["kwargs"] == exp_kwargs, pprint.pformat(plan_parsed["kwargs"])
        assert plan_parsed["meta"] == exp_meta, pprint.pformat(plan_parsed["meta"])
    else:
        with pytest.raises(Exception, match=err_msg):
            prepare_plan(
                plan,
                plans_in_nspace=plans_in_nspace,
                devices_in_nspace=devices_in_nspace,
                allowed_plans=allowed_plans,
                allowed_devices=allowed_devices,
            )


_prep_func_script_1 = """
def func1():
    return 10

def func2(p0=2, *, p1=10):
    return 20 + p0 + p1

class A:
    def __call__(self):
        return "A"
    def f(self):
        return "f"

    @staticmethod
    def fs():
        return "fs"

    @classmethod
    def fc(cls):
        return "fc"

func3 = A()
func4 = func3.f
func5 = A.fs
func6 = A.fc

def gen1():
    yield "This is a generator"

def not_allowed_func():
    pass

some_object = "some string"
"""

_prep_func_permissions = {
    "user_groups": {
        "root": {"allowed_functions": [None], "forbidden_functions": [None]},
        "admin": {
            "allowed_functions": [":^func", ":^gen", ":^some_object$", "unknown"],
            "forbidden_functions": [None],
        },
    }
}


# fmt: off
@pytest.mark.parametrize("func_info, result", [
    ({"name": "func1", "user_group": "admin"}, 10),
    ({"name": "func2", "user_group": "admin"}, 32),
    ({"name": "func2", "args": [5], "user_group": "admin"}, 35),
    ({"name": "func2", "kwargs": {"p1": 50}, "user_group": "admin"}, 72),
    ({"name": "func2", "args": [3], "kwargs": {"p1": 50}, "user_group": "admin"}, 73),
    ({"name": "func2", "kwargs": {"p0": 4, "p1": 50}, "user_group": "admin"}, 74),
    ({"name": "func3", "user_group": "admin"}, "A"),
    ({"name": "func4", "user_group": "admin"}, "f"),
    ({"name": "func5", "user_group": "admin"}, "fs"),
    ({"name": "func6", "user_group": "admin"}, "fc"),
    ({"name": "not_allowed_func", "user_group": "admin"}, None),  # No checks for permissions
])
# fmt: on
def test_prepare_function_1(func_info, result):
    """
    Basic test for 'prepare_function'. Test with different types of callables.
    """
    nspace = {}
    load_script_into_existing_nspace(script=_prep_func_script_1, nspace=nspace)

    func_prepared = prepare_function(func_info=func_info, nspace=nspace)

    def execute_func(fp):
        return fp["callable"](*fp["args"], **fp["kwargs"])

    assert execute_func(func_prepared) == result


# fmt: off
@pytest.mark.parametrize("func_info, except_type, msg", [
    ({"user_group": "admin"}, RuntimeError, "No function name is specified"),
    ({"name": "func1"}, RuntimeError, "No user group is specified"),
    ({"name": "unknown", "user_group": "admin"}, RuntimeError, "Function 'unknown' is not found"),
    ({"name": "some_object", "user_group": "admin"}, RuntimeError, "is not callable"),
    ({"name": "gen1", "user_group": "admin"}, RuntimeError, "is a generator function"),
    ({"name": "func1", "user_group": "unknown"},
     KeyError, "No permissions are defined for user group 'unknown'"),
    ({"name": "not_allowed_func", "user_group": "admin"},
     RuntimeError, "Function 'not_allowed_func' is not allowed"),
])
# fmt: on
def test_prepare_function_2(func_info, except_type, msg):
    """
    Tests for 'prepare_function': failing cases
    """
    nspace = {}
    load_script_into_existing_nspace(script=_prep_func_script_1, nspace=nspace)

    _validate_user_group_permissions_schema(_prep_func_permissions)
    with pytest.raises(except_type, match=msg):
        prepare_function(func_info=func_info, nspace=nspace, user_group_permissions=_prep_func_permissions)


def test_gen_list_of_plans_and_devices_1(tmp_path):
    """
    Copy simulated profile collection and generate the list of allowed (in this case available)
    plans and devices based on the profile collection
    """
    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=False)

    fln_yaml = "list.yaml"
    gen_list_of_plans_and_devices(startup_dir=pc_path, file_dir=pc_path, file_name=fln_yaml)
    assert os.path.isfile(os.path.join(pc_path, fln_yaml)), "List of plans and devices was not created"

    # Attempt to overwrite the file
    with pytest.raises(RuntimeError, match="already exists. File overwriting is disabled."):
        gen_list_of_plans_and_devices(startup_dir=pc_path, file_dir=pc_path, file_name=fln_yaml)

    # Allow file overwrite
    gen_list_of_plans_and_devices(startup_dir=pc_path, file_dir=pc_path, file_name=fln_yaml, overwrite=True)


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
def test_gen_list_of_plans_and_devices_cli(tmp_path, monkeypatch, test, exit_code):
    """
    Test for ``qserver-list-plans_devices`` CLI tool for generating list of plans and devices.
    Copy simulated profile collection and generate the list of allowed (in this case available)
    plans and devices based on the profile collection.
    """
    pc_path = os.path.join(tmp_path, "script_dir1")
    script_path = os.path.join(pc_path, "startup_script.py")

    os.makedirs(pc_path, exist_ok=True)
    with open(script_path, "w") as f:
        f.write(_startup_script_1)

    fln_yaml = "existing_plans_and_devices.yaml"

    # Make sure that .yaml file does not exist
    assert not os.path.isfile(os.path.join(pc_path, fln_yaml))

    os.chdir(tmp_path)

    if test == "startup_collection_at_current_dir":
        os.chdir(pc_path)
        params = ["qserver-list-plans-devices", "--startup-dir", "."]

    elif test == "startup_collection_dir":
        params = ["qserver-list-plans-devices", "--startup-dir", pc_path, "--file-dir", pc_path]

    elif test == "startup_collection_incorrect_path_A":
        # Path exists (default path is used), but there are no startup files (fails)
        params = ["qserver-list-plans-devices", "--startup-dir", "."]

    elif test == "startup_collection_incorrect_path_B":
        # Path does not exist
        path_nonexisting = os.path.join(tmp_path, "abcde")
        params = ["qserver-list-plans-devices", "--startup-dir", path_nonexisting, "--file-dir", pc_path]

    elif test == "startup_script_path":
        params = ["qserver-list-plans-devices", "--startup-script", script_path, "--file-dir", pc_path]

    elif test == "startup_script_path_incorrect":
        params = [
            "qserver-list-plans-devices",
            "--startup-script",
            "non_existing_path",
            "--file-dir",
            pc_path,
        ]

    elif test == "startup_module_name":
        monkeypatch.setenv("PYTHONPATH", os.path.split(pc_path)[0])
        s_name = "script_dir1.startup_script"
        params = ["qserver-list-plans-devices", "--startup-module", s_name, "--file-dir", pc_path]

    elif test == "startup_module_name_incorrect":
        monkeypatch.setenv("PYTHONPATH", os.path.split(pc_path)[0])
        s_name = "incorrect.module.name"
        params = ["qserver-list-plans-devices", "--startup-module", s_name, "--file-dir", pc_path]

    elif test == "file_incorrect_path":
        # Path does not exist
        path_nonexisting = os.path.join(tmp_path, "abcde")
        params = ["qserver-list-plans-devices", "--startup-dir", pc_path, "--file-dir", path_nonexisting]

    else:
        assert False, f"Unknown test '{test}'"

    assert subprocess.call(params) == exit_code

    if exit_code == 0:
        assert os.path.isfile(os.path.join(pc_path, fln_yaml))
    else:
        assert not os.path.isfile(os.path.join(pc_path, fln_yaml))


def test_load_existing_plans_and_devices_1():
    """
    Loads the list of allowed plans and devices from simulated profile collection.
    """
    pc_path = get_default_startup_dir()
    file_path = os.path.join(pc_path, "existing_plans_and_devices.yaml")

    existing_plans, existing_devices = load_existing_plans_and_devices(file_path)

    assert isinstance(existing_plans, dict), "Incorrect type of 'allowed_plans'"
    assert len(existing_plans) > 0, "List of allowed plans was not loaded"
    assert isinstance(existing_devices, dict), "Incorrect type of 'allowed_devices'"
    assert len(existing_devices) > 0, "List of allowed devices was not loaded"

    existing_plans, existing_devices = load_existing_plans_and_devices(None)
    assert existing_plans == {}
    assert existing_devices == {}


# fmt: off
@pytest.mark.parametrize("option", ["normal", "no_file", "empty_file", "corrupt_file1", "corrupt_file2"])
# fmt: on
def test_load_existing_plans_and_devices_2(tmp_path, option):

    path_to_file = os.path.join(tmp_path, "some_dir")
    os.makedirs(path_to_file, exist_ok=True)
    path_to_file = os.path.join(path_to_file, "p_and_d.yaml")

    if option == "normal":
        pc_path = get_default_startup_dir()
        original_file_path = os.path.join(pc_path, "existing_plans_and_devices.yaml")
        shutil.copy(original_file_path, path_to_file)
    elif option == "empty_file":
        with open(path_to_file, "w") as f:
            pass
    elif option == "corrupt_file1":
        # Not YAML
        with open(path_to_file, "w") as f:
            f.writelines(["This string is not expected in YAML file", "Reading the file is expected to fail"])
    elif option == "corrupt_file2":
        # Invalid YAML
        with open(path_to_file, "w") as f:
            f.writelines(["- Item 1\n- Item 2\nItem3"])
    elif option == "no_file":
        pass
    else:
        assert False, f"Unknown testing option '{option}'"

    existing_plans, existing_devices = load_existing_plans_and_devices(path_to_file)

    if option == "normal":
        assert existing_plans != {}
        assert existing_devices != {}
    else:
        assert existing_plans == {}
        assert existing_devices == {}


def _copy_built_in_yaml_files(tmp_path):
    """
    Creates a copy of built-in startup files in temporary directory. Returns full paths
    to the copies of ``existing_plans_and_devices.yaml`` and ``user_group_permissions.yaml`` files.
    """
    pc_path1 = get_default_startup_dir()
    file_path_existing_plans_and_devices1 = os.path.join(pc_path1, "existing_plans_and_devices.yaml")
    file_path_user_group_permissions1 = os.path.join(pc_path1, "user_group_permissions.yaml")

    pc_path2 = os.path.join(tmp_path, "startup")
    file_path_existing_plans_and_devices2 = os.path.join(pc_path2, "existing_plans_and_devices.yaml")
    file_path_user_group_permissions2 = os.path.join(pc_path2, "user_group_permissions.yaml")

    os.makedirs(pc_path2, exist_ok=True)
    shutil.copy(file_path_existing_plans_and_devices1, file_path_existing_plans_and_devices2)
    shutil.copy(file_path_user_group_permissions1, file_path_user_group_permissions2)

    return file_path_existing_plans_and_devices2, file_path_user_group_permissions2


# fmt: off
@pytest.mark.parametrize("load_ref_from_file", [True, False])
# fmt: on
def test_update_existing_plans_and_devices_1(tmp_path, load_ref_from_file):
    """
    Test that the file on disk IS NOT modified if the lists of existing plans and devices are not changed.
    """
    fp_existing_plans_and_devices, _ = _copy_built_in_yaml_files(tmp_path)

    existing_plans, existing_devices = load_existing_plans_and_devices(fp_existing_plans_and_devices)

    if load_ref_from_file:
        t1 = os.path.getctime(fp_existing_plans_and_devices)
        ttime.sleep(0.005)

        changes_detected = update_existing_plans_and_devices(
            path_to_file=fp_existing_plans_and_devices,
            existing_plans=existing_plans,
            existing_devices=existing_devices,
        )
    else:
        # Remove the contents of the file. The file should not be updated, since we are using
        # references passed as parameters
        with open(fp_existing_plans_and_devices, "w"):
            pass

        t1 = os.path.getctime(fp_existing_plans_and_devices)
        ttime.sleep(0.005)

        changes_detected = update_existing_plans_and_devices(
            path_to_file=fp_existing_plans_and_devices,
            existing_plans=existing_plans,
            existing_devices=existing_devices,
            existing_devices_ref=copy.deepcopy(existing_devices),
            existing_plans_ref=copy.deepcopy(existing_plans),
        )

    t2 = os.path.getctime(fp_existing_plans_and_devices)
    assert t1 == t2, "The file was modified"

    assert changes_detected is False


# fmt: off
@pytest.mark.parametrize("load_ref_from_file", [True, False])
@pytest.mark.parametrize("modified", ["plans", "devices"])
# fmt: on
def test_update_existing_plans_and_devices_2(tmp_path, load_ref_from_file, modified):
    """
    Test that the file on disk IS modified if the lists of existing plans and devices are not changed.
    """
    fp_existing_plans_and_devices, _ = _copy_built_in_yaml_files(tmp_path)
    t1 = os.path.getctime(fp_existing_plans_and_devices)
    ttime.sleep(0.005)

    existing_plans, existing_devices = load_existing_plans_and_devices(fp_existing_plans_and_devices)
    existing_plans_modified = copy.deepcopy(existing_plans)
    existing_devices_modified = copy.deepcopy(existing_devices)

    if modified == "plans":
        del existing_plans_modified["count"]
    elif modified == "devices":
        del existing_devices_modified["det"]
    else:
        assert False, f"Unknown option '{modified}'"

    if load_ref_from_file:
        changes_detected = update_existing_plans_and_devices(
            path_to_file=fp_existing_plans_and_devices,
            existing_plans=existing_plans_modified,
            existing_devices=existing_devices_modified,
        )
    else:
        changes_detected = update_existing_plans_and_devices(
            path_to_file=fp_existing_plans_and_devices,
            existing_plans=existing_plans_modified,
            existing_devices=existing_devices_modified,
            existing_devices_ref=copy.deepcopy(existing_devices),
            existing_plans_ref=copy.deepcopy(existing_plans),
        )

    t2 = os.path.getctime(fp_existing_plans_and_devices)
    assert t1 != t2, "The file was not modified"

    existing_plans2, existing_devices2 = load_existing_plans_and_devices(fp_existing_plans_and_devices)
    assert existing_plans2 == existing_plans_modified
    assert existing_devices2 == existing_devices_modified

    assert changes_detected is True


# fmt: off
@pytest.mark.parametrize("file_state", ["removed", "empty", "corrupt"])
# fmt: on
def test_update_existing_plans_and_devices_3(tmp_path, file_state):
    """
    Test that the file is correctly updated if it does not exist or empty.
    """
    fp_existing_plans_and_devices, _ = _copy_built_in_yaml_files(tmp_path)

    existing_plans, existing_devices = load_existing_plans_and_devices(fp_existing_plans_and_devices)
    if file_state == "removed":
        os.remove(fp_existing_plans_and_devices)
        assert not os.path.isfile(fp_existing_plans_and_devices)
    elif file_state == "empty":
        with open(fp_existing_plans_and_devices, "w"):
            pass
        assert os.path.getsize(fp_existing_plans_and_devices) == 0
    elif file_state == "corrupt":
        # Invalid YAML
        with open(fp_existing_plans_and_devices, "w") as f:
            f.writelines(["- Item 1\n- Item 2\nItem3"])
    else:
        assert False, f"Unknown option '{file_state}'"

    changes_detected = update_existing_plans_and_devices(
        path_to_file=fp_existing_plans_and_devices,
        existing_plans=existing_plans,
        existing_devices=existing_devices,
    )

    existing_plans2, existing_devices2 = load_existing_plans_and_devices(fp_existing_plans_and_devices)
    assert existing_plans2 == existing_plans
    assert existing_devices2 == existing_devices

    assert changes_detected is True


# fmt: off
@pytest.mark.parametrize("load_ref_from_file", [True, False])
# fmt: on
def test_update_existing_plans_and_devices_4(load_ref_from_file):
    """
    Test that the files in the built-in profile collection directory (which is part of the repository and
    the distributed package) are never modified.
    """
    pc_path = get_default_startup_dir()
    fp_existing_plans_and_devices = os.path.join(pc_path, "existing_plans_and_devices.yaml")
    t1 = os.path.getctime(fp_existing_plans_and_devices)
    ttime.sleep(0.005)

    existing_plans, existing_devices = load_existing_plans_and_devices(fp_existing_plans_and_devices)
    existing_plans_modified = copy.deepcopy(existing_plans)
    existing_devices_modified = copy.deepcopy(existing_devices)
    del existing_plans_modified["count"]
    del existing_devices_modified["det"]

    if load_ref_from_file:
        changes_detected = update_existing_plans_and_devices(
            path_to_file=fp_existing_plans_and_devices,
            existing_plans=existing_plans_modified,
            existing_devices=existing_devices_modified,
        )
    else:
        changes_detected = update_existing_plans_and_devices(
            path_to_file=fp_existing_plans_and_devices,
            existing_plans=existing_plans_modified,
            existing_devices=existing_devices_modified,
            existing_devices_ref=copy.deepcopy(existing_devices),
            existing_plans_ref=copy.deepcopy(existing_plans),
        )

    t2 = os.path.getctime(fp_existing_plans_and_devices)
    assert t1 == t2, "The file was modified"

    existing_plans2, existing_devices2 = load_existing_plans_and_devices(fp_existing_plans_and_devices)
    assert existing_plans2 == existing_plans
    assert existing_devices2 == existing_devices

    assert changes_detected is True


def test_verify_default_profile_collection():
    """
    Verify if the list of existing plans and devices matches current default profile collection.
    This test may fail if the the algorithm for generating the lists, the set of built-in
    bluesky plans or simulated Ophyd devices was changed. Generate the new list to fix the
    issue.
    """
    # Create dictionaries of existing plans and devices. Apply all preprocessing steps.
    pc_path = get_default_startup_dir()
    nspace = load_profile_collection(pc_path)

    devices = devices_from_nspace(nspace)
    devices = _prepare_devices(devices)

    plans = plans_from_nspace(nspace)
    plans = _prepare_plans(plans, existing_devices=devices)

    # Read the list of the existing plans of devices
    file_path = os.path.join(pc_path, "existing_plans_and_devices.yaml")
    existing_plans, existing_devices = load_existing_plans_and_devices(file_path)

    # Compare
    assert set(plans.keys()) == set(existing_plans.keys())
    assert set(devices) == set(existing_devices)

    # The list of plans can be large, so it is better to compare the contents plan by plan.
    #   If there is a mismatch, the printed difference is too large to be useful.
    assert len(plans) == len(existing_plans)
    for key in plans.keys():
        if python_version < (3, 9) and key == "marked_up_count":
            # The test will fail for Python 3.8 and earlier because of difference in representation
            #   of 'typing.Union[float, NoneType]' and 'typing.Optional[float]'. Python 3.8 and
            #   earlier converts 'typing.Optional[float]' to 'typing.Union[float, NoneType]' and
            #   Python 3.9 is using 'typing.Optional[float]'. The condition may be removed
            #   once there is no need to support Python 3.8 and earlier.
            # NOTE: this limitation only affects this test, not functionality.
            # TODO: remove this condition once Python 3.8 is not supported.
            continue
        assert plans[key] == existing_plans[key]

    assert devices == existing_devices


_user_groups_text = r"""user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - null  # Allow all
    forbidden_devices:
      - null  # Nothing is forbidden
    allowed_functions:
      - null  # Allow all
    forbidden_functions:
      - null  # Nothing is forbidden
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ".*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ".*"  # A different way to allow all
    forbidden_devices:
      - null  # Nothing is forbidden
    allowed_functions:
      - ".*"  # A different way to allow all
    forbidden_functions:
      - null  # Nothing is forbidden
  test_user:  # Users with limited access capabilities
    allowed_plans:
      - "^count$"  # Use regular expression patterns
      - "scan$"
    forbidden_plans:
      - "^adaptive_scan$" # Use regular expression patterns
      - "^inner_product"
    allowed_devices:
      - "^det"  # Use regular expression patterns
      - "^motor"
    forbidden_devices:
      - "^det[3-5]$" # Use regular expression patterns
      - "^motor\\d+$"
  test_user_1:
    allowed_plans:
      - "^count$"  # Use regular expression patterns
      - "scan$"
    allowed_devices:
      - "^det"  # Use regular expression patterns
      - "^motor"
    allowed_functions:
      - ".*"  # A different way to allow all
"""

_user_groups_dict = {
    "user_groups": {
        "root": {
            "allowed_plans": [None],
            "forbidden_plans": [None],
            "allowed_devices": [None],
            "forbidden_devices": [None],
            "allowed_functions": [None],
            "forbidden_functions": [None],
        },
        "admin": {
            "allowed_plans": [".*"],
            "forbidden_plans": [None],
            "allowed_devices": [".*"],
            "forbidden_devices": [None],
            "allowed_functions": [".*"],
            "forbidden_functions": [None],
        },
        "test_user": {
            "allowed_plans": ["^count$", "scan$"],
            "forbidden_plans": ["^adaptive_scan$", "^inner_product"],
            "allowed_devices": ["^det", "^motor"],
            "forbidden_devices": ["^det[3-5]$", r"^motor\d+$"],
        },
        "test_user_1": {
            "allowed_plans": ["^count$", "scan$"],
            "allowed_devices": ["^det", "^motor"],
            "allowed_functions": [".*"],
        },
    }
}


def test_load_user_group_permissions_1(tmp_path):
    """
    Create YAML file (with comments), load it and compare with the expected results.
    """
    path_to_file = os.path.join(tmp_path, "some_dir")
    os.makedirs(path_to_file, exist_ok=True)
    path_to_file = os.path.join(path_to_file, "user_permissions.yaml")
    with open(path_to_file, "w") as f:
        f.writelines(_user_groups_text)

    user_group_permissions = load_user_group_permissions(path_to_file)
    assert user_group_permissions == _user_groups_dict


def test_load_user_group_permissions_2_fail(tmp_path):
    """
    Function ``load_user_group_permissions``. Failed schema validation.
    """
    path_to_file = os.path.join(tmp_path, "some_dir")
    os.makedirs(path_to_file, exist_ok=True)
    path_to_file = os.path.join(path_to_file, "user_permissions.yaml")

    with pytest.raises(IOError, match=f"File '{path_to_file}' does not exist"):
        load_user_group_permissions(path_to_file)


def test_load_user_group_permissions_3_fail(tmp_path):
    """
    Function ``load_user_group_permissions``. Failed schema validation.
    """
    path_to_file = os.path.join(tmp_path, "some_dir")
    os.makedirs(path_to_file, exist_ok=True)
    path_to_file = os.path.join(path_to_file, "user_permissions.yaml")

    ug_dict = copy.deepcopy(_user_groups_dict)
    ug_dict["user_groups"]["test_user"]["something"] = ["a", "b"]

    with open(path_to_file, "w") as f:
        yaml.dump(ug_dict, f)

    with pytest.raises(IOError, match="Additional properties are not allowed"):
        load_user_group_permissions(path_to_file)


def test_load_user_group_permissions_4_fail(tmp_path):
    """
    Function ``load_user_group_permissions``. Failed schema validation.
    """
    path_to_file = os.path.join(tmp_path, "some_dir")
    os.makedirs(path_to_file, exist_ok=True)
    path_to_file = os.path.join(path_to_file, "user_permissions.yaml")

    ug_dict = copy.deepcopy(_user_groups_dict)
    ug_dict["unknown_key"] = ["a", "b"]

    with open(path_to_file, "w") as f:
        yaml.dump(ug_dict, f)

    with pytest.raises(IOError, match="Additional properties are not allowed"):
        load_user_group_permissions(path_to_file)


@pytest.mark.parametrize("group_to_delete", ["root"])
def test_load_user_group_permissions_5_fail(tmp_path, group_to_delete):
    """
    Function ``load_user_group_permissions``. Failed schema validation.
    """
    path_to_file = os.path.join(tmp_path, "some_dir")
    os.makedirs(path_to_file, exist_ok=True)
    path_to_file = os.path.join(path_to_file, "user_permissions.yaml")

    ug_dict = copy.deepcopy(_user_groups_dict)
    ug_dict["user_groups"].pop(group_to_delete)

    with open(path_to_file, "w") as f:
        yaml.dump(ug_dict, f)

    with pytest.raises(IOError, match="Missing required user group"):
        load_user_group_permissions(path_to_file)


def test_load_user_group_permissions_6_fail(tmp_path):
    """
    Function ``load_user_group_permissions``. Failed schema validation.
    """
    path_to_file = os.path.join(tmp_path, "some_dir")
    os.makedirs(path_to_file, exist_ok=True)
    path_to_file = os.path.join(path_to_file, "user_permissions.yaml")

    ug_dict = copy.deepcopy(_user_groups_dict)
    ug_dict["user_groups"]["test_user"]["allowed_plans"].append(50)

    with open(path_to_file, "w") as f:
        yaml.dump(ug_dict, f)

    with pytest.raises(IOError, match="is not of type 'string'"):
        load_user_group_permissions(path_to_file)


# fmt: off
@pytest.mark.parametrize("item_dict, allow_patterns, disallow_patterns, result", [
    ({"abc34": 1, "abcd": 2}, [r":^abc"], [r":^abc\d+$"], {"abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [r":^abc"], [r":^abc.*$"], {}),
    ({"abc34": 1, "abcd": 2}, [r":^abc"], [r":^abcde$", r":^abc.*$"], {}),
    ({"abc34": 1, "abcd": 2}, [r":^abc"], [r":^abcde$", r":^a.2$"], {"abc34": 1, "abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [r":d$", r":4$"], [r":^abcde$", r":^a.2$"], {"abc34": 1, "abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [None], [r":^abc\d+$"], {"abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [r":^abc"], [None], {"abc34": 1, "abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [None], [None], {"abc34": 1, "abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [], [None], {}),
    ({"abc34": 1, "abcd": 2}, [None], [], {"abc34": 1, "abcd": 2}),
    ({}, [r":^abc"], [r":^abc\d+$"], {}),
])
# fmt: on
def test_select_allowed_plans_1(item_dict, allow_patterns, disallow_patterns, result):
    """
    Tests for ``_select_allowed_plans``.
    """
    r = _select_allowed_plans(item_dict, allow_patterns, disallow_patterns)
    assert r == result


# fmt: off
@pytest.mark.parametrize("pass_data_as_parameters", [False, True])
@pytest.mark.parametrize("fln_existing_items, fln_user_groups, empty_dicts, all_users", [
    ("existing_plans_and_devices.yaml", "user_group_permissions.yaml", False, True),
    ("existing_plans_and_devices.yaml", None, False, False),
    (None, "user_group_permissions.yaml", True, True),
    (None, None, True, False),
])
# fmt: on
def test_load_allowed_plans_and_devices_1(
    fln_existing_items, fln_user_groups, empty_dicts, all_users, pass_data_as_parameters
):
    """
    Basic test for ``load_allowed_plans_and_devices``.
    """
    pc_path = get_default_startup_dir()

    fln_existing_items = None if (fln_existing_items is None) else os.path.join(pc_path, fln_existing_items)
    fln_user_groups = None if (fln_user_groups is None) else os.path.join(pc_path, fln_user_groups)

    if pass_data_as_parameters:
        existing_plans, existing_devices = load_existing_plans_and_devices(fln_existing_items)
        user_group_permissions = load_user_group_permissions(fln_user_groups)

        allowed_plans, allowed_devices = load_allowed_plans_and_devices(
            existing_plans=existing_plans,
            existing_devices=existing_devices,
            user_group_permissions=user_group_permissions,
        )
    else:
        allowed_plans, allowed_devices = load_allowed_plans_and_devices(
            path_existing_plans_and_devices=fln_existing_items, path_user_group_permissions=fln_user_groups
        )

    def check_dict(d):
        if empty_dicts:
            assert d == {}
        else:
            assert isinstance(d, dict)
            assert d

    assert "root" in allowed_plans
    assert "root" in allowed_devices
    check_dict(allowed_plans["root"])
    check_dict(allowed_devices["root"])

    if all_users:
        assert "admin" in allowed_plans
        assert "admin" in allowed_devices
        check_dict(allowed_plans["admin"])
        check_dict(allowed_devices["admin"])
        assert "test_user" in allowed_plans
        assert "test_user" in allowed_devices
        check_dict(allowed_plans["test_user"])
        check_dict(allowed_devices["test_user"])
    else:
        assert "admin" not in allowed_plans
        assert "admin" not in allowed_devices
        assert "test_user" not in allowed_plans
        assert "test_user" not in allowed_devices


_patch_junk_plan_and_device = """

from ophyd import Device
from bluesky_queueserver.manager.annotation_decorator import parameter_annotation_decorator

class JunkDevice(Device):
    ...

junk_device = JunkDevice('ABC', name='stage')

def junk_plan():
    yield None


@parameter_annotation_decorator({
    "parameters":{
        "device_param": {
            "annotation": "Devices",
            "devices": {"Devices": ("det1", "det2", "junk_device")
            }
        },
        "plan_param": {
            "annotation": "Plans",
            "plans": {"Plans": ("count", "scan", "junk_plan")
            }
        }
    }
})
def plan_check_filtering(device_param, plan_param):
    yield None
"""


_user_permissions_clear = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - null  # Allow all
    forbidden_devices:
      - null  # Nothing is forbidden
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ":.*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ":?.*"  # A different way to allow all
    forbidden_devices:
      - null  # Nothing is forbidden
"""

_user_permissions_excluding_junk1 = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Allow all
    forbidden_plans:
      - ":^junk_plan$"
    allowed_devices:
      - null  # Allow all
    forbidden_devices:
      - ":+^junk_device$:?.*"
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ":.*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ":?.*"  # A different way to allow all
    forbidden_devices:
      - null  # Nothing is forbidden
"""

_user_permissions_excluding_junk2 = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - ":^(?!.*junk)"  # Allow all plans that don't contain 'junk' in their names
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ":?^(?!.*junk)"  # Allow all devices that don't contain 'junk' in their names
    forbidden_devices:
      - null  # Nothing is forbidden
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ":.*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ":?.*"  # A different way to allow all
    forbidden_devices:
      - null  # Nothing is forbidden
"""

# Restrictions only on 'admin'
_user_permissions_excluding_junk3 = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - ":.*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ":?.*"  # A different way to allow all
    forbidden_devices:
      - null  # Nothing is forbidden
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ":^(?!.*junk)"  # Allow all plans that don't contain 'junk' in their names
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ":+^(?!.*junk):?.*"  # Allow all devices that don't contain 'junk' in their names
    forbidden_devices:
      - null  # Nothing is forbidden
"""


# fmt: off
@pytest.mark.parametrize("pass_permissions_as_parameter", [False, True])
@pytest.mark.parametrize("permissions_str, items_are_removed, only_admin", [
    (_user_permissions_clear, False, False),
    (_user_permissions_excluding_junk1, True, False),
    (_user_permissions_excluding_junk2, True, False),
    (_user_permissions_excluding_junk3, True, True),
])
# fmt: on
def test_load_allowed_plans_and_devices_2(
    tmp_path, permissions_str, items_are_removed, only_admin, pass_permissions_as_parameter
):
    """
    Tests if filtering settings for the "root" group are also applied to other groups.
    The purpose of the "root" group is to filter junk from the list of existing devices and
    plans. Additionally check if the plans and devices were removed from the parameter
    descriptions.

    Parameter 'only_admin' - permissions are applied only to the 'admin' user
    """
    pc_path = copy_default_profile_collection(tmp_path)
    create_local_imports_dirs(pc_path)
    patch_first_startup_file(pc_path, _patch_junk_plan_and_device)

    # Generate list of plans and devices for the patched profile collection
    gen_list_of_plans_and_devices(startup_dir=pc_path, file_dir=pc_path, overwrite=True)

    permissions_fln = os.path.join(pc_path, "user_group_permissions.yaml")
    with open(permissions_fln, "w") as f:
        f.write(permissions_str)

    plans_and_devices_fln = os.path.join(pc_path, "existing_plans_and_devices.yaml")

    if pass_permissions_as_parameter:
        user_group_permissions = load_user_group_permissions(permissions_fln)
        allowed_plans, allowed_devices = load_allowed_plans_and_devices(
            path_existing_plans_and_devices=plans_and_devices_fln,
            user_group_permissions=user_group_permissions,
        )
    else:
        allowed_plans, allowed_devices = load_allowed_plans_and_devices(
            path_existing_plans_and_devices=plans_and_devices_fln,
            path_user_group_permissions=permissions_fln,
        )

    if items_are_removed:
        if only_admin:
            assert "junk_device" in allowed_devices["root"]
        else:
            assert "junk_device" not in allowed_devices["root"]
        assert "junk_device" not in allowed_devices["admin"]
        if only_admin:
            assert "junk_plan" in allowed_plans["root"]
        else:
            assert "junk_plan" not in allowed_plans["root"]
        assert "junk_plan" not in allowed_plans["admin"]
    else:
        assert "junk_device" in allowed_devices["root"]
        assert "junk_device" in allowed_devices["admin"]
        assert "junk_plan" in allowed_plans["root"]
        assert "junk_plan" in allowed_plans["admin"]

    # Make sure that the 'junk' devices are removed from the description of 'plan_check_filtering'
    # for user in ("root", "admin"):
    for user in ("admin",):
        params = allowed_plans[user]["plan_check_filtering"]["parameters"]
        devs = params[0]["annotation"]["devices"]["Devices"]
        plans = params[1]["annotation"]["plans"]["Plans"]
        test_case = f"only_admin = {only_admin} user = '{user}'"
        if items_are_removed and not (only_admin and user == "root"):
            assert devs == ["det1", "det2"], test_case
            assert plans == ["count", "scan"], test_case
        else:
            assert devs == ["det1", "det2", "junk_device"], test_case
            assert plans == ["count", "junk_plan", "scan"], test_case


_user_permissions_incomplete_1 = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - null  # Allow all
    forbidden_devices:
      - null  # Nothing is forbidden
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ":?.*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ":?.*"  # A different way to allow all
    forbidden_devices:
      - null  # Nothing is forbidden
"""

_user_permissions_incomplete_2 = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - null  # Allow all
    forbidden_devices:
      - null  # Nothing is forbidden
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ":?.*"  # A different way to allow all
    allowed_devices:
      - ":?.*"  # A different way to allow all
"""

_user_permissions_incomplete_3 = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - null  # Allow all
    forbidden_devices:
      - null  # Nothing is forbidden
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    forbidden_plans:
      - null  # Nothing is forbidden
    forbidden_devices:
      - null  # Nothing is forbidden
"""

_user_permissions_incomplete_4 = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Allow all
    allowed_devices:
      - null  # Allow all
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ":?.*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ":?.*"  # A different way to allow all
    forbidden_devices:
      - null  # Nothing is forbidden
"""

_user_permissions_incomplete_5 = """user_groups:
  root:  # The group includes all available plan and devices
    forbidden_plans:
      - null  # Nothing is forbidden
    forbidden_devices:
      - null  # Nothing is forbidden
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ":?.*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ":?.*"  # A different way to allow all
    forbidden_devices:
      - null  # Nothing is forbidden
"""


# fmt: off
@pytest.mark.parametrize("pass_permissions_as_parameter", [False, True])
@pytest.mark.parametrize("permissions_str, root_empty, admin_empty", [
    (_user_permissions_incomplete_1, False, False),
    (_user_permissions_incomplete_2, False, False),
    (_user_permissions_incomplete_3, False, True),
    (_user_permissions_incomplete_4, False, False),
    (_user_permissions_incomplete_5, True, True),
])
# fmt: on
def test_load_allowed_plans_and_devices_3(
    tmp_path, permissions_str, root_empty, admin_empty, pass_permissions_as_parameter
):
    """
    Tests if filtering settings for the "root" group are also applied to other groups.
    The purpose of the "root" group is to filter junk from the list of existing devices and
    plans. Additionally check if the plans and devices were removed from the parameter
    descriptions.

    Parameter 'only_admin' - permissions are applied only to the 'admin' user
    """
    pc_path = copy_default_profile_collection(tmp_path)
    create_local_imports_dirs(pc_path)
    patch_first_startup_file(pc_path, _patch_junk_plan_and_device)

    # Generate list of plans and devices for the patched profile collection
    gen_list_of_plans_and_devices(startup_dir=pc_path, file_dir=pc_path, overwrite=True)

    permissions_fln = os.path.join(pc_path, "user_group_permissions.yaml")
    with open(permissions_fln, "w") as f:
        f.write(permissions_str)

    plans_and_devices_fln = os.path.join(pc_path, "existing_plans_and_devices.yaml")

    if pass_permissions_as_parameter:
        user_group_permissions = load_user_group_permissions(permissions_fln)
        allowed_plans, allowed_devices = load_allowed_plans_and_devices(
            path_existing_plans_and_devices=plans_and_devices_fln,
            user_group_permissions=user_group_permissions,
        )
    else:
        allowed_plans, allowed_devices = load_allowed_plans_and_devices(
            path_existing_plans_and_devices=plans_and_devices_fln,
            path_user_group_permissions=permissions_fln,
        )

    assert len(allowed_plans) == 2
    assert len(allowed_devices) == 2

    assert isinstance(allowed_plans["root"], dict)
    assert isinstance(allowed_devices["root"], dict)
    assert isinstance(allowed_plans["admin"], dict)
    assert isinstance(allowed_devices["admin"], dict)

    if root_empty:
        assert len(allowed_plans["root"]) == 0
        assert len(allowed_devices["root"]) == 0
    else:
        assert len(allowed_plans["root"]) > 0
        assert len(allowed_devices["root"]) > 0

    if admin_empty:
        assert len(allowed_plans["admin"]) == 0
        assert len(allowed_devices["admin"]) == 0
    else:
        assert len(allowed_plans["admin"]) > 0
        assert len(allowed_devices["admin"]) > 0


_patch_plan_with_subdevices = """

class SimStg(Device):
    x = Cpt(ophyd.sim.SynAxis, name="y", labels={"motors"})
    y = Cpt(ophyd.sim.SynAxis, name="y", labels={"motors"})
    z = Cpt(ophyd.sim.SynAxis, name="z", labels={"motors"})

    def set(self, x, y, z):
        # Makes the device Movable
        self.x.set(x)
        self.y.set(y)
        self.z.set(z)


class SimDets(Device):
    # The detectors are controlled by simulated 'motor1' and 'motor2'
    # defined on the global scale.

    det_A = Cpt(
        ophyd.sim.SynGauss,
        name="det_A",
        motor=motor1,
        motor_field="motor1",
        center=0,
        Imax=5,
        sigma=0.5,
        labels={"detectors"},
    )
    det_B = Cpt(
        ophyd.sim.SynGauss,
        name="det_B",
        motor=motor2,
        motor_field="motor2",
        center=0,
        Imax=5,
        sigma=0.5,
        labels={"detectors"},
    )


class SimBundle(ophyd.Device):
    mtrs = Cpt(SimStage, name="stage")
    dets = Cpt(SimDetectors, name="detectors")


stg_A = SimBundle(name="sim_bundle")
stg_B = SimBundle(name="sim_bundle")  # Used for tests

@parameter_annotation_decorator({
    "parameters":{
        "device1": {
            "annotation": "Devices1",
            "devices": {"Devices1": ("stg_A", "stg_A.dets", "stg_B.dets.det_A")}
        },
        "device2": {
            "annotation": "Devices1",
            "devices": {"Devices1": (":-^stg:+^det:A$", ":-.*:-.+mtrs$:y")}
        },
        "device3": {
            "annotation": "Devices1",
            "devices": {"Devices1": (":?tg_A.*z$", ":?tg_B.*z$")}
        },
    }
})
def plan_with_subdevices(device1, device2, device3):
    yield None
"""

_user_permissions_subdevices_1 = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Everything is allowed
    allowed_devices:
      - null  # Everything is allowed
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ":.*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ":?.*"  # A different way to allow all
    forbidden_devices:
      - null  # Nothing is forbidden
"""

_user_permissions_subdevices_2 = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Everything is allowed
    allowed_devices:
      - null  # Everything is allowed
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ":.*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ":g_B$:?.*"  # Allow 'stg_B'
    forbidden_devices:
      - null  # Nothing is forbidden
"""

_user_permissions_subdevices_3 = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Everything is allowed
    allowed_devices:
      - null  # Everything is allowed
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ":.*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ":?.*"  # A different way to allow all
    forbidden_devices:
      - ":g_B$:?.*"  # Block 'stg_B'
"""

_user_permissions_subdevices_4 = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Everything is allowed
    allowed_devices:
      - ":g_B$:?.*"  # Allow 'stg_B'
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ":.*"  # A different way to allow all
    allowed_devices:
       - ":?.*"  # A different way to allow all
"""

_user_permissions_subdevices_5 = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Everything is allowed
    allowed_devices:
      - null  # Everything is allowed
    forbidden_devices:
      - ":g_B$:?.*"  # Block 'stg_B'
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ":.*"  # A different way to allow all
    allowed_devices:
      - ":?.*"  # A different way to allow all
"""


# fmt: off
@pytest.mark.parametrize("pass_permissions_as_parameter", [False, True])
@pytest.mark.parametrize("permissions_str, dev1, dev2, dev3", [
    (_user_permissions_subdevices_1,
     ["stg_A", "stg_A.dets", "stg_B.dets.det_A"],
     ["stg_A.dets", "stg_A.dets.det_A", "stg_B.dets", "stg_B.dets.det_A"],
     ["stg_A.mtrs.z", "stg_B.mtrs.z"]),
    # Filtering of lists for the 'admin' user
    (_user_permissions_subdevices_2,
     ["stg_B.dets.det_A"],
     ["stg_B.dets", "stg_B.dets.det_A"],
     ["stg_B.mtrs.z"]),
    (_user_permissions_subdevices_3,
     ["stg_A", "stg_A.dets"],
     ["stg_A.dets", "stg_A.dets.det_A"],
     ["stg_A.mtrs.z"]),
    # Filtering of lists for the 'root' user
    (_user_permissions_subdevices_4,
     ["stg_B.dets.det_A"],
     ["stg_B.dets", "stg_B.dets.det_A"],
     ["stg_B.mtrs.z"]),
    (_user_permissions_subdevices_5,
     ["stg_A", "stg_A.dets"],
     ["stg_A.dets", "stg_A.dets.det_A"],
     ["stg_A.mtrs.z"]),
])
# fmt: on
def test_load_allowed_plans_and_devices_4(
    tmp_path, permissions_str, dev1, dev2, dev3, pass_permissions_as_parameter
):
    """
    ``load_allowed_plans_and_devices``: test if lists of devices in plan parameters
    are properly generated and filtered in case if they contain subdevices .
    """

    pc_path = copy_default_profile_collection(tmp_path)
    create_local_imports_dirs(pc_path)
    append_code_to_last_startup_file(pc_path, _patch_plan_with_subdevices)

    # Generate list of plans and devices for the patched profile collection
    gen_list_of_plans_and_devices(startup_dir=pc_path, file_dir=pc_path, overwrite=True)

    permissions_fln = os.path.join(pc_path, "user_group_permissions.yaml")
    with open(permissions_fln, "w") as f:
        f.write(permissions_str)

    plans_and_devices_fln = os.path.join(pc_path, "existing_plans_and_devices.yaml")

    if pass_permissions_as_parameter:
        user_group_permissions = load_user_group_permissions(permissions_fln)
        allowed_plans, allowed_devices = load_allowed_plans_and_devices(
            path_existing_plans_and_devices=plans_and_devices_fln,
            user_group_permissions=user_group_permissions,
        )
    else:
        allowed_plans, allowed_devices = load_allowed_plans_and_devices(
            path_existing_plans_and_devices=plans_and_devices_fln,
            path_user_group_permissions=permissions_fln,
        )
    assert "plan_with_subdevices" in allowed_plans["admin"], list(allowed_plans.keys())

    params = allowed_plans["admin"]["plan_with_subdevices"]["parameters"]
    assert params[0]["name"] == "device1"
    assert params[0]["annotation"]["type"] == "Devices1"
    assert params[0]["annotation"]["devices"]["Devices1"] == dev1
    assert params[1]["name"] == "device2"
    assert params[1]["annotation"]["type"] == "Devices1"
    assert params[1]["annotation"]["devices"]["Devices1"] == dev2
    assert params[2]["name"] == "device3"
    assert params[2]["annotation"]["type"] == "Devices1"
    assert params[2]["annotation"]["devices"]["Devices1"] == dev3


# fmt: off
@pytest.mark.parametrize("option", [
    "full_lists",
    "path_none",
    "path_invalid",
    "empty_device_list",
    "empty_plan_list",
    "empty_lists",
])
# fmt: on
def test_load_allowed_plans_and_devices_5(tmp_path, option):
    """
    Basic test for ``load_allowed_plans_and_devices``.
    """

    fln_existing_items, fln_user_groups = "existing_plans_and_devices.yaml", "user_group_permissions.yaml"

    # Load the list of allowed plans and devices from standard location
    pc_path = get_default_startup_dir()

    fln_existing_items = None if (fln_existing_items is None) else os.path.join(pc_path, fln_existing_items)
    fln_user_groups = None if (fln_user_groups is None) else os.path.join(pc_path, fln_user_groups)

    allowed_plans, allowed_devices = load_allowed_plans_and_devices(
        path_existing_plans_and_devices=fln_existing_items, path_user_group_permissions=fln_user_groups
    )

    # Load the list of existing plans and devices from standard location
    existing_plans, existing_devices = load_existing_plans_and_devices(fln_existing_items)

    # The following tests make sure that 'load_allowed_plans_and_devices' works with all possible
    #   values of parameter 'path_existing_plans_and_devices', which is ignored if both
    #   'existing_plans' and 'existing_devices' are not None.
    if option == "full_lists":
        fln_existing_items2 = fln_existing_items
        existing_plans2, existing_devices2 = existing_plans, existing_devices
    elif option == "path_none":
        fln_existing_items2 = None
        existing_plans2, existing_devices2 = existing_plans, existing_devices
    elif option == "path_invalid":
        fln_existing_items2 = tmp_path
        existing_plans2, existing_devices2 = existing_plans, existing_devices
    # The following tests verify that if 'path_existing_plans_and_devices',
    #   'existing_plans' and 'existing_devices' are specified, the lists of
    #   existing plans and devices that are passed as parameter are used
    elif option == "empty_device_list":
        fln_existing_items2 = fln_existing_items
        existing_plans2, existing_devices2 = existing_plans, {}
    elif option == "empty_plan_list":
        fln_existing_items2 = fln_existing_items
        existing_plans2, existing_devices2 = {}, existing_devices
    elif option == "empty_lists":
        fln_existing_items2 = fln_existing_items
        existing_plans2, existing_devices2 = {}, {}
    else:
        assert False, f"Unknown option '{option}'"

    allowed_plans2, allowed_devices2 = load_allowed_plans_and_devices(
        path_existing_plans_and_devices=fln_existing_items2,
        path_user_group_permissions=fln_user_groups,
        existing_plans=existing_plans2,
        existing_devices=existing_devices2,
    )

    def check_dict(d, is_empty):
        if is_empty:
            assert d == {}
        else:
            assert isinstance(d, dict)
            assert d

    def check_all_user_dicts(allowed, is_empty):
        assert "admin" in allowed
        check_dict(allowed["admin"], is_empty)
        assert "test_user" in allowed
        check_dict(allowed["test_user"], is_empty)

    if option in ("full_lists", "path_none", "path_invalid"):
        assert allowed_plans2 == allowed_plans
        assert allowed_devices2 == allowed_devices
    elif option == "empty_device_list":
        check_all_user_dicts(allowed_plans2, is_empty=False)
        check_all_user_dicts(allowed_devices2, is_empty=True)
    elif option == "empty_plan_list":
        check_all_user_dicts(allowed_plans2, is_empty=True)
        check_all_user_dicts(allowed_devices2, is_empty=False)
    elif option == "empty_lists":
        check_all_user_dicts(allowed_plans2, is_empty=True)
        check_all_user_dicts(allowed_devices2, is_empty=True)
    else:
        assert False, f"Unknown option '{option}'"


_func_permissions_dict_1 = {
    "user_groups": {
        "root": {"allowed_functions": [None], "forbidden_functions": [None]},
        "admin": {"allowed_functions": [None], "forbidden_functions": [None]},
    }
}

_func_permissions_dict_2 = {
    "user_groups": {
        "root": {"allowed_functions": [], "forbidden_functions": [None]},
        "admin": {"allowed_functions": [None], "forbidden_functions": [None]},
    }
}

_func_permissions_dict_3 = {
    "user_groups": {
        "root": {"allowed_functions": [None], "forbidden_functions": [None]},
        "admin": {"allowed_functions": [], "forbidden_functions": []},
    }
}

_func_permissions_dict_4 = {
    "user_groups": {
        "root": {"allowed_functions": [None], "forbidden_functions": [None]},
        "admin": {},
    }
}

_func_permissions_dict_5 = {
    "user_groups": {
        "root": {"allowed_functions": [None]},
        "admin": {"allowed_functions": [None], "forbidden_functions": [":.*"]},
    }
}

_func_permissions_dict_6 = {
    "user_groups": {
        "root": {"allowed_functions": [None]},
        "admin": {"allowed_functions": [":^tmp", ":^test"], "forbidden_functions": [":end$"]},
    }
}

_func_permissions_dict_7 = {
    "user_groups": {
        "root": {"allowed_functions": [None]},
        "admin": {"allowed_functions": [":^tmp", ":^test"], "forbidden_functions": [":^test"]},
    }
}


# fmt: off
@pytest.mark.parametrize("permissions_dict, func_name, group, accepted", [
    (_func_permissions_dict_1, "test_func", "root", True),
    (_func_permissions_dict_1, "test_func", "admin", True),
    (_func_permissions_dict_2, "test_func", "root", False),
    (_func_permissions_dict_2, "test_func", "admin", False),
    (_func_permissions_dict_3, "test_func", "root", True),
    (_func_permissions_dict_3, "test_func", "admin", False),
    (_func_permissions_dict_4, "test_func", "admin", False),
    (_func_permissions_dict_5, "test_func", "admin", False),
    (_func_permissions_dict_6, "test_func", "admin", True),
    (_func_permissions_dict_7, "test_func", "admin", False),
])
# fmt: on
def test_check_if_function_allowed_1(permissions_dict, func_name, group, accepted):
    _validate_user_group_permissions_schema(permissions_dict)
    is_accepted = check_if_function_allowed(func_name, group_name=group, user_group_permissions=permissions_dict)
    assert is_accepted == accepted


_func_permissions_dict_fail_1 = {
    "root": {"allowed_functions": [None]},
}

_func_permissions_dict_fail_2 = {
    "user_groups": {
        "root": {"allowed_functions": [None]},
    }
}

_func_permissions_dict_fail_3 = {
    "user_groups": {
        "admin": {"allowed_functions": [None]},
    }
}


# fmt: off
@pytest.mark.parametrize("permissions_dict, except_type, msg", [
    (_func_permissions_dict_fail_1, KeyError, "does not contain 'user_groups' key"),
    (_func_permissions_dict_fail_2, KeyError, "No permissions are defined for user group 'admin'"),
    (_func_permissions_dict_fail_3, KeyError, "No permissions are defined for user group 'root'"),
])
# fmt: on
def test_check_if_function_allowed_2(permissions_dict, except_type, msg):
    with pytest.raises(except_type, match=msg):
        check_if_function_allowed("some_name", group_name="admin", user_group_permissions=permissions_dict)


# fmt: off
@pytest.mark.parametrize("params, param_list, success, msg", [
    # Range with both boundaries
    ({"a": 10}, [{"name": "a", "min": "5", "max": "15"}], True, ""),
    ({"a": 1}, [{"name": "a", "min": "5", "max": "15"}], False, "Value 1 is out of range [5, 15]"),
    ({"a": 17}, [{"name": "a", "min": "5", "max": "15"}], False, "Value 17 is out of range [5, 15]"),
    ({"a": 10.7}, [{"name": "a", "min": "5.5", "max": "14.3"}], True, ""),
    ({"a": 1.2}, [{"name": "a", "min": "5.5", "max": "14.3"}], False, "Value 1.2 is out of range [5.5, 14.3]"),
    ({"a": 16.34}, [{"name": "a", "min": "5.5", "max": "14.3"}], False, "Value 16.34 is out of range [5.5, 14.3]"),
    ({"a": [10, 11, 12]}, [{"name": "a", "min": "5", "max": "15"}], True, ""),
    ({"a": [0, 11, 12]}, [{"name": "a", "min": "5", "max": "15"}], False, "Value 0 is out of range [5, 15]"),
    ({"a": [5, 11, 19]}, [{"name": "a", "min": "5", "max": "15"}], False, "Value 19 is out of range [5, 15]"),
    ({"a": [0, 11, 19]}, [{"name": "a", "min": "5", "max": "15"}], False, "Value 0 is out of range [5, 15]"),
    ({"a": {"v1": 10, "v2": 11}}, [{"name": "a", "min": "5", "max": "15"}], True, ""),
    ({"a": {"v1": 1, "v2": 11}}, [{"name": "a", "min": "5", "max": "15"}], False,
     "Value 1 is out of range [5, 15]"),
    ({"a": {"v1": 10, "v2": 19}}, [{"name": "a", "min": "5", "max": "15"}], False,
     "Value 19 is out of range [5, 15]"),
    ({"a": [10, "90", [50, 4]]}, [{"name": "a", "min": "5", "max": "15"}], False,
     "Value 50 is out of range [5, 15]"),
    ({"a": [10, "90", {"v": 50}]}, [{"name": "a", "min": "5", "max": "15"}], False,
     "Value 50 is out of range [5, 15]"),
    ({"a": {"v1": "abc", "v2": ["ab", 10, 19]}}, [{"name": "a", "min": "5", "max": "15"}],
     False, "Value 19 is out of range [5, 15]"),

    # Multiple parameters
    ({"a": 10, "b": 100}, [{"name": "a", "min": "5", "max": "15"}, {"name": "b", "min": "95.0", "max": "120.3"}],
     True, ""),
    ({"a": 3, "b": 150}, [{"name": "a", "min": "5", "max": "15"}, {"name": "b", "min": "95.0", "max": "120.3"}],
     False, "Parameter value is out of range: key='a', value='3': Value 3 is out of range [5, 15]\n"
            "Parameter value is out of range: key='b', value='150': Value 150 is out of range [95, 120.3]"),

    # Single-sided range
    ({"a": 10}, [{"name": "a", "min": "5"}], True, ""),
    ({"a": 1}, [{"name": "a", "min": "5"}], False, "Value 1 is out of range [5, inf]"),
    ({"a": 10}, [{"name": "a", "max": "15"}], True, ""),
    ({"a": 16}, [{"name": "a", "max": "15"}], False, "Value 16 is out of range [-inf, 15]"),

    # No range is specified
    ({"a": 10}, [{"name": "a"}], True, ""),
])
# fmt: on
def test_check_ranges(params, param_list, success, msg):
    """
    Basic test for ``_check_ranges`` function.
    """
    success_out, msg_out = _check_ranges(params, param_list)
    assert success_out == success, msg_out
    if success:
        assert msg == ""
    else:
        assert msg in msg_out


def _f1(a, b, c):
    pass


def _f2(*args, **kwargs):
    pass


def _f3(a, b, *args, c, d):
    pass


def _f4(a, b, *args, c, d=4):
    pass


def _f5(a, b=5, *args, c, d=4):
    pass


# fmt: off
@pytest.mark.parametrize("func, plan, success, errmsg", [
    (_f1, {"name": "nonexistent", "args": [1, 4, 5], "kwargs": {}}, False,
     "Plan 'nonexistent' is not in the list of allowed plans"),

    (_f1, {"name": "existing", "args": [1, 4, 5], "kwargs": {}}, True, ""),
    (_f1, {"name": "existing", "args": [1, 4], "kwargs": {"c": 5}}, True, ""),
    (_f1, {"name": "existing", "args": [], "kwargs": {"a": 1, "b": 4, "c": 5}}, True, ""),
    (_f1, {"name": "existing", "args": [], "kwargs": {"c": 1, "b": 4, "a": 5}}, True, ""),
    (_f1, {"name": "existing", "args": [1, 4], "kwargs": {}}, False,
     "Plan validation failed: missing a required argument: 'c'"),
    (_f1, {"name": "existing", "args": [], "kwargs": {}}, False,
     "Plan validation failed: missing a required argument: 'a'"),
    (_f1, {"name": "existing", "args": [1, 4, 6, 7], "kwargs": {}}, False,
     "Plan validation failed: too many positional arguments"),
    (_f1, {"name": "existing", "args": [1, 4, 6, 7], "kwargs": {"kw": 10}}, False,
     "Plan validation failed: too many positional arguments"),
    (_f1, {"name": "existing", "args": [1, 4], "kwargs": {"b": 10}}, False,
     "Plan validation failed: multiple values for argument 'b'"),

    (_f2, {"name": "existing", "args": [1, 4, 5], "kwargs": {}}, True, ""),
    (_f2, {"name": "existing", "args": [], "kwargs": {"a": 1, "b": 4, "c": 5}}, True, ""),
    (_f2, {"name": "existing", "args": [1, 4, 5], "kwargs": {"a": 1, "b": 4, "c": 5}}, True, ""),

    (_f2, {"name": "existing", "args": [1, 4, 5]}, True, ""),
    (_f2, {"name": "existing", "kwargs": {"a": 1, "b": 4, "c": 5}}, True, ""),

    (_f3, {"name": "existing", "args": [1, 4], "kwargs": {"c": 5, "d": 10}}, True, ""),
    (_f3, {"name": "existing", "args": [], "kwargs": {"a": 1, "b": 4, "c": 5, "d": 10}}, True, ""),
    (_f3, {"name": "existing", "args": [], "kwargs": {"a": 1, "b": 4, "d": 10}}, False,
     "Plan validation failed: missing a required argument: 'c'"),
    (_f3, {"name": "existing", "args": [6, 8], "kwargs": {"a": 1, "c": 4, "d": 10}}, False,
     "Plan validation failed: multiple values for argument 'a'"),

    (_f4, {"name": "existing", "args": [1, 4], "kwargs": {"c": 5, "d": 10}}, True, ""),
    (_f4, {"name": "existing", "args": [1, 4], "kwargs": {"c": 5}}, True, ""),
    (_f4, {"name": "existing", "args": [1, 4], "kwargs": {"d": 10}}, False,
     "Plan validation failed: missing a required argument: 'c'"),

    (_f5, {"name": "existing", "args": [1, 4], "kwargs": {"c": 5, "d": 10}}, True, ""),
    (_f5, {"name": "existing", "args": [1], "kwargs": {"c": 5, "d": 10}}, True, ""),
    (_f5, {"name": "existing", "args": [], "kwargs": {"c": 5, "d": 10}}, False,
     "Plan validation failed: missing a required argument: 'a"),

])
# fmt: on
def test_validate_plan_1(func, plan, success, errmsg):
    """
    Tests for the plan validation algorithm.
    """
    allowed_plans = {"existing": _process_plan(func, existing_devices={}, existing_plans={})}
    success_out, errmsg_out = validate_plan(plan, allowed_plans=allowed_plans, allowed_devices=None)

    assert success_out == success, f"errmsg: {errmsg_out}"
    if success:
        assert errmsg_out == errmsg
    else:
        assert errmsg in errmsg_out


@pytest.mark.parametrize("allowed_plans, success", [(None, True), ({}, False)])
def test_validate_plan_2(allowed_plans, success):
    """
    At this point all plans are considered valid if there is not list of allowed plans.
    """
    success_out, errmsg_out = validate_plan({}, allowed_plans=allowed_plans, allowed_devices=None)
    assert success_out is success


@parameter_annotation_decorator(
    {
        "description": "Move motors into positions; then count dets.",
        "parameters": {
            "motors": {
                "description": "List of motors to be moved into specified positions before the measurement",
                "annotation": "typing.List[Motors]",
                "devices": {"Motors": ("m1", "m2", "m3")},
            },
            "detectors": {
                "description": "Detectors to use for measurement.",
                "annotation": "typing.Union[typing.List[Detectors1], Detectors2]",
                "devices": {
                    "Detectors1": ("d1", "d2", "d3"),
                    "Detectors2": ("d4", "d5"),
                },
            },
            "positions": {
                "description": "Motor positions.",
            },
            "plans_to_run": {
                "description": "Plan to execute for measurement.",
                "annotation": "typing.Union[typing.List[Plans], Plans]",
                "plans": {"Plans": ("p1", "p3")},
            },
        },
    }
)
def _vp3a(
    motors: typing.List[typing.Any],  # The actual type should be a list of 'ophyd.device.Device'
    detectors: typing.List[typing.Any],  # The actual type should be a list of 'ophyd.device.Device'
    plans_to_run: typing.Union[typing.List[callable], callable],
    positions: typing.Union[typing.List[float], float, None] = 10,  # TYPE IS ACTUALLY USED FOR VALIDATION
) -> typing.Generator[str, None, None]:  # Type should be 'bluesky.utils.Msg', not 'str'
    yield from ["one", "two", "three"]


# fmt: off
@pytest.mark.parametrize("plan_func, plan, allowed_devices, success, errmsg", [
    # Basic use of the function.
    (_vp3a, {"args": [("m1", "m2"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # The same as the previous call, but all parameters are passed as kwargs.
    (_vp3a, {"args": [], "kwargs": {"motors": ("m1", "m2"), "detectors": ("d1", "d2"), "plans_to_run": ("p1",),
                                    "positions": (10.0, 20.0)}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # Positions are int (instead of float). Should be converted to float.
    (_vp3a, {"args": [("m1", "m2"), ("d1", "d2"), ("p1",), (10, 20)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # Position is a single value (part of type description).
    (_vp3a, {"args": [("m1", "m2"), ("d1", "d2"), ("p1",), 10], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # Position is None (part of type description).
    (_vp3a, {"args": [("m1", "m2"), ("d1", "d2"), ("p1",), None], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # Position is not specified (default value is used).
    (_vp3a, {"args": [("m1", "m2"), ("d1", "d2"), ("p1",)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),

    # Use motor that is not listed in the annotation (but exists in the list of allowed devices).
    (_vp3a, {"args": [("m2", "m4"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m2", "m4", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'm2'"),
    # The motor is not in the list of allowed devices.
    (_vp3a, {"args": [("m2", "m3"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m2", "m4", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'm2'"),
    # Both motors are not in the list of allowed devices.
    (_vp3a, {"args": [("m1", "m2"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m4", "m5", "d1", "d2"), False, "value is not a valid enumeration member; permitted:"),
    # Empty list of allowed devices (should be the same result as above).
    (_vp3a, {"args": [("m1", "m2"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     (), False, "value is not a valid enumeration member; permitted:"),
    # Single motor is passed as a scalar (instead of a list element)
    (_vp3a, {"args": ["m2", ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m2", "m4", "d1", "d2"), False, "value is not a valid list"),

    # Pass single detector (allowed).
    (_vp3a, {"args": [("m1", "m2"), "d4", ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2", "d4"), True, ""),
    # Pass single detector from 'Detectors2' group, which is not in the list of allowed devices.
    (_vp3a, {"args": [("m1", "m2"), "d4", ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "value is not a valid enumeration member; permitted:"),
    # Pass single detector from 'Detectors1' group (not allowed).
    (_vp3a, {"args": [("m1", "m2"), "d2", ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2", "d4"), False, " value is not a valid list"),
    # Pass a detector from a group 'Detector2' as a list element.
    (_vp3a, {"args": [("m1", "m2"), ("d4",), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2", "d4"), False, "value is not a valid enumeration member; permitted: 'd1', 'd2'"),

    # Plan 'p3' is not in the list of allowed plans
    (_vp3a, {"args": [("m1", "m2"), ("d1", "d2"), ("p3",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'p1'"),
    # Plan 'p2' is in the list of allowed plans, but not listed in the annotation.
    (_vp3a, {"args": [("m1", "m2"), ("d1", "d2"), ("p2",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'p1'"),
    # Plan 'p2' is in the list of allowed plans, but not listed in the annotation.
    (_vp3a, {"args": [("m1", "m2"), ("d1", "d2"), ("p1", "p2"), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'p1'"),
    # Single plan is passed as a scalar (allowed in the annotation).
    (_vp3a, {"args": [("m1", "m2"), ("d1", "d2"), "p1", (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),

    # Position is passed as a string (validation should fail)
    (_vp3a, {"args": [("m1", "m2"), ("d1", "d2"), ("p1",), ("10.0", 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "Incorrect parameter type"),
    # Int instead of a motor name (validation should fail)
    (_vp3a, {"args": [(0, "m2"), ("d1", "d2"), ("p1",), ("10.0", 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "value is not a valid enumeration member"),
])
# fmt: on
def test_validate_plan_3(plan_func, plan, allowed_devices, success, errmsg):
    """
    Test ``validate_plan`` on a function with more complicated signature and custom annotation.
    Mostly testing verification of types and use of the list of available devices.
    """
    plan["name"] = plan_func.__name__
    allowed_plans = {
        "_vp3a": _process_plan(_vp3a, existing_devices={}, existing_plans={}),
        "p1": {},  # The plan is used only as a parameter value
        "p2": {},  # The plan is used only as a parameter value
    }
    # 'allowed_devices' must be a dictionary
    allowed_devices = {_: {} for _ in allowed_devices}

    success_out, errmsg_out = validate_plan(plan, allowed_plans=allowed_plans, allowed_devices=allowed_devices)

    assert success_out == success, f"errmsg: {errmsg_out}"
    if success:
        assert errmsg_out == errmsg
    else:
        assert errmsg in errmsg_out


@parameter_annotation_decorator(
    {
        "parameters": {
            "num_int": {"min": 5, "max": 15},
            "v_float": {"min": 19.4},
            "v_list": {"max": 96.54},
        },
    }
)
def _vp4a(num_int: int, v_float: float, v_list: typing.List[typing.Any]):
    yield from [num_int, v_float, v_list]


# fmt: off
@pytest.mark.parametrize("plan_func, plan, success, errmsg", [
    (_vp4a, {"args": [10, 50.4, [20, 34.5]], "kwargs": {}}, True, ""),
    (_vp4a, {"args": [4, 50.4, [20, 34.5]], "kwargs": {}}, False, "Value 4 is out of range [5, 15]"),
    (_vp4a, {"args": [10, 4, [20, 34.5]], "kwargs": {}}, False, "Value 4 is out of range [19.4, inf]"),
    (_vp4a, {"args": [10, 50.4, [20, 104]], "kwargs": {}}, False, "Value 104 is out of range [-inf, 96.54]"),
    (_vp4a, {"args": [4, 50.4, [20, 104]], "kwargs": {}}, False, "Value 104 is out of range [-inf, 96.54]"),
])
# fmt: on
def test_validate_plan_4(plan_func, plan, success, errmsg):
    """
    Validation of ranges for numeric parameters
    """
    plan["name"] = plan_func.__name__
    allowed_plans = {
        "_vp4a": _process_plan(_vp4a, existing_devices={}, existing_plans={}),
    }

    success_out, errmsg_out = validate_plan(plan, allowed_plans=allowed_plans, allowed_devices={})

    assert success_out == success, f"errmsg: {errmsg_out}"
    if success:
        assert errmsg_out == errmsg
    else:
        assert errmsg in errmsg_out, pprint.pformat(errmsg_out)


# fmt: off
@pytest.mark.parametrize("func, plan_args, plan_kwargs, plan_bound_params, success, except_type, errmsg", [
    (_f1, [1, 2, 3], {}, {"a": 1, "b": 2, "c": 3}, True, Exception, ""),
    (_f1, (1, 2, 3), {}, {"a": 1, "b": 2, "c": 3}, True, Exception, ""),
    (_f1, [1, 2], {"c": 3}, {"a": 1, "b": 2, "c": 3}, True, Exception, ""),
    (_f1, [1], {"c": 3, "b": 2}, {"a": 1, "b": 2, "c": 3}, True, Exception, ""),
    (_f1, [], {"c": 3, "a": 1, "b": 2}, {"a": 1, "b": 2, "c": 3}, True, Exception, ""),
    (_f1, [1, 2], {}, {"a": 1, "b": 2, "c": 3}, False, TypeError, "missing a required argument"),
    (_f1, [1, 2], {"c": 3, "d": 4}, {"a": 1, "b": 2, "c": 3}, False, TypeError, "unexpected keyword argument"),
    (_f1, [1, 2, 3, 4], {}, {"a": 1, "b": 2, "c": 3}, False, TypeError, "too many positional arguments"),
    (_f1, [1, 2, 3], {"c": 3}, {"a": 1, "b": 2, "c": 3}, False, TypeError, "multiple values for argument"),

    (_f2, [1, 2, 3], {}, {"args": (1, 2, 3)}, True, Exception, ""),
    (_f2, [1, 2], {"c": 3}, {"args": (1, 2), "kwargs": {"c": 3}}, True, Exception, ""),
    (_f2, [], {"a": 1, "b": 2, "c": 3}, {"kwargs": {"a": 1, "b": 2, "c": 3}}, True, Exception, ""),

    (_f3, [1, 2], {"c": 3, "d": 4}, {"a": 1, "b": 2, "c": 3, "d": 4}, True, Exception, ""),
    (_f3, [1, 2, "ab", "cd"], {"c": 3, "d": 4},
     {"a": 1, "b": 2, "c": 3, "d": 4, "args": ("ab", "cd")}, True, Exception, ""),

    (_f4, [1, 2], {"c": 3, "d": 4}, {"a": 1, "b": 2, "c": 3, "d": 4}, True, Exception, ""),
    (_f4, [1, 2, "ab", "cd"], {"c": 3, "d": 4},
     {"a": 1, "b": 2, "c": 3, "d": 4, "args": ("ab", "cd")}, True, Exception, ""),
    (_f4, [1, 2, "ab", "cd"], {"c": 3},
     {"a": 1, "b": 2, "c": 3, "args": ("ab", "cd")}, True, Exception, ""),
    (_f4, [1, 2, "ab", "cd"], {"d": 3},
     {"a": 1, "b": 2, "d": 3, "args": ("ab", "cd")}, False, TypeError, "missing a required argument"),

    (_f5, [1, 2], {"c": 3, "d": 4}, {"a": 1, "b": 2, "c": 3, "d": 4}, True, Exception, ""),
    (_f5, [1, 2, "ab", "cd"], {"c": 3, "d": 4},
     {"a": 1, "b": 2, "c": 3, "d": 4, "args": ("ab", "cd")}, True, Exception, ""),
    (_f5, [1, 2, "ab", "cd"], {"c": 3},
     {"a": 1, "b": 2, "c": 3, "args": ("ab", "cd")}, True, Exception, ""),
    (_f5, [1], {"c": 3}, {"a": 1, "c": 3}, True, Exception, ""),
])
# fmt: on
def test_bind_plan_arguments_1(func, plan_args, plan_kwargs, plan_bound_params, except_type, success, errmsg):
    """
    Tests for ``bind_plan_arguments()`` function.
    """
    allowed_plans = {"existing": _process_plan(func, existing_devices={}, existing_plans={})}
    if success:
        plan_parameters_copy = copy.deepcopy(allowed_plans["existing"])

        bound_params = bind_plan_arguments(
            plan_args=plan_args, plan_kwargs=plan_kwargs, plan_parameters=allowed_plans["existing"]
        )
        assert bound_params.arguments == plan_bound_params

        # Make sure that the original plan parameters were not changed
        assert plan_parameters_copy == allowed_plans["existing"]
    else:
        with pytest.raises(except_type, match=errmsg):
            bind_plan_arguments(
                plan_args=plan_args, plan_kwargs=plan_kwargs, plan_parameters=allowed_plans["existing"]
            )


def _plan_ftd1a(pa, pb):
    yield from [pa, pb]


_desc_ftd1a_plain = {
    "description": "Name: _plan_ftd1a",
    "parameters": {"pa": "Name: pa\nType: -\nDefault: -", "pb": "Name: pb\nType: -\nDefault: -"},
}

_desc_ftd1a_html = {
    "description": "<i>Name:</i> <b>_plan_ftd1a</b>",
    "parameters": {
        "pa": "<i>Name:</i> <b>pa</b><br><i>Type:</i> <b>-</b><br><i>Default:</i> <b>-</b>",
        "pb": "<i>Name:</i> <b>pb</b><br><i>Type:</i> <b>-</b><br><i>Default:</i> <b>-</b>",
    },
}


def _plan_ftd1b(pa: str = "abc", pb: int = 50):
    yield from [pa, pb]


_desc_ftd1b_plain = {
    "description": "Name: _plan_ftd1b",
    "parameters": {"pa": "Name: pa\nType: str\nDefault: 'abc'", "pb": "Name: pb\nType: int\nDefault: 50"},
}

_desc_ftd1b_html = {
    "description": "<i>Name:</i> <b>_plan_ftd1b</b>",
    "parameters": {
        "pa": "<i>Name:</i> <b>pa</b><br><i>Type:</i> <b>str</b><br><i>Default:</i> <b>'abc'</b>",
        "pb": "<i>Name:</i> <b>pb</b><br><i>Type:</i> <b>int</b><br><i>Default:</i> <b>50</b>",
    },
}


def _plan_ftd1c(pa: str = "abc", pb: int = 50):
    """
    This is plan description.
    Multiline string.

    Parameters
    ----------
    pa
        Description of the parameter pa
    pb : int
        Description
        of the parameter pb
    """
    yield from [pa, pb]


_desc_ftd1c_plain = {
    "description": "Name: _plan_ftd1c\nThis is plan description.\nMultiline string.",
    "parameters": {
        "pa": "Name: pa\nType: str\nDefault: 'abc'\nDescription of the parameter pa",
        "pb": "Name: pb\nType: int\nDefault: 50\nDescription\nof the parameter pb",
    },
}

_desc_ftd1c_html = {
    "description": "<i>Name:</i> <b>_plan_ftd1c</b><br>This is plan description.<br>Multiline string.",
    "parameters": {
        "pa": "<i>Name:</i> <b>pa</b><br><i>Type:</i> <b>str</b><br><i>Default:</i> <b>'abc'</b><br>"
        "Description of the parameter pa",
        "pb": "<i>Name:</i> <b>pb</b><br><i>Type:</i> <b>int</b><br><i>Default:</i> <b>50</b><br>"
        "Description<br>of the parameter pb",
    },
}


@parameter_annotation_decorator(
    {
        "description": "This is plan description\ndefined in the decorator.",
        "parameters": {
            "pa": {
                "description": "Multiline description\nof the parameter 'pa'",
                "annotation": "str",
                "default": "default-value",
            },
            "pb": {
                "description": "Single line description of the parameter 'pb'",
                "annotation": "float",
                "default": 50.96,
            },
        },
    }
)
def _plan_ftd1d(pa=None, pb=None):
    yield from [pa, pb]


_desc_ftd1d_plain = {
    "description": "Name: _plan_ftd1d\nThis is plan description\ndefined in the decorator.",
    "parameters": {
        "pa": "Name: pa\nType: str\nDefault: 'default-value'\nMultiline description\nof the parameter 'pa'",
        "pb": "Name: pb\nType: float\nDefault: 50.96\nSingle line description of the parameter 'pb'",
    },
}

_desc_ftd1d_html = {
    "description": "<i>Name:</i> <b>_plan_ftd1d</b><br>This is plan description<br>defined in the decorator.",
    "parameters": {
        "pa": "<i>Name:</i> <b>pa</b><br><i>Type:</i> <b>str</b><br><i>Default:</i> <b>'default-value'</b><br>"
        "Multiline description<br>of the parameter 'pa'",
        "pb": "<i>Name:</i> <b>pb</b><br><i>Type:</i> <b>float</b><br><i>Default:</i> <b>50.96</b><br>"
        "Single line description of the parameter 'pb'",
    },
}


@parameter_annotation_decorator(
    {
        "parameters": {
            "pa": {
                "annotation": "int",
                "default": 50,
                "min": 1,
                "max": 100.5,
                "step": 0.001,
            },
        },
    }
)
def _plan_ftd1e(pa=None):
    yield from [pa]


_desc_ftd1e_plain = {
    "description": "Name: _plan_ftd1e",
    "parameters": {
        "pa": "Name: pa\nType: int\nDefault: 50\nMin: 1 Max: 100.5 Step: 0.001",
    },
}

_desc_ftd1e_html = {
    "description": "<i>Name:</i> <b>_plan_ftd1e</b>",
    "parameters": {
        "pa": "<i>Name:</i> <b>pa</b><br><i>Type:</i> <b>int</b><br><i>Default:</i> <b>50</b><br>"
        "<i>Min:</i> <b>1</b> <i>Max:</i> <b>100.5</b> <i>Step:</i> <b>0.001</b>",
    },
}


@parameter_annotation_decorator(
    {
        "parameters": {
            "pa": {
                "annotation": "typing.List[typing.Union[DeviceType1, PlanType1, PlanType2]]",
                "devices": {"DeviceType1": ("det1", "det2", "det3")},
                "plans": {"PlanType1": ("plan1", "plan2"), "PlanType2": ("plan3",)},
                "default": "det1",
            },
        },
    }
)
def _plan_ftd1f(pa=None):
    yield from [pa]


_desc_ftd1f_plain = {
    "description": "Name: _plan_ftd1f",
    "parameters": {
        "pa": "Name: pa\nType: typing.List[typing.Union[DeviceType1, PlanType1, PlanType2]]\n"
        "DeviceType1: ('det1', 'det2', 'det3')\nPlanType1: ('plan1', 'plan2')\nPlanType2: ('plan3',)\n"
        "Default: 'det1'",
    },
}

_desc_ftd1f_html = {
    "description": "<i>Name:</i> <b>_plan_ftd1f</b>",
    "parameters": {
        "pa": "<i>Name:</i> <b>pa</b><br><i>Type:</i> <b>typing.List[typing.Union[DeviceType1, PlanType1, "
        "PlanType2]]</b><br><b>DeviceType1:</b> ('det1', 'det2', 'det3')<br><b>PlanType1:</b> ('plan1', "
        "'plan2')<br><b>PlanType2:</b> ('plan3',)<br><i>Default:</i> <b>'det1'</b>",
    },
}


# fmt: off
@pytest.mark.parametrize("plan, desc_plain, desc_html", [
    (_plan_ftd1a, _desc_ftd1a_plain, _desc_ftd1a_html),
    (_plan_ftd1b, _desc_ftd1b_plain, _desc_ftd1b_html),
    (_plan_ftd1c, _desc_ftd1c_plain, _desc_ftd1c_html),
    (_plan_ftd1d, _desc_ftd1d_plain, _desc_ftd1d_html),
    (_plan_ftd1e, _desc_ftd1e_plain, _desc_ftd1e_html),
    (_plan_ftd1f, _desc_ftd1f_plain, _desc_ftd1f_html),
])
# fmt: on
def test_format_text_descriptions_1(plan, desc_plain, desc_html):
    plan_params = _process_plan(plan, existing_devices={}, existing_plans={})

    desc = format_text_descriptions(plan_params, use_html=False)
    assert desc == desc_plain

    desc = format_text_descriptions(plan_params, use_html=True)
    assert desc == desc_html
