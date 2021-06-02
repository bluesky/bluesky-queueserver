import os
import pytest
import copy
import yaml
import pickle
import typing
import subprocess
import pprint
import sys
from bluesky import abc

import ophyd

from .common import copy_default_profile_collection, patch_first_startup_file

from .common import reset_sys_modules  # noqa: F401

from bluesky_queueserver.manager.annotation_decorator import parameter_annotation_decorator

from bluesky_queueserver.manager.profile_ops import (
    get_default_startup_dir,
    load_profile_collection,
    load_startup_script,
    load_startup_module,
    load_worker_startup_code,
    plans_from_nspace,
    devices_from_nspace,
    prepare_plan,
    gen_list_of_plans_and_devices,
    load_existing_plans_and_devices,
    load_user_group_permissions,
    _process_plan,
    validate_plan,
    bind_plan_arguments,
    _select_allowed_items,
    load_allowed_plans_and_devices,
    hex2bytes,
    bytes2hex,
    _prepare_plans,
    _prepare_devices,
    _unpickle_types,
    StartupLoadingError,
)


def test_hex2bytes_bytes2hex():
    """
    Basic test for the functions ``hex2bytes`` and ``bytes2hex``.
    """
    dict_initial = {"abc": 50, "def": {"some_key": "some_value"}}

    # Check if pickling/unpickling a dictionary works.
    b_in = pickle.dumps(dict_initial)
    s = bytes2hex(b_in)
    assert isinstance(s, str)
    assert len(s) == 3 * len(b_in) - 1
    b_out = hex2bytes(s)
    assert b_out == b_in
    dict_result = pickle.loads(b_out)
    assert dict_result == dict_initial


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
from IPython import config #, get_ipython

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

    # Load different script (same name, but different path)
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


def test_load_startup_script_4(tmp_path, monkeypatch):
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
        },
        {
            "name": "val2",
            "description": "Description of the parameter Value 2.",
        },
    ],
    "returns": {"description": "v : int\n    Description for the return statement"},
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
    "returns": {"description": "int\n    Description for the return statement"},
}


def _pf1c(val1, val2):
    """
    Returns
    -------
    Description for the return statement
    """
    return int(val1 + int(val2[0]))


_pf1c_processed = {
    "returns": {"description": "Description for the return statement"},
}


def _pf1d(val1, val2):
    """
    Returns
    -------
    int
        Description for the return statement - Line 1
        Description for the return statement - Line 2

        Description for the return statement - Line 3

    """
    return int(val1 + int(val2[0]))


_pf1d_processed = {
    "returns": {
        "description": "int\n    Description for the return statement - Line 1\n"
        "    Description for the return statement - Line 2\n\n"
        "    Description for the return statement - Line 3"
    },
}


def _pf1e(val1, val2):
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


_pf1e_processed = {
    "parameters": [
        {
            "name": "val1",
            "description": "Description of the parameter Value 1 - Line 1.\n"
            "Description of the parameter Value 1 - Line 2.\n\n"
            "Description of the parameter Value 1 - Line 3.",
        },
        {
            "name": "val2",
            "description": "Description of the parameter Value 2 - Line 1.\n"
            "Description of the parameter Value 2 - Line 2.\n\n"
            "Description of the parameter Value 2 - Line 3.",
        },
    ],
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
    (_pf1e, _pf1e_processed),
])
# fmt: on
def test_process_plan_1(plan_func, plan_info_expected):
    def compare_values(pf_info, plan_info_expected):
        if isinstance(plan_info_expected, (tuple, list)):
            for pfp, pfe in zip(pf_info, plan_info_expected):
                compare_values(pfp, pfe)
        elif isinstance(plan_info_expected, dict):
            for name in plan_info_expected:
                compare_values(pf_info[name], plan_info_expected[name])
        else:
            assert pf_info == plan_info_expected

    pf_info = _process_plan(plan_func)
    compare_values(pf_info, plan_info_expected)


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
        assert isinstance(device, (abc.Readable, abc.Flyable)), f"The object '{device}' is not a device"

    # Check that both devices and signals are recognized by the function
    assert "custom_test_device" in devices
    assert "custom_test_signal" in devices


@pytest.mark.parametrize(
    "plan, success, err_msg",
    [
        ({"name": "count", "args": [["det1", "det2"]]}, True, ""),
        ({"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10]}, True, ""),
        ({"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 10, "delay": 1}}, True, ""),
        (
            {"name": "countABC", "args": [["det1", "det2"]]},
            False,
            "Plan 'countABC' is not allowed or does not exist.",
        ),
    ],
)
def test_prepare_plan(plan, success, err_msg):

    pc_path = get_default_startup_dir()
    nspace = load_profile_collection(pc_path)
    plans = plans_from_nspace(nspace)
    devices = devices_from_nspace(nspace)

    if success:
        plan_parsed = prepare_plan(plan, allowed_plans=plans, allowed_devices=devices)
        expected_keys = ("name", "args", "kwargs")
        for k in expected_keys:
            assert k in plan_parsed, f"Key '{k}' does not exist: {plan_parsed.keys()}"
    else:
        with pytest.raises(RuntimeError, match=err_msg):
            prepare_plan(plan, allowed_plans=plans, allowed_devices=devices)


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


def test_load_existing_plans_and_devices():
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


def test_unpickle_items():
    """
    Simple test for ``_unpickle_items()``.
    """
    # Dictionary that contains pickled values. The dictionary may contain lists (tuples)
    #   of dictionaries, so the conversion function must be able to handle the lists.
    item_dict_pickled = {
        "a": "abc",
        "b": typing.Any,
        "b_pickled": bytes2hex(pickle.dumps(typing.Any)),
        "e": {
            "f": {
                "a": "abc",
                "b": typing.List[typing.Any],
                "b_pickled": bytes2hex(pickle.dumps(typing.List[typing.Any])),
            },
            "g": [
                {
                    "d": {
                        "p_pickled": bytes2hex(pickle.dumps(typing.Union[float, str])),
                    }
                }
            ],
        },
    }

    # The dictionary with raw binary items.
    item_dict = copy.deepcopy(item_dict_pickled)
    item_dict["b_pickled"] = typing.Any
    item_dict["e"]["f"]["b_pickled"] = typing.List[typing.Any]
    item_dict["e"]["g"][0]["d"]["p_pickled"] = typing.Union[float, str]

    _unpickle_types(item_dict_pickled)
    assert item_dict_pickled == item_dict


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

    plans = plans_from_nspace(nspace)
    plans = _prepare_plans(plans)

    devices = devices_from_nspace(nspace)
    devices = _prepare_devices(devices)

    # Read the list of the existing plans of devices
    file_path = os.path.join(pc_path, "existing_plans_and_devices.yaml")
    existing_plans, existing_devices = load_existing_plans_and_devices(file_path)

    # The types must be unpicked before they could be compared (pickling format may
    #   differ depending on Python version)
    _unpickle_types(plans)
    _unpickle_types(devices)
    _unpickle_types(existing_plans)
    _unpickle_types(existing_devices)

    # Compare
    assert set(plans.keys()) == set(existing_plans.keys())
    assert set(devices) == set(existing_devices)

    # The list of plans can be large, so it is better to compare the contents plan by plan.
    #   If there is a mismatch, the printed difference is too large to be useful.
    assert len(plans) == len(existing_plans)
    for key in plans.keys():
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
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ".*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ".*"  # A different way to allow all
    forbidden_devices:
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
"""

_user_groups_dict = {
    "user_groups": {
        "root": {
            "allowed_plans": [None],
            "forbidden_plans": [None],
            "allowed_devices": [None],
            "forbidden_devices": [None],
        },
        "admin": {
            "allowed_plans": [".*"],
            "forbidden_plans": [None],
            "allowed_devices": [".*"],
            "forbidden_devices": [None],
        },
        "test_user": {
            "allowed_plans": ["^count$", "scan$"],
            "forbidden_plans": ["^adaptive_scan$", "^inner_product"],
            "allowed_devices": ["^det", "^motor"],
            "forbidden_devices": ["^det[3-5]$", r"^motor\d+$"],
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


@pytest.mark.parametrize("group_to_delete", ["root", "admin"])
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
    ({"abc34": 1, "abcd": 2}, [r"^abc"], [r"^abc\d+$"], {"abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [r"^abc"], [r"^abc.*$"], {}),
    ({"abc34": 1, "abcd": 2}, [r"^abc"], [r"^abcde$", r"^abc.*$"], {}),
    ({"abc34": 1, "abcd": 2}, [r"^abc"], [r"^abcde$", r"^a.2$"], {"abc34": 1, "abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [r"d$", r"4$"], [r"^abcde$", r"^a.2$"], {"abc34": 1, "abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [None], [r"^abc\d+$"], {"abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [r"^abc"], [None], {"abc34": 1, "abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [None], [None], {"abc34": 1, "abcd": 2}),
    ({"abc34": 1, "abcd": 2}, [], [None], {}),
    ({"abc34": 1, "abcd": 2}, [None], [], {"abc34": 1, "abcd": 2}),
    ({}, [r"^abc"], [r"^abc\d+$"], {}),
])
# fmt: on
def test_select_allowed_items(item_dict, allow_patterns, disallow_patterns, result):
    """
    Tests for ``_select_allowed_items``.
    """
    r = _select_allowed_items(item_dict, allow_patterns, disallow_patterns)
    assert r == result


# fmt: off
@pytest.mark.parametrize("fln_existing_items, fln_user_groups, empty_dict, all_users", [
    ("existing_plans_and_devices.yaml", "user_group_permissions.yaml", False, True),
    ("existing_plans_and_devices.yaml", None, False, False),
    (None, "user_group_permissions.yaml", True, True),
    (None, None, True, False),
])
# fmt: on
def test_load_allowed_plans_and_devices_1(fln_existing_items, fln_user_groups, empty_dict, all_users):
    """
    Basic test for ``load_allowed_plans_and_devices``.
    """
    pc_path = get_default_startup_dir()

    fln_existing_items = None if (fln_existing_items is None) else os.path.join(pc_path, fln_existing_items)
    fln_user_groups = None if (fln_user_groups is None) else os.path.join(pc_path, fln_user_groups)

    allowed_plans, allowed_devices = load_allowed_plans_and_devices(
        path_existing_plans_and_devices=fln_existing_items, path_user_group_permissions=fln_user_groups
    )

    if empty_dict:
        assert allowed_plans == {}
        assert allowed_devices == {}
    else:
        assert "root" in allowed_plans
        assert "root" in allowed_devices
        assert allowed_plans["root"]
        assert allowed_devices["root"]

        if all_users:
            assert "admin" in allowed_plans
            assert "admin" in allowed_devices
            assert allowed_plans["admin"]
            assert allowed_devices["admin"]
            assert "test_user" in allowed_plans
            assert "test_user" in allowed_devices
            assert allowed_plans["test_user"]
            assert allowed_devices["test_user"]
        else:
            assert "admin" not in allowed_plans
            assert "admin" not in allowed_devices
            assert "test_user" not in allowed_plans
            assert "test_user" not in allowed_devices


_patch_junk_plan_and_device = """

from ophyd import Device

class JunkDevice(Device):
    ...

junk_device = JunkDevice('ABC', name='stage')

def junk_plan():
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
      - ".*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ".*"  # A different way to allow all
    forbidden_devices:
      - null  # Nothing is forbidden
"""

_user_permissions_excluding_junk1 = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - null  # Allow all
    forbidden_plans:
      - "^junk_plan$"
    allowed_devices:
      - null  # Allow all
    forbidden_devices:
      - "^junk_device$"
  admin:  # The group includes beamline staff, includes all or most of the plans and devices
    allowed_plans:
      - ".*"  # A different way to allow all
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - ".*"  # A different way to allow all
    forbidden_devices:
      - null  # Nothing is forbidden
"""

_user_permissions_excluding_junk2 = """user_groups:
  root:  # The group includes all available plan and devices
    allowed_plans:
      - "^(?!.*junk)"  # Allow all plans that don't contain 'junk' in their names
    forbidden_plans:
      - null  # Nothing is forbidden
    allowed_devices:
      - "^(?!.*junk)"  # Allow all devices that don't contain 'junk' in their names
    forbidden_devices:
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
"""


# fmt: off
@pytest.mark.parametrize("permissions_str, items_are_removed", [
    (_user_permissions_clear, False),
    (_user_permissions_excluding_junk1, True),
    (_user_permissions_excluding_junk2, True),
])
# fmt: on
def test_load_allowed_plans_and_devices_2(tmp_path, permissions_str, items_are_removed):
    """
    Tests if filtering settings for the "root" group are also applied to other groups.
    The purpose of the "root" group is to filter junk from the list of existing devices and
    plans.
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

    allowed_plans, allowed_devices = load_allowed_plans_and_devices(
        path_existing_plans_and_devices=plans_and_devices_fln,
        path_user_group_permissions=permissions_fln,
    )

    if items_are_removed:
        assert "junk_device" not in allowed_devices["root"]
        assert "junk_device" not in allowed_devices["admin"]
        assert "junk_plan" not in allowed_plans["root"]
        assert "junk_plan" not in allowed_plans["admin"]
    else:
        assert "junk_device" in allowed_devices["root"]
        assert "junk_device" in allowed_devices["admin"]
        assert "junk_plan" in allowed_plans["root"]
        assert "junk_plan" in allowed_plans["admin"]


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
    allowed_plans = {"existing": _process_plan(func)}
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
        "returns": {
            "description": "Yields a sequence of plan messages.",
            "annotation": "typing.Generator[tuple, None, None]",
        },
    }
)
def _some_strange_plan(
    motors: typing.List[typing.Any],  # The actual type should be a list of 'ophyd.device.Device'
    detectors: typing.List[typing.Any],  # The actual type should be a list of 'ophyd.device.Device'
    plans_to_run: typing.Union[typing.List[callable], callable],
    positions: typing.Union[typing.List[float], float, None] = 10,  # TYPE IS ACTUALLY USED FOR VALIDATION
) -> typing.Generator[str, None, None]:  # Type should be 'bluesky.utils.Msg', not 'str'
    yield from ["one", "two", "three"]


# fmt: off
@pytest.mark.parametrize("plan, allowed_devices, success, errmsg", [
    # Basic use of the function.
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # The same as the previous call, but all parameters are passed as kwargs.
    ({"args": [], "kwargs": {"motors": ("m1", "m2"), "detectors": ("d1", "d2"), "plans_to_run": ("p1",),
                             "positions": (10.0, 20.0)}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # Positions are int (instead of float). Should be converted to float.
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",), (10, 20)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # Position is a single value (part of type description).
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",), 10], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # Position is None (part of type description).
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",), None], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),
    # Position is not specified (default value is used).
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),

    # Use motor that is not listed in the annotation (but exists in the list of allowed devices).
    ({"args": [("m2", "m4"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m2", "m4", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'm2'"),
    # The motor is not in the list of allowed devices.
    ({"args": [("m2", "m3"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m2", "m4", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'm2'"),
    # Both motors are not in the list of allowed devices.
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m4", "m5", "d1", "d2"), False, "value is not a valid enumeration member; permitted:"),
    # Empty list of allowed devices (should be the same result as above).
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     (), False, "value is not a valid enumeration member; permitted:"),
    # Single motor is passed as a scalar (instead of a list element)
    ({"args": ["m2", ("d1", "d2"), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m2", "m4", "d1", "d2"), False, "value is not a valid list"),

    # Pass single detector (allowed).
    ({"args": [("m1", "m2"), "d4", ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2", "d4"), True, ""),
    # Pass single detector from 'Detectors2' group, which is not in the list of allowed devices.
    ({"args": [("m1", "m2"), "d4", ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "value is not a valid enumeration member; permitted:"),
    # Pass single detector from 'Detectors1' group (not allowed).
    ({"args": [("m1", "m2"), "d2", ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2", "d4"), False, " value is not a valid list"),
    # Pass a detector from a group 'Detector2' as a list element.
    ({"args": [("m1", "m2"), ("d4",), ("p1",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2", "d4"), False, "value is not a valid enumeration member; permitted: 'd1', 'd2'"),

    # Plan 'p3' is not in the list of allowed plans
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p3",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'p1'"),
    # Plan 'p2' is in the list of allowed plans, but not listed in the annotation.
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p2",), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'p1'"),
    # Plan 'p2' is in the list of allowed plans, but not listed in the annotation.
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1", "p2"), (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "value is not a valid enumeration member; permitted: 'p1'"),
    # Single plan is passed as a scalar (allowed in the annotation).
    ({"args": [("m1", "m2"), ("d1", "d2"), "p1", (10.0, 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), True, ""),

    # Position is passed as a string (validation should fail)
    ({"args": [("m1", "m2"), ("d1", "d2"), ("p1",), ("10.0", 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "Incorrect parameter type"),
    # Int instead of a motor name (validation should fail)
    ({"args": [(0, "m2"), ("d1", "d2"), ("p1",), ("10.0", 20.0)], "kwargs": {}},
     ("m1", "m2", "d1", "d2"), False, "value is not a valid enumeration member"),
])
# fmt: on
def test_validate_plan_3(plan, allowed_devices, success, errmsg):
    """
    Test ``validate_plan`` on a function with more complicated signature and custom annotation.
    Mostly testing verification of types and use of the list of available devices.
    """
    plan["name"] = "_some_strange_plan"
    allowed_plans = {
        "_some_strange_plan": _process_plan(_some_strange_plan),
        "p1": {},  # The plan is used only as a parameter value
        "p2": {},  # The plan is used only as a parameter value
    }
    # 'allowed_devices' must be a dictionary
    allowed_devices = {_: None for _ in allowed_devices}

    success_out, errmsg_out = validate_plan(plan, allowed_plans=allowed_plans, allowed_devices=allowed_devices)

    assert success_out == success, f"errmsg: {errmsg_out}"
    if success:
        assert errmsg_out == errmsg
    else:
        assert errmsg in errmsg_out


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
def test_bind_plan_parameters_1(func, plan_args, plan_kwargs, plan_bound_params, except_type, success, errmsg):
    """
    Tests for ``bind_plan_parameters`` function.
    """
    allowed_plans = {"existing": _process_plan(func)}
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
