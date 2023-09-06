import ast
import collections
import copy
import enum
import glob
import importlib
import importlib.resources
import inspect
import logging
import numbers
import os
import random
import re
import shutil
import sys
import tempfile
import traceback
import typing
from collections.abc import Iterable, Mapping

import jsonschema
import pydantic
import yaml
from numpydoc.docscrape import NumpyDocString
from packaging import version

import bluesky_queueserver

from .logging_setup import PPrintForLogging as ppfl

logger = logging.getLogger(__name__)
qserver_version = bluesky_queueserver.__version__

pydantic_version_major = version.parse(pydantic.__version__).major


class ScriptLoadingError(Exception):
    def __init__(self, msg, tb):
        super().__init__(msg)
        self._tb = tb

    @property
    def tb(self):
        """
        Returns the full traceback (string) that is passed as a second parameter.
        The traceback is expected to be fully prepared so that it could be saved
        or passed to consumer over the network. Capturing traceback in the function
        executing the script is reducing references to Queue Server code, which
        is generic and unimportant for debugging startup scripts.
        """
        return self._tb


class _RegisteredNamespaceItems:
    def __init__(self):
        self._reg_devices = {}
        self._reg_plans = {}

    def clear(self):
        """
        Clear the dictionaries of registered plans and devices.
        """
        self._reg_devices.clear()
        self._reg_plans.clear()

    @property
    def reg_devices(self):
        """
        Property returns the reference to the dictionary of registered devices.
        """
        return self._reg_devices

    @property
    def reg_plans(self):
        """
        Property returns the reference to the dictionary of registered plans.
        """
        return self._reg_plans

    def _validate_name(self, name, *, item_type):
        """
        Validate object name (for a plan or device). Item type should be a string ('device' or 'plan'),
        used in error messages.
        """
        item_type = item_type.capitalize()
        if not isinstance(name, str):
            raise TypeError(f"{item_type} name must be a string: name={name} type(name)={type(name)}")
        if not name:
            raise ValueError(f"{item_type} name is an empty string")
        if not re.search(r"^[_a-zA-Z]([_a-zA-Z0-9])*$", name):
            raise ValueError(
                f"{item_type} name {name!r} contains invalid characters. The name must start with a letter "
                "or underscore and may contain letters, numbers and underscores.)"
            )

    def _validate_depth(self, depth):
        """
        Validate the value of 'depth' parameter.
        """
        if not isinstance(depth, int):
            raise TypeError(f"Depth must be an integer number: depth={depth} type(depth)={type(depth)}")
        if depth < 0:
            raise ValueError(f"Depth must be a positive integer: depth={depth}")

    def register_device(self, name, *, depth=1, exclude=False):
        """
        Register a device with Queue Server. The function allows to specify depth of the tree
        of subdevices included in the list or exclude the device from the list. Only top-level
        devices can be registered. Devices can be registered before they are defined in
        the startup code.

        This is experimental API. Functionality may change in the future.

        Parameters
        ----------
        name: str
            Name of the device. This should be a name of the variable in the global scope
            of the startup code. Only top level devices can be registered. Names of the device
            components are not accepted.
        depth: int
            Depth of the device tree included in the list of existing devices: 0 - include
            the tree of unlimited depth, 1 - include only top-level device, 2 - include
            the top-level device and it's components etc. Depth specified for registered
            devices overrides the depth used by default for unregistered devices.
        exclude: boolean
            Set this parameter ``False`` to exclude the device from the list of existing
            devices. The device can still be used in the code, but it can not be passed
            as a parameter value to a plan via RE Manager.

        Raises
        ------
        ValueError, TypeError
            Unsupported type or value of passed parameters.
        """
        self._validate_name(name, item_type="device")
        self._validate_depth(depth)
        exclude = bool(exclude)
        self._reg_devices[name] = dict(obj=None, depth=depth, exclude=exclude)

    def register_plan(self, name, *, exclude=False):
        """
        Register a plan with Queue Server. The function allows to exclude a plan from
        processing by the Queue Server.

        This is experimental API. Functionality may change in the future.

        Parameters
        ----------
        name: str
            Name of the plan. This should be a name of the variable in the global scope
            of the startup code.
        exclude: boolean
            Set this parameter ``False`` to exclude the plan from the list of existing
            plans. The plan can still be called from other plans, but it can not be
            started using RE Manager.

        Raises
        ------
        ValueError, TypeError
            Unsupported type or value of passed parameters.
        """
        self._validate_name(name, item_type="plan")
        exclude = bool(exclude)
        self._reg_plans[name] = dict(obj=None, exclude=exclude)


reg_ns_items = _RegisteredNamespaceItems()

clear_registered_items = reg_ns_items.clear
# The following are public functions intended to be used in startup code
register_device = reg_ns_items.register_device
register_plan = reg_ns_items.register_plan


def get_default_startup_dir():
    """
    Returns the path to the default profile collection that is distributed with the package.
    The function does not guarantee that the directory exists. Used for demo with Python-based worker.
    """
    pc_path = os.path.join(importlib.resources.files("bluesky_queueserver"), "profile_collection_sim", "")
    return pc_path


def get_default_startup_profile():
    """
    Returns the name for the default startup profile. Used for demo with IPython-based worker.
    The startup code is expected to be in ``/tmp/qserver/ipython/profile_collection_sim/startup`` directory.
    """
    return "collection_sim"


def create_demo_ipython_profile(startup_dir, *, delete_existing=True):
    """
    Create demo IPython profile in temporary location. Copy startup directory
    to the new profile. Raises IOError if the destination directory is not temporary.
    """
    tempdir = tempfile.gettempdir()
    if os.path.commonprefix([tempdir, startup_dir]) != tempdir:
        raise IOError("Attempting to create a demo profile in non-temporary startup directory: %r", startup_dir)

    # Delete the existing startup directory (but not the whole profile).
    if delete_existing:
        shutil.rmtree(startup_dir, ignore_errors=True)

    # Copy the startup files
    default_startup_dir = get_default_startup_dir()
    shutil.copytree(default_startup_dir, startup_dir)


_startup_script_patch = """
import sys
import logging
from bluesky_queueserver.manager.profile_tools import global_user_namespace
logger_patch = logging.Logger(__name__)

global_user_namespace.set_user_namespace(user_ns=locals(), use_ipython=False)

class IPDummy:
    def __init__(self, user_ns):
        self.user_ns = user_ns

        # May be this should be some meaningful logger (used by 'configure_bluesky_logging')
        self.log = logging.Logger('ipython_patch')

def get_ipython_patch():
    ip_dummy = IPDummy(global_user_namespace.user_ns)
    return ip_dummy

get_ipython = get_ipython_patch
"""


# Discard RE and db from the profile namespace (if they exist).
def _discard_re_from_nspace(nspace, *, keep_re=False):
    """
    Discard RE and db from the profile namespace (if they exist).
    """
    if not keep_re:
        nspace.pop("RE", None)
        nspace.pop("db", None)


def _patch_script_code(code_str):
    """
    Patch script code represented as a single text string:
    detect lines that import ``get_ipython`` from ``IPython`` and
    set ``get_ipython=get_ipython_patch`` after each import.
    """

    def is_get_ipython_imported_in_line(line):
        """Check if ``get_ipython()`` is imported in the line"""
        # It is assumed that commenting is done using #
        return bool(re.search(r"^[^#]*IPython[^#]+get_ipython", line))

    s_list = code_str.splitlines()
    for n in range(len(s_list)):
        line = s_list[n]
        if is_get_ipython_imported_in_line(line):
            # The line may have comments. The patch should be inserted before the comment.
            line_split = re.split("#", line)
            # Do not patch empty lines that have only comments
            if line_split[0]:
                line = line_split[0] + "; get_ipython = get_ipython_patch"
                if len(line_split) > 1:
                    line += "  #" + "#".join(line_split[1:])
                s_list[n] = line

    return "\n".join(s_list)


def load_profile_collection(path, *, patch_profiles=True, keep_re=False, nspace=None):
    """
    Load profile collection located at the specified path. The collection consists of
    .py files started with 'DD-', where D is a digit (e.g. 05-file.py). The files
    are alphabetically sorted before execution.

    Parameters
    ----------
    path: str
        path to profile collection
    patch_profiles: boolean
        enable/disable patching profiles. At this point there is no reason not to
        patch profiles.
    keep_re: boolean
        Indicates if ``RE`` and ``db`` defined in the module should be kept (``True``)
        or removed (``False``).
    nspace: dict or None
        Reference to the existing namespace, in which the code is executed. If ``nspace``
        is *None*, then the new namespace is created.

    Returns
    -------
    nspace: dict
        namespace in which the profile collection was executed. Reference to ``nspace``
        passed as a parameter is returned if ``nspace`` is not *None*.

    Raises
    ------
    IOError
        path does not exist or the profile collection contains no valid startup files
    """

    # Create the list of files to load
    path = os.path.expanduser(path)
    path = os.path.abspath(path)

    if not os.path.exists(path):
        raise IOError(f"Path '{path}' does not exist.")
    if not os.path.isdir(path):
        raise IOError(f"Failed to load the profile collection. Path '{path}' is not a directory.")

    file_pattern_py = os.path.join(path, "*.py")
    file_pattern_ipy = os.path.join(path, "*.ipy")
    file_list = glob.glob(file_pattern_py) + glob.glob(file_pattern_ipy)
    file_list.sort()  # Sort in alphabetical order

    # If the profile collection contains no startup files, it is very likely
    #   that the profile collection directory is specified incorrectly.
    if not len(file_list):
        raise IOError(f"The directory '{path}' contains no startup files (mask '[0-9][0-9]*.py').")

    # Add original path to the profile collection to allow local imports
    #   from the patched temporary file.
    if path not in sys.path:
        # We don't want to add/remove the path if it is already in `sys.path` for some reason.
        sys.path.append(path)
        path_is_set = True
    else:
        path_is_set = False

    try:
        # Load the files into the namespace 'nspace'.
        nspace = {} if nspace is None else nspace

        if patch_profiles:
            exec(_startup_script_patch, nspace, nspace)
        for file in file_list:
            try:
                logger.info("Loading startup file '%s' ...", file)

                # Set '__file__' and '__name__' variables
                patch = f"__file__ = '{file}'; __name__ = '__main__'\n"
                exec(patch, nspace, nspace)

                if not os.path.isfile(file):
                    raise IOError(f"Startup file {file!r} was not found")
                code_str = _patch_script_code(open(file).read())
                code = compile(code_str, file, "exec")
                exec(code, nspace, nspace)

            except BaseException as ex:
                # Capture traceback and send it as a message
                msg = f"Error while executing script {file!r}: {ex}"
                ex_str = traceback.format_exception(*sys.exc_info())
                ex_str = "".join(ex_str) + "\n" + msg
                raise ScriptLoadingError(msg, ex_str) from ex

            finally:
                patch = "del __file__\n"  # Do not delete '__name__'
                exec(patch, nspace, nspace)

        _discard_re_from_nspace(nspace, keep_re=keep_re)

    finally:
        try:
            if path_is_set:
                sys.path.remove(path)
        except Exception:
            pass

    return nspace


def load_startup_module(module_name, *, keep_re=False, nspace=None):
    """
    Populate namespace by import a module.

    Parameters
    ----------
    module_name: str
        name of the module to import
    keep_re: boolean
        Indicates if ``RE`` and ``db`` defined in the module should be kept (``True``)
        or removed (``False``).
    nspace: dict or None
        Reference to the existing namespace, in which the code is executed. If ``nspace``
        is *None*, then the new namespace is created.

    Returns
    -------
    nspace: dict
        namespace that contains objects loaded from the module. Reference to ``nspace``
        passed as a parameter is returned if ``nspace`` is not *None*.
    """
    importlib.invalidate_caches()

    try:
        _module = importlib.import_module(module_name)
        nspace = {} if nspace is None else nspace
        nspace.update(_module.__dict__)

    except BaseException as ex:
        # Capture traceback and send it as a message
        msg = f"Error while loading module {module_name!r}: {ex}"
        ex_str = traceback.format_exception(*sys.exc_info())
        ex_str = "".join(ex_str) + "\n" + msg
        raise ScriptLoadingError(msg, ex_str) from ex

    _discard_re_from_nspace(nspace, keep_re=keep_re)

    return nspace


def load_startup_script(script_path, *, keep_re=False, enable_local_imports=True, nspace=None):
    """
    Populate namespace by import a module.

    Parameters
    ----------
    script_path : str
        full path to the startup script
    keep_re : boolean
        Indicates if ``RE`` and ``db`` defined in the module should be kept (``True``)
        or removed (``False``).
    enable_local_imports : boolean
        If ``False``, local imports from the script will not work. Setting to ``True``
        enables local imports.
    nspace: dict or None
        Reference to the existing namespace, in which the code is executed. If ``nspace``
        is *None*, then the new namespace is created.

    Returns
    -------
    nspace: dict
        namespace that contains objects loaded from the module. Reference to ``nspace``
        passed as a parameter is returned if ``nspace`` is not *None*.
    """
    importlib.invalidate_caches()

    if not os.path.isfile(script_path):
        raise ImportError(f"Failed to load the script '{script_path}': script was not found")

    nspace = {} if nspace is None else nspace

    if enable_local_imports:
        p = os.path.split(script_path)[0]
        sys.path.insert(0, p)  # Needed to make local imports work.
        # Save the list of available modules
        sm_keys = list(sys.modules.keys())

    try:
        if not os.path.isfile(script_path):
            raise IOError(f"Startup file {script_path!r} was not found")

        # Set '__file__' and '__name__' variables
        patch = f"__file__ = '{script_path}'; __name__ = '__main__'\n"
        exec(patch, nspace, nspace)

        code = compile(open(script_path).read(), script_path, "exec")
        exec(code, nspace, nspace)

    except BaseException as ex:
        # Capture traceback and send it as a message
        msg = f"Error while executing script {script_path!r}: {ex}"
        ex_str = traceback.format_exception(*sys.exc_info())
        ex_str = "".join(ex_str) + "\n" + msg
        raise ScriptLoadingError(msg, ex_str) from ex

    finally:
        patch = "del __file__\n"  # Do not delete '__name__'
        exec(patch, nspace, nspace)

        if enable_local_imports:
            # Delete data on all modules that were loaded by the script.
            # We don't need them anymore. Modules will be reloaded from disk if
            #   the script is executed again.
            for key in list(sys.modules.keys()):
                if key not in sm_keys:
                    # print(f"Deleting the key '{key}'")
                    del sys.modules[key]

            sys.path.remove(p)

    _discard_re_from_nspace(nspace, keep_re=keep_re)

    return nspace


def load_worker_startup_code(
    *,
    startup_dir=None,
    startup_module_name=None,
    startup_script_path=None,
    keep_re=False,
    nspace=None,
):
    """
    Load worker startup code. Possible sources: startup directory (IPython-style profile collection),
    startup script or startup module.

    Parameters
    ----------
    startup_dir : str
        Name of the directory that contains startup files.
    startup_module_name : str
        Name of the startup module.
    startup_script_path : str
        Path to startup script.
    keep_re: boolean
        Indicates if ``RE`` and ``db`` defined in the module should be kept (``True``)
        or removed (``False``).
    nspace: dict or None
        Reference to the existing namespace, in which the code is executed. If ``nspace``
        is *None*, then the new namespace is created.


    Returns
    -------
    nspace : dict
       Dictionary with loaded namespace data. Reference to ``nspace`` passed as a parameter
       is returned if ``nspace`` is not *None*.
    """

    if sum([_ is None for _ in [startup_dir, startup_module_name, startup_script_path]]) != 2:
        raise ValueError("Source of the startup code was not specified or multiple sources were specified.")

    clear_registered_items()  # Operation on the global object

    if startup_dir is not None:
        logger.info("Loading RE Worker startup code from directory '%s' ...", startup_dir)
        startup_dir = os.path.abspath(os.path.expanduser(startup_dir))
        logger.info("Startup directory: '%s'", startup_dir)
        nspace = load_profile_collection(startup_dir, keep_re=keep_re, patch_profiles=True, nspace=nspace)

    elif startup_module_name is not None:
        logger.info("Loading RE Worker startup code from module '%s' ...", startup_module_name)
        nspace = load_startup_module(startup_module_name, keep_re=keep_re, nspace=nspace)

    elif startup_script_path is not None:
        logger.info("Loading RE Worker startup code from script '%s' ...", startup_script_path)
        startup_script_path = os.path.abspath(os.path.expanduser(startup_script_path))
        nspace = load_startup_script(startup_script_path, keep_re=keep_re, nspace=nspace)

    else:
        logger.warning(
            "Source of startup information is not specified. RE Worker will be started with empty environment."
        )
        nspace = {}

    return nspace


def extract_script_root_path(*, startup_dir=None, startup_module_name=None, startup_script_path=None):
    """
    The function returns path to the root directory for local imports based on the source of
    startup files. The function returns ``None`` if root directory does not exist (e.g. when
    startup scripts are imported from the module). The root directory is a parameter of
    ``load_script_into_existing_nspace`` function. The startup code locations are checked
    in the same order as in ``load_worker_startup_code`` function.

    Parameters
    ----------
    startup_dir : str
        Name of the directory that contains startup files.
    startup_module_name : str
        Name of the startup module.
    startup_script_path : str
        Path to startup script.

    Returns
    -------
    str or None
        Path to the root directory of startup files.
    """
    if startup_dir is not None:
        return startup_dir
    elif startup_module_name is not None:
        return None
    elif startup_script_path is not None:
        return os.path.split(startup_script_path)[0]
    else:
        return None


def load_script_into_existing_nspace(
    *, script, nspace, enable_local_imports=True, script_root_path=None, update_re=False
):
    """
    Execute the script into the existing namespace ``nspace``. The script is expected to be executable
    in the current namespace. An exception is raised if execution of the script fails. The code
    calling this function is responsible for capturing exceptions and displaying or storing the error
    messages in the convenient form.

    Parameters
    ----------
    script: str
        Valid Python script, which is executable in the existing namespace.
    nspace: dict
        The existing namespace.
    enable_local_imports: boolean
        Enable/disable local imports when loading the script (``True/False``).
    script_root_path: str or None
        Root path (hypothetic location of the script) to use for local imports. The root directory
        is returned by the ``extract_script_root_path`` function.
    update_re: boolean
        Update the ``RE`` and ``db`` in ``nspace`` if they are changed in the script if ``True``,
        discard those objects otherwise.

    Returns
    -------
    None

    Raises
    ------
    Exceptions may be raised by the ``exec`` function.
    """
    global _n_running_scripts

    # There is nothing to do if the script is an empty string or None
    if not script:
        return
    elif not isinstance(script, str):
        ScriptLoadingError(f"Script must be a string: (type(script)={type(script)!r})", "")

    importlib.invalidate_caches()

    root_path_exists = script_root_path and os.path.isdir(script_root_path)
    if enable_local_imports and not root_path_exists:
        logger.error("The path '%s' does not exist. Local imports will not work.", script_root_path)

    use_local_imports = enable_local_imports and root_path_exists

    if use_local_imports:
        sys.path.insert(0, script_root_path)  # Needed to make local imports work.
        # Save the list of available modules
        sm_keys = list(sys.modules.keys())

    object_backup = {}
    if not update_re:
        # Save references to RE and db in case they are changed by the script
        if "RE" in nspace:
            object_backup["RE"] = nspace["RE"]
        if "db" in nspace:
            object_backup["db"] = nspace["db"]

    # saved__file__ = None
    try:
        # Set '__name__' variables. NOTE: '__file__' variable is undefined (difference!!!)
        script_path = os.path.join(script_root_path, "script") if script_root_path else "script"
        # saved__file__ = nspace["__file__"] if "__file__" in nspace else None
        patch = f"__name__ = '__main__'\n__file__ = '{script_path}'\n"
        exec(patch, nspace, nspace)

        # A script may be partially loaded into the environment in case it fails.
        #   This is 'realistic' behavior, similar to what happens in IPython.
        exec(script, nspace, nspace)

    except BaseException as ex:
        # Capture traceback and send it as a message
        msg = f"Failed to execute stript: {ex}"
        ex_str = traceback.format_exception(*sys.exc_info())
        ex_str = "".join(ex_str) + "\n" + msg
        raise ScriptLoadingError(msg, ex_str) from ex

    finally:
        # if saved__file__ is None:
        #     patch = "del __file__\n"  # Do not delete '__name__'
        # else:
        #     # Restore the original value
        #     patch = f"__file__ = '{saved__file__}'"
        # exec(patch, nspace, nspace)

        if not update_re:
            # Remove references for 'RE' and 'db' from the namespace
            nspace.pop("RE", None)
            nspace.pop("db", None)
            # Restore the original reference to 'RE' and 'db' (if they existed)
            nspace.update(object_backup)

        if use_local_imports:
            # Delete data on all modules that were loaded by the script.
            # We don't need them anymore. Modules will be reloaded from disk if
            #   the script is executed again.
            for key in list(sys.modules.keys()):
                if key not in sm_keys:
                    print(f"Deleting the key '{key}'")
                    del sys.modules[key]

            sys.path.remove(script_root_path)


def is_plan(obj):
    """
    Returns ``True`` if the object is a plan.
    """
    return inspect.isgeneratorfunction(obj)


def is_device(obj):
    """
    Returns ``True`` if the object is a device.
    """
    from bluesky import protocols

    return isinstance(obj, (protocols.Readable, protocols.Flyable)) and not inspect.isclass(obj)


def _process_registered_objects(*, nspace, reg_objs, validator, obj_type):
    """
    Attempt to get a reference for each registered object (plan or device).
    After processing, the parameter ``obj`` of each object will contain a valid reference
    to the object, unless the object is registered as excluded, the object does not exist in
    the namespace or not recognized as a plan or a device.

    Parameters
    ----------
    nspace: dict
        Worker namespace
    reg_objs: dict
        Reference to ``reg_ns_items.reg_plans`` or ``reg_ns_items.reg_devices``.
    validator: dict
        Function, which takes reference to the object and returns *True* if the object has
        valid type (reference to ``is_plan`` or ``is_device``).
    obj_type: str
        The name of the object type used in error messages. Expected to be ``plan`` or ``device``

    Returns
    -------
    None
    """
    for name, p in reg_objs.items():
        try:
            p["obj"] = None
            if p["exclude"]:
                continue
            obj = eval(name, nspace, nspace)
            if validator(obj):
                p["obj"] = obj
            else:
                logger.warning(f"Registered object {name!r} is not recognized as a {obj_type}.")
        except Exception as ex:
            logger.warning(f"Registered {obj_type} {name!r} is not found in the namespace: {ex}.")


def plans_from_nspace(nspace):
    """
    Extract plans from the namespace. Currently the function returns the dict of callable objects.

    Parameters
    ----------
    nspace: dict
        Namespace that may contain plans.

    Returns
    -------
    dict(str: callable)
        Dictionary of Bluesky plans
    """
    plans = {}
    for name, obj in nspace.items():
        if is_plan(obj):
            plans[name] = obj

    _process_registered_objects(nspace=nspace, reg_objs=reg_ns_items.reg_plans, validator=is_plan, obj_type="plan")

    return plans


def devices_from_nspace(nspace):
    """
    Extract signals and devices from the namespace. Currently the function returns the dict of
    namespace items of types inherited from ophyd.ophydobj.OphydObject objects.

    Parameters
    ----------
    nspace: dict
        Namespace that may contain plans and devices.

    Returns
    -------
    dict(str: callable)
        Dictionary that maps device names to device objects.
    """

    devices = {}
    for name, obj in nspace.items():
        if is_device(obj):
            devices[name] = obj

    _process_registered_objects(
        nspace=nspace, reg_objs=reg_ns_items.reg_devices, validator=is_device, obj_type="device"
    )

    return devices


def _get_nspace_object(object_name, *, objects_in_nspace, nspace=None):
    """
    Search for object in namespace by name (e.g. ``count`` or ``det1.val``).
    The function searches for subdevices by using ``component_name`` device property
    (plans have no ``component_name`` attribute). Returns a reference to
    the device object if it is found or device name (string) if the device is not found.

    Parameters
    ----------
    object_name: str
        Object (device or plan) name, such as ``count``, ``det1`` or ``det1.val``.
    objects_in_nspace: dict
        Dictionary of objects (devices and/or plans) in namespace.
    nspace: dict
        Reference to plan execution namespace. If ``nspace`` is not *None*, the up-to-date references
        to plans and devices are taken directly from the namespace. The plans and devices still must be
        present in ``plans_in_nspace`` and ``devices_in_nspace``, but those dictionaries may contain
        stale references (e.g. if a plan is interactively replaced by a user).

    Returns
    -------
    device: callable, ophyd.Object or str
        Reference to the object or object name (str) if the object is not found.
    """

    components, uses_re, _ = _split_name_pattern(object_name)
    if uses_re:
        raise ValueError(f"Object (device or plan) name {object_name!r} can not contain regular expressions")

    components = [_[0] for _ in components]  # We use only the 1st element

    if not components:
        raise ValueError(f"Device name {object_name!r} contains no components")

    object_found = True
    if components[0] in objects_in_nspace:
        if nspace:
            # This is actual reference to the object from execution namespace. It may be different from
            #   the reference saved during the last update of the lists.
            device = nspace[components[0]]
        else:
            device = objects_in_nspace[components[0]]
    else:
        object_found = False

    if object_found:
        for c in components[1:]:
            if hasattr(device, "component_names") and (c in device.component_names):
                # The defice MUST have the attribute 'c', but still process the case when
                #   there is a bug and the device doesn't have the attribute
                device = getattr(device, c, None)
                if object_found is None:
                    object_found = False
                    logger.error("Device '%s' does not have attribute (subdevice) '%s'", object_name, c)
                object_found = object_found if device else False

            else:
                object_found = False

            if not object_found:
                break

    if not object_found:
        device = object_name

    return device


def prepare_plan(plan, *, plans_in_nspace, devices_in_nspace, allowed_plans, allowed_devices, nspace=None):
    """
    Prepare the plan for execution: replace the device names (str) in the plan specification
    by references to ophyd objects; replace plan name by the reference to the plan.

    Parameters
    ----------
    plan: dict
        Plan specification. Keys: `name` (str) - plan name, `args` - plan args,
        `kwargs` - plan kwargs. The plan parameters must contain ``user_group``.
    plans_in_nspace: dict(str, callable)
        Dictionary of existing plans from the RE workspace.
    devices_in_nspace: dict(str, ophyd.Device)
        Dictionary of existing devices from RE workspace.
    allowed_plans: dict(str, dict)
        Dictionary of allowed plans that maps group name to dictionary of plans allowed
        to the members of the group (group name can be found in ``plan["user_group"]``.
    allowed_devices: dict(str, dict)
        Dictionary of allowed devices that maps group name to dictionary of devices allowed
        to the members of the group.
    nspace: dict
        Reference to plan execution namespace. If ``nspace`` is not *None*, the up-to-date references
        to plans and devices are taken directly from the namespace. The plans and devices still must be
        present in ``plans_in_nspace`` and ``devices_in_nspace``, but those dictionaries may contain
        stale references (e.g. if a plan is interactively replaced by a user).

    Returns
    -------
    dict
        Parsed plan specification that contains references to plans and Ophyd devices.

    Raises
    ------
    RuntimeError
        Raised in case parsing was not successful.
    """

    success = True
    err_msg = ""

    plan_name = plan["name"]
    plan_args = plan["args"] if "args" in plan else []
    plan_kwargs = plan["kwargs"] if "kwargs" in plan else {}
    plan_meta = plan["meta"] if "meta" in plan else {}

    if "user_group" not in plan:
        raise RuntimeError(f"No user group is specified in parameters for the plan '{plan_name}'")

    user_group = plan["user_group"]  # User group is REQUIRED parameter of the plan
    if (user_group not in allowed_plans) or (user_group not in allowed_devices):
        raise RuntimeError(f"Lists of allowed plans and devices is not defined for the user group '{user_group}'")

    group_plans = allowed_plans[user_group]
    group_devices = allowed_devices[user_group]

    # Run full validation of parameters (same as during plan submission)
    success, errmsg = validate_plan(plan, allowed_plans=group_plans, allowed_devices=group_devices)
    if not success:
        raise RuntimeError(f"Validation of plan parameters failed: {errmsg}")

    def ref_from_name(v, objects_in_nspace, sel_object_names, selected_object_tree, nspace, eval_expressions):
        if isinstance(v, str):
            if (v in sel_object_names) or _is_object_name_in_list(v, allowed_objects=selected_object_tree):
                v = _get_nspace_object(v, objects_in_nspace=objects_in_nspace, nspace=nspace)
            elif eval_expressions and re.search(r"^[_A-Za-z][_A-Za-z0-9]*(\.[_A-Za-z][_A-Za-z0-9]*)*$", v):
                try:
                    v = eval(v, nspace, nspace)
                except Exception:
                    pass
        return v

    def process_argument(v, objects_in_nspace, sel_object_names, selected_object_tree, nspace, eval_expressions):
        # Recursively process lists (iterables) and dictionaries
        if isinstance(v, str):
            v = ref_from_name(
                v, objects_in_nspace, sel_object_names, selected_object_tree, nspace, eval_expressions
            )
        elif isinstance(v, dict):
            for key, value in v.copy().items():
                v[key] = process_argument(
                    value, objects_in_nspace, sel_object_names, selected_object_tree, nspace, eval_expressions
                )
        elif isinstance(v, Iterable):
            v_original = v
            v = list()
            for item in v_original:
                v.append(
                    process_argument(
                        item,
                        objects_in_nspace,
                        sel_object_names,
                        selected_object_tree,
                        nspace,
                        eval_expressions,
                    )
                )
        return v

    def process_parameter_value(value, pp, objects_in_nspace, group_plans, group_devices, nspace):
        """
        Process a parameter value ``value`` based on parameter description ``pp``
        (``pp = allowed_plans[<plan_name>][<param_name>]``).
        """

        # Include/exclude all devices/plans if required
        convert_all_plan_names = pp.get("convert_plan_names", None)  # None - not defined
        convert_all_device_names = pp.get("convert_device_names", None)
        eval_all_expressions = pp.get("eval_expressions", False)

        # A set of the selected device names (device names are listed explicitly, not as a tree)
        sel_object_names, pname_list, dname_list = set(), set(), set()

        if "annotation" not in pp:
            convert_all_plan_names = True if (convert_all_plan_names is None) else convert_all_plan_names
            convert_all_device_names = True if (convert_all_device_names is None) else convert_all_device_names
        elif ("plans" in pp["annotation"]) or ("devices" in pp["annotation"]):
            # 'Custom' annotation: attempt to convert strings only to the devices
            #    and plans that are part of the description.
            pname_list, dname_list = [], []
            for plist in pp["annotation"].get("plans", {}).values():
                plist = [_ for _ in plist if _is_object_name_in_list(_, allowed_objects=group_plans)]
                pname_list.extend(list(plist))
            for dlist in pp["annotation"].get("devices", {}).values():
                dlist = [_ for _ in dlist if _is_object_name_in_list(_, allowed_objects=group_devices)]
                dname_list.extend(list(dlist))
            pname_list, dname_list = set(pname_list), set(dname_list)

        # Create a tree of objects (dictionary of plans and devices)
        selected_object_tree = {}
        if convert_all_plan_names is True:
            selected_object_tree.update(group_plans)
        elif convert_all_plan_names is False:
            pname_list = set()

        if convert_all_device_names is True:
            selected_object_tree.update(group_devices)
        elif convert_all_device_names is False:
            dname_list = set()

        sel_object_names = pname_list.union(dname_list)

        if sel_object_names or selected_object_tree or eval_all_expressions:
            value = process_argument(
                value, objects_in_nspace, sel_object_names, selected_object_tree, nspace, eval_all_expressions
            )

        return value

    # Create the signature based on EXISTING plan from the workspace
    signature = inspect.signature(plans_in_nspace[plan_name])

    # Compare parameters in the signature and in the list of allowed plans. Make sure that the parameters
    #   in the list of allowed plans are a subset of the existing parameters (otherwise the plan can not
    #   be started). This not full validation.
    existing_names = set([_.name for _ in signature.parameters.values()])
    allowed_names = set([_["name"] for _ in group_plans[plan_name]["parameters"]])
    extra_names = allowed_names - existing_names
    if extra_names:
        raise RuntimeError(f"Plan description in the list of allowed plans has extra parameters {extra_names}")

    # Bind arguments of the plan
    bound_args = signature.bind(*plan_args, **plan_kwargs)
    # Separate dictionary for the default values define in the annotation decorator
    default_params = {}

    # Apply the default values defined in the annotation decorator. Default values defined in
    #   the decorator may still be converted to device references (e.g. str->ophyd.Device).
    for p in group_plans[plan_name]["parameters"]:
        if ("default" in p) and p.get("default_defined_in_decorator", False):
            if p["name"] not in bound_args.arguments:
                default_value = _process_default_value(p["default"])
                default_params.update({p["name"]: default_value})

    # Existing items include existing devices and existing plans
    items_in_nspace = devices_in_nspace.copy()
    items_in_nspace.update(plans_in_nspace)

    plan_func = process_argument(plan_name, plans_in_nspace, set(), group_plans, nspace, False)
    if isinstance(plan_func, str):
        success = False
        err_msg = f"Plan '{plan_name}' is not allowed or does not exist"

    # Map names to parameter descriptions
    param_descriptions = {_["name"]: _ for _ in group_plans[plan_name]["parameters"]}
    for p_name in bound_args.arguments:
        value = bound_args.arguments[p_name]
        # Fetch parameter description ({} if parameter is not found)
        pp = param_descriptions.get(p_name, {})
        bound_args.arguments[p_name] = process_parameter_value(
            value, pp, items_in_nspace, group_plans, group_devices, nspace
        )

    for p_name in default_params:
        value = default_params[p_name]
        # Fetch parameter description ({} if parameter is not found)
        pp = group_plans[plan_name].get(p_name, {})
        default_params[p_name] = process_parameter_value(
            value, pp, items_in_nspace, group_plans, group_devices, nspace
        )

    plan_args_parsed = list(bound_args.args)
    plan_kwargs_parsed = bound_args.kwargs
    plan_kwargs_parsed.update(default_params)

    # If metadata is a list of dictionaries, then merge the dictionaries into one
    #   with dictionaries with lower index having higher priority.
    success_meta = True
    if isinstance(plan_meta, (list, tuple)):
        p = {}
        for meta in reversed(plan_meta):
            if isinstance(meta, dict):
                p.update(meta)
            else:
                success_meta = False
        if success_meta:
            plan_meta = p
    elif not isinstance(plan_meta, dict):
        success_meta = False

    if not success_meta:
        success = False
        err_msg = f"Plan metadata must be a dictionary or a list of dictionaries: '{ppfl(plan_meta)}'"

    if not success:
        raise RuntimeError(f"Error while parsing the plan: {err_msg}")

    plan_prepared = {
        "callable": plan_func,
        "args": plan_args_parsed,
        "kwargs": plan_kwargs_parsed,
        "meta": plan_meta,
    }
    return plan_prepared


def prepare_function(*, func_info, nspace, user_group_permissions=None):
    """
    Prepare function for execution in the worker namespace. Returned dictionary
    contains reference to the callable object in the namespace, list of args and
    dictionary of kwargs. The function may check if the user is allowed to
    execute the function if ``user_group_permissions`` is provided (not ``None``).

    Parameters
    ----------
    func_info: dict
        Dictionary which contains keys: ``name``, ``user_group``, ``args`` (optional)
        and ``kwargs`` (optional).
    nspace: dict
        Reference to the RE Worker namespace.
    user_group_permissions: dict or None
        Dictionary that contains user group permissions. User permissions are not checked
        if ``None``.

    Returns
    -------
    dict
        Function data: ``callable`` is a reference to a callable object in RE Worker namespace,
        ``args`` is list of args and ``kwargs`` is a dictionary of kwargs.

    Raises
    ------
    RuntimeError
        Error while preparing the function.
    """

    if "name" not in func_info:
        raise RuntimeError(f"No function name is specified: {func_info!r}")

    if "user_group" not in func_info:
        raise RuntimeError(f"No user group is specified in parameters for the function: {func_info!r}")

    func_name = func_info["name"]
    user_group = func_info["user_group"]
    if user_group_permissions is not None:
        if not check_if_function_allowed(
            func_name, group_name=user_group, user_group_permissions=user_group_permissions
        ):
            raise RuntimeError(f"Function {func_name!r} is not allowed for users from {user_group!r} user group")

    func_args = list(func_info.get("args", []))
    func_kwargs = dict(func_info.get("kwargs", {}))

    if func_name not in nspace:
        raise RuntimeError(f"Function {func_name!r} is not found in the worker namespace")

    func_callable = nspace[func_name]

    # Following are basic checks to avoid most common errors. The list of check can be expanded.
    if not callable(func_callable):
        raise RuntimeError(f"Object {func_name!r} defined in worker namespace is not callable")
    # Generator functions defined in the worker namespace are plans, which return generators
    #   when called. There is no point in calling those functions directly.
    if inspect.isgeneratorfunction(func_callable):
        raise RuntimeError(f"Object {func_name!r} defined in worker namespace is a generator function")

    func_prepared = {
        "callable": func_callable,
        "args": func_args,
        "kwargs": func_kwargs,
    }
    return func_prepared


# ===============================================================================
#   Processing of plan annotations and plan descriptions

_supported_device_types = ("", "__READABLE__", "__FLYABLE__", "__DETECTOR__", "__MOTOR__")


def _split_name_pattern(name_pattern):
    """
    Split name pattern into components. The pattern may represent names of devices
    plans or functions. The patterns for device names may consist of multiple components,
    while the patterns from plan and function name always have one component.
    For devices, the list element may contain device or subdevice name (e.g. ``det1``,
    ``det1.val``, ``sim_stage.det.val``) or specify regular expressions used to find
    elements in the list of existing elements (e.g. ``:^det:^val$``, ``:-^det:^val$``,
    ``__DETECTORS__:-^sim_stage$:.+:^val$``). The ``-`` character immediately following
    ``:`` (``:-``) is not part of the regular expression. It indicates that all subdevices
    with matching regular expressions in this and previous pattern components are
    not included in the list but search continues to its subdevices. Optionally ``:``
    may be followed by ``+`` character, which indicates that the device must be included
    in the search result (this is the default behavior). The ``__DETECTORS__`` keyword
    indicates that only detectors that satisfy the conditions are included in the list.
    The following keywords are currently supported: ``__READABLE__``, ``__FLYABLE__``,
    ``__DETECTOR__`` and ``__MOTOR__``. The definition may also contain 'full name'
    patterns that are applyed to the remaining part of the subdevice name. The
    'full name' pattern is defined by putting '?' after ':', e.g. the pattern
    ``:?^det.*val$`` selects  all device and subdevice names that starts with ``det``
    and ends with ``val``, such as ``detector_val``, ``det.val``, ``det.somesubdevice.val``
    etc. The search depth may be limited by specifying ``depth`` parameter after
    the full name pattern, e.g. ``:?det.*val$:depth=5`` limits depth search to 5.
    Depth values starts with 1, e.g. in the example above, ``:?det.*val$:depth=1``
    will find only ``detector_val``. If 'full name' pattern is preceded with a number
    of subdevice name patterns, then 'full_name' pattern is applied to the remainder of
    the name, e.g. ``:^sim_stage$:?^det.*val$:depth=3`` would select ``sim_stage``
    and all its subdevices such as ``sim_stage.det5.val`` searching to the total depth
    of 4 (``sim_stage`` name is at level 1 and full name search is performed at levels
    2, 3 and 4).

    Note, that the last pattern component can not be deselected. For example ``:^det:-val$``
    is identical to ``:^det:val$``.

    The function may be applied to list elements describing plans, since they consist of
    the same set of components. The element may be a name, such as ``count`` or
    regular expression used to pick plans from the list of existing names, such as ``:^count``.
    Since plan names and patterns are much simpler, additional validation of the split
    results should be performed. The separators ``:+``, ``:-`` and ``:?`` can still be used
    with plan or function names, but they have no meaning and are ignored during processing.

    Parameters
    ----------
    name_pattern: str
        Name pattern of a device, plan or function.

    Returns
    -------
    components: list(tuple)
        List of tuples, each tuple contains the following items: regular expression or device/subdevice name,
        boolean flag that indicates if the matching is found at the current depth level should be included
        in the list (for regular expressions), boolean flag that indicates if regular expression should
        be applied to the full remaining part of the subdevice name (the full tree or subtree is going to
        be searched), depth (int, 1-based or ``None`` if not set) that limites depth of the tree searched
        for the name matching full name regular expressions.
    uses_re: boolean
        Boolean value that indicates if the components are regular expressions.
    device_type: str
        Contains the keyword that specifies device type (for regular expressions). Empty string if
        the device type is not specified or if the components are device/subdevice names.

    Raises
    ------
    TypeError
        The element definition is not a string.
    ValueError
        The element definition is incorrectly formatted and can not be processed.
    """
    if not isinstance(name_pattern, str):
        raise TypeError(
            f"Name pattern {name_pattern!r} has incorrect type {type(name_pattern)!r}. Expected type: 'str'"
        )

    # Remove spaces
    name_pattern = name_pattern.replace(" ", "")

    if not len(name_pattern):
        raise ValueError(f"Name pattern {name_pattern!r} is an empty string")

    # Check if the element is defined using regular expressions
    uses_re = ":" in name_pattern
    device_type = ""

    if uses_re:
        components = name_pattern.split(":")
        device_type = components[:1][0]  # The first element is a string (may be empty string)
        components = components[1:]

        # Find the components starting with '?'
        n_full_re = None
        for n, c in enumerate(components):
            if c.startswith("?"):
                n_full_re = n
                break

        depth = None
        if n_full_re is not None:
            # 'Full name' RE can be the last or next to last component.
            if n_full_re < len(components) - 2:
                raise ValueError(
                    f"Full name regular expression {components[n_full_re]!r} must be the last: {name_pattern!r}"
                )
            elif n_full_re == len(components) - 2:
                # If 'full name' RE is next to last, it can be followed only by 'depth' specification
                if not components[-1].startswith("depth="):
                    raise ValueError(
                        f"Full name regular expression {components[n_full_re]!r} can be only followed by "
                        f"the depth specification: {name_pattern!r}"
                    )
                elif not re.search("^depth=[0-9]+$", components[-1]):
                    raise ValueError(
                        f"Depth specification {components[-1]!r} has incorrect format: {name_pattern!r}"
                    )
                else:
                    _, depth = components.pop().split("=")  # Remove depth specification
                    depth = int(depth)

        if (depth is not None) and (depth < 1):
            raise ValueError(f"Depth ({depth}) must be positive integer greater or equal to 1: {name_pattern!r}")

        if device_type not in _supported_device_types:
            raise ValueError(
                f"Device type {device_type!r} is not supported. Supported types: {_supported_device_types}"
            )

        components_include = [False if _.startswith("-") else True for _ in components]
        components_include[-1] = True  # Always set the last component as included.

        components_full_re = [False] * len(components)
        components_depth = [None] * len(components)  # 'depth == None' means that depth is not specified
        if n_full_re is not None:
            components_full_re[n_full_re] = True
            components_depth[n_full_re] = depth

        # Remove '+' and '?' from the beginning of each component
        components = [
            _[1:] if _.startswith("+") or _.startswith("-") or _.startswith("?") else _ for _ in components
        ]

        for c in components:
            if not c:
                raise ValueError(f"Name pattern {name_pattern!r} contains empty components")
            try:
                re.compile(c)
            except re.error:
                raise ValueError(f"Name pattern {name_pattern!r} contains invalid regular expression {c!r}")

        components = list(zip(components, components_include, components_full_re, components_depth))

    else:
        if not re.search(r"^[_a-zA-Z]([_a-zA-Z0-9\.]*[_a-zA-Z0-9])?$", name_pattern):
            raise ValueError(
                f"Name pattern {name_pattern!r} contains invalid characters. "
                "The pattern could be a valid regular expression, but it is not labeled with ':' (e.g. ':^det$')"
            )
        components = name_pattern.split(".")
        for c in components:
            if not c:
                raise ValueError(
                    f"Plan, device or subdevice name in the description {name_pattern!r} is an empty string"
                )
        components_include = [True] * len(components)
        components_full_re = [False] * len(components)
        components_depth = [None] * len(components)

        components = list(zip(components, components_include, components_full_re, components_depth))

    return components, uses_re, device_type


def _is_object_name_in_list(object_name, *, allowed_objects):
    """
    Search for object (plan or device) name (e.g. ``count`` or ``det1.val``) in the tree
    of device names (list of allowed devices) and returns ``True`` if the device is in
    the list and ``False`` otherwise. If search for a device name n the tree is terminated
    prematurely or at the node which has optional ``excluded`` attribute set ``True``, then
    it is considered that the name is not in the list.

    Parameters
    ----------
    object_name: str
        Object name, such as ``count``, ``det1`` or ``det1.val``.
    allowed_objects: dict
        Dictionary that contains descriptions of allowed objects (plans and/or devices)
        for a user group.

    Returns
    -------
    bool
        ``True`` if device was found in the list, ``False`` if the device is not in the list or
        the ``object_name`` is not a valid name for the object.
    """

    try:
        components, uses_re, _ = _split_name_pattern(object_name)
        object_in_list = True
    except (TypeError, ValueError):
        object_in_list = False

    if object_in_list and uses_re:
        object_in_list = False

    if object_in_list:
        components = [_[0] for _ in components]  # We use only the 1st element
        if not components:
            object_in_list = False

    # The object name is valid, so search for the object in the list
    if object_in_list:
        root = None
        if components[0] in allowed_objects:
            root = allowed_objects[components[0]]
            object_in_list = not root.get("excluded", False)
        else:
            object_in_list = False

        if root:
            for c in components[1:]:
                if ("components" in root) and (c in root["components"]):
                    root = root["components"][c]
                    object_in_list = not root.get("excluded", False)
                else:
                    object_in_list = False
                    break

    return object_in_list


def _get_device_type_condition(device_type):
    """
    Generator function that returns reference to a function that verifies if device
    satisfy condition for the selecte device type.

    Parameters
    ----------
    device_type: str
        Deivce type

    Returns
    -------
    callable
    """
    # The condition is applied to the device to decide if it is included in the list
    if device_type == "":

        def condition(_btype):
            return True

    elif device_type == "__READABLE__":

        def condition(_btype):
            return bool(_btype["is_readable"])

    elif device_type == "__DETECTOR__":

        def condition(_btype):
            return _btype["is_readable"] and not _btype["is_movable"]

    elif device_type == "__MOTOR__":

        def condition(_btype):
            return _btype["is_readable"] and _btype["is_movable"]

    elif device_type == "__FLYABLE__":

        def condition(_btype):
            return bool(_btype["is_flyable"])

    else:
        raise ValueError(f"Unsupported device type: {device_type!r}. Supported types: {_supported_device_types}")

    return condition


def _build_device_name_list(*, components, uses_re, device_type, existing_devices):
    """
    Generate list of device names (including subdevices) based on one element of a device
    list from plan parameter annotation. The element is preprocessed with
    ``_split_list_element_definition()`` represented as ``components``, ``uses_re`` and
    ``device_type`` parameters. If the element is a device name, it is simply included
    in the list. If the element is a pattern, the matching devices are selected
    from ``existing_devices`` dictionary and added to the list. The ``components`` is
    a list of tuples for each depth level. The first element of the tuple represents
    a pattern device/subdevice name at the given level. If matching devices are found,
    they are added to the list of devices if the second tuple element is ``True`` and then
    the subdevices of each matching device are processed using the pattern defined for
    the next depth level (the second element may be ``True`` or ``False``).
    A pattern may also be a 'full name' regular expression applied to the remainder
    of the subdevice (if 3rd element of the tuple is True). Depth for 'full name' search
    can be restricted (4th component of the tuple). Depth may be integer (>=1) or ``None``
    (depth is not define, therefore considered infinite).

    The devices with set parameter ``'excluded': True`` are not included in the list.

    It is assumed that element definitions are first processed using
    ``_split_list_element_definition``, which catches and processes all the errors,
    so this function does not perform any validation of data integrity.

    Parameters
    ----------
    components: list(tuple)
        List of tuples, each tuple consists of four elements: device/subdevice name or
        pattern for name search; a boolean flag that tells if the matching devices or
        subdevices at this level should be added to the device list; a boolean flag
        that tells if the pattern should be applied to the remainder of the device name
        (full name search); integer value that limits the depth of full name search
        (``None`` if the depth is not limited).
    uses_re: boolean
        Boolean parameter that tells if the components represent device name or pattern based
        on regular expressions.
    device_type: str
        If components define the pattern, then device type is used to limit search to the devices
        of certain type.
    existing_devices: dict
        The list of existing (or allowed) devices.

    Returns
    -------
    list(str)
        List of device names.
    """

    condition = _get_device_type_condition(device_type)

    if uses_re:
        max_depth, device_list = len(components), []

        def process_devices_full_name(devices, base_name, name_suffix, pattern, depth, depth_stop):
            if (depth_stop is None) or (depth < depth_stop):
                for dname, dparams in devices.items():
                    name_suffix_new = dname if not name_suffix else (name_suffix + "." + dname)
                    full_name = name_suffix_new if not base_name else (base_name + "." + name_suffix_new)
                    if (
                        re.search(pattern, name_suffix_new)
                        and not dparams.get("excluded", False)
                        and condition(dparams)
                    ):
                        device_list.append(full_name)
                    if "components" in dparams:
                        process_devices_full_name(
                            devices=dparams["components"],
                            base_name=base_name,
                            name_suffix=name_suffix_new,
                            pattern=pattern,
                            depth=depth + 1,
                            depth_stop=depth_stop,
                        )

        def process_devices(devices, base_name, depth):
            if (depth < max_depth) and devices:
                pattern, include_devices, is_full_re, remaining_depth = components[depth]
                if is_full_re:
                    process_devices_full_name(
                        devices=devices,
                        base_name=base_name,
                        name_suffix="",
                        pattern=pattern,
                        depth=depth,
                        depth_stop=depth + remaining_depth if remaining_depth else None,
                    )
                else:
                    for dname, dparams in devices.items():
                        if re.search(pattern, dname):
                            full_name = dname if not base_name else (base_name + "." + dname)
                            if include_devices and not dparams.get("excluded", False) and condition(dparams):
                                device_list.append(full_name)
                            if "components" in dparams:
                                process_devices(
                                    devices=dparams["components"], base_name=full_name, depth=depth + 1
                                )

        process_devices(devices=existing_devices, base_name="", depth=0)

    else:
        # Simplified approach: just copy the device name to list. No checks are performed.
        # Change the code in this block if device names are to be handled differently.
        device_name = ".".join([_[0] for _ in components])
        device_list = [device_name]

    return device_list


def _filter_device_tree(item_dict, allow_patterns, disallow_patterns):
    """
    Filter device tree based on sets of patterns for 'allowed' and 'disallowed'
    devices.

    Parameters
    ----------
    item_dict: dict
        Dictionary that represents a tree of existing devices. Some of the devices
        may be labeled as excluded: those devices will remain excluded and could be
        removed from the tree.
    allowed_patterns: list(str)
        List of patterns from ``allowed_devices`` section of user group permissions.
        If the parameter value is ``[]`` or the first list element is ``None``,
        then it is assumed that all the devices are allowed.
    disallow_patterns: list(str)
        List of patterns from ``forbidden_devices`` section of user group permissions.
        If the parameter value is ``[]`` or the first list element is ``None``,
        then it is assumed that no devices are forbidden.

    Returns
    -------
    allowed_devices: dict
        Dictionary that represents a tree of devices after filtering. Excluded
        devices could be labeled as excluded (parameter ``"exclude": True``).
        Branches that consist of excluded parameters are removed from the tree.

    Raises
    ------
        May raise exceptions if provided patterns are invalid and/or filtering
        can not be completed.
    """

    # Add temporary flag ('excl': False) to each device
    def add_excl_flag(devices_dict, exclude_all):
        """
        Add temporary ``excl`` parameter to each device and subdevice.

        Parameters
        ----------
        devices_dict: dict
            Dictionary that contains the device tree
        exclude_all: boolean
            The value for the ``excl`` parameter.
        """
        for _, dparam in devices_dict.items():
            dparam["excl"] = exclude_all
            if "components" in dparam:
                add_excl_flag(dparam["components"], exclude_all)

    def apply_pattern_filter(*, pattern, devices_dict, exclude):
        """
        Apply a pattern to the tree of deivce. Pattern can be an explicitly state name
        such as ``"det"`` or ``"det.val"`` or regular expression, such as ``":^det:val$"``.

        Parameters
        ----------
        pattern_item: str
            Device/subdevice name or regular expression to be applied to the device names.
        existing_devices: dict
            Dictionary that represents a tree of devices/subdevices.
        exclude: boolean
            True - exclude devices, False - include devices.

        Returns
        -------
        None
        """
        components, uses_re, device_type = _split_name_pattern(pattern)

        if uses_re:
            condition = _get_device_type_condition(device_type)
            max_depth = len(components)

            def apply_filter_full_name(devices, name_suffix, pattern, depth, depth_stop, *, exclude):
                if (depth_stop is None) or (depth < depth_stop):
                    for dname, dparams in devices.items():
                        name_suffix_new = dname if not name_suffix else (name_suffix + "." + dname)
                        if re.search(pattern, name_suffix_new) and condition(dparams):
                            dparams["excl"] = exclude
                        if "components" in dparams:
                            apply_filter_full_name(
                                devices=dparams["components"],
                                name_suffix=name_suffix_new,
                                pattern=pattern,
                                depth=depth + 1,
                                depth_stop=depth_stop,
                                exclude=exclude,
                            )

            def apply_filter(devices, depth, *, exclude):
                if (depth < max_depth) and devices:
                    pattern, include_devices, is_full_re, remaining_depth = components[depth]
                    if is_full_re:
                        apply_filter_full_name(
                            devices=devices,
                            name_suffix="",
                            pattern=pattern,
                            depth=depth,
                            depth_stop=depth + remaining_depth if remaining_depth else None,
                            exclude=exclude,
                        )
                    else:
                        for dname, dparams in devices.items():
                            if re.search(pattern, dname):
                                if include_devices and condition(dparams):
                                    dparams["excl"] = bool(exclude)
                                if "components" in dparams:
                                    apply_filter(devices=dparams["components"], depth=depth + 1, exclude=exclude)

            apply_filter(devices=devices_dict, depth=0, exclude=exclude)

        else:
            # 'components' represent an explicitly stated name
            root = allowed_devices
            for n, c in enumerate([_[0] for _ in components]):
                if c in root:
                    if n == len(components) - 1:
                        root[c]["excl"] = exclude
                    if "components" in root[c]:
                        root = root[c]["components"]
                    else:
                        break
                else:
                    break

    def trim_device_dict(devices_dict):
        """
        Remove ``excl`` parameter from each device. Remove branches that contain only
        the devices that were excluded. Add ``"excluded": True`` to the devices that
        were excluded but can not be removed from the tree (``"excl": True``).

        Parameters
        ----------
        devices_dict: dict
            Dictionary representing the tree of devices.
        """
        contains_devices = False
        for dname in list(devices_dict.keys()):
            dparam = devices_dict[dname]

            exclude_this_device = True

            if "components" in dparam:
                if trim_device_dict(dparam["components"]):
                    exclude_this_device = False
                else:
                    del dparam["components"]

            if dparam.get("excluded", False) or (dparam["excl"] is True):
                dparam["excluded"] = True
            else:
                exclude_this_device = False

            if exclude_this_device:
                # Device is excluded and contains no subdevices that were not excluded.
                del devices_dict[dname]
            else:
                del dparam["excl"]
                contains_devices = True

        return contains_devices

    allowed_devices = copy.deepcopy(item_dict)

    # Process the case when all devices/subdevices are allowed
    allow_all = len(allow_patterns) and (allow_patterns[0] is None)

    add_excl_flag(allowed_devices, exclude_all=not allow_all)

    # Process 'allow_patterns'
    if not allow_all:
        for pattern in allow_patterns:
            apply_pattern_filter(pattern=pattern, devices_dict=allowed_devices, exclude=False)

    # Process 'disallow_patterns'
    if len(disallow_patterns) and (disallow_patterns[0] is not None):
        for pattern in disallow_patterns:
            apply_pattern_filter(pattern=pattern, devices_dict=allowed_devices, exclude=True)

    trim_device_dict(allowed_devices)

    return allowed_devices


def _build_plan_name_list(*, components, uses_re, device_type, existing_plans):
    """
    Parameters
    ----------
    components: list(tuple)
        List of tuples returned by ``_split_list_element_definition()``. For the plans
        the list must contain exactly one component, elements 2 and 3 of each tuple
        must be ``False`` and element 4 must be ``None``. Exception will be raised if those
        requirements are not satisfied (this is part of normal operation, it means that
        the definition contains elements, such as ``+`` or ``?`` that are not supported
        for plans, so the user should be informed that the definition can not be processed)
    uses_re: boolean
        Boolean parameter that tells if the components represent device name or pattern based
        on regular expressions.
    device_type: str
        This parameter is returned by ``_split_list_element_definition()`` and must be
        an empty string ``""``. Exception will be raised otherwise (device type can not be
        used in the pattern defining a plan).
    existing_plans: set or dict
        A set of plan names or a list (dictionary) of existing (or allowed) plans.

    Returns
    -------
    list(str)
        List of device names.
    """

    if len(components) != 1:
        c = [_[0] for _ in components]
        raise ValueError(f"List element describing a plan may contain only one component. Components: {c}")

    # At this point we know that 'components' has precisely one element.
    pattern, include_devices, is_full_re, remaining_depth = components[0]

    if remaining_depth is not None:
        raise ValueError(f"Depth specification can not be part of the name pattern for a plan: {pattern!r}")

    if device_type:
        raise ValueError(
            f"Device type can not be included in the name pattern for a plan: {(device_type + ':')!r}"
        )

    plan_list = []

    if uses_re:
        for plan_name in existing_plans:
            if re.search(pattern, plan_name):
                plan_list.append(plan_name)
    else:
        # Simplified approach: just add the plan name to the list. No checks are performed.
        # Change the code in this block if device names are to be handled differently.
        plan_list = [pattern]

    return plan_list


def _expand_parameter_annotation(annotation, *, existing_devices, existing_plans):
    """
    Expand the lists if plans and device in annotation. The function does not filter
    the lists: all names of plans and devices that are explicitly defined in the lists
    are copied to the new lists. Plans and devices that are defined using regular
    expressions are searched in the lists of existing plans and devices respectively.

    Parameters
    ----------
    annotation: dict
        Parameter annotation.
    existing_devices: dict, None
        Dictionary with allowed or existing devices. If ``None``, then all the expanded lists
        will be empty.
    existing_plans: set, dict or None
        A set of existng plan names or a dictionary where the keys are the existing plan names.
        The function does not require complete plan descriptions, since they are typically not
        available on this stage. If ``None``, then all the expanded lists will be empty.

    Returns
    -------
    dict
        Expanded annotation. Always returns deep copy, so the original annotation remains unchanged
    """
    existing_devices = existing_devices or {}
    existing_plans = existing_plans or {}

    annotation = copy.deepcopy(annotation)
    for section in ("devices", "plans"):
        if section not in annotation:
            continue
        for k, v in annotation[section].items():
            if not isinstance(v, (list, tuple)):
                raise ValueError(f"Unsupported type of a device or plan list {k!r}: {v}")
            item_list = []
            for d in v:
                components, uses_re, device_type = _split_name_pattern(d)
                if section == "plans":
                    name_list = _build_plan_name_list(
                        components=components,
                        uses_re=uses_re,
                        device_type=device_type,
                        existing_plans=existing_plans,
                    )
                else:
                    name_list = _build_device_name_list(
                        components=components,
                        uses_re=uses_re,
                        device_type=device_type,
                        existing_devices=existing_devices,
                    )
                item_list.extend(name_list)
            # Remove repeated entries
            item_list = list(set(item_list))
            # Sort the list of names by lower case letters
            item_list = sorted(item_list, key=lambda _: (_.upper(), _[0].islower()))
            annotation[section][k] = item_list

    # Process enums: convert tuples to lists
    if "enums" in annotation:
        an_enums = annotation["enums"]
        for k in an_enums.keys():
            an_enums[k] = list(an_enums[k])

    return annotation


def _expand_plan_description(plan_description, *, existing_devices, existing_plans):
    """
    Expand lists of items for custom enum types for each parameter in the plan description.
    After expansion, plan description is ready for use in plan validation. The function
    does not change the plan description if there are no lists to expand, therefore it can
    be repeatedly applied to the same plan description without consequences (except extra
    execution time). The function always returnes a COPY of the plan description, so
    it is safe to apply to elements of the list of allowed/existing plans without changing
    the original descriptions.

    Parameters
    ----------
    plan_description: dict
        Plan description. The format of plan description is the same as in the list of
        allowed/existing plans. The function will not change the original dictionary,
        which is referenced by this parameter.
    existing_devices: dict, None
        Dictionary with allowed or existing devices. If ``None``, then all the expanded lists
        will be empty.
    existing_plans: set, dict or None
        A set of existng plan names or a dictionary where the keys are the existing plan names.
        The function does not require complete plan descriptions, since they are typically not
        available on this stage. If ``None``, then all the expanded lists will be empty.

    Returns
    -------
    dict
        Expanded plan description, which contains full lists of items. Always returns the
        copy of the plan description.

    Raises
    ------
        Exceptions are raised if plan description can not be properly processed.
    """
    plan_description = copy.deepcopy(plan_description)

    if "parameters" in plan_description:
        for p in plan_description["parameters"]:
            if "annotation" in p:
                p["annotation"] = _expand_parameter_annotation(
                    p["annotation"], existing_devices=existing_devices, existing_plans=existing_plans
                )

    return plan_description


# ===============================================================================
#   Validation of plan parameters


def _convert_str_to_number(value_in):
    """
    Interpret a string ``value_in`` as ``int`` or ``float`` (whichever is sufficient).
    Raise the exception if conversion fails.
    """
    try:
        # Try to convert string value to float
        if isinstance(value_in, str):
            value_out = float(value_in)
        else:
            raise ValueError(f"Value '{value_in}' is not a string (type: {type(value_in)})")
        if int(value_out) == value_out:
            value_out = int(value_out)
    except Exception as ex:
        raise ValueError(f"Failed to interpret the value {value_in!r} as integer or float number: {ex}")
    return value_out


def _compare_types(object_in, object_out):
    """
    Compares types of parameters. If both parameters are iterables (list or tuple),
    then the function is recursively called for each element. A set of rules is
    applied to types to determine if the types are matching.
    """

    def both_iterables(x, y):
        return isinstance(x, Iterable) and isinstance(y, Iterable) and not isinstance(x, str)

    def both_mappings(x, y):
        return isinstance(x, Mapping) and isinstance(y, Mapping)

    if both_iterables(object_in, object_out):
        match = True
        for o_in, o_out in zip(object_in, object_out):
            if not _compare_types(o_in, o_out):
                match = False
    elif both_mappings(object_in, object_out):
        match = True
        for k in object_in.keys():
            if k not in object_out:
                match = False
            if not _compare_types(object_in[k], object_out[k]):
                match = False
    else:
        match = False
        # This is the set of rules used to determine if types are matching.
        #   Types are required to be identical, except in the following cases:
        if isinstance(object_out, type(object_in)):
            match = True

        # 'int' is allowed to be converted to float
        if isinstance(object_out, float) and isinstance(object_in, int):
            match = True

        # Conversion to Enum is valid (parameters often contain values, that are converted to Enums).
        elif issubclass(type(object_out), enum.Enum) and object_in == object_out.value:
            match = True

    return match


def _compare_in_out(kwargs_in, kwargs_out):
    """
    Compares input and output kwargs for ``pydantic`` model. Accumulates and returns
    the error message that includes the data on all parameters with mismatching types.
    """
    match, msg = True, ""
    for k, v in kwargs_out.items():
        if k in kwargs_in:
            valid = _compare_types(kwargs_in[k], v)
            if not valid:
                msg = f"{msg}\n" if msg else msg
                msg += (
                    f"Incorrect parameter type: key='{k}', "
                    f"value='{kwargs_in[k]}' ({type(kwargs_in[k])}), "
                    f"interpreted as '{v}' ({type(v)})"
                )
                match = False
    return match, msg


def _check_ranges(kwargs_in, param_list):
    """
    Check if each parameter in ``kwargs_in`` is within the range limited by
    the minimum and maximum values if the range is specified in plan parameter
    descriptions.
    """

    def process_argument(v, v_min, v_max):
        queue = [v]

        while queue:
            v_cur = queue.pop(0)

            if isinstance(v_cur, numbers.Number):
                out_of_range = False
                if (v_min is not None) and (v_cur < v_min):
                    out_of_range = True
                if (v_max is not None) and (v_cur > v_max):
                    out_of_range = True
                if out_of_range:
                    s_v_min = f"{v_min}" if v_min is not None else "-inf"
                    s_v_max = f"{v_max}" if v_max is not None else "inf"
                    raise ValueError(f"Value {v_cur} is out of range [{s_v_min}, {s_v_max}]")
            elif isinstance(v_cur, dict):
                queue.extend(v_cur.values())
            elif isinstance(v_cur, Iterable) and not isinstance(v_cur, str):
                queue.extend(v_cur)

    match, msg = True, ""
    for p in param_list:
        k = p["name"]
        v_min = p.get("min", None)
        v_max = p.get("max", None)

        # Perform check only if the parameter value is passed to the plan and
        #   v_max or v_min is specified.
        if (k in kwargs_in) and (v_min is not None or v_max is not None):
            v = kwargs_in[k]

            if v_min is not None:
                v_min = _convert_str_to_number(v_min)
            if v_max is not None:
                v_max = _convert_str_to_number(v_max)

            try:
                process_argument(v, v_min, v_max)

            except Exception as ex:
                msg = f"{msg}\n" if msg else msg
                msg += f"Parameter value is out of range: key='{k}', " f"value='{kwargs_in[k]}': {ex}"
                match = False

    return match, msg


def _find_and_replace_built_in_types(annotation_type_str, *, plans=None, devices=None, enums=None):
    """
    Find and replace built-in types in parameter annotation type string. Built-in types are
    ``__PLAN__``, ``__DEVICE__``, ``__PLAN_OR_DEVICE__``, ``__READABLE__``, ``__MOVABLE__``,
    ``__FLYABLE__``, ``__CALLABLE__``.
    Since plan and device names are passed as strings and then converted to respective object,
    the built-in plan names are replaced by ``str`` type, which is then used in parameter validation.
    The function also determines whether plans, devices or both plans and devices need to be converted.
    If the name of the built-in type is redefined in ``plans``, ``devices`` or ``enums`` section,
    then the keyword is ignored.

    Parameters
    ----------
    annotation_type_str: str
        String that represents a type from parameter annotation
    plans: dict or None
        Reference to the ``plans`` section of the parameter annotation
    devices: dict or None
        Reference to the ``devices`` section of the parameter annotation
    enums: dict or None
        Reference to the ``enum`` section of the parameter annotation

    Returns
    -------
    annotation_type_str: str
        Modified parameter type
    convert_values: dict
        Dictionary that contains information on how the parameter values should be processed.
        Keys: ``convert_plan_names``, ``convert_device_names``, ``eval_expressions``.
    """
    # Built-in types that may be contained in 'annotation_type_str' and
    #   should be converted to another known type
    plans = plans or {}
    devices = devices or {}
    enums = enums or {}

    built_in_types = {
        "__PLAN__": ("str", True, False, False),
        "__DEVICE__": ("str", False, True, False),
        "__PLAN_OR_DEVICE__": ("str", True, True, False),
        "__READABLE__": ("str", False, True, False),
        "__MOVABLE__": ("str", False, True, False),
        "__FLYABLE__": ("str", False, True, False),
        "__CALLABLE__": ("str", False, False, True),
    }
    convert_plan_names = False
    convert_device_names = False
    eval_expressions = False

    for btype, (btype_replace, pl, dev, ev_expr) in built_in_types.items():
        # If 'plans', 'devices' or 'enums' contains the type name, then leave it as is
        if (btype in plans) or (btype in devices) or (btype in enums):
            continue
        if re.search(btype, annotation_type_str):
            annotation_type_str = re.sub(btype, btype_replace, annotation_type_str)
            convert_plan_names = convert_plan_names or pl
            convert_device_names = convert_device_names or dev
            eval_expressions = eval_expressions or ev_expr

    convert_values = dict(
        convert_plan_names=convert_plan_names,
        convert_device_names=convert_device_names,
        eval_expressions=eval_expressions,
    )
    return annotation_type_str, convert_values


def _process_annotation(encoded_annotation, *, ns=None):
    """
    Processed annotation is encoded the same way as in the descriptions of existing plans.
    Returns reference to the annotation (type object) and the list of temporary types
    that needs to be deleted once the processing (parameter validation) is complete.
    The built-in type names (__DEVICE__, __PLAN__, __PLAN_OR_DEVICE__, ``__READABLE__``,
    ``__MOVABLE__``, ``__FLYABLE__``, ``__CALLABLE__``) are replaced with ``str`` unless types with
    the same name are defined in ``plans``, ``devices`` or ``enums`` sections of the annotation.
    Explicitly defined the types with the same names as built-in types are treated as regular types.

    The function validates annotation and raises exceptions in the following cases:
    the types defined in 'plans', 'devices' or 'enums' sections are not lists or tuples;
    the types defined in 'plans', 'devices' or 'enums' sections are not used as part
    of the parameter type (may indicate a bug in the user code); annotation contains
    unsupported keys.

    This function does not change the annotation.

    Parameters
    ----------
    encoded_annotation : dict
        Dictionary that contains encoded annotation. The dictionary may contain the following
        keys: ``type`` - type represented as a string, ``devices`` - a dictionary that maps
        device types and lists of device names, ``plans`` - similar dictionary for plan types
        and ``enums`` - similar dictionary for simple enums (strings).
    ns : dict
        Namespace that is used to evaluate the annotations. Custom types with unique names
        may be added to the namesace by the function.

    Returns
    -------
    annotation_type: type
        Type reconstructed from annotation.
    convert_values: dict
        Dictionary that contains information on how the parameter values should be processed.
        Keys: ``convert_plan_names``, ``convert_device_names``, ``eval_expressions``.
    ns: dict
        Namespace dictionary with created types.
    """
    import bluesky

    # Namespace that contains the types created in the process of processing the annotation
    ns = ns or {}
    if "typing" not in ns:
        ns.update({"typing": typing})
    if "collections" not in ns:
        ns.update({"collections": collections})
    if "bluesky" not in ns:
        ns.update({"bluesky": bluesky})
    if "enum" not in ns:
        ns.update({"enum": enum})
    if "NoneType" not in ns:
        ns.update({"NoneType": type(None)})

    annotation_type_str = "<not-specified>"
    annotation_type = None

    try:
        if "type" not in encoded_annotation:
            raise ValueError("Type is not specififed")

        # Verify that the annotation does not have any unsupported keys
        allowed_keys = ("type", "devices", "plans", "enums")
        invalid_keys = []
        for k in encoded_annotation:
            if k not in allowed_keys:
                invalid_keys.append(k)
        if invalid_keys:
            raise ValueError(
                f"Annotation contains unsupported keys: {invalid_keys}. Supported keys: {allowed_keys}"
            )

        annotation_type_str = encoded_annotation.get("type")  # Required!
        devices = encoded_annotation.get("devices", {})
        plans = encoded_annotation.get("plans", {})
        enums = encoded_annotation.get("enums", {})
        items = devices.copy()
        items.update(plans)
        items.update(enums)

        annotation_type_str, convert_values = _find_and_replace_built_in_types(
            annotation_type_str, plans=plans, devices=devices, enums=enums
        )

        # Create types
        for item_name in items:
            # The dictionary value for the devices/plans may be a list(tuple) of items.
            if not isinstance(items[item_name], (list, tuple)):
                raise TypeError(
                    f"The list of items ({item_name!r}: {items[item_name]!r}) must be a list of a tuple."
                )

            if item_name not in annotation_type_str:
                raise ValueError(
                    f"Type {item_name!r} is defined in the annotation, but not used in the parameter type"
                )

            # Create temporary unique type name
            type_name = type_name_base = item_name
            while True:
                # Try the original name first
                if type_name not in ns:
                    break
                type_name = f"{type_name_base}_{random.randint(0, 100000)}_"

            # Replace all occurrences of the type name in the custom annotation.
            annotation_type_str = annotation_type_str.replace(item_name, type_name)

            # Create temporary type as a subclass of enum.Enum, e.g.
            # enum.Enum('Device', {'det1': 'det1', 'det2': 'det2', 'det3': 'det3'})
            type_code = f"enum.Enum('{type_name}', {{"
            for d in items[item_name]:
                type_code += f"'{d}': '{d}',"
            type_code += "})"

            ns[type_name] = eval(type_code, ns, ns)

        # Once all the types are created,  execute the code for annotation.
        annotation_type = eval(annotation_type_str, ns, ns)

    except Exception as ex:
        raise TypeError(f"Failed to process annotation '{annotation_type_str}': {ex}'")

    return annotation_type, convert_values, ns


def _process_default_value(encoded_default_value):
    """
    Evaluates the default value represented as a string.

    Parameters
    ----------
    encoded_default_value : str
        Default value represented as a string (e.g. "10" or "'some-str'"

    Returns
    -------
    p_default
        Default value.

    Raises
    ------
    ValueError
        Raised if evaluation of the string containing the default value fails
    """
    try:
        p_default = ast.literal_eval(encoded_default_value)
    except Exception as ex:
        raise ValueError(f"Failed to decode the default value '{encoded_default_value}': {ex}")
    return p_default


def _decode_parameter_types_and_defaults(param_list):
    """
    Decode parameter types and default values by using string representations of
    types and defaults stored in list of available devices.

    Parameters
    ----------
    parameters : list(dict)
        List of dictionaries that contains annotations and default values of parameters.
        Used keys of the dictionaries: ``annotation`` and ``default``. If ``annotation``
        key is missing, then the the type is set to ``typing.Any``. If ``default`` is
        missing, then the default value is set to ``inspect.Parameter.empty``.

    Returns
    -------
    dict
        Mapping of parameter names to the dictionaries containing type (``type`` key)
        and default value (``default`` key).
    """

    decoded_types_and_defaults = {}
    for p in param_list:
        if "name" not in p:
            raise KeyError(f"No 'name' key in the parameter description {p}")

        if "annotation" in p:
            p_type, _, _ = _process_annotation(p["annotation"])
        else:
            p_type = typing.Any

        if "default" in p:
            p_default = _process_default_value(p["default"])
        else:
            p_default = inspect.Parameter.empty

        decoded_types_and_defaults[p["name"]] = {"type": p_type, "default": p_default}

    return decoded_types_and_defaults


def construct_parameters(param_list, *, params_decoded=None):
    """
    Construct the list of ``inspect.Parameter`` parameters based on the parameter list.

    Parameters
    ----------
    param_list : list(dict)
        List of item (plan) parameters as it is stored in the list of existing plans.
        Used keys of each parameter dictionary: ``name``, ``kind``, ``default``,
        ``annotation``.
    params_decoded : dict or None
        Dictionary that maps parameter name to the decoded parameter type
        and default value. The dictionary is created by calling
        ``_decode_parameter_types_and_defaults()``.  If the value is ``None``,
        then the decoded parameters are created automatically from the list ``param_list``.
        The parameter is intended for cases when the instantiated values are used repeatedly
        to avoid unnecessary multiple calls to ``_decode_parameter_types_and_defaults()``.

    Returns
    -------
    parameters : inspect.Parameters
        Item (plan) parameters represented in the form accepted by ``inspect.Signature``:
        ``sig=inspect.Signature(parameters)``.

    Raises
    ------
    Exceptions with meaningful error messages are generated if parameters can not be
    instantiated.
    """
    if params_decoded is None:
        params_decoded = _decode_parameter_types_and_defaults(param_list)

    parameters = []
    try:
        for p in param_list:
            p_name = p.get("name", None)
            p_kind = p.get("kind", {}).get("value", None)

            if p_name is None:
                raise ValueError(f"Description for parameter contains no key 'name': {p}")
            if p_kind is None:
                raise ValueError(f"Description for parameter contains no key 'kind': {p}")

            # The following values are ALWAYS created by '_instantiate_parameter_types_and_defaults'
            p_type = params_decoded[p_name]["type"]
            p_default = params_decoded[p_name]["default"]

            param = inspect.Parameter(name=p_name, kind=p_kind, default=p_default, annotation=p_type)
            parameters.append(param)

    except Exception as ex:
        raise ValueError(f"Failed to construct 'inspect.Parameters': {ex}")

    return parameters


def pydantic_construct_model_class(parameters):
    """
    Construct the 'pydantic' model based on parameters.

    Parameters
    ----------
    parameters : inspect.Parameters
        Parameters of the plan with annotations created by ``_create_parameters`` function.

    Returns
    -------
    class
        Dynamically created Pydantic model class
    """
    model_kwargs = {}
    for p in parameters:
        if hasattr(p, "default") and p.default != inspect.Parameter.empty:
            default = p.default
        else:
            if p.kind == p.VAR_POSITIONAL:
                default = []
            elif p.kind == p.VAR_KEYWORD:
                default = {}
            else:
                default = ...
        if hasattr(p, "annotation"):
            if p.kind == p.VAR_POSITIONAL:
                annotation = typing.List[p.annotation]
            elif p.kind == p.VAR_KEYWORD:
                annotation = typing.Dict[str, p.annotation]
            else:
                annotation = p.annotation
        else:
            annotation = None
        if annotation is not None:
            model_kwargs[p.name] = (annotation, default)
        else:
            model_kwargs[p.name] = default
    return pydantic.create_model("Model", **model_kwargs)


def pydantic_validate_model(kwargs, pydantic_model_class):
    """
    Validate the function parameters using 'pydantic' model based
    (by instantiating the model using model class and bound model parameters).

    Parameters
    ----------
    kwargs: dict
        Bound parameters of the function call.
    pydantic_model_class : class
        Pydantic model class (created using ``pydantic_construct_model_class()``).

    Returns
    -------
    class
        Constructed Pydantic model class
    """

    # Verify the arguments by instantiating the 'pydantic' model
    return pydantic_model_class(**kwargs)


def _validate_plan_parameters(param_list, call_args, call_kwargs):
    """
    Validate plan parameters based on parameter annotations.

    Parameters
    ----------
    param_list: dict
        The list of parameters with annotations (including custom annotations if available).
    call_args: list
        'args' of the plan
    call_kwargs: dict
        'kwargs' of the plan

    Returns
    -------
    dict
        Bound arguments of the called plan.

    Raises
    ------
    Exception
        Exception is raised if the plan is invalid. The exception message contains
        information on the error.
    """
    try:
        # Reconstruct 'inspect.Parameter' parameters based on the parameter list
        parameters = construct_parameters(param_list)

        # Create signature based on the list of parameters
        sig = inspect.Signature(parameters)

        # Verify the list of parameters based on signature.
        bound_args = sig.bind(*call_args, **call_kwargs)

        pydantic_model_class = pydantic_construct_model_class(parameters)
        m = pydantic_validate_model(bound_args.arguments, pydantic_model_class)

        # The next step: match types of the output value of the pydantic model with input
        #   types. 'Pydantic' is converting types whenever possible, but we need precise
        #   type checking with a fiew exceptions
        # NOTE: in the next statement we are using 'm.__dict__' instead of officially
        #   recommended 'm.dict()', because 'm.dict()' was causing performance problems
        #   when validating large batches of plans. Based on testing, 'm.__dict__' seems
        #   to work fine in this application.
        # NOTE: Pydantic 2 performs strict type checking and some checks performed by
        #   '_compare_in_out' may not be needed.
        success, msg = _compare_in_out(bound_args.arguments, m.__dict__)
        if not success:
            raise ValueError(f"Error in argument types: {msg}")

        # Finally check the ranges of parameters that have min. and max. are defined
        success, msg = _check_ranges(bound_args.arguments, param_list)
        if not success:
            raise ValueError(f"Argument values are out of range: {msg}")

    except Exception:
        raise

    return bound_args.arguments


def filter_plan_description(plan_description, *, allowed_plans, allowed_devices):
    """
    Filter plan description from the list of existing plans (modify parameters
    ``plan["annotation"]["plans"]`` and ``plan["annotation"]["devices"]``)
    to include only plans and devices from the lists of allowed plans and allowed devices
    for the given group. Creates and returns a modified copy of the plan.

    Parameters
    ----------
    plan_description: dict
        The dictionary containing plan description from the list of existing plans
    allowed_plans: dict or None
        The dictionary with allowed plans: key - plan name. If None, then
        all plans are allowed (may change in the future). If ``{}`` then no
        plans are allowed.
    allowed_devices: dict or None
        The dictionary with allowed devices: key - device name. If None, then
        all devices are allowed (may change in the future). If ``{}`` then no
        devices are allowed.

    Returns
    -------
    dict
        The dictionary with modified plan description.
    """
    plan_description = copy.deepcopy(plan_description)

    if "parameters" in plan_description:
        param_list = plan_description["parameters"]

        # Filter 'devices' and 'plans' entries of 'param_list'. Leave only plans that are
        #   in 'allowed_plans' and devices that are in 'allowed_devices'
        for p in param_list:
            if "annotation" in p:
                if (allowed_plans is not None) and ("plans" in p["annotation"]):
                    p_plans = p["annotation"]["plans"]
                    for p_type in p_plans:
                        p_plans[p_type] = [
                            _ for _ in p_plans[p_type] if _is_object_name_in_list(_, allowed_objects=allowed_plans)
                        ]
                if (allowed_devices is not None) and ("devices" in p["annotation"]):
                    p_dev = p["annotation"]["devices"]
                    for p_type in p_dev:
                        p_dev[p_type] = [
                            _ for _ in p_dev[p_type] if _is_object_name_in_list(_, allowed_objects=allowed_devices)
                        ]
    return plan_description


def _filter_allowed_plans(*, allowed_plans, allowed_devices):
    """
    Apply ``filter_plan_description`` to each plan in the list of allowed plans so that
    only the allowed plans and devices are left in the enums for ``plans`` and ``devices``.

    Parameters
    ----------
    allowed_plans: dict or None
        The dictionary with allowed plans: key - plan name. If None, then
        all plans are allowed (may change in the future). If ``{}`` then no
        plans are allowed.
    allowed_devices: dict or None
        The dictionary with allowed devices: key - device name. If None, then
        all devices are allowed (may change in the future). If ``{}`` then no
        devices are allowed.

    Returns
    -------
    dict
       The modified copy of ``allowed_plans``.
    """
    return {
        k: filter_plan_description(v, allowed_plans=allowed_plans, allowed_devices=allowed_devices)
        for k, v in allowed_plans.items()
    }


def validate_plan(plan, *, allowed_plans, allowed_devices):
    """
    Validate the dictionary of plan parameters. The function is called before the plan
    is added to the queue. The function can also be called by the client application
    to validate a plan before it is submitted to the queue. The dictionaries
    ``allowed_plans`` and ``allowed_devices`` must contain plans and devices
    that are allowed for the user and can be downloaded from the server.

    Parameters
    ----------
    plan: dict
        The dictionary of plan parameters
    allowed_plans: dict or None
        The dictionary with allowed plans: key - plan name. If None, then
        all plans are allowed (may change in the future). If ``{}`` then no
        plans are allowed.
    allowed_devices: dict or None
        The dictionary with allowed devices: key - device name. If None, then
        all devices are allowed (may change in the future). If ``{}`` then no
        devices are allowed.

    Returns
    -------
    boolean
        Indicates if validation was successful (``True``) or failed (``False``).
    str
        Error message that explains the reason for validation failure. Empty string
        if validation is successful.
    """
    try:
        success, msg = True, ""

        # If there is no list of allowed plans (it is None), then consider the plan valid.
        if allowed_plans is not None:
            # Verify that plan name is in the list of allowed plans
            plan_name = plan["name"]

            if plan_name not in allowed_plans:
                msg = f"Plan '{plan['name']}' is not in the list of allowed plans."
                raise Exception(msg)

            plan_description = allowed_plans[plan_name]
            # Plan description should contain only allowed plans and devices as parameters at
            #   this point. But run the filtering again just in case.
            plan_description = filter_plan_description(
                plan_description, allowed_plans=allowed_plans, allowed_devices=allowed_devices
            )
            param_list = plan_description["parameters"]

            call_args = plan.get("args", {})
            call_kwargs = plan.get("kwargs", {})

            _validate_plan_parameters(param_list=param_list, call_args=call_args, call_kwargs=call_kwargs)

        # Check if supplied plan metadata is a dictionary or a list of dictionaries.
        if "meta" in plan:
            meta_msg = "Plan parameter 'meta' must be a dictionary or a list of dictionaries"
            if isinstance(plan["meta"], (tuple, list)):
                for meta in plan["meta"]:
                    if not isinstance(meta, dict):
                        raise Exception(meta_msg)
            elif not isinstance(plan["meta"], dict):
                raise Exception(meta_msg)

    except Exception as ex:
        success = False
        msg = f"Plan validation failed: {str(ex)}\nPlan: {ppfl(plan)}"

    return success, msg


def bind_plan_arguments(*, plan_args, plan_kwargs, plan_parameters):
    """
    Bind plan arguments given as ``plan_args`` and ``plan_kwargs`` to plan parameters.
    The function contains shortened version of code executed in ``validate_plan`` function.
    The dictionary of bound arguments can be accessed as ``bound_args.arguments``.

    Parameters
    ----------
    plan_args : list
        A list containing plan args
    plan_kwargs : dict
        A dictionary containing plan kwargs
    plan_parameters : dict
        A dictionary containing description of plan signature. The dictionary is the entry
        of ``allowed_plans`` dictionary (e.g. ``allowed_plans['count']``)

    Returns
    -------
    bound_args : inspect.BoundArgument
        Bound arguments of the plan.

    Raises
    ------
    TypeError
        Arguments could not be bound to plan parameters (raised by ``inspect.Signature.bind``.
    """
    param_list = copy.deepcopy(plan_parameters["parameters"])
    parameters = construct_parameters(param_list)
    # Create signature based on the list of parameters
    sig = inspect.Signature(parameters)
    # Verify the list of parameters based on signature.
    bound_args = sig.bind(*plan_args, **plan_kwargs)
    return bound_args


def _parse_docstring(docstring):
    """
    Parse docstring of a function using ``numpydoc``.

    Parameters
    ----------
    docstring: str or None
        Docstring to be parsed.

    Returns
    -------
    dict
        Dictionary that contains the extracted data. Returns ``{}`` if ``docstring`` is ``None``.
    """
    doc_annotation = {}
    if docstring:
        # Make sure that the first line of the docstring is properly indented, otherwise it is
        #   incorrectly parsed by 'numpydoc' (new line is optional).
        # Find the minimum
        ds_split = docstring.split("\n")
        ds_split = ds_split[1:]  # Ignore the first line
        n_indent = None
        for s in ds_split:
            if s.strip():  # Don't process empty strings (that contain only spaces)
                n = None
                for i, ch in enumerate(s):
                    if ch != " ":
                        n = i
                        break
                if n is not None:
                    n_indent = n if n_indent is None else min(n, n_indent)

        if n_indent is not None:
            docstring = "\n" + " " * n_indent + docstring.strip() + "\n"

        doc = NumpyDocString(docstring)

        summary = doc["Summary"]
        if summary:
            doc_annotation["description"] = "\n".join(summary)

        params = doc["Parameters"]
        doc_annotation["parameters"] = {}
        for p in params:
            names = p.name
            desc = p.desc
            if ":" in names:
                n = names.index(":")
                names = names[:n]
            names = names.split(",")
            # Remove '*' (should not be part of the name, but used in some plan annotations)
            names = [_.replace("*", "") for _ in names]
            for name in names:
                name = name.strip()
                if name:
                    doc_annotation["parameters"][name] = {}
                    doc_annotation["parameters"][name]["description"] = "\n".join(desc)

        def params_to_str(params):
            """
            Assembles return type info into one string. This joins the description
            of return parameters into one string.
            """
            p_list = []
            for p in params:
                s = ""
                indent = 0
                if p.name:
                    s += f"{p.name}"
                if p.name and p.type:
                    s += " : "
                if p.type:
                    s += f"{p.type}"
                if (p.name or p.type) and p.desc:
                    s += "\n"
                    indent = 4
                if p.desc:
                    lines = p.desc
                    lines = [" " * indent + _ if _.strip() else _ for _ in lines]
                    s += "\n".join(lines)
                p_list.append(s)
            return "\n".join(p_list)

        returns = doc["Returns"]
        yields = doc["Yields"]
        doc_annotation["returns"] = {}
        p_str = None
        if yields:
            p_str = params_to_str(yields)
        elif returns:
            p_str = params_to_str(returns)
        if p_str:
            doc_annotation["returns"]["description"] = p_str

    return doc_annotation


def _process_plan(plan, *, existing_devices, existing_plans):
    """
    Returns parameters of a plan. The function accepts callable object that implements the plan
    and returns the dictionary with descriptions of the plan and plan parameters. The function
    extracts the parameters and their annotations from the plan signature and plan and parameter
    descriptions from the docstring. If the plan is decorated with``parameter_annotation_decorator``,
    then the specifications of plan and parameter descriptions and parameter annotations passed
    to the decorator override the descriptions from the docstring and Python parameter annotations
    from the function header.

    Limitation on the support types of the plan parameters and the default values that could be
    used in function signatures. Parameter types must be native Python types (such as ``int``,
    ``float``, ``str``, etc.) or generic types based on Python native types
    (``typing.List[typing.Union[int, str]]``). The operation of recreating the type from its
    string representation using ``eval`` and the namespace with imported ``typing`` module
    and ``NoneType`` type should be successful. The default values are reconstructed using
    ``ast.literal_eval`` and the operation ``ast.literal_eval(f"{v!r}")`` must run successfully.

    The description of the plan is represented as a Python dictionary in the following format
    (all elements are optional unless they are labeled as REQUIRED):

    .. code-block:: python

        {
            "name": <plan name>,  # REQUIRED
            "module": <module name>,  # whenever available
            "description": <plan description (multiline text)>,
            "properties": {  # REQUIRED, may be empty
                "is_generator": <boolean that indicates if this is a generator function>
            },
            "parameters": [  # REQUIRED, may be empty
                <param_name_1>: {
                     "name": <parameter name>,  # REQUIRED
                     "description": <parameter description>,
                     # For values of the 'kind' see documentation for 'inspect.Parameter'
                     "kind": {  # REQUIRED
                         "name": <string representation>,
                         "value": <integer representation>,
                     },
                     "annotation": {
                         # 'type' is REQUIRED if annotation is present
                         "type": <parameter type represented as a string>,
                         # Enums for devices, plans and simple enums are in the same format as
                         #   the 'parameter_annotation_decorator'.
                         "devices": <dict of device types (lists of device names)>,
                         "plans": <dict of plan types (lists of plan names)>,
                         "enums": <dict of enum types (lists of strings)>,
                     }
                     "default": <string representation of the default value>,
                     "default_defined_in_decorator": boolean,  # True if the default value is defined
                                                              # in decorator, otherwise False/not set
                     "min": <string representing int or float>,
                     "max": <string representing int or float>,
                     "step": <string representing int or float>,
                }
                <parameter_name_2>: ...
                <parameter_name_3>: ...
                ...
            ]
        }

    .. note::

        This function does not expand built-in lists of devices or add subdevices for devices with
        specified length if those are used in the plan annotation decorator. Call ``expand_plan_description()``
        for each plan description in order for the lists to be expanded based on the current list of
        available or existing devices.

    Parameters
    ----------
    plan: callable
        Reference to the function implementing the plan
    existing_devices : dict
        Prepared dictionary of existing devices (returned by the function ``_prepare_devices``.
        The dictionary is used to create lists of devices for built-in device lists ``AllDevicesList``,
        ``AllDetectorsList``, ``AllMotorsList``, ``AllFlyersList``. If it is known that built-in plans
        are not used, then empty dictionary can be passed instead of the list of existing devices.

    Returns
    -------
    dict
        Dictionary with plan parameters.

    Raises
    ------
    ValueError
        Error occurred while creating plan description.
    """

    def _get_full_type_name(patterns, s):
        """
        Returns the full name of the type with parameters. The function tries to find the name
        of the type in the string ``s`` using the list of regular expressions ``patterns``.
        For example, if the list of patterns is ``["typing.Callable", "collections.abc.Callable"]``
        the function will find both isolated ``typing.Callable`` and ``collections.abc.Callable``
        and the types with parameters such as ``typing.Callable[[int, str], float]``, whichever
        comes first in the string ``s``.

        Parameters
        ----------
        patterns : list
            List of regular expressions to find the type name in the string ``s``.
        s : str
            String to search for the type name.

        Returns
        -------
        str or None
            Full name of the type with parameters or ``None`` if the type name was not found.
        """
        if not s:
            return None

        # Try all patterns. Use the first pattern that matches.
        matches = [re.search(p, s) for p in patterns]
        match = None
        for m in matches:
            if m:
                if not match or m.start() < match.start():
                    match = m

        # No patterns were found.
        if not match:
            return None

        name = match[0]
        start_ind, end_name_ind = match.span()

        # The type has no parameters
        if end_name_ind == len(s) or s[end_name_ind] != "[":
            return name

        # Find full name with parameters (find the closing bracket)
        end_ind, n_brackets = -1, 0
        for n in range(end_name_ind, len(s)):
            if s[n] == "[":
                n_brackets += 1
            elif s[n] == "]":
                n_brackets -= 1
                if n_brackets == 0:
                    end_ind = n + 1
                    break

        if end_ind < 0:
            return name

        return s[start_ind:end_ind]

    def convert_annotation_to_string(annotation):
        """
        Ignore the type if it can not be properly reconstructed using 'eval'
        """
        import bluesky
        from bluesky.protocols import (
            Checkable,
            Configurable,
            Flyable,
            Locatable,
            Movable,
            Pausable,
            Readable,
            Stageable,
            Stoppable,
            Subscribable,
            Triggerable,
        )

        protocols_mapping = {
            "__READABLE__": [Readable],
            "__MOVABLE__": [Movable],
            "__FLYABLE__": [Flyable],
            "__DEVICE__": [
                Configurable,
                Triggerable,
                Locatable,
                Stageable,
                Pausable,
                Stoppable,
                Subscribable,
                Checkable,
            ],
        }
        protocols_inv = {_: k for k, v in protocols_mapping.items() for _ in v}

        protocols_patterns = {
            k: [f"bluesky.protocols.{_.__name__}" for _ in v] for k, v in protocols_mapping.items()
        }
        callables_patterns = {"__CALLABLE__": [r"typing.Callable", r"collections.abc.Callable"]}
        # Patterns for Callable MUST go first
        type_patterns = {**callables_patterns, **protocols_patterns}

        ns = {
            "typing": typing,
            "collections": collections,
            "bluesky": bluesky,
            "NoneType": type(None),
        }

        substitutions_dict = {}

        # This will work for generic types like 'typing.List[int]'
        a_str = f"{annotation!r}"
        # The following takes care of Python base types, such as 'int', 'float'
        #   expressed as "<class 'int'>", but have valid '__name__' attribute
        #   containing type name, such as "int".
        if hasattr(annotation, "__name__") and re.search("^<.*>$", a_str):
            # Note, that starting with Python 3.10 parameter annotation always have
            #   '__name__', which is meaningless for types other than base types.
            a_str = annotation.__name__
            mapping = {k.__name__: v for k, v in protocols_inv.items()}
            if a_str in mapping:
                a_str = mapping[a_str]
                ns[a_str] = annotation  # 'a_str' is __DEVICE__, __READABLE__ or similar
        else:
            # Replace each expression with a unique string in the form of '__CALLABLE<n>__'
            n_patterns = 0  # Number of detected callables
            for type_name, type_patterns in type_patterns.items():
                while True:
                    pattern = _get_full_type_name(type_patterns, a_str)
                    if not pattern:
                        break
                    try:
                        p_type = eval(pattern, ns, ns)
                    except Exception:
                        p_type = None
                    p_str_prefix, p_str_suffix = re.findall(r".*[a-zA-Z0-9]|__$", type_name)
                    p_str = f"{p_str_prefix}{n_patterns + 1}{p_str_suffix}"
                    a_str = re.sub(re.escape(pattern), p_str, a_str)
                    if p_type:
                        # Evaluation of the annotation will fail later and the parameter
                        #   will have no type annotation
                        ns[p_str] = p_type
                    substitutions_dict[p_str] = type_name
                    n_patterns += 1

        # Verify if the type could be recreated by evaluating the string during validation.
        try:
            an = eval(a_str, ns, ns)
            if an != annotation:
                raise Exception()

            # Replace all callables ('__CALLABLE1__', '__CALLABLE2__', etc.) with '__CALLABLE__' string
            for k, v in substitutions_dict.items():
                a_str = re.sub(k, v, a_str)

        except Exception:
            # Ignore the type if it can not be recreated.
            a_str = None

        return a_str

    def convert_expression_to_string(value, expression_role=None):
        """
        Raise 'ValueError' if the string representation of the value can not be evaluated
        """
        role_str = f" of {expression_role}" if expression_role else ""

        s_value = f"{value!r}"
        try:
            ast.literal_eval(s_value)
        except Exception:
            # If default value can not be accepted, then the plan is considered invalid.
            #    Processing should be interrupted.
            raise ValueError(
                f"The expression ({s_value}) can not be evaluated with 'ast.literal_eval()': "
                f"unsupported type{role_str}."
            )
        return s_value

    def assemble_custom_annotation(parameter, *, existing_plans, existing_devices):
        """
        Assemble annotation from parameters of the decorator.
        Returns ``None`` if there is no annotation.
        """
        annotation = {}
        if "annotation" not in parameter:
            return None

        annotation["type"] = parameter["annotation"]
        keys = {"devices", "plans", "enums"}
        for k in keys:
            if k in parameter:
                annotation[k] = copy.copy(parameter[k])

        annotation = _expand_parameter_annotation(
            annotation, existing_devices=existing_devices, existing_plans=existing_plans
        )

        return annotation

    sig = inspect.signature(plan)
    docstring = getattr(plan, "__doc__", None)

    param_annotation = getattr(plan, "_custom_parameter_annotation_", None)
    doc_annotation = _parse_docstring(docstring)

    # Returned value: dictionary with fully processed plan description
    ret = {"name": plan.__name__, "properties": {}, "parameters": []}

    module_name = plan.__module__
    if module_name != "<run_path>":
        ret.update({"module": module_name})

    try:
        # Properties
        ret["properties"]["is_generator"] = inspect.isgeneratorfunction(plan) or inspect.isgenerator(plan)

        # Plan description
        desc = None
        if param_annotation:
            desc = param_annotation.get("description", None)
        if not desc and doc_annotation:
            desc = doc_annotation.get("description", None)
        if desc:
            ret["description"] = desc

        # Function parameters
        use_docstring = "parameters" in doc_annotation
        use_custom = param_annotation and ("parameters" in param_annotation)
        for p in sig.parameters.values():
            working_dict = {"name": p.name}
            ret["parameters"].append(working_dict)

            working_dict["kind"] = {"name": p.kind.name, "value": p.kind.value}

            # Parameter description (attempt to get it from the decorator, then from docstring)
            desc, annotation, default = None, None, None
            vmin, vmax, step = None, None, None
            default_defined_in_decorator = False
            if use_custom and (p.name in param_annotation["parameters"]):
                desc = param_annotation["parameters"][p.name].get("description", None)

                # Copy 'convert_plan_names' and 'convert_device_names' if they exist
                for k in ("convert_plan_names", "convert_device_names"):
                    if k in param_annotation["parameters"][p.name]:
                        working_dict[k] = param_annotation["parameters"][p.name][k]

                annotation = assemble_custom_annotation(
                    param_annotation["parameters"][p.name],
                    existing_plans=existing_plans,
                    existing_devices=existing_devices,
                )
                try:
                    _ = param_annotation["parameters"][p.name].get("default", None)

                    if _ is None:
                        default = _
                    else:
                        default = convert_expression_to_string(_, expression_role="default value in decorator")

                    if default:
                        default_defined_in_decorator = True

                    _ = param_annotation["parameters"][p.name].get("min", None)

                    if _ is None:
                        vmin = _
                    else:
                        vmin = convert_expression_to_string(_, expression_role="min value in decorator")

                    _ = param_annotation["parameters"][p.name].get("max", None)

                    if _ is None:
                        vmax = _
                    else:
                        vmax = convert_expression_to_string(_, expression_role="max value in decorator")

                    _ = param_annotation["parameters"][p.name].get("step", None)
                    if _ is None:
                        step = _
                    else:
                        step = convert_expression_to_string(_, expression_role="step value in decorator")

                except Exception as ex:
                    raise ValueError(f"Parameter '{p.name}': {ex}")

            if not desc and use_docstring and (p.name in doc_annotation["parameters"]):
                desc = doc_annotation["parameters"][p.name].get("description", None)
            if not annotation and p.annotation is not inspect.Parameter.empty:
                annotation = convert_annotation_to_string(p.annotation)
                if annotation:
                    # The case when annotation does exist (otherwise it is None)
                    annotation = {"type": annotation}
            if default and p.default is inspect.Parameter.empty:
                # The default value in the decorator overrides the default value in the header,
                #   not replace it. Therefore the default value is required in the header if
                #   one is specified in the decorator.
                raise ValueError(
                    f"Missing default value for the parameter '{p.name}' in the plan signature: "
                    f"The default value {default} is specified in the annotation decorator, "
                    f"there for a default value is required in the plan header."
                )
            if not default and (p.default is not inspect.Parameter.empty):
                try:
                    default = convert_expression_to_string(p.default, expression_role="default value")
                except Exception as ex:
                    raise ValueError(f"Parameter '{p.name}': {ex}")

            if desc:
                working_dict["description"] = desc

            if annotation:
                # Verify that the encoded type could be decoded (raises an exception if fails)
                _, convert_values, _ = _process_annotation(annotation)
                # _, convert_plan_names, convert_device_names, _ = _process_annotation(annotation)
                working_dict["annotation"] = annotation

                # Set the following parameters True only if they do not already exist (ignore if False)
                if convert_values["convert_plan_names"] and ("convert_plan_names" not in working_dict):
                    working_dict["convert_plan_names"] = True
                if convert_values["convert_device_names"] and ("convert_device_names" not in working_dict):
                    working_dict["convert_device_names"] = True
                if convert_values["eval_expressions"] and ("eval_expressions" not in working_dict):
                    working_dict["eval_expressions"] = True

            if default:
                # Verify that the encoded representation of the default can be decoded.
                _process_default_value(default)  # May raises exception
                working_dict["default"] = default
                if default_defined_in_decorator:
                    working_dict["default_defined_in_decorator"] = True

            # 'min', 'max' and 'step' (may exist only in decorator)
            if vmin is not None:
                try:
                    # Attempt to convert to number (use the same function as during validation)
                    vmin = _convert_str_to_number(vmin)
                    working_dict["min"] = f"{vmin}"  # Save as a string
                except Exception as ex:
                    raise ValueError(f"Failed to process min. value: {ex}")
            if vmax is not None:
                try:
                    vmax = _convert_str_to_number(vmax)
                    working_dict["max"] = f"{vmax}"
                except Exception as ex:
                    raise ValueError(f"Failed to process max. value: {ex}")
            if step is not None:
                try:
                    step = _convert_str_to_number(step)
                    working_dict["step"] = f"{step}"
                except Exception as ex:
                    raise ValueError(f"Failed to process step value: {ex}")

    except Exception as ex:
        raise ValueError(f"Failed to create description of plan '{plan.__name__}': {ex}") from ex

    return ret


def _prepare_plans(plans, *, existing_devices, ignore_invalid_plans=False):
    """
    Prepare dictionary of existing plans for saving to YAML file.

    Parameters
    ----------
    plans: dict
        Dictionary of plans extracted from the workspace
    existing_devices: dict
        Prepared dictionary of existing devices (returned by the function ``_prepare_devices``.
    ignore_invalid_plans: bool
        Ignore plans with unsupported signatures. If the argument is ``False`` (default), then
        an exception is raised otherwise a message is printed and the plan is not included in the list.

    Returns
    -------
    dict
        Dictionary that maps plan names to plan descriptions
    """
    reg_plans = reg_ns_items.reg_plans

    plans_selected = {}
    for name in set(plans.keys()).union(set(reg_plans.keys())):
        obj = None
        if name in reg_plans:
            obj = reg_plans[name]["obj"]
        elif name in plans:
            obj = plans[name]
        if obj:
            plans_selected[name] = obj

    plans_names = set(plans_selected)

    prepared_plans = {}
    for k, v in plans_selected.items():
        try:
            prepared_plans[k] = _process_plan(v, existing_devices=existing_devices, existing_plans=plans_names)
        except Exception as ex:
            if ignore_invalid_plans:
                logger.warning("PLAN WAS IGNORED: %s", ex)
            else:
                raise

    return prepared_plans


def _prepare_devices(devices, *, max_depth=0, ignore_all_subdevices_if_one_fails=True, expand_areadetectors=False):
    """
    Prepare dictionary of existing devices for saving to YAML file.
    ``max_depth`` is the maximum depth for the components. The default value (50)
    is a very large number.

    Parameters
    ----------
    devices: dict
        Dictionary of devices from the namespace (key - device name, value - reference
        to the device object).
    max_depth: int
        Maximum depth for the device search: 0 - infinite depth, 1 - only top level
        devices, 2 - device and subdevices etc.
    ignore_all_subdevices_if_one_fails: bool
        Ignore all components of devices if at least one component (PVs) can not
        be accessed. It saves a lot of time to ignore all components, since
        stale code may contain devices with many components with non-existing PVs
        and respective timeout may amount to substantial waiting time.
    expand_areadetectors: bool
        Find subdevices of areadetectors. It may take significant time to expand an
        areadetector and it is unlikely that the areadetector subdevices should be
        accessed via plan parameters.

    Returns
    -------
    dict
        List of existing devices (tree of devices and subdevices).
    """
    max_depth = max(0, max_depth)  # must be >= 0

    from bluesky import protocols
    from ophyd.areadetector import ADBase

    def get_device_params(device):
        return {
            "is_readable": isinstance(device, protocols.Readable),
            "is_movable": isinstance(device, protocols.Movable),
            "is_flyable": isinstance(device, protocols.Flyable),
            "classname": type(device).__name__,
            "module": type(device).__module__,
        }

    def get_device_component_names(device):
        if hasattr(device, "component_names"):
            component_names = device.component_names
            if not isinstance(component_names, Iterable):
                component_names = []
        else:
            component_names = []
        return component_names

    def create_device_description(device, device_name, *, depth=0, max_depth=0, is_registered=False):
        description = get_device_params(device)
        comps = get_device_component_names(device)
        components = {}

        is_areadetector = isinstance(device, ADBase)
        expand = is_registered or not is_areadetector or expand_areadetectors

        if expand and (not max_depth or (depth < max_depth - 1)):
            ignore_subdevices = False
            for comp_name in comps:
                try:
                    if hasattr(device, comp_name):
                        c = getattr(device, comp_name)
                        desc = create_device_description(
                            c,
                            device_name + "." + comp_name,
                            depth=depth + 1,
                            max_depth=max_depth,
                            is_registered=is_registered,
                        )
                        components[comp_name] = desc
                except Exception as ex:
                    ignore_subdevices = ignore_all_subdevices_if_one_fails
                    logger.warning(
                        "Device '%s': component '%s' can not be processed: %s", device_name, comp_name, ex
                    )
                if ignore_subdevices:
                    components = {}  # Ignore all components of the subdevice
                    break
            if components:
                description["components"] = components

        return description

    def process_devices(*, max_depth):
        """
        Process devices one by one based on information from the namespace and
        the dictionary of registered devices.
        """
        reg_devices = reg_ns_items.reg_devices

        devices_selected = {}
        for name in set(devices.keys()).union(set(reg_devices.keys())):
            obj, _max_depth, is_registered = None, max_depth, False
            if name in reg_devices:
                obj = reg_devices[name]["obj"]
                _max_depth = reg_devices[name]["depth"]
                if obj:
                    is_registered = True
            elif name in devices:
                obj = devices[name]
            if obj:
                devices_selected[name] = dict(obj=obj, max_depth=_max_depth, is_registered=is_registered)

        device_dict = {}
        for name, p in devices_selected.items():
            device_dict[name] = create_device_description(
                p["obj"], name, max_depth=p["max_depth"], is_registered=p["is_registered"]
            )

        return device_dict

    return process_devices(max_depth=max_depth)


def existing_plans_and_devices_from_nspace(*, nspace, max_depth=0, ignore_invalid_plans=False):
    """
    Generate lists of existing plans and devices from namespace. The namespace
    must be in the form returned by ``load_worker_startup_code()``.

    Parameters
    ----------
    nspace: dict
        Namespace that contains plans and devices
    max_depth: int
        Default maximum depth for device search: 0 - unlimited depth, 1 - include only top level devices,
        2 - include top level devices and subdevices, etc.
    ignore_invalid_plans: bool
        Ignore plans with unsupported signatures. If the argument is ``False`` (default), then
        an exception is raised otherwise a message is printed and the plan is not included in the list.


    Returns
    -------
    existing_plans : dict
        Dictionary of descriptions of existing plans
    existing_devices : dict
        Dictionary of descriptions of existing devices
    plans_in_nspace : dict
        Dictionary of plans in namespace
    devices_in_nspace : dict
        Dictionary of devices in namespace
    """
    logger.debug("Extracting existing plans and devices from the namespace ...")

    plans_in_nspace = plans_from_nspace(nspace)
    devices_in_nspace = devices_from_nspace(nspace)

    existing_devices = _prepare_devices(devices_in_nspace, max_depth=max_depth)
    existing_plans = _prepare_plans(
        plans_in_nspace, existing_devices=existing_devices, ignore_invalid_plans=ignore_invalid_plans
    )

    return existing_plans, existing_devices, plans_in_nspace, devices_in_nspace


def save_existing_plans_and_devices(
    *,
    existing_plans,
    existing_devices,
    file_dir=None,
    file_name=None,
    overwrite=False,
):
    """
    Save dictionaries of existing plans and devices to YAML file. The dictionaries of existing
    plans and devices must be in the form returned by ``existing_plans_and_devices_from_nspace``.

    Parameters
    ----------
    existing_plans : dict
        dictionary of existing plans (key - plan name, value - plan description)
    existing_devices : dict
        dictionary of existing devices (key - device name, value - device description)
    startup_script_path : str or None
        name of the startup script
    file_dir : str or None
        path to the directory where the file is to be created. None - create file in current directory.
    file_name : str
        name of the output YAML file, None - default file name is used
    overwrite : boolean
        overwrite the file if it already exists
    """

    existing_plans_and_devices = {
        "existing_plans": existing_plans,
        "existing_devices": existing_devices,
    }

    file_path = os.path.join(file_dir, file_name)
    if os.path.exists(file_path) and not overwrite:
        raise IOError(f"File '{file_path}' already exists. File overwriting is disabled.")

    with open(file_path, "w") as stream:
        stream.write("# This file is automatically generated. Edit at your own risk.\n")
        yaml.dump(existing_plans_and_devices, stream)


def load_existing_plans_and_devices(path_to_file=None):
    """
    Load the lists of allowed plans and devices from YAML file. Returns empty lists
    if `path_to_file` is None or "" or the file does not exist or corrupt.

    Parameters
    ----------
    path_to_file: str on None
        Full path to .yaml file that contains the lists.

    Returns
    -------
    (dict, dict)
        List of allowed plans and list of allowed devices.
    """
    msg = "List of plans and devices is not loaded."
    if not path_to_file:
        logger.warning("%s File path is not specified.", msg)
        return {}, {}

    if not os.path.isfile(path_to_file):
        logger.warning("%s File '%s' does not exist.", msg, path_to_file)
        return {}, {}

    with open(path_to_file, "r") as stream:
        try:
            existing_plans_and_devices = yaml.load(stream, Loader=yaml.FullLoader)
        except Exception as ex:
            logger.error("%s File '%s' has invalid YAML: %s", msg, path_to_file, ex)
            existing_plans_and_devices = {}

    if not isinstance(existing_plans_and_devices, dict):
        logger.warning("%s The file '%s' has invalid format or empty.", msg, path_to_file)
        return {}, {}

    existing_plans = existing_plans_and_devices.get("existing_plans", {})
    existing_devices = existing_plans_and_devices.get("existing_devices", {})

    return existing_plans, existing_devices


def compare_existing_plans_and_devices(
    *,
    existing_plans,
    existing_devices,
    existing_plans_ref,
    existing_devices_ref,
):
    """
    Compares the lists of existing plans and devices with the reference lists. Returns ``False``
    if the exception is raised while comparing the lists. (Consistent exception handling is
    the only reason we need this function.)

    Parameters
    ----------
    existing_plans : dict
        The updated list (dictionary) of descriptions of the existing plans.
    existing_devices : dict
        The updated list (dictionary) of descriptions of the existing devices.
    existing_plans_ref : dict or None
        The reference list (dictionary) of descriptions of the existing plans.
    existing_devices_ref : dict or None
        The reference list (dictionary) of descriptions of the existing devices.

    Returns
    -------
    boolean
        ``True`` - the lists are equal, ``False`` otherwise.
    """
    try:
        lists_equal = (existing_plans == existing_plans_ref) and (existing_devices == existing_devices_ref)
    except Exception as ex:
        # If the lists (dictionaries) of plans and devices can not be compared, then save the new lists.
        #   This issue should be investigated if it is repeated regularly.
        logger.warning("Failed to compare existing plans or existing devices: %s", str(ex))
        lists_equal = False
    return lists_equal


def update_existing_plans_and_devices(
    *,
    path_to_file,
    existing_plans,
    existing_devices,
    existing_plans_ref=None,
    existing_devices_ref=None,
    always_save=False,
):
    """
    Update the lists of existing plans and devices stored in the file ``path_to_file`` on disk if
    the lists are different from the reference lists. The reference lists can be passed to the function
    or loaded from the file if ``existing_plans_ref`` or ``existing_devices_ref`` are ``None``.
    If ``always_save`` is False, then the descriptions are updated only if the lists have changed,
    otherwise the lists are always saved. The function will never modified files in the directory
    with startup scripts, which is part of the repository or distributed package.

    This function never raises exceptions by design, since successful updating of stored lists of plans
    and devices is not considered critical for executing plans. If the function regularly generates
    warnings or error messages, the issue must be investigated.

    .. note::

        Lists of plans are devices are represented as Python dictionaries.

    Parameters
    ----------
    path_to_file : str
        Path to file that contains the lists of existing plans and devices
    existing_plans : dict
        The updated list (dictionary) of descriptions of the existing plans.
    existing_devices : dict
        The updated list (dictionary) of descriptions of the existing devices.
    existing_plans_ref : dict or None
        The reference list (dictionary) of descriptions of the existing plans.
    existing_devices_ref : dict or None
        The reference list (dictionary) of descriptions of the existing devices.
    always_save : boolean
        If ``False`` then save lists of plans and devices only if it is different
        from reference lists. Always save the lists to disk if ``True``.

    Returns
    -------
    boolean
        ``True`` if the lists of existing plans and devices is different from the reference
        lists or if ``always_save`` is ``True``.
    """
    if not always_save:
        if (existing_plans_ref is None) or (existing_devices_ref is None):
            ep, ed = load_existing_plans_and_devices(path_to_file)
        if existing_plans_ref is not None:
            ep = existing_plans_ref
        if existing_devices_ref is not None:
            ed = existing_devices_ref

        changes_exist = not compare_existing_plans_and_devices(
            existing_plans=existing_plans,
            existing_devices=existing_devices,
            existing_plans_ref=ep,
            existing_devices_ref=ed,
        )

    else:
        changes_exist = True

    if changes_exist:
        logger.info("Updating the list of existing plans and devices in the file '%s'", path_to_file)

        def normalize_path(fpath):
            """
            Normalize path to make it easier to compare to other paths
            """
            return os.path.realpath(os.path.abspath(os.path.expanduser(fpath)))

        default_startup_dir = normalize_path(get_default_startup_dir())
        file_path = normalize_path(path_to_file)
        try:
            path_is_default = os.path.commonpath([default_startup_dir, file_path]) == default_startup_dir
        except Exception:
            path_is_default = False

        if path_is_default:
            logger.warning(
                "The file '%s' is located in the directory, which is part of the package and can not be updated",
                path_to_file,
            )
        else:
            # Saving the updated plans and devices is not critical for real-time operation of the queue server.
            #   Plans can still be executed, so the error can be ignored, but it should be investigated, because
            #   some functionality may not be working properly.
            try:
                save_existing_plans_and_devices(
                    existing_plans=existing_plans,
                    existing_devices=existing_devices,
                    file_dir=os.path.dirname(path_to_file),
                    file_name=os.path.basename(path_to_file),
                    overwrite=True,
                )
            except Exception as ex:
                logger.error(
                    "Failed to save lists of existing plans and device to file '%s': %s", path_to_file, ex
                )

    return changes_exist


_user_group_permission_schema = {
    "type": "object",
    "additionalProperties": False,
    "required": ["user_groups"],
    "properties": {
        "user_groups": {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "allowed_plans": {
                        "type": "array",
                        "items": {"type": ["string", "null"]},
                    },
                    "forbidden_plans": {
                        "type": "array",
                        "items": {"type": ["string", "null"]},
                    },
                    "allowed_devices": {
                        "type": "array",
                        "items": {"type": ["string", "null"]},
                    },
                    "forbidden_devices": {
                        "type": "array",
                        "items": {"type": ["string", "null"]},
                    },
                    "allowed_functions": {
                        "type": "array",
                        "items": {"type": ["string", "null"]},
                    },
                    "forbidden_functions": {
                        "type": "array",
                        "items": {"type": ["string", "null"]},
                    },
                },
            },
        },
    },
}


def _validate_user_group_permissions_schema(user_group_permissions):
    """
    Validate user group permissions schema. Raises exception if validation fails.

    Parameters
    ----------
    user_group_permissions: dict
        A dictionary with user group permissions.
    """
    jsonschema.validate(instance=user_group_permissions, schema=_user_group_permission_schema)


def validate_user_group_permissions(user_group_permissions):
    """
    Validate the dictionary with user group permissions. Exception is raised errors are detected.

    Parameters
    ----------
    user_group_permissions: dict
        The dictionary with user group permissions.
    """
    if not isinstance(user_group_permissions, dict):
        raise TypeError(f"User group permissions: Invalid type '{type(user_group_permissions)}', must be 'dict'")

    _validate_user_group_permissions_schema(user_group_permissions)

    if "root" not in user_group_permissions["user_groups"]:
        raise KeyError("User group permissions: Missing required user group: 'root'")


def load_user_group_permissions(path_to_file=None):
    """
    Load the data on allowed plans and devices for user groups. User group 'root'
    is required. Exception is raised in user group 'root' is missing.

    Parameters
    ----------
    path_to_file: str
        Full path to YAML file that contains user data permissions.

    Returns
    -------
    dict
        Data structure with user permissions. Returns ``{}`` if path is an empty string
        or None.

    Raises
    ------
    IOError
        Error while reading the YAML file.
    """

    if not path_to_file:
        return {}

    try:
        if not os.path.isfile(path_to_file):
            raise IOError(f"File '{path_to_file}' does not exist.")

        with open(path_to_file, "r") as stream:
            user_group_permissions = yaml.safe_load(stream)

        validate_user_group_permissions(user_group_permissions)

        if "root" not in user_group_permissions["user_groups"]:
            raise Exception("Missing required user group: 'root'")

    except Exception as ex:
        msg = f"Error while loading user group permissions from file '{path_to_file}': {str(ex)}"
        raise IOError(msg)

    return user_group_permissions


def _check_if_plan_or_func_allowed(item_name, allow_patterns, disallow_patterns):
    """
    Check if an item with ``item_name`` is allowed based on ``allow_patterns``
    and ``disallow_patterns``. Patterns may be explicitly listed names and regular expressions.
    ``None`` may appear only as a first element of the pattern list.
    The function should be used only with function and plan names.

    Parameters
    ----------
    item_name: str
        Name of the item.
    allow_patterns: list(str)
        Selected item should match at least one of the re patterns. If the value is ``[None]``
        then all items are selected. If ``[]``, then no items are selected.
    disallow_patterns: list(str)
        Items are deselected based on re patterns in the list: if an item matches at least one
        of the patterns, it is deselected. If the value is ``[None]`` or ``[]`` then no items
        are deselected.

    Returns
    -------
    boolean
        Indicates if the item is permitted based on ``allow_patterns`` and ``disallow_patterns``.
    """
    item_is_allowed = False

    def prepare_pattern(pattern):
        if not isinstance(pattern, str):
            raise TypeError("Pattern is not a string: {pattern!r} ({allow_patterns})")
        # Prepare patterns
        components, uses_re, _ = _split_name_pattern(pattern)
        if len(components) != 1:
            raise ValueError("Name pattern for functions and plans must contain one component: {components}")
        component = components[0][0]
        if not uses_re:
            if not re.search("[_a-zA-Z][_0-9a-zA-Z]*", component):
                assert ValueError(
                    f"Plan or function name pattern {component!r} contains invalid characters. "
                    "The pattern could be a valid regular expression, "
                    "but it is not labeled with ':' (e.g. ':^count$')"
                )
        return component, uses_re

    if allow_patterns:
        if allow_patterns[0] is None:
            item_is_allowed = True
        else:
            for pattern in allow_patterns:
                component, uses_re = prepare_pattern(pattern)
                if uses_re:
                    if re.search(component, item_name):
                        item_is_allowed = True
                        break
                else:
                    if item_name == component:
                        item_is_allowed = True
                        break

    if item_is_allowed:
        if disallow_patterns and (disallow_patterns[0] is not None):
            for pattern in disallow_patterns:
                component, uses_re = prepare_pattern(pattern)
                if uses_re:
                    if re.search(component, item_name):
                        item_is_allowed = False
                        break
                else:
                    if item_name == component:
                        item_is_allowed = False
                        break

    return item_is_allowed


def check_if_function_allowed(function_name, *, group_name, user_group_permissions=None):
    """
    Check if the function with name ``function_name`` is allowed for users of ``group_name`` group.
    The function first checks if the function name passes root permissions and then permissions
    for the specific group.

    Parameters
    ----------
    function_name: str
        Function name to check
    group_name: str
        User group name. Permissions must be defined for this group.
    user_group_permissions: dict or None
        Reference to a dictionary, which contains the defined user group permissions.

    Returns
    -------
    boolean
        Indicates if the user that belong to ``user_group`` are allowed to run the function.

    Raises
    ------
    KeyError
        ``user_group_permissions`` do not contain ``user_groups`` key or permissions for
        ``group_name`` group are not defined.
    """
    if not user_group_permissions:
        return False

    if "user_groups" not in user_group_permissions:
        raise KeyError("User group permissions dictionary does not contain 'user_groups' key")

    user_groups = user_group_permissions["user_groups"]

    if "root" not in user_groups:
        raise KeyError("No permissions are defined for user group 'root'")
    if group_name not in user_groups:
        raise KeyError(f"No permissions are defined for user group {group_name!r}")

    root_allow_patterns = user_groups["root"].get("allowed_functions", [])
    root_disallow_patterns = user_groups["root"].get("forbidden_functions", [])
    group_allow_patterns = user_groups[group_name].get("allowed_functions", [])
    group_disallow_patterns = user_groups[group_name].get("forbidden_functions", [])

    return _check_if_plan_or_func_allowed(
        function_name,
        root_allow_patterns,
        root_disallow_patterns,
    ) and _check_if_plan_or_func_allowed(
        function_name,
        group_allow_patterns,
        group_disallow_patterns,
    )


def _select_allowed_plans(item_dict, allow_patterns, disallow_patterns):
    """
    Creates the dictionary of items selected from `item_dict` with names names that
    satisfy patterns in `allow_patterns` and not satisfy patterns in `disallow_patterns`.
    The function does not modify the dictionary, but creates a new dictionary with
    selected items, where item values are deep copies of the values of the original
    dictionary.

    Parameters
    ----------
    item_dict: dict
        Dictionary of items.
    allow_patterns: list(str)
        Selected item should match at least one of the re patterns. If the value is ``[None]``
        then all items are selected. If ``[]``, then no items are selected.
    disallow_patterns: list(str)
        Items are deselected based on re patterns in the list: if an item matches at least one
        of the patterns, it is deselected. If the value is ``[None]`` or ``[]`` then no items
        are deselected.

    Returns
    -------
    dict
        Dictionary of the selected items.
    """
    items_selected = {}
    for item_name in item_dict:
        if _check_if_plan_or_func_allowed(item_name, allow_patterns, disallow_patterns):
            items_selected[item_name] = copy.deepcopy(item_dict[item_name])

    return items_selected


def load_allowed_plans_and_devices(
    *,
    path_existing_plans_and_devices=None,
    path_user_group_permissions=None,
    existing_plans=None,
    existing_devices=None,
    user_group_permissions=None,
):
    """
    Generate dictionaries of allowed plans and devices for each user group. If there are no user
    groups defined (path is None), then output dictionaries will have one user group 'root'
    with all existing plans and devices allowed. The function is using the lists (dict) of
    existing plans and devices passed as parameters. If the lists are not passed (or ``None``),
    then they are loaded from the file ``path_existing_plans_and_devices``.
    If the lists of plans and devices do not exist (path_existing_plans_and_devices is ``None``
    or points to non-existing file), then empty dictionaries are returned.

    Parameters
    ----------
    path_existing_plans_and_devices: str or None
        Full path to YAML file, which contains dictionaries of existing plans and devices.
        Raises an exception if the file does not exist. If ``None`` or the file contains
        no plans and/or devices, then empty dictionaries of the allowed plans and devices
        are returned for all groups.
    path_user_group_permissions: str or None
        Full path to YAML file, which contains information on user group permissions.
        Exception is raised if the file does not exist. The path is ignored if
        ``user_group_permissions`` is set. If both ``path_user_group_permissions`` is ``None``
        and ``user_group_permissions`` is ``None`` or ``{}``, then dictionaries of allowed
        plans and devices will contain one group 'root' with all existing devices and plans
        set as allowed.
    existing_plans: dict or None
        List (dict) of the existing plans. If ``None``, then the list is loaded from
        the file ``path_existing_plans_and_devices``.
    existing_devices: dict or None
        List (dict) of the existing devices. If ``None``, then the list is loaded from
        the file ``path_existing_plans_and_devices``.
    user_group_permissions: str or None
        Dictionary with user group permissions. If ``None``, then permissions are loaded
        from the file ``path_user_group_permissions``. If ``{}``, then dictionaries of allowed
        plans and devices will contain one group 'root' with all existing devices and plans
        set as allowed.

    Returns
    -------
    dict, dict
        Dictionaries of allowed plans and devices. Dictionary keys are names of the user groups.
        Dictionaries are ``{}`` if no data on existing plans and devices is provided.
        Dictionaries contain one group (``root``) if no user perission data is provided.

    """
    allowed_plans, allowed_devices = {}, {}

    try:
        if user_group_permissions is None:
            user_group_permissions = load_user_group_permissions(path_user_group_permissions)

        # Data from the file is loaded only if ``existing_plans`` or ``existing_devices`` is ``None``.
        if (existing_plans is None) or (existing_devices is None):
            ep, ed = load_existing_plans_and_devices(path_existing_plans_and_devices)
        if existing_plans is None:
            existing_plans = ep
        if existing_devices is None:
            existing_devices = ed

        # Detect some possible errors
        if not user_group_permissions:
            raise RuntimeError("No user permissions are specified")

        if not isinstance(user_group_permissions, dict):
            raise RuntimeError(
                f"'user_group_permissions' has type {type(user_group_permissions)} instead of 'dict'"
            )

        if "user_groups" not in user_group_permissions:
            raise RuntimeError("No user groups are defined: 'user_groups' keys is not in 'user_group_permissions'")

        user_groups = list(user_group_permissions["user_groups"].keys())

        # First filter the plans and devices based on the permissions for 'root' user group.
        #   The 'root' user group permissions should be used to exclude all 'junk', i.e.
        #   unused/outdated/not functioning plans and devices collected from namespace, from
        #   the lists available to ALL the users.
        plans_root, devices_root = existing_plans, existing_devices
        if "root" in user_groups:
            group_permissions_root = user_group_permissions["user_groups"]["root"]
            if existing_devices:
                devices_root = _filter_device_tree(
                    existing_devices,
                    group_permissions_root.get("allowed_devices", []),
                    group_permissions_root.get("forbidden_devices", []),
                )
            if existing_plans:
                plans_root = _select_allowed_plans(
                    existing_plans,
                    group_permissions_root.get("allowed_plans", []),
                    group_permissions_root.get("forbidden_plans", []),
                )

        # Now create lists of allowed devices and plans based on the lists for the 'root' user group
        for group in user_groups:
            selected_devices, selected_plans = {}, {}
            if group == "root":
                selected_devices = devices_root
                selected_plans = plans_root
            else:
                group_permissions = user_group_permissions["user_groups"][group]

                if existing_devices:
                    selected_devices = _filter_device_tree(
                        devices_root,
                        group_permissions.get("allowed_devices", []),
                        group_permissions.get("forbidden_devices", []),
                    )

                if existing_plans:
                    selected_plans = _select_allowed_plans(
                        plans_root,
                        group_permissions.get("allowed_plans", []),
                        group_permissions.get("forbidden_plans", []),
                    )

            selected_plans = _filter_allowed_plans(allowed_plans=selected_plans, allowed_devices=selected_devices)

            allowed_devices[group] = selected_devices
            allowed_plans[group] = selected_plans

    except Exception as ex:
        logger.exception("Error occurred while generating the list of allowed plans and devices: %s", ex)

        # The 'default' lists in case error occurred while generating lists
        allowed_plans["root"] = existing_plans
        allowed_devices["root"] = existing_devices

    return allowed_plans, allowed_devices


def load_profile_collection_from_ipython(path=None):
    """
    Load profile collection from IPython. Useful utility function that may be used to
    test if a profile collection can be loaded from IPython.
    Manually run this function from IPython:

    .. code-block:: python

        from bluesky_queueserver.manager.profile_ops import load_profile_collection_from_ipython
        load_profile_collection_from_ipython()
    """
    ip = get_ipython()  # noqa F821
    for f in sorted(glob.glob("*.py") + glob.glob("*.ipy")):
        print(f"Executing '{f}' in TravisCI")
        ip.parent._exec_file(f)
    print("Profile collection was loaded successfully.")


def format_text_descriptions(item_parameters, *, use_html=False):
    """
    Format parameter descriptions for a plan from the list of allowed plans.
    Returns description of the plan and each parameter represented as formatted strings
    containing plan name/description and parameter name/type/default/min/max/step/description.
    The text is prepared for presentation in user interfaces. The descriptions are
    represented as a dictionary:

    .. code-block:: python

        {
            "description": "Multiline formatted text description of the plan",
            "parameters": {
                "param_name1": "Multiline formatted description",
                "param_name2": "Multiline formatted description",
                "param_name3": "Multiline formatted description",
            }
        }

    Parameters
    ----------
    item_parameters : dict
        A dictionary of item parameters, e.g. an element from the list of existing or allowed plans.
    use_html : boolean
        Select if the formatted text should be returned as plain text (default) or HTML.

    Returns
    -------
    dict
        The dictionary that contains formatted descriptions for the plan and its parameters.

    """

    if not item_parameters:
        return {}

    start_bold = "<b>" if use_html else ""
    stop_bold = "</b>" if use_html else ""
    start_it = "<i>" if use_html else ""
    stop_it = "</i>" if use_html else ""
    new_line = "<br>" if use_html else "\n"

    not_available = "Description is not available"

    descriptions = {}

    item_name = item_parameters.get("name", "")

    item_description = str(item_parameters.get("description", ""))
    item_description = item_description.replace("\n", new_line)
    s = f"{start_it}Name:{stop_it} {start_bold}{item_name}{stop_bold}"
    if item_description:
        s += f"{new_line}{item_description}"

    descriptions["description"] = s if s else not_available

    descriptions["parameters"] = {}
    for p in item_parameters.get("parameters", []):
        p_name = p.get("name", None) or "-"

        p_type = "-"
        p_custom_types = []
        annotation = p.get("annotation", None)
        if annotation:
            p_type = str(annotation.get("type", "")) or "-"
            for t in ("devices", "plans", "enums"):
                if t in annotation:
                    for ct in annotation[t]:
                        p_custom_types.append((ct, tuple(annotation[t][ct])))

        p_default = p.get("default", None) or "-"

        p_min = p.get("min", None)
        p_max = p.get("max", None)
        p_step = p.get("step", None)

        p_description = p.get("description", "")
        p_description = p_description.replace("\n", new_line)

        desc = (
            f"{start_it}Name:{stop_it} {start_bold}{p_name}{stop_bold}{new_line}"
            f"{start_it}Type:{stop_it} {start_bold}{p_type}{stop_bold}{new_line}"
        )

        for ct, ct_items in p_custom_types:
            desc += f"{start_bold}{ct}:{stop_bold} {ct_items}{new_line}"

        desc += f"{start_it}Default:{stop_it} {start_bold}{p_default}{stop_bold}"

        if (p_min is not None) or (p_max is not None) or (p_step is not None):
            desc += f"{new_line}"
            insert_space = False
            if p_min is not None:
                desc += f"{start_it}Min:{stop_it} {start_bold}{p_min}{stop_bold}"
                insert_space = True
            if p_max is not None:
                if insert_space:
                    desc += " "
                desc += f"{start_it}Max:{stop_it} {start_bold}{p_max}{stop_bold}"
                insert_space = True
            if p_step is not None:
                if insert_space:
                    desc += " "
                desc += f"{start_it}Step:{stop_it} {start_bold}{p_step}{stop_bold}"

        if p_description:
            desc += f"{new_line}{p_description}"

        descriptions["parameters"][p_name] = desc if desc else not_available

    return descriptions
