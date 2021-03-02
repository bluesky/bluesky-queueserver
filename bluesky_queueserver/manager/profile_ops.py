import os
import glob
import runpy
import inspect
from collections.abc import Iterable
import pkg_resources
import yaml
import tempfile
import re
import sys
import pprint
import jsonschema
import copy
import pickle
import typing
import pydantic
import enum
import random
import argparse
import importlib
from numpydoc.docscrape import NumpyDocString

import logging
import bluesky_queueserver

logger = logging.getLogger(__name__)
qserver_version = bluesky_queueserver.__version__


def get_default_startup_dir():
    """
    Returns the path to the default profile collection that is distributed with the package.
    The function does not guarantee that the directory exists.
    """
    pc_path = pkg_resources.resource_filename("bluesky_queueserver", "profile_collection_sim/")
    return pc_path


_patch1 = """

import sys
import logging
from bluesky_queueserver.manager.profile_tools import global_user_namespace
logger_patch = logging.Logger(__name__)

global_user_namespace.set_user_namespace(user_ns=locals(), use_ipython=False)

try:
    pass  # Prevent errors when patching an empty file

"""

_patch2 = """

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

_patch3 = """

except BaseException as ex:
    # Save exception data
    __plan_exc_info = sys.exc_info()

"""


def _patch_profile(file_name):
    """
    Patch the profile (.py file from a beamline profile collection).
    Patching includes placing the code in the file in ``try..except..` block
    and inserting patch for ``get_ipython()`` function after the line
    ``from IPython import get_python``.

    The patched file is saved to the temporary file ``qserver/profile_temp.py``
    in standard directory for the temporary files. For Linux it is ``/tmp``.
    It is assumed that files in profile collection are processed one by one, so
    overwriting the same temporary file is a good way to eliminate resource leaks.

    Parameters
    ----------
    file_name: str
        full path to the patched file.

    Returns
    -------
    str
        full path to the patched temporary file.
    """

    # On Linux the temporary .py file will be always '/tmp/qserver/profile_temp.py'
    #   On other systems the file will be placed in appropriate location, but
    #   it will always be the same file.
    tmp_dir = os.path.join(tempfile.gettempdir(), "qserver")
    os.makedirs(tmp_dir, exist_ok=True)
    tmp_fln = os.path.join(tmp_dir, "profile_temp.py")

    with open(file_name, "r") as fln_in:
        code = fln_in.readlines()

    class GetIPythonUsed(enum.Enum):
        NOT_PRESENT = 0
        IMPORTED = 1
        CALLED = 2

    def is_get_ipython_in_line(line):
        """Check if ``get_ipython()`` is imported or called in the line"""
        # It is assumed that commenting is done using #
        result = GetIPythonUsed.NOT_PRESENT
        if re.search(r"^[^#]*IPython[^#]+get_ipython", line):
            result = GetIPythonUsed.IMPORTED
        elif re.search(r"^[^#]*get_ipython", line):
            result = GetIPythonUsed.CALLED
        return result

    def patch_before_first_line(code):
        """
        Determine if the code file needs to be patched before the first line.
        The file should be patched if ``get_ipython`` is called before it is imported.
        Otherwise it should be patched each time it is imported
        """
        for line in code:
            is_get_ipython = is_get_ipython_in_line(line)
            if is_get_ipython == GetIPythonUsed.IMPORTED:
                return False
            elif is_get_ipython == GetIPythonUsed.CALLED:
                return True
        # 'get_ipython()' was not found.Don't patch the file.
        return False

    def apply_patch2(stream, prefix):
        patch2_lines = _patch2.split("\n")
        for lp in patch2_lines:
            stream.write(prefix + lp + "\n")

    def get_prefix(s):
        # Returns the sequence of spaces and tabs at the beginning of the code line
        prefix = ""
        while s and (s[0] == " " or s[0] == "\t"):
            prefix += s[0]
            s = s[1:]
        return prefix

    patch_first = patch_before_first_line(code)

    with open(tmp_fln, "w") as fln_out:
        # insert 'try ..'
        fln_out.writelines(_patch1)
        if patch_first:
            apply_patch2(fln_out, "")
        for line in code:
            fln_out.write(" " * 4 + line)
            if is_get_ipython_in_line(line) == GetIPythonUsed.IMPORTED:
                # Keep the same indentation as in the preceding line
                prefix = get_prefix(line)
                apply_patch2(fln_out, prefix)
        # insert 'except ..'
        fln_out.writelines(_patch3)

    return tmp_fln


# Discard RE and db from the profile namespace (if they exist).
def _discard_re_from_nspace(nspace, *, keep_re=False):
    """
    Discard RE and db from the profile namespace (if they exist).
    """
    if not keep_re:
        nspace.pop("RE", None)
        nspace.pop("db", None)


def load_profile_collection(path, *, patch_profiles=True, keep_re=False):
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

    Returns
    -------
    nspace: dict
        namespace in which the profile collection was executed

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

    # Load the files into the namespace 'nspace'.
    try:
        nspace = {}
        for file in file_list:
            logger.info(f"Loading startup file '{file}' ...")
            fln_tmp = _patch_profile(file) if patch_profiles else file
            nspace = runpy.run_path(fln_tmp, nspace)

            if "__plan_exc_info" in nspace:
                exc_info = nspace["__plan_exc_info"]
                raise exc_info[1].with_traceback(exc_info[2])

        _discard_re_from_nspace(nspace, keep_re=keep_re)

    finally:
        try:
            if path_is_set:
                sys.path.remove(path)
        except Exception:
            pass

    return nspace


def load_startup_module(module_name, *, keep_re=False):
    """
    Populate namespace by import a module.

    Parameters
    ----------
    module_name: str
        name of the module to import
    keep_re: boolean
        Indicates if ``RE`` and ``db`` defined in the module should be kept (``True``)
        or removed (``False``).

    Returns
    -------
    nspace: dict
        namespace that contains objects loaded from the module.
    """
    importlib.invalidate_caches()

    _module = importlib.import_module(module_name)
    nspace = _module.__dict__

    _discard_re_from_nspace(nspace, keep_re=keep_re)

    return nspace


class StartupLoadingError(Exception):
    ...


def load_startup_script(script_path, *, keep_re=False, enable_local_imports=False):
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

    Returns
    -------
    nspace: dict
        namespace that contains objects loaded from the module.
    """
    importlib.invalidate_caches()

    if not os.path.isfile(script_path):
        raise ImportError(f"Failed to load the script '{script_path}': script was not found")

    nspace = {}

    if enable_local_imports:
        p = os.path.split(script_path)[0]
        sys.path.insert(0, p)  # Needed to make local imports work.
        # Save the list of available modules
        sm_keys = list(sys.modules.keys())

    try:
        nspace_global, nspace_local = {}, {}
        exec(open(script_path).read(), nspace_global, nspace_local)
        nspace = nspace_global
        nspace.update(nspace_local)

    except BaseException as ex:
        raise StartupLoadingError(f"Error encountered executing startup script at '{script_path}'") from ex

    finally:
        if enable_local_imports:
            # Delete data on all modules that were loaded by the script.
            # We don't need them anymore. Modules will be reloaded from disk if
            #   the script is executed again.
            for key in list(sys.modules.keys()):
                if key not in sm_keys:
                    print(f"Deleting the key '{key}'")
                    del sys.modules[key]

            sys.path.remove(p)

    _discard_re_from_nspace(nspace, keep_re=keep_re)

    return nspace


def load_worker_startup_code(
    *, startup_dir=None, startup_module_name=None, startup_script_path=None, keep_re=False
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

    Returns
    -------
    nspace : dict
       Dictionary with loaded namespace data
    """

    if sum([_ is None for _ in [startup_dir, startup_module_name, startup_script_path]]) != 2:
        raise ValueError("Source of the startup code was not specified or multiple sources were specified.")

    if startup_dir is not None:
        logger.info("Loading RE Worker startup code from directory '%s' ...", startup_dir)
        startup_dir = os.path.abspath(os.path.expanduser(startup_dir))
        print(f"startup_dir={startup_dir}")
        nspace = load_profile_collection(startup_dir, keep_re=keep_re)

    elif startup_module_name is not None:
        logger.info("Loading RE Worker startup code from module '%s' ...", startup_module_name)
        nspace = load_startup_module(startup_module_name, keep_re=keep_re)

    elif startup_script_path is not None:
        logger.info("Loading RE Worker startup code from script '%s' ...", startup_script_path)
        startup_script_path = os.path.abspath(os.path.expanduser(startup_script_path))
        nspace = load_startup_script(startup_script_path, keep_re=keep_re)

    else:
        logger.warning(
            "Source of startup information is not specified. RE Worker will be started with empty environment."
        )
        nspace = {}

    return nspace


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
        if inspect.isgeneratorfunction(obj):
            plans[name] = obj
    return plans


def devices_from_nspace(nspace):
    """
    Extract devices from the namespace. Currently the function returns the dict of ophyd.Device objects.

    Parameters
    ----------
    nspace: dict
        Namespace that may contain plans.

    Returns
    -------
    dict(str: callable)
        Dictionary of devices.
    """
    import ophyd

    devices = {}
    for item in nspace.items():
        if isinstance(item[1], ophyd.Device):
            devices[item[0]] = item[1]
    return devices


def prepare_plan(plan, *, allowed_plans, allowed_devices):
    """
    Prepare the plan: replace the device names (str) in the plan specification by
    references to ophyd objects; replace plan name by the reference to the plan.

    Parameters
    ----------
    plan: dict
        Plan specification. Keys: `name` (str) - plan name, `args` - plan args,
        `kwargs` - plan kwargs.
    allowed_plans: dict(str, callable)
        Dictionary of allowed plans.
    allowed_devices: dict(str, ophyd.Device)
        Dictionary of allowed devices

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

    def ref_from_name(v, allowed_items):
        if isinstance(v, str):
            if v in allowed_items:
                v = allowed_items[v]
        return v

    def process_argument(v, allowed_items):
        # Recursively process lists (iterables) and dictionaries
        if isinstance(v, str):
            v = ref_from_name(v, allowed_items)
        elif isinstance(v, dict):
            for key, value in v.copy().items():
                v[key] = process_argument(value, allowed_items)
        elif isinstance(v, Iterable):
            v_original = v
            v = list()
            for item in v_original:
                v.append(process_argument(item, allowed_items))
        return v

    # TODO: should we allow plan names as arguments?
    allowed_items = allowed_devices

    plan_func = process_argument(plan_name, allowed_plans)
    if isinstance(plan_func, str):
        success = False
        err_msg = f"Plan '{plan_name}' is not allowed or does not exist."

    plan_args_parsed = process_argument(plan_args, allowed_items)
    plan_kwargs_parsed = process_argument(plan_kwargs, allowed_items)

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
        err_msg = f"Plan metadata must be a dictionary or a list of dictionaries: '{pprint.pformat(plan_meta)}'"

    if not success:
        raise RuntimeError(f"Error while parsing the plan: {err_msg}")

    plan_prepared = {
        "name": plan_func,
        "args": plan_args_parsed,
        "kwargs": plan_kwargs_parsed,
        "meta": plan_meta,
    }
    return plan_prepared


# ===============================================================================
#   Validation of plan parameters


def _compare_types(object_in, object_out):
    """
    Compares types of parameters. If both parameters are iterables (list or tuple),
    then the function is recursively called for each element. A set of rules is
    applied to types to determine if the types are matching.
    """
    if isinstance(object_in, (list, tuple)) and isinstance(object_out, (list, tuple)):
        match = True
        if len(object_in) == len(object_out):
            for o_in, o_out in zip(object_in, object_out):
                if not _compare_types(o_in, o_out):
                    match = False
        else:
            # This should never happen, since 'object_out' is obtained by
            #   applying transformations on 'object_in', but it is still
            #   good to check to avoid exceptions.
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


def _process_custom_annotation(custom_annotation):
    """
    Process custom annotation if it exists (custom_annotation["annotation"]).
    If there is no annotation, then return None and empty list of temporary type objects.
    If it exists, then return reference to the annotation (type object) and the list
    temporary types that need to be deleted once the processing (parameter validation)
    is complete.

    Parameters
    ----------
    custom_annotation: dict
        Dictionary that may contain custom annotation (key ``custom_annotation``).

    Returns
    -------
    type, list
        Tuple (annotation type, list of temporary types).
    """
    created_type_list = []
    annotation = None
    try:
        if "annotation" in custom_annotation:
            # Read annotation (as a string)
            annotation_str = custom_annotation["annotation"]

            # Create the dictionary of devices or plans
            devices = custom_annotation.get("devices", {})
            plans = custom_annotation.get("plans", {})
            items = devices.copy()
            items.update(plans)

            if items:
                # Create types
                for item_name in items:
                    # The dictionary value for the devices/plans may be a list(tuple) of items
                    #   or None. If the value is None, then any device or plan will be accepted.
                    #   The list of device or plan names will be used to construct enum.Enum
                    #   type, which is used to verify if the name in args or kwargs matches
                    #   one of the arguments.
                    if items[item_name] is not None:
                        # Create temporary unique type name
                        while True:
                            type_name = f"_type__{random.randint(0, 100000)}_"
                            if type_name not in globals():
                                break

                        # Replace all occurrences of the type nae in the custom annotation.
                        annotation_str = annotation_str.replace(item_name, type_name)

                        # Create temporary type as a subclass of enum.Enum, e.g.
                        # enum.Enum('Device', {'det1': 'det1', 'det2': 'det2', 'det3': 'det3'})
                        type_code = f"enum.Enum('{type_name}', {{"
                        for d in items[item_name]:
                            type_code += f"'{d}': '{d}',"
                        type_code += "})"
                        globals()[type_name] = eval(type_code)
                        created_type_list.append(type_name)

                    else:
                        # Accept any device or plan: any string will be accepted without
                        #   verification.
                        annotation_str = annotation_str.replace(item_name, "str")

            # Once all the types are created,  execute the code for annotation.
            annotation = eval(annotation_str)

    except Exception:
        # In case of exception, delete all types that were already created
        for type_name in created_type_list:
            globals().pop(type_name)
        raise

    return annotation, created_type_list


def _construct_parameters(param_list):
    """
    Construct the list of ``inspect.Parameter`` parameters based on parameter list.
    """
    parameters = []
    created_type_list = []
    try:

        for p in param_list:

            # Generate annotation from custom annotation data if possible.
            #   'annotation == None', then use 'official' Python annotation
            annotation_custom = None
            if "custom" in p:
                annotation_custom, type_list = _process_custom_annotation(p["custom"])
                created_type_list += type_list

            # Custom annotation always overrides Python annotation
            if annotation_custom:
                annotation = annotation_custom
            elif "annotation_pickled" in p:
                annotation = pickle.loads(hex2bytes(p["annotation_pickled"]))
            else:
                # If no annotation is provided, then accept any value
                annotation = typing.Any

            # If there is no annotation, then accepty any value:
            #   'pydantic' doesn't understand 'empty' annotation.
            if annotation == inspect.Parameter.empty:
                annotation = typing.Any

            if "default_pickled" in p:
                default = pickle.loads(hex2bytes(p["default_pickled"]))
            else:
                default = inspect.Parameter.empty

            param = inspect.Parameter(
                name=p["name"], kind=p["kind"]["value"], default=default, annotation=annotation
            )
            parameters.append(param)

    except Exception:
        # In case of exception, delete all types that were already created
        for type_name in created_type_list:
            globals().pop(type_name)
        raise

    return parameters, created_type_list


def pydantic_create_model(kwargs, parameters):
    """
    Create the 'pydantic' model based on parameters.

    Parameters
    ----------
    kwargs: dict
        Bound parameters of the functioncall
    parameters: list
        Parameters of the plan with annotation.
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

    Model = pydantic.create_model("Model", **model_kwargs)

    # Verify the arguments using 'pydantic' model
    return Model(**kwargs)


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
    # Reconstruct signature
    created_type_list = []
    try:
        # Reconstruct 'inspect.Parameter' parameters based on the parameter list
        parameters, created_type_list = _construct_parameters(param_list)

        # Create signature based on the list of parameters
        sig = inspect.Signature(parameters)

        # Verify the list of parameters based on signature.
        bound_args = sig.bind(*call_args, **call_kwargs)

        m = pydantic_create_model(bound_args.arguments, parameters)

        # The final step: match types of the output value of the pydantic model with input
        #   types. 'Pydantic' is converting types whenever possible, but we need precise
        #   type checking with a fiew exceptions
        success, msg = _compare_in_out(bound_args.arguments, m.dict())
        if not success:
            raise ValueError(f"Error in argument types: {msg}")

    except Exception:
        raise

    finally:
        # Delete all temporary types from global namespace.
        for type_name in created_type_list:
            globals().pop(type_name)

    return bound_args.arguments


def validate_plan(plan, *, allowed_plans, allowed_devices):
    """
    Validate the dictionary of plan parameters. Expected to be called before the plan
    is added to the queue.

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
    (boolean, str)
        Success (True/False) and error message that indicates the reason for plan
        rejection
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

            param_list = copy.deepcopy(allowed_plans[plan_name]["parameters"])

            # Filter 'devices' and 'plans' entries of 'param_list'. Leave only plans that are
            #   in 'allowed_plans' and devices that are in 'allowed_devices'
            for p in param_list:
                if ("custom" in p) and ("plans" in p["custom"]):
                    p_plans = p["custom"]["plans"]
                    for p_type in p_plans:
                        p_plans[p_type] = tuple(_ for _ in p_plans[p_type] if _ in allowed_plans)
                if (allowed_devices is not None) and ("custom" in p) and ("devices" in p["custom"]):
                    p_dev = p["custom"]["devices"]
                    for p_type in p_dev:
                        p_dev[p_type] = tuple(_ for _ in p_dev[p_type] if _ in allowed_devices)

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
        msg = f"Plan validation failed: {str(ex)}\nPlan: {pprint.pformat(plan)}"

    return success, msg


def bytes2hex(bytes_array):
    """
    Converts byte array (output of ``pickle.dumps()``) to spaced hexadecimal string representation.

    Parameters
    ----------
    bytes_array: bytes
        Array of bytes to be converted.

    Returns
    -------
    str
        Hexadecimal representation of the byte array.
    """
    s_hex = bytes_array.hex()
    # Insert spaces between each hex number. It makes YAML file formatting better.
    return " ".join([s_hex[n : n + 2] for n in range(0, len(s_hex), 2)])  # noqa: E203


def hex2bytes(hex_str):
    """
    Converts spaced hexadecimal string (output of ``bytes2hex()``) function to an array of bytes.

    Parameters
    ----------
    hex_str: str
        String of hexadecimal numbers separated by spaces.

    Returns
    -------
    bytes
        Array of bytes (ready to be unpicked).
    """
    # Delete spaces from the string to prepare it for conversion.
    hex_str = hex_str.replace(" ", "")
    return bytes.fromhex(hex_str)


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


def _process_plan(plan):
    """
    Returns parameters of a plan.

    Parameters
    ----------
    plan: function
        reference to the function implementing the plan

    Returns
    -------
    dict
        Dictionary with plan parameters with the following keys: ``module`` - name
        of the module where the plan is located, ``name`` - name of the plan, ``parameters`` -
        the list of plan parameters. The list of plan parameters include ``name``, ``kind``,
        ``default`` (if available) and ``annotation`` (if available).
    """

    def filter_values(v):
        if v is inspect.Parameter.empty:
            return ""
        return str(v)

    sig = inspect.signature(plan)
    docstring = getattr(plan, "__doc__", None)

    param_annotation = getattr(plan, "_custom_parameter_annotation_", None)
    doc_annotation = _parse_docstring(docstring)

    ret = {"name": plan.__name__, "parameters": []}

    desc = None
    if param_annotation:
        desc = param_annotation.get("description", "")

    # Try extracting description from docstring data
    if not desc and doc_annotation:
        desc = doc_annotation.get("description", "")

    ret["description"] = desc

    # Function parameters
    for p in sig.parameters.values():
        working_dict = {"name": p.name}
        ret["parameters"].append(working_dict)

        if ("parameters" in doc_annotation) and (p.name in doc_annotation["parameters"]):
            desc = doc_annotation["parameters"][p.name].get("description", None)
            if desc:
                working_dict["description"] = desc

        working_dict["kind"] = {"name": p.kind.name, "value": p.kind.value}

        working_dict["default"] = str(filter_values(p.default))
        working_dict["default_pickled"] = bytes2hex(pickle.dumps(p.default))

        working_dict["annotation"] = str(filter_values(p.annotation))
        working_dict["annotation_pickled"] = bytes2hex(pickle.dumps(p.annotation))

        if param_annotation and ("parameters" in param_annotation):
            working_dict["custom"] = param_annotation["parameters"].get(p.name, {})

    # Return value
    return_info = {}

    if "returns" in doc_annotation:
        desc = doc_annotation["returns"].get("description", None)
        if desc:
            return_info["description"] = desc

    return_info["annotation"] = str(filter_values(sig.return_annotation))
    return_info["annotation_pickled"] = bytes2hex(pickle.dumps(sig.return_annotation))
    if param_annotation and ("returns" in param_annotation):
        return_info["custom"] = param_annotation["returns"]
    ret["returns"] = return_info
    return ret


def _prepare_plans(plans):
    """
    Prepare dictionary of existing plans for saving to YAML file.
    """
    return {k: _process_plan(v) for k, v in plans.items()}


def _prepare_devices(devices):
    """
    Prepare dictionary of existing devices for saving to YAML file.
    """
    from bluesky.utils import is_movable

    return {
        k: {
            "is_movable": is_movable(v),  # True - motor, False - detector
            "classname": type(v).__name__,
            "module": type(v).__module__,
        }
        for k, v in devices.items()
    }


def _unpickle_types(existing_dict):
    """
    Unpickle types in the dictionary of existing devices or plans. Unpickling is needed if
    two dictionaries need to be compared (representation of pickled items may differ between
    different versions of Python). Currently used in unit tests.

    The data structure contains lists of dictionaries, so unpickling function must be able
    to iteratively process the list elements. The data is processed in place, the function
    does not return anything.
    """
    if isinstance(existing_dict, dict):
        for key, value in existing_dict.items():
            if key.endswith("_pickled"):
                if isinstance(value, str):
                    unpickled_value = pickle.loads(hex2bytes(value))
                    existing_dict[key] = unpickled_value
            else:
                _unpickle_types(value)
    elif isinstance(existing_dict, list) or isinstance(existing_dict, tuple):
        for item in existing_dict:
            _unpickle_types(item)


def gen_list_of_plans_and_devices(
    *,
    startup_dir=None,
    startup_module_name=None,
    startup_script_path=None,
    file_dir=None,
    file_name=None,
    overwrite=False,
):
    """
    Generate the list of plans and devices from a collection of startup files, python module or
    a script. Only one source of startup code should be specified, otherwise an exception will be
    raised.

    If ``file_name`` is specified, it is used as a name for the output file, otherwise
    the default file name ``existing_plans_and_devices.yaml`` is used. The file will be saved
    to ``file_dir`` or current directory if ``file_dir`` is not specified or ``None``.

    Parameters
    ----------
    startup_dir: str or None
        path to the directory that contains a collection of startup files (IPython-style)
    startup_module_name: str or None
        name of the startup module to load
    startup_script_path: str or None
        name of the startup script
    file_dir: str or None
        path to the directory where the file is to be created. None - create file in current directory.
    file_name: str
        name of the output YAML file, None - default file name is used
    overwrite: boolean
        overwrite the file if it already exists

    Returns
    -------
    None

    Raises
    ------
    RuntimeError
        Error occurred while creating or saving the lists.
    """
    file_name = file_name or "existing_plans_and_devices.yaml"
    try:
        if file_dir is None:
            file_dir = os.getcwd()

        if sum([_ is None for _ in [startup_dir, startup_module_name, startup_script_path]]) != 2:
            raise ValueError("Source of the startup code was not specified or multiple sources were specified.")

        nspace = load_worker_startup_code(
            startup_dir=startup_dir,
            startup_module_name=startup_module_name,
            startup_script_path=startup_script_path,
        )
        plans = plans_from_nspace(nspace)
        devices = devices_from_nspace(nspace)

        existing_plans_and_devices = {
            "existing_plans": _prepare_plans(plans),
            "existing_devices": _prepare_devices(devices),
        }

        file_path = os.path.join(file_dir, file_name)
        if os.path.exists(file_path) and not overwrite:
            raise IOError(f"File '{file_path}' already exists. File overwriting is disabled.")

        with open(file_path, "w") as stream:
            stream.write("# This file is automatically generated. Edit at your own risk.\n")
            yaml.dump(existing_plans_and_devices, stream)

    except Exception as ex:
        raise RuntimeError(f"Failed to create the list of plans and devices: {str(ex)}")


def gen_list_of_plans_and_devices_cli():
    """
    'qserver-list-plans-devices'
    CLI tool for generating the list of existing plans and devices based on profile collection.
    The tool is supposed to be called as 'qserver-list-plans-devices' from command line.
    The function will ALWAYS overwrite the existing list of plans and devices (the list
    is automatically generated, so overwriting (updating) should be a safe operation that doesn't
    lead to loss configuration data.
    """
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("bluesky_queueserver").setLevel("INFO")

    parser = argparse.ArgumentParser(
        description="Bluesky-QServer: CLI tool for generating the list of plans and devices\n"
        "  from beamline profile collection.",
        epilog=f"Bluesky-QServer version {qserver_version}.",
    )
    parser.add_argument(
        "--file-dir",
        dest="file_dir",
        action="store",
        required=False,
        default=None,
        help="Directory where the list of plans and devices is saved. By default, the list is saved "
        "to the file 'existing_plans_and_devices.yaml' in the current directory.",
    )
    parser.add_argument(
        "--file-name",
        dest="file_name",
        action="store",
        required=False,
        default=None,
        help="Name of the file where the list of plans and devices is saved. Default file name "
        "'existing_plans_and_devices.yaml' is used unless the parameter is not specified.",
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--startup-dir",
        dest="startup_dir",
        type=str,
        default=None,
        help="Path to directory that contains a set of startup files (*.py and *.ipy). All the scripts "
        "in the directory will be sorted in alphabetical order of their names and loaded in "
        "the Run Engine Worker environment. The set of startup files may be located in any accessible "
        "directory. Example: 'qserver-list-plans-devices --startup-dir .' load startup "
        "files from the current directory and saves the lists to the file in current directory.",
    )
    group.add_argument(
        "--startup-module",
        dest="startup_module_name",
        type=str,
        default=None,
        help="The name of the module with startup code. Example: "
        "'qserver-list-plans-devices --startup-module some.startup.module' loads startup "
        "code from the module 'some.startup.module' and saves results to the file in the current directory.",
    )
    group.add_argument(
        "--startup-script",
        dest="startup_script_path",
        type=str,
        default=None,
        help="The path to the script with startup code. Example: "
        "'qserver-list-plans-devices --startup-script ~/startup/scripts/script.py' loads"
        "startup code from the script and saves the results to the file in the current directory.",
    )

    args = parser.parse_args()
    file_dir = args.file_dir
    file_name = args.file_name
    startup_dir = args.startup_dir
    startup_module_name = args.startup_module_name
    startup_script_path = args.startup_script_path

    if file_dir is not None:
        file_dir = os.path.expanduser(file_dir)
        file_dir = os.path.abspath(file_dir)

    try:
        gen_list_of_plans_and_devices(
            startup_dir=startup_dir,
            startup_module_name=startup_module_name,
            startup_script_path=startup_script_path,
            file_dir=file_dir,
            file_name=file_name,
            overwrite=True,
        )
        print("The list of existing plans and devices was created successfully.")
        exit_code = 0
    except BaseException as ex:
        logger.exception("Failed to create the list of plans and devices: %s", str(ex))
        exit_code = 1
    return exit_code


def load_existing_plans_and_devices(path_to_file=None):
    """
    Load the lists of allowed plans and devices from YAML file. Returns empty lists
    if `path_to_file` is None or "".

    Parameters
    ----------
    path_to_file: str on None
        Full path to .yaml file that contains the lists.

    Returns
    -------
    (dict, dict)
        List of allowed plans and list of allowed devices.

    Raises
    ------
    IOError in case the file does not exist or no startup files were found.
    """
    if not path_to_file:
        return {}, {}

    if not os.path.isfile(path_to_file):
        raise IOError(
            f"Failed to load the list of allowed plans and devices: file '{path_to_file}' does not exist."
        )

    with open(path_to_file, "r") as stream:
        existing_plans_and_devices = yaml.load(stream, Loader=yaml.FullLoader)

    existing_plans = existing_plans_and_devices["existing_plans"]
    existing_devices = existing_plans_and_devices["existing_devices"]

    return existing_plans, existing_devices


_user_group_permission_schema = {
    "type": "object",
    "additionalProperties": False,
    "required": ["user_groups"],
    "properties": {
        "user_groups": {
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "required": ["allowed_plans", "forbidden_plans", "allowed_devices", "forbidden_devices"],
                "additionalProperties": False,
                "properties": {
                    "allowed_plans": {
                        "type": "array",
                        "additionalItems": {"type": "string"},
                        "items": [{"type": ["string", "null"]}],
                    },
                    "forbidden_plans": {
                        "type": "array",
                        "additionalItems": {"type": "string"},
                        "items": [{"type": ["string", "null"]}],
                    },
                    "allowed_devices": {
                        "type": "array",
                        "additionalItems": {"type": "string"},
                        "items": [{"type": ["string", "null"]}],
                    },
                    "forbidden_devices": {
                        "type": "array",
                        "additionalItems": {"type": "string"},
                        "items": [{"type": ["string", "null"]}],
                    },
                },
            },
        },
    },
}


def load_user_group_permissions(path_to_file=None):
    """
    Load the data on allowed plans and devices for user groups.

    Parameters
    ----------
    path_to_file: str
        Full path to YAML file that contains user data permissions.

    Returns
    -------
    dict
        Data structure with user permissions. Returns ``{}`` if path is empty string
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

        # if "user_groups" not in user_group_permissions:
        #    raise IOError(f"Incorrect format of user group permissions: key 'user_groups' is missing.")
        jsonschema.validate(instance=user_group_permissions, schema=_user_group_permission_schema)

        if "root" not in user_group_permissions["user_groups"]:
            raise Exception("Missing required user group: 'root'")
        if "admin" not in user_group_permissions["user_groups"]:
            raise Exception("Missing required user group: 'admin'")

    except Exception as ex:
        msg = f"Error while loading user group permissions from file '{path_to_file}': {str(ex)}"
        raise IOError(msg)

    return user_group_permissions


def _select_allowed_items(item_dict, allow_patterns, disallow_patterns):
    """
    Creates the dictionary of items selected from `item_dict` such that item names
    satisfy re patterns in `allow_patterns` and not satisfy patterns in `disallow_patterns`.
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
        Selected item should not match any of the re patterns. If the value is ``[None]``
        or ``[]`` then no items are deselected.

    Returns
    -------
    dict
        Dictionary of the selected items.
    """
    items_selected = {}
    for item in item_dict:
        select_item = False
        if allow_patterns:
            if allow_patterns[0] is None:
                select_item = True
            else:
                for pattern in allow_patterns:
                    if re.search(pattern, item):
                        select_item = True
                        break
        if select_item:
            if disallow_patterns and (disallow_patterns[0] is not None):
                for pattern in disallow_patterns:
                    if re.search(pattern, item):
                        select_item = False
                        break
        if select_item:
            items_selected[item] = copy.deepcopy(item_dict[item])

    return items_selected


def load_allowed_plans_and_devices(path_existing_plans_and_devices=None, path_user_group_permissions=None):
    """
    Generate dictionaries of allowed plans and devices for each user group. If there are no user
    groups defined (path is None), then output dictionaries will have one user group 'root'
    with all existing plans and devices allowed. If the plans and dictionaries do not exist
    (path_existing_plans_and_devices is None), then empty dictionaries are returned.

    Parameters
    ----------
    path_existing_plans_and_devices: str or None
        Full path to YAML file, which contains dictionaries of existing plans and devices.
        Raises an exception if the file does not exist. If None, then the empty dictionaries
        for the allowed plans and devices are returned.
    path_user_group_permissions: str or None
        Full path to YAML file, which contains information on user group permissions.
        Exception is raised if the file does not exist. If None, then the output
        dictionaries will contain one group 'root' with all existing devices and plans
        set as allowed.

    Returns
    -------
    dict, dict
        Dictionaries of allowed plans and devices. Dictionary keys are names of the user groups.
        Dictionaries are ``{}`` if no data on existing plans and devices is provided.
        Dictionaries contain one group (``root``) if no user perission data is provided.

    """
    user_group_permissions = load_user_group_permissions(path_user_group_permissions)
    existing_plans, existing_devices = load_existing_plans_and_devices(path_existing_plans_and_devices)

    allowed_plans, allowed_devices = {}, {}

    if user_group_permissions:
        user_groups = list(user_group_permissions["user_groups"].keys())

        # First filter the plans and devices based on the permissions for 'root' user group.
        #   The 'root' user group permissions should be used to exclude all 'junk', i.e.
        #   unused/outdated/not functioning plans and devices collected from namespace, from
        #   the lists available to ALL the users.
        plans_root, devices_root = existing_plans, existing_devices
        if "root" in user_groups:
            group_permissions_root = user_group_permissions["user_groups"]["root"]
            if existing_plans:
                plans_root = _select_allowed_items(
                    existing_plans,
                    group_permissions_root["allowed_plans"],
                    group_permissions_root["forbidden_plans"],
                )
            if existing_devices:
                devices_root = _select_allowed_items(
                    existing_devices,
                    group_permissions_root["allowed_devices"],
                    group_permissions_root["forbidden_devices"],
                )

        # Now create lists of allowed devices and plans based on the lists for the 'root' user group
        for group in user_groups:
            group_permissions = user_group_permissions["user_groups"][group]

            if existing_plans:
                selected_plans = _select_allowed_items(
                    plans_root, group_permissions["allowed_plans"], group_permissions["forbidden_plans"]
                )
                allowed_plans[group] = selected_plans

            if existing_devices:
                selected_devices = _select_allowed_items(
                    devices_root, group_permissions["allowed_devices"], group_permissions["forbidden_devices"]
                )
                allowed_devices[group] = selected_devices

    if not allowed_plans and existing_plans:
        allowed_plans["root"] = existing_plans

    if not allowed_devices and existing_devices:
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
