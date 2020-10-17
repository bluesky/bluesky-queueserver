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

import logging

logger = logging.getLogger(__name__)


def get_default_profile_collection_dir():
    """
    Returns the path to the default profile collection that is distributed with the package.
    The function does not guarantee that the directory exists.
    """
    pc_path = pkg_resources.resource_filename("bluesky_queueserver", "profile_collection_sim/")
    return pc_path


_patch1 = """

import logging
logger_patch = logging.Logger(__name__)

__local_namespace = locals()

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
        ip_dummy = IPDummy(__local_namespace)
        return ip_dummy

    get_ipython = get_ipython_patch

"""

_patch3 = """

except BaseException as ex:
    logger_patch.exception("Exception while loading profile: '%s'", str(ex))
    raise

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

    def get_prefix(s):
        # Returns the sequence of spaces and tabs at the beginning of the code line
        prefix = ""
        while s and (s == " " or s == "\t"):
            prefix += s[0]
            s = s[1:]
        return prefix

    with open(tmp_fln, "w") as fln_out:
        # insert 'try ..'
        fln_out.writelines(_patch1)
        is_patched = False
        for line in code:
            fln_out.write("    " + line)
            # The following RE patterns cover only the cases of commenting with '#'.
            if not is_patched:
                if re.search(r"^[^#]*IPython[^#]+get_ipython", line):
                    # Keep the same indentation as in the preceding line
                    prefix = get_prefix(line)
                    for lp in _patch2:
                        fln_out.write(prefix + lp)
                    is_patched = True  # Patch only once
                elif re.search(r"^[^#]*get_ipython *\(", line):
                    # 'get_ipython()' is called before the patch was applied
                    raise RuntimeError(
                        "Profile calls 'get_ipython' before the patch was "
                        "applied. Inspect and correct the code."
                    )
        # insert 'except ..'
        fln_out.writelines(_patch3)

    return tmp_fln


def load_profile_collection(path, patch_profiles=True):
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

    Returns
    -------
    nspace: dict
        namespace in which the profile collection was executed
    """

    # Create the list of files to load
    path = os.path.expanduser(path)
    path = os.path.abspath(path)

    if not os.path.exists(path):
        raise IOError(f"Path '{path}' does not exist.")
    if not os.path.isdir(path):
        raise IOError(f"Failed to load the profile collection. Path '{path}' is not a directory.")

    file_pattern = os.path.join(path, "[0-9][0-9]*.py")
    file_list = glob.glob(file_pattern)
    file_list.sort()  # Sort in alphabetical order

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
        nspace = None
        for file in file_list:
            logger.info(f"Loading startup file '{file}' ...")
            fln_tmp = _patch_profile(file) if patch_profiles else file
            nspace = runpy.run_path(fln_tmp, nspace)

        # Discard RE and db from the profile namespace (if they exist).
        nspace.pop("RE", None)
        nspace.pop("db", None)
    finally:
        try:
            if path_is_set:
                sys.path.remove(path)
        except Exception:
            pass

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
        if callable(obj) and obj.__module__ != "typing":
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


def parse_plan(plan, *, allowed_plans, allowed_devices):
    """
    Parse the plan: replace the device names (str) in the plan specification by
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

    if not success:
        raise RuntimeError("Error while parsing the plan: %s", err_msg)

    plan_parsed = {
        "name": plan_func,
        "args": plan_args_parsed,
        "kwargs": plan_kwargs_parsed,
    }
    return plan_parsed


def validate_plan(plan, *, allowed_plans):
    """
    Validate the dictionary of plan parameters. Expected to be called before the plan
    is added to the queue.

    Parameters
    ----------
    plan: dict
        The dictionary of plan parameters
    allowed_plans: dict
        The dictionary with allowed plans: key - plan name.

    Returns
    -------
    (boolean, str)
        Success (True/False) and error message that indicates the reason for plan
        rejection
    """
    success, msg = True, ""
    completed = False

    # For now allow execution of all plans if no plan descriptions are provided
    #   (plan list is empty). This will change in the future.
    # TODO: at some point make the list of allowed plans required. This will require
    #   implementation of some tools to make generation of basic plan list convenient.
    if not allowed_plans:
        return success, msg

    try:
        if "args" not in plan:
            plan["args"] = []

        if "kwargs" not in plan:
            plan["kwargs"] = {}

        if "name" not in plan:
            msg = "Plan name is not specified."
            raise Exception(msg)

        # Verify that plan name is in the list of allowed plans
        plan_name = plan["name"]
        if plan_name not in allowed_plans:
            msg = f"Plan '{plan['name']}' is not in the list of allowed plans."
            raise Exception(msg)

        parameters = allowed_plans[plan_name]["parameters"]
        plan_args, plan_kwargs = plan["args"], plan["kwargs"]

        # Check if the number of positional arguments is not greater than expected.
        n_param = 0
        for _ in plan_args:
            if n_param >= len(parameters):
                # Too many parameters (more args than the total number of parameters
                #   and there is no '*args')
                raise ValueError(
                    f"The number of args exceeds the number of allowed parameters: "
                    f"{len(parameters)} parameters are allowed."
                )

            param = parameters[n_param]
            pkind = param["kind"]["name"]
            if pkind == "VAR_POSITIONAL":
                # Absorbs the rest of 'args' even if there are no more args.
                n_param += 1
                break
            if pkind in ("POSITIONAL_OR_KEYWORD", "POSITIONAL_ONLY"):
                # Move to the next parameter
                n_param += 1
            elif pkind in ("KEYWORD_ONLY", "VAR_KEYWORD"):
                raise ValueError(
                    f"Plan parameters contain too many args ({len(plan_args)}): " f"{n_param} args are expected."
                )
            else:
                # This should happen only if no args or kwargs are expected.
                raise ValueError(f"Plan parameters contain {len(plan_args)}. No args are expected.")

        if n_param >= len(parameters):
            if plan_kwargs:
                raise ValueError("Plan parameters contains kwargs. No kwargs are expected.")
            else:
                completed = True

        if not completed:
            if parameters[n_param]["kind"]["name"] == "VAR_POSITIONAL":
                n_param += 1

            if n_param >= len(parameters):
                if plan_kwargs:
                    raise ValueError("Plan parameters contains kwargs. No kwargs are expected.")
                else:
                    completed = True

        if not completed:
            # Dictionary of the remaining keys
            premaining = {_["name"]: _ for _ in parameters[n_param:]}

            # Check if there exists a parameter of VAR_POSITIONAL kind. It will indicate that
            #   more args are expected.
            var_keyword_exists = False
            for pname, pinfo in premaining.copy().items():
                if pinfo["kind"]["name"] == "VAR_POSITIONAL":
                    premaining.pop(pname)
                if pinfo["kind"]["name"] == "VAR_KEYWORD":
                    var_keyword_exists = True
                    premaining.pop(pname)

            # Check if there are no kwargs with unexpected names.
            unexpected_kwargs = []
            for pa in plan_kwargs.keys():
                if pa in premaining:
                    premaining.pop(pa)
                else:
                    unexpected_kwargs.append(pa)

            if not var_keyword_exists and unexpected_kwargs:
                raise ValueError(f"Unexpected kwargs {unexpected_kwargs} in the plan parameters.")

            # Now checked if there are any missing kwargs that don't have default values.
            kwargs_missed = []
            for pname, pinfo in premaining.items():
                if ("default" not in pinfo) or (not pinfo["default"]):
                    kwargs_missed.append(pname)
            if kwargs_missed:
                raise ValueError(f"Plan parameters do not contain required args or kwargs {kwargs_missed}.")

    except Exception as ex:
        success = False
        msg = f"Plan validation failed: {str(ex)}\nPlan: {pprint.pformat(plan)}"

    return success, msg


# TODO: it may be a good idea to implement 'gen_list_of_plans_and_devices' as a separate CLI tool.
#       For now it can be called from IPython. It shouldn't be called automatically
#       at any time, since it loads profile collection. The list of allowed plans
#       and devices can be also typed manually, since it shouldn't be very large.

def bytes2hex(bytes_array):
    """
    Converts byte array (result of ``pickle.dumps`` to spaced hexadecimal string representation.

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
    return ' '.join([s_hex[_: _ + 2] for _ in range(0, len(s_hex), 2)])


def hex2bytes(hex_str):
    """
    Converts spaced hexadecimal string (output of ``bytes2hex`` function to an array of bytes.

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

    if hasattr(plan, "_custom_parameter_annotation_"):
        param_annotation = plan._custom_parameter_annotation_
    else:
        param_annotation = None

    ret = {"name": plan.__name__, "parameters": []}

    if param_annotation:
        # The function description should always be present
        ret["description"] = param_annotation.get("description", "")

    # Function parameters
    for p in sig.parameters.values():
        working_dict = {"name": p.name}
        ret["parameters"].append(working_dict)

        working_dict["kind"] = {"name": p.kind.name, "value": p.kind.value}

        working_dict["default"] = str(filter_values(p.default))
        working_dict["default_pickled"] = bytes2hex(pickle.dumps(p.default))

        working_dict["annotation"] = str(filter_values(p.annotation))
        working_dict["annotation_pickled"] = bytes2hex(pickle.dumps(p.annotation))

        if param_annotation and ("parameters" in param_annotation):
            working_dict["custom"] = param_annotation["parameters"].get(p.name, {})

    # Return value
    return_info = {}
    return_info["annotation"] = str(filter_values(sig.return_annotation))
    return_info["annotation_pickled"] = bytes2hex(pickle.dumps(sig.return_annotation))
    if param_annotation and ("returns" in param_annotation):
        return_info["custom"] = param_annotation["returns"]
    ret["returns"] = return_info
    return ret


def gen_list_of_plans_and_devices(path=None, file_name="existing_plans_and_devices.yaml", overwrite=False):
    """
    Generate the list of plans and devices from profile collection.
    The list is saved to file ``existing_plans_and_devices.yaml``.

    Parameters
    ----------
    path: str or None
        path to the directory where the file is to be created. None - create file in current directory.
    file_name: str
        name of the output YAML file
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
    try:
        if path is None:
            path = os.getcwd()

        nspace = load_profile_collection(path)
        plans = plans_from_nspace(nspace)
        devices = devices_from_nspace(nspace)

        existing_plans_and_devices = {
            "existing_plans": {k: _process_plan(v) for k, v in plans.items()},
            "existing_devices": {
                k: {"classname": type(v).__name__, "module": type(v).__module__} for k, v in devices.items()
            },
        }

        file_path = os.path.join(path, file_name)
        if os.path.exists(file_path) and not overwrite:
            raise IOError(f"File '{file_path}' already exists. File overwriting is disabled.")

        with open(file_path, "w") as stream:
            stream.write("# This file is automatically generated. Edit at your own risk.\n")
            yaml.dump(existing_plans_and_devices, stream)

    except Exception as ex:
        raise RuntimeError(f"Failed to create the list of devices and plans: {str(ex)}")


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
    IOError in case the file does not exist.
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

        for group in user_groups:
            group_permissions = user_group_permissions["user_groups"][group]

            if existing_plans:
                selected_plans = _select_allowed_items(
                    existing_plans, group_permissions["allowed_plans"], group_permissions["forbidden_plans"]
                )
                allowed_plans[group] = selected_plans

            if existing_devices:
                selected_devices = _select_allowed_items(
                    existing_devices, group_permissions["allowed_devices"], group_permissions["forbidden_devices"]
                )
                allowed_devices[group] = selected_devices

    if not allowed_plans and existing_plans:
        allowed_plans["root"] = existing_plans

    if not allowed_devices and existing_devices:
        allowed_devices["root"] = existing_devices

    return allowed_plans, allowed_devices
