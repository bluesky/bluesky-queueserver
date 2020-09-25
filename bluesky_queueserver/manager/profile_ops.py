import os
import glob
import runpy
from collections.abc import Iterable
import pkg_resources
import yaml

import ophyd


def get_default_profile_collection_dir():
    """
    Returns the path to the default profile collection that is distributed with the package.
    The function does not guarantee that the directory exists.
    """
    pc_path = pkg_resources.resource_filename(
        "bluesky_queueserver", "profile_collection_sim/"
    )
    return pc_path


def load_profile_collection(path):
    """
    Load profile collection located at the specified path. The collection consists of
    .py files started with 'DD-', where D is a digit (e.g. 05-file.py). The files
    are alphabetically sorted before execution.

    Parameters
    ----------
    path: str
        path to profile collection

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
        raise IOError(
            f"Failed to load the profile collection. Path '{path}' is not a directory."
        )

    file_pattern = os.path.join(path, "[0-9][0-9]*.py")
    file_list = glob.glob(file_pattern)
    file_list.sort()  # Sort in alphabetical order

    # Load the files into the namespace 'nspace'.
    nspace = None
    for file in file_list:
        nspace = runpy.run_path(file, nspace)

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
    for item in nspace.items():
        if callable(item[1]):
            plans[item[0]] = item[1]
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


# TODO: it may be a good idea to implement 'gen_list_of_plans_and_devices' as a separate CLI tool.
#       For now it can be called from IPython. It shouldn't be called automatically
#       at any time, since it loads profile collection. The list of allowed plans
#       and devices can be also typed manually, since it shouldn't be very large.


def gen_list_of_plans_and_devices(
    path=None, file_name="allowed_plans_and_devices.yaml", overwrite=False
):
    """
    Generate the list of plans and devices from profile collection.
    The list is saved to file `allowed_plans_and_devices.yaml`.

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

        plan_list = list(plans.keys())
        device_list = list(devices.keys())

        allowed_plans_and_devices = {
            "allowed_plans": plan_list,
            "allowed_devices": device_list,
        }

        file_path = os.path.join(path, file_name)
        if os.path.exists(file_path) and not overwrite:
            raise IOError("File '%s' already exists. File overwriting is disabled.")

        with open(file_path, "w") as stream:
            yaml.dump(allowed_plans_and_devices, stream)

    except Exception as ex:
        raise RuntimeError(f"Failed to create the list of devices and plans: {str(ex)}")


def load_list_of_plans_and_devices(path_to_file=None):
    """
    Load the lists of allowed plans and devices from YAML file. Returns empty lists
    if `path_to_file` is None or "".

    Parameters
    ----------
    path_to_file: str on None
        Full path to .yaml file that contains the lists.

    Returns
    -------
    (list, list)
        List of allowed plans and list of allowed devices.

    Raises
    ------
    IOError in case the file does not exist.
    """
    if not path_to_file:
        return {"allowed_plans": [], "allowed_devices": []}

    if not os.path.isfile(path_to_file):
        raise IOError(
            f"Failed to load the list of allowed plans and devices: "
            f"file '{path_to_file}' does not exist."
        )

    with open(path_to_file, "r") as stream:
        allowed_plans_and_devices = yaml.safe_load(stream)

    allowed_plans = allowed_plans_and_devices["allowed_plans"]
    allowed_devices = allowed_plans_and_devices["allowed_devices"]

    return allowed_plans, allowed_devices
