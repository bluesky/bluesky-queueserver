import os
import glob
import runpy

import ophyd


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
        raise IOError(f"Failed to load the profile collection. Path '{path}' is not a directory.")

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
