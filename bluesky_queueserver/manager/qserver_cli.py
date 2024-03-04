import argparse
import ast
import asyncio
import copy
import enum
import getpass
import json
import logging
import os
import pprint
import shutil
import tempfile
import time as ttime
import uuid
from datetime import datetime

import yaml

import bluesky_queueserver

from .comms import (
    default_zmq_control_address,
    generate_zmq_keys,
    generate_zmq_public_key,
    validate_zmq_key,
    zmq_single_request,
)
from .logging_setup import PPrintForLogging as ppfl
from .plan_queue_ops import PlanQueueOperations

logger = logging.getLogger(__name__)

qserver_version = bluesky_queueserver.__version__


class CommandParameterError(Exception): ...


class QServerExitCodes(enum.Enum):
    SUCCESS = 0
    PARAMETER_ERROR = 1
    REQUEST_FAILED = 2
    COMMUNICATION_ERROR = 3
    EXCEPTION_OCCURRED = 4
    OPERATION_FAILED = 5


default_user = "qserver-cli"
default_user_group = "primary"

# The following text is displayed as part of help information (-h or --help option)
cli_examples = """
Examples of CLI commands
------------------------
qserver -h       # Display help
qserver monitor  # Start 'qserver' in monitoring mode

qserver ping     # Send 'ping' request to RE Manager via ZMQ
qserver status   # Request status of RE Manager

qserver config   # Get RE Manager config

qserver environment open         # Open RE environment
qserver environment close        # Close RE environment
qserver environment destroy      # Destroy RE environment (kill RE worker process)

qserver environment update             # Update the worker state based on contents of worker namespace
qserver environment update background  # Update the worker state as a background task

qserver existing plans           # Request the list of existing plans
qserver existing devices         # Request the list of existing devices
qserver allowed plans            # Request the list of allowed plans
qserver allowed devices          # Request the list of allowed devices
qserver permissions reload       # Reload user permissions and generate lists of allowed plans and devices.
qserver permissions reload lists # Same, but reload lists of existing plans and devices from disk.

qserver permissions set <path-to-file>  # Set user group permissions (from .yaml file)
qserver permissions get                 # Get current user group permissions

qserver queue add plan '<plan-params>'                 # Add plan to the back of the queue
qserver queue add instruction <instruction>            # Add instruction to the back of the queue
qserver queue add plan front '<plan-params>'           # Add plan to the front of the queue
qserver queue add plan back '<plan-params>'            # Add plan to the back of the queue
qserver queue add plan 2 '<plan-params>'               # Insert plan at position 2
qserver queue add instruction 2 <instruction>          # Insert instruction at position 2
qserver queue add plan -1 '<plan-params>'              # Insert plan at position -1
qserver queue add plan before '<uid>' '<plan-params>'  # Insert the plan before the plan with given UID
qserver queue add plan after '<uid>' '<plan-params>'   # Insert the plan after the plan with given UID
NOTE: Position indices are 0-based. Inserting a plan to position 0 pushes it to the front of the queue.
      Negative position indices are counted from the back of the queue. Request for a plan with index -1
      returns the last plan of the queue. Inserting a plan at position -1 makes it previous to last.

qserver queue update plan <uid> '<plan-params>'         #  Update item with <uid> with a plan
qserver queue replace plan <uid> '<plan-params>'        #  Replace item with <uid> with a plan
qserver queue update instruction <uid> '<instruction>'  #  Update item with <uid> with an instruction
qserver queue replace instruction <uid> '<instruction>' #  Replace item with <uid> with an instruction

qserver queue execute plan '<plan-params>'              # Immediately execute the plan
qserver queue execute instruction <instruction>         # Immediately execute an instruction

Example of JSON specification of a plan:
    '{"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 10, "delay": 1}}'

Supported queue instructions:
    queue-stop  # stops execution of the queue

qserver queue get    # Request the list of items (plans or instructions) in the queue
qserver queue clear  # Clear the queue (remove all plans from the queue)

qserver queue item get           # Request the last item in the queue
qserver queue item get back      # Request the last item in the queue
qserver queue item get front     # Request the first item in the queue
qserver queue item get 2         # Request the item at position 2
qserver queue item get '<uid>'   # Request the item with given Item UID

qserver queue item remove          # Remove the last item from the queue
qserver queue item remove back     # Remove the last item from the queue
qserver queue item remove front    # Remove the first item from the queue
qserver queue item remove 2        # Remove the item at position 2
qserver queue item remove '<uid>'  # Remove the item with the given UID

qserver queue item move 2 5                             # Move item from position 2 to position 5 of the queue
qserver queue item move back front                      # Move item from the back to the front of the queue
qserver queue item move front -2                        # Move item from the front of the queue to position -2
qserver queue item move '<uid-src>' 5                   # Move item with UID <uid-src> to position 5
qserver queue item move 2 before '<uid-dest>'           # Place item at position 2 before an item with <uid-dest>
qserver queue item move 2 after '<uid-dest>'            # Place item at position 2 after an item with <uid-dest>
qserver queue item move '<uid-src>' before '<uid-dest>' # Place item with <uid-src> before item with <uid-dest>

qserver queue start        # Start execution of the queue
qserver queue stop         # Request execition of the queue to stop after current plan
qserver queue stop cancel  # Cancel request to stop execution of the queue

# Enable and disable autostart
qserver queue autostart enable
qserver queue autostart disable

# Change the queue mode. Enable/disable LOOP and IGNORE_FAILURES modes:
qserver queue mode set loop True
qserver queue mode set loop False
qserver queue mode set ignore_failures True
qserver queue mode set ignore_failures False

# The following requests are forwarded to the Run Engine:
qserver re pause           # Request to PAUSE currently executed plan at the next checkpoint
qserver re pause deferred  # Request to PAUSE currently executed plan at the next checkpoint
qserver re pause immediate # Request to immediately PAUSE currently executed plan
qserver re resume          # RESUME execution of a paused plan
qserver re stop            # STOP execution of a paused plan
qserver re abort           # ABORT execution of a paused plan
qserver re halt            # HALT execution of a paused plan

qserver re runs            # Get the list of active runs (runs generated by the currently running plans)
qserver re runs active     # Get the list of active runs
qserver re runs open       # Get the list of open runs (subset of active runs)
qserver re runs closed     # Get the list of closed runs (subset of active runs)

qserver history get        # Request plan history
qserver history clear      # Clear plan history

qserver function execute <function-params>             # Start execution of a function
qserver function execute <function-params> background  # ... in the background thread

Example of JSON specification of a function ("args" and "kwargs" are optional):
    '{"name": "function_sleep", "args": [20], "kwargs": {}}'

qserver script upload <path-to-file>              # Upload a script to RE Worker environment
qserver script upload <path-to-file> background   # ... in the background
qserver script upload <path-to-file> update-re    # ... allow 'RE' and 'db' to be updated
qserver script upload <path-to-file> keep-lists   # ... leave lists of allowed and existing plans and devices
                                                  #   unchanged (saves processing time)

qserver task result <task-uid>  # Load status or result of a task with the given UID
qserver task status <task-uid>  # Check status of a task with the given UID

qserver kernel interrupt            # Send interrupt (Ctrl-C) to IPython kernel
qserver kernel interrupt task       # ... if the manager is executing a task
qserver kernel interrupt plan       # ... if the manager is executing a plan
qserver kernel interrupt task plan  # ... if the manager is executing a plan or a task

qserver lock environment  -k 90g94                   # Lock the environment
qserver lock environment "Locked for 1 hr" -k 90g94  # Add a text note
qserver lock queue -k 90g94                          # Lock the queue
qserver lock all -k 90g94                            # Lock environment and the queue

qserver lock info                        # Load lock status
qserver lock info -k 90g94               # Load lock status and validate the key

qserver unlock -k 90g94                  # Unlock RE Manager

qserver manager stop           # Safely exit RE Manager application
qserver manager stop safe on   # Safely exit RE Manager application
qserver manager stop safe off  # Force RE Manager application to stop
NOTE: Exit with 'safe on' option will succeed only if RE Manager is in IDLE state (queue is not running).
If called with 'safe off' option, the request will force RE Manager to terminate RE Worker process and
exit even if a plan is running.

qserver manager kill test  # Kills RE Manager by stopping asyncio event loop. Used only for testing.
"""


def extract_source_address(params):
    """
    Extract 'source' item address (index or UID) from the list of parameters. Returns
    the list of remaining parameters. The source address is represented by 1 parameter:
    keywords ``front``, ``back``, or integer number represent a positional address (index),
    any other string is interpreted as UID.

    Parameters
    ----------
    params : list(str)
         List of parameters. The first parameter is interpreted as a source address.

    Returns
    -------
    dict
        Dictionary that contains source address. Elements: ``pos`` - positional address
        (value is int or a string from the set ``front``, ``back``), ``uid`` - uid of
        the item (value is a string representing UID). Empty dictionary if no address
        is found.
    list(str)
        List of the remaining parameters

    Raises
    ------
    IndexError
        Insufficient number of parameters is provided (less than 1)
    """
    n_used = 0

    pos, uid = None, None
    if params[0] in ("front", "back"):
        pos = params[0]
        n_used = 1
    else:
        try:
            pos = int(params[0])
            n_used = 1
        except Exception:
            ...

        if pos is None:
            uid = params[0]
            n_used = 1

    if pos is not None:
        addr_param = {"pos": pos}
    elif uid is not None:
        addr_param = {"uid": uid}
    else:
        addr_param = {}
    return addr_param, params[n_used:]


def extract_destination_address(params):
    """
    Extract 'destination' item address (index or UID) from the list of parameters. Returns
    the list of remaining parameters. The source address is represented by 1 or 2 parameters:
    if 1st parameter is a keywords ``front``, ``back``, or integer number, then the parameter
    represents a positional address (index); if the 1st parameter is a keyword ``before`` or
    ``after``, the 2nd parameter is considered to represent item UID (string). If the 1st
    parameter can not be converted to ``int`` or equal to one of the keywords, it is considered
    that the address is not found.

    Parameters
    ----------
    params : list(str)
         List of parameters. The 1st and optionally the 2nd parameters are interpreted as a destination
         address.

    Returns
    -------
    dict
        Dictionary that contains destination address. Elements: ``pos`` - positional address
        (value is int or a string from the set ``front``, ``back``), ``before_uid`` or ``after_uid``
        - uid of the item preceding or following the destination for the item in the queue (value
        is a string representing UID).  Empty dictionary if no address is found.
    list(str)
        List of the remaining parameters.

    Raises
    ------
    IndexError
        Insufficient number of parameters is provided.
    """
    n_used = 0

    pos, uid, uid_key = None, None, None
    if params[0] in ("front", "back"):
        pos = params[0]
        n_used = 1
    elif params[0] in ("before", "after"):
        uid = params[1]
        # Keys are "before_uid" and "after_uid"
        uid_key = f"{params[0]}_uid"
        n_used = 2
    else:
        try:
            pos = int(params[0])
            n_used = 1
        except Exception:
            ...

    if pos is not None:
        addr_param = {"pos": pos}
    elif uid is not None:
        addr_param = {uid_key: uid}
    else:
        addr_param = {}
    return addr_param, params[n_used:]


def format_list_as_command(params):
    """
    Format list of items as a string. Restores the look of the parameters as they appear in command
    line. Used for printing error messages.

    Parameters
    ----------
    params : list
        List of parameters to print

    Returns
    -------
    str
        Representation of the list as a formatted string
    """
    return " ".join([str(_) for _ in params])


def raise_request_not_supported(params):
    """
    Raises ``CommandParameterError`` exception with ``request is not supported`` message.

    Parameters
    ----------
    params : list
        List of parameters that represent the request. The request will be included in the error
        message

    Raises
    ------
    CommandParameterError
    """
    s = format_list_as_command(params)
    raise CommandParameterError(f"Request '{s}' is not supported")


def check_number_of_parameters(params, n_min, n_max, params_report=None):
    """
    Checks if the number of parameters in ``params`` list is in the range ``[n_min, n_max]``.
    Raises exception if the number of parameters is less than ``n_min`` or more than ``n_max``.

    Parameters
    ----------
    params : list
        The list of parameters. If the number of parameters in the list is outside the
        range ``[n_min, n_max]``, then ``CommandParameterError`` exception is raised.
    n_min, n_max : int
        The range for the number of parameters in ``params`` list.
    params_report : list
        The list of parameters that is included in the error message. If the value is
        ``None``, then parameters are not included in the error message.

    Raises
    ------
    CommandParameterError
    """
    s = format_list_as_command(params_report) if params_report is not None else None
    if len(params) < n_min:
        err_msg = "Some parameters are missing in request"
        if params_report is not None:
            err_msg += f" '{s}'"
        if params_report == params:
            err_msg += f": Minimum number of parameters: {n_min}"
        raise CommandParameterError(err_msg)
    if len(params) > n_max:
        err_msg = "Request"
        if params_report is not None:
            err_msg += f" '{s}'"
        err_msg += " contains extra parameters"
        if params_report == params:
            err_msg += f": Minimum number of parameters: {n_max}"
        raise CommandParameterError(err_msg)


def msg_queue_add_update(params, *, cmd_opt):
    """
    Generate outgoing message for `queue add` command. See ``cli_examples`` for supported formats
    for the command.

    Parameters
    ----------
    params : list
        List of parameters of the command. The first element of the list is expected to be ``add`` keyword.
    cmd_opt : str
        Command option, must match ``param[0]``.

    Returns
    -------
    str
        Name of the method from RE Manager API
    dict
        Dictionary of the method parameters

    Raises
    ------
    CommandParameterError
    """
    # Check if the function was called for the appropriate command
    command = "queue"
    expected_p0 = cmd_opt
    if params[0] != expected_p0:
        raise ValueError(f"Incorrect parameter value '{params[0]}'. Expected value: '{expected_p0}'")

    # Make sure that there is sufficient number of parameters to start processing
    if len(params) < 3:
        raise CommandParameterError(f"Item type and value are not specified: '{command} {params[0]}'")

    if (params[0] in ("update", "replace")) and (len(params) != 4):
        raise CommandParameterError(f"Incorrect number of parameters: '{command} {params[0]}'")

    p_item_type = params[1]
    if p_item_type not in ("plan", "instruction"):
        raise_request_not_supported([command, params[0], params[1]])

    try:
        # Destination address is optional. If no destination index or UID found, then
        #   'addr_param' is {}.
        if params[0] == "add":
            update_uid = None
            addr_param, p_item = extract_destination_address(params[2:])
        elif params[0] in ("update", "replace"):
            update_uid = params[2]  # Next parameter is UID
            addr_param, p_item = {}, params[3:]
        elif params[0] == "execute":
            update_uid = None
            addr_param, p_item = {}, params[2:]
        else:
            raise CommandParameterError(f"Option '{params[0]}' is not supported: '{command} {params[0]}'")

        # There should be exactly 1 parameter left. This parameter should contain a plan
        #   or an instruction.
        check_number_of_parameters(p_item, 1, 1, params)
        if p_item_type == "plan":
            try:
                # Convert quoted string to dictionary.
                plan = ast.literal_eval(p_item[0])
            except Exception:
                raise CommandParameterError(f"Error occurred while parsing the plan '{p_item[0]}'")
            if update_uid:
                plan["item_uid"] = update_uid
            plan["item_type"] = "plan"
            addr_param.update({"item": plan})
        elif p_item_type == "instruction":
            if p_item[0] == "queue-stop":
                instruction = {"name": "queue_stop"}
            else:
                raise CommandParameterError(f"Unsupported instruction type: {p_item[0]}")
            if update_uid:
                instruction["item_uid"] = update_uid
            instruction["item_type"] = "instruction"
            addr_param.update({"item": instruction})
        else:
            # This indicates a bug in the program.
            raise ValueError(f"Unknown item type: {p_item_type}")

    except IndexError:
        raise CommandParameterError(f"The command '{params}' contains insufficient number of parameters")

    option = params[0] if (params[0] != "replace") else "update"
    method = f"{command}_item_{option}"
    prms = addr_param
    prms["user"] = default_user
    prms["user_group"] = default_user_group
    if params[0] in ("update", "replace"):
        prms["replace"] = params[0] == "replace"

    return method, prms


def msg_function_execute(params, *, cmd_opt):
    """
    Generate outgoing message for `function_execute` command. See ``cli_examples`` for supported formats
    for the command.

    Parameters
    ----------
    params : list
        List of parameters of the command. The first elements of the list is expected to be
        ``["function", "execute"]``.
    cmd_opt : str
        Command option, must match ``param[0]``.

    Returns
    -------
    str
        Name of the method from RE Manager API
    dict
        Dictionary of the method parameters

    Raises
    ------
    CommandParameterError
    """
    # Check if the function was called for the appropriate command
    command = "function"
    expected_p0 = cmd_opt
    if params[0] != expected_p0:
        raise ValueError(f"Incorrect parameter value '{params[0]}'. Expected value: '{expected_p0}'")

    call_params = {}

    try:
        if params[0] == "execute":
            if len(params) < 2:
                raise CommandParameterError(f"Incorrect number of parameters: '{command} {params[0]}'")

            item_str = params[1]
            try:
                # Convert quoted string to dictionary.
                item = ast.literal_eval(item_str)
            except Exception:
                raise CommandParameterError(f"Error occurred while parsing the plan '{item_str}'")

            item["item_type"] = "function"

            run_in_background = False
            allowed_params = ("background",)
            for p in params[2:]:
                if p not in allowed_params:
                    raise CommandParameterError(f"Unsupported combination of parameters: '{command} {params}'")
                if p == "background":
                    run_in_background = True

            call_params["item"] = item
            call_params["run_in_background"] = run_in_background
        else:
            raise CommandParameterError(f"Option '{params[0]}' is not supported: '{command} {params[0]}'")

    except IndexError:
        raise CommandParameterError(f"The command '{params}' contains insufficient number of parameters")

    method = f"{command}_{params[0]}"
    prms = call_params
    prms["user"] = default_user
    prms["user_group"] = default_user_group

    return method, prms


def msg_queue_item(params, *, lock_key):
    """
    Generate outgoing message for `queue item` command. The supported options are ``get``,
    ``move`` and ``remove``.

    Parameters
    ----------
    params : list
        List of parameters of the command. The first element of the list is expected to be ``get`` keyword.

    Returns
    -------
    str
        Name of the method from RE Manager API
    dict
        Dictionary of the method parameters

    """
    # Check if the function was called for the appropriate command
    command = "queue"
    expected_p0 = "item"
    if params[0] != expected_p0:
        raise ValueError(f"Incorrect parameter value '{params[0]}'. Expected value: '{expected_p0}'")

    # Make sure that there is a sufficient number of parameters to start processing
    if len(params) < 2:
        raise CommandParameterError(f"Item type and options are not specified '{command} {params[0]}'")

    p_item_type = params[1]
    if p_item_type not in ("get", "remove", "move"):
        raise_request_not_supported([command, params[0], params[1]])
    try:
        if p_item_type in ("get", "remove"):
            if len(params) >= 3:
                addr_param_src, p_item = extract_source_address(params[2:])
                # There should be no parameters left
                check_number_of_parameters(p_item, 0, 0, params)
                if not addr_param_src:
                    raise CommandParameterError(
                        f"Source address could not be found: '{format_list_as_command(params)}'"
                    )

                addr_param = addr_param_src
            else:
                # Default: operation with the element at the back of the queue.
                #   We don't need to pass the item address explicitly.
                addr_param = {}

        elif p_item_type == "move":
            addr_param_src, p_item = extract_source_address(params[2:])
            addr_param_dest, p_item = extract_destination_address(p_item)

            # There should be no parameters left
            check_number_of_parameters(p_item, 0, 0, params)
            if not addr_param_src:
                raise CommandParameterError(
                    f"Source index or UID is not found: '{format_list_as_command(params)}'"
                )
            if not addr_param_dest:
                raise CommandParameterError(
                    f"Destination aindex or UID is not found: '{format_list_as_command(params)}'"
                )

            # Change the key from 'pos' to 'pos_dest' ('pos' is used for 'source' position)
            pos_dest = addr_param_dest.pop("pos", None)
            if pos_dest is not None:
                addr_param_dest["pos_dest"] = pos_dest

            addr_param = addr_param_src
            addr_param.update(addr_param_dest)

        else:
            # This indicates a bug in the program.
            raise ValueError(f"Unknown item type: {p_item_type}")

    except IndexError:
        raise CommandParameterError(f"The command '{params}' contain insufficient number of parameters")

    method = f"{command}_{params[0]}_{params[1]}"
    prms = addr_param

    if p_item_type in ("remove", "move"):
        if lock_key:
            prms["lock_key"] = lock_key

    return method, prms


def msg_queue_mode(params):
    """
    Generate outgoing messages for `queue_mode_...` commands. The supported option is ``set``.

    Parameters
    ----------
    params : list
        List of parameters of the command. The first two elements of the list are expected to
        be ``mode`` and ``set`` keywords.

    Returns
    -------
    str
        Name of the method from RE Manager API
    dict
        Dictionary of the method parameters
    """
    # Check if the function was called for the appropriate command
    command = "queue"
    expected_p0 = "mode"
    if params[0] != expected_p0:
        raise ValueError(f"Incorrect parameter value '{params[0]}'. Expected value: '{expected_p0}'")

    # Make sure that there is a sufficient number of parameters to start processing
    if len(params) < 2:
        raise CommandParameterError(f"Item type and options are not specified '{command} {params[0]}'")

    p_item_type = params[1]
    if p_item_type != "set":
        raise_request_not_supported([command, params[0], params[1]])

    try:
        if p_item_type == "set":
            params_mode = params[2:]
            if len(params_mode) % 2:
                raise CommandParameterError(
                    f"The list of queue mode parameters must have even number of elements: {params_mode}"
                )

            queue_mode = {params_mode[i]: params_mode[i + 1] for i in range(0, len(params_mode), 2)}
            for k in queue_mode.keys():
                # Attempt to evaluate key parameters (e.g. "True" should become boolean True)
                #   If a parameter fails to evaluate, it should remain a string.
                try:
                    queue_mode[k] = eval(queue_mode[k], {}, {})
                except Exception:
                    pass
            cmd_prms = {"mode": queue_mode}
        else:
            # This indicates a bug in the program.
            raise ValueError(f"Unknown item type: {p_item_type}")

    except IndexError:
        raise CommandParameterError(f"The command '{params}' contain insufficient number of parameters")

    method = f"{command}_{params[0]}_{params[1]}"
    prms = cmd_prms

    return method, prms


def msg_queue_autostart(params):
    """
    Generate outgoing messages for `queue_autostart` command: 'queue autostart enable' and
    'queue autostart disable'

    Parameters
    ----------
    params : list
        List of parameters of the command. The first two elements of the list are expected to
        be ``autostart`` and ``enable``/``disable`` keywords.

    Returns
    -------
    str
        Name of the method from RE Manager API
    dict
        Dictionary of the method parameters
    """
    command, prms = "queue", {}
    method = f"{command}_{params[0]}"
    if len(params) != 2:
        raise CommandParameterError(f"Request '{command} {params[0]}' must include two parameters")
    if params[1] == "enable":
        prms["enable"] = True
    elif params[1] == "disable":
        prms["enable"] = False
    else:
        raise CommandParameterError(f"Request '{command} {params[0]} {params[1]}' is not supported")

    return method, prms


def msg_queue_stop(params):
    """
    Generate outgoing message for `queue stop` command.

    Parameters
    ----------
    params : list
        List of parameters of the command. The first element of the list is expected to be ``stop`` keyword.

    Returns
    -------
    str
        Name of the method from RE Manager API
    dict
        Dictionary of the method parameters

    """
    # Check if the function was called for the appropriate command
    command = "queue"
    expected_p0 = "stop"
    if params[0] != expected_p0:
        raise ValueError(f"Incorrect parameter value '{params[0]}'. Expected value: '{expected_p0}'")

    check_number_of_parameters(params, 1, 2, params)

    method = f"{command}_{params[0]}"
    if len(params) == 2:
        if params[1] == "cancel":
            method = f"{command}_{params[0]}_{params[1]}"
        else:
            cmd = format_list_as_command([command] + params)
            raise CommandParameterError(f"Unknown option '{params[1]}' in the command '{cmd}'")

    prms = {}
    return method, prms


def msg_re_pause(params):
    """
    Generate outgoing message for `re pause` command.

    Parameters
    ----------
    params : list
        List of parameters of the command. The first element of the list is expected to be ``pause`` keyword.

    Returns
    -------
    str
        Name of the method from RE Manager API
    dict
        Dictionary of the method parameters

    """
    # Check if the function was called for the appropriate command
    command = "re"
    expected_p0 = "pause"
    if params[0] != expected_p0:
        raise ValueError(f"Incorrect parameter value '{params[0]}'. Expected value: '{expected_p0}'")

    check_number_of_parameters(params, 1, 2, params)

    method = f"{command}_{params[0]}"
    option = "deferred"
    if len(params) == 2:
        if params[1] in ("deferred", "immediate"):
            option = params[1]
        else:
            cmd = format_list_as_command([command] + params)
            raise CommandParameterError(f"Unknown option '{params[1]}' in the command '{cmd}'")

    prms = {"option": option}

    return method, prms


def create_msg(params, *, lock_key):
    """
    Create outgoing message based on command-line arguments.

    Parameters
    ----------
    params: list
        List of parameters (positional arguments) supplied via command line.

    lock_key: str or None
        Lock key

    Returns
    -------
    method: str
        The name of the method.
    prms: dict
        The dictionary of method parameters.
    monitoring_mode: bool
        Indicates if monitoring mode is enabled.

    Raises
    ------
    CommandParameterError
        Failure to generate consistent outgoing message based on provided list of parameters.
    """
    if not params:
        raise CommandParameterError("Command is not specified")

    if not isinstance(params, list):
        raise TypeError("Command arguments are not represented as a list")

    monitoring_mode = False

    command = params[0]
    params = params[1:]

    prms = {}

    # ----------- monitor ------------
    if command == "monitor":
        if len(params) != 0:
            raise CommandParameterError(f"Parameters are not allowed for '{command}' request")
        monitoring_mode = True
        method = "ping"

    # ----------- ping ------------
    elif command == "ping":
        if len(params) != 0:
            raise CommandParameterError(f"Parameters are not allowed for '{command}' request")
        method = command

    # ----------- status ------------
    elif command == "status":
        if len(params) != 0:
            raise CommandParameterError(f"Parameters are not allowed for '{command}' request")
        method = command

    # ----------- status ------------
    elif command == "config":
        if len(params) != 0:
            raise CommandParameterError(f"Parameters are not allowed for '{command}' request")
        method = "config_get"

    # ----------- environment ------------
    elif command == "environment":
        if len(params) not in (1, 2):
            raise CommandParameterError(f"Request '{command}' must include at one or two parameters")
        supported_params = ("open", "close", "destroy", "update")
        if params[0] in supported_params:
            if params[0] == "update":
                if len(params) == 2:
                    if params[1] == "background":
                        prms["run_in_background"] = True
                    else:
                        raise CommandParameterError(
                            f"Request '{command} {params[0]} {params[1]}' is not supported"
                        )
            else:
                if len(params) == 2:
                    raise CommandParameterError(
                        f"Request '{command} {params[1]}' may have no additional parameters"
                    )
            method = f"{command}_{params[0]}"
            if lock_key:
                prms["lock_key"] = lock_key
        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    # ----------- allowed ------------
    elif command == "allowed":
        if len(params) != 1:
            raise CommandParameterError(f"Request '{command}' must include only one parameter")
        supported_params = ("plans", "devices")
        if params[0] in supported_params:
            method = f"{params[0]}_{command}"
            prms["user_group"] = default_user_group
        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    # ----------- existing ------------
    elif command == "existing":
        if len(params) != 1:
            raise CommandParameterError(f"Request '{command}' must include only one parameter")
        supported_params = ("plans", "devices")
        if params[0] in supported_params:
            method = f"{params[0]}_{command}"
        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    # ----------- permissions ------------
    elif command == "permissions":
        if params[0] == "reload":
            if len(params) not in (1, 2):
                raise CommandParameterError(
                    f"Request '{command} {params[0]}' must include only one or two parameters"
                )
            method = f"{command}_{params[0]}"
            if len(params) == 2:
                if params[1] == "lists":
                    prms["restore_plans_devices"] = True
                    if lock_key:
                        prms["lock_key"] = lock_key
                else:
                    CommandParameterError(f"Request '{command} {params[0]} {params[1]}' is not supported")

        elif params[0] == "get":
            if len(params) != 1:
                raise CommandParameterError(f"Request '{command} {params[0]}' must include only one parameter")
            method = f"{command}_{params[0]}"

        elif params[0] == "set":
            if len(params) != 2:
                raise CommandParameterError(f"Request '{command} {params[0]}' must include only one parameter")
            method = f"{command}_{params[0]}"

            file_name = params[1]
            try:
                with open(file_name, "r") as f:
                    permissions_yaml = f.read()
                permissions_dict = yaml.load(permissions_yaml, Loader=yaml.FullLoader)
            except Exception as ex:
                raise CommandParameterError(
                    f"Request '{command}': failed to read the user group permissions from file '{file_name}': {ex}"
                )
            prms["user_group_permissions"] = permissions_dict
            if lock_key:
                prms["lock_key"] = lock_key

        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    # ----------- script ------------
    elif command == "script":
        if len(params) < 2:
            raise CommandParameterError(f"Request '{command}' must include at least two parameters")
        if params[0] == "upload":
            # Parameter 1 (required) - file name, parameter 2 (optional) - 'background'
            file_name = params[1]
            try:
                with open(file_name, "r") as f:
                    script = f.read()
            except Exception as ex:
                raise CommandParameterError(
                    f"Request '{command}': failed to read the script from file '{file_name}': {ex}"
                )

            run_in_background = False
            update_re = False
            update_lists = True
            allowed_values = ("background", "update-re", "keep-lists")

            for p in params[2:]:
                if p not in allowed_values:
                    raise CommandParameterError(
                        f"Request '{command}': parameter combination {params} is not supported"
                    )
                if p == "background":
                    run_in_background = True
                elif p == "update-re":
                    update_re = True
                elif p == "keep-lists":
                    update_lists = False

            method = f"{command}_{params[0]}"
            prms = {
                "script": script,
                "run_in_background": run_in_background,
                "update_re": update_re,
                "update_lists": update_lists,
                "lock_key": lock_key,
            }

        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    # ----------- task ------------
    elif command == "task":
        if len(params) != 2:
            raise CommandParameterError(f"Request '{command}' must include at 3 parameters")
        if params[0] == "result":
            task_uid = str(params[1])
            method = f"{command}_{params[0]}"
            prms = {"task_uid": task_uid}
        elif params[0] == "status":
            task_uid = str(params[1])
            method = f"{command}_{params[0]}"
            prms = {"task_uid": task_uid}
        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    # ----------- function ------------
    elif command == "function":
        if len(params) < 1:
            raise CommandParameterError(f"Request '{command}' must include at least one parameter")
        if params[0] == "execute":
            method, prms = msg_function_execute(params, cmd_opt=params[0])
            if lock_key:
                prms["lock_key"] = lock_key
        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    # ----------- queue ------------
    elif command == "queue":
        if len(params) < 1:
            raise CommandParameterError(f"Request '{command}' must include at least one parameter")
        supported_params = (
            "add",
            "update",
            "replace",
            "execute",
            "get",
            "clear",
            "item",
            "start",
            "stop",
            "mode",
            "autostart",
        )
        if params[0] in supported_params:
            if params[0] in ("add", "update", "replace", "execute"):
                method, prms = msg_queue_add_update(params, cmd_opt=params[0])
                if lock_key:
                    prms["lock_key"] = lock_key

            elif params[0] in ("get", "clear", "start"):
                if len(params) != 1:
                    raise CommandParameterError(f"Request '{command} {params[0]}' must include only one parameter")
                method, prms = f"{command}_{params[0]}", {}
                if params[0] in ("clear", "start"):
                    if lock_key:
                        prms["lock_key"] = lock_key

            elif params[0] == "stop":
                method, prms = msg_queue_stop(params)
                if lock_key:
                    prms["lock_key"] = lock_key

            elif params[0] == "item":
                method, prms = msg_queue_item(params, lock_key=lock_key)

            elif params[0] == "mode":
                method, prms = msg_queue_mode(params)
                if lock_key:
                    prms["lock_key"] = lock_key

            elif params[0] == "autostart":
                method, prms = msg_queue_autostart(params)

                if len(params) != 2:
                    raise CommandParameterError(f"Request '{command} {params[0]}' must include two parameters")
                if params[1] == "enable":
                    prms["enable"] = True
                elif params[1] == "disable":
                    prms["enable"] = False
                else:
                    raise CommandParameterError(f"Request '{command} {params[0]} {params[1]}' is not supported")

        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    # ----------- re ------------
    elif command == "re":
        if len(params) < 1:
            raise CommandParameterError(f"Request '{command}' must include at least one parameter")
        supported_params = ("pause", "resume", "stop", "abort", "halt", "runs")
        if params[0] in supported_params:
            if params[0] == "pause":
                method, prms = msg_re_pause(params)
            elif params[0] == "runs":
                if len(params) == 1:
                    method, prms = f"{command}_{params[0]}", {}
                elif (len(params) == 2) and (params[1] in ("active", "open", "closed")):
                    method, prms = f"{command}_{params[0]}", {"option": params[1]}
                else:
                    raise CommandParameterError(
                        f"Unrecognized combination of parameters: {format_list_as_command(params)}"
                    )
            else:
                method, prms = f"{command}_{params[0]}", {}

            if lock_key:
                prms["lock_key"] = lock_key

        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    elif command == "history":
        if len(params) < 1:
            raise CommandParameterError(f"Request '{command}' must include at least one parameter")
        supported_params = ("get", "clear")
        if params[0] in supported_params:
            method, prms = f"{command}_{params[0]}", {}
            if params[0] == "clear":
                if lock_key:
                    prms["lock_key"] = lock_key
        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    elif command == "manager":
        if len(params) < 1:
            raise CommandParameterError(f"Request '{command}' must include at least one parameter")
        if params[0] == "stop":
            method = f"{command}_{params[0]}"
            if len(params) == 1:
                prms = {}
            elif (len(params) == 3) and (params[1] == "safe") and (params[2] in ("on", "off")):
                option = f"{params[1]}_{params[2]}"
                prms = {"option": option}
            else:
                raise CommandParameterError(
                    f"Unsupported number or combination of parameters: {format_list_as_command(params)}"
                )
        elif params[0] == "kill":
            if params == ["kill", "test"]:
                method = f"{command}_{params[0]}"
                prms = {}
            else:
                raise CommandParameterError(
                    f"Unsupported number or combination of parameters: {format_list_as_command(params)}"
                )
        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    elif command == "kernel":
        if len(params) < 1:
            raise CommandParameterError(f"Request '{command}' must include at least one parameter")
        if params[0] == "interrupt":
            method = f"{command}_{params[0]}"
            prms = {}
            for p in params[1:]:
                if p not in ("task", "plan"):
                    raise CommandParameterError(f"Unsupported parameter {p!r}: {format_list_as_command(params)}")
                if p == "task":
                    prms.update(dict(interrupt_task=True))
                elif p == "plan":
                    prms.update(dict(interrupt_plan=True))
        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    elif command == "lock":
        if len(params) == 0:
            raise CommandParameterError("Command {command!r} requires at least one parameter.")
        elif params[0] == "info":
            if len(params) != 1:
                raise CommandParameterError(
                    f"Too many parameters (only one expected): {format_list_as_command(params)}"
                )
            method = "lock_info"
            if lock_key:
                prms["lock_key"] = lock_key
        elif params[0] in ("environment", "queue", "all"):
            if len(params) > 2:
                raise CommandParameterError(
                    f"Too many parameters (1 or 2 are expected): {format_list_as_command(params)}"
                )
            if (len(params) == 2) and (params[1] in ("environment", "queue", "all")):
                raise CommandParameterError(
                    f"Unsupported combination of parameters:  {format_list_as_command(params)}"
                )
            method = "lock"
            if params[0] in ("environment", "all"):
                prms["environment"] = True
            if params[0] in ("queue", "all"):
                prms["queue"] = True
            if len(params) == 2:
                prms["note"] = params[1]
            if lock_key:
                prms["lock_key"] = lock_key
            prms["user"] = default_user
        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    elif command == "unlock":
        if len(params) != 0:
            raise CommandParameterError(
                f"The command accepts no extra parameters: {format_list_as_command(params)}"
            )
        method = "unlock"
        if lock_key:
            prms["lock_key"] = lock_key

    else:
        raise CommandParameterError(f"Unrecognized command '{command}'")

    return method, prms, monitoring_mode


def prepare_qserver_output(msg):
    """
    Prepare received message for printing by 'qserver' CLI tool.

    Parameters
    ----------
    msg: dict
        Full message received from RE Manager.

    Returns
    -------
    str
        Formatted message, which is ready for printing.
    """
    msg = copy.deepcopy(msg)
    # Do not print large dicts in the log: replace values with "..."
    large_dicts = ("plans_allowed", "plans_existing", "devices_allowed", "devices_existing")
    for dict_name in large_dicts:
        if dict_name in msg:
            d = msg[dict_name]
            for k in d.keys():
                d[k] = "{...}"
    return ppfl(msg)


def qserver():
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("bluesky_queueserver").setLevel("ERROR")

    s_enc = (
        "If RE Manager is configured to use encrypted ZeroMQ communication channel,\n"
        "the encryption must also be enabled before running 'qserver' CLI tool by setting\n"
        "the environment variable QSERVER_ZMQ_PUBLIC_KEY to the value of a valid public key\n"
        "(z85-encoded 40 character string):\n\n"
        "    export QSERVER_ZMQ_PUBLIC_KEY='<public_key>'\n\n"
        "Encryption is disabled by default."
    )

    def formatter(prog):
        # Set maximum width such that printed help mostly fits in the RTD theme code block (documentation).
        return argparse.RawDescriptionHelpFormatter(prog, max_help_position=20, width=90)

    parser = argparse.ArgumentParser(
        description="Command-line tool for communicating with RE Monitor.\n"
        f"bluesky-queueserver version {qserver_version}.\n",
        formatter_class=formatter,
        epilog=f"\n\n{s_enc}\n\n{cli_examples}\n\n",
    )

    parser.add_argument(
        "command",
        metavar="command",
        type=str,
        nargs="+",
        help="a sequence of keywords and parameters that define the command",
    )

    parser.add_argument(
        "--zmq-control-addr",
        "-a",
        dest="zmq_control_addr",
        action="store",
        default=None,
        help="Address of the control socket of RE Manager. The parameter overrides the address set using "
        "the environment variable QSERVER_ZMQ_CONTROL_ADDRESS. The default value is used if the address is not "
        "set using the parameter or the environment variable. Address format: 'tcp://127.0.0.1:60615' "
        f"(default: {default_zmq_control_address!r}).",
    )

    parser.add_argument(
        "--address",
        dest="address",
        action="store",
        default=None,
        help="The parameter is deprecated and will be removed. Use --zmq-control-addr instead.",
    )

    parser.add_argument(
        "--lock-key",
        "-k",
        dest="lock_key",
        action="store",
        default=None,
        help="Lock key. The key is an arbitrary string is used to lock and unlock RE Manager "
        "('lock' and 'unlock' API) and control the manager when the environment or the queue is locked.",
    )

    args = parser.parse_args()
    print(f"Arguments: {args.command}")

    try:
        address = args.zmq_control_addr
        if args.address is not None:
            logger.warning(
                "The parameter --address is deprecated and will be removed. Use --zmq-control-addr instead."
            )
        address = address or args.address
        # If the 0MQ server address is not specified, try reading it from the environment variable.
        address = address or os.environ.get("QSERVER_ZMQ_CONTROL_ADDRESS", None)
        # If the address is not specified, then use the default address
        address = address or default_zmq_control_address

        lock_key = args.lock_key
        if lock_key is not None:
            if not isinstance(lock_key, str) or not lock_key:
                raise CommandParameterError(
                    f"Lock key must be a non-empty string: submitted lock key is {lock_key!r}"
                )

        # Read public key from the environment variable, then check if the CLI parameter exists
        zmq_public_key = os.environ.get("QSERVER_ZMQ_PUBLIC_KEY", None)
        zmq_public_key = zmq_public_key if zmq_public_key else None  # Case of key==""
        if zmq_public_key is not None:
            try:
                validate_zmq_key(zmq_public_key)
            except Exception as ex:
                raise CommandParameterError(f"ZMQ public key is improperly formatted: {ex}")

        method, params, monitoring_mode = create_msg(args.command, lock_key=lock_key)
        if monitoring_mode:
            print("Running QServer monitor. Press Ctrl-C to exit ...")

        while True:
            msg, msg_err = zmq_single_request(
                method, params, zmq_server_address=address, server_public_key=zmq_public_key
            )

            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")

            if not msg_err:
                print(f"{current_time} - MESSAGE: \n{prepare_qserver_output(msg)}")
                if isinstance(msg, dict) and ("success" in msg) and (msg["success"] is False):
                    exit_code = QServerExitCodes.REQUEST_FAILED
                else:
                    exit_code = QServerExitCodes.SUCCESS
            else:
                print(f"{current_time} - ERROR: {msg_err}")
                exit_code = QServerExitCodes.COMMUNICATION_ERROR

            if not monitoring_mode:
                break
            ttime.sleep(1)

    except CommandParameterError as ex:
        logger.error("Invalid command or parameters: %s.", ex)
        exit_code = QServerExitCodes.PARAMETER_ERROR
    except Exception as ex:
        logger.exception("Exception occurred: %s.", ex)
        exit_code = QServerExitCodes.EXCEPTION_OCCURRED
    except KeyboardInterrupt:
        print("\nThe program was manually stopped.")
        exit_code = QServerExitCodes.SUCCESS

    return exit_code.value


def qserver_zmq_keys():
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("bluesky_queueserver").setLevel("INFO")

    def formatter(prog):
        # Set maximum width such that printed help mostly fits in the RTD theme code block (documentation).
        return argparse.RawDescriptionHelpFormatter(prog, max_help_position=20, width=90)

    parser = argparse.ArgumentParser(
        description="Bluesky-QServer:\nZMQ security: Generate public-private key pair for "
        f"ZeroMQ control communication channel.\nbluesky-queueserver version {qserver_version}.\n\n"
        f"Generate new public-private key pair for secured 0MQ control connection between\n"
        f"RE Manager and client applications. If private key is passed as ``--zmq-private-key``\n"
        f"parameter, then the generated key pair is based on the provided private key.\n",
        formatter_class=formatter,
    )
    parser.add_argument(
        "--zmq-private-key",
        dest="zmq_private_key",
        type=str,
        default=None,
        help="Private key used by RE Manager. If the private key is provided, then the public "
        "key is generated based on the private key. This option allows to create (recover) "
        "public key based on known private key. The passed value should be 40 character "
        "string containing z85 encrypted key.",
    )

    args = parser.parse_args()
    try:
        if args.zmq_private_key is not None:
            private_key = args.zmq_private_key
            validate_zmq_key(private_key)  # Will generate nice error message in case the key is invalid
            public_key = generate_zmq_public_key(private_key)
            msg = "Private key generated based on provided private key."
        else:
            public_key, private_key = generate_zmq_keys()
            msg = "New public-private key pair."

        print("====================================================================================")
        print(f"     ZMQ SECURITY: {msg}")
        print("====================================================================================")
        print(f" Private key (RE Manager):                 {private_key}")
        print(f" Public key (CLI client or HTTP server):   {public_key}")
        print("====================================================================================\n")

    except Exception as ex:
        print(f"Failed to generate keys: {ex}")
        return QServerExitCodes.EXCEPTION_OCCURRED.value

    return QServerExitCodes.SUCCESS.value


def qserver_clear_lock():
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("bluesky_queueserver").setLevel("INFO")

    def formatter(prog):
        # Set maximum width such that printed help mostly fits in the RTD theme code block (documentation).
        return argparse.RawDescriptionHelpFormatter(prog, max_help_position=20, width=90)

    parser = argparse.ArgumentParser(
        description="Bluesky-QServer: Clear RE Manager lock.\n"
        f"bluesky-queueserver version {qserver_version}.\n\n"
        "Recover locked RE Manager if the lock key is lost. The utility requires access to Redis\n"
        "used by RE Manager. Provide the address of Redis service using '--redis-addr' parameter.\n"
        "Restart the RE Manager service after clearing the lock.\n",
        formatter_class=formatter,
    )

    parser.add_argument(
        "--redis-addr",
        dest="redis_addr",
        type=str,
        default="localhost",
        help="The address of Redis server, e.g. 'localhost', '127.0.0.1', 'localhost:6379' "
        "(default: %(default)s). ",
    )

    parser.add_argument(
        "--redis-name-prefix",
        dest="redis_name_prefix",
        type=str,
        default="qs_default",
        help="The prefix for the names of Redis keys used by RE Manager (default: %(default)s). ",
    )

    args = parser.parse_args()

    exit_code = 0

    async def remove_lock():
        nonlocal exit_code
        try:
            pq = PlanQueueOperations(redis_host=args.redis_addr, name_prefix=args.redis_name_prefix)
            await pq.start()
            lock_info = await pq.lock_info_retrieve()
            if lock_info is not None:
                print(f"Detected lock info:\n{ppfl(lock_info)}")
                print("Clearing the lock ...")
                await pq.lock_info_clear()
                print("RE Manager lock was cleared. Restart RE Manager service to unlock the manager!")
            else:
                print("No lock was detected.")
        except Exception as ex:
            exit_code = 1
            logger.error("Failed to clear RE Manager lock: %s", ex)

    asyncio.run(remove_lock())

    return exit_code


def qserver_console_base(*, app_name):
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("bluesky_queueserver").setLevel("INFO")

    def formatter(prog):
        # Set maximum width such that printed help mostly fits in the RTD theme code block (documentation).
        return argparse.RawDescriptionHelpFormatter(prog, max_help_position=20, width=90)

    parser = argparse.ArgumentParser(
        description="Bluesky-QServer: Start Jupyter console for IPython kernel running in the worker process.\n"
        f"bluesky-queueserver version {qserver_version}.\n\n"
        "Requests IPython kernel connection info from RE Manager and starts Jupyter Console. The RE Worker\n"
        "must be running (environment opened) and using IPython kernel. The address of 0MQ control port of\n"
        "RE Manager can be passed as a parameter or an environment variable. If encryption of the control\n"
        "channel is enabled, the public key can be passed by setting QSERVER_ZMQ_PUBLIC_KEY environment\n"
        "variable. Use 'Ctrl-D' to exit the console. Typing 'quit' or 'exit' in the console will close\n"
        "the worker environment.\n",
        formatter_class=formatter,
    )

    parser.add_argument(
        "--zmq-control-addr",
        "-a",
        dest="zmq_control_addr",
        action="store",
        default=None,
        help="Address of the control socket of RE Manager. The parameter overrides the address set using "
        "the environment variable QSERVER_ZMQ_CONTROL_ADDRESS. The default value is used if the address is not "
        "set using the parameter or the environment variable. Address format: 'tcp://127.0.0.1:60615' "
        f"(default: {default_zmq_control_address!r}).",
    )

    args = parser.parse_args()

    exit_code = QServerExitCodes.SUCCESS

    try:
        address = args.zmq_control_addr
        # If the 0MQ server address is not specified, try reading it from the environment variable.
        address = address or os.environ.get("QSERVER_ZMQ_CONTROL_ADDRESS", None)
        # If the address is not specified, then use the default address
        address = address or default_zmq_control_address

        # Read public key from the environment variable, then check if the CLI parameter exists
        zmq_public_key = os.environ.get("QSERVER_ZMQ_PUBLIC_KEY", None)
        zmq_public_key = zmq_public_key if zmq_public_key else None  # Case of key==""
        if zmq_public_key is not None:
            try:
                validate_zmq_key(zmq_public_key)
            except Exception as ex:
                raise CommandParameterError(f"ZMQ public key is improperly formatted: {ex}")

        # Request connection info
        msg, msg_err = zmq_single_request(
            "config_get", zmq_server_address=address, server_public_key=zmq_public_key
        )

        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")

        if not msg_err:
            if isinstance(msg, dict) and ("success" in msg) and (msg["success"] is False):
                print(f"{current_time} - MESSAGE: \n{prepare_qserver_output(msg)}")
                exit_code = QServerExitCodes.REQUEST_FAILED
            else:
                exit_code = QServerExitCodes.SUCCESS
        else:
            print(f"{current_time} - ERROR: {msg_err}")
            exit_code = QServerExitCodes.COMMUNICATION_ERROR

        if exit_code == QServerExitCodes.SUCCESS:
            connect_info = msg["config"]["ip_connect_info"]
            if not connect_info:
                exit_code = QServerExitCodes.OPERATION_FAILED
                logger.error(
                    "Failed to start the console: \nWorker environment is closed or RE Worker is configured "
                    "to run in Python mode (without IPython kernel)."
                )

        if exit_code == QServerExitCodes.SUCCESS:
            logger.info("IPython Kernel Connect Info: \n%s", pprint.pformat(connect_info))

            username = getpass.getuser()
            file_dir = os.path.join(tempfile.gettempdir(), f"qserver_{username}", "kernel_files")
            file_name = "kernel-" + str(uuid.uuid4()).split("-")[0] + ".json"
            file_path = os.path.join(file_dir, file_name)

            logger.info("Creating kernel connection file %r", file_path)
            os.makedirs(file_dir, exist_ok=True)
            with open(file_path, "w") as f:
                json.dump(connect_info, f)

            logger.info("Starting Jupyter Console ...")
            path_exec = shutil.which(app_name)
            if not path_exec:
                logger.error(f"{app_name!r} is not installed.")
                exit_code = QServerExitCodes.OPERATION_FAILED
            else:
                os.execl(path_exec, app_name, f"--existing={file_path}")

    except CommandParameterError as ex:
        logger.error("Invalid command or parameters: %s.", ex)
        exit_code = QServerExitCodes.PARAMETER_ERROR
    except Exception as ex:
        logger.exception("Exception occurred: %s.", ex)
        exit_code = QServerExitCodes.EXCEPTION_OCCURRED
    except KeyboardInterrupt:
        print("\nThe program was manually stopped.")
        exit_code = QServerExitCodes.SUCCESS

    return exit_code.value


def qserver_console():
    return qserver_console_base(app_name="jupyter-console")


def qserver_qtconsole():
    return qserver_console_base(app_name="jupyter-qtconsole")
