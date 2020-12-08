import ast
import time as ttime
from datetime import datetime
import pprint
import argparse
import enum

import bluesky_queueserver
from .comms import zmq_single_request

import logging

logger = logging.getLogger(__name__)

qserver_version = bluesky_queueserver.__version__


class CommandParameterError(Exception):
    ...


class QServerExitCodes(enum.Enum):
    SUCCESS = 0
    PARAMETER_ERROR = 1
    REQUEST_FAILED = 2
    COMMUNICATION_ERROR = 3
    EXCEPTION_OCCURRED = 4


default_user = "qserver-cli"
default_user_group = "admin"

# The following text is displayed as part of help information (-h or --help option)
cli_examples = """
Examples of CLI commands
------------------------
qserver -h       # Display help
qserver monitor  # Start 'qserver' in monitoring mode

qserver ping     # Send 'ping' request to RE Manager via ZMQ
qserver status   # Request status of RE Manager

qserver environment open       # Open RE environment
qserver environment close      # Close RE environment
qserver environment destroy    # Destroy RE environment (kill RE worker process)

qserver allowed plans          # Request the list of allowed plans
qserver allowed devices        # Request the list of allowed devices

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
      Negative position indexes are counted from the back of the queue. Request for a plan with index -1
      returns the last plan of the queue. Inserting a plan at position -1 makes it previous to last.

Example of JSON specification of a plan:
    '{"name":"count", "args":[["det1", "det2"]], "kwargs":{"num":10, "delay":1}}'

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

# The following requests are forwarded to the Run Engine:
qserver re pause           # Request to PAUSE currently executed plan at the next checkpoint
qserver re pause deferred  # Request to PAUSE currently executed plan at the next checkpoint
qserver re pause immediate # Request to immediately PAUSE currently executed plan
qserver re resume          # RESUME execution of a paused plan
qserver re stop            # STOP execution of a paused plan
qserver re abort           # ABORT execution of a paused plan
qserver re halt            # HALT execution of a paused plan

qserver history get        # Request plan history
qserver history clear      # Clear plan history

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
    keywords ``front`` ``back`` or integer number represent a positional address (index),
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
        Insufficient number of parameters are provided (less than 1)
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
    if 1st parameter is a keywords ``front`` ``back`` or integer number, then the parameter
    represents a positional address (index); if the 1st parameter is a keyword ``before`` or
    ``after``, the 2nd parameter is considered to represent item UID (string). If the 1st
    parameter can not be converted to ``int`` or equal to one of the keywords, it is considered
    that the address is not found

    Parameters
    ----------
    params : list(str)
         List of parameters. The 1st and optionally 2nd parameter are interpreted as a destination
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
        Insufficient number of parameters are provided.
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


def msg_queue_add(params):
    """
    Generate outgoing message for `queue add` command. See ``cli_examples`` for supported formats
    for the command.

    Parameters
    ----------
    params : list
        List of parameters of the command. The first element of the list is expected to be ``add`` keyword.

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
    expected_p0 = "add"
    if params[0] != expected_p0:
        raise ValueError(f"Incorrect parameter value '{params[0]}'. Expected value: '{expected_p0}'")

    # Make sure that there are sufficient number of parameters to start processing
    if len(params) < 3:
        raise CommandParameterError(f"Item type and value are not specified '{command} {params[0]}'")

    p_item_type = params[1]
    if p_item_type not in ("plan", "instruction"):
        raise_request_not_supported([command, params[0], params[1]])
    try:
        # Destination address is optional. If no destination index or UID found, then
        #   'addr_param' is {}.
        addr_param, p_item = extract_destination_address(params[2:])
        # There should be exactly 1 parameter left. This parameter should contain a plan
        #   or an instruction.
        check_number_of_parameters(p_item, 1, 1, params)
        if p_item_type == "plan":
            try:
                # Convert quoted string to dictionary.
                plan = ast.literal_eval(p_item[0])
            except Exception:
                raise CommandParameterError(f"Error occurred while parsing the plan '{p_item[0]}'")
            addr_param.update({"plan": plan})
        elif p_item_type == "instruction":
            if p_item[0] == "queue-stop":
                instruction = {"action": "queue_stop"}
            else:
                raise CommandParameterError(f"Unsupported instruction type: {p_item[0]}")
            addr_param.update({"instruction": instruction})
        else:
            # This indicates a bug in the program.
            raise ValueError(f"Unknown item type: {p_item_type}")
    except IndexError:
        raise CommandParameterError(f"The command '{params}' contains insufficient number of parameters")

    method = f"{command}_item_{params[0]}"
    prms = addr_param
    prms["user"] = default_user
    prms["user_group"] = default_user_group

    return method, prms


def msg_queue_item(params):
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

    # Make sure that there are sufficient number of parameters to start processing
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


def create_msg(params):
    """
    Create outgoing message based on command-line arguments.

    Parameters
    ----------
    params : list
        List of parameters (positional arguments) supplied via command line.

    Returns
    -------
    method : str
        The name of the method.
    prms : dict
        The dictionary of method parameters.
    monitoring_mode : bool
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

    # ----------- environment ------------
    elif command == "environment":
        if len(params) != 1:
            raise CommandParameterError(f"Request '{command}' must include only one parameter")
        supported_params = ("open", "close", "destroy")
        if params[0] in supported_params:
            method = f"{command}_{params[0]}"
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

    # ----------- queue ------------
    elif command == "queue":
        if len(params) < 1:
            raise CommandParameterError(f"Request '{command}' must include at least one parameter")
        supported_params = ("add", "get", "clear", "item", "start", "stop")
        if params[0] in supported_params:
            if params[0] == "add":
                method, prms = msg_queue_add(params)

            elif params[0] in ("get", "clear", "start"):
                if len(params) != 1:
                    raise CommandParameterError(f"Request '{command} {params[0]}' must include only one parameter")
                method, prms = f"{command}_{params[0]}", {}

            elif params[0] == "stop":
                method, prms = msg_queue_stop(params)

            elif params[0] == "item":
                method, prms = msg_queue_item(params)

        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    elif command == "re":
        if len(params) < 1:
            raise CommandParameterError(f"Request '{command}' must include at least one parameter")
        supported_params = ("pause", "resume", "stop", "abort", "halt")
        if params[0] in supported_params:
            if params[0] == "pause":
                method, prms = msg_re_pause(params)
            else:
                method, prms = f"{command}_{params[0]}", {}
        else:
            raise CommandParameterError(f"Request '{command} {params[0]}' is not supported")

    elif command == "history":
        if len(params) < 1:
            raise CommandParameterError(f"Request '{command}' must include at least one parameter")
        supported_params = ("get", "clear")
        if params[0] in supported_params:
            method, prms = f"{command}_{params[0]}", {}
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

    else:
        raise CommandParameterError(f"Unrecognized command '{command}'")

    return method, prms, monitoring_mode


def qserver():

    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("bluesky_queueserver").setLevel("ERROR")

    parser = argparse.ArgumentParser(
        description="Command-line tool for communicating with RE Monitor.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Bluesky-QServer version {qserver_version}.\n{cli_examples}",
    )

    parser.add_argument(
        "command",
        metavar="command",
        type=str,
        nargs="+",
        help="a sequence of keywords and parameters that define the command",
    )

    parser.add_argument(
        "--address",
        "-a",
        dest="address",
        action="store",
        default=None,
        help="Address of the server (e.g. 'tcp://localhost:5555', quoted string)",
    )

    args = parser.parse_args()
    print(f"Arguments: {args.command}")

    try:
        method, params, monitoring_mode = create_msg(args.command)

        if monitoring_mode:
            print("Running QSever monitor. Press Ctrl-C to exit ...")

        while True:
            msg, msg_err = zmq_single_request(method, params, zmq_server_address=args.address)

            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")

            if not msg_err:
                print(f"{current_time} - MESSAGE: {pprint.pformat(msg)}")
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
        logger.error("Invalid command or parameters: %s.", str(ex))
        exit_code = QServerExitCodes.PARAMETER_ERROR
    except Exception as ex:
        logger.exception("Exception occurred: %s.", str(ex))
        exit_code = QServerExitCodes.EXCEPTION_OCCURRED
    except KeyboardInterrupt:
        print("\nThe program was manually stopped.")
        exit_code = QServerExitCodes.SUCCESS

    return exit_code.value
