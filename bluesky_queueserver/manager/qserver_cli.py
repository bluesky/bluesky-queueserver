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
NOTE: Position indexes are 0-based. Inserting a plan to position 0 pushes it to the font of the queue.
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

# The following requests are forwarded to the Run Engine
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


# def get_supported_commands():
#     """
#     Get the dictionary that maps command names supported by the cli tool to
#     the command names in RE Manager API.
#
#     Returns
#     -------
#     dict(str, str)
#         Dictionary that maps supported commands to commands from RE Manager API.
#     """
#     command_dict = {
#         "ping": "",
#         "status": "status",
#         "plans_allowed": "plans_allowed",
#         "devices_allowed": "devices_allowed",
#         "history_get": "history_get",
#         "history_clear": "history_clear",
#         "environment_open": "environment_open",
#         "environment_close": "environment_close",
#         "environment_destroy": "environment_destroy",
#         "queue_get": "queue_get",
#         "queue_item_add": "queue_item_add",
#         "queue_item_get": "queue_item_get",
#         "queue_item_remove": "queue_item_remove",
#         "queue_item_move": "queue_item_move",
#         "queue_clear": "queue_clear",
#         "queue_start": "queue_start",
#         "queue_stop": "queue_stop",
#         "queue_stop_cancel": "queue_stop_cancel",
#         "re_pause": "re_pause",
#         "re_resume": "re_resume",
#         "re_stop": "re_stop",
#         "re_abort": "re_abort",
#         "re_halt": "re_halt",
#         "manager_stop": "manager_stop",
#         "manager_kill": "manager_kill",
#     }
#     return command_dict
#
#
# def _create_msg2(command, params=None):  ## Delete later
#     # This function may transform human-friendly command names to API names
#     params = params or []
#
#     def _pos_or_uid(v):
#         if isinstance(v, int) or v in ("front", "back"):
#             prms = {"pos": v}
#         elif isinstance(v, str):
#             prms = {"uid": v}
#         else:
#             raise ValueError(f"Parameter must be an integer or a string: give {v} ({type(v)})")
#         return prms
#
#     command_dict = get_supported_commands()
#     try:
#         command = command_dict[command]
#         # Present value in the proper format. This will change as the format is changed.
#         if command == "queue_item_add":
#             if (len(params) == 1) and isinstance(params[0], dict):
#                 # Arguments: <plan>
#                 prms = {"plan": params[0]}  # Value is dict
#             elif len(params) == 2:
#                 # The order of arguments: <pos> <plan>
#                 if isinstance(params[0], (int, str)) and isinstance(params[1], dict):
#                     prms = {"pos": params[0], "plan": params[1]}
#                 else:
#                     raise ValueError(f"Invalid set of method arguments: '{pprint.pformat(params)}'")
#             elif len(params) == 3:
#                 # The order of arguments: [before_uid, after_uid], <uid>, <plan>
#                 kwds = {"before_uid", "after_uid"}
#                 if (params[0] in kwds) and isinstance(params[1], str) and isinstance(params[2], dict):
#                     prms = {params[0]: params[1], "plan": params[2]}
#                 else:
#                     raise ValueError(
#                         f"Invalid set of method arguments: '{pprint.pformat(params)}'",
#                     )
#             else:
#                 raise ValueError(f"Invalid number of method arguments: '{pprint.pformat(params)}'")
#             prms["user"] = "qserver-cli"
#             prms["user_group"] = "root"
#
#         elif command in ("queue_item_remove", "queue_item_get"):
#             if len(params) == 0:
#                 prms = {}
#             elif len(params) == 1:
#                 prms = _pos_or_uid(params[0])
#             else:
#                 raise ValueError(f"Invalid number of method arguments: '{pprint.pformat(params)}'")
#
#         elif command == "queue_item_move":
#             if len(params) == 2:
#                 # Argument order: [<pos_source>, <uid_source>] <pos_dest>
#                 prms = _pos_or_uid(params[0])
#                 prms.update({"pos_dest": params[1]})
#
#             elif len(params) == 3:
#                 # Argument order: [<pos_source>, <uid_source>] ["before", "after"] <uid_dest>
#                 prms = _pos_or_uid(params[0])
#                 if params[1] in ("before", "after") and isinstance(params[2], str):
#                     if params[1] == "before":
#                         prms.update({"before_uid": params[2]})
#                     else:
#                         prms.update({"after_uid": params[2]})
#                 else:
#                     raise ValueError(
#                         f"Invalid set of method arguments: '{pprint.pformat(params)}'",
#                     )
#             else:
#                 raise ValueError(f"Invalid number of method arguments: '{pprint.pformat(params)}'")
#
#         elif command in ("plans_allowed", "devices_allowed"):
#             prms = {"user_group": "root"}
#
#         else:
#             if 0 <= len(params) <= 1:
#                 prms = {"option": params[0]} if len(params) else {}  # Value is str
#             else:
#                 raise ValueError(f"Invalid number of method arguments: '{pprint.pformat(params)}'")
#
#         return command, prms
#
#     except KeyError:
#         raise ValueError(f"Command '{command}' is not supported.")


def extract_source_address(params):
    """
    The function will raise 'IndexError' if there is insufficient number of parameters
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
    The function will raise 'IndexError' if there is insufficient number of parameters
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
    return " ".join([str(_) for _ in params])


def raise_request_not_supported(params):
    s = format_list_as_command(params)
    raise CommandParameterError(f"Request '{s}' is not supported")


def check_number_of_parameters(params, n_min, n_max, params_report=None):
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
    Generate outgoing message for `queue add` command.

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

    """
    command = "queue"
    expected_p0 = "add"
    if params[0] != expected_p0:
        raise ValueError(f"Incorrect parameter value '{params[0]}'. Expected value: '{expected_p0}'")
    if len(params) < 3:
        raise CommandParameterError(f"Item type and value are not specified '{command} {params[0]}'")
    p_item_type = params[1]
    if p_item_type not in ("plan", "instruction"):
        raise_request_not_supported([command, params[0], params[1]])
    try:
        addr_param, p_item = extract_destination_address(params[2:])
        if p_item_type == "plan":
            check_number_of_parameters(p_item, 1, 1, params)
            try:
                plan = ast.literal_eval(p_item[0])
            except Exception:
                raise CommandParameterError(f"Error occurred while parsing the plan '{p_item[0]}'")
            addr_param.update({"plan": plan})
        elif p_item_type == "instruction":
            if p_item[0] == "queue-stop":
                check_number_of_parameters(p_item, 1, 1, params)
                instruction = {"action": "queue_stop"}
            else:
                raise CommandParameterError(f"Unsupported instruction type: {p_item[0]}")
            addr_param.update({"instruction": instruction})
        else:
            # This indicates a bug in the program. It should not occur in normal operation.
            raise ValueError(f"Unknown item type: {p_item_type}")
    except IndexError:
        raise CommandParameterError(f"The command '{params}' contain insufficient number of parameters")

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
    command = "queue"
    expected_p0 = "item"
    if params[0] != expected_p0:
        raise ValueError(f"Incorrect parameter value '{params[0]}'. Expected value: '{expected_p0}'")
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
                    f"Source address could not be found: '{format_list_as_command(params)}'"
                )
            if not addr_param_dest:
                raise CommandParameterError(
                    f"Destination address could not be found: '{format_list_as_command(params)}'"
                )

            # Change the key from 'pos' to 'pos_dest'
            pos_dest = addr_param_dest.pop("pos", None)
            if pos_dest is not None:
                addr_param_dest["pos_dest"] = pos_dest

            addr_param = addr_param_src
            addr_param.update(addr_param_dest)

        else:
            # This indicates a bug in the program. It should not occur in normal operation.
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

    # Some string parameters may represent dictionaries. We need to convert it into dictionaries.
    # for key in prms.keys():
    #     if prms[key] is not None:
    #         try:
    #             prms[key] = ast.literal_eval(prms[key])
    #         except Exception as ex:
    #             # Failures to parse are OK (sometimes expected) unless the parameter is a dictionary.
    #             # TODO: probably it's a good idea to check if it is a list. (Currently no parameters
    #             #     accept lists.)
    #             if ("{" in prms[key]) or ("}" in prms[key]):
    #                 raise CommandParameterError(
    #                     f"Failed to parse parameter string {prms[key]}: {str(ex)}. "
    #                     f"The parameters must represent a valid Python dictionary"
    #                 )

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

    # command, params = args.command, args.params
    # params = params or []
    #
    # if command not in supported_commands:
    #     print(
    #         f"Command '{command}' is not supported. Please enter a valid command.\n"
    #         f"Call 'qserver' with the option '-h' to see full list of supported commands."
    #     )
    #     sys.exit(1)
    #
    # # 'params' is a string representing a python dictionary. We need to convert it into a dictionary.
    # #   Also don't evaluate the expression that is a non-quoted string with alphanumeric characters.
    # for n in range(len(params)):
    #     if params[n] is not None:
    #         try:
    #             params[n] = ast.literal_eval(params[n])
    #         except Exception as ex:
    #             # Failures to parse are OK (sometimes expected) unless the parameter is a dictionary.
    #             # TODO: probably it's a good idea to check if it is a list. (Currently no parameters
    #             #     accept lists.)
    #             if ("{" in params[n]) or ("}" in params[n]):
    #                 print(
    #                     f"Failed to parse parameter string {params[n]}: {str(ex)}. "
    #                     f"The parameters must represent a valid Python dictionary"
    #                 )
    #                 sys.exit(1)
    #
    # # 'ping' command will be sent to RE Manager periodically if 'monitor' command is entered
    # monitor_on = command == "monitor"
    # if monitor_on:
    #     command = "ping"
    #     print("Running QSever monitor. Press Ctrl-C to exit ...")
    #
    # try:
    #     while True:
    #         method, params_out = create_msg(command, params)
    #         msg, msg_err = zmq_single_request(method, params_out, zmq_server_address=args.address)
    #
    #         now = datetime.now()
    #         current_time = now.strftime("%H:%M:%S")
    #
    #         if not msg_err:
    #             print(f"{current_time} - MESSAGE: {pprint.pformat(msg)}")
    #             if isinstance(msg, dict) and ("success" in msg) and (msg["success"] is False):
    #                 exit_code = 2
    #             else:
    #                 exit_code = None
    #         else:
    #             print(f"{current_time} - ERROR: {msg_err}")
    #             exit_code = 3
    #
    #         if not monitor_on:
    #             break
    #         ttime.sleep(1)
    # except Exception as ex:
    #     logger.error("Exception occurred: %s.", str(ex))
    #     exit_code = 4
    # except KeyboardInterrupt:
    #     print("\nThe program was manually stopped.")
    #     exit_code = None

    return exit_code.value
