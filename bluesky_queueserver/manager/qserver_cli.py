import asyncio

import ast
import time as ttime
from datetime import datetime
import pprint
import sys
import zmq
import zmq.asyncio
import argparse

import bluesky_queueserver

import logging

logger = logging.getLogger(__name__)

qserver_version = bluesky_queueserver.__version__


class CliClient:
    def __init__(self, *, address=None):
        # ZeroMQ communication
        self._ctx = zmq.asyncio.Context()
        self._zmq_socket = None

        if address is None:
            self._zmq_server_address = "tcp://localhost:5555"
        else:
            self._zmq_server_address = address

        # The attributes for storing output command and received input
        self._msg_command_out = ""
        self._msg_params_out = {}
        self._msg_in = {}
        self._msg_err_in = ""

    @staticmethod
    def get_supported_commands():
        """
        Get the dictionary that maps command names supported by the cli tool to
        the command names in RE Manager API.

        Returns
        -------
        dict(str, str)
            Dictionary that maps supported commands to commands from RE Manager API.
        """
        command_dict = {
            "ping": "",
            "status": "status",
            "plans_allowed": "plans_allowed",
            "devices_allowed": "devices_allowed",
            "history_get": "history_get",
            "history_clear": "history_clear",
            "environment_open": "environment_open",
            "environment_close": "environment_close",
            "environment_destroy": "environment_destroy",
            "queue_get": "queue_get",
            "queue_plan_add": "queue_plan_add",
            "queue_plan_get": "queue_plan_get",
            "queue_plan_remove": "queue_plan_remove",
            "queue_clear": "queue_clear",
            "queue_start": "queue_start",
            "queue_stop": "queue_stop",
            "queue_stop_cancel": "queue_stop_cancel",
            "re_pause": "re_pause",
            "re_resume": "re_resume",
            "re_stop": "re_stop",
            "re_abort": "re_abort",
            "re_halt": "re_halt",
            "manager_stop": "manager_stop",
            "manager_kill": "manager_kill",
        }
        return command_dict

    # ==========================================================================
    #    Functions that support ZeroMQ communications with RE Manager

    async def _zmq_send(self, msg):
        await self._zmq_socket.send_json(msg)

    async def _zmq_receive(self):
        return await self._zmq_socket.recv_json()

    async def _zmq_communicate(self, msg_out):
        try:
            await self._zmq_send(msg_out)
            msg_in = await self._zmq_receive()
            return msg_in
        except Exception as ex:
            raise RuntimeError(f"ZeroMQ communication failed: {str(ex)}")

    async def _zmq_open_connection(self, address):
        if self._zmq_socket.connect(address):
            msg_err = f"Failed to connect to the server '{address}'"
            raise RuntimeError(msg_err)

    async def zmq_single_request(self):
        self._zmq_socket = self._ctx.socket(zmq.REQ)
        self._zmq_socket.RCVTIMEO = 2000  # Timeout for 'recv' operation
        self._zmq_socket.SNDTIMEO = 500  # Timeout for 'send' operation
        # Clear the buffer quickly after the socket is closed
        self._zmq_socket.setsockopt(zmq.LINGER, 100)

        try:
            await self._zmq_open_connection(self._zmq_server_address)
            logger.info("Connected to ZeroMQ server '%s'", self._zmq_server_address)
            self._msg_in = await self._send_command(command=self._msg_command_out, params=self._msg_params_out)
            self._msg_err_in = ""
        except Exception as ex:
            self._msg_in = None
            self._msg_err_in = str(ex)
            # Close the socket to clear the buffer quickly in case of an error.
            self._zmq_socket.close()

        if self._msg_err_in:
            logger.warning("Communication with RE Manager failed: %s", str(self._msg_err_in))

    async def _send_command(self, *, command, params=None):
        msg_out = self._create_msg(command=command, params=params)
        msg_in = await self._zmq_communicate(msg_out)
        return msg_in

    def _create_msg(self, command, params=None):
        # This function may transform human-friendly command names to API names
        params = params or []

        command_dict = self.get_supported_commands()
        try:
            command = command_dict[command]
            # Present value in the proper format. This will change as the format is changed.
            if command == "queue_plan_add":
                if (len(params) == 1) and isinstance(params[0], dict):
                    prms = {"plan": params[0]}  # Value is dict
                elif len(params) == 2:
                    plan_found, pos_found = False, False
                    prms = {}
                    for n in range(2):
                        if isinstance(params[n], dict):
                            prms.update({"plan": params[n]})
                            plan_found = True
                        else:
                            prms.update({"pos": params[n]})
                            pos_found = True
                    if not plan_found or not pos_found:
                        raise ValueError("Invalid set of method arguments: '%s'", pprint.pformat(params))
                else:
                    raise ValueError("Invalid number of method arguments: '%s'", pprint.pformat(params))
                prms["user"] = "qserver-cli"
                prms["user_group"] = "root"

            elif command in ("queue_plan_remove", "queue_plan_get"):
                if 0 <= len(params) <= 1:
                    prms = {"pos": params[0]} if len(params) else {}
                else:
                    raise ValueError("Invalid number of method arguments: '%s'", pprint.pformat(params))

            elif command in ("plans_allowed", "devices_allowed"):
                prms = {"user_group": "root"}

            else:
                if 0 <= len(params) <= 1:
                    prms = {"option": params[0]} if len(params) else {}  # Value is str
                else:
                    raise ValueError("Invalid number of method arguments: '%s'", pprint.pformat(params))

            return {"method": command, "params": prms}
        except KeyError:
            raise ValueError(f"Command '{command}' is not supported.")

    def set_msg_out(self, command, params):
        self._msg_command_out = command
        self._msg_params_out = params

    def get_msg_in(self):
        return self._msg_in, self._msg_err_in


def qserver():

    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("bluesky_queueserver").setLevel("CRITICAL")

    supported_commands = list(CliClient.get_supported_commands().keys())
    # Add the command 'monitor' to the list. This command is not sent to RE Manager.
    supported_commands = ["monitor"] + supported_commands

    parser = argparse.ArgumentParser(
        description="Command-line tool for communicating with RE Monitor.",
        epilog=f"Bluesky-QServer version {qserver_version}.",
    )
    parser.add_argument(
        "--command",
        "-c",
        dest="command",
        action="store",
        required=True,
        help=f"Command sent to the server. Supported commands: {supported_commands}.",
    )
    parser.add_argument(
        "--parameters",
        "-p",
        nargs="*",
        dest="params",
        action="store",
        default=None,
        help="Parameters that are sent with the command. Currently the parameters "
        "must be represented as a string that contains a python dictionary.",
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

    command, params = args.command, args.params
    params = params or []

    if command not in supported_commands:
        print(
            f"Command '{command}' is not supported. Please enter a valid command.\n"
            f"Call 'qserver' with the option '-h' to see full list of supported commands."
        )
        sys.exit(1)

    # 'params' is a string representing a python dictionary. We need to convert it into a dictionary.
    #   Also don't evaluate the expression that is a non-quoted string with alphanumeric characters.
    for n in range(len(params)):
        if params[n] is not None:
            try:
                params[n] = ast.literal_eval(params[n])
            except Exception as ex:
                # Failures to parse are OK (sometimes expected) unless the parameter is a dictionary.
                # TODO: probably it's a good idea to check if it is a list. (List are not used
                #     as parameter values at this point.)
                if ("{" in params[n]) or ("}" in params[n]):
                    print(
                        f"Failed to parse parameter string {params[n]}: {str(ex)}. "
                        f"The parameters must represent a valid Python dictionary"
                    )
                    sys.exit(1)

    # 'ping' command will be sent to RE Manager periodically if 'monitor' command is entered
    monitor_on = command == "monitor"
    if monitor_on:
        command = "ping"
        print("Running QSever monitor. Press Ctrl-C to exit ...")

    re_server = CliClient(address=args.address)
    try:
        while True:
            re_server.set_msg_out(command, params)
            asyncio.run(re_server.zmq_single_request())
            msg, msg_err = re_server.get_msg_in()

            now = datetime.now()
            current_time = now.strftime("%H:%M:%S")

            if not msg_err:
                print(f"{current_time} - MESSAGE: {pprint.pformat(msg)}")
                if isinstance(msg, dict) and ("success" in msg) and (msg["success"] is False):
                    exit_code = 2
                else:
                    exit_code = None
            else:
                print(f"{current_time} - ERROR: {msg_err}")
                exit_code = 3

            if not monitor_on:
                break
            ttime.sleep(1)
    except Exception as ex:
        logger.exception("Exception occurred: %s.", str(ex))
        exit_code = 4
    except KeyboardInterrupt:
        print("\nThe program was manually stopped.")
        exit_code = None

    # Note: exit codes are arbitrarily selected. None translates to 0.
    return exit_code
