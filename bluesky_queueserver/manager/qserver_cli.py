import asyncio

import ast
import time as ttime
from datetime import datetime
import pprint
import re
import sys
import zmq
import zmq.asyncio
import argparse

import bluesky_queueserver

import logging
logger = logging.getLogger(__name__)

qserver_version = bluesky_queueserver.__version__

"""
#  The following plans that can be used to test the server

http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]]}'

# This is the slowly running plan (convenient to test pausing)
http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]],
"kwargs":{"num":10, "delay":1}}'

http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"scan", "args":[["det1", "det2"], "motor", -1, 1, 10]}'
"""


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
            "queue_view": "queue_view",
            "add_to_queue": "add_to_queue",
            "pop_from_queue": "pop_from_queue",
            "clear_queue": "clear_queue",
            "create_environment": "create_environment",
            "close_environment": "close_environment",
            "process_queue": "process_queue",
            "re_pause": "re_pause",
            "re_continue": "re_continue",
            "print_db_uids": "print_db_uids",
            "stop_manager": "stop_manager",
            "kill_manager": "kill_manager",
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
            self._msg_in = await self._send_command(command=self._msg_command_out,
                                                    params=self._msg_params_out)
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
        command_dict = self.get_supported_commands()
        try:
            command = command_dict[command]
            # Present value in the proper format. This will change as the format is changed.
            if command == "add_to_queue":
                params = {"plan": params}  # Value is dict
            else:
                params = {"option": params}  # Value is str
            return {"method": command, "params": params}
        except KeyError:
            raise ValueError(f"Command '{command}' is not supported.")

    def set_msg_out(self, command, params):
        self._msg_command_out = command
        self._msg_params_out = params

    def get_msg_in(self):
        return self._msg_in, self._msg_err_in


def qserver():

    logging.basicConfig(level=logging.WARNING)
    logging.getLogger('bluesky_queueserver').setLevel("CRITICAL")

    supported_commands = list(CliClient.get_supported_commands().keys())
    # Add the command 'monitor' to the list. This command is not sent to RE Manager.
    supported_commands = ["monitor"] + supported_commands

    parser = argparse.ArgumentParser(description='Command-line tool for communicating with RE Monitor.',
                                     epilog=f'Bluesky-QServer version {qserver_version}.')
    parser.add_argument('--command', '-c', dest="command", action='store', required=True,
                        help=f"Command sent to the server. Supported commands: {supported_commands}.")
    parser.add_argument('--parameters', '-p', dest="params", action='store', default=None,
                        help="Parameters that are sent with the command. Currently the parameters "
                             "must be represented as a string that contains a python dictionary.")
    parser.add_argument('--address', '-a', dest="address", action='store', default=None,
                        help="Address of the server (e.g. 'tcp://localhost:5555', quoted string)")

    args = parser.parse_args()

    command, params = args.command, args.params

    if command not in supported_commands:
        print(f"Command '{command}' is not supported. Please enter a valid command.\n"
              f"Call 'qserver' with the option '-h' to see full list of supported commands.")
        sys.exit(1)

    # 'params' is a string representing a python dictionary. We need to convert it into a dictionary.
    #   Also don't evaluate the expression that is a non-quoted string with alphanumeric characters.
    if (params is not None) and not re.search(r"^\w+$", params):
        try:
            params = ast.literal_eval(params)
        except Exception as ex:
            print(f"Failed to parse parameter string {params}: {str(ex)}. "
                  f"The parameters must represent a valid Python dictionary")
            sys.exit(1)

    # 'ping' command will be sent to RE Manager periodically if 'monitor' command is entered
    monitor_on = (command == "monitor")
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
            else:
                print(f"{current_time} - ERROR: {msg_err}")

            if not monitor_on:
                break
            ttime.sleep(1)
    except KeyboardInterrupt:
        print("\nThe program was manually stopped.")
