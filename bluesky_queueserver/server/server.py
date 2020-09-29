import asyncio
import logging
from enum import Enum

import zmq
import zmq.asyncio
from fastapi import FastAPI, HTTPException

logger = logging.getLogger(__name__)

"""
#  The following plans that can be used to test the server

http POST http://localhost:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]]}'

# This is the slowly running plan (convenient to test pausing)
http POST http://localhost:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]],
"kwargs":{"num":10, "delay":1}}'

http POST http://localhost:8080/add_to_queue plan:='{"name":"scan", "args":[["det1", "det2"], "motor", -1, 1, 10]}'
"""


class ZMQComm:
    def __init__(self, zmq_host="localhost", zmq_port="5555"):
        self._loop = asyncio.get_event_loop()

        # ZeroMQ communication
        self._ctx = zmq.asyncio.Context()
        self._zmq_socket = None
        self._zmq_server_address = f"tcp://{zmq_host}:{zmq_port}"

        # Start communication task
        self._event_zmq_stop = None
        self._task_zmq_client_conn = asyncio.ensure_future(self._zmq_start_client_conn())

    def __del__(self):
        # Cancel the communication task
        if not self._task_zmq_client_conn.done():
            self._task_zmq_client_conn.cancel()

    def get_loop(self):
        """
        Returns the asyncio loop.
        """
        return self._loop

    # ==========================================================================
    #    Functions that support ZeroMQ communications with RE Manager

    async def _zmq_send(self, msg):
        await self._zmq_socket.send_json(msg)

    async def _zmq_receive(self):
        try:
            msg = await self._zmq_socket.recv_json()
        except Exception as ex:
            # TODO: proper error handling for cases when connection is interrupted.
            # This is primitive error handling to make sure the server doesn't get stuck
            #   if there is no reply after timeout. The server still needs to be restarted
            #   if it was disconnected, since there is not mechanism to reestablish
            #   connection.
            logger.exception("ZeroMQ communication failed: %s" % str(ex))
            msg = {}
        return msg

    async def _zmq_communicate(self, msg_out):
        await self._zmq_send(msg_out)
        msg_in = await self._zmq_receive()
        return msg_in

    async def _zmq_start_client_conn(self):
        self._event_zmq_stop = asyncio.Event()
        self._zmq_socket = self._ctx.socket(zmq.REQ)
        self._zmq_socket.RCVTIMEO = 2000  # Timeout for 'recv' operation

        self._zmq_socket.connect(self._zmq_server_address)
        logger.info("Connected to ZeroMQ server '%s'" % str(self._zmq_server_address))

        # The event must be set somewhere else
        await self._event_zmq_stop.wait()

    def _create_msg(self, *, command, params=None):
        return {"method": command, "params": params}

    async def send_command(self, *, command, params=None):
        msg_out = self._create_msg(command=command, params=params)
        msg_in = await self._zmq_communicate(msg_out)
        return msg_in


logging.basicConfig(level=logging.WARNING)
logging.getLogger("bluesky_queueserver").setLevel("DEBUG")

# Use FastAPI
app = FastAPI()
re_server = ZMQComm()


class REPauseOptions(str, Enum):
    deferred = "deferred"
    immediate = "immediate"


class REResumeOptions(str, Enum):
    resume = "resume"
    abort = "abort"
    stop = "stop"
    halt = "halt"


@app.get("/")
async def _ping_handler():
    """
    May be called to get response from the server. Returns the number of plans in the queue.
    """
    msg = await re_server.send_command(command="")
    return msg


@app.get("/get_queue")
async def _get_queue_handler():
    """
    Returns the contents of the current queue.
    """
    msg = await re_server.send_command(command="get_queue")
    return msg


@app.get("/list_allowed_plans_and_devices")
async def _list_allowed_plans_and_devices_handler():
    """
    Returns the lists of allowed plans and devices.
    """
    msg = await re_server.send_command(command="list_allowed_plans_and_devices")
    return msg


@app.post("/add_to_queue")
async def _add_to_queue_handler(payload: dict):
    """
    Adds new plan to the end of the queue
    """
    # TODO: validate inputs!
    msg = await re_server.send_command(command="add_to_queue", params=payload)
    return msg


@app.post("/clear_queue")
async def _clear_queue_handler():
    """
    Adds new plan to the end of the queue
    """
    msg = await re_server.send_command(command="clear_queue")
    return msg


@app.post("/pop_from_queue")
async def _pop_from_queue_handler():
    """
    Pop the last item from back of the queue
    """
    msg = await re_server.send_command(command="pop_from_queue")
    return msg


@app.post("/create_environment")
async def _create_environment_handler():
    """
    Creates RE environment: creates RE Worker process, starts and configures Run Engine.
    """
    msg = await re_server.send_command(command="create_environment")
    return msg


@app.post("/close_environment")
async def _close_environment_handler():
    """
    Deletes RE environment. In the current 'demo' prototype the environment will be deleted
    only after RE completes the current scan.
    """
    msg = await re_server.send_command(command="close_environment")
    return msg


@app.post("/process_queue")
async def _process_queue_handler():
    """
    Start execution of the loaded queue. Additional runs can be added to the queue while
    it is executed. If the queue is empty, then nothing will happen.
    """
    msg = await re_server.send_command(command="process_queue")
    return msg


@app.post("/re_pause")
async def _re_pause_handler(payload: dict):
    """
    Pause Run Engine
    """
    if not hasattr(REPauseOptions, payload["option"]):
        msg = (
            f'The specified option "{payload["option"]}" is not allowed.\n'
            f"Allowed options: {list(REPauseOptions.__members__.keys())}"
        )
        raise HTTPException(status_code=444, detail=msg)
    msg = await re_server.send_command(command="re_pause", params=payload)
    return msg


@app.post("/re_continue")
async def _re_continue_handler(payload: dict):
    """
    Control Run Engine in the paused state
    """
    if not hasattr(REResumeOptions, payload["option"]):
        msg = (
            f'The specified option "{payload["option"]}" is not allowed.\n'
            f"Allowed options: {list(REResumeOptions.__members__.keys())}"
        )
        raise HTTPException(status_code=444, detail=msg)
    msg = await re_server.send_command(command="re_continue", params=payload)
    return msg


@app.post("/print_db_uids")
async def _print_db_uids_handler():
    """
    Prints the UIDs of the scans in 'temp' database. Just for the demo.
    Not part of future API.
    """
    msg = await re_server.send_command(command="print_db_uids")
    return msg
