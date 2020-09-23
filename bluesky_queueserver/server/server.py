# from aiohttp import web
import asyncio
import logging
from enum import Enum

import zmq
import zmq.asyncio
from fastapi import FastAPI, HTTPException

logger = logging.getLogger(__name__)

"""
#  The following plans that can be used to test the server

http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]]}'

# This is the slowly running plan (convenient to test pausing)
http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]],
"kwargs":{"num":10, "delay":1}}'

http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"scan", "args":[["det1", "det2"], "motor", -1, 1, 10]}'
"""


class ZMQ_Comm:
    def __init__(self, zmq_host='localhost', zmq_port='5555'):
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

    def _create_msg(self, *, command, value=None):
        return {"command": command, "value": value}

    async def _send_command(self, *, command, value=None):
        msg_out = self._create_msg(command=command, value=value)
        msg_in = await self._zmq_communicate(msg_out)
        return msg_in

    # =========================================================================
    #    REST API handlers

    # async def _hello_handler(self):
    #     """
    #     May be called to get response from the server. Returns the number of plans in the queue.
    #     """
    #     msg = await self._send_command(command="")
    #     return web.json_response(msg)

    # async def _queue_view_handler(self):
    #     """
    #     Returns the contents of the current queue.
    #     """
    #     msg = await self._send_command(command="queue_view")
    #     return web.json_response(msg)

    # async def _add_to_queue_handler(self, request):
    #     """
    #     Adds new plan to the end of the queue
    #     """
    #     data = await request.json()
    #     # TODO: validate inputs!
    #     msg = await self._send_command(command="add_to_queue", value=data)
    #     return web.json_response(msg)

    # async def _pop_from_queue_handler(self, request):
    #     """
    #     Pop the last item from back of the queue
    #     """
    #     msg = await self._send_command(command="pop_from_queue")
    #     return web.json_response(msg)

    # async def _create_environment_handler(self, request):
    #     """
    #     Creates RE environment: creates RE Worker process, starts and configures Run Engine.
    #     """
    #     msg = await self._send_command(command="create_environment")
    #     return web.json_response(msg)

    # async def _close_environment_handler(self, request):
    #     """
    #     Deletes RE environment. In the current 'demo' prototype the environment will be deleted
    #     only after RE completes the current scan.
    #     """
    #     msg = await self._send_command(command="close_environment")
    #     return web.json_response(msg)

    # async def _process_queue_handler(self, request):
    #     """
    #     Start execution of the loaded queue. Additional runs can be added to the queue while
    #     it is executed. If the queue is empty, then nothing will happen.
    #     """
    #     msg = await self._send_command(command="process_queue")
    #     return web.json_response(msg)

    # async def _re_pause_handler(self, request):
    #     """
    #     Pause Run Engine
    #     """
    #     data = await request.json()
    #     msg = await self._send_command(command="re_pause", value=data)
    #     return web.json_response(msg)

    # async def _re_continue_handler(self, request):
    #     """
    #     Control Run Engine in the paused state
    #     """
    #     data = await request.json()
    #     msg = await self._send_command(command="re_continue", value=data)
    #     return web.json_response(msg)

    # async def _print_db_uids_handler(self, request):
    #     """
    #     Prints the UIDs of the scans in 'temp' database. Just for the demo.
    #     Not part of future API.
    #     """
    #     msg = await self._send_command(command="print_db_uids")
    #     return web.json_response(msg)

    # def setup_routes(self, app):
    #     """
    #     Setup routes to handler for web.Application
    #     """
    #     app.add_routes(
    #         [
    #             # web.get("/", self._hello_handler),
    #             # web.get("/queue_view", self._queue_view_handler),
    #             # web.post("/add_to_queue", self._add_to_queue_handler),
    #             # web.post("/pop_from_queue", self._pop_from_queue_handler),
    #             # web.post("/create_environment", self._create_environment_handler),
    #             # web.post("/close_environment", self._close_environment_handler),
    #             # web.post("/process_queue", self._process_queue_handler),
    #             # web.post("/re_continue", self._re_continue_handler),
    #             # web.post("/re_pause", self._re_pause_handler),
    #             # web.post("/print_db_uids", self._print_db_uids_handler),
    #         ]
    #     )


logging.basicConfig(level=logging.WARNING)
logging.getLogger('bluesky_queueserver').setLevel("DEBUG")

# Use FastAPI
app = FastAPI()
re_server = ZMQ_Comm()


class REPauseOptions(str, Enum):
    deferred = 'deferred'
    immediate = 'immediate'


class REResumeOptions(str, Enum):
    resume = 'resume'
    abort = 'abort'
    stop = 'stop'
    halt = 'halt'


@app.get('/')
async def _hello_handler():
    """
    May be called to get response from the server. Returns the number of plans in the queue.
    """
    msg = await re_server._send_command(command="")
    return msg


@app.get('/queue_view')
async def _queue_view_handler():
    """
    Returns the contents of the current queue.
    """
    msg = await re_server._send_command(command="queue_view")
    return msg


@app.post('/add_to_queue')
async def _add_to_queue_handler(payload: dict):
    """
    Adds new plan to the end of the queue
    """
    # TODO: validate inputs!
    msg = await re_server._send_command(command="add_to_queue", value=payload)
    return msg


@app.post('/pop_from_queue')
async def _pop_from_queue_handler():
    """
    Pop the last item from back of the queue
    """
    msg = await re_server._send_command(command="pop_from_queue")
    return msg


@app.post('/create_environment')
async def _create_environment_handler():
    """
    Creates RE environment: creates RE Worker process, starts and configures Run Engine.
    """
    msg = await re_server._send_command(command="create_environment")
    return msg


@app.post('/close_environment')
async def _close_environment_handler():
    """
    Deletes RE environment. In the current 'demo' prototype the environment will be deleted
    only after RE completes the current scan.
    """
    msg = await re_server._send_command(command="close_environment")
    return msg


@app.post('/process_queue')
async def _process_queue_handler():
    """
    Start execution of the loaded queue. Additional runs can be added to the queue while
    it is executed. If the queue is empty, then nothing will happen.
    """
    msg = await re_server._send_command(command="process_queue")
    return msg


@app.post('/re_pause')
async def _re_pause_handler(payload: dict):
    """
    Pause Run Engine
    """
    if not hasattr(REResumeOptions, payload['option']):
        msg = f'The specified option "{payload["option"]}" is not allowed.'
        raise HTTPException(status_code=444, detail=msg)
    msg = await re_server._send_command(command="re_pause", value=payload)
    return msg


@app.post('/re_continue')
async def _re_continue_handler(payload: dict):
    """
    Control Run Engine in the paused state
    """
    if not hasattr(REResumeOptions, payload['option']):
        msg = f'The specified option "{payload["option"]}" is not allowed.'
        raise HTTPException(status_code=444, detail=msg)
    msg = await re_server._send_command(command="re_continue", value=payload)
    return msg


@app.post('/print_db_uids')
async def _print_db_uids_handler():
    """
    Prints the UIDs of the scans in 'temp' database. Just for the demo.
    Not part of future API.
    """
    msg = await re_server._send_command(command="print_db_uids")
    return msg
