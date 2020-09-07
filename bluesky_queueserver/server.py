from aiohttp import web
from multiprocessing import Pipe
import threading
import asyncio

from .worker import RunEngineWorker

import logging
logger = logging.getLogger(__name__)

#  Plans that can be used to test the server
#  http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]]}'
#  http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]], "kwargs":{"num":10, "delay":1}}'  # noqa: 501
#  http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"scan", "args":[["det1", "det2"], "motor", -1, 1, 10]}'


class RunEngineServer:

    def __init__(self):
        self._re_worker = None
        self._server_conn = None
        self._worker_conn = None

        self._environment_exists = False

        self._thread_conn = None

        self._queue_plans = []

        self._loop = asyncio.get_event_loop()

        self._start_conn_pipes()
        self._start_conn_thread()

    def get_loop(self):
        """
        Returns the asyncio loop.
        """
        return self._loop

    def _start_conn_pipes(self):
        self._server_conn, self._worker_conn = Pipe()

    def _start_conn_thread(self):
        self._thread_conn = threading.Thread(target=self._receive_packet_thread,
                                             name="RE Server Comm",
                                             daemon=True)
        self._thread_conn.start()

    # ======================================================================
    #   Functions that implement functionality of the server

    def _start_re_worker(self):
        """
        Creates worker process.
        """
        self._re_worker = RunEngineWorker(conn=self._worker_conn)
        self._re_worker.start()

    def _stop_re_worker(self):
        """
        Stops and destroys the worker process. The process is not simply killed,
        but instead let to exit gracefully. Separate function could be added that could
        kill the process or it could be killed after significant timeout (process is not
        responding).
        """
        msg = {"type": "command", "value": "quit"}
        self._server_conn.send(msg)
        self._re_worker.join()

    def _run_task(self):
        """
        Upload the plan to the worker process for execution.
        Plan in the queue is represented as a dictionary with the keys "name" (plan name),
        "args" (list of args), "kwargs" (list of kwargs). Only the plan name is mandatory.
        Names of plans and devices are strings.
        """
        if self._queue_plans:
            logger.info(f"Starting new plan: {len(self._queue_plans)} plans are left in the queue")
            new_plan = self._queue_plans[0]

            plan_name = new_plan["name"]
            args = new_plan["args"] if "args" in new_plan else []
            kwargs = new_plan["kwargs"] if "kwargs" in new_plan else {}

            msg = {"type": "plan",
                   "value": {"name": plan_name,
                             "args": args,
                             "kwargs": kwargs
                             }
                   }

            self._server_conn.send(msg)
            return True
        else:
            logger.info("Queue is empty")
            return False

    def _pause_run_engine(self, option):
        msg = {"type": "command", "value": "pause", "option": option}
        self._server_conn.send(msg)

    def _continue_run_engine(self, option):
        msg = {"type": "command", "value": "continue", "option": option}
        self._server_conn.send(msg)

    # =======================================================================
    #   Functions for communication with the worker process

    def _receive_packet_thread(self):
        while True:
            if self._server_conn.poll(0.1):
                try:
                    msg = self._server_conn.recv()
                    self._loop.call_soon_threadsafe(self._conn_received, msg)
                except Exception as ex:
                    logger.error(f"Server: Exception occurred while waiting for packet: {ex}")
                    break

    def _conn_received(self, msg):
        type, value = msg["type"], msg["value"]

        if type == "report":
            completed = value["completed"]
            success = value["success"]
            result = value["result"]
            logger.info(f"Report received from RE Worker:\nsuccess={success}\n{result}\n)")
            if completed and success:
                # Executed plan is removed from the queue only after it is successfully completed
                self._queue_plans.pop(0)
                self._run_task()

        if type == "acknowledge":
            status = value["status"]
            result = value["result"]
            msg_original = value["msg"]
            logger.info("Acknownegement received from RE Worker:\n"
                        f"Status: '{status}'\nResult: '{result}'\nMessage: {msg_original}")

    # =========================================================================
    #    REST API handlers

    async def _hello_handler(self, request):
        """May be called to get some response from the server"""
        return web.Response(text=f"Hello, world. "
                                 f"There are {len(self._queue_plans)} plans enqueed")

    async def _queue_view_handler(self, request):
        """
        Returns the contents of the current queue
        """
        out = {"queue": self._queue_plans}
        return web.json_response(out)

    async def _add_to_queue_handler(self, request):
        """
        Adds new plan to the end of the queue
        """
        data = await request.json()
        # TODO: validate inputs!
        plan = data["plan"]
        location = data.get("location", len(self._queue_plans))
        self._queue_plans.insert(location, plan)
        return web.json_response(data)

    async def _pop_from_queue_handler(self, request):
        """
        Pop the last item from back of the queue
        """
        if self._queue_plans:
            plan = self._queue_plans.pop()  # Pops from the back of the queue
            return web.json_response(plan)
        else:
            return web.json_response({})  # No items

    async def _create_environment_handler(self, request):
        """
        Creates RE environment: creates RE Worker process, starts and configures Run Engine.
        """
        if not self._environment_exists:
            self._start_re_worker()
            self._environment_exists = True
            success, msg = True, ""
        else:
            success, msg = False, "Environment already exists."
        return web.json_response({"success": success, "msg": msg})

    async def _close_environment_handler(self, request):
        """
        Deletes RE environment. In the current 'demo' prototype the environment will be deleted
        only after RE completes the current scan.
        """
        if self._environment_exists:
            self._stop_re_worker()
            self._environment_exists = False
            success, msg = True, ""
        else:
            success, msg = False, "Environment does not exist."
        return web.json_response({"success": success, "msg": msg})

    async def _process_queue_handler(self, request):
        """
        Start execution of the loaded queue. Additional runs can be added to the queue while
        it is executed. If the queue is empty, then nothing will happen.
        """
        if self._environment_exists:
            self._run_task()
            success, msg = True, ""
        else:
            success, msg = False, "Environment does not exist. Can not start the task."
        return web.json_response({"success": success, "msg": msg})

    async def _re_pause_handler(self, request):
        """
        Pause Run Engine
        """
        data = await request.json()
        option = data["option"]
        available_options = ("deferred", "immediate")
        if option in available_options:
            if self._environment_exists:
                self._pause_run_engine(option)
                success, msg = True, ""
            else:
                success, msg = False, "Environment does not exist. Can not pause Run Engine."
        else:
            success, msg = False, f"Option '{option}' is not supported. " \
                                  f"Available options: {available_options}"
        return web.json_response({"success": success, "msg": msg})

    async def _re_continue_handler(self, request):
        """
        Control Run Engine in the paused state
        """
        data = await request.json()
        option = data["option"]
        available_options = ("resume", "abort", "stop", "halt")
        if option in available_options:
            if self._environment_exists:
                self._continue_run_engine(option)
                success, msg = True, ""
            else:
                success, msg = False, "Environment does not exist. Can not pause Run Engine."
        else:
            success, msg = False, f"Option '{option}' is not supported. " \
                                  f"Available options: {available_options}"
        return web.json_response({"success": success, "msg": msg})

    def setup_routes(self, app):
        """
        Setup routes to handler for web.Application
        """
        app.add_routes(
            [
                web.get("/", self._hello_handler),
                web.get("/queue_view", self._queue_view_handler),
                web.post("/add_to_queue", self._add_to_queue_handler),
                web.post("/pop_from_queue", self._pop_from_queue_handler),
                web.post("/create_environment", self._create_environment_handler),
                web.post("/close_environment", self._close_environment_handler),
                web.post("/process_queue", self._process_queue_handler),
                web.post("/re_continue", self._re_continue_handler),
                web.post("/re_pause", self._re_pause_handler),
            ]
        )


def init_func(argv):
    re_server = RunEngineServer()

    app = web.Application(loop=re_server.get_loop())
    re_server.setup_routes(app)
    app["re_server"] = re_server  # To keep it alive
    return app
