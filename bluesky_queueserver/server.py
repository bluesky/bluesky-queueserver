from aiohttp import web
from multiprocessing import Process, Pipe
import threading
import time as ttime
import asyncio

from .worker import RunEngineWorker

import logging
logger = logging.getLogger(__name__)

#  Plans that can be used to test the server
#  http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]]}'
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
        return self._loop

    async def _hello(self, request):
        return web.Response(text=f"Hello, world. "
                                 f"There are {len(self._queue_plans)} plans enqueed")

    async def _queue_view_handler(self, request):
        print(f"Queue_view called")
        out = {"queue": self._queue_plans}
        return web.json_response(out)

    async def _add_to_queue_handler(self, request):
        data = await request.json()
        # TODO: validate inputs!
        plan = data["plan"]
        location = data.get("location", len(self._queue_plans))
        self._queue_plans.insert(location, plan)
        return web.json_response(data)

    async def _pop_from_queue_handler(self, request):
        """Pop the last item from the queue"""
        if self._queue_plans:
            plan = self._queue_plans.pop()  # Pops from the back of the queue
            return web.json_response(plan)
        else:
            return web.json_response({})  # No items

    def _start_conn_pipes(self):
        self._server_conn, self._worker_conn = Pipe()

    def _start_conn_thread(self):
        self._thread_conn = threading.Thread(target=self._receive_packet_thread,
                                             name="RE Server Comm",
                                             daemon=True)
        self._thread_conn.start()

    def _start_re_worker(self):
        self._re_worker = RunEngineWorker(conn=self._worker_conn)
        self._re_worker.start()

    def _stop_re_worker(self):
        msg = {"type": "command", "value": "quit"}
        self._server_conn.send(msg)
        self._re_worker.join()

    def _run_task(self):
        if self._queue_plans:
            logger.info(f"Starting new plan: {len(self._queue_plans)} plans are left in the queue")
            new_plan = self._queue_plans.pop(0)

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

    def _receive_packet_thread(self):
        while True:
            ttime.sleep(0.1)
            if self._server_conn.poll():
                try:
                    msg = self._server_conn.recv()
                    self._loop.call_soon_threadsafe(self._conn_received, msg)
                except Exception as ex:
                    logger.error(f"Server: Exception occurred while waiting for packet: {ex}")
                    break

    def _conn_received(self, msg):
        type, value = msg["type"], msg["value"]
        if type == "report":
            uids = value["uids"]
            logger.info(f"Successfully finished execution of the plan (run UIDs: {uids})")
            self._run_task()

    async def _start_environment(self, request):
        if not self._environment_exists:
            self._start_re_worker()
            self._environment_exists = True
            success, msg = True, ""
        else:
            success, msg = False, "Environment already exists."
        return web.json_response({"success": success, "msg": msg})

    async def _close_environment(self, request):
        if self._environment_exists:
            self._stop_re_worker()
            self._environment_exists = False
            success, msg = True, ""
        else:
            success, msg = False, "Environment does not exist."
        return web.json_response({"success": success, "msg": msg})

    async def _process_queue(self, request):
        if self._environment_exists:
            self._run_task()
            success, msg = True, ""
        else:
            success, msg = False, "Environment does not exist. Can not start the task."
        return web.json_response({"success": success, "msg": msg})

    def setup_routes(self, app):
        app.add_routes(
            [
                web.get("/", self._hello),
                web.get("/queue_view", self._queue_view_handler),
                web.post("/add_to_queue", self._add_to_queue_handler),
                web.post("/pop_from_queue", self._pop_from_queue_handler),
                web.post("/start_environment", self._start_environment),
                web.post("/close_environment", self._close_environment),
                web.post("/process_queue", self._process_queue),
            ]
        )


def init_func(argv):
    re_server = RunEngineServer()

    app = web.Application(loop=re_server.get_loop())
    re_server.setup_routes(app)
    app["re_server"] = re_server  # To keep it alive
    return app
