from aiohttp import web
from multiprocessing import Process, Pipe
import threading
import time as ttime

#from ophyd.sim import det1, det2
#from bluesky.plans import count

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

        self._start_conn_pipes()
        self._start_conn_thread()

    async def hello(self, request):
        return web.Response(text=f"Hello, world. "
                                 f"There are {len(self._queue_plans)} plans enqueed")

    async def queue_view_handler(self, request):
        out = {"queue": self._queue_plans}
        return web.json_response(out)

    async def addto_queue_handler(self, request):
        data = await request.json()
        # TODO validate inputs!
        plan = data["plan"]
        location = data.get("location", len(self._queue_plans))
        self._queue_plans.insert(location, plan)
        return web.json_response(data)

    async def pop_from_queue_handler(self, request):
        if len(self._queue_plans):
            plan = self._queue_plans.pop()  # Pops from the back of the queue
            return web.json_response(plan)
        else:
            return web.json_response({"plan": "sleep", "args": [10]})

    def _start_conn_pipes(self):
        self._server_conn, self._worker_conn = Pipe()

    def _start_conn_thread(self):
        self._thread_conn = threading.Thread(target=self.receive_packet_thread,
                                             name="RE Server Comm",
                                             daemon=True)
        self._thread_conn.start()

    def start_re_worker(self):
        self._re_worker = RunEngineWorker(conn=self._worker_conn)
        self._re_worker.start()

    def stop_re_worker(self):
        msg = {"type": "command", "value": "quit"}
        self._server_conn.send(msg)
        self._re_worker.join()

    def run_task(self):
        if self._queue_plans:
            print(f"Starting new plan: {len(self._queue_plans)} plans are left in the queue")
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

            print(f"Sending message 'run_task' ...")
            self._server_conn.send(msg)
            return True
        else:
            print(f"Queue is empty")
            return False

    def receive_packet_thread(self):
        while True:
            ttime.sleep(0.1)
            if self._server_conn.poll():
                try:
                    msg = self._server_conn.recv()
                    self.run_task()
                except Exception as ex:
                    logger.error(f"Server: Exception occurred while waiting for packet: {ex}")
                    break

    async def start_environment(self, request):
        if not self._environment_exists:
            self.start_re_worker()
            self._environment_exists = True
            success, msg = True, ""
        else:
            success, msg = False, "Environment already exists."
        return web.json_response({"success": success, "msg": msg})

    async def close_environment(self, request):
        if self._environment_exists:
            self.stop_re_worker()
            self._environment_exists = False
            success, msg = True, ""
        else:
            success, msg = False, "Environment does not exist."
        return web.json_response({"success": success, "msg": msg})

    async def process_queue(self, request):
        if self._environment_exists:
            self.run_task()
            success, msg = True, ""
        else:
            success, msg = False, "Environment does not exist. Can not start the task."
        return web.json_response({"success": success, "msg": msg})

    def setup_routes(self, app):
        app.add_routes(
            [
                web.get("/", self.hello),
                web.get("/queue_view", self.queue_view_handler),
                web.post("/add_to_queue", self.addto_queue_handler),
                web.post("/pop_from_queue", self.pop_from_queue_handler),
                web.post("/start_environment", self.start_environment),
                web.post("/close_environment", self.close_environment),
                web.post("/process_queue", self.process_queue),
            ]
        )



def init_func(argv):
    re_server = RunEngineServer()

    app = web.Application()
    re_server.setup_routes(app)
    app["re_server"] = re_server  # To keep it alive
    return app
