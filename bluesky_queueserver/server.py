from aiohttp import web
from multiprocessing import Process, Pipe
import threading
import time as ttime

from ophyd.sim import det1, det2
from bluesky.plans import count

from .worker import RunEngineWorker

import logging
logger = logging.getLogger(__name__)

re_worker, server_conn, worker_conn = None, None, None
environment_exists = False

thread_conn = None

queue_plans = []


#   http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]]}'
#   http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"scan", "args":[["det1", "det2"], "motor", -1, 1, 10]}'


async def hello(request):
    return web.Response(text=f"Hello, world.  There are {len(queue_plans)} plans enqueed")


async def queue_view_handler(request):
    out = {"queue": queue_plans}
    return web.json_response(out)


async def addto_queue_handler(request):
    data = await request.json()
    # TODO validate inputs!
    plan = data["plan"]
    location = data.get("location", len(queue_plans))
    queue_plans.insert(location, plan)
    return web.json_response(data)


async def pop_from_queue_handler(request):
    if len(queue_plans):
        plan = queue_plans.pop()  # Pops from the back of the queue
        return web.json_response(plan)
    else:
        return web.json_response({"plan": "sleep", "args": [10]})


def start_re_worker():
    global re_worker
    re_worker = RunEngineWorker(conn=worker_conn)
    re_worker.start()


def stop_re_worker():
    global re_worker
    msg = {"type": "command", "value": "quit"}
    server_conn.send(msg)
    re_worker.join()


def run_task():
    if queue_plans:
        print(f"Starting new plan: {len(queue_plans)} plans are left in the queue")
        new_plan = queue_plans.pop(0)

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
        server_conn.send(msg)
        return True
    else:
        print(f"Queue is empty")
        return False


async def start_environment(request):
    global environment_exists
    if not environment_exists:
        start_re_worker()
        environment_exists = True
        success, msg = True, ""
    else:
        success, msg = False, "Environment already exists."
    return web.json_response({"success": success, "msg": msg})


async def close_environment(request):
    global environment_exists
    if environment_exists:
        stop_re_worker()
        environment_exists = False
        success, msg = True, ""
    else:
        success, msg = False, "Environment does not exist."
    return web.json_response({"success": success, "msg": msg})


async def process_queue(request):
    global environment_exists
    if environment_exists:
        run_task()
        success, msg = True, ""
    else:
        success, msg = False, "Environment does not exist. Can not start the task."
    return web.json_response({"success": success, "msg": msg})


def setup_routes(app):
    app.add_routes(
        [
            web.get("/", hello),
            web.get("/queue_view", queue_view_handler),
            web.post("/add_to_queue", addto_queue_handler),
            web.post("/pop_from_queue", pop_from_queue_handler),
            web.post("/start_environment", start_environment),
            web.post("/close_environment", close_environment),
            web.post("/process_queue", process_queue),
        ]
    )


def receive_packet_thread():
    while True:
        ttime.sleep(0.1)
        if server_conn.poll():
            try:
                print(f"Waiting for message")
                msg = server_conn.recv()
                run_task()
                #self._loop.call_soon_threadsafe(self._conn_received, msg)
                print(f"Server: message received {msg}")
            except Exception as ex:
                logger.error(f"Server: Exception occurred while waiting for packet: {ex}")
                break


def init_func(argv):
    global server_conn, worker_conn, thread_conn
    server_conn, worker_conn = Pipe()

    # Start the thread as 'daemon', because there is no place to join it in the current configuration
    thread_conn = threading.Thread(target=receive_packet_thread,
                                   name="RE Server Comm",
                                   daemon=True)
    thread_conn.start()

    app = web.Application()
    app["queue"] = []
    setup_routes(app)
    return app
