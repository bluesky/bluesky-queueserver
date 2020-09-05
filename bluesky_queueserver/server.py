from aiohttp import web
from multiprocessing import Process, Pipe
import threading

from ophyd.sim import det1, det2
from bluesky.plans import count

from .worker import RunEngineWorker

re_worker, server_conn, worker_conn = None, None, None
environment_exists = False



async def hello(request):
    queue = request.app["queue"]

    return web.Response(text=f"Hello, world.  There are {len(queue)} plans enqueed")


async def queue_view_handler(request):
    out = {"queue": request.app["queue"]}
    return web.json_response(out)


async def addto_queue_handler(request):
    queue = request.app["queue"]
    data = await request.json()
    # TODO validate inputs!
    plan = data["plan"]
    location = data.get("location", len(queue))
    queue.insert(location, plan)
    return web.json_response(data)


async def pop_from_queue_handler(request):
    queue = request.app["queue"]
    if len(queue):
        plan = queue.pop()
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
    plan_name = "count"
    args = [["det1", "det2"]]
    kwargs = {"num": 5, "delay": [1, 2, 3, 4]}

    msg = {"type": "plan",
           "value": {"name": plan_name,
                     "args": args,
                     "kwargs": kwargs
                    }
          }

    print(f"Sending emssage 'run_task' ...")
    server_conn.send(msg)


async def start_environment(request):
    global environment_exists
    queue = request.app["queue"]
    if not environment_exists:
        start_re_worker()
        environment_exists = True
        success, msg = True, ""
    else:
        success, msg = False, "Environment already exists."
    return web.json_response({"success": success, "msg": msg})


async def close_environment(request):
    global environment_exists
    queue = request.app["queue"]
    if environment_exists:
        stop_re_worker()
        environment_exists = False
        success, msg = True, ""
    else:
        success, msg = False, "Environment does not exist."
    return web.json_response({"success": success, "msg": msg})


async def process_queue(request):
    global environment_exists
    queue = request.app["queue"]
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


def init_func(argv):
    global server_conn, worker_conn
    server_conn, worker_conn = Pipe()

    app = web.Application()
    app["queue"] = []
    setup_routes(app)
    return app
