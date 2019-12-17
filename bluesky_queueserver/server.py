from aiohttp import web


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


def setup_routes(app):
    app.add_routes(
        [
            web.get("/", hello),
            web.get("/queue_view", queue_view_handler),
            web.post("/add_to_queue", addto_queue_handler),
            web.post("/pop_from_queue", pop_from_queue_handler),
        ]
    )


def init_func(argv):
    app = web.Application()
    app["queue"] = []
    setup_routes(app)
    return app
