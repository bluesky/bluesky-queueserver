import requests
import bluesky.plan_stubs as bps


def configure_plan(devices, plans, url):

    plans.setdefault("sleep", bps.sleep)

    def _to_objects(inp):
        if isinstance(inp, str):
            return devices.get(inp, inp)
        if isinstance(inp, dict):
            return {k: _to_objects(v) for k, v in inp.items()}
        if isinstance(inp, list):
            return [_to_objects(o) for o in inp]
        return inp

    def queueserver_plan():
        while True:
            r = requests.post(f"{url}/pop_from_queue")
            if r.ok:
                payload = r.json()
                plan = plans[payload["plan"]]
                args = [_to_objects(o) for o in payload.get("args", [])]
                kwargs = {k: _to_objects(v) for k, v in payload.get("kwargs", {})}
                yield from plan(*args, **kwargs)
            else:
                raise RuntimeError("Server is down", r)

    return queueserver_plan
