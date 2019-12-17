===================
bluesky-queueserver
===================

.. image:: https://img.shields.io/travis/tacaswell/bluesky-queueserver.svg
        :target: https://travis-ci.org/tacaswell/bluesky-queueserver

.. image:: https://img.shields.io/pypi/v/bluesky-queueserver.svg
        :target: https://pypi.python.org/pypi/bluesky-queueserver


Server for queueing plans

* Free software: 3-clause BSD license
* Documentation: (COMING SOON!) https://tacaswell.github.io/bluesky-queueserver.

Features
--------

A minimal example.

From a shell::

  python -m aiohttp.web -H 0.0.0.0 -P 8080 bluesky_queueserver.server:init_func


From Python ::

  from ophyd.sim import motor, det
  from bluesky.plans import count, scan
  from bluesky_queueserver.plan import configure_plan
  from bluesky import RunEngine


  devices = {d.name: d for d in [motor, det]}
  plans = {'count': count, 'scan': scan}
  url = 'http://0.0.0.0:8080'

  plan = configure_plan(devices, plans, url)

  RE = RunEngine()
  # be _very_ verbose
  RE.msg_hook = print
  RE.subscribe(print)

  RE(plan())


From a different shell::

   http POST 0.0.0.0:8080/add_to_queue plan:='{"plan":"count", "args":[["det"]]}'
   http POST 0.0.0.0:8080/add_to_queue plan:='{"plan":"scan", "args":[["det"], "motor", -1, 1, 10]}'
