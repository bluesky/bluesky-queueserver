===============
Release History
===============


v0.0.7 (2021-10-06)
===================

Fixed
-----

* Behavior of ``re_pause`` 0MQ API: if ``re_pause`` is called past the last checkpoint of the plan,
  the plan is considered successfully completed and execution of the queue is stopped.
  The stopped queue can be started again using ``queue_start`` API request.

* JSON schemas and code using validation of JSON schemas was modified for compatibility with
  ``jsonschema`` v4.0.1. Queue server still works with older versions of ``jsonschema``.

Added
-----

* A new boolean flag (``pause_pending``) added to dictionary returned by ``status`` API.
  The flag is ``True`` when request to pause a plan (``re_pause`` API) was accepted by the Queue Server,
  but not processed by the Run Engine. The flag is set in case of immediate and deferred pause request.
  The flag is cleared automatically (set to ``False``) when the request is processed and the plan is paused
  or the queue is stopped (if deferred pause is requested after the last checkpoint of the plan).


v0.0.6 (2021-09-16)
===================

Added
-----

* New API: ``ReceiveConsoleOutputAsync`` (async version of ``ReceiveConsoleOutput``)
  for receiving console output from RE Manager in `asyncio`-based applications (e.g. HTTP Server).

Changed
-------

* Renamed parameters of `start-re-manager`: ``--zmq-publish`` is renamed to ``--zmq-publish-console``,
  ``--zmq-publish-addr`` is renamed to ``--zmq-publish-console-addr``.
* Parameters ``default``, ``min``, ``max`` and ``step`` of ``parameter_annotation_decorator`` now must be
  python expressions of supported types (``default``) or `int` or `float` numbers (``min``, ``max``
  and ``step``). In previous versions the parameter values had to be converted to strings in user code.
