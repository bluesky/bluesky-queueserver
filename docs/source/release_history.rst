===============
Release History
===============

v0.0.6 (2021-09-16)
===================

Added
-----

* New API: ``ReceiveConsoleOutputAsync`` (async version of ``ReceiveConsoleOutput``)
  for receiving console output from RE Manager in `asyncio`-based applications (e.g. HTTP Server).

Changed
-------

* Renamed parameters of `start-re-manager`:
  - ``--zmq-publish`` is renamed to ``--zmq-publish-console``
  - ``--zmq-publish-addr`` is renamed to ``--zmq-publish-console-addr``
* Parameters ``default``, ``min``, ``max`` and ``step`` of ``parameter_annotation_decorator`` now must be
  python expressions of supported types (``default``) or `int` or `float` numbers (``min``, ``max``
  and ``step``). In previous versions the parameter values had to be converted to strings in user code.
