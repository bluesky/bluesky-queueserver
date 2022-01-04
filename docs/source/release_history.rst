===============
Release History
===============

v0.0.9 (2022-01-04)
===================

Fixed
-----

- Numerous fixes related to reliability of Queue Server operation.

- Implemented changes to make Queue Server compatible with ``aioredis`` v2.


Added
-----

- The new ``--update-existing-plans-devices`` CLI parameter of ``start-re-manager`` was added that
  controls when the file that stores existing plans and devices is updated.

- A new parameter of ``permissions_reload`` 0MQ API: ``reload_plans_devices`` (boolean, the default
  value is ``False``). If set ``True``, the parameter forces RE Manager to load the list of
  existing plans and devices from the disk file. The API may be called with ``reload_plans_devices=True``
  using ``qserver`` CLI tool as ``qserver permissions reload lists``.

- A new parameter of ``permissions_reload`` 0MQ API: ``reload_permissions`` (boolean, the default
  value is ``True``). If ``True``, permissions are reloaded from the disk file (if allowed), otherwise
  the currently used permissions are used in computations.

- Extended the number of states of worker environment. Currently used states include ``initializing``,
  ``idle``, ``executing_plan``, ``executing_task``, ``closing``, ``closed``.

- A new status fields (``status`` 0MQ API): ``worker_environment_state``, ``worker_background_tasks``,
  ``task_results_uid``, ``plans_existing_uid``, ``devices_existing_uid``.

- Extended the number of sections in specification of user group permissions (e.g. in
  ``user_group_permissions.yaml`` file). The new sections (``allowed_functions`` and ``forbidden_functions``)
  define conditions for names of functions that are accessible using ``function_execute`` API by users
  from each user groups.

- New 0MQ API: ``script_upload``, ``function_execute``, ``task_result``, ``plans_existing``,
  ``devices_existing``. CLI implementation: ``qserver script upload`` (``script_upload`` API),
  ``qserver function execute`` (``function_execute`` API), ``qserver task result`` (``task_result`` API),
  ``qserver existing devices`` (``devices_existing`` API), ``qserver existing plans`` (``plans_existing`` API).

- A new 0MQ API: ``permissions_set`` and ``permissions_get`` that allow uploading and downloading
  user group permissions. CLI support for the new API: ``qserver permissions set <fln.yaml>`` and
  ``qserver permissions get``.

- A new parameter of ``start-re-manager`` CLI tool: ``--user-group-permissions-reload``. The parameter accepts
  values ``NEVER``, ``ON_REQUEST`` and ``ON_STARTUP``.

- A new section in documentation on management of user group permissions.

Changed
-------

- Refactoring of the code for management of user group permissions and lists of existing and
  allowed plans and devices to make it more consistent. The identical lists of user permissions
  and existing plans and devices are now maintained by worker and manager processes.

- The lists of existing plans and devices used by RE Manager (both manager and worker processes)
  is automatically updated each time plans and devices in are changed in RE namespace (currently
  RE namespace is changed only when a new worker environment is opened).

- All sections for user group permissions are now optional. The ``forbidden_plans``,
  ``forbidden_devices`` and ``forbidden_functions`` sections could be skipped if there are
  no forbidden items that need to be excluded. Skipping ``allowed_...`` section disables all
  items (plans, devices or functions) for the group, e.g. if ``allowed_plans`` is skipped,
  users from this group will not be able to submit or run any plans. Since rules for
  the ``root`` group are applied to the lists accessible by users from all other groups,
  skipping ``allowed_plans`` for ``root`` disables all plans for all other groups.


v0.0.8 (2021-10-15)
===================

Maintenance release.

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
