===============
Release History
===============

v0.0.18 (2022-10-31)
====================

Fixed
-----

- Improved manager and worker stability in case of malfunctioning plans (plans that block
  Run Engine event loop).

Added
-----

- New ``timeout`` parameter for ``ZMQCommSendThreads.send_message()``, ``ZMQCommSendAsync.send_message()``
  and ``zmq_single_request()`` functions. The timeout overrides the default timeout ``timeout_recv``
  set during instantiation of the respective classes for the particular request.

- Support for managing parameters to RE Manager using configuration YML files.

- New CLI parameter ``--config`` and environment variable ``QSERVER_CONFIG`` for passing
  the path to config file to RE Manager.


v0.0.17 (2022-10-02)
====================

Changed
-------

- Now requires ``bluesky>=1.7.0``.

- Default user group name is changed from ``admin`` to ``primary``. Users of applications
  that rely on default user group name should change the group name in ``user_group_permissions.yaml`` file.

Removed
-------

- Removed built-in protocol support, now relies on ``bluesky.protocols``.


v0.0.16 (2022-07-30)
====================

Added
-----

- New parameter ``update_lists`` added to ``script_upload`` API. The parameter accepts boolean value
  (``True`` by default) and allows to disable update of lists of existing and allowed plans and
  devices after execution of the script. The parameter allows to improve efficiency of execution
  of scripts that do not add or modify plans and devices in RE worker namespace. Update of
  the lists may be disabled from CLI as ``qserver script upload <path-to-file> keep-lists``.

- New ``lock``, ``unlock`` and ``lock_info`` API. The API are accessible from CLI using ``qserver lock``
  and ``qserver unlock`` commands.

- ``qserver-clear-lock`` CLI tool for unlocking RE Manager if the lock key is lost and the emergency
  lock key is not set or unknown.

Fixed
-----

- Support for ``happi v1.14.0``.

Changed
-------

- Foreground tasks (started using ``script_upload`` and ``function_execute`` API) are now executed
  in the main thread of RE Worker.

v0.0.15 (2022-06-24)
====================

Added
-----

- Plan results (in plan history) now include error message (``msg`` key), which contains error message or
  full traceback in case of failing plan.

- Support for ``environment_destroy`` API in ``creating_environment`` RE Manager state. Now the requests
  to destroy environment are accepted when ``status["worker_environment_exists"] is True`` or
  ``status["manager_state"] == "creating_environment"``.

- API functions ``generate_zmq_keys``, ``generate_zmq_public_key``, ``validate_zmq_key`` can now be imported
  directly from ``bluesky_queueserver``

- Patching of IPython-style startup scripts: ``__file__`` variable now returns the path to the original unpatched script.

Fixed
-----

- Capturing console output with updating progress bars (Python 3.8, 3.9).

- A bug in the code for management of exceptions that occur during preparation of plans for execution.

- A bug that prevented single character device/plan names to be properly handled by the code that
  converts device/plan names to the respective objects.

Changed
-------

- The plan ``exit_status`` (in plan history) now takes values ``completed``, ``failed``, ``stopped``, ``aborted``,
  ``halted``, ``unknown``.

- The ``stopped`` plans (``re_stop`` API) are considered successful and no longer pushed back in the queue.
  The ``stopped`` plans are inserted in the back of the queue in LOOP mode.

- Standard names for parameters for CLI tools: ``--zmq-control-addr`` is used to pass address of RE Manager
  control socket and ``--zmq-info-addr`` is used to pass the address of RE Manager information socket
  (currently used for publishing console output). Old parameter names are deprecated, but still supported.

- Standard names for environment variables: ``QSERVER_ZMQ_CONTROL_ADDRESS_FOR_SERVER``,
  ``QSERVER_ZMQ_CONTROL_ADDRESS``, ``QSERVER_ZMQ_INFO_ADDRESS_FOR_SERVER``, ``QSERVER_ZMQ_INFO_ADDRESS``
  are used to pass control and information socket address to the server (``start-re-manager``) and clients
  (``qserver``, ``qserver-console-monitor``). An address passed as a parameter overrides the address passed as
  environment variable. Old environment variable names are deprecated, but still supported.

- Changed name of the environment variable used to pass the private encryption key to ``start-re-manager``
  to ``QSERVER_ZMQ_PRIVATE_KEY_FOR_SERVER``. (``QSERVER_ZMQ_PRIVATE_KEY`` is still supported, but deprecated.)
  Public key is still passed to ``qserver`` using ``QSERVER_ZMQ_PUBLIC_KEY``.

- The components of Area Detectors are no longer included in the list of available devices.

- Improved handling of IPython-style startup scripts.

- Minor change in representation of plan execution results in items of the plan history.
  If plan execution fails, the ``msg`` parameter contains a brief message that identify the error
  (may not be helpful) and ``traceback`` parameter contains full traceback. The parameters are empty strings
  in case the plan succeeds.

- Similar change to representation of task execution results returned by ``task_result`` API. Now ``return_value``
  is ``None`` in case the task fails and ``msg`` and ``traceback`` contain brief error message and traceback
  of the raised exception.

- Improved default handling of strings in the parameter processing code. Now any string (any combination
  of characters) can be passed with a parameter, which does not have type annotation. The strings that
  match one of the allowed device or plan names are going to be converted to the respective objects.


v0.0.14 (2022-04-08)
====================

Fixed
-----

- Capturing console output with updating progress bars (Python 3.8, 3.9).


v0.0.13 (2022-04-05)
====================

Added
-----

- Implementation of ``subscribe()`` and ``unsubscribe()`` methods in ``ReceiveConsoleOutput``
  and ``ReceiveConsoleOutputAsync`` classes

- ``ReceiveConsoleOutputAsync.stop()`` method now accepts an optional ``unsubscribe`` parameter
  that controls if 0MQ socket is unsubscribed when the acquisition is stopped. Default is ``True``.

- Timestamps ``time_start`` and ``time_stop`` are now added to ``result`` dictionary of
  each item in plan history.

Fixed
-----

- Implemented proper handling of non-JSON or invalid JSON requests.


v0.0.12 (2022-03-08)
====================

Fixed
-----

- Bug in handling of negative indices by ``queue_item_move`` and API.

- proper update of ``plan_queue_uid`` by ``queue_item_execute`` API.

Changed
-------

- Renamed parameters of ``permissions_reload`` API: ``reload_permissions`` is renamed
  to ``restore_permissions``, ``reload_plans_devices`` is renamed to ``restore_plans_devices``.

- Default BEC: no longer plot with best effort callback (improves performance).


v0.0.11 (2022-02-27)
====================

Fixed
-----

- Fixed handling of negative item indices by ``queue_item_add`` API
  (``pos=-1`` now adds an item to the back of the queue).

Added
-----

- New ``task_status`` API. The API may be called for a single task from CLI as
  ``qserver task status <task-uid>``.

Changed
-------

- ``status`` API is now returning Queue Server version number as part of ``msg``,
  e.g. ``"RE Manager v0.0.11"``.

- Extended ``re_pause`` API. Now the ``option`` parameter is optional.
  The default value is ``"option": "deferred"``.


v0.0.10 (2022-02-08)
====================

Fixed
-----

- A bug that allowed classes defined in the global scope of the startup script and recognized as
  ``bluesky.protocols.Movable``, ``Readable`` or ``Flyable`` (e.g. ``ophyd.Device``) to be
  included in the list of existing devices. Only instantiated class objects are currently
  included in the list.

- A deficiency in the code that loads Python scripts (not startup scripts from the folder
  as in IPython ``profile_collection``) into the environment that failed to load scripts containing
  definitions of devices with components.

Added
-----

- Support for passing subdevice names as values of plan parameters.

- Support for regular expressions in the lists of names defined in ``plans``
  and ``devices`` sections of ``parameter_annotation_decorator``. Keywords ``__MOTOR__``,
  ``__DETECTOR__``, ``__READABLE__`` or ``__FLYABLE__`` can be used in conjunction with
  regular expression to select device of the respective types.

- New boolean parameters of the plan parameter annotation (``convert_plan_names``
  and ``convert_device_names``) for explicitly enabling/disabling conversion of names
  of plans and/or devices passed as parameter values. Setting those parameters
  overrides the default behavior and should be used with caution.

- Support for subdevice names in **'user_group_permissions.yaml'**.


Changed
-------

- The algorithm for processing of user group permissions has changed. The old
  **'user_group_permissions.yaml'** may no longer work as expected. If the stock
  **'user_group_permissions.yaml'** is used for the project, replace it with
  the updated file from the repository. Otherwise update the existing file
  using following guidelines:

  - If the project uses custom **'user_group_permissions.yaml'**, then insert ``:``
    before each regular expression in the lists (e.g. change ``"^count"`` to
    ``":^count"``, ``"^det"`` to ``":^det"`` etc.).
  - In previous versions, only the lists with regular expressions were supported.
    Now the lists may include explicitly listed plan, device or subdevice names,
    such as ``"count"``, ``"det1"``, ``"det1.val"`` (there is no need to use regular
    expressions such as ``":^count$"`` to allow the plan ``count``).
  - The supported patterns allow to control which subdevices are included. For example,
    the pattern ``:^det`` includes all devices with names starting with ``det``,
    but no subdevices. The pattern ``":^det:?.*"`` selects all subdevices with
    unlimited depth. Patterns may include the parameter ``depth`` that limits
    maximum depth for subdevices, for example ``":^det:?.*:depth=2"`` adds
    subdevices and subdevices of subdevices. See
    `Configuring User Group Permissions
    <https://blueskyproject.io/bluesky-queueserver/features_and_config.html#configuring-user-group-permissions>`_
    for more detailed instructions.

Removed
-------

- Built-in types ``AllDetectors``, ``AllMotors``, ``AllFlyers`` and ``AllPlans`` can no
  longer be used in parameter annotations of defined in ``parameter_annotation_decorator``.
  Use regular expressions in conjunction with keywords ``__MOTOR__``, ``__DETECTOR__``,
  ``__READABLE__`` or ``__FLYABLE__`` to create lists of devices of respective types.
  Use built-in types ``__PLAN__``, ``__DEVICE__``, ``__PLAN_OR_DEVICE__`` in parameter
  annotations to selectively enable conversion of names for all plans and/or
  devices without creating lists of names. Alternatively, use ``convert_plan_names``
  or ``convert_device_names`` parameters of the annotation in order to explicitly
  enable/disable conversion of all plan/device names.

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
