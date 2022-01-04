==========================
Features and Configuration
==========================

Documentation is coming soon ...

Managing User Group Permissions
-------------------------------

Direct access via API calls to plans, devices and functions can be restricted by defining permissions
for different groups of users. The permissions are represented in the form of dictionary and saved in
a YAML file (default file name is ``user_group_permissions.yaml``) located in the same directory as
the startup code. Alternative path to the YAML file may be specified using ``--user-group-permissions``
parameter of ``start-re-manager``. The YAML file contains lists of conditions (based on regular expressions)
that are applied to the names of plans and devices defined in startup scripts to generate lists of
allowed plans and devices for each user group. Plans from the allowed list could be submitted to
the queue using API calls such as :ref:`method_queue_item_add` and the names of allowed devices could
be passed as plan parameters and then automatically converted to actual device objects before plans
are executed. Additional filters could be defined for each user group that allow/forbid execution of
functions (see :ref:`method_function_execute`).

TODO: GUIDE/TUTORIAL FOR WRITING ``user_group_permissions.yaml``.

Simple workflows could be implemented using static permissions defined in a YAML file and loaded
at startup of RE Manager. If the YAML file is changed while RE Manager is running, the permissions
may be reloaded by stopping and restarting RE Manager or sending :ref:`method_permissions_reload` API
request. Note, that changes in permissions affect the plans that are already in the queue, i.e. some
plans may fail to start if the respective user permissions are removed. The dictionary with current
user group permissions can be downloaded by client applications at any time using
:ref:`method_permissions_get` API.

More complex workflows may require dynamically changing user group permissions. Dictionary with permissions
can be uploaded by client applications using :ref:`method_permissions_set` API. (Application developers
should be careful about exposing this API directly to facility users unless users are expected to be able
to change permissions at will. The API is primarily intended for use by a secure server that is managing
user data and permissions.) The way RE Manager is handling updated permissions is defined by
``--user-group-permissions-reload`` parameter of ``start-re-manager``, which may take the following values:

- ``ON_STARTUP`` (default): RE Manager is always loading permissions from disk file at startup.
  Permissions from disk can be reloaded at any time using :ref:`method_permissions_reload`
  API. Permissions can be uploaded by the client application using :ref:`method_permissions_set` API.
  RE manager will use the updated permissions until the end of the session. RE Manager is using
  the uploaded permissions until the end of the session (until the manager is stopped and started again).

- ``ON_REQUEST``: RE Manager attempts to load internally stored (in Redis) permissions at startup.
  If no permissions are found (e.g. when starting newly installed Queue Server), RE Manager loads permissions
  from the disk file and saves the permissions internally so that they could be loaded at the next startup.
  Permissions can be reloaded from disk at any time by :ref:`method_permissions_reload` API
  or uploaded using :ref:`method_permissions_set` API. Each time permissions are changed, they are
  saved internally, so that they could be loaded on the next startup.

- ``NEVER``: RE manager behaves as if it was called with ``ON_REQUEST`` option except that
  :ref:`method_permissions_reload` API can not be used to reload permissions from disk (API call fails).
  Permissions are still loaded from disk at startup if no internally stored permissions are found.
  This option is the most appropriate for workflows, where permissions are managed externally and
  uploaded to RE Manager when changed. In this case the disk file may contain some default, simple
  and very restrictive set of permissions used for initialization during the first startup of RE Manager.


Remote Monitoring of Console Output
-----------------------------------

RE Manager is capable of capturing and publishing console output to 0MQ socket.
0MQ publishing is disabled by default and must be enabled using ``--zmq-publish``
parameter of ``start-re-manager``. A simple monitoring application (``qserver-console-monitor``)
allows to visualize the published output. See :ref:`remote_monitoring_tutorial` for a brief
tutorial.

``bluesky_queueserver`` package provides ``ReceiveConsoleOutput`` and ``ReceiveConsoleOutputAsync``
class, which can be helpful in implementing remote monitoring features in client applications. See
:ref:`subscribing_to_console_output` for more details.