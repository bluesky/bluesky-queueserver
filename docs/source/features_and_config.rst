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
functions (see :ref:`method_function_execute`). The basic principles of configuring user group
permissions are outlined in the section :ref:`configuring_user_group_permissions`.

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

.. _configuring_user_group_permissions:

Configuring User Group Permissions
----------------------------------

Each user group is assigned a set of permissions, which restrict the plans that users
are allowed to execute and devices users may submit to plans as parameters.
The permissions do not influence the code running in RE Worker environment, therefore
any existing plans and devices could be used from within plans. Optionally, permissions
may be configured to allow users to execute Python functions defined in RE Worker namespace
(see :ref:`method_function_execute` API).

User groups names are defined in user permissions dictionary, which could be saved in
``user_group_permissions.yaml`` file and loaded on startup or uploaded by the client application
(see :ref:`method_permissions_set` API). The dictionary must define at least one required user group
named ``root``. Restrictions defined for ``root`` are applied to plans and devices accessible
by any other defined group (consider it as a root of the tree of permissions). Internally,
the lists of existing plans and devices are initially filtered using ``root`` permissions
before the permissions for the other defined groups are applied to create lists of allowed
plans and devices. Putting common restrictions in the permissions for the ``root`` group
may reduce time of processing permissions. It is not recommended to assign users to
the ``root`` group or submit plans as ``root``, but currently there are no restrictions
that would prevent from doing it.

Permission for each group include lists of allowed and forbidden plans, devices and functions.
Each lists contains names and/or patterns for filtering names of device, plan or function
objects. In order for a device, plan or function to be accessible to users of a group,
the name of the object must match one of the names or patterns from the 'allowed' list and
not match any of the names or patterns from the 'forbidden' list. The guidelines for
composing lists of names and patterns for devices may be found in :ref:`lists_of_device_names`
and for plans and functions in :ref:`lists_of_plan_names`.

All the lists are optional. If 'allowed' list is not defined for a given type of objects
(plans, devices or functions), then no objects of this type are allowed. The most efficient
method to allow all objects is to set the first element of the 'allowed' list ``None``
(it could be the only element of the list, all other elements are ignored). If 'forbidden'
list is missing or if the first element of the list is ``None``, then no objects are forbidden,
i.e. all the objects matching the 'allowed' patterns will appear in the list of allowed objects.
Typically, permissions for a group would contain at least ``allowed_plans`` section to allow
users to submit some plans to the queue, but it may not be necessary in some workflows.
Missing ``allowed_devices`` section means that no devices could be passed to plans as parameters.

Following is an example of a trivial user permission dictionary (in YAML format), which
allows all plans and devices for ``admin`` user group ('admin' is an arbitrarily chosen name).
Restrictions for the ``root`` group forbid access to all plans and devices starting with local
names (starting with '_'). Note, that those plans and devices can still be used in plans.
The ``root`` permissions are applied to all other groups, which means that no group
could be configured to access objects with local names. Additional user group ``test_user``
is created with the sole purpose of demonstrating different types of name patterns.

.. code-block::

  user_groups:
    root:  # The group includes all available plan and devices
      allowed_plans:
        - null  # Allow all
      forbidden_plans:
        - ":^_"  # All plans with names starting with '_'
      allowed_devices:
        - null  # Allow all
      forbidden_devices:
        - ":^_:?.*"  # All devices with names starting with '_' and their subdevices
      allowed_functions:
        - null  # Allow all
      forbidden_functions:
        - ":^_"  # All functions with names starting with '_'
    admin:  # The group includes beamline staff, includes all or most of the plans and devices
      allowed_plans:
        - ":.*"  # Different way to allow all plans.
      allowed_devices:
        - ":?.*:depth=5"  # Allow all device and subdevices. Maximum deepth for subdevices is 5.
      allowed_functions:
        - "function_sleep"  # Explicitly listed name
    test_user:  # Some examples of patterns that could be used in lists
      allowed_plans:
        - ":^count"  # Allow all plan names starting with 'count'
        - ":scan$"  # Allow all plan names ending with 'scan'
        - ":product"  # Allow all plans names containing the word 'product'
      forbidden_plans:
        - "adaptive_scan"  # Do not allow the plan 'adaptive_scan'
        - ":^inner_product"  # Do not allow all plan names starting with 'inner_product'
      allowed_devices:
        - ":^det:?.*"  # Allow all devices starting with 'det' and their subdevices
        - ":^motor:?.*"  # Allow allow all devices starting with 'motor' and their subdevices
        - ":^sim_bundle_A:?.*"  # Same for devices starting with 'sim_bundle_A'
      forbidden_devices:
        - ":^det[3-5]$:?.*"  # Do not allow devices 'det3', 'det4' and 'det5' and all subdevices
        - ":^motor\\d+$:?.*"  # Same for all numbered motors, such as 'motor2' or 'motor256'

It is recommended that new projects are started using the sample file
`user_group_permissions.yaml <https://github.com/bluesky/bluesky-queueserver/blob/main/bluesky_queueserver/profile_collection_sim/user_group_permissions.yaml>`_,
which could be copied to the directory containing startup files and then modified according
to the project needs.

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