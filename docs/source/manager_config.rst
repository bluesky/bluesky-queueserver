.. _manager_configuration:

========================
RE Manager Configuration
========================

Introduction
------------

The default settings of RE Manager are optimized for running basic tutorials and
simple demos using built-in startup scripts with simulated plans and devices,
and are not sufficient for development and production deployments. The parameter
values may be passed to RE Manager using environment variables, config file(s)
and CLI parameters. Parameters passed using config file(s) override the parameters
passed using environment variables. Parameters passed using CLI override all other
parameters.

CLI Parameters
--------------

Comprehensive list of CLI parameters of RE Manager may be found in the documentation
for :ref:`start_re_manager_cli`. Note, that not setting parameters such as ``--keep-re``
or ``--use-persistent-metadata`` does not disable the respective features if
they are enabled in the config file.

Environment Variables
---------------------

Several parameters can be passed to RE Manager using environment variables:

  - ``QSERVER_CONFIG`` - a path to a single config file or a directory containing multiple
    config files. The path can be also passed using ``--config`` CLI parameter.

  - ``QSERVER_ZMQ_CONTROL_ADDRESS_FOR_SERVER`` - address of the 0MQ REQ-REP socket used
    for control communication channel. The address may also be set in the config file or
    passed using ``--zmq-control-addr`` CLI parameter.

  - ``QSERVER_ZMQ_INFO_ADDRESS_FOR_SERVER`` - address of the 0MQ PUB-SUB socked used to
    publish console output. The address may also be set in the config file or passed using
    ``--zmq-info-addr`` CLI parameter.

  - ``QSERVER_EMERGENCY_LOCK_KEY_FOR_SERVER`` - emergency lock key used to unlock RE Manager
    if the lock key set by a user is lost. The key may also be set in the config file.

  - ``QSERVER_ZMQ_PRIVATE_KEY_FOR_SERVER`` - private key used to decrypt control messages sent
    by clients. **Using environment variables may be preferred way of setting the public key.**
    Alternatively, the private key may be set in the config file by referencing a different
    environment variable. Explicitly listing security keys in the config file is not recommended.

Configuration Files
-------------------

The example of a configuration file (e.g. ``qserver_config.yml``) that contains all supported
parameters:

.. code-block::

    network:
      zmq_control_addr: tcp://*:60615
      zmq_private_key: ${CUSTOM_EV_FOR_PRIVATE_KEY}
      zmq_info_addr: tcp://*:60625
      zmq_publish_console: true
      redis_addr: localhost:6379
    startup:
      keep_re: true
      startup_dir: ~/.ipython/profile_collection/startup
      existing_plans_and_devices_path: ~/.ipython/profile_collection/startup
      user_group_permissions_path: ~/.ipython/profile_collection/startup
    operation:
      print_console_output: true
      console_logging_level: NORMAL
      update_existing_plans_and_devices: ENVIRONMENT_OPEN
      user_group_permissions_reload: ON_REQUEST
      emergency_lock_key: custom_lock_key
    run_engine:
      use_persistent_metadata: true
      kafka_server: 127.0.0.1:9092
      kafka_topic: custom_topic_name
      zmq_data_proxy_addr: localhost:5567
      databroker_config: TST

All the parameters are optional. The default values or values passed using environment
variables are used for missing parameters. The configuration may be split into multiple YML
files and the path to the directory containing the files passed to RE Manager.

Assuming the configuration is saved in ``~/.config/qserver/qserver_config.yml``,
RE Manager can be started as ::

    $ start-re-manager --config=~/.config/qserver/qserver_config.yml

or ::

    $ QSERVER_CONFIG=~/.config/qserver/qserver_config.yml start-re-manager

Additional CLI parameters override the respective configuration or default parameters.

network
+++++++

Parameters that define for network settings used by RE Manager:

- ``zmq_control_addr`` - address of the 0MQ REQ-REP socket used  for control communication channel.
  The address may also be set using environment variable ``QSERVER_ZMQ_CONTROL_ADDRESS_FOR_SERVER``
  or passed using ``--zmq-control-addr`` CLI parameter. Address format: ``tcp://*:60615``.

- ``zmq_private_key`` - private key used to decrypt control messages sent by clients (40 characters).
  The value should be a string referencing an environment variable (e.g. ``${CUSTOM_EV_NAME}``)
  or a string containing a public key (not recommended). The private key may also be set
  using environment variable ``QSERVER_ZMQ_PRIVATE_KEY_FOR_SERVER``.

- ``zmq_info_addr`` - address of the 0MQ PUB-SUB socked used to publish console output. The address
  may also be passed using environment variable QSERVER_ZMQ_INFO_ADDRESS_FOR_SERVER or
  ``--zmq-info-addr`` CLI parameter. Address format: ``tcp://*:60625``.

- ``zmq_publish_console`` - enable or disable publishing console output to the socket set using
  ``zmq_info_addr``. Accepted values are ``true`` and ``false``. The value can also passed using
  ``--zmq-publish-console`` CLI parameter.

- ``redis_addr`` - the address of Redis server, e.g. ``localhost``, ``127.0.0.1``, ``localhost:6379``.
  The value may also be passed using ``--redis-addr`` CLI parameter.

startup
+++++++

  Parameters that control opening the worker environment and handling of startup files:

  - ``keep_re`` - keep and use the instance of the Run Engine created in startup scripts (``true``)
    or delete the instance of the Run Engine created in startup scripts and create a new instance
    based on settings in :ref:`config_file_run_engine` (``false``). The built-in configuration
    options for Run Engine are very limited and it is assumed that Run Engine is created in startup
    scripts in production deployments.

  - ``startup_dir``, ``startup_profile``, ``startup_module`` and ``startup_script`` are mutually
    exclusive parameters that specify a path to startup directory, name of the startup IPython
    profile, name of installed Python module containing startup code or a path to startup script.
    The values may be passed using ``--startup-dir``, ``--startup-profile``, ``--startup-module``
    or ``--startup-script`` CLI parameters.

  - ``existing_plans_and_devices_path`` - path to file that contains the list of existing plans
    and devices. The path may be a relative path to the directory containing startup files.
    If the path is directory, then the default file name 'existing_plans_and_devices.yaml' is used.
    The value may also be passed using ``--existing-plans-devices`` CLI parameter.

  - ``user_group_permissions_path`` - path to a file that contains lists of plans and devices
    available to users. The path may be a relative path to the profile collection directory.
    If the path is a directory, then the default file name 'user_group_permissions.yaml' is used.
    The value may also be passed using ``--user-group-permissions`` CLI parameter.


operation
+++++++++

The parameters that define run-time behavior of RE Manager:

- ``print_console_output`` - enables (``true``) or disables (``false``) printing of console
  output in the terminal. The value may also be set using ``--console-output`` CLI parameter.

- ``console_logging_level`` - sets logging level used by RE Manager. The accepted values are
  ``'SILENT'``, ``'QUIET'`` ``'NORMAL'`` (default) and ``'VERBOSE'``. The non-default value
  may also be selected using ``--silent``, ``--quiet`` and ``--verbose`` CLI parameters.

- ``update_existing_plans_and_devices`` - select when the list of existing plans and devices
  stored on disk should be updated. The available choices are not to update the stored
  lists (``'NEVER'``), update the lists when the environment is opened
  (``'ENVIRONMENT_OPEN'``, default) or update the lists each the lists are changed (``'ALWAYS'``).
  The value may be set using ``--update-existing-plans-devices`` parameter.

- ``user_group_permissions_reload`` - select when user group permissions are reloaded from disk.
  Options: ``'NEVER'`` - RE Manager never attempts to load permissions from disk file.
  If permissions fail to load from Redis, they are loaded from disk at the first startup
  of RE Manager or on request. ``'ON_REQUEST'`` - permissions are loaded from disk file when
  requested by 'permission_reload' API call. ``'ON_STARTUP'`` (default) - permissions are loaded
  from disk each time RE Manager is started or when 'permission_reload' API request is received.
  The value may be set using ``--user-group-permissions-reload`` CLI parameter.

- ``emergency_lock_key`` - emergency lock key used to unlock RE Manager if the lock key set by
  a user is lost. The key may also be set using environment variable
  ``QSERVER_EMERGENCY_LOCK_KEY_FOR_SERVER``.

.. _config_file_run_engine:

run_engine
++++++++++

The parameters that define configuration of Run Engine created by RE Manager and some basic
subscriptions for the Run Engine. The configuration options are very limited and primarily
intended for use in quick demos. It is assumed that in production systems, Run Engine and
its subscriptions are fully defined in startup scripts and this section is skipped completely.

- ``use_persistent_metadata`` - use msgpack-based persistent storage for scan metadata
  (``true/false``). The option can also be enabled using ``--use-persistent-metadata`` CLI
  parameter.

- ``kafka_server`` - bootstrap server to for Kafka Run Engine callback, e.g. ``127.0.0.1:9092``.
  The value can be set using ``--kafka-server`` CLI parameter.

- ``kafka_topic`` - kafka topic of Kafka Run Engine callback. The value can also be set using
  ``--kafka-topic`` CLI parameter.

- ``zmq_data_proxy_addr`` - address of ZMQ proxy used to publish data by ZMQ Run Engine callback.
  The value can also be set using ``--zmq-data-proxy-addr`` CLI parameter.

- ``databroker_config`` -  databroker configuration (e.g. ``'srx'``) used by Databroker
  callback. The value can also be set using ``--databroker-config`` CLI parameter.
