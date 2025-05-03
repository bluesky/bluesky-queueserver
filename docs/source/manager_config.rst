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

Configuring 0MQ Communication
-----------------------------

RE Manager opens two 0MQ sockets for communication with clients: REP socket for
receiving and replying to control commands from clients and PUB socket for streaming
information (console output) to clients. The socket addresses, if different from
default, may be set using ``--zmq-control-addr`` and ``--zmq-info-addr`` CLI parameters
of ``start-re-manager``. The addresses may also be set using environment variables
or config file parameters.

If RE Manager and the client (e.g. HTTP Server) are running on the same host,
0MQ may be configured to use IPC communication. RE Manager and the clients accept
IPC addresses, such as ``ipc:///tmp/my_channel`` or ``ipc://@my_channel``.
IPC may be used both for control communication and info streaming.

Incoming communication in the control channel may be encrypted using configurable
public/private key pair. The private key for RE Manager is set using
``QSERVER_ZMQ_PRIVATE_KEY_FOR_SERVER`` environment variable. The key pair may be generated
using using :ref:`qserver_zmq_keys_cli` CLI tool. If encryption
is enabled, the outgoing message are also encrypted, but the key pair is fixed and
can not be changed. Customization of the key pair for outgoing messages may be changed
if necessary.

The 0MQ messages are encoded as JSON by default. RE Manager also supports MSGPACK encoding.
The encoding is set for both sockets using ``--zmq-encoding`` CLI parameter or respective
environment variable or config file parameter.

CLI Parameters
--------------

Comprehensive list of CLI parameters of RE Manager may be found in the documentation
for :ref:`start_re_manager_cli`. Note, that not setting parameters such as ``--keep-re``
or ``--use-persistent-metadata`` does not disable the respective features if
they are enabled in the config file.

.. _config_environment_variables:

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

  - ``QSERVER_ZMQ_ENCODING_FOR_SERVER`` - encoding for messages passed over 0MQ sockets
    (both CONTROL and INFO). The encoding may also be set in the config file or passed
    using ``--zmq-encoding`` CLI parameter. The supported values are *'json'* (default)
    and *'msgpack'*.

  - ``QSERVER_EMERGENCY_LOCK_KEY_FOR_SERVER`` - emergency lock key used to unlock RE Manager
    if the lock key set by a user is lost. The key may also be set in the config file.

  - ``QSERVER_ZMQ_PRIVATE_KEY_FOR_SERVER`` - private key used to decrypt control messages sent
    by clients. **Using environment variables may be preferred way of setting the public key.**
    Alternatively, the private key may be set in the config file by referencing a different
    environment variable. Explicitly listing security keys in the config file is not recommended.

  - ``QSERVER_USE_IPYTHON_KERNEL`` - tells RE Manager whether to start tbe worker in IPython mode
    (start IPython kernel) or use plain Python worker. Boolean value.

  - ``QSERVER_IPYTHON_KERNEL_IP`` - sets IP address for IPython kernel opened in the worker process.
    The values are ``localhost``, ``auto`` (automatically find network IP address of the host running
    the worker) or valid network IP address of the host. If the address is ``localhost`` or
    ``127.0.0.1``, the clients running on remote hosts will not be able to connect to the kernel.
    Used only in IPython mode.

  - ``QSERVER_IPYTHON_KERNEL_CONNECTION_FILE`` - sets the name of IPython kernel connection file.
    Used only in IPython mode.

  - ``QSERVER_IPYTHON_KERNEL_CONNECTION_DIR`` - sets the directory for IPython kernel connection
    files. Used only in IPython mode.

  - ``QSERVER_IPYTHON_KERNEL_IOPUB_PORT`` - sets the port for IPython kernel IOPub socket.
    Used only in IPython mode.

  - ``QSERVER_IPYTHON_KERNEL_HB_PORT`` - sets the port for IPython kernel heartbeat socket.
    Used only in IPython mode.

  - ``QSERVER_IPYTHON_KERNEL_CONTROL_PORT`` - sets the port for IPython kernel control socket.
    Used only in IPython mode.

  - ``QSERVER_IPYTHON_KERNEL_SHELL_PORT`` - sets the port for IPython kernel shell socket.
    Used only in IPython mode.

  - ``QSERVER_IPYTHON_KERNEL_STDIN_PORT`` - sets the port for IPython kernel stdin socket.
    Used only in IPython mode.

  - ``QSERVER_IPYTHON_KERNEL_MATPLOTLIB`` - sets Matplotlib backend for IPython kernel.
    Used only in IPython mode.

.. _config_configuration_files:

Configuration Files
-------------------

The example of a configuration file (e.g. ``qserver_config.yml``) that contains all sections and
most of the supported parameters:

.. code-block::

    network:
      zmq_control_addr: tcp://*:60615
      zmq_private_key: ${CUSTOM_EV_FOR_PRIVATE_KEY}
      zmq_info_addr: tcp://*:60625
      zmq_encoding: json
      zmq_publish_console: true
      redis_addr: localhost:6379
      redis_name_prefix: qs_default
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
    worker:
      use_ipython_kernel: true
      ipython_kernel_ip: auto
      ipython_matplotlib: qt5
      ipython_connection_file: connection_file.json,
      ipython_connection_dir: /tmp
      ipython_shell_port: 60000
      ipython_iopub_port: 60001
      ipython_stdin_port: 60002
      ipython_hb_port: 60003
      ipython_control_port: 60004
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

- ``zmq_encoding`` - encoding used for sending and receiving 0MQ messages in all the sockets.
  The encoding may also be set using ``--zmq-encoding`` CLI parameter or ``QSERVER_ZMQ_ENCODING_FOR_SERVER``
  environment variables. The supported values are *'json'* (default) and *'msgpack'*.

- ``zmq_publish_console`` - enable or disable publishing console output to the socket set using
  ``zmq_info_addr``. Accepted values are ``true`` and ``false``. The value can also passed using
  ``--zmq-publish-console`` CLI parameter.

- ``redis_addr`` - the address of Redis server, e.g. ``localhost``, ``127.0.0.1``, ``localhost:6379``.
  The address may also contain a Redis database number, e.g. ``localhost:6379/0``.
  Redis address may also be passed using ``--redis-addr`` CLI parameter.

- ``redis_name_prefix`` - the prefix is appended to the Redis keys to differentiate between keys
  created by different instances of RE Manager. The value may also be passed using
  ``--redis-name-prefix`` CLI parameter.

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

.. _config_file_worker:

worker
++++++

The parameters that define configuration of RE Worker.

- ``use_ipython_kernel`` - enable/disable IPython mode (``true/false``, default ``false``).
  In IPython mode the worker creates IPython kernel used to run the worker environment.
  If IPython mode is disabled, the worker environment is run using plain Python. The option
  can also be set using ``--use-ipython-kernel`` CLI parameter or ``QSERVER_USE_IPYTHON_KERNEL``
  environment variable. See :ref:`worker_ipython_kernel` and :ref:`config_of_ipython_kernel`
  for more details.

- ``ipython_kernel_ip`` - set IP address of IPython kernel. The option is ignored if worker
  is running not in IPython mode. The supported values are ``localhost``, ``auto`` or valid
  network IP address of the host. If the IP address is ``localhost`` (default) or ``127.0.0.1``,
  the clients running on remote hosts can not connect to the kernel. If the value is ``auto``,
  the worker attempts to find network address of the host. The option can also be set using
  ``--ipython-kernel-ip`` CLI parameter or ``QSERVER_IPYTHON_KERNEL_IP`` environment variable.

- ``ipython_matplotlib`` - set Matplotlib backend for IPython kernel. The parameter is ignored
  if the worker is not in IPython mode. The parameter accepts the set of values identical to
  the parameter ``--matplotlib`` of ``IPython``. Typical values are ``agg`` (default, disables
  plotting) or ``qt5`` (plotting using Qt5 backend). The option can also be set using
  ``--ipython-matplotlib`` CLI parameter.

- ``ipython_connection_file`` - the name of the IPython kernel connection file.

- ``ipython_connection_dir`` - the name and directory where IPython kernel creates and looks
  for the connection files. The default value is good in most cases.

- ``ipython_shell_port``, ``ipython_iopub_port``, ``ipython_stdin_port``, ``ipython_hb_port``,
  ``ipython_control_port`` - 0MQ ports used by IPython kernel.


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


Using Redis
-----------

RE Manager is using Redis as a persistent storage for plan queue, plan history and a few other
parameters, that are expected to be preserved between RE Manager restarts. Starting from version
v0.0.20, RE Manager is appending a prefix to each Redis key. The prefix can be used to identify
the keys created by different instances of RE Manager (not necessarily running simultaneously,
but maintaining different plan queue and history). The prefixed keys can also be easily
distinguished from keys created by other applications using the same Redis server. The default
prefix is ``qs_default``. Custom prefix can be passed using ``--redis-name-prefix`` CLI parameter
or set in the config file using ``redis_name_prefix`` parameter in the ``network`` section.

.. note::

  It is recommended that Redis server is installed locally on the machine running the Queue Server

Prior to version v0.0.20, RE Manager did not append any prefix to the keys. If it is desirable
to continue using RE Manager without prefix, e.g. to access the plan queue and history created
by the older version of RE Manager, pass `""` (empty string) as the parameter value.

.. _config_of_ipython_kernel:

Configuration of IPython Kernel
-------------------------------

Queue Server can be configured to execute plans using IPython or plain Python (default mode).
If IPython mode is enabled, the worker process is starting a new in-process IPython kernel
each time the environment is opened. The worker then connects to the kernel 0MQ ports to
monotor the kernel state and run tasks. External client applications, such as Jupyter console,
may also connect to the same 0MQ ports to communicate with the kernel. The kernel connection,
including kernel IP address, port numbers and location of the connection file, is configured
using a group of connection parameters. The connection parameters may be passed as CLI parameters
(see :ref:`start_re_manager_ipython_kernel`), environment variables (see :ref:`config_environment_variables`)
or set in the manager config file (see :ref:`config_configuration_files`).

The IPython kernel mode is enable using ``--use-ipython-kernel`` CLI parameter,  ``QSERVER_USE_IPYTHON_KERNEL``
environment variable or ``use_ipython_kernel`` parameter in the manager config file. IPython mode is
disabled by default. If IPython mode is disabled, the remaining parameters from the group are ignored.

The following rules apply when IPython mode is enabled:

- Default behavior. If no connection parameters are specified, IPython kernel is created with random ports.
  The kernel IP address is set to ``localhost``, random port numbers are assigned to kernel 0MQ ports and
  a new connection file with unique name is created in the default directory.

- Kernel IP address may be set using ``--ipython-kernel-ip`` CLI parameter. If the parameter value is
  ``localhost`` or ``127.0.0.1``, the kernel can not be accessed from remote machines. If the value
  is ``auto``, the worker attempts to find network IP address of the host running the worker. In rare cases
  when automatic detection fails, the IP address may be explicitly specified.

- Kernel 0MQ port numbers may be explicitly assigned using the following parameters:
  ``--ipython-shell-port``, ``--ipython-iopub-port``, ``--ipython-stdin-port``, ``--ipython-hb-port``,
  ``--ipython-control-port``. Those parameters are optional. Random port numbers are generated for
  unassigned ports.

- If the default location used by the kernel to store connection files is not desirable, the directory
  may be set using ``--ipython-connection-dir`` CLI parameter.

- In many cases, it is desirable to reuse the same connection file for new instances of the kernel.
  Using the same connection parameters, including the ``key`` parameter (UUID), allows the clients
  to reconnect to the new kernel automatically after the environment is restarted. The connection
  file name may be set using ``--ipython-connection-file`` CLI parameter. If the connection file
  does not exist, a new file is created. If the connection file exists, the kernel loads connection
  parameters from the file. If any of the loaded connection parameters do not match the parameters
  in the manager configuration (e.g. one of the 0MQ port numbers is different), the existing connection
  file is updated with the new parameters and the new ``key`` (UUID) is generated.

.. note::

  The connection parameters in manager configuration override the parameters in IPython kernel config
  files, such as ``ipython_kernel_config.py``.
