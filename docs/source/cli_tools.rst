==================
Command-Line Tools
==================

start-re-manager
----------------

Documentation is coming soon ...


qserver
-------

Documentation is coming soon ...


.. _qserver_list_plans_devices_cli:

qserver-list-plans-devices
--------------------------

Documentation is coming soon ...


qserver-zmq-keys
----------------

Documentation is coming soon ...

.. _qserver_console_monitor:

qserver-console-monitor
-----------------------

``qserver-console-monitor`` is a simple application that subscribes to the console output (``stdout`` and
``stderr``) published by RE Manager via 0MQ and prints the received messages to terminal (to ``stdout``).
The console output printed by the monitor is expected to be identical to the output printed in
RE Manager terminal. The monitor may be run on the same workstation as RE Manager or any computer,
which can access the workstation running RE Manager over the network. If the address of
the 0MQ socket is different from default, it can be passed to the monitor application
as a parameter (``--zmq-subscribe-addr``). RE Manager does not publishing the console output
to 0MQ socket by default. Publishing can be enabled by starting RE Manager with the parameter
``--zmq-publish``:

.. code-block::

    start-re-manager --zmq-publish ON

Start the monitor with the parameter ``-h`` to display help:

.. code-block::

    $ qserver-console-monitor -h
    usage: qserver-console-monitor [-h] [--zmq-subscribe-addr ZMQ_SUBSCRIBE_ADDR]

    Queue Server Console Monitor: CLI tool for remote monitoring of console output
    published by RE Manager.

    optional arguments:
      -h, --help            show this help message and exit
      --zmq-subscribe-addr ZMQ_SUBSCRIBE_ADDR
                            The address of ZMQ server to subscribe, e.g.
                            'tcp://127.0.0.1:60625' (default:
                            tcp://localhost:60625).
