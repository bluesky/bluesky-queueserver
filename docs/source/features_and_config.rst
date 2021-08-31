==========================
Features and Configuration
==========================

Documentation is coming soon ...

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