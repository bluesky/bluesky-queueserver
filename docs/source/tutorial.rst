===============
Tutorial (Demo)
===============

Documentation is coming soon ...

Starting the Queue Server
-------------------------

Adding Items to Queue
---------------------

Opening and Closing RE Worker Environment
-----------------------------------------

Starting and Stopping the Queue
-------------------------------

Running RE Manager with Custom Startup Code
-------------------------------------------

.. _remote_monitoring_tutorial:

Remote Monitoring of RE Manager Console Output
----------------------------------------------

Queue Server provides a simple ``qserver-console-monitor`` CLI tool for remote
monitoring of console output of RE Manager. The tool subscribes to messages
published by RE Manager over 0MQ and displays text contents of the messages. The
output of ``qserver-console-monitor`` is expected to be identical to the output
of RE Manager. There is also an option to disable printing of console output
RE Manager and use the external monitoring application for visualizing of
RE Manager output.

In Terminal 1 start ``qserver-console-monitor``::

  qserver-console-monitor

In Terminal 2 start RE Manager with console output publishing available::

  start-re-manager --zmq-publish-console ON

Use Terminal 3 to run some commands using ``qserver`` tool. Terminals 1 and 2
must display identical output. Multiple instances of ``qserver-console-monitor``
may be running simultaneously and display the same console output.
Experiment with closing (Ctrl-C) and restarting ``qserver-console-monitor``.
Notice that all published console output is lost while the monitor is closed.

In Terminal 2, close RE Manager (Ctrl C) and restart it with the option that
disables printing of the console output::

  start-re-manager --zmq-publish-console ON --console-output OFF

Notice that no output is printed in Terminal2. External monitor (running in
Terminal 1) is needed to visualize the output from RE Manager.

In practice, the client applications are expected to implement the
functionality for subscribing to published RE Manager output and displaying
it to users. The use of ``qserver-console-monitor`` tool should be limited to
evaluation, testing and debugging of the systems using RE Manager.
