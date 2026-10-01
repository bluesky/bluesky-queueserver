=============================
Interacting with Queue Server
=============================

.. currentmodule:: bluesky_queueserver

.. _subscribing_to_console_output:

Subscribing to Published Console Output
---------------------------------------

If console output publishing is enabled at RE Manager (parameter ``--zmq-publish-console``), the output
is published to 0MQ socket. Client applications may subscribe to the messages and use them
for processing, display them to users or forward to other applications.

The messages are published to a ``PUB`` 0MQ socket running. Multiple applications may subscribe
to the socket simultaneously. The clients must be subscribed to the socket to receive the messages.
The messages are not delivered to the client if they are published while the client is
not subscribed to the socket. The messages are published using the topic named ``QS_Console``.

Each message contains timestamped string printed by RE Manager. Some strings can be empty or
contain multiple lines. Messages are python JSON-represented dictionaries with the following
format::

  {"time": <timestamp>, "msg": <message>}

``<timestamp>`` is floating point number (returned by ``time.time()``) and ``<message>`` is a string.

The ``bluesky-queueserver`` package provides convenience API class (``ReceiveConsoleOutput``) for
use in synchronous or thread-based applications. The class provides ``subscribe()`` and ``unsubscribe()``
methods that enable caching of published messages by 0MQ and blocking ``recv()`` method (with timeout)
for loading published messages. See the class documentation for detailed description of the methods
and code examples.

.. autosummary::
   :nosignatures:
   :toctree: generated

    ReceiveConsoleOutput
    ReceiveConsoleOutput.subscribe
    ReceiveConsoleOutput.unsubscribe
    ReceiveConsoleOutput.recv


Asyncio-based applications (e.g. HTTP server) may use ``ReceiveConsoleOutputAsync`` API class to
receive captured console output. The class provides ``subscribe()`` and ``unsubscribe()`` methods
to explicitly subscribe and unsubscribe 0MQ socket. The class may be used in two modes: polling
using ``recv()`` method or setting callback function or coroutine with ``set_callback()`` method
and starting and then stopping acquisition with ``start()`` and ``stop()`` methods.

.. autosummary::
   :nosignatures:
   :toctree: generated

    ReceiveConsoleOutputAsync
    ReceiveConsoleOutputAsync.subscribe
    ReceiveConsoleOutputAsync.unsubscribe
    ReceiveConsoleOutputAsync.recv
    ReceiveConsoleOutputAsync.set_callback
    ReceiveConsoleOutputAsync.start
    ReceiveConsoleOutputAsync.stop

.. _subscribing_to_system_info:

Subscribing to Published System Info
------------------------------------

In addition to streaming of console output, RE Manager is publishing additional information
for system monitoring on the same 0MQ PUB socket using the topic ``QS_info``. The published
information currently includes:

- RE Manager status;
- device progress updates.

Additional information may be added to the stream in the future. Clients are responsible for 
selecting messages using the key name. System info streaming is disabled by default and must 
be enabled via the ``--zmq-publish-info`` CLI parameter or the ``network/zmq_publish_info`` 
config file parameter, which enables streaming of default data such as status. 
Streaming of optional data can be enabled independently using dedicated parameters.

Status information
++++++++++++++++++

The information includes complete dictionary returned by ``status`` API. The messages are 
published once per second or each time status is changed by RE Manager. Periodically published 
status can be used as 'heartbeat' to detect that RE Manager is running properly.

The message format used for streaming is similar to the message format for console output.
The ``status`` key indicates that the message contains status information. Messages with
other information may be added to the stream in the future::

  {"time": <timestamp>, "msg": {"status": <status-info>}}

Device progress updates (optional)
++++++++++++++++++++++++++++++++++

While a plan is running, RE Manager also streams device progress updates (RunEngine
``waiting_hook`` / watcher updates, e.g. the position of a moving motor) on the same socket
under the ``device_progress`` key. Each update contains the device name, current/initial/target
values, engineering units, precision, completion fraction, elapsed/remaining time, and a ``done``
flag. A message with ``{"completed": true}`` is sent when the RunEngine finishes waiting::

  {"time": <timestamp>, "msg": {"device_progress": <progress-info>}}

Device progress streaming is disabled by default and can be enabled using the
``--zmq-stream-device-progress`` CLI parameter of ``start-re-manager`` or 
``network/zmq_stream_device_progress`` parameter in the config file.


RunEngine messages (optional)
+++++++++++++++++++++++++++++

While a plan is running, RE Manager may also stream serialized RunEngine messages (RunEngine
``msg_hook`` updates) on the same socket under the ``re_message`` key. Each update contains the
timestamp when the message was processed, the message ``command``, the name of the associated
object (``obj``), and the message ``args``, ``kwargs`` and ``run`` key (all serialized to
JSON-safe values)::

  {"time": <timestamp>, "msg": {"re_message": <message-info>}}

Message streaming is disabled by default and can be enabled using the
``--zmq-stream-re-messages`` CLI parameter of ``start-re-manager`` or 
``network/zmq_stream_re_messages`` parameter in the config file.


The ``ReceiveSystemInfo`` class can be used in synchronous or thread-based applications to
receive the streamed messages:

.. autosummary::
   :nosignatures:
   :toctree: generated

    ReceiveSystemInfo
    ReceiveSystemInfo.subscribe
    ReceiveSystemInfo.unsubscribe
    ReceiveSystemInfo.recv

``ReceiveSystemInfoAsync`` is the asyncio-based version of the class:

.. autosummary::
   :nosignatures:
   :toctree: generated

    ReceiveSystemInfoAsync
    ReceiveSystemInfoAsync.subscribe
    ReceiveSystemInfoAsync.unsubscribe
    ReceiveSystemInfoAsync.recv
    ReceiveSystemInfoAsync.set_callback
    ReceiveSystemInfoAsync.start
    ReceiveSystemInfoAsync.stop


Formatting Descriptions of Plans and Plan Parameters
----------------------------------------------------

``format_text_descriptions`` function may be used to generate formatted text descriptions of
plans and plan parameters. The formatted descriptions are intended to be displayed to users
by client applications. See the docstring for the function for more details.

.. autosummary::
   :nosignatures:
   :toctree: generated

    format_text_descriptions
