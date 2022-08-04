=============================
Interacting with Queue Server
=============================

.. currentmodule:: bluesky_queueserver

.. _subscribing_to_console_output:

Subscribing to Published Console Output
---------------------------------------

If console output publishing is enabled at RE Manager (parameter ``--zmq-publish``), the output
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


Formatting Descriptions of Plans and Plan Parameters
----------------------------------------------------

``format_text_descriptions`` function may be used to generate formatted text descriptions of
plans and plan parameters. The formatted descriptions are intended to be displayed to users
by client applications. See the docstring for the function for more details.

.. autosummary::
   :nosignatures:
   :toctree: generated

    format_text_descriptions
