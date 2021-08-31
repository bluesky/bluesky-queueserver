=============================
Interacting with Queue Server
=============================

.. currentmodule:: bluesky_queueserver

Documentation is coming soon ...

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

The ``bluesky-queueserver`` package provides convenience API class (``ReceiveConsoleOutput``)
that allows to subscribe to RE Manager socket and contains ``recv`` method (blocking with timeout)
for reading published messages. See the docstring for the class for the description of the
class parameters and code example.

.. autosummary::
   :nosignatures:
   :toctree: generated

    ReceiveConsoleOutput
    ReceiveConsoleOutput.recv

Asyncio-based applications (e.g. HTTP server) may use ``ReceiveConsoleOutputAsync`` API class to
receive captured console output:

.. autosummary::
   :nosignatures:
   :toctree: generated

    ReceiveConsoleOutputAsync
    ReceiveConsoleOutputAsync.set_callback
    ReceiveConsoleOutputAsync.recv
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
