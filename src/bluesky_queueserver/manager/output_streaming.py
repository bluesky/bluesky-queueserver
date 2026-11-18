import argparse
import asyncio
import inspect
import io
import json
import logging
import os
import sys
import threading
import time as ttime

import msgpack
import zmq

import bluesky_queueserver

from .comms import ZMQEncoding, process_zmq_encoding_name

logger = logging.getLogger(__name__)
qserver_version = bluesky_queueserver.__version__

default_zmq_info_address_for_server = "tcp://*:60625"
default_zmq_info_address = "tcp://localhost:60625"


class ConsoleOutputStream(io.TextIOBase):
    """
    Class that implements writable text file object that collects printed console messages
    and adds timestamps to messages and adds the message to the queue. The messages are
    dictionaries in the form ``{"time": <timestamp>, "msg": <printed text>}.

    Parameters
    ----------
    msg_queue : multiprocessing.Queue
        Reference to the queue used for collecting messages.
    """

    def __init__(self, *, msg_queue):
        super().__init__()
        self._msg_queue = msg_queue
        self._stdout = sys.__stdout__

    def write(self, s):
        """
        Overrides the method of ``io.TextIOBase``.
        """
        s = str(s)

        msg = {"channel": "console", "time": ttime.time(), "msg": s}
        self._msg_queue.put(msg)
        return len(s)


def redirect_output_streams(file_obj):
    """
    Override the default output streams with custom file object.
    The object may be an instance of ``ConsoleOutputStream``.

    Parameters
    ----------
    file_obj : ConsoleOutputStream
        Reference for the open writable file object (text output).
    """
    sys.stdout = file_obj
    sys.stderr = file_obj


def setup_console_output_redirection(msg_queue):
    """
    Set up redirection of console output. If ``msg_queue`` is ``None``, then do nothing.

    Parameters
    ----------
    msg_queue : multiprocessing.Queue
        Queue that is used to collect console output messages.
    """
    if msg_queue:
        fobj = ConsoleOutputStream(msg_queue=msg_queue)
        redirect_output_streams(fobj)

        # Disable 'colorama' (used by Bluesky). We don't need it in Queue Server.
        #   Colorama overrides 'sys.stdout' and interferes with capturing console output.
        def do_nothing(*args, **kwargs): ...

        try:
            import colorama

            colorama.init = do_nothing
            colorama.reinit = do_nothing
        except Exception:
            pass


def push_info_to_msg_queue(*, key, msg, msg_queue):
    """
    Format the message and put it into the message queue. The message is published to
    the ``info`` socket in the ``"info"`` channel.

    Parameters
    ----------
    key : str
        The key used for the message. The message is formatted as a dictionary:
        ``{"channel": "info", "time": ttime.time(), "msg": {key: msg}}``.
        The key is used to identify the information in the message. E.g. ``"status"``.
    msg : dict
        The message is a dictionary with useful information (actual payload).
    msg_queue : multiprocessing.Queue
        Reference to the queue used for collecting messages.

    Returns
    -------
    None
    """
    msg = {"channel": "info", "time": ttime.time(), "msg": {key: msg}}
    msg_queue.put(msg)


_log_message_key = "log_message"


class LogStreamHandler(logging.Handler):
    """
    Logging handler that publishes log records to the ``info`` channel under
    the ``"log_message"`` key. The message is formatted as
    ``{"name": <logger name>, "level": <level name>, "message": <text>, "time": <timestamp>,
    "module": <module>, "lineno": <line number>}``.

    Parameters
    ----------
    msg_queue : multiprocessing.Queue
        Reference to the queue used for collecting messages.
    """

    def __init__(self, *, msg_queue):
        super().__init__()
        self._msg_queue = msg_queue

    def emit(self, record):
        try:
            msg = {
                "name": record.name,
                "level": record.levelname,
                "message": self.format(record),
                "time": record.created,
                "module": record.module,
                "lineno": record.lineno,
            }
            push_info_to_msg_queue(key=_log_message_key, msg=msg, msg_queue=self._msg_queue)
        except Exception:
            self.handleError(record)


def setup_log_streaming(*, loggers, msg_queue):
    """
    Attach ``LogStreamHandler`` to the selected loggers. Logger levels are not changed.

    Parameters
    ----------
    loggers : list(str)
        Names of the loggers. Messages of child loggers are also streamed.
    msg_queue : multiprocessing.Queue
        Reference to the queue used for collecting messages. Nothing is done if ``None``.
    """
    if not loggers or not msg_queue:
        return

    handler = LogStreamHandler(msg_queue=msg_queue)
    for name in set(loggers):
        # Children of selected loggers are already covered via propagation
        if not any(name.startswith(_ + ".") for _ in loggers):
            logging.getLogger(name).addHandler(handler)


_default_zmq_console_topic = "QS_Console"
_default_zmq_info_topic = "QS_Info"


class PublishZMQStreamOutput:
    """
    The class that is publishing the collected console output messages to 0MQ socket.
    The queue is expected to be filled with messages in the format
    ``{"time": <timestamp>, "msg": <text message>}``. The object of the class
    receives the reference to the queue during initialization. The collected messages
    are published as they are added to the queue. The messages may be collected
    in multiple processes.

    Parameters
    ----------
    msg_queue : multiprocessing.Queue
        Reference to the queue object, used for collecting of the output messages.
        The messages added to the queue will be automatically published to 0MQ socket.
    console_output_on : boolean
        Enable/disable printing console output to the terminal
    zmq_publish_console : boolean
        Enable/disable publishing console output to 0MQ socket (``QS_Console`` topic).
    zmq_publish_info : boolean
        Enable/disable publishing info/status messages to 0MQ socket (``QS_Info`` topic).
    zmq_publish_addr : str, None
        Address of 0MQ PUB socket for the publishing server. If ``None``, then
        the default address ``tcp://*:60625`` is used.
    encoding : str or ZMQEncoding
        Encoding of used for 0MQ messages. Supported values: "json" or "msgpack".
    zmq_topic_console : str
        Name of the 0MQ topic where the console messages are published.
    zmq_topic_info : str
        Name of the 0MQ topic where the system information messages are published.
    name : str
        Name of the thread where the messages are published.
    """

    def __init__(
        self,
        *,
        msg_queue,
        console_output_on=True,
        zmq_publish_console=True,
        zmq_publish_info=True,
        zmq_publish_addr=None,
        encoding="json",
        zmq_topic_console=_default_zmq_console_topic,
        zmq_topic_info=_default_zmq_info_topic,
        name="RE Console Output Publisher",
    ):
        self._thread_running = False  # Set True to exit the thread
        self._thread_name = name
        self._msg_queue = msg_queue
        self._polling_timeout = 0.1  # in sec.

        self._console_output_on = console_output_on
        self._zmq_publish_console = zmq_publish_console
        self._zmq_publish_info = zmq_publish_info

        self._encoding = process_zmq_encoding_name(encoding)

        zmq_publish_addr = zmq_publish_addr or default_zmq_info_address_for_server

        self._zmq_publish_addr = zmq_publish_addr
        self._zmq_topic_console = zmq_topic_console
        self._zmq_topic_info = zmq_topic_info

        zmq_publish_on = zmq_publish_console or zmq_publish_info

        self._context = None
        self._socket = None

        if zmq_publish_on:
            try:
                self._context = zmq.Context()
                self._socket = self._context.socket(zmq.PUB)
                self._socket.bind(self._zmq_publish_addr)
            except Exception as ex:
                logger.error(
                    "Failed to create 0MQ socket at %s. Output will not be published. Exception: %s",
                    self._zmq_publish_addr,
                    ex,
                )

        if self._socket and zmq_publish_on:
            logging.info("Publishing output to 0MQ socket at %s", zmq_publish_addr)

    def start(self):
        """
        Start thread polling the queue.
        """
        self._start_processing_thread()

    def stop(self):
        """
        Stop thread that polls the queue (and exit the tread)
        """
        self._thread_running = False

    def __del__(self):
        self.stop()
        if self._socket:
            self._socket.close()
        if self._context:
            self._context.term()

    def _start_processing_thread(self):
        # The thread should not be started of Message Queue object does not exist
        if not self._thread_running and self._msg_queue:
            self._thread_running = True
            self._thread_conn = threading.Thread(
                target=self._publishing_thread, name=self._thread_name, daemon=True
            )
            self._thread_conn.start()

    def _publishing_thread(self):
        while True:
            try:
                msg = self._msg_queue.get(block=True, timeout=self._polling_timeout)
                self._publish(msg)
            except Exception:
                pass

            if not self._thread_running:  # Exit thread
                break

    def _publish(self, payload):
        channel = payload["channel"]
        if self._console_output_on and channel == "console":
            sys.__stdout__.write(payload["msg"])
            sys.__stdout__.flush()

        if self._socket:
            if channel == "console" and self._zmq_publish_console:
                topic = self._zmq_topic_console
            elif channel == "info" and self._zmq_publish_info:
                topic = self._zmq_topic_info
            else:
                return
            payload = {k: payload[k] for k in ("time", "msg")}
            if self._encoding == ZMQEncoding.JSON:
                payload_json = json.dumps(payload)
                self._socket.send_multipart([topic.encode("ascii"), payload_json.encode("utf8")])
            else:
                payload_pickle = msgpack.packb(payload)
                self._socket.send_multipart([topic.encode("ascii"), payload_pickle])


class _ReceiveZMQStreamOutput:
    """
    The class allows to subscribe to published 0MQ messages and read the messages one by
    one as they arrive. Subscription is performed using the remote 0MQ address and topic.

    The class provides blocking (with timeout) ``recv()`` method that waits for the next
    published message. The following example contains the code illustrating using the class.
    In real-world  application the loop will be running in a separate thread and generating
    callbacks on each received message.

    The ``subscribe()`` and ``unsubscribe()`` methods allow to explicitly subscribe and
    unsubscribe the socket to the topic. The messages published while the socket is unsubscribed
    are discarded. First call to ``recv()`` method automatically subscribes the socket.

    The following code is an example of using the ``ReceiveConsoleOutput`` subclass,
    which is configured to receive console output:

    .. code-block:: python

        from bluesky_queueserver import ReceiveConsoleOutput

        zmq_subscribe_addr = "tcp://localhost:60625"
        rco = ReceiveConsoleOutput(zmq_subscribe_addr=zmq_subscribe_addr)

        while True:
            try:
                payload = rco.recv()
                time, msg = payload.get("time", None), payload.get("msg", None)
                # In this example the messages are printed in the terminal.
                sys.stdout.write(msg)
                sys.stdout.flush()
            except TimeoutError:
                # Timeout does not mean communication error!!!
                # Insert the code that needs to be executed on timeout (if any).
                pass
            # Place for the code that should be executed after receiving each
            #   message or after timeout (e.g. check a condition and exit
            #   the loop once the condition is satisfied).


    Parameters
    ----------
    zmq_subscribe_addr : str or None
        Address of ZMQ server (PUB socket). If None, then the default address is
        ``tcp://localhost:60625`` is used.
    encoding : str or ZMQEncoding
        Encoding of used for 0MQ messages. Supported values: "json" or "msgpack".
    zmq_topic_console : str
        0MQ topic for console output. Only messages from this topic are going to be received.
    zmq_topic_info : str
        0MQ topic for info output. Only messages from this topic are going to be received.
    timeout : int, float or None
        Timeout for the receive operation in milliseconds. If `None`, then wait
        for the message indefinitely.
    """

    def __init__(self, *, zmq_subscribe_addr, encoding, zmq_topic, timeout):
        self._timeout = timeout  # Timeout for 'recv' operation (ms)

        zmq_subscribe_addr = zmq_subscribe_addr or default_zmq_info_address

        logger.info("Subscribing to console output stream from 0MQ address: %s ...", zmq_subscribe_addr)
        logger.info("Subscribing to 0MQ topic: '%s' ...", zmq_topic)
        self._zmq_subscribe_addr = zmq_subscribe_addr
        self._zmq_topic = zmq_topic

        self._encoding = process_zmq_encoding_name(encoding)

        self._context = None
        self._socket = None
        self._socket_subscribed = False

        if self._zmq_subscribe_addr:
            self._context = zmq.Context()
            self._socket = self._context.socket(zmq.SUB)
            self._socket.connect(self._zmq_subscribe_addr)

    def subscribe(self):
        """
        Subscribe 0MQ socket to the topic. Once the socket is subscribed,
        the published messages are cached by 0MQ and could be loaded with ``recv()`` method.
        The function does nothing if the socket is already subscribed.
        """
        if self._socket and not self._socket_subscribed:
            self._socket.subscribe(self._zmq_topic)
            self._socket_subscribed = True

    def unsubscribe(self):
        """
        Unsubscribe 0MQ socket from the topic. Once the socket is unsubscribed,
        all published messages are discarded.
        """
        if self._socket and self._socket_subscribed:
            self._socket.unsubscribe(self._zmq_topic)
            self._socket_subscribed = False

    def recv(self, timeout=-1):
        """
        Get the next published message. The function subscribes the socket to 0MQ topic
        if the socket is not already subscribed. If timeout expires then ``TimeoutError``
        is raised.

        Parameters
        ----------
        timeout : int, float or None
            Timeout for the receive operation in milliseconds. If timeout is
            a negative number (default), the timeout value passed to the class
            constructor is used. If `None`, then wait indefinitely.

        Returns
        -------
        dict
            Received message. The dictionary contains timestamp (``time`` key)
            and text message (``msg`` key).

        Raises
        ------
        TimeoutError
            Timeout occurred. Timeout does not indicate communication error.
        """

        if (timeout is not None) and (timeout < 0):
            timeout = self._timeout

        # Subscribe the socket to the topic if it is not already subscribed
        self.subscribe()

        if not self._socket.poll(timeout=timeout):
            raise TimeoutError("No message received during timeout period {timeout} ms")

        topic, payload_received = self._socket.recv_multipart()
        if self._encoding == ZMQEncoding.JSON:
            payload_json = payload_received.decode("utf8", "strict")
            payload = json.loads(payload_json)
        else:
            payload = msgpack.unpackb(payload_received)

        return payload

    def __del__(self):
        if self._socket:
            self._socket.close()
        if self._context:
            self._context.term()


class ReceiveConsoleOutput(_ReceiveZMQStreamOutput):
    """
    The class defaults are set to receive 0MQ messages with console output.
    """

    def __init__(
        self, *, zmq_subscribe_addr=None, encoding="json", zmq_topic=_default_zmq_console_topic, timeout=1000
    ):
        super().__init__(
            zmq_subscribe_addr=zmq_subscribe_addr,
            encoding=encoding,
            zmq_topic=zmq_topic,
            timeout=timeout,
        )


class ReceiveSystemInfo(_ReceiveZMQStreamOutput):
    """
    The class defaults are set to receive 0MQ messages with system information.
    """

    def __init__(
        self, *, zmq_subscribe_addr=None, encoding="json", zmq_topic=_default_zmq_info_topic, timeout=1000
    ):
        super().__init__(
            zmq_subscribe_addr=zmq_subscribe_addr,
            encoding=encoding,
            zmq_topic=zmq_topic,
            timeout=timeout,
        )


ReceiveConsoleOutput.__doc__ += _ReceiveZMQStreamOutput.__doc__
ReceiveSystemInfo.__doc__ += _ReceiveZMQStreamOutput.__doc__


class _ReceiveZMQStreamOutputAsync:
    """
    Async version of ``_ReceiveZMQStreamOutput`` class. There are two ways to use the class:
    explicitly awaiting for the ``recv`` function (same as in ``_ReceiveZMQStreamOutput``)
    or setting up a callback function (plain function or coroutine).

    The ``subscribe()`` and ``unsubscribe()`` methods allow to explicitly subscribe and
    unsubscribe the socket to the topic. The messages published while the socket is unsubscribed
    are discarded. Calls to ``recv()`` and ``start()`` methods always subscribe the socket,
    ``stop()`` method unsubscribes the socket unless called with ``unsubscribe=False``.

    Explicitly awaiting ``recv`` function (``ReceiveConsoleOutputAsync`` is a subclass):

    .. code-block:: python

        from bluesky_queueserver import ReceiveConsoleOutputAsync

        zmq_subscribe_addr = "tcp://localhost:60625"
        rco = ReceiveConsoleOutputAsync(zmq_subscribe_addr=zmq_subscribe_addr)

        async def run_acquisition():
            while True:
                try:
                    payload = await rco.recv()
                    time, msg = payload.get("time", None), payload.get("msg", None)
                    # In this example the messages are printed in the terminal.
                    sys.stdout.write(msg)
                    sys.stdout.flush()
                except TimeoutError:
                    # Timeout does not mean communication error!!!
                    # Insert the code that needs to be executed on timeout (if any).
                    pass
                # Place for the code that should be executed after receiving each
                #   message or after timeout (e.g. check a condition and exit
                #   the loop once the condition is satisfied).

        # Subscribe to start caching messages. Calling 'recv()' also subscribes the socket.
        rco.subscribe()

        asyncio.run(run_acquisition())

        # Unsubscribe to discard all new messages
        rco.unsubscribe()


    Setting up callback function or coroutine (awaitable function):

    .. code-block:: python

        from bluesky_queueserver import ReceiveConsoleOutputAsync

        zmq_subscribe_addr = "tcp://localhost:60625"
        rco = ReceiveConsoleOutputAsync(zmq_subscribe_addr=zmq_subscribe_addr)

        async def cb_coro(payload):
            time, msg = payload.get("time", None), payload.get("msg", None)
            # In this example the messages are printed in the terminal.
            sys.stdout.write(msg)
            sys.stdout.flush()

        rco.set_callback(cb_coro)

        async def run_acquisition():
            rco.start()
            # Do something useful here, e.g. sleep
            asyncio.sleep(60)
            rco.stop()

            # Acquisition can be started and stopped multiple time if necessary
            rco.start()
            asyncio.sleep(60)
            rco.stop()

        asyncio.run(run_acquisition())

    .. note::
        If callback is a plain function, it is executed immediately after the message is received
        and may potentially block the loop if it takes too long to complete (even occasionally).
        If the callback is a coroutine, it is not awaited, but instead placed in the loop
        (with ``ensure_future``), so acquisition of messages will continue. Typically the callback
        will do a simple operation such as adding the received message to the queue.

    Parameters
    ----------
    zmq_subscribe_addr : str or None
        Address of ZMQ server (PUB socket). If None, then the default address is
        ``tcp://localhost:60625`` is used.
    encoding : str or ZMQEncoding
        Encoding of used for 0MQ messages. Supported values: "json" or "msgpack".
    zmq_topic : str
        0MQ topic for console output. Only messages from this topic are going to be received.
    timeout : int, float or None
        Timeout for the receive operation in milliseconds. If `None`, then wait
        for the message indefinitely.
    """

    def __init__(self, *, zmq_subscribe_addr, encoding, zmq_topic, timeout):
        self._timeout = timeout  # Timeout for 'recv' operation (ms)

        zmq_subscribe_addr = zmq_subscribe_addr or "tcp://localhost:60625"

        self._callback = None  # Function that is awaited once a message is received from RE Manager
        self._exit = False
        self._is_running = False

        logger.info("Subscribing to console output stream from 0MQ address: %s ...", zmq_subscribe_addr)
        logger.info("Subscribing to 0MQ topic: '%s' ...", zmq_topic)
        self._zmq_subscribe_addr = zmq_subscribe_addr
        self._zmq_topic = zmq_topic

        self._encoding = process_zmq_encoding_name(encoding)

        self._context = None
        self._socket = None
        self._socket_subscribed = False
        self._unsubscribe_when_stopping = False

        if self._zmq_subscribe_addr:
            self._context = zmq.asyncio.Context()
            self._socket = self._context.socket(zmq.SUB)
            self._socket.connect(self._zmq_subscribe_addr)

    def subscribe(self):
        """
        Subscribe 0MQ socket to the topic. Once the socket is subscribed,
        the published messages are cached by 0MQ and could be loaded with ``recv()`` method.
        The function does nothing if the socket is already subscribed.
        """
        if self._socket and not self._socket_subscribed:
            self._socket.subscribe(self._zmq_topic)
            self._socket_subscribed = True

    def unsubscribe(self):
        """
        Unsubscribe 0MQ socket from the topic. Once the socket is unsubscribed,
        all published messages are discarded.
        """
        if self._socket and self._socket_subscribed:
            self._socket.unsubscribe(self._zmq_topic)
            self._socket_subscribed = False

    def set_callback(self, cb):
        """
        Set callback function, which is called once for each received message. If ``cb`` is
        a function, it is called immediately and execution of the loop is blocked until the
        execution of the function is complete. If ``cb`` is coroutine, it is not awaited, but
        instead placed in the loop using ``asyncio.ensure_future``. Only one callback function
        can be set.

        Parameters
        ----------
        cb : callable, coroutine or None
            Reference to a callback function or coroutine. The function signature is expected
            to receive a message as a parameter (message is a dictionary with keys ``time`` and ``msg``)
            and return ``None``. The function is expected to handle exceptions that are raised
            internally. Pass ``None`` to clear callback (messages will be received and discarded).
        """
        self._callback = cb

    async def recv(self, timeout=-1):
        """
        Get the next published message. If timeout expires then ``TimeoutError`` is raised.
        If the socket is not subscribed to to topic, then subscribes the socket.

        Parameters
        ----------
        timeout : int, float or None
            Timeout for the receive operation in milliseconds. If timeout is
            a negative number (default), the timeout value passed to the class
            constructor is used. If `None`, then wait indefinitely.

        Returns
        -------
        dict
            Received message. The dictionary contains timestamp (``time`` key)
            and text message (``msg`` key).

        Raises
        ------
        TimeoutError
            Timeout occurred. Timeout does not indicate communication error.
        """

        if (timeout is not None) and (timeout < 0):
            timeout = self._timeout

        # Subscribe the socket to the topic if it is not already subscribed
        self.subscribe()

        if not await self._socket.poll(timeout=timeout):
            raise TimeoutError("No message received during timeout period {timeout} ms")

        topic, payload_received = await self._socket.recv_multipart()
        if self._encoding == ZMQEncoding.JSON:
            payload_json = payload_received.decode("utf8", "strict")
            payload = json.loads(payload_json)
        else:
            payload = msgpack.unpackb(payload_received)

        return payload

    async def _recv_next_message(self):
        try:
            payload = await self.recv()
            if self._callback:
                if inspect.iscoroutinefunction(self._callback):
                    asyncio.ensure_future(self._callback(payload))
                else:
                    self._callback(payload)
        except TimeoutError:
            pass
        except Exception as ex:
            logger.exception(
                "Exception occurred while while waiting for RE Manager console output message: %s", ex
            )

        if not self._exit:
            asyncio.ensure_future(self._recv_next_message())
        else:
            if self._unsubscribe_when_stopping:
                self.unsubscribe()
            self._is_running = False

    def start(self):
        """
        Start collection of messages published by RE Manager. Collection may be started and stopped
        multiple times during a session. Repeated calls to the ``start`` method are ignored.
        The function MUST be called from the event loop. The method always subscribes the socket.
        """
        self._exit = False
        if not self._is_running:
            self._is_running = True
            self.subscribe()
            asyncio.ensure_future(self._recv_next_message())

    def stop(self, *, unsubscribe=True):
        """
        Stop collection of messages published by RE Manager. Call to ``stop`` method unsubscribes
        the client from 0MQ topic, therefore all the messages published until collection is started
        are ignored. The function MUST be called from the event loop.

        Parameters
        ----------
        unsubscribe: boolean (optional)
            Unsubscribe the socket if ``True`` (default), otherwise leave the socket subscribed.
        """
        self._unsubscribe_when_stopping = unsubscribe
        self._exit = True

    def __del__(self):
        self.stop()
        if self._socket:
            self._socket.close()
        if self._context:
            self._context.term()


class ReceiveConsoleOutputAsync(_ReceiveZMQStreamOutputAsync):
    """
    The class defaults are set to receive 0MQ messages with console output.
    """

    def __init__(
        self, *, zmq_subscribe_addr=None, encoding="json", zmq_topic=_default_zmq_console_topic, timeout=1000
    ):
        super().__init__(
            zmq_subscribe_addr=zmq_subscribe_addr,
            encoding=encoding,
            zmq_topic=zmq_topic,
            timeout=timeout,
        )


class ReceiveSystemInfoAsync(_ReceiveZMQStreamOutputAsync):
    """
    The class defaults are set to receive 0MQ messages with system information.
    """

    def __init__(
        self, *, zmq_subscribe_addr=None, encoding="json", zmq_topic=_default_zmq_info_topic, timeout=1000
    ):
        super().__init__(
            zmq_subscribe_addr=zmq_subscribe_addr,
            encoding=encoding,
            zmq_topic=zmq_topic,
            timeout=timeout,
        )


ReceiveConsoleOutputAsync.__doc__ += _ReceiveZMQStreamOutputAsync.__doc__
ReceiveSystemInfoAsync.__doc__ += _ReceiveZMQStreamOutputAsync.__doc__


def qserver_console_monitor_cli():
    """
    CLI tool for remote monitoring of console output from RE Manager. The function is also
    expected to be used as an example of using  ``ReceiveConsoleOutput`` class.
    """

    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("bluesky_queueserver").setLevel("INFO")

    def formatter(prog):
        # Set maximum width such that printed help mostly fits in the RTD theme code block (documentation).
        return argparse.RawDescriptionHelpFormatter(prog, max_help_position=20, width=90)

    parser = argparse.ArgumentParser(
        description="Queue Server Console Monitor:\nCLI tool for remote monitoring of console output "
        f"published by RE Manager.\nbluesky-queueserver version {qserver_version}\n",
        formatter_class=formatter,
    )
    parser.add_argument(
        "--zmq-info-addr",
        dest="zmq_info_addr",
        type=str,
        default=None,
        help="The address of RE Manager socket used for publishing console output, status info, and "
        "progress updates. The parameter overrides the address set using QSERVER_ZMQ_INFO_ADDRESS "
        "environment variable. The default value is used if the address is not set using the parameter "
        "or the environment variable. Address format: "
        f"'tcp://127.0.0.1:60625' (default: {default_zmq_info_address}).",
    )

    parser.add_argument(
        "--zmq-encoding",
        dest="zmq_encoding",
        type=str,
        default=None,
        help="The encoding used for 0MQ communication. The encoding must match the encoding used by RE Manager. "
        "The parameter value overrides the value set by QSERVER_ZMQ_ENCODING environment variable. "
        "The supported values: 'json' (default) or 'msgpack'.",
    )

    args = parser.parse_args()

    zmq_info_addr = args.zmq_info_addr
    zmq_info_addr = zmq_info_addr or os.environ.get("QSERVER_ZMQ_INFO_ADDRESS", None)
    zmq_info_addr = zmq_info_addr or default_zmq_info_address

    zmq_encoding = args.zmq_encoding
    zmq_encoding = zmq_encoding or os.environ.get("QSERVER_ZMQ_ENCODING", None)
    zmq_encoding = zmq_encoding or "json"

    try:
        rco = ReceiveConsoleOutput(zmq_subscribe_addr=zmq_info_addr, encoding=zmq_encoding)
        rco.subscribe()
        while True:
            try:
                payload = rco.recv()
                time, msg = payload.get("time", None), payload.get("msg", None)  # noqa: F841
                sys.stdout.write(msg)
                sys.stdout.flush()
            except TimeoutError:
                # Timeout does not mean communication error!!!
                # There is no need to use or process timeouts. This code
                #   serves mostly as an example of how to use it.
                pass
            # Place for the code that should be executed after receiving each
            #   message or after timeout. (E.g. the code may check some condition
            #   and exit the loop once the condition is fulfilled.)
        exit_code = 0  # The code is set if the loope is exited (which does not happen here)
    except BaseException as ex:
        logger.exception("Queue Server Console Monitor failed with exception: %s", str(ex))
        exit_code = 1
    return exit_code
