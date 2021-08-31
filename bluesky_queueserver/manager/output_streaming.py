import argparse
import asyncio
import inspect
import io
import json
import sys
import time as ttime
import threading
import zmq

import bluesky_queueserver

import logging

logger = logging.getLogger(__name__)
qserver_version = bluesky_queueserver.__version__


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
        msg = {"time": ttime.time(), "msg": s}
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


_default_zmq_console_topic = "QS_Console"


class PublishConsoleOutput:
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
    zmq_publish_on : boolean
        Enable/disable publishing console output to 0MQ socket
    zmq_publish_addr : str, None
        Address of 0MQ PUB socket for the publishing server. If ``None``, then
        the default address ``tcp://*:60625`` is used.
    zmq_topic : str
        Name of the 0MQ topic where the messages are published.
    name : str
        Name of the thread where the messages are published.
    """

    def __init__(
        self,
        *,
        msg_queue,
        console_output_on=True,
        zmq_publish_on=True,
        zmq_publish_addr=None,
        zmq_topic=_default_zmq_console_topic,
        name="RE Console Output Publisher",
    ):
        self._thread_running = False  # Set True to exit the thread
        self._thread_name = name
        self._msg_queue = msg_queue
        self._polling_timeout = 0.1  # in sec.

        self._console_output_on = console_output_on
        self._zmq_publish_on = zmq_publish_on

        zmq_publish_addr = zmq_publish_addr or "tcp://*:60625"

        self._zmq_publish_addr = zmq_publish_addr
        self._zmq_topic = zmq_topic

        self._socket = None
        if self._zmq_publish_on:
            try:
                context = zmq.Context()
                self._socket = context.socket(zmq.PUB)
                self._socket.bind(self._zmq_publish_addr)
            except Exception as ex:
                logger.error(
                    "Failed to create 0MQ socket at %s. Console output will not be published. Exception: %s",
                    self._zmq_publish_addr,
                    str(ex),
                )

        if self._socket and self._zmq_publish_on:
            logging.info("Publishing console output to 0MQ socket at %s", zmq_publish_addr)

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
        if self._console_output_on:
            sys.__stdout__.write(payload["msg"])
            sys.__stdout__.flush()

        if self._zmq_publish_on and self._socket:
            topic = self._zmq_topic
            payload_json = json.dumps(payload)
            self._socket.send_multipart([topic.encode("ascii"), payload_json.encode("utf8")])


class ReceiveConsoleOutput:
    """
    The class allows to subscribe to published 0MQ messages and read the messages one by
    one as they arrive. Subscription is performed using the remote 0MQ address and topic.

    The class provides blocking (with timeout) ``recv`` method that waits for the next
    published message. The following example contains the code illustrating using the class.
    In real-world  application the loop will be running in a separate thread and generating
    callbacks on each received message.

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
    zmq_topic : str
        0MQ topic for console output. Only messages from this topic are going to be received.
    timeout : int, float or None
        Timeout for the receive operation in milliseconds. If `None`, then wait
        for the message indefinitely.
    """

    def __init__(self, *, zmq_subscribe_addr=None, zmq_topic=_default_zmq_console_topic, timeout=1000):

        self._timeout = timeout  # Timeout for 'recv' operation (ms)

        zmq_subscribe_addr = zmq_subscribe_addr or "tcp://localhost:60625"

        logger.info(f"Subscribing to console output stream from 0MQ address: {zmq_subscribe_addr} ...")
        logger.info(f"Subscribing to 0MQ topic: '{zmq_topic}' ...")
        self._zmq_subscribe_addr = zmq_subscribe_addr
        self._zmq_topic = zmq_topic

        self._socket = None
        if self._zmq_subscribe_addr:
            context = zmq.Context()
            self._socket = context.socket(zmq.SUB)
            self._socket.connect(self._zmq_subscribe_addr)
            self._socket.subscribe(self._zmq_topic)

    def recv(self, timeout=-1):
        """
        Get the next published message. If timeout expires then
        ``TimeoutError`` is raised.

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

        if not self._socket.poll(timeout=timeout):
            raise TimeoutError("No message received during timeout period {timeout} ms")

        topic, payload_json = self._socket.recv_multipart()
        payload_json = payload_json.decode("utf8", "strict")
        payload = json.loads(payload_json)
        return payload

    def __del__(self):
        self._socket.close()


class ReceiveConsoleOutputAsync:
    """
    Async version of ``ReceiveConsoleOutput`` class. There are two ways to use the class:
    explicitly awaiting for the ``recv`` function (same as in ``ReceiveConsoleOutput``)
    or setting up a callback function (plain function or coroutine).

    Explicitly awaiting ``recv`` function:

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

        asyncio.run(run_acquisition())

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
    zmq_topic : str
        0MQ topic for console output. Only messages from this topic are going to be received.
    timeout : int, float or None
        Timeout for the receive operation in milliseconds. If `None`, then wait
        for the message indefinitely.
    """

    def __init__(self, *, zmq_subscribe_addr=None, zmq_topic=_default_zmq_console_topic, timeout=1000):

        self._timeout = timeout  # Timeout for 'recv' operation (ms)

        zmq_subscribe_addr = zmq_subscribe_addr or "tcp://localhost:60625"

        self._callback = None  # Function that is awaited once a message is received from RE Manager
        self._exit = False
        self._is_running = False

        logger.info(f"Subscribing to console output stream from 0MQ address: {zmq_subscribe_addr} ...")
        logger.info(f"Subscribing to 0MQ topic: '{zmq_topic}' ...")
        self._zmq_subscribe_addr = zmq_subscribe_addr
        self._zmq_topic = zmq_topic

        self._socket = None
        if self._zmq_subscribe_addr:
            context = zmq.asyncio.Context()
            self._socket = context.socket(zmq.SUB)
            self._socket.connect(self._zmq_subscribe_addr)

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

        if not await self._socket.poll(timeout=timeout):
            raise TimeoutError("No message received during timeout period {timeout} ms")

        topic, payload_json = await self._socket.recv_multipart()

        payload_json = payload_json.decode("utf8", "strict")
        payload = json.loads(payload_json)
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
            logger.exception(f"Exception occurred while while waiting for RE Manager console output message: {ex}")

        if not self._exit:
            asyncio.ensure_future(self._recv_next_message())
        else:
            self._socket.unsubscribe(self._zmq_topic)
            self._is_running = False

    def start(self):
        """
        Start collection of messages published by RE Manager. Collection may be started and stopped
        multiple times during a session. Repeated calls to the ``start`` method are ignored.
        The function MUST be called from the event loop.
        """
        self._exit = False
        if not self._is_running:
            self._is_running = True
            self._socket.subscribe(self._zmq_topic)
            asyncio.ensure_future(self._recv_next_message())

    def stop(self):
        """
        Stop collection of messages published by RE Manager. Call to ``stop`` method unsubscribes
        the client from 0MQ topic, therefore all the messages published until collection is started
        are ignored. The function MUST be called from the event loop.
        """
        self._exit = True

    def __del__(self):
        self.stop()
        if self._socket:
            self._socket.close()


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
        "--zmq-subscribe-addr",
        dest="zmq_subscribe_addr",
        type=str,
        default="tcp://localhost:60625",
        help="The address of ZMQ server to subscribe, e.g. 'tcp://127.0.0.1:60625' (default: %(default)s).",
    )

    args = parser.parse_args()
    zmq_subscribe_addr = args.zmq_subscribe_addr

    try:
        rco = ReceiveConsoleOutput(zmq_subscribe_addr=zmq_subscribe_addr)
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
