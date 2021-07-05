import argparse
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
    def __init__(self, *, msg_queue):
        """
        Parameters
        ----------
        msg_queue : multiprocessing.Queue
            Queue that is used to collect messages from processes
        """
        super().__init__()
        self._msg_queue = msg_queue
        self._stdout = sys.__stdout__

    def write(self, s):
        s = str(s)
        msg = {"time": ttime.time(), "msg": s}
        self._msg_queue.put(msg)
        return len(s)


def redirect_output_streams(file_obj):
    sys.stdout = file_obj
    sys.stderr = file_obj


def setup_console_output_redirection(msg_queue):
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
    zmq_publishing_on : boolean
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
        name="Output Publisher",
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
                    "Failed to create 0MQ socket at %s. Console output will not be published. " "Exception: %s",
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

        if self._zmq_publish_on and self._socket:
            topic = self._zmq_topic
            payload_json = json.dumps(payload)
            self._socket.send_multipart([topic.encode("ascii"), payload_json.encode("utf8")])


class ReceiveConsoleOutput:
    """
    The class allows to subscribing to published 0MQ messages and reading the message one by
    one as they arrive. Subscription is performed based on remote 0MQ address and topic.

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
            self._socket.subscribe("QS_Console")

    def recv(self, timeout=-1):
        """
        Get the next published message. If timeout expires then
        ``TimeoutError`` is raised.

        Parameters
        ----------
        timeout : int, float or None
            Timeout for the receive operation in milliseconds. If timeout is
            a negative number (default) the use timeout passed to the class
            constructor. If `None`, then wait indefinitely.

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


def qserver_console_monitor_cli():
    """
    CLI tool for remote monitoring of console output from RE Manager. The function is also
    expected to be used as an example of using  ``ReceiveConsoleOutput`` class.
    """
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("bluesky_queueserver").setLevel("INFO")

    parser = argparse.ArgumentParser(
        description="Queue Server Console Monitor: CLI tool for remote monitoring of console output "
        "published by RE Manager.",
        epilog=f"Bluesky-QServer version {qserver_version}.",
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
