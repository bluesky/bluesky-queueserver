import threading
import pprint
import json
from jsonrpc import JSONRPCResponseManager
from jsonrpc.dispatcher import Dispatcher

import logging
logger = logging.getLogger(__name__)


class PipeJsonRpcReceive:
    """
    The class contains functions for receiving and processing JSON RPC messages received on
    communication pipe.

    Parameters
    ----------
    conn: multiprocessing.Connection
        Reference to bidirectional end of a pipe (multiprocessing.Pipe)

    Examples
    --------

    .. code-block:: python

        conn1, conn2 = multiprocessing.Pipe()
        pc = PipeJsonRPC(conn=conn1)

        def func():
            print("Testing")

        pc.add_handler(func, "some_method")
        pc.start()   # Wait and process commands
        # The function 'func' is called when the message with method=="some_method" is received
        pc.stop()  # Stop before exit to stop the thread.
    """
    def __init__(self, conn):
        self._conn = conn
        self._dispatcher = Dispatcher()  # json-rpc dispatcher
        self._stop_thread = False  # Set True to exit the thread

        self._conn_polling_timeout = 0.1  # in sec.

    def start(self):
        """
        Start processing of the pipe messages
        """
        self._start_conn_thread()

    def stop(self):
        """
        Stop processing of the pipe messages (and exit the tread)
        """
        self._stop_thread = True

    def __del__(self):
        self.stop()

    def add_method(self, handler, name=None):
        """
        Add method to json-rpc dispatcher.

        Parameters
        ----------
        handler: callable
            Reference to a handler
        name: str, optional
            Name to register (default is the handler name)
        """
        # Add method to json-rpc dispatcher
        self._dispatcher.add_method(handler, name)

    def _start_conn_thread(self):
        self._thread_conn = threading.Thread(target=self._receive_conn_thread,
                                             name="RE Watchdog Comm",
                                             daemon=True)
        self._thread_conn.start()

    def _receive_conn_thread(self):
        while True:
            if self._conn.poll(self._conn_polling_timeout):
                try:
                    msg = self._conn.recv()
                    # Messages should be handled in the event loop
                    self._conn_received(msg)
                except Exception as ex:
                    logger.exception("Exception occurred while waiting for "
                                     "RE Manager-> Watchdog message: %s", str(ex))
                    break
                if self._stop_thread:  # Exit thread
                    break

    def _conn_received(self, msg):

        if logger.level < 11:  # Print output only if logging level is DEBUG (10) or less
            msg_json = json.loads(msg)
            # We don't want to print 'heartbeat' messages
            if not isinstance(msg_json, dict) or (msg_json["method"] != "heartbeat"):
                logger.debug("Command received RE Manager->Watchdog: %s", pprint.pformat(msg_json))

        response = JSONRPCResponseManager.handle(msg, self._dispatcher)
        if response:
            response = response.json
            self._conn.send(response)
