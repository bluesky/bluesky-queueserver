import threading
import pprint
import json
import asyncio
import uuid
from jsonrpc import JSONRPCResponseManager
from jsonrpc.dispatcher import Dispatcher

import logging

logger = logging.getLogger(__name__)


class CommTimeoutError(TimeoutError):
    """
    Raised when communication error occurs
    """

    pass


class CommJsonRpcError(RuntimeError):
    """
    Raised when returned json-rpc message contains error

    Parameters
    ----------
    message: str
        Error message
    error_code: int
        Error code (returned by `json-rpc`)
    error_type: str
        Error type (returned by `json-rpc` or set to `'CommJsonRpcError'`)
    """

    def __init__(self, message, error_code, error_type):
        super().__init__(message)
        # TODO: change 'code' and 'type' to read-only properties
        self.__error_code = error_code
        self.__error_type = error_type

    @property
    def error_code(self):
        return self.__error_code

    @property
    def error_type(self):
        return self.__error_type

    @property
    def message(self):
        return super().__str__()

    def __str__(self):
        msg = super().__str__() + f"\nError code: {self.error_code}. Error type: {self.error_type}"
        return msg

    def __repr__(self):
        return f"CommJsonRpcError('{self.message}', {self.error_code}, '{self.error_type}')"


def format_jsonrpc_msg(method, params=None, *, notification=False):
    """
    Returns dictionary that contains JSON RPC message.

    Parameters
    ----------
    method: str
        Method name
    params: dict or list, optional
        List of args or dictionary of kwargs.
    notification: boolean
        If the message is notification, no response will be expected.
    """
    msg = {"method": method, "jsonrpc": "2.0"}
    if params is not None:
        msg["params"] = params
    if not notification:
        msg["id"] = str(uuid.uuid4())
    return msg


class PipeJsonRpcReceive:
    """
    The class contains functions for receiving and processing JSON RPC messages received on
    communication pipe.

    Parameters
    ----------
    conn: multiprocessing.Connection
        Reference to bidirectional end of a pipe (multiprocessing.Pipe)
    name: str
        Name of the receiving thread (it is better to assign meaningful unique names to threads.

    Examples
    --------

    .. code-block:: python

        conn1, conn2 = multiprocessing.Pipe()
        pc = PipeJsonRPC(conn=conn1, name="RE QServer Receive")

        def func():
            print("Testing")

        pc.add_handler(func, "some_method")
        pc.start()   # Wait and process commands
        # The function 'func' is called when the message with method=="some_method" is received
        pc.stop()  # Stop before exit to stop the thread.
    """

    def __init__(self, conn, *, name="RE QServer Comm"):
        self._conn = conn
        self._dispatcher = Dispatcher()  # json-rpc dispatcher
        self._thread_running = False  # Set True to exit the thread

        self._thread_name = name

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
        self._thread_running = False

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
        if not self._thread_running:
            self._thread_running = True
            self._thread_conn = threading.Thread(
                target=self._receive_conn_thread, name=self._thread_name, daemon=True
            )
            self._thread_conn.start()

    def _receive_conn_thread(self):
        while True:
            if self._conn.poll(self._conn_polling_timeout):
                try:
                    msg = self._conn.recv()
                    # Messages should be handled in the event loop
                    self._conn_received(msg)
                except Exception as ex:
                    logger.exception(
                        "Exception occurred while waiting for RE Manager-> Watchdog message: %s", str(ex)
                    )
                    break
            if not self._thread_running:  # Exit thread
                break

    def _conn_received(self, msg):

        # if logger.level < 11:  # Print output only if logging level is DEBUG (10) or less
        #     msg_json = json.loads(msg)
        #     We don't want to print 'heartbeat' messages
        #     if not isinstance(msg_json, dict) or (msg_json["method"] != "heartbeat"):
        #         logger.debug("Command received RE Manager->Watchdog: %s", pprint.pformat(msg_json))

        response = JSONRPCResponseManager.handle(msg, self._dispatcher)
        if response:
            response = response.json
            self._conn.send(response)


class PipeJsonRpcSendAsync:
    """
    The class contains functions for supporting asyncio-based client for JSON RPC comminucation
    using interprocess communication pipe. The class object must be created on the loop (from one of
    `async` functions). This implementation allows calls only to one method at a time. Multiple
    `send_msg` requests may be put on the loop, but the next message is never sent before
    the response to the previous message is received or timeout occurred.

    Parameters
    ----------
    conn: multiprocessing.Connection
        Reference to bidirectional end of a pipe (multiprocessing.Pipe)
    timeout: float
        Default value of timeout: maximum time to wait for response after a message is sent
    name: str
        Name of the receiving thread (it is better to assign meaningful unique names to threads.

    Examples
    --------

    .. code-block:: python

        conn1, conn2 = multiprocessing.Pipe()

        async def send_messages():
            # Must be instantiated and used within the loop
            p_send = PipeJsonRpcSendAsync(conn=conn1, name="comm-client")
            p_send.start()

            method = "method_name"
            params = {"value": 10}   #   or list of args [10, 25]
            response = await p_send.send_msg(method, params, notification=notification)

            p_send.stop()

        asyncio.run(send_messages())
        pc.stop()


        pc = PipeJsonRpcSendAsync(conn=conn1, name="RE QServer Receive")

        def func():
            print("Testing")

        pc.add_handler(func, "some_method")
        pc.start()   # Wait and process commands
        # The function 'func' is called when the message with method=="some_method" is received
        pc.stop()  # Stop before exit to stop the thread.

    """

    def __init__(self, conn, *, timeout=0.5, name="RE QServer Comm"):
        self._conn = conn
        self._loop = asyncio.get_running_loop()

        self._thread_name = name

        self._fut_comm = None  # Future for waiting for messages from watchdog
        # Lock that prevents sending of the next message before response
        #   to the previous message is received.
        self._lock_comm = asyncio.Lock()
        self._timeout_comm = timeout  # Timeout (time to wait for response to a message)

        # Polling timeout for the pipe. The data will be read from the pipe instantly once it is available.
        #   The timeout determines how long it would take to stop the thread when needed.
        self._conn_polling_timeout = 0.1

        self._thread_running = False  # True - thread is running

        # Expected ID of the received message. The ID must be the same as the ID of the sent message.
        #   Ignore all message that don't have matching ID or no ID.
        self._expected_msg_id = None

    def start(self):
        """
        Start processing of the pipe messages
        """
        self._start_conn_thread()

    def stop(self):
        """
        Stop processing of the pipe messages (and exit the tread)
        """
        self._thread_running = False

    def __del__(self):
        self.stop()

    def _start_conn_thread(self):
        # Start 'receive' thread
        if not self._thread_running:
            self._thread_running = True
            self._pipe_receive_thread = threading.Thread(
                target=self._pipe_receive, name=self._thread_name, daemon=True
            )
            self._pipe_receive_thread.start()

    async def send_msg(self, method, params=None, *, notification=False, timeout=None):
        """
        Send JSON RPC message to server and return the result of the function (method)
        or raise exception in case of an error. Returns None if the message is notification.

        Parameters
        ----------
        method: str
            name of JSON RPC method
        params: list or dict
            args or kwargs of the remote method
        notification: boolean
            True - message is notification. The function returns immediately without
            waiting of the response, which is never generated for notification.
        timeout: float
            Timeout in seconds. If no response is received at expiration of timeout,
            `CommTimeoutError` is raised. If the response will be received later, it
            will be ignored.

        Raises
        ------
        CommTimeoutError
            Timeout occurred. Response is not received in time
        CommJsonRpcError
            Error occurred while processing the message. This could indicate an error
            in `json-rpc` package (e.g. method not found) or exception raised by
            the method itself. It is recommended that the methods catch and process
            their exceptions (may be except parameter validation) and leave
            `CommJsonRpcError` for reporting `json-rpc` errors. In well tested
            program this exception should never be raised.
        RuntimeError
            Unrecognized message received (message doesn't contain `result` or `error`
            keys. This should never happen in well tested program.

        The function will raise `CommTimeoutError` in case of communication timeout
        """
        # The lock protects from sending the next message
        #   before response to the previous message is received.

        if timeout is None:
            timeout = self._timeout_comm

        async with self._lock_comm:
            msg = format_jsonrpc_msg(method, params, notification=notification)
            try:
                msg_json = json.dumps(msg)
                self._conn.send(msg_json)

                # No response is expected if this is a notification
                if not notification:
                    self._expected_msg_id = msg["id"]
                    self._fut_comm = self._loop.create_future()
                    # Waiting for the future may raise 'asyncio.TimeoutError'
                    await asyncio.wait_for(self._fut_comm, timeout=timeout)
                    response = self._fut_comm.result()

                    if "result" in response:
                        return response["result"]
                    elif "error" in response:
                        # TODO: verify that this is all information that should be saved
                        err_code = response["error"]["code"]
                        if "data" in response["error"]:
                            # Server Error (issue with execution of the method)
                            err_type = response["error"]["data"]["type"]
                            # Message: "Server error: <message text>"
                            err_msg = f'{response["error"]["message"]}: {response["error"]["data"]["message"]}'
                        else:
                            # Other json-rpc errors
                            err_type = "CommJsonRpcError"
                            err_msg = response["error"]["message"]
                        raise CommJsonRpcError(err_msg, error_code=err_code, error_type=err_type)
                    else:
                        err_msg = (
                            f"Message {pprint.pformat(msg)}\n"
                            f"resulted in response with unknown format: {pprint.pformat(response)}"
                        )
                        raise RuntimeError(err_msg)
                else:
                    response = None
                return response

            except asyncio.TimeoutError:
                raise CommTimeoutError(f"Timeout while waiting for response to message: \n{pprint.pformat(msg)}")
            finally:
                self._fut_comm = None
                self._expected_msg_id = None

    async def _response_received(self, response):
        """
        Set the future with the results. Ignore all messages with unexpected or missing IDs.
        Also ignore all unexpected messages.
        """
        if self._expected_msg_id is not None:
            if "id" in response:
                if response["id"] != self._expected_msg_id:
                    # Incorrect ID: ignore the message.
                    logger.error(
                        "Received response with incorrect message ID: %s. Expected %s.\nMessage: %s",
                        response["id"],
                        self._expected_msg_id,
                        pprint.pformat(response),
                    )
                else:
                    # Accept the message. Otherwise wait for timeout
                    self._fut_comm.set_result(response)
            else:
                # Missing ID: ignore the message
                logger.error("Received response with missing message ID: %s", pprint.pformat(response))
        else:
            logger.error(
                "Unsolicited message received: %s. Message is ignored",
                pprint.pformat(response),
            )

    def _conn_received(self, response):
        asyncio.create_task(self._response_received(response))

    def _pipe_receive(self):
        while True:
            if self._conn.poll(self._conn_polling_timeout):
                try:
                    msg_json = self._conn.recv()
                    msg = json.loads(msg_json)
                    # logger.debug("Message Watchdog->Manager received: '%s'", pprint.pformat(msg))
                    # Messages should be handled in the event loop
                    self._loop.call_soon_threadsafe(self._conn_received, msg)
                except Exception as ex:
                    logger.exception("Exception occurred while waiting for packet: %s", str(ex))
                    break
            if not self._thread_running:  # Exit thread
                break
