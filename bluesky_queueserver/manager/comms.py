import threading
import pprint
import json
import asyncio
import uuid
import zmq
import zmq.asyncio
from jsonrpc import JSONRPCResponseManager
from jsonrpc.dispatcher import Dispatcher

import logging

logger = logging.getLogger(__name__)

_fixed_public_key = "wt8[6a8eoXFRVL<l2JBbOzs(hcI%kRBIr0Do/eLC"
_fixed_private_key = "=@e7WwVuz{*eGcnv{AL@x2hmX!z^)wP3vKsQ{S7s"


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

            # Clear the pipe from outdated unprocessed messages.
            while self._conn.poll():
                self._conn.recv()

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


# =========================================================================================
#                      Generation and validation of ZMQ key pairs


def generate_new_zmq_key_pair():
    """
    Generates new public-private key pair for ZMQ encryption.

    Returns
    -------
    str, str
        Public (first) and private (second) keys.
    """
    key_public, key_private = zmq.curve_keypair()
    return key_public.decode("utf-8"), key_private.decode("utf-8")


def generate_zmq_public_key(key_private):
    """
    Generates public key based on private key for ZMQ encryption.

    Returns
    -------
    str
        Public key.
    """
    key_public = zmq.curve_public(key_private.encode("utf-8"))
    return key_public.decode("utf-8")


def validate_zmq_key(key):
    """
    Validates format of a public or private key by feeding it to the function that generates
    public key from a private key. The function will raise an exception if the key is improperly
    formatted. The key is expected to be encoded using z85 (https://rfc.zeromq.org/spec/32/)
    and represented as a 40 character string.

    Parameters
    ----------
    key: str
        public or private key.

    Raises
    ------
    ValueError
        raised in case the key is not valid.
    """
    try:
        generate_zmq_public_key(key)
    except Exception:
        raise ValueError(f"Invalid key '{key}': the key must be a 40 byte z85 encoded string")


# =========================================================================================
#                                    ZMQ communication


class ZMQCommSendThreads:
    """
    Thread-based API for communication with RE Manager via ZMQ.

    Parameters
    ----------
    zmq_server_address : str or None
        Address of ZMQ server. If None, then the default address is ``tcp://localhost:60615``
        is used.
    timeout_recv : int
        Timeout (in ms) for ZMQ receive operations.
    timeout_send : int
        Timeout (in ms) for ZMQ send operations.
    raise_exceptions : bool
        Tells if exceptions should be raised in case of communication errors (mostly timeouts)
        when ``send_message()`` is called in blocking mode. This setting can be overridden by
        specifying ``raise_exceptions`` parameter of ``send_message()`` function. The exception
        ``CommTimeoutError`` is raised if the parameter is ``True``, otherwise error message
        is returned by ``send_message()``.
    server_public_key : str or None
        Server public key (z85-encoded 40 character string). The valid public key from the server
        public/private key pair must be passed if encryption is enabled at the 0MQ server side.
        Communication requests will time out if the key is invalid. Exception will be raised if
        the key is improperly formatted. Encryption will be disabled if ``None`` is passed.

    Examples
    --------

    .. code-block:: python

        zmq_comm = ZMQCommSendThreads()

        # Blocking call
        try:
            msg = zmq_comm.send_message(method="some_method", params={"some_value": n})
            # Code that uses msg
        except CommTimeoutError as ex:
            logger.exception("Exception occurred: %s", str(ex)

        # Non-blocking call (trivial example)
        msg_received, msg_err_received = [], []

        def cb(msg, msg_err):
            # msg - dict of parameters ({} if communication failed,
            # msg_err - string ("" if communication is successful)

            # In QT application, 'cb' would typically send a signal. There should be
            #   very limited amount of processing done in the callback, since it would
            #   block ZMQ communication.

            msg_received.append(msg)
            msg_err_received.append(msg_err)

        zmq_comm.send_message(method="some_method", params={"some_value": n}, cb=cb)

        # Wait for the message.
        while not msg_received:
            time.sleep(0.01)

        # Code to process received 'msg'
    """

    def __init__(
        self,
        *,
        zmq_server_address=None,
        timeout_recv=2000,
        timeout_send=500,
        raise_exceptions=True,
        server_public_key=None,
    ):

        if server_public_key is not None:
            validate_zmq_key(server_public_key)

        zmq_server_address = zmq_server_address or "tcp://localhost:60615"
        self._server_public_key = server_public_key

        self._timeout_receive = timeout_recv  # Timeout for 'recv' operation (ms)
        self._timeout_send = timeout_send  # # Timeout for 'send' operation (ms)
        self._raise_exceptions = raise_exceptions

        # ZeroMQ communication
        self._ctx = zmq.Context()
        self._zmq_socket = None
        self._zmq_server_address = zmq_server_address

        self._zmq_socket_open()
        self._lock_zmq = threading.Lock()

        self._blocking_call = False
        self._event_wait_for_msg = threading.Event()
        self._event_msg_received = threading.Event()

        self._zmq_error_occurred = False  # If True, then restart the socket
        self._cb = None  # Reference to callback function (used by 'send_message()')
        self._received_data = {}  # Keeps received data (used by 'send_message()')
        self._polling_timeout = 100  # Timeout used for polling the socket
        self._thread_name = "QServer ZMQ API"
        self._zmq_receive_thread = None  # Ref. to 'threading.Thread' object
        self._thread_running = False  # True - thread is running. Set False to exit the thread.

        # Start the thread. The thread will poll the socket for incoming data.
        self._start_receive_thread()

    def __del__(self):
        self.close()

    def _receive_thread(self):
        """
        Function that is executed in the 'receive' thread.
        """
        while True:
            if self._event_wait_for_msg.wait(timeout=self._polling_timeout / 1000):
                msg, msg_err = {}, ""
                try:
                    if self._zmq_socket.poll(timeout=self._timeout_receive):
                        msg = self._zmq_socket.recv_json()
                    else:
                        # This is very likely a timeout (RE Manager is not responding)
                        raise Exception("timeout occurred")
                except Exception as ex:
                    msg_err = f"ZeroMQ communication failed: {str(ex)}"
                    self._zmq_error_occurred = True

                if self._cb:
                    try:
                        self._cb(msg, msg_err)
                    except Exception as ex:
                        # Operation should continue even if there are problems with callback
                        logger.exception("Exception occurred in ZMQ communication callback: %s", str(ex))

                self._event_wait_for_msg.clear()
                if self._blocking_call:
                    self._event_msg_received.set()
                else:
                    # Release the lock here if the call is not blocking.
                    self._lock_zmq.release()

            if not self._thread_running:  # Exit thread
                break

    def _start_receive_thread(self):
        """
        Start the receive thread
        """
        # Initializations
        self._cb = None
        self._event_msg_received.clear()
        self._event_wait_for_msg.clear()

        # Start 'receive' thread
        if not self._thread_running:
            self._thread_running = True
            self._zmq_receive_thread = threading.Thread(
                target=self._receive_thread, name=self._thread_name, daemon=True
            )
            self._zmq_receive_thread.start()

    def _zmq_receive_blocking_cb(self, msg, msg_err):
        """
        Callback function used when ``send_message()`` is called in blocking mode.
        This function can also be used as an example of a user-defined callback function
        in case of non-blocking calls.

        Parameters
        ----------
        msg: dict
            Dictionary of parameters returned by the server.
        msg_err: str
            Error message: ``""`` if communication was successful, error message
            if communication error (most likely timeout) occurred.
        """
        self._received_data = {"msg": msg, "msg_err": msg_err}
        self._event_msg_received.set()

    def _zmq_communicate(self, msg_out, *, cb):
        """
        Send message to the server and receive the response.

        Parameters
        ----------
        msg_out: dict
            Dictionary that contains outgoing message (method name and parameters).
        cb: callable or None
            Reference to callback function. If callback function is specified, then
            the `_zmq_communicate` sends outgoing message and exits. Callback function
            is called once the response is received from the server or timeout occurs.

        Returns
        -------
        msg: dict or None
            Returns dictionary with parameters returned by the server or communication
            error message if the function is called in blocking mode (no callback is
            specified). In non-blocking mode the function always returns ``None``.
        """

        # Blocking attempt to acquire the lock.
        self._lock_zmq.acquire()

        # Initializations
        self._event_msg_received.clear()
        self._event_wait_for_msg.clear()
        self._received_data = {}

        # Restart the socket
        if self._zmq_error_occurred:
            self._zmq_socket_restart()
            self._zmq_error_occurred = False

        if cb is None:
            self._cb = self._zmq_receive_blocking_cb
            self._blocking_call = True
        else:
            self._cb = cb
            self._blocking_call = False

        self._zmq_socket.send_json(msg_out)
        self._event_wait_for_msg.set()

        if self._blocking_call:
            self._event_msg_received.wait()
            self._event_msg_received.clear()

            msg, msg_err = {}, ""
            if self._received_data:
                msg, msg_err = self._received_data["msg"], self._received_data["msg_err"]
            else:
                msg_err = "No received data was found"

            # Release the lock here if the call is blocking. If the call is not blocking
            #   then release the lock in the communication thread.
            self._lock_zmq.release()

            if msg_err:
                raise CommTimeoutError(msg_err)

            return msg
        else:
            # There is nothing to return if the call is not blocking.
            return None

    def _zmq_socket_open(self):
        """
        Open ZMQ socket.
        """
        self._zmq_socket = self._ctx.socket(zmq.REQ)

        if self._server_public_key:
            # Set server public key
            self._zmq_socket.set(zmq.CURVE_SERVERKEY, self._server_public_key.encode("utf-8"))
            # Set public and private keys for the client
            self._zmq_socket.set(zmq.CURVE_PUBLICKEY, _fixed_public_key.encode("utf-8"))
            self._zmq_socket.set(zmq.CURVE_SECRETKEY, _fixed_private_key.encode("utf-8"))

        # Increment `self._timeout_receive` so that timeout supplied to `self._zmq_socket.poll`
        #   expires first so that correct message is produced.
        self._zmq_socket.RCVTIMEO = self._timeout_receive + 1
        self._zmq_socket.SNDTIMEO = self._timeout_send
        # Clear the buffer quickly after the socket is closed
        self._zmq_socket.setsockopt(zmq.LINGER, 100)

        # Successful connection does not mean that the socket exists
        self._zmq_socket.connect(self._zmq_server_address)

        logger.info("Connected to ZeroMQ server '%s'" % str(self._zmq_server_address))
        logger.info("ZMQ encryption: %s", "disabled" if self._server_public_key is None else "enabled")

    def _zmq_socket_restart(self):
        """
        Restart (close and open the socket). Should be called in case of communication
        error (e.g. the server did not return the response).
        """
        self._zmq_socket.close()
        self._zmq_socket_open()

    def _create_msg(self, *, method, params=None):
        """
        Format the message.

        Parameters
        ----------
        method: str
            Method name
        params: dict or None
            Dictionary of parameters or None if no parameters should be supplied for the method.
        """
        return {"method": method, "params": params}

    def send_message(self, *, method, params=None, cb=None, raise_exceptions=None):
        """
        Send message to ZMQ server and wait for the response. The message must contain
        a name of a method supported by the server and a dictionary of parameters that
        are required by the method. In case of communication error (timeout), the function
        returns error message or raises ``CommTimeoutError`` exception depending on
        the values ``raise_exceptions`` parameter in this function and class constructor.

        Parameters
        ----------
        method: str
            Name of the method to be invoked on the server. The method must be supported
            by the server.
        params: dict or None
            Dictionary of parameters passed to the method. If ``None``, then an empty dictionary
            is passed to the server.
        cb: callable or None
            Callback function. If None, then the function blocks until communication is complete.
            Otherwise the function exits after the message is sent to the server. The callback
            is called when the response is received or timeout occurs.
            Function signature is ``cb(msg: dict, msg_err: str)``. Here ``msg`` is the dictionary
            of the parameters returned by the server and ``msg_err`` is ``""`` if communication
            was successful and contains error message in case communication failed (timeout).
        raise_exceptions: bool or None
            The flag indicates if exception should be raised in case of communication error
            (such as timeout). If ``False``, then error message is returned instead.
            If None, then the field value ``self._raise_timeout_exceptions`` is used to
            determine if the exception needs to be raised. Non-blocking calls do not raise
            the exception. Instead, callback function receives the error message as one of the
            parameters

        Returns
        -------
        dict
            Message returned by the server.

        Raises
        ------
        CommTimeoutError
            Raised if communication error occurs and ``raise_exceptions`` is set ``True``.
        """

        # Send empty dictionary if no parameters are passed
        params = params or {}

        try:
            msg_out = self._create_msg(method=method, params=params)
            msg_in = self._zmq_communicate(msg_out, cb=cb)
        except Exception as ex:
            errmsg = f"ZMQ communication error: {str(ex)}"
            use_ex = raise_exceptions if (raise_exceptions is not None) else self._raise_exceptions
            if use_ex:
                raise CommTimeoutError(errmsg)
            msg_in = {"success": False, "msg": errmsg}

        return msg_in

    def close(self):
        """
        Close ZMQ socket. Call to close socket if the object is no longer needed, but may
        not be destroyed for some time.
        """
        self._zmq_socket.close()
        self._thread_running = False


class ZMQCommSendAsync:
    """
    API for communication with RE Manager via ZMQ. The object has to be created
    from the running even loop or the loop has to be passed as a parameter during
    initialization.

    Parameters
    ----------
    loop : asyncio loop
        Current event loop
    zmq_server_address : str or None
        Address of ZMQ server. If None, then the default address is ``tcp://localhost:60615``
        is used.
    timeout_recv : int
        Timeout (in ms) for ZMQ receive operations.
    timeout_send: int
        Timeout (in ms) for ZMQ send operations.
    raise_exceptions : bool
        Tells if exceptions should be raised in case of communication errors (mostly timeouts)
        when ``send_message()`` is awaited. This setting can be overridden by
        specifying ``raise_exceptions`` parameter of ``send_message()`` function. The exception
        ``CommTimeoutError`` is raised if the parameter is ``True``, otherwise error message
        is returned by ``send_message()``.
    server_public_key : str or None
        Server public key (z85-encoded 40 character string). The valid public key from the server
        public/private key pair must be passed if encryption is enabled at the 0MQ server side.
        Communication requests will time out if the key is invalid. Exception will be raised if
        the key is improperly formatted. Encryption will be disabled if ``None`` is passed.

    Examples
    --------

    .. code-block:: python

        async def communicate():
            zmq_comm = ZMQCommSendAsync()
            for n in range(10):
                msg = await zmq_comm.send_message(method="some_method", params={"some_value": n})
                print(f"msg={msg}")
            zmq_comm.close()

        asyncio.run(communicate())

    """

    def __init__(
        self,
        *,
        loop=None,
        zmq_server_address=None,
        timeout_recv=2000,
        timeout_send=500,
        raise_exceptions=True,
        server_public_key=None,
    ):
        self._loop = loop if loop else asyncio.get_event_loop()

        zmq_server_address = zmq_server_address or "tcp://localhost:60615"
        self._server_public_key = server_public_key

        self._timeout_receive = timeout_recv  # Timeout for 'recv' operation (ms)
        self._timeout_send = timeout_send  # # Timeout for 'send' operation (ms)
        self._raise_exceptions = raise_exceptions

        # ZeroMQ communication
        self._ctx = zmq.asyncio.Context()
        self._zmq_socket = None
        self._zmq_server_address = zmq_server_address

        if self._server_public_key is not None:
            validate_zmq_key(self._server_public_key)

        self._zmq_socket_open()
        self._lock_zmq = asyncio.Lock()

    def __del__(self):
        self.close()

    def get_loop(self):
        """
        Returns the asyncio event loop.
        """
        return self._loop

    async def _zmq_send(self, msg):
        await self._zmq_socket.send_json(msg)

    async def _zmq_receive(self):
        try:
            if await self._zmq_socket.poll(timeout=self._timeout_receive):
                msg = await self._zmq_socket.recv_json()
            else:
                # This is very likely a timeout (RE Manager is not responding)
                raise Exception("timeout occurred")
        except Exception as ex:
            # Timeout occurred. Socket needs to be reset.
            logger.exception("ZeroMQ communication failed: %s" % str(ex))
            raise
        return msg

    async def _zmq_communicate(self, msg_out):
        await self._zmq_send(msg_out)
        msg_in = await self._zmq_receive()
        return msg_in

    def _zmq_socket_open(self):
        self._zmq_socket = self._ctx.socket(zmq.REQ)

        if self._server_public_key:
            # Set server public key
            self._zmq_socket.set(zmq.CURVE_SERVERKEY, self._server_public_key.encode("utf-8"))
            # Set public and private keys for the client
            self._zmq_socket.set(zmq.CURVE_PUBLICKEY, _fixed_public_key.encode("utf-8"))
            self._zmq_socket.set(zmq.CURVE_SECRETKEY, _fixed_private_key.encode("utf-8"))

        # Increment `self._timeout_receive` so that timeout supplied to `self._zmq_socket.poll`
        #   expires first so that correct message is produced.
        self._zmq_socket.RCVTIMEO = self._timeout_receive + 1
        self._zmq_socket.SNDTIMEO = self._timeout_send
        # Clear the buffer quickly after the socket is closed
        self._zmq_socket.setsockopt(zmq.LINGER, 100)

        # Successful connection does not mean that the socket exists
        self._zmq_socket.connect(self._zmq_server_address)

        logger.info("Connected to ZeroMQ server '%s'" % str(self._zmq_server_address))
        logger.info("ZMQ encryption: %s", "disabled" if self._server_public_key is None else "enabled")

    def _zmq_socket_restart(self):
        self._zmq_socket.close()
        self._zmq_socket_open()

    def _create_msg(self, *, method, params=None):
        return {"method": method, "params": params}

    async def send_message(self, *, method, params=None, raise_exceptions=None):
        """
        Send message to ZMQ server and wait for the response. The message must contain
        a name of a method supported by the server and a dictionary of parameters that
        are required by the method. In case of communication error (timeout), the function
        returns error message or raises ``CommTimeoutError`` exception depending on
        the values ``raise_exceptions`` parameter in this function and class constructor.

        Parameters
        ----------
        method: str
            Name of the method to be invoked on the server. The method must be supported
            by the server.
        params: dict or None
            Dictionary of parameters passed to the method. If ``None``, then an empty dictionary
            is passed to the server.
        raise_exceptions: bool or None
            The flag indicates if exception should be raised in case of communication error
            (such as timeout). If ``False``, then error message is returned instead.
            If None, then the field value ``self._raise_exceptions`` is used to
            determine if the exception needs to be raised. Non-blocking calls do not raise
            the exception. Instead, callback function receives the error message as one of the
            parameters

        Returns
        -------
        dict
            Message returned by the server.

        Raises
        ------
        CommTimeoutError
            Raised if communication error occurs and ``raise_timeout_exceptions`` is set ``True``.
        """

        # Send empty dictionary if no parameters are passed
        params = params or {}

        async with self._lock_zmq:
            try:
                msg_out = self._create_msg(method=method, params=params)
                msg_in = await self._zmq_communicate(msg_out)
            except Exception as ex:
                # This is very likely a timeout (RE Manager is not responding)
                self._zmq_socket_restart()
                errmsg = f"ZMQ communication error: {str(ex)}"
                use_ex = raise_exceptions if (raise_exceptions is not None) else self._raise_exceptions
                if use_ex:
                    raise CommTimeoutError(errmsg)
                msg_in = {"success": False, "msg": errmsg}
            return msg_in

    def close(self):
        """
        Close ZMQ socket. Call to close socket if the object is no longer needed, but may
        not be destroyed for some time.
        """
        if self._zmq_socket:
            self._zmq_socket.close()


def zmq_single_request(method, params=None, *, zmq_server_address=None, server_public_key=None):
    """
    Send a single request to ZMQ server. The function opens the socket, sends
    a single ZMQ request and closes the socket. The function is not expected
    to raise exceptions. In case of communication error the return value
    of ``msg`` is ``None`` and ``err_msg`` contains the error message. Otherwise
    ``err_msg`` is empty and ``msg`` contains the dictionary returned by the server.

    Parameters
    ----------
    method: str
        Name of the method called in RE Manager
    params: dict or None
        Dictionary of parameters (payload of the message). If ``None`` then
        the message is sent with empty payload: ``params = {}``.
    server_public_key: str or None
        Server public key (z85-encoded 40 character string). The Valid public key from the server
        public/private key pair must be passed if encryption is enabled at the 0MQ server side.
        Communication requests will time out if the key is invalid. Exception will be raised if
        the key is improperly formatted. Encryption will be disabled if ``None`` is passed.


    Returns
    -------
    msg: dict or None
        Message received from RE Manager in response to the request. None if communication
        error (timeout) occurred.
    err_msg: str
        Contains a message in case communication error (timeout) occurs. Empty string otherwise.
    """

    msg_received = None

    async def send_request(method, params):

        nonlocal msg_received
        zmq_to_manager = ZMQCommSendAsync(
            zmq_server_address=zmq_server_address, server_public_key=server_public_key
        )
        msg_received = await zmq_to_manager.send_message(method=method, params=params, raise_exceptions=True)
        zmq_to_manager.close()

    try:
        asyncio.run(send_request(method, params))

        msg = msg_received
        msg_err = ""
    except Exception as ex:
        msg = None
        msg_err = str(ex)

    if msg_err:
        logger.warning("Communication with RE Manager failed: %s", str(msg_err))

    return msg, msg_err
