import inspect
import json
import logging

logger = logging.getLogger(__name__)


class JSONRPCResponseManager:
    def __init__(self, *, use_json=True):
        """
        Simplified implementation of message handler for JSON RPC protocol.
        Written as a replacement for ``json-rpc`` package and supports all features
        used by Queue Server. This implementation also supports binary messages
        (not encoded as JSON), which significatly speeds up interprocess communication.

        Parameters
        ----------
        use_json: boolean
            If the parameter is *True*, then incoming messages (passed to the ``handle``
            method) are expected to be in JSON format. Otherwise, the messages are expected
            to be dictionary or a list of dictionaries and no decoding is applied.
        """
        self._methods = {}
        self._use_json = use_json

    def add_method(self, handler, method):
        """
        Register a handler.

        Parameters
        ----------
        handler: Callable
            Method handler.
        method: str
            Method name. JSON messages must call one of the registered methods.

        Returns
        -------
        None
        """
        self._methods[method] = handler

    def _decode(self, msg):
        """
        Decode the incoming message from JSON (if ``use_json`` is *True*) or return
        the message unchanged.

        Parameters
        ----------
        msg: str, dict or list(dict)
            Encoded message in JSON format (*str*) or unencoded message (*dict* or
            *list(dict)*).

        Returns
        -------
        dict or list(dict)
            Decoded message.
        """
        if self._use_json:
            return json.loads(msg)
        else:
            return msg

    def _encode(self, msg):
        """
        Encode the response message to JSON (if ``use_json`` is *True*) or return
        the message unchanged.

        Parameters
        ----------
        msg: dict or list(dict)
            A single message (*dict*) or a batch of messages (*list(dict)*).

        Returns
        -------
        str, dict or list(dict)
            Encoded response message in JSON format (*str*) or original representation
            (*dict* or *list(dict)*).
        """
        if self._use_json:
            return json.dumps(msg)
        else:
            return msg

    def _get_error_msg(self, error_code):
        """
        Returns a standard JSON RPC message based on the error code.

        Parameters
        ----------
        error_code: int
            One of the standard JSON RPC error codes.

        Returns
        -------
        str
            Standard message based on ``error_code``.
        """
        msgs = {
            -32700: "Parse error",
            -32600: "Invalid request",
            -32601: "Method not found",
            -32602: "Invalid params",
            -32603: "Internal error",
            -32000: "Server error",
        }
        return msgs.get(error_code, "Unknown error")

    def _handle_single_msg(self, msg):
        """
        Handle a single JSON RPC message.

        Parameters
        ----------
        msg: dict
            Decoded JSON RPC message.

        Returns
        -------
        response: dict
            Response message
        """
        error_code, is_notification, msg_id, response = 0, "id" not in msg, None, None

        try:
            if not isinstance(msg, dict) or "method" not in msg or "jsonrpc" not in msg or msg["jsonrpc"] != "2.0":
                error_code = -32600
                raise TypeError(f"Invalid message format: {msg!r}")

            method = msg["method"]
            params = msg.get("params", {})
            msg_id = msg.get("id", None)

            if not isinstance(params, (tuple, list, dict)):
                error_code = -32602
                raise TypeError(f"Invalid params in the message {msg!r}")

            handler = self._methods.get(method)
            if handler is None:
                error_code = -32601
                raise TypeError(f"Unknown method: {method}")

            try:
                if isinstance(params, dict):
                    inspect.getcallargs(handler, **params)
                else:
                    inspect.getcallargs(handler, *params)
            except Exception as ex:
                error_code = -32602
                raise TypeError(f"Invalid params in the message {msg!r}: {ex}") from ex

            try:
                if isinstance(params, dict):
                    result = handler(**params)
                else:  # Tuple or list
                    result = handler(*params)
                if not is_notification:
                    response = {"jsonrpc": "2.0", "id": msg_id, "result": result}
            except Exception:
                error_code = -32000
                raise

        except Exception as ex:
            if not is_notification:
                data = {"type": ex.__class__.__name__, "message": str(ex)}
                error = {"code": error_code, "message": self._get_error_msg(error_code), "data": data}
                response = {"jsonrpc": "2.0", "id": msg_id, "error": error}

        return response

    def handle(self, msg_full):
        """
        Handle JSON RPC message. The message can contain a single message (*dict*) or a batch of messages
        (*list(dict)*). Messages in the batch are executed one by one. The response is also a single
        message if input message is *dict* or a batch of messages if the input message is *list(dict)*.

        If the response value returned by the function is *None*, it should not be sent to client.
        It happens when the input message is a notification ('id' is missing) or all messages in
        the batch are notifications. Responses to notifications are not included in the batch of
        the response messages, so the response batch may contain less messages than the input batch.

        If an input message can not be decoded (invalid JSON), then the response has 'id' set to *None*.

        Parameters
        ----------
        msg_full: str, dict or list(dict)
            Input message encoded as JSON (*str*) or not encoded (single message represented as *dict* or
            a batch of messages represented as *list(dict)*). The constructor parameter ``use_json`` must
            be *True* to use JSON encoding and *False* otherwise.

        Response
        --------
        str, dict, list(dict) or None
            Output message or a batch of messages in the same format as ``msg_full``. The response
            is *None* if there is nothing to send back to the client.
        """
        response, is_batch = [], False

        try:
            try:
                msg_full = self._decode(msg_full)
            except Exception as ex:
                raise TypeError(f"Failed to parse the message '{msg_full}': {ex}") from ex

            is_batch = isinstance(msg_full, list)
            if not is_batch:
                msg_full = [msg_full]

            for msg in msg_full:
                single_response = self._handle_single_msg(msg)
                if single_response:
                    response.append(single_response)

            if not response:
                response = None
            elif not is_batch:
                response = response[0]

            if response:
                response = self._encode(response)
        except Exception as ex:
            data = {"type": ex.__class__.__name__, "message": str(ex)}
            response = {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32700, "message": self._get_error_msg(-32700), "data": data},
            }
            response = self._encode(response)

        return response
