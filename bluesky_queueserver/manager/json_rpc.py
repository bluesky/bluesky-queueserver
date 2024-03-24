import inspect
import json
import logging

logger = logging.getLogger(__name__)


class JSONRPCResponseManager:
    def __init__(self, *, use_json=True):
        self._methods = {}
        self._use_json = use_json

    def add_method(self, handler, method):
        self._methods[method] = handler

    def _decode(self, msg):
        if self._use_json:
            return json.loads(msg)
        else:
            return msg

    def _encode(self, msg):
        if self._use_json:
            return json.dumps(msg)
        else:
            return msg

    def _get_error_msg(self, error_code):
        msgs = {
            -32700: "Parse error",
            -32600: "Invalid Request",
            -32601: "Method not found",
            -32602: "Invalid params",
            -32603: "Internal error",
            -32000: "Server error",
        }
        return msgs.get(error_code, "Unknown error")

    def _handle_single_msg(self, msg):
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
        response, is_batch = [], False

        try:
            try:
                msg_full = self._decode(msg_full)
            except Exception as ex:
                raise TypeError(f"Failed to parse the message {msg_full!r}: {ex}") from ex

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
