import uuid


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
