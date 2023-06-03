def to_boolean(value):
    """
    Returns ``True`` or ``False`` if ``value`` is found in one of the lists of supported values.
    Otherwise returns ``None`` (typicall means that the value is not set).
    """
    v = value.lower() if isinstance(value, str) else value
    if v in (True, "y", "yes", "t", "true", "on", "1"):
        return True
    elif v in (False, "", "n", "no", "f", "false", "off", "0"):
        return False
    else:
        return None


def generate_random_port(ip=None):
    """
    Generate random port number for a free port. The code is vendored from here:
    https://github.com/jupyter/jupyter_client/blob/58017fc04199ab012ad2b6f5a01b8d3e11698e7c/
    jupyter_client/connect.py#L102-L112
    """
    import socket

    from jupyter_client.localinterfaces import localhost

    ip = ip or localhost()
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\0" * 8)
    sock.bind((ip, 0))
    port = sock.getsockname()[1]
    sock.close()

    return port
