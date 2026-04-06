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


def filter_dict_by_permitted_keys(d, permitted_keys):
    """
    Filter the dictionary `d` by the list of `permitted_keys`. The keys in the dictionary that are not in the list
    of permitted keys will be removed. The keys in the list of permitted keys can be specified as absolute paths
    (e.g. "/key1/key2").
    """

    def _match_path(path: str, permitted_keys: list[str]) -> bool:
        # If root ("/") is in permitted_keys, always match
        if "/" in permitted_keys:
            return True
        for p in permitted_keys:
            # Trim trailing slash(es) for matching
            p = p.rstrip("/")
            if path == p:
                return True

        return False

    def _filter(d, permitted_keys, prefix=""):
        filtered = {}
        for k, v in d.items():
            path = f"{prefix}/{k}" if prefix else f"/{k}"
            # If value is a dict, recurse
            if isinstance(v, dict):
                # If any permitted key matches this path as a prefix, include the whole dict
                if _match_path(path + "/", permitted_keys) or _match_path(path, permitted_keys):
                    filtered[k] = v
                else:
                    nested = _filter(v, permitted_keys, path)
                    if nested:
                        filtered[k] = nested
            else:
                if _match_path(path, permitted_keys) or _match_path(path + "/", permitted_keys):
                    filtered[k] = v
        return filtered

    return _filter(d, permitted_keys)
