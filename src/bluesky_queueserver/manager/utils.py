import copy
from collections.abc import Collection, Mapping
from typing import Any


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


def filter_dict_by_permitted_keys(d: Mapping[str, Any], permitted_keys: Collection[str]) -> dict[str, Any]:
    """
    Filter the dictionary `d` by the list of `permitted_keys`. The keys in the dictionary that are not in the list
    of permitted keys will be removed. The keys in the list of permitted keys can be specified as absolute paths
    (e.g. "/key1/key2").
    """

    def _match_path(path: str, permitted_keys: Collection[str]) -> bool:
        # If root ("/") is in permitted_keys, always match
        if "/" in permitted_keys:
            return True
        for p in permitted_keys:
            # Trim trailing slash(es) for matching
            p = p.rstrip("/")
            if path == p:
                return True

        return False

    def _filter(d: Mapping[str, Any], permitted_keys: Collection[str], prefix: str = "") -> dict[str, Any]:
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


def _is_path_permitted(path: str, permitted_keys: Collection[str]) -> bool:
    """
    Returns ``True`` if ``path`` (e.g. ``"/key1/key2"``) is equal to or nested under one of
    ``permitted_keys``. The key ``"/"`` permits all paths.
    """
    for p in permitted_keys:
        # Ignore malformed entries, e.g. '' from a trailing ':' in the env. variable, which would match all paths
        if not p.startswith("/"):
            continue
        p = p.rstrip("/")
        if not p or path == p or path.startswith(p + "/"):
            return True
    return False


def merge_dict_by_permitted_keys(
    d: Mapping[str, Any], updates: dict[str, Any], permitted_keys: Collection[str]
) -> tuple[dict[str, Any], list[str]]:
    """
    Merge ``updates`` into the mapping ``d`` and return a dictionary with new values of the top-level
    keys of ``d`` that are modified and the list of updated paths. The mapping ``d`` is not modified.
    The values at permitted paths are replaced. Nested dictionaries at non-permitted paths are merged
    recursively, so that only permitted subkeys are changed. The keys in ``permitted_keys`` are specified
    as absolute paths (e.g. ``"/key1/key2"``), ``"/"`` permits all keys.

    Raises
    ------
    ValueError
        ``updates`` contains invalid or non-permitted keys.
    """
    if not isinstance(updates, dict):
        raise ValueError(f"Metadata updates must be a dictionary: {updates!r}")

    forbidden, updated_paths = [], []

    def _merge(target: dict[str, Any], upd: dict[str, Any], prefix: str):
        for k, v in upd.items():
            if not isinstance(k, str) or not k or "/" in k:
                raise ValueError(f"Invalid metadata key {k!r}: keys must be non-empty strings without '/'")
            path = f"{prefix}/{k}"
            if _is_path_permitted(path, permitted_keys):
                target[k] = copy.deepcopy(v)
                updated_paths.append(path)
            elif isinstance(v, dict) and v and isinstance(target.get(k, {}), dict):
                target.setdefault(k, {})
                _merge(target[k], v, path)
            else:
                forbidden.append(path)

    # Deep copies also convert nested mappings of RedisJSONDict to regular dictionaries.
    #   Each key is read once: reads from RedisJSONDict are requests to Redis.
    new_values = {}
    for k in updates:
        if isinstance(k, str):
            try:
                new_values[k] = copy.deepcopy(d[k])
            except KeyError:
                pass
    _merge(new_values, updates, "")

    if forbidden:
        raise ValueError(f"Updating metadata keys {forbidden} is not permitted")

    return new_values, updated_paths
