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
