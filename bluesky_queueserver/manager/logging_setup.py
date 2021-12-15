import logging
import sys


def setup_loggers(*, log_level, name="bluesky_queueserver"):
    """
    Configure loggers used by Bluesky Queue Server.

    Parameters
    ----------
    name: str
        Module name (typically ``__name__``)
    log_level
        Logging level (e.g. ``logging.INFO``, ``"INFO"`` or ``20``)
    """
    log_stream_handler = logging.StreamHandler(sys.stdout)
    log_stream_handler.setLevel(log_level)
    if (
        (log_level == logging.DEBUG)
        or (log_level == "DEBUG")
        or (isinstance(log_level, int) and (log_level <= 10))
    ):
        log_stream_format = "[%(levelname)1.1s %(asctime)s.%(msecs)03d %(name)s %(module)s:%(lineno)d] %(message)s"
    else:
        log_stream_format = "[%(levelname)1.1s %(asctime)s %(name)s] %(message)s"

    log_stream_handler.setFormatter(logging.Formatter(fmt=log_stream_format))
    logging.getLogger(name).handlers.clear()
    logging.getLogger(name).addHandler(log_stream_handler)
    logging.getLogger(name).setLevel(log_level)
    logging.getLogger(name).propagate = False
