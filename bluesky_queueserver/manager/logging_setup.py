import copy
import logging
import sys
import pprint
from collections.abc import Iterable, Mapping
from platform import python_version
from packaging import version


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


class PPrintForLogging:
    """
    Applies ``pprint.pformat`` to logging messages::

      logger.debug("Received message: %s", PPrintForLogging(msg))

    The ``pprint.pformat`` is called only if the debug output is required, which reduces overhead.

    Parameters
    ----------
    msg: object
        A message to be printed
    max_list_size: int (optional)
        Maximum number of displayed elements of a lists (iterable)
    max_dict_size: int (optional)
        Maximum number of displayed elements of a dictionary (mapping). The number
        should be large enought to fully display 'status'.

    Returns
    -------
    str
        Formatted object ``msg``.
    """

    def __init__(self, msg, *, max_list_size=10, max_dict_size=25):
        self._msg = msg
        self._max_list_size = max_list_size
        self._max_dict_size = max_dict_size

    def __repr__(self):
        return self.__str__()

    def __str__(self):

        large_dict_keys = ("plans_allowed", "plans_existing", "devices_allowed", "devices_existing")

        def convert_large_dicts(msg_in):
            """
            Some large dictionaries need to be treated in a special way.
            """
            if not isinstance(msg_in, Mapping):
                return reduce_msg(msg_in)
            else:
                msg_out = copy.deepcopy(msg_in)
                for k in msg_out.keys():
                    msg_out[k] = "{...}"
                return msg_out

        def reduce_msg(msg_in):
            if isinstance(msg_in, Mapping):
                msg_out = {}
                for n, k in enumerate(msg_in):
                    if n < self._max_dict_size:
                        if k in large_dict_keys:
                            msg_out[k] = convert_large_dicts(msg_in[k])
                        else:
                            msg_out[k] = reduce_msg(msg_in[k])
                    else:
                        msg_out["..."] = "..."
                        break
            elif isinstance(msg_in, Iterable) and not isinstance(msg_in, str):
                msg_out = list(msg_in[: self._max_list_size + 1])
                n_pts = min(len(msg_out), self._max_list_size)
                for n in range(n_pts):
                    msg_out[n] = reduce_msg(msg_out[n])
                if len(msg_out) > self._max_list_size:
                    msg_out[-1] = "..."
            else:
                msg_out = msg_in
            return msg_out

        if version.parse(python_version()) < version.parse("3.8"):
            # NOTE: delete this after support for 3.7 is dropped
            f_sorted = pprint._sorted
            pprint._sorted = lambda x: x
            msg_out = pprint.pformat(reduce_msg(self._msg))
            pprint._sorted = f_sorted
        else:
            msg_out = pprint.pformat(reduce_msg(self._msg), sort_dicts=False)

        return msg_out
