import logging
import pprint
import sys
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
        should be large enought to fully display 'status'. The dictionary values for
        the keys ``plans_allowed``, ``plans_existing``, ``devices_allowed`` and
        ``devices_existing``, which are dictionaries themselves are treated differently:
        all the keys (names of plans or devices) are kept, but all values are replaced with
        ``"{ ... }"`` (string).
    max_chars_in_str: int (optional)
        Strings longer then specified number of characters are truncated. Only the first
        and the last ``max_chars_in_str / 2`` are printed. If the string is the value
        of a ``traceback`` key of a dictionary, then it is not truncated.

    Returns
    -------
    str
        Formatted object ``msg``.
    """

    def __init__(self, msg, *, max_list_size=10, max_dict_size=25, max_chars_in_str=1024):
        self._msg = msg
        self._max_list_size = max_list_size
        self._max_dict_size = max_dict_size
        self._max_chars_in_str = max_chars_in_str

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        # Non-iterative implementation of the transformation. Copying is minimized.
        large_dict_keys = ("plans_allowed", "plans_existing", "devices_allowed", "devices_existing")
        keep_strings_keys = "traceback"

        def convert_large_dicts(msg_in):
            """
            Some large dictionaries need to be treated in a special way.
            """
            msg_out = {}
            for k in msg_in.keys():
                msg_out[k] = "{...}"
            return msg_out

        def process_entry(msg_in):
            if isinstance(msg_in, Mapping):
                msg_out = {}
                for n, k in enumerate(msg_in):
                    if n < self._max_dict_size:
                        msg_out[k] = msg_in[k]
                    else:
                        msg_out["..."] = "..."
                        break
                new_entries = [msg_out]
            elif isinstance(msg_in, str):
                if len(msg_in) > self._max_chars_in_str:
                    half = int(self._max_chars_in_str / 2)
                    msg_out = msg_in[:half] + " ...\n... " + msg_in[-half:]
                else:
                    msg_out = msg_in
                new_entries = []
            elif isinstance(msg_in, Iterable):
                msg_out = list(msg_in[: self._max_list_size + 1])
                if len(msg_out) > self._max_list_size:
                    msg_out[-1] = "..."
                new_entries = [msg_out]
            else:
                msg_out = msg_in
                new_entries = []

            return msg_out, new_entries

        msg_reduced, queue = process_entry(self._msg)

        while queue:
            msg = queue.pop(0)

            # The queue contains processed objects, so it is safe to assume that
            #   all mappings are 'dict' and all iterables are 'list'.
            if isinstance(msg, dict):
                for k in msg.keys():
                    # msg[k] is still unprocessed, so use Mapping
                    if (k in large_dict_keys) and isinstance(msg[k], Mapping):
                        msg[k] = convert_large_dicts(msg[k])
                    elif (k in keep_strings_keys) and isinstance(msg[k], str):
                        pass
                    else:
                        mo, qe = process_entry(msg[k])
                        msg[k] = mo
                        queue.extend(qe)

            elif isinstance(msg, list):
                for k in range(len(msg)):
                    mo, qe = process_entry(msg[k])
                    msg[k] = mo
                    queue.extend(qe)

        if version.parse(python_version()) < version.parse("3.8"):
            # TODO: delete this after support for 3.7 is dropped
            pprint.sorted = lambda x, key=None: x
            msg_out = pprint.pformat(msg_reduced)
            delattr(pprint, "sorted")
        else:
            msg_out = pprint.pformat(msg_reduced, sort_dicts=False)

        return msg_out
