import copy
import enum
import logging
import math
import threading
import time as ttime

from bluesky import Msg
from bluesky.callbacks.core import CallbackBase

try:
    import numpy as np
except ImportError:
    np = None

from .output_streaming import push_info_to_msg_queue

logger = logging.getLogger(__name__)

_device_progress_key = "device_progress"
_messages_key = "re_message"


class RunList:
    """
    The class for maintaining the list of active runs (used in RE Worker).
    The calls of the class methods are thread-safe.
    """

    def __init__(self):
        self._run_list = []
        self._lock = threading.Lock()
        self._list_changed = False
        self._enabled = False

    def enable(self):
        """
        Enable collection of runs.
        """
        self._enabled = True

    def disable(self):
        """
        Disable collection of runs. The list can still be cleared.
        """
        self._enabled = False

    def is_enabled(self):
        """
        Returns ``True`` if run collection is enabled, ``False`` otherwise.
        """
        return self._enabled

    def is_empty(self):
        """
        The method reports whether the list of runs is empty.

        Returns
        -------
        boolean
            True - the list contains no runs
        """
        return bool(self._run_list)

    @property
    def nruns(self):
        """
        Returns the number of collected runs (completed and incomplete).

        Returns
        -------
        int
            The number of collected runs.

        """
        return len(self._run_list)

    def is_changed(self):
        """
        Verifies if the list was changed since the list state was last cleared.

        Returns
        -------
        bool
            True - the list changed since the state was cleared.
        """
        return self._list_changed

    def clear(self):
        """
        Clears the list of runs.
        """
        with self._lock:
            self._run_list.clear()
            self._list_changed = True

    def add_run(self, *, uid, scan_id):
        """
        Add run to the end of the list. The run is labeled as 'open' (``is_open`` is set ``True``).

        Parameters
        ----------
        uid : str
            UID of the run.
        """
        if not self._enabled:
            return

        with self._lock:
            self._run_list.append({"uid": uid, "scan_id": scan_id, "is_open": True, "exit_status": None})
            self._list_changed = True

    def set_run_closed(self, *, uid, exit_status):
        """
        Set run with ``uid`` as 'closed'.

        Parameters
        ----------
        uid : str
            UID of the run
        exit_status : str
            exit status of the run (Bluesky run exit status as returned in 'stop' document).
        """
        if not self._enabled:
            return

        with self._lock:
            run = None
            # 'reversed' - if a plan sequentially opens/closes many runs, the open run is much more
            #   likely to be at the end of the list.
            for r in reversed(self._run_list):
                if r["uid"] == uid:
                    run = r
                    break

            if run is None:
                raise Exception("Run with UID '%s' was not found in the list", uid)

            run["is_open"] = False
            run["exit_status"] = exit_status
            self._list_changed = True

    def get_run_list(self, *, clear_state=False):
        """
        Returns the copy (deep copy) of run list. Copying is needed for thread safety.
        Optionally the state of the list could be cleared. The state is used to monitor if
        changes were made to the list.

        Parameters
        ----------
        clear_state : boolean
            indicates if the state of the list should be cleared.
        """
        with self._lock:
            run_list_copy = copy.deepcopy(self._run_list)
            if clear_state:
                self._list_changed = False
            return run_list_copy

    def get_uids(self):
        """
        Return the list of run UIDs
        """
        return [_["uid"] for _ in self._run_list]

    def get_scan_ids(self):
        """
        Return the list of scan IDs
        """
        return [_["scan_id"] for _ in self._run_list]


class CallbackRegisterRun(CallbackBase):
    """
    Callback used to process 'start' and 'stop' documents emitted by Run Engine.
    Run UIDs is extracted from 'start' documents and inserted into run list.
    When 'stop' document is emitted, the respective run in the run list is set
    as stopped, and exit status of the run is saved.

    Parameters
    ----------
    run_list : RunList
        reference to ``RunList`` object used to store Run UIDs.
    """

    def __init__(self, *, run_list):
        super().__init__()
        self._run_list = run_list

    def start(self, doc):
        """
        Process START documents. Overrides the method of ``CallbackBase``.
        """
        try:
            uid = doc["uid"]
            scan_id = doc.get("scan_id", None)
            if scan_id is not None:
                try:
                    scan_id = int(scan_id)
                except Exception:
                    scan_id = None

            self._run_list.add_run(uid=uid, scan_id=scan_id)

            logger.info("New run was open: %r", uid)
            logger.debug("Run list: %s", self._run_list.get_run_list())
        except Exception as ex:
            logger.exception("RE Manager: Could not register new run: %s", ex)

    def stop(self, doc):
        """
        Process STOP documents. Overrides the method of ``CallbackBase``.
        """
        try:
            uid = doc["run_start"]
            exit_status = doc["exit_status"]
            self._run_list.set_run_closed(uid=uid, exit_status=exit_status)

            logger.info("Run was closed: %r", uid)
        except Exception as ex:
            logger.exception("RE Manager: Failed to label run as closed: %s", ex)


def _to_json_safe(value):
    """
    Coerce a value to a JSON-serializable type. Returns ``None`` for values
    that cannot be represented as a number or string.
    """
    if value is None:
        return None
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        return value
    try:
        return float(value)
    except (TypeError, ValueError):
        return str(value)


class WatcherStreamManager:
    """
    RunEngine ``waiting_hook``-compatible class. Instead of rendering progress bars,
    it serializes watcher updates and pushes them to ``msg_queue`` on the ``"info"``
    channel (``QS_Info`` 0MQ topic) under the ``"device_progress"`` key so they are
    published to 0MQ / websocket subscribers.

    The RunEngine calls instances of this class with a set of Status objects each time
    it enters a wait, and with ``None`` when the wait completes. For each status object
    that supports ``watch()``, a callback is registered that streams position/progress
    updates.

    Parameters
    ----------
    msg_queue : multiprocessing.Queue
        Reference to the shared message queue used for publishing messages.
    min_update_period : float
        Minimum interval in seconds between published updates for a single status
        object. The final update (when the status is done) is always sent regardless
        of throttling. Default: ``0.2``.
    """

    def __init__(self, *, msg_queue, min_update_period=0.2):
        self._msg_queue = msg_queue
        self._min_update_period = min_update_period
        # Track status objects we have already subscribed to, keyed by id(status)
        self._watched = set()
        self._last_sent = {}  # id(status) -> timestamp of last sent update
        self._status_counter = 0  # Counter for generating labels when name is None

        # Reference to the callback that was already set at the Run Engine, (e.x.
        # the callback that prints the progress bar in the terminal). The existing
        # callback is replaced by the reference of WatcherStreamManager class.
        self._waiting_hook = None

    @property
    def waiting_hook(self):
        return self._waiting_hook

    @waiting_hook.setter
    def waiting_hook(self, v):
        self._waiting_hook = v or None

    def __call__(self, status_objs_or_none):
        """
        Called by the RunEngine with a set of Status objects or ``None``.
        """
        if status_objs_or_none is None:
            # Waiting is complete — send a completion message and reset state
            self._send_completed()
            self._watched.clear()
            self._last_sent.clear()
            self._status_counter = 0

        else:
            for st in status_objs_or_none:
                st_id = id(st)
                if st_id in self._watched:
                    continue
                self._watched.add(st_id)
                if not hasattr(st, "watch") or getattr(st, "done", False):
                    continue
                try:
                    self._status_counter += 1
                    label = self._status_counter
                    st.watch(self._make_callback(st, label))
                except Exception:
                    logger.debug("Status object does not support watch(): %r", st, exc_info=True)

        if self.waiting_hook:
            self.waiting_hook(status_objs_or_none)

    def _make_callback(self, status_obj, label):
        """
        Create a watch callback bound to a specific status object.
        """
        st_id = id(status_obj)

        def _cb(
            *,
            name=None,
            current=None,
            initial=None,
            target=None,
            unit=None,
            precision=None,
            fraction=None,
            time_elapsed=None,
            time_remaining=None,
            **kwargs,
        ):
            now = ttime.time()
            done = getattr(status_obj, "done", False)

            # The final update (the status reached its target) must never be throttled,
            # otherwise progress bars may freeze just short of 100%. ophyd status objects
            # emit their last watcher update with ``current == target`` *before* ``done``
            # is set (watchers are cleared once the status settles), and ophyd reports
            # ``fraction`` as the fraction *remaining* (0 when the target is reached).
            at_target = (fraction is not None and fraction <= 0) or (
                current is not None and target is not None and current == target
            )
            is_final = bool(done) or at_target

            # Throttle: skip non-final updates that arrive too quickly
            last = self._last_sent.get(st_id, 0)
            if not is_final and (now - last) < self._min_update_period:
                return
            self._last_sent[st_id] = now

            if is_final:
                # Clean up tracking for this status
                self._last_sent.pop(st_id, None)

            display_name = name if name is not None else f"Status {label}"

            payload = {
                "name": display_name,
                "current": _to_json_safe(current),
                "initial": _to_json_safe(initial),
                "target": _to_json_safe(target),
                "unit": unit,
                "precision": precision,
                "fraction": fraction,
                "time_elapsed": time_elapsed,
                "time_remaining": time_remaining,
                "done": is_final,
            }

            try:
                push_info_to_msg_queue(key=_device_progress_key, msg=payload, msg_queue=self._msg_queue)
            except Exception:
                logger.debug("Failed to push progress update to msg_queue", exc_info=True)

        return _cb

    def _send_completed(self):
        """
        Send a message indicating that the waiting period is complete (all statuses done).
        """
        payload = {"completed": True}
        try:
            push_info_to_msg_queue(key=_device_progress_key, msg=payload, msg_queue=self._msg_queue)
        except Exception:
            logger.debug("Failed to push progress completion to msg_queue", exc_info=True)


def _coerce_float(value):
    """
    Convert a float to a JSON-safe value: ``NaN`` becomes ``None`` and infinities become
    the strings ``"Infinity"`` / ``"-Infinity"`` (default ``json.dumps`` would otherwise
    emit bare ``NaN``/``Infinity`` tokens that are not valid JSON).
    """
    if math.isnan(value):
        return None
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return float(value)


def _serialize_msg_component(value):
    """
    Coerce a ``Msg`` component (``obj``, elements of ``args``, values of ``kwargs`` or
    ``run``) to a JSON-serializable representation. Enum members are represented by their
    value, non-finite floats are made JSON-safe, numpy scalars/arrays are converted to
    native Python types, devices and other objects with a ``name`` attribute are
    represented by their ``repr()`` (which includes the type, making it nicer to read),
    containers are serialized recursively, and anything else (e.g. bytes, datetimes)
    falls back to ``str()`` (or ``None`` on failure).
    """
    if value is None:
        return value
    if isinstance(value, enum.Enum):
        return _serialize_msg_component(value.value)
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        return _coerce_float(value)
    if isinstance(value, (int, str)):
        return value
    if np is not None:
        if isinstance(value, np.generic):
            return _serialize_msg_component(value.item())
        if isinstance(value, np.ndarray):
            return _serialize_msg_component(value.tolist())
    name = getattr(value, "name", None)
    if isinstance(name, str):
        return repr(value)
    if isinstance(value, (list, tuple, set)):
        return [_serialize_msg_component(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _serialize_msg_component(v) for k, v in value.items()}
    try:
        return str(value)
    except Exception:
        return None


class MsgHookStreamManager:
    """
    RunEngine ``msg_hook``-compatible class. It serializes each ``Msg`` processed by the
    RunEngine together with a timestamp and pushes it to ``msg_queue`` on the ``"info"``
    channel (``QS_Info`` 0MQ topic) under the ``"re_message"`` key, so the plan messages are
    published to 0MQ / websocket subscribers.

    The RunEngine calls instances of this class with a single ``Msg`` object each time it
    processes a message from the running plan.

    Parameters
    ----------
    msg_queue : multiprocessing.Queue
        Reference to the shared message queue used for publishing messages.
    """

    def __init__(self, *, msg_queue):
        self._msg_queue = msg_queue

        # Reference to the callback that was already set at the Run Engine. The existing
        # callback is replaced by the reference of MsgHookStreamManager class and is called
        # after the message is streamed.
        self._msg_hook = None

    @property
    def msg_hook(self):
        return self._msg_hook

    @msg_hook.setter
    def msg_hook(self, v):
        self._msg_hook = v or None

    def __call__(self, msg: Msg):
        """
        Called by the RunEngine with a single ``Msg`` object.
        """
        try:
            payload = {
                "time": ttime.time(),
                "command": getattr(msg, "command", None),
                "obj": _serialize_msg_component(getattr(msg, "obj", None)),
                "args": [_serialize_msg_component(_) for _ in getattr(msg, "args", ()) or ()],
                "kwargs": {
                    str(k): _serialize_msg_component(v) for k, v in (getattr(msg, "kwargs", {}) or {}).items()
                },
                "run": _serialize_msg_component(getattr(msg, "run", None)),
            }
            push_info_to_msg_queue(key=_messages_key, msg=payload, msg_queue=self._msg_queue)
        except Exception:
            logger.debug("Failed to push RE message to msg_queue", exc_info=True)

        if self.msg_hook:
            self.msg_hook(msg)
