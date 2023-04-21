import copy
import logging
import threading

from bluesky.callbacks.core import CallbackBase

logger = logging.getLogger(__name__)


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
