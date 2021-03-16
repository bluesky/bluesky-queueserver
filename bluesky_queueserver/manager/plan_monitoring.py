import copy
import threading
import logging

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

    def add_run(self, *, uid):
        """
        Add run to the end of the list. The run is labeled as 'open' (``is_open`` is set ``True``).

        Parameters
        ----------
        uid : str
            UID of the run.
        """
        with self._lock:
            self._run_list.append({"uid": uid, "is_open": True, "exit_status": None})
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
            self._run_list.add_run(uid=uid)

            logger.info("New run was open: '%s'", uid)
            logger.debug("Run list: %s", str(self._run_list.get_run_list()))
        except Exception as ex:
            logger.exception(f"RE Manager: Could not register new run: {ex}")

    def stop(self, doc):
        """
        Process STOP documents. Overrides the method of ``CallbackBase``.
        """
        try:
            uid = doc["run_start"]
            exit_status = doc["exit_status"]
            self._run_list.set_run_closed(uid=uid, exit_status=exit_status)

            print(f"Run was closed: '{uid}'")
        except Exception as ex:
            logger.exception(f"RE Manager: Failed to label run as closed: {ex}")
