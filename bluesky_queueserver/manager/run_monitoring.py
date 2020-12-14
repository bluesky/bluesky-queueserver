import copy
import threading
import logging

from bluesky.callbacks.core import CallbackBase

logger = logging.getLogger(__name__)


class RunList:
    def __init__(self):
        self._run_list = []
        self._lock = threading.Lock()
        self._list_changed = False

    def is_changed(self):
        return self._list_changed

    def clear(self):
        with self._lock:
            self._run_list.clear()
            self._list_changed = True

    def add_run(self, *, uid):
        with self._lock:
            self._run_list.append({"uid": uid, "is_open": True, "exit_status": None})
            self._list_changed = True

    def set_run_closed(self, *, uid, exit_status):
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
        with self._lock:
            run_list_copy = copy.deepcopy(self._run_list)
            if clear_state:
                self._list_changed = False
            return run_list_copy


class CallbackRegisterRun(CallbackBase):
    def __init__(self, *, run_list):
        super().__init__()
        self._run_list = run_list

    def start(self, doc):
        """
        Process START documents
        """
        try:
            uid = doc["uid"]
            self._run_list.add_run(uid=uid)

            print(f"New run was open: '{uid}'")
            print(f"Run list: {self._run_list.get_run_list()}")
        except Exception as ex:
            logger.exception(f"RE Manager: Could not register new run: {ex}")

    def stop(self, doc):
        """
        Process STOP documents
        """
        try:
            uid = doc["run_start"]
            exit_status = doc["exit_status"]
            self._run_list.set_run_closed(uid=uid, exit_status=exit_status)

            print(f"Run was closed: '{uid}'")
        except Exception as ex:
            logger.exception(f"RE Manager: Failed to label run as closed: {ex}")
