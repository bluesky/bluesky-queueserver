import time as ttime
import threading
import uuid


class TaskResults:
    def __init__(self, *, retention_time=120):
        self._retention_time = retention_time
        self._running_tasks = {}
        self._completed_tasks_time = []
        self._completed_tasks_data = {}
        self._lock = threading.Lock()

        self._task_results_uid = None
        self._update_task_results_uid()

    def _update_task_results_uid(self):
        self._task_results_uid = str(uuid.uuid4())

    @property
    def task_results_uid(self):
        return self._task_results_uid

    def clear(self):
        self._running_tasks.clear()
        self._completed_tasks_time.clear()
        self._completed_tasks_data.clear()

    def add_running_task(self, *, task_uid, payload=None):
        payload = payload or {}
        with self._lock:
            self._running_tasks[task_uid] = {"time": ttime.time(), "payload": payload}
            self._update_task_results_uid()

    def _remove_running_task(self, *, task_uid):
        if task_uid in self._running_tasks:
            del self._running_tasks[task_uid]

    def remove_running_task(self, *, task_uid):
        with self._lock:
            self._remove_running_task(task_uid=task_uid)

    def add_completed_task(self, *, task_uid, payload=None):
        payload = payload or {}
        with self._lock:
            self._clean_completed_tasks()
            self._remove_running_task(task_uid=task_uid)
            insertion_time = ttime.time()
            self._completed_tasks_data[task_uid] = {"time": insertion_time, "payload": payload}
            self._completed_tasks_time.append({"task_uid": task_uid, "time": insertion_time})
            self._update_task_results_uid()

    def get_task_info(self, *, task_uid):
        with self._lock:
            if task_uid in self._completed_tasks_data:
                status, payload = "completed", self._completed_tasks_data[task_uid]["payload"]
            elif task_uid in self._running_tasks:
                status, payload = "running", self._running_tasks[task_uid]["payload"]
            else:
                status, payload = "not_found", {}

        return status, payload

    def _clean_completed_tasks(self):

        changed = False

        time_threshold = ttime.time() - self._retention_time
        ctt = self._completed_tasks_time
        while ctt and (ctt[0]["time"] < time_threshold):
            task_uid = ctt[0]["task_uid"]
            ctt.pop(0)
            if task_uid in self._completed_tasks_info:
                del self._completed_tasks_info[task_uid]
                changed = True

        if changed:
            self._update_task_results_uid()

    def clean_completed_tasks(self):
        with self._lock:
            self._clean_completed_tasks()
