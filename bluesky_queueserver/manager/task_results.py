import asyncio
import time as ttime
import uuid


class TaskResults:
    """
    The class that defines an object for storage and management of the results of tasks.

    Parameters
    ----------
    retention_time: float
        Defines minimum life time of the results of the completed tasks stored
        by the object.
    """

    # TODO: It may be a good idea to store the results or backup of the results in Redis
    #       so that they persist if the manager process is restarted. The class contains some
    #       infrastructure, in particular all operations on the lists of task results are
    #       protected by 'asyncio.Lock' and methods are made `async`.
    def __init__(self, *, retention_time=120):
        self._retention_time = retention_time
        self._running_tasks = {}
        self._completed_tasks_time = []
        self._completed_tasks_data = {}
        self._lock = asyncio.Lock()

        self._task_results_uid = None
        self._update_task_results_uid()

    def _update_task_results_uid(self):
        """
        Generate a new UID of the state of the object. UID should be update
        each time the new result of a completed task is added. UID is used
        to inform client applications that initiated execution of a task that
        some task was completed and it is reasonable to check if the result
        for the specific task is ready.
        """
        self._task_results_uid = str(uuid.uuid4())

    @property
    def task_results_uid(self):
        """
        Return UID of the list of task results.
        """
        return self._task_results_uid

    async def clear(self):
        """
        Clear the lists of running and completed tasks.
        """
        async with self._lock:
            self._running_tasks.clear()
            self._completed_tasks_time.clear()
            self._completed_tasks_data.clear()

    async def clear_running_tasks(self):
        """
        Clear the list of running tasks.
        """
        async with self._lock:
            self._running_tasks.clear()

    async def add_running_task(self, *, task_uid, payload=None):
        """
        Add a task to the list of running tasks. For teach task UID, the payload
        passed as a parameter and time of insertion is saved.

        Parameters
        ----------
        task_uid: str
            UID of the task
        payload: dict
            Dictionary that contain information on the running task
            (such as UID, start time etc.). This dictionary is passed to
            the client application when requested.
        """
        payload = payload or {}
        async with self._lock:
            self._running_tasks[task_uid] = {"time": ttime.time(), "payload": payload}

    def _remove_running_task(self, *, task_uid):
        """
        See docstring for ``self.remove_running_task()``.
        """
        if task_uid in self._running_tasks:
            del self._running_tasks[task_uid]

    async def remove_running_task(self, *, task_uid):
        """
        Remove running task from the list of running tasks.

        Parameters
        ----------
        task_uid: str
            UID of the task to be removed.
        """
        async with self._lock:
            self._remove_running_task(task_uid=task_uid)

    async def add_completed_task(self, *, task_uid, payload=None):
        """
        Add task to the list of completed task (and remove it from the list of running tasks).
        The function is updating ``task_results_uid`` to inform that there are tasks that
        were just completed.

        Parameters
        ----------
        task_uid: str
            UID of the new task
        payload: dict or None
            Dictionary with the results of the task execution. If ``None``, the payload is set
            to ``{}`` (empty dictionary).
        """
        payload = payload or {}
        async with self._lock:
            self._clean_completed_tasks()
            self._remove_running_task(task_uid=task_uid)
            insertion_time = ttime.time()
            self._completed_tasks_data[task_uid] = {"time": insertion_time, "payload": payload}
            self._completed_tasks_time.append({"task_uid": task_uid, "time": insertion_time})
            self._update_task_results_uid()

    async def get_task_info(self, *, task_uid):
        """
        Return information on the task with ``task_uid``. The function returns a tuple, which
        contains task status and available information on the task or result of the task
        execution.

        Parameters
        ----------
        task_uid: str
            UID of the task

        Returns
        -------
        status: str
            Status of the task with ``task_uid``. Status values are ``running``, ``completed``
            and ``not_found``.
        payload: dict
            Dictionary with task information for a running task or task results for completed task.
            If ``status`` is ``not_found``, then payload is ``{}`` (empty dictionary).
        """
        async with self._lock:
            if task_uid in self._completed_tasks_data:
                status, payload = "completed", self._completed_tasks_data[task_uid]["payload"]
            elif task_uid in self._running_tasks:
                status, payload = "running", self._running_tasks[task_uid]["payload"]
            else:
                status, payload = "not_found", {}

        return status, payload

    def _clean_completed_tasks(self):
        """
        See the docstring for ``self.clean_completed_tasks()``.
        """
        time_threshold = ttime.time() - self._retention_time
        ctt = self._completed_tasks_time
        while ctt and (ctt[0]["time"] < time_threshold):
            task_uid = ctt[0]["task_uid"]
            ctt.pop(0)
            if task_uid in self._completed_tasks_data:
                del self._completed_tasks_data[task_uid]

    async def clean_completed_tasks(self):
        """
        Remove expired tasks from the list of completed task. The ``retention_time`` parameter of
        the class constructor is used to identify the expired tasks.
        """
        async with self._lock:
            self._clean_completed_tasks()
