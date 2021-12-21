import asyncio
import time as ttime

from bluesky_queueserver.manager.task_results import TaskResults


def test_TaskResults_update_uid():
    """
    TaskResults: Test that task result UID is updated.
    """

    async def testing():
        tr = TaskResults()

        uid = tr.task_results_uid
        assert isinstance(uid, str)
        tr._update_task_results_uid()
        assert tr.task_results_uid != uid

    asyncio.run(testing())


def test_TaskResults_add_running_task():
    """
    TaskResults: tests for ``add_running_task``, ``clear_running_task``
    """

    async def testing():
        tr = TaskResults()

        assert tr._running_tasks == {}
        uid = tr.task_results_uid

        await tr.add_running_task(task_uid="abc")
        await tr.add_running_task(task_uid="def", payload={"some_value": 10})
        assert len(tr._running_tasks) == 2

        assert tr._running_tasks["abc"]["payload"] == {}
        assert isinstance(tr._running_tasks["abc"]["time"], float)
        assert tr._running_tasks["def"]["payload"] == {"some_value": 10}
        assert isinstance(tr._running_tasks["def"]["time"], float)

        await tr.clear_running_tasks()
        assert tr._running_tasks == {}

        # UID is not expected to change
        assert tr.task_results_uid == uid

    asyncio.run(testing())


def test_TaskResults_remove_running_task():
    """
    TaskResults: tests for ``remove_running_task``
    """

    async def testing():
        tr = TaskResults()

        assert tr._running_tasks == {}
        uid = tr.task_results_uid

        await tr.add_running_task(task_uid="abc")
        await tr.add_running_task(task_uid="def", payload={"some_value": 10})
        assert len(tr._running_tasks) == 2

        await tr.remove_running_task(task_uid="abc")

        assert len(tr._running_tasks) == 1
        assert tr._running_tasks["def"]["payload"] == {"some_value": 10}
        assert isinstance(tr._running_tasks["def"]["time"], float)

        # UID is not expected to change
        assert tr.task_results_uid == uid

    asyncio.run(testing())


def test_TaskResults_add_completed_task():
    """
    TaskResults: tests for ``add_running_task``, ``clear_running_task``
    """

    async def testing():
        tr = TaskResults()

        uid1 = tr.task_results_uid

        # Add running tasks. The running tasks should be removed as completed tasks
        #   with the same UID are added.
        await tr.add_running_task(task_uid="abc", payload={"some_value": "arbitrary_payload"})
        await tr.add_running_task(task_uid="def", payload={"some_value": "arbitrary_payload"})

        assert tr.task_results_uid == uid1

        assert tr._completed_tasks_time == []
        assert tr._completed_tasks_data == {}
        assert len(tr._running_tasks) == 2

        await tr.add_completed_task(task_uid="abc")
        uid2 = tr.task_results_uid
        await tr.add_completed_task(task_uid="def", payload={"some_value": 10})
        uid3 = tr.task_results_uid

        assert len(tr._completed_tasks_time) == 2
        assert len(tr._completed_tasks_data) == 2
        assert len(tr._running_tasks) == 0

        assert uid1 != uid2
        assert uid2 != uid3

        assert tr._completed_tasks_data["abc"]["payload"] == {}
        assert isinstance(tr._completed_tasks_data["abc"]["time"], float)
        assert tr._completed_tasks_time[0]["task_uid"] == "abc"
        assert tr._completed_tasks_time[0]["time"] == tr._completed_tasks_data["abc"]["time"]
        assert tr._completed_tasks_data["def"]["payload"] == {"some_value": 10}
        assert isinstance(tr._completed_tasks_data["def"]["time"], float)
        assert tr._completed_tasks_time[1]["task_uid"] == "def"
        assert tr._completed_tasks_time[1]["time"] == tr._completed_tasks_data["def"]["time"]

        await tr.clear()
        assert tr._completed_tasks_time == []
        assert tr._completed_tasks_data == {}

        # UID is not expected to change
        assert tr.task_results_uid == uid3

    asyncio.run(testing())


def test_TaskResults_clear():
    """
    TaskResults: tests for ``clear``
    """

    async def testing():
        tr = TaskResults()

        await tr.add_running_task(task_uid="abc", payload={"some_value": "arbitrary_payload"})
        await tr.add_running_task(task_uid="def", payload={"some_value": "arbitrary_payload"})
        await tr.add_completed_task(task_uid="abc")

        assert len(tr._completed_tasks_time) == 1
        assert len(tr._completed_tasks_data) == 1
        assert len(tr._running_tasks) == 1

        uid = tr.task_results_uid
        await tr.clear()

        assert tr._running_tasks == {}
        assert tr._completed_tasks_time == []
        assert tr._completed_tasks_data == {}

        # UID is not expected to change
        assert tr.task_results_uid == uid

    asyncio.run(testing())


def test_TaskResults_clean_completed_tasks_1():
    """
    TaskResults: tests for ``clean_completed_tasks``
    """

    async def testing():
        tr = TaskResults(retention_time=1)  # Intentionally set short retention time

        await tr.add_completed_task(task_uid="abc")
        assert len(tr._completed_tasks_data) == 1
        assert len(tr._completed_tasks_time) == 1

        await tr.clean_completed_tasks()  # No effect
        assert len(tr._completed_tasks_data) == 1
        assert len(tr._completed_tasks_time) == 1

        ttime.sleep(0.8)

        # 'add_completed_task' is expected to 'clean' tha task list, but there are no expired tasks yet.
        await tr.add_completed_task(task_uid="def", payload={"some_value": 10})
        assert len(tr._completed_tasks_data) == 2
        assert len(tr._completed_tasks_time) == 2

        ttime.sleep(0.5)

        await tr.clean_completed_tasks()  # Should remove the 1st task
        assert len(tr._completed_tasks_data) == 1
        assert len(tr._completed_tasks_time) == 1

        ttime.sleep(0.8)
        await tr.clean_completed_tasks()  # Should remove the 2nd task
        assert len(tr._completed_tasks_data) == 0
        assert len(tr._completed_tasks_time) == 0

    asyncio.run(testing())


def test_TaskResults_clean_completed_tasks_2():
    """
    TaskResults: tests that ``clean_completed_tasks`` is implicitely called when completed task is added.
    """

    async def testing():
        tr = TaskResults(retention_time=1)  # Intentionally set short retention time

        await tr.add_completed_task(task_uid="abc")
        assert len(tr._completed_tasks_data) == 1
        assert len(tr._completed_tasks_time) == 1

        ttime.sleep(1.5)

        # Adds the 2nd task, but removes the 1st (because it is expired)
        await tr.add_completed_task(task_uid="def", payload={"some_value": 10})
        assert len(tr._completed_tasks_data) == 1
        assert len(tr._completed_tasks_time) == 1

        assert tr._completed_tasks_time[0]["task_uid"] == "def"
        assert list(tr._completed_tasks_data.keys())[0] == "def"

    asyncio.run(testing())


def test_TaskResults_get_task_info():
    """
    TaskResults: ``get_task_info``.
    """

    async def testing():
        tr = TaskResults(retention_time=1)  # Intentionally set short retention time

        await tr.add_running_task(task_uid="abc", payload={"some_value": 5})
        await tr.add_running_task(task_uid="def", payload={"some_value": 10})
        await tr.add_completed_task(task_uid="def", payload={"some_value": 20})

        status, payload = await tr.get_task_info(task_uid="abc")
        assert status == "running"
        assert payload == {"some_value": 5}

        status, payload = await tr.get_task_info(task_uid="def")
        assert status == "completed"
        assert payload == {"some_value": 20}

        status, payload = await tr.get_task_info(task_uid="gih")
        assert status == "not_found"
        assert payload == {}

    asyncio.run(testing())
