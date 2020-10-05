import time as ttime
import subprocess
import asyncio
import pytest

from bluesky_queueserver.manager.qserver_cli import CliClient
from bluesky_queueserver.manager.plan_queue_ops import PlanQueueOperations

from ._common import copy_default_profile_collection, patch_first_startup_file, patch_first_startup_file_undo


def get_queue_state():
    re_server = CliClient()
    command, params = "status", None
    re_server.set_msg_out(command, params)
    asyncio.run(re_server.zmq_single_request())
    msg, _ = re_server.get_msg_in()
    if msg is None:
        raise TimeoutError("Timeout occured while monitoring RE Manager state")
    return msg


def get_queue():
    """
    Returns current queue.
    """
    re_server = CliClient()
    command, params = "queue_get", None
    re_server.set_msg_out(command, params)
    asyncio.run(re_server.zmq_single_request())
    msg, _ = re_server.get_msg_in()
    if msg is None:
        raise TimeoutError("Timeout occured while monitoring RE Manager state")
    return msg


def get_reduced_state_info():
    msg = get_queue_state()
    plans_in_queue = msg["plans_in_queue"]
    queue_is_running = msg["manager_state"] == "executing_queue"
    plans_in_history = msg["plans_in_history"]
    return plans_in_queue, queue_is_running, plans_in_history


def condition_manager_idle(msg):
    return msg["manager_state"] == "idle"


def condition_manager_paused(msg):
    return msg["manager_state"] == "paused"


def condition_environment_created(msg):
    return msg["worker_environment_exists"]


def condition_environment_closed(msg):
    return not msg["worker_environment_exists"]


def condition_queue_processing_finished(msg):
    plans_in_queue = msg["plans_in_queue"]
    queue_is_running = msg["manager_state"] == "executing_queue"
    return (plans_in_queue == 0) and not queue_is_running


def wait_for_condition(time, condition):
    """
    Wait until queue is processed. Note: processing of TimeoutError is needed for
    monitoring RE Manager while it is restarted.
    """
    dt = 0.5  # Period for checking the queue status
    time_stop = ttime.time() + time
    while ttime.time() < time_stop:
        ttime.sleep(dt / 2)
        try:
            msg = get_queue_state()
            if condition(msg):
                return True
        except TimeoutError:
            pass
        ttime.sleep(dt / 2)
    return False


def clear_redis_pool():
    # Remove all Redis entries.
    pq = PlanQueueOperations()

    async def run():
        await pq.start()
        await pq.delete_pool_entries()

    asyncio.run(run())


class ReManager:
    def __init__(self, params=None):
        self._p = None
        self._start_manager(params)

    def _start_manager(self, params=None):
        """
        Start RE manager.

        Parameters
        ----------
        params: list(str)
            The list of command line parameters passed to the manager at startup

        Returns
        -------
        subprocess.Popen
            Subprocess in which the manager is running. It needs to be stopped at certain point.
        """
        if not self._p:
            clear_redis_pool()

            cmd = ["start-re-manager"]
            if params:
                cmd += params
            self._p = subprocess.Popen(
                cmd, universal_newlines=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )

    def check_if_stopped(self, timeout=5):
        """
        Waits for the process to be stopped. Returns ``True`` when it is stopped.
        and ``False`` otherwise.
        """
        try:
            # Make sure that the process is terminated
            self._p.wait(5)
            # Disable additional attempts to stop the process
            self._p = None
            return True
        except subprocess.TimeoutExpired:
            return False

    def stop_manager(self, timeout=5):
        """
        Attempt to exit the subprocess that is running manager in orderly way and kill it
        after timeout.

        Parameters
        ----------
        p: subprocess.Popen
            Subprocess in which the manager is running
        timeout: float
            Timeout in seconds.
        """
        if self._p:
            # Try to stop the manager in a nice way first by sending the command
            subprocess.call(["qserver", "-c", "manager_stop"])
            try:
                self._p.wait(timeout)
                clear_redis_pool()

            except subprocess.TimeoutExpired:
                # The manager is not responsive, so just kill the process.
                self._p.kill()
                clear_redis_pool()
                assert False, "RE Manager failed to stop"

            self._p = None


@pytest.fixture
def re_manager():
    """
    Start RE Manager as a subprocess. Tests will communicate with RE Manager via ZeroMQ.
    """
    re = ReManager()
    yield re  # Nothing to return
    re.stop_manager()


@pytest.fixture
def re_manager_pc_copy(tmp_path):
    """
    Start RE Manager as a subprocess. Tests will communicate with RE Manager via ZeroMQ.
    Copy profile collection and return its temporary path.
    """
    pc_path = copy_default_profile_collection(tmp_path)
    re = ReManager(["-p", pc_path])
    yield re, pc_path  # Location of the copy of the default profile collection.
    re.stop_manager()


def test_qserver_cli_and_manager(re_manager):
    """
    Long test runs a series of CLI commands.
    """
    assert wait_for_condition(
        time=3, condition=condition_manager_idle
    ), "Timeout while waiting for manager to initialize."

    # Clear queue
    assert subprocess.call(["qserver", "-c", "queue_clear"]) == 0

    # Request the list of allowed plans and devices (we don't check what is returned)
    assert subprocess.call(["qserver", "-c", "plans_allowed"], stdout=subprocess.DEVNULL) == 0
    assert subprocess.call(["qserver", "-c", "devices_allowed"], stdout=subprocess.DEVNULL) == 0

    # Add a number of plans
    plan_1 = "{'name':'count', 'args':[['det1', 'det2']]}"
    plan_2 = "{'name':'scan', 'args':[['det1', 'det2'], 'motor', -1, 1, 10]}"
    plan_3 = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':10, 'delay':1}}"
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan_1]) == 0
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan_2]) == 0
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan_3]) == 0

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 3, "Incorrect number of plans in the queue"
    assert not is_plan_running, "Plan is executed while it shouldn't"

    assert subprocess.call(["qserver", "-c", "queue_get"]) == 0
    assert subprocess.call(["qserver", "-c", "queue_plan_remove"]) == 0

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 2, "Incorrect number of plans in the queue"

    assert subprocess.call(["qserver", "-c", "environment_open"]) == 0
    assert wait_for_condition(
        time=3, condition=condition_environment_created
    ), "Timeout while waiting for environment to be created"

    assert subprocess.call(["qserver", "-c", "queue_start"]) == 0

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    # Smoke test for 'history_get' and 'history_clear'
    assert subprocess.call(["qserver", "-c", "history_get"]) == 0
    assert subprocess.call(["qserver", "-c", "history_clear"]) == 0

    # Queue is expected to be empty (processed). Load one more plan.
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan_3]) == 0

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 1, "Incorrect number of plans in the queue"

    assert subprocess.call(["qserver", "-c", "queue_start"]) == 0
    ttime.sleep(1)
    assert subprocess.call(["qserver", "-c", "re_pause", "-p", "immediate"]) == 0
    assert wait_for_condition(
        time=60, condition=condition_manager_paused
    ), "Timeout while waiting for manager to pause"

    assert subprocess.call(["qserver", "-c", "re_resume"]) == 0
    ttime.sleep(1)
    assert subprocess.call(["qserver", "-c", "re_pause", "-p", "deferred"]) == 0
    assert wait_for_condition(
        time=60, condition=condition_manager_paused
    ), "Timeout while waiting for manager to pause"

    assert subprocess.call(["qserver", "-c", "re_resume"]) == 0

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan_1])
    subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan_1])

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 2, "Incorrect number of plans in the queue"

    assert subprocess.call(["qserver", "-c", "queue_start"]) == 0

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    # Test 'killing' the manager during running plan. Load long plan and two short ones.
    #   The tests checks if execution of the queue is continued uninterrupted after
    #   the manager restart
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan_3]) == 0
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan_3]) == 0
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan_3]) == 0
    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 3, "Incorrect number of plans in the queue"

    assert subprocess.call(["qserver", "-c", "queue_start"]) == 0
    ttime.sleep(1)
    assert subprocess.call(["qserver", "-c", "manager_kill"]) != 0
    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    assert subprocess.call(["qserver", "-c", "environment_close"]) == 0
    assert wait_for_condition(
        time=5, condition=condition_environment_closed
    ), "Timeout while waiting for environment to be closed"


def test_qserver_environment_close(re_manager):
    """
    Test for `environment_close` command
    """
    assert wait_for_condition(
        time=3, condition=condition_manager_idle
    ), "Timeout while waiting for manager to initialize."

    # Clear queue
    assert subprocess.call(["qserver", "-c", "queue_clear"]) == 0

    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':5, 'delay':1}}"
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan]) == 0

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 1, "Incorrect number of plans in the queue"
    assert is_plan_running is False

    assert subprocess.call(["qserver", "-c", "environment_open"]) == 0
    assert wait_for_condition(
        time=3, condition=condition_environment_created
    ), "Timeout while waiting for environment to be opened"

    assert subprocess.call(["qserver", "-c", "queue_start"]) == 0
    ttime.sleep(2)
    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is True

    # Call is expected to fail, because a plan is currently running
    assert subprocess.call(["qserver", "-c", "environment_close"]) != 0

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    n_plans, is_plan_running, n_history = get_reduced_state_info()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is False
    assert n_history == 1

    # Now we can close the environment because plan execution is complete
    assert subprocess.call(["qserver", "-c", "environment_close"]) == 0
    assert wait_for_condition(
        time=5, condition=condition_environment_closed
    ), "Timeout while waiting for environment to be closed"


def test_qserver_environment_destroy(re_manager):
    """
    Test for `environment_destroy` command
    """
    assert wait_for_condition(
        time=3, condition=condition_manager_idle
    ), "Timeout while waiting for manager to initialize."

    # Clear queue
    assert subprocess.call(["qserver", "-c", "queue_clear"]) == 0

    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':5, 'delay':1}}"
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan]) == 0

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 1, "Incorrect number of plans in the queue"
    assert is_plan_running is False

    assert subprocess.call(["qserver", "-c", "environment_open"]) == 0
    assert wait_for_condition(
        time=3, condition=condition_environment_created
    ), "Timeout while waiting for environment to be opened"

    assert subprocess.call(["qserver", "-c", "queue_start"]) == 0
    ttime.sleep(2)
    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is True

    assert subprocess.call(["qserver", "-c", "environment_destroy"]) == 0
    assert wait_for_condition(
        time=3, condition=condition_manager_idle
    ), "Timeout while waiting for environment to be destroyed."

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 1, "Incorrect number of plans in the queue"
    assert is_plan_running is False

    assert subprocess.call(["qserver", "-c", "environment_open"]) == 0
    assert wait_for_condition(
        time=3, condition=condition_environment_created
    ), "Timeout while waiting for environment to be opened"

    assert subprocess.call(["qserver", "-c", "queue_start"]) == 0
    ttime.sleep(2)
    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is True

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    n_plans, is_plan_running, n_history = get_reduced_state_info()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is False
    assert n_history == 2

    assert subprocess.call(["qserver", "-c", "environment_close"]) == 0
    assert wait_for_condition(
        time=5, condition=condition_environment_closed
    ), "Timeout while waiting for environment to be closed"


# fmt: off
@pytest.mark.parametrize("option_pause, option_continue", [
    ("deferred", "resume"),
    ("immediate", "resume"),
    ("deferred", "stop"),
    ("deferred", "abort"),
    ("deferred", "halt")
])
# fmt: on
def test_qserver_re_pause_continue(re_manager, option_pause, option_continue):
    """
    Test for `re_pause`, `re_resume`, `re_stop`, `re_abort` and `re_halt` commands
    """
    re_continue = f"re_{option_continue}"

    assert wait_for_condition(
        time=3, condition=condition_manager_idle
    ), "Timeout while waiting for manager to initialize."

    # Out of place calls
    assert subprocess.call(["qserver", "-c", re_continue]) != 0
    assert subprocess.call(["qserver", "-c", "re_pause", "-p", option_pause]) != 0

    # Clear queue
    assert subprocess.call(["qserver", "-c", "queue_clear"]) == 0

    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num': 10, 'delay': 1}}"
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan]) == 0
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan]) == 0

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 2, "Incorrect number of plans in the queue"
    assert is_plan_running is False

    assert subprocess.call(["qserver", "-c", "environment_open"]) == 0
    assert wait_for_condition(
        time=3, condition=condition_environment_created
    ), "Timeout while waiting for environment to be opened"

    assert subprocess.call(["qserver", "-c", "queue_start"]) == 0
    ttime.sleep(2)

    # Out of place calls
    assert subprocess.call(["qserver", "-c", re_continue]) != 0

    assert subprocess.call(["qserver", "-c", "re_pause", "-p", option_pause]) == 0
    assert wait_for_condition(
        time=3, condition=condition_manager_paused
    ), "Timeout while waiting for manager to pause"

    status = get_queue_state()
    assert status["manager_state"] == "paused"

    n_plans, is_plan_running, n_history = get_reduced_state_info()
    assert n_plans == 1, "Incorrect number of plans in the queue"
    assert is_plan_running is False
    assert n_history == 0

    # Out of place call
    assert subprocess.call(["qserver", "-c", "re_pause", "-p", option_pause]) != 0

    assert subprocess.call(["qserver", "-c", re_continue]) == 0

    if option_continue == "resume":
        n_history_expected = 2
    else:
        assert wait_for_condition(time=3, condition=condition_manager_idle)

        n_plans, is_plan_running, n_history = get_reduced_state_info()
        assert n_plans == 2, "Incorrect number of plans in the queue"
        assert is_plan_running is False
        assert n_history == 1

        assert subprocess.call(["qserver", "-c", "queue_start"]) == 0

        n_history_expected = 3  # Includes entry related to 1 stopped plan

    ttime.sleep(1)

    n_plans, is_plan_running, n_history = get_reduced_state_info()
    assert n_plans == 1, "Incorrect number of plans in the queue"
    assert is_plan_running is True
    assert n_history == n_history_expected - 2

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    n_plans, is_plan_running, n_history = get_reduced_state_info()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is False
    assert n_history == n_history_expected

    assert subprocess.call(["qserver", "-c", "environment_close"]) == 0
    assert wait_for_condition(
        time=5, condition=condition_environment_closed
    ), "Timeout while waiting for environment to be closed"


# fmt: off
@pytest.mark.parametrize("time_kill", ["before", 2, 8, "paused"])
# fmt: on
def test_qserver_manager_kill(re_manager, time_kill):
    """
    Test for `test_manager_kill` command. The command is stopping the event loop of RE Manager,
    causeing RE Watchdog to restart it. RE Manager can be restarted at any time: the restart
    should not affect executed plans or the state of the queue or RE Worker. Response to this
    command is never returned, so it can also be used to test how the system handles communication
    timeouts. It takes 5 seconds of RE Manager inactivity befor it is restarted. The following cases
    are tested:
    - RE Manager is killed and restarted before queue processing is started.
    - RE Manager is killed and restarted while the 1st plan in the queue is executed.
    - RE Manager is killed while the 1st plan is still executed and is not restarted before
    the plan execution is finished. RE Manager is supposed to recognize that the plan is completed,
    process the report and start processing of the next plan.
    - RE Manager is killed and restarted while the 1st plan is in 'paused' state. RE Manager is
    supposed to switch to 'paused' state at the restart. The plan can execution can be resumed.
    """
    assert wait_for_condition(
        time=3, condition=condition_manager_idle
    ), "Timeout while waiting for manager to initialize."

    # Clear queue
    assert subprocess.call(["qserver", "-c", "queue_clear"]) == 0

    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num': 10, 'delay': 1}}"
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan]) == 0
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan]) == 0

    assert subprocess.call(["qserver", "-c", "environment_open"]) == 0
    assert wait_for_condition(
        time=3, condition=condition_environment_created
    ), "Timeout while waiting for environment to be opened"

    if time_kill == "before":
        # The command that kills manager always times out
        assert subprocess.call(["qserver", "-c", "manager_kill"]) != 0
        ttime.sleep(8)  # It takes 5 seconds before the manager is restarted

        status = get_queue_state()
        assert status["manager_state"] == "idle"

    # Start queue processing
    assert subprocess.call(["qserver", "-c", "queue_start"]) == 0

    if isinstance(time_kill, int):
        ttime.sleep(time_kill)
        # The command that kills manager always times out
        assert subprocess.call(["qserver", "-c", "manager_kill"]) != 0
        ttime.sleep(8)  # It takes 5 seconds before the manager is restarted

        status = get_queue_state()
        assert status["manager_state"] == "executing_queue"

    elif time_kill == "paused":
        ttime.sleep(3)
        assert subprocess.call(["qserver", "-c", "re_pause", "-p", "deferred"]) == 0
        assert wait_for_condition(time=3, condition=condition_manager_paused)
        assert subprocess.call(["qserver", "-c", "manager_kill"]) != 0
        ttime.sleep(8)  # It takes 5 seconds before the manager is restarted

        status = get_queue_state()
        assert status["manager_state"] == "paused"

        assert subprocess.call(["qserver", "-c", "re_resume"]) == 0

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    n_plans, is_plan_running, n_history = get_reduced_state_info()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is False
    assert n_history == 2

    assert subprocess.call(["qserver", "-c", "environment_close"]) == 0
    assert wait_for_condition(
        time=5, condition=condition_environment_closed
    ), "Timeout while waiting for environment to be closed"


# fmt: off
@pytest.mark.parametrize("additional_code, success", [
    # Nothing is added. Load profiles as usual.
    ("""
""", True),

    # Simulate profile that takes long time to load.
    ("""
\n
import time as ttime
ttime.sleep(20)

""", True),

    # Raise exception while loading the profile. This should cause RE Worker to exit.
    ("""
\n
raise Exception("This exception is raised to test if error handling works correctly")

""", False),
])
# fmt: on
def test_qserver_env_open_various_cases(re_manager_pc_copy, additional_code, success):

    _, pc_path = re_manager_pc_copy

    # Patch one of the startup files.
    patch_first_startup_file(pc_path, additional_code)

    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    # Attempt to create the environment
    assert subprocess.call(["qserver", "-c", "environment_open"]) == 0
    assert wait_for_condition(time=30, condition=condition_manager_idle)

    status = get_queue_state()
    assert status["worker_environment_exists"] == success

    if not success:
        # Remove the offending patch and try to start the environment again. It should work
        patch_first_startup_file_undo(pc_path)
        assert subprocess.call(["qserver", "-c", "environment_open"]) == 0
        assert wait_for_condition(time=3, condition=condition_environment_created)

    # Run a plan to make sure RE Manager is functional after the startup.
    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num': 10, 'delay': 1}}"
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan]) == 0

    # Start queue processing
    assert subprocess.call(["qserver", "-c", "queue_start"]) == 0
    ttime.sleep(2)
    status = get_queue_state()
    assert status["manager_state"] == "executing_queue"

    assert wait_for_condition(time=60, condition=condition_queue_processing_finished)
    n_plans, is_plan_running, n_history = get_reduced_state_info()
    assert n_plans == 0, "Incorrect number of plans in the queue"
    assert is_plan_running is False
    assert n_history == 1

    assert subprocess.call(["qserver", "-c", "environment_close"]) == 0
    assert wait_for_condition(time=5, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("option", [None, "safe_on", "safe_off"])
# fmt: on
def test_qserver_manager_stop_1(re_manager, option):
    """
    Method ``manager_stop``. Environment is in 'idle' state.
    """
    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    # Attempt to create the environment
    assert subprocess.call(["qserver", "-c", "environment_open"]) == 0
    assert wait_for_condition(time=30, condition=condition_manager_idle)

    cmd = ["qserver", "-c", "manager_stop"]
    if option:
        cmd += ["-p", option]

    assert subprocess.call(cmd) == 0

    # Check if RE Manager was stopped.
    assert re_manager.check_if_stopped() is True


# fmt: off
@pytest.mark.parametrize("option", [None, "safe_on", "safe_off"])
# fmt: on
def test_qserver_manager_stop_2(re_manager, option):
    """
    Method ``manager_stop``. Environment is running a plan.
    """
    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    # Attempt to create the environment
    assert subprocess.call(["qserver", "-c", "environment_open"]) == 0
    assert wait_for_condition(time=30, condition=condition_manager_idle)

    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num': 10, 'delay': 1}}"
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan]) == 0
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan]) == 0

    assert subprocess.call(["qserver", "-c", "queue_start"]) == 0
    ttime.sleep(2)
    status = get_queue_state()
    assert status["manager_state"] == "executing_queue"

    cmd = ["qserver", "-c", "manager_stop"]
    if option:
        cmd += ["-p", option]

    if option == "safe_off":
        assert subprocess.call(cmd) == 0

        # Check if RE Manager was stopped.
        assert re_manager.check_if_stopped() is True

    else:
        assert subprocess.call(cmd) != 0

        assert wait_for_condition(time=60, condition=condition_queue_processing_finished)
        n_plans, is_plan_running, n_history = get_reduced_state_info()
        assert n_plans == 0, "Incorrect number of plans in the queue"
        assert is_plan_running is False
        assert n_history == 2


# fmt: off
@pytest.mark.parametrize("pos, pos_result, success", [
    (None, 2, True),
    ("back", 2, True),
    ("front", 0, True),
    ("some", None, False),
    (0, 0, True),
    (1, 1, True),
    (2, 2, True),
    (3, 2, True),
    (100, 2, True),
    (-1, 1, True),
    (-2, 0, True),
    (-3, 0, True),
    (-100, 0, True),
])
# fmt: on
def test_queue_plan_add_1(re_manager, pos, pos_result, success):  # noqa F811

    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    plan1 = "{'name':'count', 'args':[['det1']]}"
    plan2 = "{'name':'count', 'args':[['det1', 'det2']]}"

    # Create the queue with 2 entries
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan1]) == 0
    assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", plan1]) == 0

    # Add another entry at the specified position
    params = [plan2]
    if pos is not None:
        params.append(str(pos))

    res = subprocess.call(["qserver", "-c", "queue_plan_add", "-p", *params])
    if success:
        assert res == 0
    else:
        assert res != 0

    resp = get_queue()
    assert len(resp["queue"]) == (3 if success else 2)

    if success:
        assert resp["queue"][pos_result]["args"] == [["det1", "det2"]]
        assert "plan_uid" in resp["queue"][pos_result]


# fmt: off
@pytest.mark.parametrize("pos", [None, "back"])
# fmt: on
def test_queue_plan_add_2_fail(re_manager, pos):  # noqa F811
    """
    No plan is supplied.
    """
    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle)

    if pos:
        assert subprocess.call(["qserver", "-c", "queue_plan_add", "-p", pos]) != 0
    else:
        assert subprocess.call(["qserver", "-c", "queue_plan_add"]) != 0
