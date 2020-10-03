import time as ttime
import subprocess
import asyncio
import pytest

from bluesky_queueserver.manager.qserver_cli import CliClient
from bluesky_queueserver.manager.plan_queue_ops import PlanQueueOperations


def get_queue_state():
    re_server = CliClient()
    command, params = "ping", None
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


@pytest.fixture
def re_manager():
    """
    Start RE Manager as a subprocess. Tests will communicate with RE Manager via ZeroMQ.
    """

    def clear_redis_pool():
        # Remove all Redis entries.
        pq = PlanQueueOperations()

        async def run():
            await pq.start()
            await pq.delete_pool_entries()

        asyncio.run(run())

    clear_redis_pool()
    p = subprocess.Popen(
        ["start-re-manager"], universal_newlines=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    yield  # Nothing to return

    # Try to stop the manager in a nice way first by sending the command
    subprocess.call(["qserver", "-c", "stop_manager"])
    try:
        p.wait(5)
        clear_redis_pool()
    except subprocess.TimeoutExpired:
        # The manager is not responsive, so just kill the process.
        p.kill()
        clear_redis_pool()
        assert False, "RE Manager failed to stop"


def test_qserver_cli_and_manager(re_manager):
    """
    Long test runs a series of CLI commands.
    """
    assert wait_for_condition(
        time=3, condition=condition_manager_idle
    ), "Timeout while waiting for manager to initialize."

    # Clear queue
    assert subprocess.call(["qserver", "-c", "clear_queue"]) == 0

    # Request the list of allowed plans and devices (we don't check what is returned)
    assert subprocess.call(["qserver", "-c", "plans_allowed"], stdout=subprocess.DEVNULL) == 0
    assert subprocess.call(["qserver", "-c", "devices_allowed"], stdout=subprocess.DEVNULL) == 0

    # Add a number of plans
    plan_1 = "{'name':'count', 'args':[['det1', 'det2']]}"
    plan_2 = "{'name':'scan', 'args':[['det1', 'det2'], 'motor', -1, 1, 10]}"
    plan_3 = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':10, 'delay':1}}"
    assert subprocess.call(["qserver", "-c", "add_to_queue", "-p", plan_1]) == 0
    assert subprocess.call(["qserver", "-c", "add_to_queue", "-p", plan_2]) == 0
    assert subprocess.call(["qserver", "-c", "add_to_queue", "-p", plan_3]) == 0

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 3, "Incorrect number of plans in the queue"
    assert not is_plan_running, "Plan is executed while it shouldn't"

    assert subprocess.call(["qserver", "-c", "get_queue"]) == 0
    assert subprocess.call(["qserver", "-c", "pop_from_queue"]) == 0

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 2, "Incorrect number of plans in the queue"

    assert subprocess.call(["qserver", "-c", "environment_open"]) == 0
    assert wait_for_condition(
        time=3, condition=condition_environment_created
    ), "Timeout while waiting for environment to be created"

    assert subprocess.call(["qserver", "-c", "process_queue"]) == 0

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    # Smoke test for 'get_history' and 'clear_history'
    assert subprocess.call(["qserver", "-c", "get_history"]) == 0
    assert subprocess.call(["qserver", "-c", "clear_history"]) == 0

    # Queue is expected to be empty (processed). Load one more plan.
    assert subprocess.call(["qserver", "-c", "add_to_queue", "-p", plan_3]) == 0

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 1, "Incorrect number of plans in the queue"

    assert subprocess.call(["qserver", "-c", "process_queue"]) == 0
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

    subprocess.call(["qserver", "-c", "add_to_queue", "-p", plan_1])
    subprocess.call(["qserver", "-c", "add_to_queue", "-p", plan_1])

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 2, "Incorrect number of plans in the queue"

    assert subprocess.call(["qserver", "-c", "process_queue"]) == 0

    assert wait_for_condition(
        time=60, condition=condition_queue_processing_finished
    ), "Timeout while waiting for process to finish"

    # Test 'killing' the manager during running plan. Load long plan and two short ones.
    #   The tests checks if execution of the queue is continued uninterrupted after
    #   the manager restart
    assert subprocess.call(["qserver", "-c", "add_to_queue", "-p", plan_3]) == 0
    assert subprocess.call(["qserver", "-c", "add_to_queue", "-p", plan_3]) == 0
    assert subprocess.call(["qserver", "-c", "add_to_queue", "-p", plan_3]) == 0
    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 3, "Incorrect number of plans in the queue"

    assert subprocess.call(["qserver", "-c", "process_queue"]) == 0
    ttime.sleep(1)
    assert subprocess.call(["qserver", "-c", "kill_manager"]) != 0
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
    assert subprocess.call(["qserver", "-c", "clear_queue"]) == 0

    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':5, 'delay':1}}"
    assert subprocess.call(["qserver", "-c", "add_to_queue", "-p", plan]) == 0

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 1, "Incorrect number of plans in the queue"
    assert is_plan_running is False

    assert subprocess.call(["qserver", "-c", "environment_open"]) == 0
    assert wait_for_condition(
        time=3, condition=condition_environment_created
    ), "Timeout while waiting for environment to be opened"

    assert subprocess.call(["qserver", "-c", "process_queue"]) == 0
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
    assert subprocess.call(["qserver", "-c", "clear_queue"]) == 0

    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':5, 'delay':1}}"
    assert subprocess.call(["qserver", "-c", "add_to_queue", "-p", plan]) == 0

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 1, "Incorrect number of plans in the queue"
    assert is_plan_running is False

    assert subprocess.call(["qserver", "-c", "environment_open"]) == 0
    assert wait_for_condition(
        time=3, condition=condition_environment_created
    ), "Timeout while waiting for environment to be opened"

    assert subprocess.call(["qserver", "-c", "process_queue"]) == 0
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

    assert subprocess.call(["qserver", "-c", "process_queue"]) == 0
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
    assert subprocess.call(["qserver", "-c", "clear_queue"]) == 0

    plan = "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':5, 'delay':1}}"
    assert subprocess.call(["qserver", "-c", "add_to_queue", "-p", plan]) == 0
    assert subprocess.call(["qserver", "-c", "add_to_queue", "-p", plan]) == 0

    n_plans, is_plan_running, _ = get_reduced_state_info()
    assert n_plans == 2, "Incorrect number of plans in the queue"
    assert is_plan_running is False

    assert subprocess.call(["qserver", "-c", "environment_open"]) == 0
    assert wait_for_condition(
        time=3, condition=condition_environment_created
    ), "Timeout while waiting for environment to be opened"

    assert subprocess.call(["qserver", "-c", "process_queue"]) == 0
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

        assert subprocess.call(["qserver", "-c", "process_queue"]) == 0

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
