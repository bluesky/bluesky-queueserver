import time as ttime
import subprocess
import asyncio
import pytest

from bluesky_queueserver.manager.qserver_cli import CliClient


@pytest.fixture
def re_manager():
    """
    Start RE Manager as a subprocess. Tests will communicate with RE Manager via ZeroMQ.
    """
    p = subprocess.Popen(["start-re-manager"], universal_newlines=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    yield  # Nothing to return

    # Try to stop the manager in a nice way first by sending the command
    subprocess.call(["qserver", "-c", "stop_manager"])
    try:
        p.wait(5)
    except subprocess.TimeoutExpired:
        # The manager is not responsive, so just kill the process.
        p.kill()
        assert False, "RE Manager failed to stop"


def test_qserver_cli_and_manager(re_manager):
    # TODO: Redis pool should be cleaned before each test. Now it's only one tests,
    #   so it is not important, but cleaning should be implemented.

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
        queue_is_running = (msg["manager_state"] == "executing_queue")
        return plans_in_queue, queue_is_running

    def condition_manager_idle(msg):
        return msg["manager_state"] == "idle"

    def condition_environment_created(msg):
        return msg["worker_environment_exists"]

    def condition_environment_closed(msg):
        return not msg["worker_environment_exists"]

    def condition_queue_processing_finished(msg):
        plans_in_queue = msg["plans_in_queue"]
        queue_is_running = (msg["manager_state"] == "executing_queue")
        return (plans_in_queue == 0) and not queue_is_running

    def wait_for_condition(time, condition):
        """
        Wait until queue is processed. Note: processing of TimeoutError is needed for
        monitoring RE Manager while it is restarted.
        """
        dt = 0.5  # Period for checking the queue status
        time_stop = ttime.time() + time
        while ttime.time() < time_stop:
            ttime.sleep(dt/2)
            try:
                msg = get_queue_state()
                if condition(msg):
                    return True
            except TimeoutError:
                pass
            ttime.sleep(dt/2)
        return False

    assert wait_for_condition(time=3, condition=condition_manager_idle), \
        "Timeout while waiting for manager to initialize."

    # Clear queue
    subprocess.call(["qserver", "-c", "clear_queue"])

    # Add a number of plans
    subprocess.call(["qserver", "-c", "add_to_queue", "-p",
                     "{'name':'count', 'args':[['det1', 'det2']]}"])
    subprocess.call(["qserver", "-c", "add_to_queue", "-p",
                     "{'name':'scan', 'args':[['det1', 'det2'], 'motor', -1, 1, 10]}"])
    subprocess.call(["qserver", "-c", "add_to_queue", "-p",
                     "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':10, 'delay':1}}"])

    n_plans, is_plan_running = get_reduced_state_info()
    assert n_plans == 3, "Incorrect number of plans in the queue"
    assert not is_plan_running, "Plan is executed while it shouldn't"

    subprocess.call(["qserver", "-c", "queue_view"])
    subprocess.call(["qserver", "-c", "pop_from_queue"])

    n_plans, is_plan_running = get_reduced_state_info()
    assert n_plans == 2, "Incorrect number of plans in the queue"

    subprocess.call(["qserver", "-c", "create_environment"])

    assert wait_for_condition(time=3, condition=condition_environment_created), \
        "Timeout while waiting for environment to be created"

    subprocess.call(["qserver", "-c", "process_queue"])

    assert wait_for_condition(time=60, condition=condition_queue_processing_finished), \
        "Timeout while waiting for process to finish"

    # Queue is expected to be empty (processed). Load one more plan.
    subprocess.call(["qserver", "-c", "add_to_queue", "-p",
                     "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':10, 'delay':1}}"])

    n_plans, is_plan_running = get_reduced_state_info()
    assert n_plans == 1, "Incorrect number of plans in the queue"

    subprocess.call(["qserver", "-c", "process_queue"])
    ttime.sleep(1)
    subprocess.call(["qserver", "-c", "re_pause", "-p", "immediate"])
    subprocess.call(["qserver", "-c", "re_continue", "-p", "resume"])
    ttime.sleep(1)
    subprocess.call(["qserver", "-c", "re_pause", "-p", "deferred"])
    ttime.sleep(2)  # Need some time to finish the current plan step
    subprocess.call(["qserver", "-c", "re_continue", "-p", "resume"])

    assert wait_for_condition(time=60, condition=condition_queue_processing_finished), \
        "Timeout while waiting for process to finish"

    subprocess.call(["qserver", "-c", "add_to_queue", "-p",
                     "{'name':'count', 'args':[['det1', 'det2']]}"])
    subprocess.call(["qserver", "-c", "add_to_queue", "-p",
                     "{'name':'count', 'args':[['det1', 'det2']]}"])

    n_plans, is_plan_running = get_reduced_state_info()
    assert n_plans == 2, "Incorrect number of plans in the queue"

    subprocess.call(["qserver", "-c", "process_queue"])

    assert wait_for_condition(time=60, condition=condition_queue_processing_finished), \
        "Timeout while waiting for process to finish"

    # Test 'killing' the manager during running plan. Load long plan and two short ones.
    #   The tests checks if execution of the queue is continued uninterrupted after
    #   the manager restart
    subprocess.call(["qserver", "-c", "add_to_queue", "-p",
                     "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':10, 'delay':1}}"])
    subprocess.call(["qserver", "-c", "add_to_queue", "-p",
                     "{'name':'count', 'args':[['det1', 'det2']]}"])
    subprocess.call(["qserver", "-c", "add_to_queue", "-p",
                     "{'name':'count', 'args':[['det1', 'det2']]}"])
    n_plans, is_plan_running = get_reduced_state_info()
    assert n_plans == 3, "Incorrect number of plans in the queue"
    subprocess.call(["qserver", "-c", "process_queue"])
    ttime.sleep(1)
    subprocess.call(["qserver", "-c", "kill_manager"])
    assert wait_for_condition(time=60, condition=condition_queue_processing_finished), \
        "Timeout while waiting for process to finish"

    subprocess.call(["qserver", "-c", "close_environment"])

    assert wait_for_condition(time=5, condition=condition_environment_closed), \
        "Timeout while waiting for environment to be closed"
