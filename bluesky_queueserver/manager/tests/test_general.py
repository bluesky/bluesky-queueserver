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
    p = subprocess.Popen(["start-re-manager"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

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

    def get_queue_status():
        re_server = CliClient()
        command, value = "ping", None
        re_server.set_msg_out(command, value)
        asyncio.run(re_server.zmq_single_request())
        msg, _ = re_server.get_msg_in()
        n_plans, is_plan_running = msg["n_plans"], msg["is_plan_running"]
        return n_plans, is_plan_running

    def wait_for_processing_to_finish(time):
        """Wait until queue is processed"""
        dt = 1  # Period for checking the queue status
        n_attempts = int(time/dt)  # Number of attempts
        for n in range(n_attempts):
            ttime.sleep(dt/2)
            n_plans, is_plan_running = get_queue_status()
            if (n_plans == 0) and not is_plan_running:
                return True
            ttime.sleep(dt/2)
        return False

    # Clear queue
    subprocess.call(["qserver", "-c", "clear_queue"])

    # Add a number of plans
    subprocess.call(["qserver", "-c", "add_to_queue", "-v",
                     "{'name':'count', 'args':[['det1', 'det2']]}"])
    subprocess.call(["qserver", "-c", "add_to_queue", "-v",
                     "{'name':'scan', 'args':[['det1', 'det2'], 'motor', -1, 1, 10]}"])
    subprocess.call(["qserver", "-c", "add_to_queue", "-v",
                     "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':10, 'delay':1}}"])

    n_plans, is_plan_running = get_queue_status()
    assert n_plans == 3, "Incorrect number of plans in the queue"
    assert not is_plan_running, "Plan is executed while it shouldn't"

    subprocess.call(["qserver", "-c", "queue_view"])
    subprocess.call(["qserver", "-c", "pop_from_queue"])

    n_plans, is_plan_running = get_queue_status()
    assert n_plans == 2, "Incorrect number of plans in the queue"

    subprocess.call(["qserver", "-c", "create_environment"])
    subprocess.call(["qserver", "-c", "process_queue"])

    assert wait_for_processing_to_finish(60), "Timeout while waiting for process to finish"

    # Queue is expected to be empty (processed). Load one more plan.
    subprocess.call(["qserver", "-c", "add_to_queue", "-v",
                     "{'name':'count', 'args':[['det1', 'det2']], 'kwargs':{'num':10, 'delay':1}}"])

    n_plans, is_plan_running = get_queue_status()
    assert n_plans == 1, "Incorrect number of plans in the queue"

    subprocess.call(["qserver", "-c", "process_queue"])
    ttime.sleep(1)
    subprocess.call(["qserver", "-c", "re_pause", "-v", "immediate"])
    subprocess.call(["qserver", "-c", "re_continue", "-v", "resume"])
    ttime.sleep(1)
    subprocess.call(["qserver", "-c", "re_pause", "-v", "deferred"])
    ttime.sleep(2)  # Need some time to finish the current plan step
    subprocess.call(["qserver", "-c", "re_continue", "-v", "resume"])

    assert wait_for_processing_to_finish(60), "Timeout while waiting for process to finish"

    subprocess.call(["qserver", "-c", "add_to_queue", "-v",
                     "{'name':'count', 'args':[['det1', 'det2']]}"])
    subprocess.call(["qserver", "-c", "add_to_queue", "-v",
                     "{'name':'count', 'args':[['det1', 'det2']]}"])

    n_plans, is_plan_running = get_queue_status()
    assert n_plans == 2, "Incorrect number of plans in the queue"

    subprocess.call(["qserver", "-c", "process_queue"])

    assert wait_for_processing_to_finish(60), "Timeout while waiting for process to finish"

    subprocess.call(["qserver", "-c", "close_environment"])
