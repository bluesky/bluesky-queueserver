import json
import logging
import threading
import time as ttime

from bluesky_queueserver.manager.start_manager import WatchdogProcess
from bluesky_queueserver.tests.common import format_jsonrpc_msg

# Must match the settings in WatchdogProcess class
_use_json = False


class ReManagerEmulation(threading.Thread):
    """
    Emulation of RE Manager, which is using Thread instead of Process.
    The functionality of the emulator is to test if Watchdog can start
    and restart RE Manager properly. The emulator also generates periodic
    'heartbeat' messages to inform RE Manager that it is running.
    """

    def __init__(
        self,
        *args,
        conn_watchdog,
        conn_worker,
        config=None,
        msg_queue=None,
        log_level=logging.DEBUG,
        number_of_restarts,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._conn_watchdog = conn_watchdog
        self.n_loops = 0
        self._exit = False
        self._restart = False
        self._send_heartbeat = True
        self._lock = threading.Lock()
        self._config_dict = config or {}
        self._log_level = log_level
        self._number_of_restarts = number_of_restarts

    def _heartbeat(self):
        hb_period, dt = 0.5, 0.01
        n_wait = round(hb_period / dt)
        msg = format_jsonrpc_msg("heartbeat", {"value": "alive"}, notification=True)
        msg_json = json.dumps(msg) if _use_json else msg
        while True:
            # Since we are emulating 'kill' method, we want the function to
            #   react to 'exit' quickly.
            for n in range(n_wait):
                ttime.sleep(0.005)
                if self._exit:
                    return
            if self._send_heartbeat:
                with self._lock:
                    self._conn_watchdog.send(msg_json)

    def exit(self, *, restart=False):
        """
        Stop the emulated RE Manager (exit the 'run' method). Set 'restart=True'
        to skip informing Watchdog that the exit is intentional: Watchdog is expected
        to restart the process.
        """
        self._restart = restart
        self._exit = True

    def kill(self):
        """
        This is emulation of 'kill' method of mp.Process. The method just normally
        exists the current process.
        """
        self.exit(restart=True)

    def send_msg_to_watchdog(self, method, params=None, *, notification=False, timeout=0.5):
        # The function may block all communication for the period of 'timeout', but
        #   this is acceptable for testing. Timeout would typically indicate an error.
        msg = format_jsonrpc_msg(method, params, notification=notification)
        with self._lock:
            msg_json = json.dumps(msg) if _use_json else msg
            self._conn_watchdog.send(msg_json)
            if notification:
                return
            if self._conn_watchdog.poll(timeout):
                response_json = self._conn_watchdog.recv()
                response = json.loads(response_json) if _use_json else response_json
                result = response["result"]
            else:
                result = None
        return result

    def stop_heartbeat(self):
        """
        Heatbeat generator may be stopped to emulate 'freezing' of the event loop of RE Manager.
        """
        self._send_heartbeat = False

    def run(self):
        th_hb = threading.Thread(target=self._heartbeat)
        th_hb.start()

        while not self._exit:
            ttime.sleep(0.01)
            self.n_loops += 1

        if not self._restart:
            msg = format_jsonrpc_msg("manager_stopping", notification=True)
            msg_json = json.dumps(msg) if _use_json else msg
            with self._lock:
                self._conn_watchdog.send(msg_json)

        th_hb.join()


class ReWorkerEmulation(threading.Thread):
    def __init__(
        self,
        *args,
        conn,
        config=None,
        msg_queue=None,
        log_level=logging.DEBUG,
        user_group_permissions=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._config_dict = config or {}
        self._exit = False
        self.n_loops = 0
        self._log_level = log_level

    def exit(self):
        self._exit = True

    def kill(self):
        self.exit()

    def run(self):
        while not self._exit:
            ttime.sleep(0.005)
            self.n_loops += 1


def test_WatchdogProcess_1():
    """Test starting and orderly existing the RE Manager"""
    wp = WatchdogProcess(cls_run_engine_manager=ReManagerEmulation)
    wp_th = threading.Thread(target=wp.run)
    wp_th.start()
    ttime.sleep(1)  # Let RE Manager run 1 second
    assert wp._re_manager.n_loops > 0, "RE is not running"
    wp._re_manager.exit(restart=False)
    ttime.sleep(0.05)
    assert wp._manager_is_stopping is True, "'Manager Stopping' flag is not set"
    wp_th.join(0.1)


def test_WatchdogProcess_2():
    """
    Test starting RE Manager, stopping heartbeat generator and waiting for restart of RE Manager.
    Check that the number of restarts of the manager process is properly counted and propagated.
    """
    wp = WatchdogProcess(cls_run_engine_manager=ReManagerEmulation)
    wp_th = threading.Thread(target=wp.run)
    wp_th.start()

    # Test with disabled watchdog, then enable it within the same test
    for wd_enabled in (False, True):
        print(f"wd_enabled = {wd_enabled}")

        if wd_enabled:
            ttime.sleep(0.01)
            response = wp._re_manager.send_msg_to_watchdog("watchdog_enable")
            assert response["success"] is True

        ttime.sleep(1)  # Let RE Manager run 1 second
        assert wp._re_manager.n_loops > 0, f"RE is not running, wd_enabled={wd_enabled}"
        assert wp._re_manager_n_restarts == 1

        n_loops = wp._re_manager.n_loops

        wp._re_manager.stop_heartbeat()
        hb_timeout = wp._heartbeat_timeout
        ttime.sleep(hb_timeout + 0.5)
        if wd_enabled:
            # Manager is restarted
            #
            # At this point RE Manager is expected to run for 0.5 second, so
            #   the new number of loops must be about 'n_loops/2'.
            #   Here we check if RE Manager was really restarted and the number of
            #   loops reset.
            assert wp._re_manager.n_loops < n_loops, "Unexpected number of loops"
            assert wp._re_manager_n_restarts == 2
        else:
            # Manager continues to run without restart
            assert wp._re_manager.n_loops > n_loops, "Unexpected number of loops"
            assert wp._re_manager_n_restarts == 1

    wp._re_manager.exit(restart=False)
    ttime.sleep(0.05)
    assert wp._manager_is_stopping is True, "'Manager Stopping' flag is not set"
    wp_th.join(0.1)


def test_WatchdogProcess_3():
    """Test starting RE Manager, exiting without sending notification and
    and waiting for the restart of RE Manager"""
    wp = WatchdogProcess(cls_run_engine_manager=ReManagerEmulation)
    wp_th = threading.Thread(target=wp.run)
    wp_th.start()

    ttime.sleep(0.01)
    response = wp._re_manager.send_msg_to_watchdog("watchdog_enable")
    assert response["success"] is True

    ttime.sleep(1)  # Let RE Manager run 1 second
    assert wp._re_manager.n_loops > 0, "RE is not running"
    n_loops = wp._re_manager.n_loops

    # Stop RE Manager without notifying the Watchdog (emulates crashing of RE Manager)
    wp._re_manager.exit(restart=True)
    hb_timeout = wp._heartbeat_timeout
    ttime.sleep(hb_timeout + 0.5)
    # At this point RE Manager is expected to run for 0.5 second, so
    #   the new number of loops must be about 'n_loops/2'.
    #   Here we check if RE Manager was really restarted and the number of
    #   loops reset.
    assert wp._re_manager.n_loops < n_loops, "Unexpected number of loops"

    wp._re_manager.exit(restart=False)
    ttime.sleep(0.05)
    assert wp._manager_is_stopping is True, "'Manager Stopping' flag is not set"
    wp_th.join(0.1)


def test_WatchdogProcess_4():
    """
    Test if Watchdog correctly executing commands that control starting
    and stopping RE Worker.
    """
    wp = WatchdogProcess(cls_run_engine_manager=ReManagerEmulation, cls_run_engine_worker=ReWorkerEmulation)
    wp_th = threading.Thread(target=wp.run)
    wp_th.start()
    ttime.sleep(0.01)

    response = wp._re_manager.send_msg_to_watchdog("start_re_worker", params={"user_group_permissions": {}})
    assert response["success"] is True, f"Unexpected response from RE Manager: {response}"

    # Worker is expected to be alive
    response = wp._re_manager.send_msg_to_watchdog("is_worker_alive")
    assert response["worker_alive"] is True, f"Unexpected response from RE Manager: {response}"

    # Join running process (thread). Expected to timeout.
    # Note: here timeout should be set to be smaller than timeout for the message
    #   in 'send_msg_to_watchdog method.
    response = wp._re_manager.send_msg_to_watchdog("join_re_worker", {"timeout": 0.1})
    assert response["success"] is False, f"Unexpected response from RE Manager: {response}"

    # Worker is expected to be alive
    response = wp._re_manager.send_msg_to_watchdog("is_worker_alive")
    assert response["worker_alive"] is True, "Unexpected response from RE Manager"

    # Exit the process (thread).
    wp._re_worker.exit()
    ttime.sleep(0.01)

    # Worker is expected to be stopped
    response = wp._re_manager.send_msg_to_watchdog("is_worker_alive")
    assert response["worker_alive"] is False, "Unexpected response from RE Manager"

    response = wp._re_manager.send_msg_to_watchdog("join_re_worker", {"timeout": 0.5})
    assert response["success"] is True, "Unexpected response from RE Manager"

    wp._re_manager.exit(restart=False)
    wp_th.join(0.1)


def test_WatchdogProcess_5():
    """
    Test 'kill_re_worker' command RE Worker.
    """
    wp = WatchdogProcess(cls_run_engine_manager=ReManagerEmulation, cls_run_engine_worker=ReWorkerEmulation)
    wp_th = threading.Thread(target=wp.run)
    wp_th.start()
    ttime.sleep(0.01)

    response = wp._re_manager.send_msg_to_watchdog("start_re_worker", params={"user_group_permissions": {}})
    assert response["success"] is True, "Unexpected response from RE Manager"

    # Worker is expected to be alive
    response = wp._re_manager.send_msg_to_watchdog("is_worker_alive")
    assert response["worker_alive"] is True, "Unexpected response from RE Manager"

    # Kill RE Worker process (emulated, since RE Worker is a thread)
    response = wp._re_manager.send_msg_to_watchdog("kill_re_worker")
    assert response["success"] is True, "Unexpected response from RE Manager"

    # Worker is expected to be stopped
    response = wp._re_manager.send_msg_to_watchdog("is_worker_alive")
    assert response["worker_alive"] is False, "Unexpected response from RE Manager"

    response = wp._re_manager.send_msg_to_watchdog("join_re_worker", {"timeout": 0.5})
    assert response["success"] is True, "Unexpected response from RE Manager"

    wp._re_manager.exit(restart=False)
    wp_th.join(0.1)


def test_WatchdogProcess_6():
    """
    Test if RE configuration is passed to RE Worker
    """
    config_worker = {"some_parameter1": "some_value1"}
    config_manager = {"some_parameter2": "some_value2"}

    wp = WatchdogProcess(
        config_worker=config_worker,
        config_manager=config_manager,
        cls_run_engine_manager=ReManagerEmulation,
        cls_run_engine_worker=ReWorkerEmulation,
    )
    wp_th = threading.Thread(target=wp.run)
    wp_th.start()
    ttime.sleep(0.01)

    response = wp._re_manager.send_msg_to_watchdog("start_re_worker", params={"user_group_permissions": {}})
    assert response["success"] is True, "Unexpected response from RE Manager"

    # Check if configuration was set correctly in RE Worker and RE manager
    assert wp._re_worker._config_dict == config_worker, "Worker configuration was not passed correctly"
    assert wp._re_manager._config_dict == config_manager, "Manager configuration was not passed correctly"

    # Exit the process (thread).
    wp._re_worker.exit()
    ttime.sleep(0.01)

    response = wp._re_manager.send_msg_to_watchdog("join_re_worker", {"timeout": 0.5})
    assert response["success"] is True, "Unexpected response from RE Manager"

    wp._re_manager.exit(restart=False)
    wp_th.join(0.1)


def test_WatchdogProcess_7():
    """
    Test if the Watchdog and Manager processes are initialized with correct logger
    """
    config_worker = {"some_parameter1": "some_value1"}
    config_manager = {"some_parameter2": "some_value2"}

    log_level = logging.INFO

    wp = WatchdogProcess(
        config_worker=config_worker,
        config_manager=config_manager,
        cls_run_engine_manager=ReManagerEmulation,
        cls_run_engine_worker=ReWorkerEmulation,
        log_level=log_level,
    )
    wp_th = threading.Thread(target=wp.run)
    wp_th.start()
    ttime.sleep(0.01)

    response = wp._re_manager.send_msg_to_watchdog("start_re_worker", params={"user_group_permissions": {}})
    assert response["success"] is True, "Unexpected response from RE Manager"

    # Check if configuration was set correctly in RE Worker and RE manager
    assert wp._re_worker._log_level == log_level
    assert wp._re_manager._log_level == log_level

    # Exit the process (thread).
    wp._re_worker.exit()
    ttime.sleep(0.01)

    response = wp._re_manager.send_msg_to_watchdog("join_re_worker", {"timeout": 0.5})
    assert response["success"] is True, "Unexpected response from RE Manager"

    wp._re_manager.exit(restart=False)
    wp_th.join(0.1)
