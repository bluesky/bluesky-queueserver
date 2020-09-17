import threading
import uuid
import time as ttime
import json

from bluesky_queueserver.manager.start_manager import WatchdogProcess


def format_jsonrpc_msg(method, params=None, *, notification=False):
    """Returns dictionary that contains JSON RPC message"""
    msg = {"method": method, "jsonrpc": "2.0"}
    if params is not None:
        msg["params"] = params
    if not notification:
        msg["id"] = uuid.uuid4()
    return msg


class ReManagerEmulation(threading.Thread):
    """
    Emulation of RE Manager, which is using Thread instead of Process.
    The functionality of the emulator is to test if Watchdog can start
    and restart RE Manager properly. The emulator also generates periodic
    'heartbeat' messages to inform RE Manager that it is running.
    """

    def __init__(self, *args, conn_watchdog, conn_worker, **kwargs):
        super().__init__(*args, **kwargs)
        self._conn_watchdog = conn_watchdog
        self._n_loops = 0
        self._exit = False
        self._restart = False
        self._send_heartbeat = True

    def _heartbeat(self):
        hb_period, dt = 0.5, 0.01
        n_wait = round(hb_period / dt)
        msg = format_jsonrpc_msg("heartbeat", {"value": "alive"}, notification=True)
        msg_json = json.dumps(msg)
        while True:
            # Since we are emulating 'kill' method, we want the function to
            #   react to 'exit' quickly.
            for n in range(n_wait):
                ttime.sleep(0.005)
                if self._exit:
                    return
            if self._send_heartbeat:
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
            self._n_loops += 1

        if not self._restart:
            msg = format_jsonrpc_msg("manager_stopping", notification=True)
            self._conn_watchdog.send(json.dumps(msg))

        th_hb.join()


def test_WatchdogProcess_1():
    """Test starting and orderly existing the RE Manager"""
    wp = WatchdogProcess(cls_run_engine_manager=ReManagerEmulation)
    wp_th = threading.Thread(target=wp.run)
    wp_th.start()
    ttime.sleep(1)  # Let RE Manager run 1 second
    assert wp._re_manager._n_loops > 0, "RE is not running"
    wp._re_manager.exit(restart=False)
    ttime.sleep(0.05)
    assert wp._manager_is_stopping is True, "'Manager Stopping' flag is not set"
    wp_th.join(0.1)


def test_WatchdogProcess_2():
    """Test starting RE Manager, stopping heartbeat generator
    and waiting for restart of RE Manager"""
    wp = WatchdogProcess(cls_run_engine_manager=ReManagerEmulation)
    wp_th = threading.Thread(target=wp.run)
    wp_th.start()
    ttime.sleep(1)  # Let RE Manager run 1 second
    assert wp._re_manager._n_loops > 0, "RE is not running"
    n_loops = wp._re_manager._n_loops

    wp._re_manager.stop_heartbeat()
    hb_timeout = wp._heartbeat_timeout
    ttime.sleep(hb_timeout + 0.5)
    # At this point RE Manager is expected to run for 0.5 second, so
    #   the new number of loops must be about 'n_loops/2'.
    #   Here we check if RE Manager was really restarted and the number of
    #   loops reset.
    assert wp._re_manager._n_loops < n_loops, "Unexpected number of loops"

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
    ttime.sleep(1)  # Let RE Manager run 1 second
    assert wp._re_manager._n_loops > 0, "RE is not running"
    n_loops = wp._re_manager._n_loops

    # Stop RE Manager without notifying the Watchdog (emulates crashing of RE Manager)
    wp._re_manager.exit(restart=True)
    hb_timeout = wp._heartbeat_timeout
    ttime.sleep(hb_timeout + 0.5)
    # At this point RE Manager is expected to run for 0.5 second, so
    #   the new number of loops must be about 'n_loops/2'.
    #   Here we check if RE Manager was really restarted and the number of
    #   loops reset.
    assert wp._re_manager._n_loops < n_loops, "Unexpected number of loops"

    wp._re_manager.exit(restart=False)
    ttime.sleep(0.05)
    assert wp._manager_is_stopping is True, "'Manager Stopping' flag is not set"
    wp_th.join(0.1)
