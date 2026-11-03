import os
import time as ttime

import pytest

from bluesky_queueserver.manager.profile_ops import clear_registered_items

from .common import _test_sqlite_path


@pytest.fixture(scope="session", autouse=True)
def remove_test_sqlite_db():
    yield
    for path in (_test_sqlite_path, _test_sqlite_path + "-journal"):
        if os.path.exists(path):
            os.remove(path)


@pytest.fixture(autouse=True)
def setup_and_teardown_for_every_test():
    print("Clearing registered items ...")
    clear_registered_items()
    yield
    print("Clearing registered items ...")
    clear_registered_items()


@pytest.fixture(autouse=True)
def close_dangling_event_loops():
    """
    Dangling loops are mostly due to RE instances loaded as part of startup script.
    RE does not explicitly stop the running loop.
    """
    import asyncio
    import gc

    yield
    gc.collect()
    loops = [obj for obj in gc.get_objects() if isinstance(obj, asyncio.AbstractEventLoop) and not obj.is_closed()]
    for loop in loops:
        if loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
    if loops:
        ttime.sleep(0.1)  # allow running loops to stop before closing
    for loop in loops:
        if not loop.is_closed() and not loop.is_running():
            loop.close()


@pytest.fixture(scope="session", autouse=True)
def print_open_file_descriptors(request):
    yield
    ttime.sleep(1)
    pid = os.getpid()
    fd_dir = f"/proc/{pid}/fd"
    fd_entries = sorted(os.listdir(fd_dir), key=int)
    msg = f"+++ PID={pid} OPEN FILE DESCRIPTORS = {len(fd_entries)}"
    # terminalreporter works on CI (it is supposed to work locally, but it doesn't)
    reporter = request.config.pluginmanager.get_plugin("terminalreporter")
    reporter.write_line(msg)
    # /dev/tty bypasses all pytest capture layers (local use only)
    try:
        with open("/dev/tty", "w") as tty:
            tty.write("\n" + msg + "\n")
    except OSError:
        import sys

        sys.stderr.write("\n" + msg + "\n")
        sys.stderr.flush()
