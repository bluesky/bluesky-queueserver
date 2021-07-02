import io
import sys
import time as ttime
import threading


class LogStream(io.TextIOBase):
    def __init__(self, *, msg_queue):
        """
        Parameters
        ----------
        msg_queue : multiprocessing.Queue
            Queue that is used to collect messages from processes
        """
        super().__init__()
        self._msg_queue = msg_queue
        self._stdout = sys.__stdout__

    def write(self, s):
        s = str(s)
        msg = {"time": ttime.time(), "msg": s}
        self._msg_queue.put(msg)
        return len(s)


def override_streams(file_obj):
    sys.stdout = file_obj
    sys.stderr = file_obj


class PublishStreamOutput:
    def __init__(self, *, msg_queue, name="Output Publisher"):
        self._thread_running = False  # Set True to exit the thread
        self._thread_name = name
        self._msg_queue = msg_queue
        self._polling_timeout = 0.1  # in sec.

    def start(self):
        """
        Start thread polling the queue.
        """
        self._start_processing_thread()

    def stop(self):
        """
        Stop thread that polls the queue (and exit the tread)
        """
        self._thread_running = False

    def __del__(self):
        self.stop()

    def _start_processing_thread(self):
        # The thread should not be started of Message Queue object does not exist
        if not self._thread_running and self._msg_queue:
            self._thread_running = True
            self._thread_conn = threading.Thread(
                target=self._publishing_thread, name=self._thread_name, daemon=True
            )
            self._thread_conn.start()

    def _publishing_thread(self):
        while True:
            try:
                msg = self._msg_queue.get(block=True, timeout=self._polling_timeout)
                self._publish(msg)
            except Exception:
                pass

            if not self._thread_running:  # Exit thread
                break

    def _publish(self, msg):
        sys.__stdout__.write(msg["msg"])
