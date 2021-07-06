import time as ttime
import threading
import multiprocessing

from bluesky_queueserver.manager.output_streaming import PublishConsoleOutput, ReceiveConsoleOutput


def test_ReceiveConsoleOutput_1():

    zmq_port = 61223  # Arbitrary port
    zmq_topic = "testing_topic"

    zmq_publish_addr = f"tcp://*:{zmq_port}"
    zmq_subscribe_addr = f"tcp://localhost:{zmq_port}"

    queue = multiprocessing.Queue()

    pco = PublishConsoleOutput(
        msg_queue=queue,
        console_output_on=True,
        zmq_publish_on=True,
        zmq_publish_addr=zmq_publish_addr,
        zmq_topic=zmq_topic,
    )

    class ReceiveMessages(threading.Thread):
        def __init__(self, *, zmq_subscribe_addr, zmq_topic):
            super().__init__()
            self._rco = ReceiveConsoleOutput(zmq_subscribe_addr=zmq_subscribe_addr, zmq_topic=zmq_topic)
            self._exit = False
            self.received_msgs = []
            self.n_timeouts = 0

        def run(self):
            while True:
                try:
                    msg = self._rco.recv()
                    self.received_msgs.append(msg)
                except TimeoutError:
                    ++self.n_timeouts
                if self._exit:
                    break

        def stop(self):
            self._exit = True

    rm = ReceiveMessages(zmq_subscribe_addr=zmq_subscribe_addr, zmq_topic=zmq_topic)

    pco.start()
    rm.start()

    msgs = ["message-one", "message-two", "message-three"]
    for msg in msgs:
        queue.put({"time": ttime.time(), "msg": msg})
    ttime.sleep(0.1)

    pco.stop()
    rm.stop()
    rm.join()

    assert len(rm.received_msgs) == 3
    assert rm.n_timeouts == 0
