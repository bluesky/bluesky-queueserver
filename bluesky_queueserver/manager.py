import asyncio
import json
import aioredis
import zmq
import zmq.asyncio
from multiprocessing import Pipe
import threading

from .worker import RunEngineWorker

from databroker import Broker

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

db_logger = logging.getLogger("databroker")
db_logger.setLevel(logging.INFO)


"""
#  The following plans that can be used to test the syste

http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]]}'

# This is the slowly running plan (convenient to test pausing)
http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]],
"kwargs":{"num":10, "delay":1}}'

http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"scan", "args":[["det1", "det2"], "motor", -1, 1, 10]}'
"""


class RunEngineManager:

    def __init__(self):
        self._re_worker = None
        self._manager_conn = None
        self._worker_conn = None

        self._environment_exists = False

        self._thread_conn = None

        self._loop = None

        self._start_conn_pipes()
        self._start_conn_thread()

        # Communication with the server using ZMQ
        self._ctx = zmq.asyncio.Context()
        self._zmq_socket = None
        self._ip_zmq_server = "tcp://*:5555"
        self._r_worker = None

        # Create Databroker instance. This reference is passed to RE Worker process.
        # The experimental data can later be retrieved from the database.
        # This subscription mechanism is strictly for the demo, since using reference
        # to the databroker in several processes may not be a good idea. Here it is assumed
        # that only one process access Databroker at a time.
        self._db = Broker.named('temp')

    def _start_conn_pipes(self):
        self._manager_conn, self._worker_conn = Pipe()

    def _start_conn_thread(self):
        self._thread_conn = threading.Thread(target=self._receive_packet_thread,
                                             name="RE Server Comm",
                                             daemon=True)
        self._thread_conn.start()

    # ======================================================================
    #   Functions that implement functionality of the server

    def _start_re_worker(self):
        """
        Creates worker process.
        """
        # Passing reference to Databroker to a different process may be not a great idea.
        # This is here strictly for the demo.
        self._re_worker = RunEngineWorker(conn=self._worker_conn, db=self._db)
        self._re_worker.start()

    def _stop_re_worker(self):
        """
        Closes Run Engine execution environment (destroys the worker process). Running plan needs
        to be stopped before the environment can be closed. Separate function could be added that could
        kill unresponsive process that can not be closed gracefully.
        """
        msg = {"type": "command", "value": "quit"}
        self._manager_conn.send(msg)
        self._re_worker.join()

    async def _run_task(self):
        """
        Upload the plan to the worker process for execution.
        Plan in the queue is represented as a dictionary with the keys "name" (plan name),
        "args" (list of args), "kwargs" (list of kwargs). Only the plan name is mandatory.
        Names of plans and devices are strings.
        """
        new_plan = await self._r_pool.lpop('plan_queue')
        if new_plan is not None:
            await self._r_pool.set('running_plan', json.dumps(new_plan))
            new_plan = json.loads(new_plan)
            pending_plans = await self._r_pool.llen('plan_queue')
            logger.info(f"Starting new plan: {pending_plans} plans are left in the queue")

            plan_name = new_plan["name"]
            args = new_plan["args"] if "args" in new_plan else []
            kwargs = new_plan["kwargs"] if "kwargs" in new_plan else {}

            msg = {"type": "plan",
                   "value": {"name": plan_name,
                             "args": args,
                             "kwargs": kwargs
                             }
                   }

            self._manager_conn.send(msg)
            return True
        else:
            logger.info("Queue is empty")
            return False

    def _pause_run_engine(self, option):
        """
        Pause execution of a running plan. Run Engine must be in 'running' state in order for
        the request to pause to be accepted by RE Worker.
        """
        msg = {"type": "command", "value": "pause", "option": option}
        self._manager_conn.send(msg)

    def _continue_run_engine(self, option):
        """
        Continue handling of a paused plan.
        """
        msg = {"type": "command", "value": "continue", "option": option}
        self._manager_conn.send(msg)

    def _print_db_uids(self):
        """
        Prints the UIDs of the scans in 'temp' database. Just for the demo.
        Not part of future API.
        """
        print("\n===================================================================")
        print("             The contents of 'temp' database.")
        print("-------------------------------------------------------------------")
        n_runs = 0
        for run_id in range(1, 100000):
            try:
                hdr = self._db[run_id]
                uid = hdr.start["uid"]
                n_runs += 1
                print(f"Run ID: {run_id}   UID: {uid}")
            except Exception:
                break
        print("-------------------------------------------------------------------")
        print(f"  Total of {n_runs} runs were found in 'temp' database.")
        print("===================================================================\n")

    # =======================================================================
    #   Functions for communication with the worker process

    def _receive_packet_thread(self):
        while True:
            if self._manager_conn.poll(0.1):
                try:
                    msg = self._manager_conn.recv()
                    logger.debug(f"Message received from RE Worker: {msg}")
                    # Messages should be handled in the event loop
                    self._loop.call_soon_threadsafe(self._conn_received, msg)
                except Exception as ex:
                    logger.error(f"Exception occurred while waiting for packet: {ex}")
                    break

    def _conn_received(self, msg):
        async def process_message(msg):
            type, value = msg["type"], msg["value"]

            if type == "report":
                completed = value["completed"]
                success = value["success"]
                result = value["result"]
                logger.info(f"Report received from RE Worker:\nsuccess={success}\n{result}\n)")
                if completed and success:
                    # Executed plan is removed from the queue only after it is successfully completed.
                    # If a plan was not completed or not successful (exception was raised), then
                    # execution of the queue is stopped. It can be restarted later (failed or
                    # interrupted plan will still be in the queue.
                    await self._r_pool.set('running_plan', '')
                    await self._run_task()

            if type == "acknowledge":
                status = value["status"]
                result = value["result"]
                msg_original = value["msg"]
                logger.info("Acknownegement received from RE Worker:\n"
                            f"Status: '{status}'\nResult:' {result}'\nMessage: {msg_original}")

        asyncio.create_task(process_message(msg))
    # =========================================================================
    #    REST API handlers

    async def _hello_handler(self, request):
        """
        May be called to get response from the Manager. Returns the number of plans in the queue.
        """
        logger.info("Processing 'Hello' request.")
        pending_plans = await self._r_pool.llen('plan_queue')
        msg = {"msg": f"Hello, world! There are {pending_plans} plans enqueed."}
        return msg

    async def _queue_view_handler(self, request):
        """
         Returns the contents of the current queue.
         """
        logger.info("Returning current queue.")
        all_plans = await self._r_pool.lrange('plan_queue', 0, -1)

        return {"queue": [json.loads(_) for _ in all_plans]}

    async def _add_to_queue_handler(self, request):
        """
        Adds new plan to the end of the queue
        """
        # TODO: validate inputs!
        logger.info(f"Adding new plan to the queue: {request}")
        if "plan" in request:
            plan = request["plan"]
            await self._r_pool.rpush('plan_queue', json.dumps(plan))
        else:
            plan = {}
        return plan

    async def _pop_from_queue_handler(self, request):
        """
        Pop the last item from back of the queue
        """
        logger.info("Popping the last item from the queue.")
        plan = await self._r_pool.rpop('plan_queue')
        if plan is not None:
            return json.loads(plan)
        else:
            return {}  # No items

    async def _create_environment_handler(self, request):
        """
        Creates RE environment: creates RE Worker process, starts and configures Run Engine.
        """
        logger.info("Creating the new RE environment.")
        if not self._environment_exists:
            self._start_re_worker()
            self._environment_exists = True
            success, msg = True, ""
        else:
            success, msg = False, "Environment already exists."
        return {"success": success, "msg": msg}

    async def _close_environment_handler(self, request):
        """
        Deletes RE environment. In the current 'demo' prototype the environment will be deleted
        only after RE completes the current scan.
        """
        logger.info("Closing current RE environment.")
        if self._environment_exists:
            self._stop_re_worker()
            self._environment_exists = False
            success, msg = True, ""
        else:
            success, msg = False, "Environment does not exist."
        return {"success": success, "msg": msg}

    async def _process_queue_handler(self, request):
        """
        Start execution of the loaded queue. Additional runs can be added to the queue while
        it is executed. If the queue is empty, then nothing will happen.
        """
        logger.info("Starting queue processing.")
        if self._environment_exists:
            await self._run_task()
            success, msg = True, ""
        else:
            success, msg = False, "Environment does not exist. Can not start the task."
        return {"success": success, "msg": msg}

    async def _re_pause_handler(self, request):
        """
        Pause Run Engine
        """
        logger.info("Pausing the queue (currently running plan).")
        option = request["option"] if "option" in request else None
        available_options = ("deferred", "immediate")
        if option in available_options:
            if self._environment_exists:
                self._pause_run_engine(option)
                success, msg = True, ""
            else:
                success, msg = False, "Environment does not exist. Can not pause Run Engine."
        else:
            success, msg = False, f"Option '{option}' is not supported. " \
                                  f"Available options: {available_options}"
        return {"success": success, "msg": msg}

    async def _re_continue_handler(self, request):
        """
        Control Run Engine in the paused state
        """
        logger.info("Continue paused queue (plan).")
        option = request["option"] if "option" in request else None
        available_options = ("resume", "abort", "stop", "halt")
        if option in available_options:
            if self._environment_exists:
                self._continue_run_engine(option)
                success, msg = True, ""
            else:
                success, msg = False, "Environment does not exist. Can not pause Run Engine."
        else:
            success, msg = False, f"Option '{option}' is not supported. " \
                                  f"Available options: {available_options}"
        return {"success": success, "msg": msg}

    async def _print_db_uids_handler(self, request):
        """
        Prints the UIDs of the scans in 'temp' database. Just for the demo.
        Not part of future API.
        """
        logger.info("Print UIDs of collected run ('temp' Databroker).")
        self._print_db_uids()
        return {"success": True, "msg": ""}

    async def _zmq_execute(self, msg):
        command = msg["command"]
        value = msg["value"]
        handler_dict = {
            "": "_hello_handler",
            "queue_view": "_queue_view_handler",
            "add_to_queue": "_add_to_queue_handler",
            "pop_from_queue": "_pop_from_queue_handler",
            "create_environment": "_create_environment_handler",
            "close_environment": "_close_environment_handler",
            "process_queue": "_process_queue_handler",
            "re_pause": "_re_pause_handler",
            "re_continue": "_re_continue_handler",
            "print_db_uids": "_print_db_uids_handler",
        }

        try:
            handler_name = handler_dict[command]
            handler = getattr(self, handler_name)
            result = await handler(value)
        except KeyError:
            result = {"success": False, "msg": f"Unknown command '{command}'"}
        except AttributeError:
            result = {"success": False, "msg": f"Handler for the command '{command}' is not implemented"}
        return result

    async def _zmq_receive(self):
        msg_in = await self._zmq_socket.recv_json()
        return msg_in

    async def _zmq_send(self, msg):
        await self._zmq_socket.send_json(msg)

    async def zmq_server_comm(self):
        """
        This function is supposed to be executed by asyncio.run() to start the manager.
        """
        self._loop = asyncio.get_running_loop()
        self._r_pool = await aioredis.create_redis_pool(
            'redis://localhost', encoding='utf8'
        )

        logger.info("Starting ZeroMQ server")
        self._zmq_socket = self._ctx.socket(zmq.REP)
        self._zmq_socket.bind(self._ip_zmq_server)
        logger.info(f"ZeroMQ server is waiting on {self._ip_zmq_server}")

        while True:
            #  Wait for next request from client
            msg_in = await self._zmq_receive()
            logger.info(f"ZeroMQ server received request: {msg_in}")

            msg_out = await self._zmq_execute(msg_in)

            #  Send reply back to client
            logger.info(f"ZeroMQ server sending response: {msg_out}")
            await self._zmq_send(msg_out)


if __name__ == "__main__":
    re_manager = RunEngineManager()
    try:
        asyncio.run(re_manager.zmq_server_comm())
    except KeyboardInterrupt:
        logger.info("The application was stopped")
