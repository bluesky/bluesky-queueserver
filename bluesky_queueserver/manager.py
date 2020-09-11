import asyncio
import json
import aioredis
import zmq
import zmq.asyncio
from multiprocessing import Process
import threading
import time as ttime
import pprint

from .worker import DB

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


class RunEngineManager(Process):

    def __init__(self, *, conn_watchdog, conn_worker):
        super().__init__(name="RE Manager Process")
        self._re_worker = None

        self._watchdog_conn = conn_watchdog
        self._worker_conn = conn_worker

        self._environment_exists = False

        # Threads must be started in the 'run' function so that they run in the correct process.
        self._thread_conn_worker = None
        self._thread_conn_watchdog = None

        self._loop = None

        # Communication with the server using ZMQ
        self._ctx = zmq.asyncio.Context()
        self._zmq_socket = None
        self._ip_zmq_server = "tcp://*:5555"

        self._r_pool = None

        self._heartbeat_generator_task = None  # Task for heartbeat generator

        self._event_worker_created = None
        self._event_worker_closed = None
        self._fut_is_worker_alive = None

    def _start_conn_threads(self):
        self._thread_conn_watchdog = threading.Thread(target=self._receive_packet_watchdog_thread,
                                                      name="RE QServer Comm1",
                                                      daemon=True)
        self._thread_conn_watchdog.start()
        self._thread_conn_worker = threading.Thread(target=self._receive_packet_worker_thread,
                                                    name="RE QServer Comm2",
                                                    daemon=True)
        self._thread_conn_worker.start()

    async def _heartbeat_generator(self):
        """
        Heartbeat generator for Watchdog (indicates that the loop is running)
        """
        t_period = 0.5
        msg = {"type": "notification", "value": "alive"}
        while True:
            await asyncio.sleep(t_period)
            self._watchdog_conn.send(msg)

    # ======================================================================
    #   Functions that implement functionality of the server

    async def _start_re_worker(self):
        """
        Creates worker process.
        """
        self._event_worker_created = asyncio.Event()

        msg = {"type": "command", "value": "start_re_worker"}
        self._watchdog_conn.send(msg)

        logger.debug("Waiting for RE worker to start ...")
        await self._event_worker_created.wait()
        logger.debug("Worker started successfully.")

    async def _started_re_worker(self):
        # Report from RE Worker received: environment was created successfully.
        self._event_worker_created.set()

    async def _stop_re_worker(self):
        """
        Closes Run Engine execution environment in orderly way. Running plan needs
        to be stopped before the environment can be closed.
        """
        self._event_worker_closed = asyncio.Event()

        msg = {"type": "command", "value": "quit"}
        self._worker_conn.send(msg)

        logger.debug("Waiting for RE Worker to close ...")
        await self._event_worker_closed.wait()
        logger.debug("RE Worker to closed")

        msg = {"type": "command", "value": "join_re_worker"}
        self._watchdog_conn.send(msg)

    async def _stopped_re_worker(self):
        # Report from RE Worker received: environment was closed successfully.
        self._event_worker_closed.set()

    async def _is_worker_alive(self):
        self._fut_is_worker_alive = self._loop.create_future()

        msg = {"type": "request", "value": "is_worker_alive"}
        self._watchdog_conn.send(msg)

        return await self._fut_is_worker_alive

    async def _is_worker_alive_received(self, is_alive):
        self._fut_is_worker_alive.set_result(is_alive)

    async def _run_task(self):
        """
        Upload the plan to the worker process for execution.
        Plan in the queue is represented as a dictionary with the keys "name" (plan name),
        "args" (list of args), "kwargs" (list of kwargs). Only the plan name is mandatory.
        Names of plans and devices are strings.
        """
        n_pending_plans = await self._r_pool.llen('plan_queue')
        logger.info("Starting a new plan: %d plans are left in the queue" % n_pending_plans)

        new_plan = await self._r_pool.lpop('plan_queue')
        if new_plan is not None:
            await self._r_pool.set('running_plan', json.dumps(new_plan))
            new_plan = json.loads(new_plan)

            plan_name = new_plan["name"]
            args = new_plan["args"] if "args" in new_plan else []
            kwargs = new_plan["kwargs"] if "kwargs" in new_plan else {}

            msg = {"type": "plan",
                   "value": {"name": plan_name,
                             "args": args,
                             "kwargs": kwargs
                             }
                   }

            self._worker_conn.send(msg)
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
        self._worker_conn.send(msg)

    def _continue_run_engine(self, option):
        """
        Continue handling of a paused plan.
        """
        msg = {"type": "command", "value": "continue", "option": option}
        self._worker_conn.send(msg)

    def _print_db_uids(self):
        """
        Prints the UIDs of the scans in 'temp' database. Just for the demo.
        Not part of future API.
        """
        print("\n===================================================================")
        print("             The contents of 'temp' database.")
        print("-------------------------------------------------------------------")
        n_runs = 0
        db_instance = DB[0]
        for run_id in range(1, 100000):
            try:
                hdr = db_instance[run_id]
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

    def _receive_packet_watchdog_thread(self):
        while True:
            if self._watchdog_conn.poll(0.1):
                try:
                    msg = self._watchdog_conn.recv()
                    logger.debug("Message Watchdog->Manager received: '%s'" % pprint.pformat(msg))
                    # Messages should be handled in the event loop
                    self._loop.call_soon_threadsafe(self._conn_watchdog_received, msg)
                except Exception as ex:
                    logger.exception("Exception occurred while waiting for packet: %s" % str(ex))
                    break

    def _receive_packet_worker_thread(self):
        while True:
            if self._worker_conn.poll(0.1):
                try:
                    msg = self._worker_conn.recv()
                    logger.debug("Message received from RE Worker: %s" % pprint.pformat(msg))
                    # Messages should be handled in the event loop
                    self._loop.call_soon_threadsafe(self._conn_worker_received, msg)
                except Exception as ex:
                    logger.error("Exception occurred while waiting for packet: %s" % str(ex))
                    break

    def _conn_worker_received(self, msg):
        async def process_message(msg):
            type, value = msg["type"], msg["value"]

            if type == "report":
                action = value["action"]
                if action == "plan_exit":
                    completed = value["completed"]
                    success = value["success"]
                    result = value["result"]
                    logger.info("Report received from RE Worker:\nsuccess=%s\n%s\n)" %
                                (str(success), str(result)))
                    if completed and success:
                        # Executed plan is removed from the queue only after it is successfully completed.
                        # If a plan was not completed or not successful (exception was raised), then
                        # execution of the queue is stopped. It can be restarted later (failed or
                        # interrupted plan will still be in the queue.
                        await self._r_pool.set('running_plan', '')
                        await self._run_task()

                elif action == "environment_created":
                    await self._started_re_worker()
                elif action == "environment_closed":
                    await self._stopped_re_worker()

            if type == "acknowledge":
                status = value["status"]
                result = value["result"]
                msg_original = value["msg"]
                logger.info("Acknownegement received from RE Worker:\n"
                            "Status: '%s'\nResult: '%s'\nMessage: %s"
                            % (str(status), str(result), pprint.pformat(msg_original)))

        asyncio.create_task(process_message(msg))

    def _conn_watchdog_received(self, msg):
        async def process_message(msg):
            type, value = msg["type"], msg["value"]
            if type == "result":
                request = value["request"]
                if request == "is_worker_alive":
                    await self._is_worker_alive_received(value["result"])
            if type == "report":
                logger.info("Report Watchdog->Re Manager was received: %s" % pprint.pformat(msg))

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
        logger.info("Adding new plan to the queue: %s" % pprint.pformat(request))
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
            await self._start_re_worker()
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
            await self._stop_re_worker()
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

    async def _kill_manager_handler(self, request):
        # This is expected to block the event loop forever
        while True:
            ttime.sleep(10)

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
            "kill_manager": "_kill_manager_handler",
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

        self._start_conn_threads()

        # Set the environment state based on whether the worker process is alive (request Watchdog)
        self._environment_exists = await self._is_worker_alive()

        # Start heartbeat generator
        self._heartbeat_generator_task = asyncio.ensure_future(self._heartbeat_generator(), loop=self._loop)

        self._r_pool = await aioredis.create_redis_pool(
            'redis://localhost', encoding='utf8')

        logger.info("Starting ZeroMQ server")
        self._zmq_socket = self._ctx.socket(zmq.REP)
        self._zmq_socket.bind(self._ip_zmq_server)
        logger.info("ZeroMQ server is waiting on %s" % str(self._ip_zmq_server))

        while True:
            #  Wait for next request from client
            msg_in = await self._zmq_receive()
            logger.info("ZeroMQ server received request: %s" % pprint.pformat(msg_in))

            msg_out = await self._zmq_execute(msg_in)

            #  Send reply back to client
            logger.info("ZeroMQ server sending response: %s" % pprint.pformat(msg_out))
            await self._zmq_send(msg_out)

    # ------------------------------------------------------------

    def run(self):
        """
        Overrides the `run()` function of the `multiprocessing.Process` class. Called
        by the `start` method.
        """
        logger.info("Starting RE Manager process")
        try:
            asyncio.run(self.zmq_server_comm())
        except Exception as ex:
            logger.exception("Exiting RE Manager with exception %s" % str(ex))
        except KeyboardInterrupt:
            # TODO: RE Manager must be orderly closed before Watchdog module is stopped.
            #   Right now it is just killed by SIGINT.
            logger.info("RE Manager Process was stopped by SIGINT. Handling of Ctrl-C has to be revised!!!")
