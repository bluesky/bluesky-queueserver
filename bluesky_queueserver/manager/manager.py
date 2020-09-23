import asyncio
import json
import aioredis
import zmq
import zmq.asyncio
from multiprocessing import Process
import time as ttime
import pprint
import uuid
import enum
from super_state_machine import machines

from .worker import DB
from .comms import PipeJsonRpcSendAsync, CommTimeoutError

import logging

logger = logging.getLogger(__name__)

"""
#  The following plans that can be used to test the syste

http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]]}'

# This is the slowly running plan (convenient to test pausing)
http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]],
"kwargs":{"num":10, "delay":1}}'

http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"scan", "args":[["det1", "det2"], "motor", -1, 1, 10]}'
"""


# TODO: this is incomplete set of states. Expect it to be expanded.
class ManagerState(machines.StateMachine):

    state = 'idle'

    class States(enum.Enum):
        IDLE = "idle"
        CREATING_ENVIRONMENT = "creating_environment"
        EXECUTING_QUEUE = "executing_queue"
        CLOSING_ENVIRONMENT = "closing_environment"


class RunEngineManager(Process):
    """
    The class implementing Run Engine Worker thread.

    Parameters
    ----------
    conn_watchdog: multiprocessing.Connection
        One end of bidirectional (input/output) for communication to Watchdog process.
    conn_worker: multiprocessing.Connection
        One end of bidirectional (input/output) for communication to RE Worker process.
    args, kwargs
        `args` and `kwargs` of the `multiprocessing.Process`
    """
    def __init__(self, *args, conn_watchdog, conn_worker, **kwargs):

        if not conn_watchdog:
            raise RuntimeError("Value of the parameter 'conn_watchdog' is invalid: %s.",
                               str(conn_watchdog))
        if not conn_worker:
            raise RuntimeError("Value of the parameter 'conn_worker' is invalid: %s.",
                               str(conn_worker))

        super().__init__(*args, **kwargs)

        self._watchdog_conn = conn_watchdog
        self._worker_conn = conn_worker

        # The following attributes hold the state of the system
        self._manager_stopping = False  # Set True to exit manager (by _stop_manager_handler)
        self._environment_exists = False  # True if RE Worker environment exists
        self._manager_state = ManagerState()
        self._worker_state = None  # Copy of the last downloaded state of RE Worker

        self._loop = None

        # Communication with the server using ZMQ
        self._ctx = None
        self._zmq_socket = None
        self._ip_zmq_server = "tcp://*:5555"

        self._r_pool = None

        self._heartbeat_generator_task = None  # Task for heartbeat generator
        self._worker_status_task = None  # Task for periodic checks of Worker status

        self._fut_manager_task_completed = None  # Used for multiple purposes

        # The objects of PipeJsonRpcSendAsync used for communciation with
        #   Watchdog and Worker modules. The object must be instantiated in the loop.
        self._comm_to_watchdog = None
        self._comm_to_worker = None

        # Data on a task executed in the background
        self._background_task = None  # asyncio.Task
        self._background_task_status = {"status": "success",
                                        "err_msg": ""}

    async def _heartbeat_generator(self):
        """
        Heartbeat generator for Watchdog (indicates that the loop is running)
        """
        t_period = 0.5
        while True:
            await asyncio.sleep(t_period)
            await self._watchdog_send_heartbeat()

    # ======================================================================
    #          Communication with Redis

    async def _set_running_plan_info(self, plan):
        """
        Write info on the currently running to Redis
        """
        await self._r_pool.set("running_plan", json.dumps(plan))

    async def _get_running_plan_info(self):
        """
        Read info on the currently running plan from Redis
        """
        return json.loads(await self._r_pool.get("running_plan"))

    async def _clear_running_plan_info(self):
        """
        Clear info on the currently running plan in Redis.
        """
        await self._set_running_plan_info({})

    async def _exists_running_plan_info(self):
        """
        Check if plan exists in the ppol
        """
        return await self._r_pool.exists("running_plan")

    async def _init_running_plan_info(self):
        """
        Initialize running plan info: create Redis entry that hold empty plan ({})
        a record doesn't exist.
        """
        # Create entry 'running_plan' in the pool if it does not exist yet
        if (not await self._exists_running_plan_info()) \
                or (not await self._get_running_plan_info()):
            await self._clear_running_plan_info()

    # ======================================================================
    #          Functions that implement functionality of the server

    async def _execute_background_task(self, bckg_fut):
        """
        Monitors execution of a task in the background. Catches unhandled exceptions and
        maintains the status of the background task.

        Parameters
        ----------
        bckg_task: asyncio.Task
            The task scheduled to execute on the background

        Examples
        --------

        .. code-block:: python

            # Call in the loop
            asyncio.ensure_future(self._execute_background_task(some_coroutine()))
        """
        self._background_task_status["status"] = "running"
        self._background_task_status["err_msg"] = False

        try:
            # 'self._background_task' may be used to cancel the task
            self._background_task = asyncio.ensure_future(bckg_fut)
            await self._background_task
            success, err_msg = self._background_task.result()
        except Exception as ex:
            success = False
            err_msg = f"Unhandled exception: {str(ex)}"
            logger.exception("Unhandled exception during background task execution: %s", str(ex))

        self._background_task_status["status"] = "success" if success else "failed"
        self._background_task_status["err_msg"] = err_msg

    async def _start_re_worker(self):
        """
        Initiate creation of RE Worker environment. The function does not wait until
        the environment is created. Returns True/False depending on whether
        the command is accepted.
        """
        if self._environment_exists:
            accepted = False
        else:
            accepted = True
            asyncio.ensure_future(self._execute_background_task(self._start_re_worker_task()))
        return accepted

    async def _start_re_worker_task(self):
        """
        Creates worker process.
        """
        if self._environment_exists:
            return False, "Rejected: environment already exists"

        self._fut_manager_task_completed = self._loop.create_future()
        self._manager_state.set_creating_environment()
        self._creating_environment = True

        try:
            success = await self._watchdog_start_re_worker()
            if not success:
                raise RuntimeError("Failed to create Worker process")
            logger.debug("Waiting for RE worker to start ...")
            await self._fut_manager_task_completed  # TODO: timeout may be needed here

            self._environment_exists = True
            logger.debug("Worker started successfully.")
            success, err_msg = True, ""
        except Exception as ex:
            logger.exception("Failed to start_Worker: %s", str(ex))
            success, err_msg = False, f"Failed to start_Worker {str(ex)}"

        self._manager_state.set_idle()
        return success, err_msg

    async def _stop_re_worker(self):
        """
        Initiate closing of RE Worker environment.
        """
        if self._environment_exists:
            accepted = True
            asyncio.ensure_future(self._execute_background_task(self._stop_re_worker_task()))
        else:
            accepted = False
        return accepted

    async def _stop_re_worker_task(self):
        """
        Stop RE Worker. Returns the result as "success", "rejected" or "failed"
        """
        if not self._environment_exists:
            return False, "Rejected: environment does not exists"

        self._fut_manager_task_completed = self._loop.create_future()

        success, err_msg = await self._worker_command_close_env()
        if success:
            # Wait for RE Worker to be prepared to close
            self._event_worker_closed = asyncio.Event()

            self._manager_state.set_closing_environment()
            await self._fut_manager_task_completed  # TODO: timeout may be needed here
            self._manager_state.set_idle()

            if not await self._confirm_re_worker_exit():
                success = False
                err_msg = "Failed to confirm closing of RE Worker thread"

        return success, err_msg

    async def _confirm_re_worker_exit(self):
        """
        Confirm RE worker exit and make sure the worker thread exits.
        """
        success = True
        if self._environment_exists:
            logger.debug("Waiting for exit confirmation from RE worker ...")
            success, err_msg = await self._worker_command_confirm_exit()

            # Environment is not in valid state anyway. So assume it does not exist.
            self._environment_exists = False
            if success:
                logger.debug("Wait for RE Worker process to close (join)")

                if not await self._watchdog_join_re_worker(timeout_join=0.5):
                    success, err_msg = False, "Failed to join RE Worker thread."
                    # TODO: this error should probably be handled differently than this,
                    #   since it may indicate that the worker process is stalled.
                    logger.error("Failed to properly join the worker process. "
                                 "The process may not be properly closed.")
            else:
                success, err_msg = False, "RE Worker failed to exit (no confirmation)"
        else:
            success, err_msg = False, "RE Worker environment does not exist"
        return success, err_msg

    async def _is_worker_alive(self):
        return await self._watchdog_is_worker_alive()

    async def _periodic_worker_state_request(self):
        """
        Periodically update locally stored RE Worker status
        """
        t_period = 0.5
        while True:
            await asyncio.sleep(t_period)
            if self._environment_exists or self._manager_state.is_creating_environment:
                ws, _ = await self._worker_request_state()
                self._worker_state = ws
                if self._manager_state.is_closing_environment:
                    if ws["environment_state"] == "closing":
                        self._fut_manager_task_completed.set_result(None)

                if self._manager_state.is_creating_environment:
                    if ws["environment_state"] == "ready":
                        self._fut_manager_task_completed.set_result(None)

                if self._manager_state.is_executing_queue:
                    if ws["re_report_available"]:
                        self._loop.create_task(self._process_plan_report())

    async def _process_plan_report(self):
        """
        Process plan report. Called when plan report is available.
        """
        async def push_plan_back_to_queue():
            p = await self._get_running_plan_info()
            await self._r_pool.lpush('plan_queue', json.dumps(p))
            await self._clear_running_plan_info()

        # Read report first
        plan_report, err_msg = await self._worker_request_plan_report()
        if plan_report is None:
            # TODO: this would typically mean a bug (communciation error). Probably more
            #       complicated processing is needed
            logger.error(f"Failed to download plan report: {err_msg}. Stopping queue processing.")
            await push_plan_back_to_queue()
            self._manager_state.set_idle()
        else:
            plan_state = plan_report["plan_state"]
            success = plan_report["success"]
            result = plan_report["result"]
            err_msg = plan_report["err_msg"]

            msg_display = result if result else err_msg
            logger.info("Report received from RE Worker:\n"
                        "plan_state=%s\n"
                        "success=%s\n%s\n)",
                        plan_state, str(success), str(msg_display))

            if plan_state == "completed":
                # Executed plan is removed from the queue only after it is successfully completed.
                # If a plan was not completed or not successful (exception was raised), then
                # execution of the queue is stopped. It can be restarted later (failed or
                # interrupted plan will still be in the queue.
                await self._clear_running_plan_info()
                await self._start_plan_task()
            elif plan_state in ("stopped", "error"):
                # Paused plan was stopped/aborted/halted
                await push_plan_back_to_queue()
                self._manager_state.set_idle()
            elif plan_state == "paused":
                # The plan was paused (nothing should be done).
                self._manager_state.set_idle()
            else:
                logger.error("Unknown plan state %s was returned by RE Worker.", plan_state)

    async def _start_plan(self):
        """
        Initiate creation of RE Worker environment. The function does not wait until
        the environment is created. Returns True/False depending on whether
        the command is accepted.
        """
        if not self._environment_exists:
            success, err_msg = False, "RE Worker environment does not exist."
        elif not self._manager_state.is_idle:
            success, err_msg = False, "RE Manager is busy."
        else:
            asyncio.ensure_future(self._execute_background_task(self._start_plan_task()))
            success, err_msg = True, ""
        return success, err_msg

    async def _start_plan_task(self):
        """
        Upload the plan to the worker process for execution.
        Plan in the queue is represented as a dictionary with the keys "name" (plan name),
        "args" (list of args), "kwargs" (list of kwargs). Only the plan name is mandatory.
        Names of plans and devices are strings.
        """
        n_pending_plans = await self._r_pool.llen('plan_queue')
        logger.info("Starting a new plan: %d plans are left in the queue", n_pending_plans)

        new_plan = await self._r_pool.lpop('plan_queue')
        if new_plan is not None:
            # Reset RE environment (worker)
            success, err_msg = await self._worker_command_reset_worker()
            if not success:
                self._manager_state.set_idle()
                err_msg = f"Failed to reset RE Worker: {err_msg}"
                logger.error(err_msg)
                return success, err_msg

            self._manager_state.set_executing_queue()

            new_plan = json.loads(new_plan)
            await self._set_running_plan_info(new_plan)

            plan_name = new_plan["name"]
            args = new_plan["args"] if "args" in new_plan else []
            kwargs = new_plan["kwargs"] if "kwargs" in new_plan else {}
            plan_uid = new_plan["plan_uid"]

            plan_info = {"name": plan_name,
                         "args": args,
                         "kwargs": kwargs,
                         "plan_uid": plan_uid,
                         }

            success, err_msg = await self._worker_command_run_plan(plan_info)
            if not success:
                self._manager_state.set_idle()
                logger.error("Failed to start the plan %s.\nError: %s",
                             pprint.pformat(plan_info), err_msg)
                err_msg = f"Failed to start the plan: {err_msg}"
        else:
            self._manager_state.set_idle()
            success, err_msg = False, "Queue is empty."
            logger.info(err_msg)

        return success, err_msg

    async def _pause_run_engine(self, option):
        """
        Pause execution of a running plan. Run Engine must be in 'running' state in order for
        the request to pause to be accepted by RE Worker.
        """
        success, err_msg = await self._worker_command_pause_plan(option)
        if not success:
            logger.error("Failed to pause the running plan: %s", err_msg)
        else:
            self._manager_state.set_executing_queue()
        return success, err_msg

    async def _continue_run_engine(self, option):
        """
        Continue handling of a paused plan.
        """
        success, err_msg = await self._worker_command_continue_plan(option)
        if not success:
            logger.error("Failed to pause the running plan: %s", err_msg)
        else:
            self._manager_state.set_executing_queue()
        return success, err_msg

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

    # ===============================================================================
    #         Functions that send commands/request data from Worker process

    async def _worker_request_state(self):
        try:
            re_state = await self._comm_to_worker.send_msg("request_state")
            err_msg = ""
        except CommTimeoutError:
            re_state, err_msg = None, "Timeout occurred"
        return re_state, err_msg

    async def _worker_request_plan_report(self):
        try:
            plan_report = await self._comm_to_worker.send_msg("request_plan_report")
            err_msg = ""
            if plan_report is None:
                err_msg = "Report is not available at RE Worker"
        except CommTimeoutError:
            plan_report, err_msg = None, "Timeout occurred"
        return plan_report, err_msg

    async def _worker_command_close_env(self):
        try:
            response = await self._comm_to_worker.send_msg("command_close_env")
            success = (response["status"] == "accepted")
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred"
        return success, err_msg

    async def _worker_command_confirm_exit(self):
        try:
            response = await self._comm_to_worker.send_msg("command_confirm_exit")
            success = (response["status"] == "accepted")
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred"
        return success, err_msg

    async def _worker_command_run_plan(self, plan_info):
        try:
            response = await self._comm_to_worker.send_msg("command_run_plan",
                                                           {"plan_info": plan_info})
            success = (response["status"] == "accepted")
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred"
        return success, err_msg

    async def _worker_command_pause_plan(self, option):
        try:
            response = await self._comm_to_worker.send_msg("command_pause_plan",
                                                           {"option": option})
            success = (response["status"] == "accepted")
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred"
        return success, err_msg

    async def _worker_command_continue_plan(self, option):
        try:
            response = await self._comm_to_worker.send_msg("command_continue_plan",
                                                           {"option": option})
            success = (response["status"] == "accepted")
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred"
        return success, err_msg

    async def _worker_command_reset_worker(self):
        try:
            response = await self._comm_to_worker.send_msg("command_reset_worker")
            success = (response["status"] == "accepted")
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred"
        return success, err_msg

    # ===============================================================================
    #         Functions that send commands/request data from Watchdog process

    async def _watchdog_start_re_worker(self):
        """
        Initiate the startup of the RE Worker. Returned 'success==True' means that the process
        was created successfully and RE environment initialization is started.
        """
        try:
            response = await self._comm_to_watchdog.send_msg("start_re_worker")
            success = response["success"]
        except CommTimeoutError:
            success = False
        # TODO: add processing of CommJsonRpcError and RuntimeError to all handlers !!!
        return success

    async def _watchdog_join_re_worker(self, timeout_join=0.5):
        """
        Request Watchdog to join RE Worker process. The sequence of orderly closing of the process
        needs to be initiated before attempting to join the process.
        """
        try:
            # Communication timeout must be a little bit larger than join timeout.
            response = await self._comm_to_watchdog.send_msg("join_re_worker",
                                                             {"timeout": timeout_join},
                                                             timeout=timeout_join + 0.1)
            success = response["success"]
        except CommTimeoutError:
            success = False
        return success

    async def _watchdog_kill_re_worker(self):
        """
        Request Watchdog to kill RE Worker process (justified only if the process is not responsive).
        """
        try:
            response = await self._comm_to_watchdog.send_msg("kill_re_worker")
            success = response["success"]
        except CommTimeoutError:
            success = False
        return success

    async def _watchdog_is_worker_alive(self):
        """
        Check if RE Worker process is alive.
        """
        try:
            response = await self._comm_to_watchdog.send_msg("is_worker_alive")
            worker_alive = response["worker_alive"]
        except asyncio.TimeoutError:
            worker_alive = False
        return worker_alive

    async def _watchdog_manager_stopping(self):
        """
        Inform Watchdog process that the manager is intentionally being stopped and
        it should not be restarted.
        """
        await self._comm_to_watchdog.send_msg("manager_stopping", notification=True)

    async def _watchdog_send_heartbeat(self):
        """
        Send (periodic) heartbeat signal to Watchdog.
        """
        await self._comm_to_watchdog.send_msg("heartbeat",
                                              {"value": "alive"},
                                              notification=True)

    # =========================================================================
    #                        ZMQ message handlers

    async def _ping_handler(self, request):
        """
        May be called to get response from the Manager. Returns the number of plans in the queue.
        """
        logger.info("Processing 'Hello' request.")
        n_pending_plans = await self._r_pool.llen('plan_queue')
        msg = {"msg": "RE Manager",
               "n_plans": n_pending_plans,
               "is_plan_running": bool(await self._get_running_plan_info())}
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
        logger.info("Adding new plan to the queue: %s", pprint.pformat(request))
        if "plan" in request:
            plan = request["plan"]
            # Create Plan UID (used internally by QServer, user is not expected to see it)
            # Note, Plan UID is not related to Scan UID generated by Run Engine
            plan["plan_uid"] = str(uuid.uuid4())
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

    async def _clear_queue_handler(self, request):
        """
        Remove all entries from the plan queue (does not affect currently executed run)
        """
        logger.info("Clearing the queue")
        while True:
            plan = await self._r_pool.rpop('plan_queue')
            if plan is None:
                break
        return {"success": True, "msg": "Plan queue is now empty."}

    async def _create_environment_handler(self, request):
        """
        Creates RE environment: creates RE Worker process, starts and configures Run Engine.
        """
        logger.info("Creating the new RE environment.")
        success = await self._start_re_worker()
        msg = "Environment already exists." if not success else ""
        return {"success": success, "msg": msg}

    async def _close_environment_handler(self, request):
        """
        Deletes RE environment. In the current 'demo' prototype the environment will be deleted
        only after RE completes the current scan.
        """
        logger.info("Closing current RE environment.")
        success = await self._stop_re_worker()
        msg = "" if success else "Environment does not exist."
        return {"success": success, "msg": msg}

    async def _process_queue_handler(self, request):
        """
        Start execution of the loaded queue. Additional runs can be added to the queue while
        it is executed. If the queue is empty, then nothing will happen.
        """
        logger.info("Starting queue processing.")
        success, msg = await self._start_plan()
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
                success, msg = await self._pause_run_engine(option)
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
                success, msg = await self._continue_run_engine(option)
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

    async def _stop_manager_handler(self, request):
        # This is expected to block the event loop forever
        self._manager_stopping = True
        return {"success": True, "msg": "Initiated sequence of stopping RE Manager."}

    async def _kill_manager_handler(self, request):
        # This is expected to block the event loop forever
        while True:
            ttime.sleep(10)

    async def _zmq_execute(self, msg):
        command = msg["command"]
        value = msg["value"]
        handler_dict = {
            "": "_ping_handler",
            "queue_view": "_queue_view_handler",
            "add_to_queue": "_add_to_queue_handler",
            "pop_from_queue": "_pop_from_queue_handler",
            "clear_queue": "_clear_queue_handler",
            "create_environment": "_create_environment_handler",
            "close_environment": "_close_environment_handler",
            "process_queue": "_process_queue_handler",
            "re_pause": "_re_pause_handler",
            "re_continue": "_re_continue_handler",
            "print_db_uids": "_print_db_uids_handler",
            "stop_manager": "_stop_manager_handler",
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

    # ======================================================================
    #          Functions that support communication via 0MQ

    async def _zmq_receive(self):
        msg_in = await self._zmq_socket.recv_json()
        return msg_in

    async def _zmq_send(self, msg):
        await self._zmq_socket.send_json(msg)

    async def zmq_server_comm(self):
        """
        This function is supposed to be executed by asyncio.run() to start the manager.
        """
        self._ctx = zmq.asyncio.Context()

        self._loop = asyncio.get_running_loop()

        self._comm_to_watchdog = PipeJsonRpcSendAsync(conn=self._watchdog_conn,
                                                      name="RE Manager-Watchdog Comm")
        self._comm_to_worker = PipeJsonRpcSendAsync(conn=self._worker_conn,
                                                    name="RE Manager-Worker Comm")
        self._comm_to_watchdog.start()
        self._comm_to_worker.start()

        # Start heartbeat generator
        self._heartbeat_generator_task = asyncio.ensure_future(self._heartbeat_generator(),
                                                               loop=self._loop)
        self._worker_status_task = asyncio.ensure_future(self._periodic_worker_state_request(),
                                                         loop=self._loop)

        self._r_pool = await aioredis.create_redis_pool(
            'redis://localhost', encoding='utf8')

        # It may be useful to have an API that would delete all used entries in Redis pool
        #   The following code may go into this new API.
        # await self._r_pool.delete("running_plan")
        # await self._r_pool.delete("plan_queue")

        # Create entry 'running_plan' in the pool if it does not exist yet
        await self._init_running_plan_info()

        # Set the environment state based on whether the worker process is alive (request Watchdog)
        self._environment_exists = await self._is_worker_alive()

        # Now check if the plan is still being executed (if it was executed)
        if self._environment_exists:
            self._worker_state, err_msg = await self._worker_request_state()
            if self._worker_state:
                plan_uid_running = self._worker_state["running_plan_uid"]
                if plan_uid_running:
                    # Plan is running. Check if it is the same plan as in redis.
                    plan_stored = await self._get_running_plan_info()
                    if "plan_uid" in plan_stored:
                        plan_uid_stored = plan_stored["plan_uid"]
                        self._manager_state.set_executing_queue()  # Wait for plan completion
                        if plan_uid_stored != plan_uid_running:
                            # Guess is that the environment may still work, so restart is
                            #   only recommended if it is convenient.
                            logger.warning(
                                "Inconsistency of internal QServer data was detected: \n"
                                "UID of currently running plan is '%s', "
                                "instead of '%s'.\n"
                                "RE execution environment may need to be closed and created again \n"
                                "to restore data integrity.", plan_uid_running, plan_uid_stored)
            else:
                logger.error("Error while reading RE Worker status: %s", err_msg)
        if not self._manager_state.is_executing_queue:
            await self._clear_running_plan_info()

        logger.info("Starting ZeroMQ server")
        self._zmq_socket = self._ctx.socket(zmq.REP)
        self._zmq_socket.bind(self._ip_zmq_server)
        logger.info("ZeroMQ server is waiting on %s", str(self._ip_zmq_server))

        while True:
            #  Wait for next request from client
            msg_in = await self._zmq_receive()
            logger.info("ZeroMQ server received request: %s", pprint.pformat(msg_in))

            msg_out = await self._zmq_execute(msg_in)

            #  Send reply back to client
            logger.info("ZeroMQ server sending response: %s", pprint.pformat(msg_out))
            await self._zmq_send(msg_out)

            if self._manager_stopping:
                await self._stop_re_worker_task()  # Quitting RE Manager
                await self._watchdog_manager_stopping()
                self._comm_to_watchdog.stop()
                self._comm_to_worker.stop()
                self._zmq_socket.close()
                logger.info("RE Manager was stopped by ZMQ command.")
                break

    # ======================================================================

    def run(self):
        """
        Overrides the `run()` function of the `multiprocessing.Process` class. Called
        by the `start` method.
        """
        logger.info("Starting RE Manager process")
        try:
            asyncio.run(self.zmq_server_comm())
        except Exception as ex:
            logger.exception("Exiting RE Manager with exception %s", str(ex))
        except KeyboardInterrupt:
            # TODO: RE Manager must be orderly closed before Watchdog module is stopped.
            #   Right now it is just killed by SIGINT.
            logger.info("RE Manager Process was stopped by SIGINT. Handling of Ctrl-C has to be revised!!!")
