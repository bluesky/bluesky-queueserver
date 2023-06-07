import argparse
import importlib
import logging
import multiprocessing
import os
import re
import threading
import time as ttime
import traceback

import bluesky_queueserver

from .profile_ops import (
    existing_plans_and_devices_from_nspace,
    load_worker_startup_code,
    save_existing_plans_and_devices,
)
from .utils import to_boolean

logger = logging.getLogger(__name__)
qserver_version = bluesky_queueserver.__version__


class GenLists(multiprocessing.Process):
    def __init__(
        self,
        *,
        startup_profile,
        startup_dir,
        startup_module_name,
        startup_script_path,
        ipython_dir,
        file_dir,
        file_name,
        overwrite,
        ignore_invalid_plans,
        device_max_depth,
        use_ipython_kernel,
    ):
        super().__init__()
        self._pconn, self._cconn = multiprocessing.Pipe()
        self._exception = None

        self._ip_kernel_monitor_stop = False
        self._tracebacks = []

        self._startup_profile = startup_profile
        self._startup_dir = startup_dir
        self._startup_module_name = startup_module_name
        self._startup_script_path = startup_script_path
        self._ipython_dir = ipython_dir
        self._file_dir = file_dir
        self._file_name = file_name
        self._overwrite = overwrite
        self._ignore_invalid_plans = ignore_invalid_plans
        self._device_max_depth = device_max_depth
        self._use_ipython_kernel = use_ipython_kernel

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception

    def _ip_kernel_iopub_monitor_thread(self):
        import queue

        while True:
            if self._ip_kernel_monitor_stop:
                break

            try:
                msg = self._ip_kernel_client.get_iopub_msg(timeout=0.5)

                try:
                    if msg["header"]["msg_type"] == "error":
                        tb = msg["content"]["traceback"]
                        # Remove escape characters from traceback
                        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
                        tb = [ansi_escape.sub("", _) for _ in tb]
                        tb = "\n".join(tb)
                        self._tracebacks.append(tb)
                        # print(f"Traceback: {tb}")
                except KeyError:
                    pass
            except queue.Empty:
                pass
            except Exception as ex:
                logger.exception(ex)

    def run(self):
        try:
            from .config import get_profile_name_from_path, profile_name_to_startup_dir
            from .profile_tools import (
                clear_ipython_mode,
                clear_re_worker_active,
                set_ipython_mode,
                set_re_worker_active,
            )

            startup_profile = self._startup_profile
            startup_dir = self._startup_dir
            startup_module_name = self._startup_module_name
            startup_script_path = self._startup_script_path
            ipython_dir = self._ipython_dir
            file_dir = self._file_dir
            file_name = self._file_name
            overwrite = self._overwrite
            ignore_invalid_plans = self._ignore_invalid_plans
            device_max_depth = self._device_max_depth
            use_ipython_kernel = self._use_ipython_kernel

            errmsg = "Source of the startup code was not specified or multiple sources were specified."

            file_name = file_name or "existing_plans_and_devices.yaml"
            file_dir = file_dir or os.getcwd()

            set_re_worker_active()
            set_ipython_mode(use_ipython_kernel)

            if use_ipython_kernel:
                # Make sure that there is an even loop in the main thread
                # (otherwise IP Kernel may not initialize)
                import asyncio

                from bluesky.run_engine import get_bluesky_event_loop
                from ipykernel.kernelapp import IPKernelApp

                from .utils import generate_random_port

                loop = get_bluesky_event_loop() or asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                if startup_dir:
                    if startup_profile or ipython_dir:
                        raise ValueError(errmsg)
                    else:
                        startup_profile, ipython_dir = get_profile_name_from_path(startup_dir)

                nspace = {}
                ip_kernel_app = IPKernelApp(user_ns=nspace)

                if startup_profile:
                    logger.info("IP Kernel: startup profile: '%s'", startup_profile)
                    ip_kernel_app.profile = startup_profile
                if startup_module_name:
                    # NOTE: Startup files are still loaded.
                    logger.info("IP Kernel: startup module: '%s'", startup_module_name)
                    ip_kernel_app.module_to_run = startup_module_name
                if startup_script_path:
                    # NOTE: Startup files are still loaded.
                    logger.info("IP Kernel: startup script: '%s'", startup_script_path)
                    ip_kernel_app.file_to_run = startup_script_path
                if ipython_dir:
                    logger.info("IP Kernel: IPython directory: '%s'", ipython_dir)
                    ip_kernel_app.ipython_dir = ipython_dir

                ip_kernel_app.capture_fd_output = False
                ip_kernel_app.quiet = False
                ip_kernel_app.matplotlib = "agg"

                ip_kernel_app.shell_port = generate_random_port()
                ip_kernel_app.iopub_port = generate_random_port()
                ip_kernel_app.stdin_port = generate_random_port()
                ip_kernel_app.hb_port = generate_random_port()
                ip_kernel_app.control_port = generate_random_port()
                self._ip_connect_info = ip_kernel_app.get_connection_info()

                def start_jupyter_client():
                    from jupyter_client import BlockingKernelClient

                    self._ip_kernel_client = BlockingKernelClient()
                    self._ip_kernel_client.load_connection_info(self._ip_connect_info)
                    # logger.info(
                    #     "Session ID for communication with IP kernel: %s", self._ip_kernel_client.session.session
                    # )
                    self._ip_kernel_client.start_channels()

                    ip_kernel_iopub_monitor_thread = threading.Thread(
                        target=self._ip_kernel_iopub_monitor_thread,
                        # kwargs=dict(output_stream=out_stream, error_stream=err_stream),
                        daemon=True,
                    )
                    ip_kernel_iopub_monitor_thread.start()

                start_jupyter_client()

                ttime.sleep(0.5)  # Wait unitl 0MQ monitor is connected to the kernel ports

                logger.info("Initializing IPython kernel ...")
                ip_kernel_app.initialize([])
                logger.info("IPython kernel initialization is complete.")

                ttime.sleep(0.2)  # Wait until the error message are delivered (if startup fails)

                self._ip_kernel_monitor_stop = True

                if self._tracebacks:
                    tb_str = "\n".join(self._tracebacks)
                    raise Exception(tb_str)

            else:
                if not startup_dir and (startup_profile or ipython_dir):
                    if not (startup_module_name or startup_script_path):
                        startup_dir = profile_name_to_startup_dir(
                            startup_profile or "default", ipython_dir=ipython_dir
                        )

                if sum([_ is None for _ in [startup_dir, startup_module_name, startup_script_path]]) != 2:
                    raise ValueError(errmsg)

                nspace = load_worker_startup_code(
                    startup_dir=startup_dir,
                    startup_module_name=startup_module_name,
                    startup_script_path=startup_script_path,
                )

            existing_plans, existing_devices, _, _ = existing_plans_and_devices_from_nspace(
                nspace=nspace, ignore_invalid_plans=ignore_invalid_plans, max_depth=device_max_depth
            )

            save_existing_plans_and_devices(
                existing_plans=existing_plans,
                existing_devices=existing_devices,
                file_dir=file_dir,
                file_name=file_name,
                overwrite=overwrite,
            )

        except Exception:
            error = traceback.format_exc()
            self._cconn.send((error,))

        finally:
            clear_re_worker_active()
            clear_ipython_mode()


def gen_list_of_plans_and_devices(
    *,
    startup_profile=None,
    startup_dir=None,
    startup_module_name=None,
    startup_script_path=None,
    file_dir=None,
    file_name=None,
    ipython_dir=None,
    overwrite=False,
    ignore_invalid_plans=False,
    device_max_depth=0,
    use_ipython_kernel=False,
):
    """
    Generate the list of plans and devices from a collection of startup files, python module or
    a script. Only one source of startup code should be specified, otherwise an exception will be
    raised.

    If ``file_name`` is specified, it is used as a name for the output file, otherwise
    the default file name ``existing_plans_and_devices.yaml`` is used. The file will be saved
    to ``file_dir`` or current directory if ``file_dir`` is not specified or ``None``.

    Parameters
    ----------
    startup_profile: str or None
        name of IPython profile to load. The startup code is expected to be located in
        ``<ipython_dir>/<startup_profile>/startup`` directory. If ``use_ipython_kernel=False``
        and one of the parameters ``startup_dir``, ``startup_module_name`` or
        ``startup_script_path`` are specified, then the parameter is ignored.
        If ``use_ipython_kernel=True``, then the profile is always loaded.
        The function fails if both ``startup_dir`` and ``startup_profile`` are specified
        (ambiguous location of the startup code).
    startup_dir: str or None
        path to the directory that contains a collection of startup files (IPython-style)
    startup_module_name: str or None
        name of the startup module to load
    startup_script_path: str or None
        name of the startup script
    ipython_dir: str or None
        The path to IPython root directory, which contains profiles. Overrides IPYTHONDIR environment
        variable. The parameter is used to compute the location of startup code based on
        ``startup_profile``. The parameters ``ipython_dir`` and ``startup_dir`` are mutually exclusive.
    file_dir: str or None
        path to the directory where the file is to be created. None - create file in current directory.
    file_name: str
        name of the output YAML file, None - default file name is used
    overwrite: boolean
        overwrite the file if it already exists
    ignore_invalid_plans: bool
        Ignore plans with unsupported signatures. If the argument is ``False`` (default), then
        an exception is raised otherwise a message is printed and the plan is not included in the list.
    device_max_depth: int
        Default maximum depth for devices included in the list of existing devices:
        0 - unlimited depth (full tree of subdevices is included for all devices except areadetectors),
        1 - only top level devices are included, 2 - top level devices and subdevices are included, etc.
    use_ipython_kernel: boolean
        Select between loading startup code using pure Python (``False``) or use IPython (``True``).
        IPython mode allows to load the code that contains IPython features.

    Returns
    -------
    None

    Raises
    ------
    RuntimeError
        Error occurred while creating or saving the lists.
    """
    startup_profile = startup_profile or None
    startup_dir = startup_dir or None
    startup_module_name = startup_module_name or None
    startup_script_path = startup_script_path or None
    ipython_dir = ipython_dir or None

    if startup_dir is not None:
        startup_dir = os.path.abspath(os.path.expanduser(startup_dir))
    if startup_script_path is not None:
        startup_script_path = os.path.abspath(os.path.expanduser(startup_script_path))
    if ipython_dir is not None:
        ipython_dir = os.path.abspath(os.path.expanduser(ipython_dir))

    try:
        if startup_script_path and not os.path.isfile(startup_script_path):
            raise IOError(f"Startup script {startup_script_path!r} is not found")

        if startup_module_name and importlib.util.find_spec(startup_module_name) is None:
            raise ImportError(f"Startup module {startup_module_name!r} is not found")

        gen_lists_kwargs = dict(
            startup_profile=startup_profile,
            startup_dir=startup_dir,
            startup_module_name=startup_module_name,
            startup_script_path=startup_script_path,
            ipython_dir=ipython_dir,
            file_dir=file_dir,
            file_name=file_name,
            overwrite=overwrite,
            ignore_invalid_plans=ignore_invalid_plans,
            device_max_depth=device_max_depth,
            use_ipython_kernel=use_ipython_kernel,
        )

        p = GenLists(**gen_lists_kwargs)
        p.start()
        p.join()
        if p.exception:
            (error,) = p.exception
            raise Exception(error)

    except Exception as ex:
        raise RuntimeError(f"Failed to create the list of plans and devices:\n\n{str(ex)}")


def gen_list_of_plans_and_devices_cli():
    """
    'qserver-list-plans-devices'
    CLI tool for generating the list of existing plans and devices based on profile collection.
    The tool is supposed to be called as 'qserver-list-plans-devices' from command line.
    The function will ALWAYS overwrite the existing list of plans and devices (the list
    is automatically generated, so overwriting (updating) should be a safe operation that doesn't
    lead to loss configuration data.
    """
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("bluesky_queueserver").setLevel("INFO")

    def formatter(prog):
        # Set maximum width such that printed help mostly fits in the RTD theme code block (documentation).
        return argparse.RawDescriptionHelpFormatter(prog, max_help_position=20, width=90)

    parser = argparse.ArgumentParser(
        description="Bluesky-QServer:\nCLI tool for generating the list of plans and devices "
        f"from beamline startup scripts.\nbluesky-queueserver version {qserver_version}\n",
        formatter_class=formatter,
    )
    parser.add_argument(
        "--file-dir",
        dest="file_dir",
        action="store",
        required=False,
        default=None,
        help="Directory name where the list of plans and devices is saved. By default, the list is saved "
        "to the file 'existing_plans_and_devices.yaml' in the current directory.",
    )
    parser.add_argument(
        "--file-name",
        dest="file_name",
        action="store",
        required=False,
        default=None,
        help="Name of the file where the list of plans and devices is saved. Default file name: "
        "'existing_plans_and_devices.yaml'.",
    )

    parser.add_argument(
        "--startup-profile",
        dest="startup_profile",
        type=str,
        help="The name of IPython profile used to find the location of startup files. Example: if IPython is "
        "configured to look for profiles in '~/.ipython' directory (default behavior) and the profile "
        "name is 'testing', then RE Manager will look for startup files in "
        "'~/.ipython/profile_testing/startup' directory. If IPython-based worker is used, the code in "
        "the startup profile or the default profile is always executed before running "
        "a startup module or a script",
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--startup-dir",
        dest="startup_dir",
        type=str,
        default=None,
        help="Path to directory that contains a set of startup files (*.py and *.ipy). All the scripts "
        "in the directory will be sorted in alphabetical order of their names and loaded in "
        "the Run Engine Worker environment. The set of startup files may be located in any accessible "
        "directory. For example, 'qserver-list-plans-devices --startup-dir .' loads startup "
        "files from the current directory and saves the lists to the file in current directory.",
    )
    group.add_argument(
        "--startup-module",
        dest="startup_module_name",
        type=str,
        default=None,
        help="The name of the module that contains the startup code. The module must be installed "
        " in the current environment For example, 'qserver-list-plans-devices "
        "--startup-module some.startup.module' loads startup code from the module 'some.startup.module' "
        "and saves results to the file in the current directory.",
    )
    group.add_argument(
        "--startup-script",
        dest="startup_script_path",
        type=str,
        default=None,
        help="The path to the script with startup code. For example, "
        "'qserver-list-plans-devices --startup-script ~/startup/scripts/script.py' loads "
        "startup code from the script and saves the results to the file in the current directory.",
    )

    parser.add_argument(
        "--ipython-dir",
        dest="ipython_dir",
        type=str,
        help="The path to IPython root directory, which contains profiles. Overrides IPYTHONDIR environment "
        "variable.",
    )

    parser.add_argument(
        "--use-ipython-kernel",
        dest="use_ipython_kernel",
        type=str,
        choices=["ON", "OFF"],
        default="OFF",
        help="Run the Run Engine worker in IPython kernel (default: %(default)s).",
    )

    parser.add_argument(
        "--ignore-invalid-plans",
        dest="ignore_invalid_plans",
        type=str,
        choices=["ON", "OFF"],
        default="OFF",
        help="Ignore plans with unsupported signatures When loading startup code or executing scripts. "
        "The default behavior is to raise an exception. If the parameter is set, the message is printed for each "
        "invalid plan and only plans that were processed correctly are included in the list of existing plans "
        "(default: %(default)s).",
    )
    parser.add_argument(
        "--device-max-depth",
        dest="device_max_depth",
        type=int,
        default=0,
        help="Default maximum depth for devices included in the list of existing devices: "
        "0 - unlimited depth (full tree of subdevices is included for all devices except areadetectors), "
        "1 - only top level devices are included, 2 - top level devices and subdevices are included, etc. "
        "(default: %(default)s).",
    )

    args = parser.parse_args()
    file_dir = args.file_dir
    file_name = args.file_name
    startup_profile = args.startup_profile
    startup_dir = args.startup_dir
    startup_module_name = args.startup_module_name
    startup_script_path = args.startup_script_path
    ipython_dir = args.ipython_dir
    use_ipython_kernel = args.use_ipython_kernel
    ignore_invalid_plans = to_boolean(args.ignore_invalid_plans)
    device_max_depth = int(args.device_max_depth)

    use_ipython_kernel = use_ipython_kernel == "ON"

    if file_dir is not None:
        file_dir = os.path.abspath(os.path.expanduser(file_dir))

    try:
        gen_list_of_plans_and_devices(
            startup_profile=startup_profile,
            startup_dir=startup_dir,
            startup_module_name=startup_module_name,
            startup_script_path=startup_script_path,
            file_dir=file_dir,
            file_name=file_name,
            ipython_dir=ipython_dir,
            overwrite=True,
            ignore_invalid_plans=ignore_invalid_plans,
            device_max_depth=device_max_depth,
            use_ipython_kernel=use_ipython_kernel,
        )
        print("The list of existing plans and devices was created successfully.")
        exit_code = 0
    except BaseException as ex:
        # Error message contains full traceback!!!
        logger.error("Operation failed ...\n%s", str(ex))
        exit_code = 1
    return exit_code
