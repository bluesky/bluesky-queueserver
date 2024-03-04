import copy
import getpass
import os
import pprint
import re
import subprocess

import pytest
import yaml

from bluesky_queueserver import gen_list_of_plans_and_devices
from bluesky_queueserver.manager.config import default_existing_pd_fln, default_user_group_pd_fln

from ..comms import zmq_single_request
from .common import re_manager_cmd  # noqa: F401
from .common import (
    _test_redis_name_prefix,
    _user,
    _user_group,
    append_code_to_last_startup_file,
    clear_redis_pool,
    condition_environment_closed,
    condition_environment_created,
    condition_manager_idle,
    condition_queue_processing_finished,
    copy_default_profile_collection,
    get_manager_status,
    set_qserver_zmq_address,
    set_qserver_zmq_public_key,
    use_ipykernel_for_tests,
    wait_for_condition,
    wait_for_task_result,
)

# Plans used in most of the tests: '_plan1' and '_plan2' are quickly executed '_plan3' runs for 5 seconds.
_plan1 = {"name": "count", "args": [["det1", "det2"]], "item_type": "plan"}
_plan2 = {"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10], "item_type": "plan"}
_plan3 = {"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 5, "delay": 1}, "item_type": "plan"}
_instruction_stop = {"name": "queue_stop", "item_type": "instruction"}


timeout_env_open = 10


# fmt: off
@pytest.mark.parametrize("option", ["--verbose", "--quiet", "--silent"])
# fmt: on
def test_start_re_manager_logging_1(re_manager_cmd, option):  # noqa: F811
    """
    Test if RE Manager is correctly started with parameters that define logging verbosity.
    The test also creates the worker environment to make sure that the program does not crash
    when worker process is created.

    This is a smoke test: it does not verify that logging works.
    """
    re_manager_cmd([option])

    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert resp1["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    # Attemtp to communicate with RE Manager
    resp2, _ = zmq_single_request("status")
    assert resp2["items_in_queue"] == 0
    assert resp2["items_in_history"] == 0

    resp3, _ = zmq_single_request("environment_close")
    assert resp3["success"] is True
    assert resp3["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("console_print, console_zmq", [
    (None, None),
    (True, True),
    (True, False),
    (False, True),
    (False, False),
])
# fmt: on
def test_start_re_manager_console_output_1(re_manager_cmd, console_print, console_zmq):  # noqa: F811
    """
    Test for printing and publishing the console output (--console-output and --zmq-publish).
    """
    params = []
    if console_print is True:
        params.extend(["--console-output", "ON"])
    elif console_print is False:
        params.extend(["--console-output", "OFF"])
    if console_zmq is True:
        params.extend(["--zmq-publish-console", "ON"])
    elif console_zmq is False:
        params.extend(["--zmq-publish-console", "OFF"])

    # Default values (if parameters are not specified)
    if console_print is None:
        console_print = True
    if console_zmq is None:
        console_zmq = False

    # Start monitor (captures messages published to 0MQ)
    p_monitor = subprocess.Popen(
        ["qserver-console-monitor"], universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    re_manager = re_manager_cmd(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    resp1, _ = zmq_single_request("environment_open")
    assert resp1["success"] is True
    assert resp1["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp2, _ = zmq_single_request("queue_item_add", {"item": _plan1, "user": _user, "user_group": _user_group})
    assert resp2["success"] is True

    resp3, _ = zmq_single_request("queue_start")
    assert resp3["success"] is True

    assert wait_for_condition(time=10, condition=condition_queue_processing_finished)

    resp3, _ = zmq_single_request("environment_close")
    assert resp3["success"] is True
    assert resp3["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    p = re_manager._p
    re_manager.stop_manager()
    re_manager_stdout, re_manager_stderr = p.communicate()

    p_monitor.terminate()
    streamed_stdout, streamed_stderr = p_monitor.communicate()

    def check_output_contents(collected_stdout):
        assert collected_stdout != ""
        # Verify that output from all sources is present in the output
        # Logging from manager
        assert "bluesky_queueserver.manager.manager manager:" in collected_stdout
        assert "bluesky_queueserver.manager.profile_ops profile_ops:" in collected_stdout
        # Logging from Worker
        assert "bluesky_queueserver.manager.worker worker:" in collected_stdout
        assert "RE Environment is ready" in collected_stdout
        # Printing from live table
        assert "generator count" in collected_stdout
        assert "Run was closed:" in collected_stdout

    # The following error is added when tests are executed in WSL environment
    wsl_err = "QStandardPaths: wrong permissions on runtime directory"
    assert re_manager_stderr == "" or wsl_err in re_manager_stderr, re_manager_stderr
    assert "bluesky_queueserver.manager.output_streaming" in streamed_stderr

    if console_print:
        check_output_contents(re_manager_stdout)
    else:
        assert re_manager_stdout == ""

    if console_zmq:
        check_output_contents(streamed_stdout)
    else:
        assert streamed_stdout == ""


# fmt: off
@pytest.mark.parametrize("test_mode", ["none", "parameter", "env_var", "both_success", "both_fail"])
# fmt: on
def test_start_re_manager_console_output_2(monkeypatch, re_manager_cmd, test_mode):  # noqa: F811
    """
    Check that the parameter ``--zmq-info-addr`` the and environment variable
    ``QSERVER_ZMQ_INFO_ADDRESS_FOR_SERVER` are properly handled by ``start-re-manager``.
    """
    address_info_server = "tcp://*:60621"
    address_info_server_incorrect = "tcp://*:60622"
    address_info_client = "tcp://localhost:60621"

    params_server = ["--zmq-publish-console=ON"]
    if test_mode == "none":
        # Use default address, communication fails
        success = False
    elif test_mode == "parameter":
        # Pass the address as a parameter
        success = True
        params_server.append(f"--zmq-info-addr={address_info_server}")
    elif test_mode == "env_var":
        # Pass the address as an environment variable
        success = True
        monkeypatch.setenv("QSERVER_ZMQ_INFO_ADDRESS_FOR_SERVER", address_info_server)
    elif test_mode == "both_success":
        # Pass the correct address as a parameter and incorrect as environment variable (ignored)
        success = True
        params_server.append(f"--zmq-info-addr={address_info_server}")
        monkeypatch.setenv("QSERVER_ZMQ_INFO_ADDRESS_FOR_SERVER", address_info_server_incorrect)
    elif test_mode == "both_fail":
        # Pass incorrect address as an environment variable (ignored) and correct address as a parameter
        success = False
        params_server.append(f"--zmq-info-addr={address_info_server_incorrect}")
        monkeypatch.setenv("QSERVER_ZMQ_INFO_ADDRESS_FOR_SERVER", address_info_server)
    else:
        raise RuntimeError(f"Unrecognized test mode '{test_mode}'")

    re_manager_cmd(params_server)

    # Start monitor (captures messages published to 0MQ)
    p_monitor = subprocess.Popen(
        ["qserver-console-monitor", f"--zmq-info-addr={address_info_client}"],
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    zmq_single_request("environment_open")
    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)
    zmq_single_request("environment_close")
    assert wait_for_condition(time=3, condition=condition_environment_closed)

    p_monitor.terminate()
    streamed_stdout, streamed_stderr = p_monitor.communicate()

    if success:
        assert streamed_stdout != ""
        assert "RE Environment is ready" in streamed_stdout
    else:
        assert streamed_stdout == ""
        assert "RE Environment is ready" not in streamed_stdout


# fmt: off
@pytest.mark.parametrize("option", ["unchanged", "add_plan", "add_device", "add_plan_device"])
@pytest.mark.parametrize("update_existing_plans_devices", ["NEVER", "ENVIRONMENT_OPEN", "ALWAYS"])
# fmt: on
def test_cli_update_existing_plans_devices_01(
    re_manager_cmd, tmp_path, update_existing_plans_devices, option  # noqa: F811
):
    """
    Testing the modes defined by ``--update-existing-plans-devices`` parameter: create a copy of
    profile collection, generate the list of existing plans and devices from startup files, add
    a plan and/or a device to startup files, start RE Manager, open and close the environment
    (it is expected to generate the updated lists of existing plans and devices and possibly
    save them to disk), verify that the new plan and/or device is in the lists of allowed plans
    and devices, reload permissions and existing plans and devices from disk and verify that
    the new plan and/or devices is in the list of allowed plans (in case the new list of existing
    devices is saved to file).
    """
    # Copy the default profile collection and generate the list of existing devices
    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=True)
    gen_list_of_plans_and_devices(
        startup_dir=pc_path, file_dir=pc_path, file_name="existing_plans_and_devices.yaml", overwrite=True
    )

    # Start the manager
    params = ["--startup-dir", pc_path, "--update-existing-plans-devices", update_existing_plans_devices]
    re_manager_cmd(params)

    resp1, _ = zmq_single_request("status")
    devices_allowed_uid1 = resp1["devices_allowed_uid"]
    plans_allowed_uid1 = resp1["plans_allowed_uid"]

    # Add a plan ('count50') and a device ('det50') if needed
    with open(os.path.join(pc_path, "zz.py"), "w") as f:
        f.writelines("print(f'Loading file {__file__!r}')\n")
        if option in ("add_device", "add_plan_device"):
            f.writelines("det50 = det\n")
        if option in ("add_plan", "add_plan_device"):
            f.writelines("count50 = count\n")

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp3, _ = zmq_single_request("environment_close")
    assert resp3["success"] is True
    assert resp3["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    resp4, _ = zmq_single_request("status")
    devices_allowed_uid2 = resp4["devices_allowed_uid"]
    plans_allowed_uid2 = resp4["plans_allowed_uid"]

    def verify_allowed_lists(*, new_plan_added, new_device_added):
        resp5a, _ = zmq_single_request("plans_allowed", params={"user_group": _user_group})
        assert resp5a["success"] is True, f"resp={resp5a}"
        plans_allowed = resp5a["plans_allowed"]
        resp5b, _ = zmq_single_request("devices_allowed", params={"user_group": _user_group})
        assert resp5b["success"] is True, f"resp={resp5b}"
        devices_allowed = resp5b["devices_allowed"]

        if new_device_added:
            assert "det50" in devices_allowed
        else:
            assert "det50" not in devices_allowed
        if new_plan_added:
            assert "count50" in plans_allowed
        else:
            assert "count50" not in plans_allowed

    new_plan_added = option in ("add_plan", "add_plan_device")
    new_device_added = option in ("add_device", "add_plan_device")

    verify_allowed_lists(new_plan_added=new_plan_added, new_device_added=new_device_added)

    if new_device_added:
        assert devices_allowed_uid2 != devices_allowed_uid1
    else:
        assert devices_allowed_uid2 == devices_allowed_uid1
    if new_plan_added:
        assert plans_allowed_uid2 != plans_allowed_uid1
    else:
        assert plans_allowed_uid2 == plans_allowed_uid1

    # Reload the list of existing plans and devices from disk and make sure the new device/plan
    #   is loaded/not loaded depending on the update mode.
    resp6, _ = zmq_single_request("permissions_reload", params={"restore_plans_devices": True})
    assert resp6["success"] is True

    new_plan_added = new_plan_added and update_existing_plans_devices != "NEVER"
    new_device_added = new_device_added and update_existing_plans_devices != "NEVER"

    verify_allowed_lists(new_plan_added=new_plan_added, new_device_added=new_device_added)


_permissions_dict_not_allow_count = {
    "user_groups": {
        "root": {"allowed_plans": [None], "allowed_devices": [None]},
        "primary": {"allowed_plans": [None], "forbidden_plans": [":^count$"], "allowed_devices": [None]},
    }
}


# fmt: off
@pytest.mark.parametrize("sim_corrupt_redis_key", [False, True])
@pytest.mark.parametrize("user_group_permissions_reload", ["NEVER", "ON_REQUEST", "ON_STARTUP"])
# fmt: on
def test_cli_user_group_permissions_reload_01(
    re_manager_cmd, user_group_permissions_reload, sim_corrupt_redis_key  # noqa: F811
):
    """
    Tests for parameter ``--user-group-permissions-reload``: start manager, set permissions that
    are different from default (on disk), stop the manager and start it again (without removing
    Redis keys), check if correct permissions are loaded at startup, try to reload permissions
    using ``permissions_reload`` API and check if correct permissions are loaded.

    In addition, the case when Redis does not contain valid permissions is simulated.
    Permissions should be loaded from disk with any value of the parameter.
    """
    re = re_manager_cmd()

    resp1, _ = zmq_single_request(
        "permissions_set", params={"user_group_permissions": _permissions_dict_not_allow_count}
    )
    assert resp1["success"] is True, pprint.pformat(resp1)
    assert resp1["msg"] == ""

    # Stop the manager
    re.stop_manager(cleanup=False)

    # Simulate corrupt or missing redis key (should be treated the same).
    if sim_corrupt_redis_key:
        clear_redis_pool()

    # Start the manager again
    params = ["--user-group-permissions-reload", user_group_permissions_reload]
    re.start_manager(params=params, cleanup=False)
    wait_for_condition(time=10, condition=condition_manager_idle)

    resp2, _ = zmq_single_request("permissions_get")
    assert resp2["success"] is True
    user_group_permissions = resp2["user_group_permissions"]

    if (user_group_permissions_reload == "ON_STARTUP") or sim_corrupt_redis_key:
        assert user_group_permissions != _permissions_dict_not_allow_count
    else:
        assert user_group_permissions == _permissions_dict_not_allow_count

    # This should have no effect on the results of the test, but let's still check if it works.
    if sim_corrupt_redis_key:
        clear_redis_pool()

    # Attempt to reload permissions (from disk)
    resp3, _ = zmq_single_request("permissions_reload")

    if user_group_permissions_reload == "NEVER":
        assert resp3["success"] is False
        assert "RE Manager was started with option user_group_permissions_reload='NEVER'" in resp3["msg"]
    else:
        assert resp3["success"] is True

    resp4, _ = zmq_single_request("permissions_get")
    assert resp4["success"] is True
    user_group_permissions = resp4["user_group_permissions"]

    if (user_group_permissions_reload != "NEVER") or sim_corrupt_redis_key:
        assert user_group_permissions != _permissions_dict_not_allow_count
    else:
        assert user_group_permissions == _permissions_dict_not_allow_count


# fmt: off
@pytest.mark.parametrize("test_mode", ["none", "parameter", "env_var", "both_success", "both_fail"])
# fmt: on
def test_cli_parameters_zmq_server_address_1(monkeypatch, re_manager_cmd, test_mode):  # noqa: F811
    """
    Check that passing server address as a parameter and environment variable works as
    expected.
    """
    address_server = "tcp://*:60621"
    address_server_incorrect = "tcp://*:60620"
    address_client = "tcp://localhost:60621"

    params_server = []
    if test_mode == "none":
        # Use default address, communication fails
        success = False
    elif test_mode == "parameter":
        # Pass the address as a parameter
        success = True
        params_server.append(f"--zmq-control-addr={address_server}")
        set_qserver_zmq_address(monkeypatch, zmq_server_address=address_server.replace("*", "localhost"))
    elif test_mode == "env_var":
        # Pass the address as an environment variable
        success = True
        monkeypatch.setenv("QSERVER_ZMQ_CONTROL_ADDRESS_FOR_SERVER", address_server)
        set_qserver_zmq_address(monkeypatch, zmq_server_address=address_server.replace("*", "localhost"))
    elif test_mode == "both_success":
        # Pass the correct address as a parameter and incorrect as environment variable (ignored)
        success = True
        params_server.append(f"--zmq-control-addr={address_server}")
        monkeypatch.setenv("QSERVER_ZMQ_CONTROL_ADDRESS_FOR_SERVER", address_server_incorrect)
        set_qserver_zmq_address(monkeypatch, zmq_server_address=address_server.replace("*", "localhost"))
    elif test_mode == "both_fail":
        # Pass incorrect address as an environment variable (ignored) and correct address as a parameter
        success = False
        params_server.append(f"--zmq-control-addr={address_server_incorrect}")
        monkeypatch.setenv("QSERVER_ZMQ_CONTROL_ADDRESS_FOR_SERVER", address_server)
        set_qserver_zmq_address(monkeypatch, zmq_server_address=address_server_incorrect.replace("*", "localhost"))
    else:
        raise RuntimeError(f"Unrecognized test mode '{test_mode}'")

    re_manager_cmd(params_server)

    status, msg = zmq_single_request("status", zmq_server_address=address_client)
    if success:
        assert msg == "", (status, msg)
        assert status["manager_state"] == "idle", (status, msg)
    else:
        assert msg != "", (status, msg)
        assert status is None, (status, msg)


def _get_expected_settings_default_1(tmpdir):
    use_ip_kernel = use_ipykernel_for_tests()
    username = getpass.getuser()
    if use_ip_kernel:
        startup_dir = None
        ipython_dir = f"/tmp/qserver_{username}/ipython"
        startup_profile = "collection_sim"
        user_group_permissions_path = f"/tmp/qserver_{username}/ipython/profile_collection_sim/startup"
        existing_plans_and_devices_path = f"/tmp/qserver_{username}/ipython/profile_collection_sim/startup"
    else:
        startup_dir = "/bluesky_queueserver/profile_collection_sim/"
        ipython_dir = None
        startup_profile = None
        user_group_permissions_path = "/bluesky_queueserver/profile_collection_sim"
        existing_plans_and_devices_path = "/bluesky_queueserver/profile_collection_sim"
    user_group_permissions_path += "/" + default_user_group_pd_fln
    existing_plans_and_devices_path += "/" + default_existing_pd_fln

    return {
        "console_logging_level": 10,
        "databroker_config": None,
        "demo_mode": True,
        "emergency_lock_key": None,
        "existing_plans_and_devices_path": existing_plans_and_devices_path,
        "ignore_invalid_plans": False,
        "ipython_dir": ipython_dir,
        "kafka_server": "127.0.0.1:9092",
        "kafka_topic": None,
        "keep_re": False,
        "device_max_depth": 0,
        "use_ipython_kernel": bool(use_ip_kernel),
        "ipython_kernel_ip": "localhost",
        "ipython_matplotlib": None,
        "print_console_output": True,
        "redis_addr": "localhost",
        "redis_name_prefix": _test_redis_name_prefix,  # We use it for unit tests
        "startup_dir": startup_dir,
        "startup_module": None,
        "startup_profile": startup_profile,
        "startup_script": None,
        "update_existing_plans_devices": "ENVIRONMENT_OPEN",
        "use_persistent_metadata": False,
        "user_group_permissions_path": user_group_permissions_path,
        "user_group_permissions_reload": "ON_STARTUP",
        "zmq_control_addr": "tcp://*:60615",
        "zmq_data_proxy_addr": None,
        "zmq_info_addr": "tcp://*:60625",
        "zmq_private_key": None,
        "zmq_publish_console": False,
    }


_dir_2 = "test2"


# matching public key: =E0[czQkp!!%0TL1LCJ5X[<wjYD[iV+p[yuaI0an
def _get_config_file_2(file_dir):
    s = """
network:
  zmq_control_addr: tcp://*:60617
  zmq_private_key: {0}
  zmq_info_addr: tcp://*:60627
  zmq_publish_console: true
  redis_addr: localhost:6379
  redis_name_prefix: qs_unit_tests2
worker:
  use_ipython_kernel: {4}
  ipython_kernel_ip: auto
  ipython_matplotlib: qt5
startup:
  keep_re: false
  device_max_depth: 2
  ignore_invalid_plans: true
  startup_dir: {1}
  existing_plans_and_devices_path: {2}
  user_group_permissions_path: {3}
operation:
  print_console_output: true
  console_logging_level: VERBOSE
  update_existing_plans_and_devices: ALWAYS
  user_group_permissions_reload: ON_REQUEST
  emergency_lock_key: different_lock_key
run_engine:
  use_persistent_metadata: true
  kafka_server: 127.0.0.1:9095
  kafka_topic: different_topic_name
  zmq_data_proxy_addr: tcp://localhost:5569
  databroker_config: DIF
"""
    use_ip_kernel = "true" if use_ipykernel_for_tests() else "false"
    return s.format("Ue=.po0aQ9.}<Xvrny+f{V04XMc6JZ9ufKf5aeFy", file_dir, file_dir, file_dir, use_ip_kernel)


def _get_expected_settings_config_2(file_dir):
    use_ip_kernel = use_ipykernel_for_tests()
    if use_ip_kernel:
        startup_dir = None
        ipython_dir = os.path.split(os.path.split(file_dir)[0])[0]
        startup_profile = "collection_sim"
        user_group_permissions_path = file_dir
        existing_plans_and_devices_path = file_dir
    else:
        startup_dir = file_dir
        ipython_dir = None
        startup_profile = None
        user_group_permissions_path = file_dir
        existing_plans_and_devices_path = file_dir

    user_group_permissions_path += "/" + default_user_group_pd_fln
    existing_plans_and_devices_path += "/" + default_existing_pd_fln

    return {
        "console_logging_level": 10,
        "databroker_config": "DIF",
        "demo_mode": False,
        "emergency_lock_key": "different_lock_key",
        "existing_plans_and_devices_path": existing_plans_and_devices_path,
        "ipython_dir": ipython_dir,
        "kafka_server": "127.0.0.1:9095",
        "kafka_topic": "different_topic_name",
        "keep_re": False,
        "device_max_depth": 2,
        "ignore_invalid_plans": True,
        "use_ipython_kernel": bool(use_ip_kernel),
        "ipython_kernel_ip": "auto",
        "ipython_matplotlib": "qt5",
        "print_console_output": True,
        "redis_addr": "localhost:6379",
        "redis_name_prefix": "qs_unit_tests2",  # Specific for this test
        "startup_dir": startup_dir,
        "startup_module": None,
        "startup_profile": startup_profile,
        "startup_script": None,
        "update_existing_plans_devices": "ALWAYS",
        "use_persistent_metadata": True,
        "user_group_permissions_path": user_group_permissions_path,
        "user_group_permissions_reload": "ON_REQUEST",
        "zmq_control_addr": "tcp://*:60617",
        "zmq_data_proxy_addr": "tcp://localhost:5569",
        "zmq_info_addr": "tcp://*:60627",
        "zmq_private_key": "Ue=.po0aQ9.}<Xvrny+f{V04XMc6JZ9ufKf5aeFy",
        "zmq_publish_console": True,
    }


_dir_3 = "test3"


def _get_cli_params_3(file_dir):
    use_ip_kernel = "ON" if use_ipykernel_for_tests() else "OFF"
    return [
        "--zmq-control-addr=tcp://*:60619",
        f"--startup-dir={file_dir}",
        f"--existing-plans-devices={file_dir}",
        "--update-existing-plans-devices=NEVER",
        f"--user-group-permissions={file_dir}",
        "--user-group-permissions-reload=NEVER",
        "--redis-addr=localhost:6379",
        "--redis-name-prefix=qs_unit_tests3",
        "--kafka-topic=yet_another_topic",
        "--kafka-server=127.0.0.1:9099",
        "--keep-re",
        "--device-max-depth=5",
        "--ignore-invalid-plans=ON",
        f"--use-ipython-kernel={use_ip_kernel}",
        "--ipython-kernel-ip=127.0.0.1",
        "--ipython-matplotlib=qt",
        "--zmq-data-proxy-addr=tcp://localhost:5571",
        "--databroker-config=NEW",
        "--zmq-info-addr=tcp://*:60629",
        "--zmq-publish-console=OFF",
        "--console-output=OFF",
    ]


def _get_expected_settings_params_3(file_dir):
    use_ip_kernel = use_ipykernel_for_tests()
    if use_ip_kernel:
        startup_dir = None
        ipython_dir = os.path.split(os.path.split(file_dir)[0])[0]
        startup_profile = "collection_sim"
        user_group_permissions_path = file_dir
        existing_plans_and_devices_path = file_dir
    else:
        startup_dir = file_dir
        ipython_dir = None
        startup_profile = None
        user_group_permissions_path = file_dir
        existing_plans_and_devices_path = file_dir

    user_group_permissions_path += "/" + default_user_group_pd_fln
    existing_plans_and_devices_path += "/" + default_existing_pd_fln

    return {
        "console_logging_level": 10,
        "databroker_config": "NEW",
        "demo_mode": False,
        "emergency_lock_key": "different_lock_key",
        "existing_plans_and_devices_path": existing_plans_and_devices_path,
        "ipython_dir": ipython_dir,
        "kafka_server": "127.0.0.1:9099",
        "kafka_topic": "yet_another_topic",
        "keep_re": True,
        "device_max_depth": 5,
        "ignore_invalid_plans": True,
        "use_ipython_kernel": bool(use_ip_kernel),
        "ipython_kernel_ip": "127.0.0.1",
        "ipython_matplotlib": "qt",
        "print_console_output": False,
        "redis_addr": "localhost:6379",
        "redis_name_prefix": "qs_unit_tests3",  # Specific for this test
        "startup_dir": startup_dir,
        "startup_module": None,
        "startup_profile": startup_profile,
        "startup_script": None,
        "update_existing_plans_devices": "NEVER",
        "use_persistent_metadata": True,
        "user_group_permissions_path": user_group_permissions_path,
        "user_group_permissions_reload": "NEVER",
        "zmq_control_addr": "tcp://*:60619",
        "zmq_data_proxy_addr": "tcp://localhost:5571",
        "zmq_info_addr": "tcp://*:60629",
        "zmq_private_key": "Ue=.po0aQ9.}<Xvrny+f{V04XMc6JZ9ufKf5aeFy",
        "zmq_publish_console": False,
    }


def _get_empty_params_1(file_dir):
    return []


# fmt: off
@pytest.mark.parametrize("pass_config, dest_dir, get_cli_params, get_expected_settings", [
    # Starting RE Manager using default parameters (--verbose CLI parameter is always set)
    (None, None, _get_empty_params_1, _get_expected_settings_default_1),
    # Pass config file (use EV to pass the path)
    ("name_as_ev", _dir_2, _get_empty_params_1, _get_expected_settings_config_2),
    # Pass config file (use --config CLI parameter to pass the path)
    ("name_as_param1", _dir_2, _get_empty_params_1, _get_expected_settings_config_2),
    # Pass the config file and a set of CLI parameters that override the config parameters
    ("name_as_param2", _dir_3, _get_cli_params_3, _get_expected_settings_params_3),
])
# fmt: on
def test_manager_with_config_file_01(
    tmpdir, monkeypatch, re_manager_cmd, pass_config, dest_dir, get_cli_params, get_expected_settings  # noqa: F811
):
    """
    Basic test for parameter handling functionality. Test if the parameters are successfully
    loaded from config file and if CLI parameters override the parameters from config.
    The test is not comprehensive or well organized, so it does not test the details, but
    it is likely to fail if there are major issues with parameter handling.
    """
    if dest_dir:
        file_dir = copy_default_profile_collection(os.path.join(tmpdir, dest_dir))
    else:
        file_dir = None

    save_settings_path = os.path.join(tmpdir, "current_settings.yaml")
    monkeypatch.setenv("QSERVER_SETTINGS_SAVE_TO_FILE", save_settings_path)

    config_path = os.path.join(tmpdir, "config.yml")
    if pass_config:
        set_qserver_zmq_public_key(monkeypatch, server_public_key="=E0[czQkp!!%0TL1LCJ5X[<wjYD[iV+p[yuaI0an")
        set_qserver_zmq_address(monkeypatch, zmq_server_address="tcp://localhost:60617")
        with open(config_path, "w") as f:
            f.writelines(_get_config_file_2(file_dir))

    cli_params = get_cli_params(file_dir)
    if cli_params:
        set_qserver_zmq_public_key(monkeypatch, server_public_key="=E0[czQkp!!%0TL1LCJ5X[<wjYD[iV+p[yuaI0an")
        set_qserver_zmq_address(monkeypatch, zmq_server_address="tcp://localhost:60619")

    params_server = cli_params

    if pass_config == "name_as_ev":
        monkeypatch.setenv("QSERVER_CONFIG", config_path)
    elif pass_config in ("name_as_param1", "name_as_param2"):
        params_server.append(f"--config={config_path}")
    elif pass_config is not None:
        assert False, f"Unknown option {pass_config!r}"

    set_redis_name_prefix = pass_config not in ("name_as_ev", "name_as_param1")
    re = re_manager_cmd(params_server, set_redis_name_prefix=set_redis_name_prefix)
    if get_cli_params == _get_empty_params_1:
        re.set_used_redis_name_prefix("qs_unit_tests2")

    # Test that the manager started successfully
    state = get_manager_status()
    assert state["manager_state"] == "idle"

    with open(save_settings_path, "r") as f:
        current_settings = yaml.load(f, Loader=yaml.FullLoader)

    # Remove 'startup_dir' from the settings dictionaries and compare them
    #   separately. This is needed because the default startup directory
    #   returned by 'get_default_startup_dir()` is different for the manager
    #   and the testing code when the test is running on CI.
    expected_settings = copy.deepcopy(get_expected_settings(file_dir))

    print("Current_settings:\n" + pprint.pformat(current_settings))  # Useful in case of failure
    print("Expected settings:\n" + pprint.pformat(expected_settings))  # Useful in case of failure

    startup_dir = current_settings.pop("startup_dir")
    suffix = expected_settings.pop("startup_dir")
    if isinstance(suffix, str):
        assert isinstance(startup_dir, str), startup_dir
        assert startup_dir.endswith(suffix), (startup_dir, suffix)
    else:  # Could be None
        assert startup_dir == suffix

    user_group_permissions_path = current_settings.pop("user_group_permissions_path")
    suffix = expected_settings.pop("user_group_permissions_path")
    if isinstance(suffix, str):
        assert isinstance(user_group_permissions_path, str), user_group_permissions_path
        assert user_group_permissions_path.endswith(suffix), (user_group_permissions_path, suffix)
    else:  # Could be None
        assert user_group_permissions_path == suffix

    existing_plans_and_devices_path = current_settings.pop("existing_plans_and_devices_path")
    suffix = expected_settings.pop("existing_plans_and_devices_path")
    if isinstance(suffix, str):
        assert isinstance(existing_plans_and_devices_path, str), existing_plans_and_devices_path
        assert existing_plans_and_devices_path.endswith(suffix), (existing_plans_and_devices_path, suffix)
    else:  # Could be None
        assert existing_plans_and_devices_path == suffix

    ipython_dir = current_settings.pop("ipython_dir")
    suffix = expected_settings.pop("ipython_dir")
    if isinstance(suffix, str):
        assert isinstance(ipython_dir, str), ipython_dir
        assert ipython_dir.endswith(suffix), (ipython_dir, suffix)
    else:  # Could be None
        assert ipython_dir == suffix

    assert current_settings == expected_settings


_sim_bundle_A_depth_0 = {
    "dets": {
        "det_A": {"Imax": {}, "center": {}, "noise": {}, "noise_multiplier": {}, "sigma": {}, "val": {}},
        "det_B": {"Imax": {}, "center": {}, "noise": {}, "noise_multiplier": {}, "sigma": {}, "val": {}},
    },
    "mtrs": {
        "x": {"acceleration": {}, "readback": {}, "setpoint": {}, "unused": {}, "velocity": {}},
        "y": {"acceleration": {}, "readback": {}, "setpoint": {}, "unused": {}, "velocity": {}},
        "z": {"acceleration": {}, "readback": {}, "setpoint": {}, "unused": {}, "velocity": {}},
    },
}

_sim_bundle_A_depth_1 = {}

_sim_bundle_A_depth_2 = {"dets": {}, "mtrs": {}}


# fmt: off
@pytest.mark.parametrize("device_max_depth, det_structure", [
    (None, _sim_bundle_A_depth_0),
    (0, _sim_bundle_A_depth_0),
    (1, _sim_bundle_A_depth_1),
    (2, _sim_bundle_A_depth_2),
])
# fmt: on
def test_cli_device_max_depth_01(re_manager_cmd, tmp_path, device_max_depth, det_structure):  # noqa: F811
    """
    CLI parameter '--device-max-depth': pass default max depth as a CLI parameter.
    Verify that the device descriptions are including components up to correct depth both
    for the devices from startup code and from uploaded script.
    """
    # Copy the default profile collection and generate the list of existing devices
    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=True)

    # Start the manager
    params = ["--startup-dir", pc_path]
    if device_max_depth is not None:
        params.extend(["--device-max-depth", str(device_max_depth)])
    re_manager_cmd(params)

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp3, _ = zmq_single_request("devices_allowed", {"user_group": _user_group})
    assert resp3["success"] is True
    devices_allowed_1 = resp3["devices_allowed"]
    assert "sim_bundle_A" in devices_allowed_1

    resp3, _ = zmq_single_request("devices_allowed")

    script = "sim_bundle_copy = sim_bundle_A"
    resp4, _ = zmq_single_request("script_upload", params={"script": script})
    assert resp4["success"] is True, resp4

    wait_for_condition(time=10, condition=condition_manager_idle)

    resp5, _ = zmq_single_request("devices_allowed", {"user_group": _user_group})
    assert resp5["success"] is True
    devices_allowed_2 = resp5["devices_allowed"]
    assert "sim_bundle_A" in devices_allowed_2
    assert "sim_bundle_copy" in devices_allowed_2

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)

    def reduce_device_description(description):
        def inner(dev, dev_out):
            if "components" in dev:
                for name in dev["components"]:
                    dev_out[name] = {}
                    inner(dev["components"][name], dev_out[name])

        dev_out = {}
        inner(description, dev_out)
        return dev_out

    desc1 = reduce_device_description(devices_allowed_1["sim_bundle_A"])
    desc2a = reduce_device_description(devices_allowed_2["sim_bundle_A"])
    desc2b = reduce_device_description(devices_allowed_2["sim_bundle_copy"])

    assert desc1 == det_structure, pprint.pformat(desc1)
    assert desc2a == det_structure, pprint.pformat(desc2a)
    assert desc2b == det_structure, pprint.pformat(desc2b)


_script_check_env = """
def func_for_test_check_env():
    from bluesky_queueserver import is_re_worker_active, is_ipython_mode
    is_ipython = is_ipython_mode()
    is_worker = is_re_worker_active()
    return is_worker, is_ipython
"""


# fmt: off
@pytest.mark.parametrize("use_ipython_kernel", [False, True, None])
# fmt: on
def test_cli_use_ipython_kernel_01(re_manager_cmd, use_ipython_kernel):  # noqa: F811
    """
    CLI parameter '--use-ipython-kernel': start the worker with IPython kernel.

    The test designed to work correctly for tests with IP mode enabled and disabled
    (by env variable USE_IPYKERNEL).
    """
    # Start the manager
    params = []
    if use_ipython_kernel is not None:
        on_off = "ON" if use_ipython_kernel else "OFF"
        params.extend([f"--use-ipython-kernel={on_off}"])
    re_manager_cmd(params)

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp, _ = zmq_single_request("script_upload", params={"script": _script_check_env})
    assert resp["success"] is True, pprint.pformat(resp)
    assert wait_for_condition(time=timeout_env_open, condition=condition_manager_idle)
    task_uid = resp["task_uid"]

    resp, _ = zmq_single_request("task_result", params={"task_uid": task_uid})
    assert resp["success"] is True, pprint.pformat(resp)
    assert resp["result"]["success"] is True, pprint.pformat(resp)

    func_item = {"name": "func_for_test_check_env", "item_type": "function"}
    params = {"user": _user, "user_group": _user_group}
    resp, _ = zmq_single_request("function_execute", params={"item": func_item, **params})
    assert resp["success"] is True, pprint.pformat(resp)
    assert wait_for_condition(time=timeout_env_open, condition=condition_manager_idle)
    task_uid = resp["task_uid"]

    resp, _ = zmq_single_request("task_result", params={"task_uid": task_uid})
    assert resp["success"] is True, pprint.pformat(resp)
    assert resp["result"]["success"] is True, pprint.pformat(resp)

    if use_ipython_kernel is not None:
        expected_use_ipython = use_ipython_kernel
    else:
        expected_use_ipython = bool(use_ipykernel_for_tests())
    assert resp["result"]["return_value"] == [True, expected_use_ipython]

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("ipython_kernel_ip", ['localhost', 'auto', '127.0.0.1', None])
# fmt: on
@pytest.mark.skipif(not use_ipykernel_for_tests(), reason="Test is run only with IPython worker")
def test_cli_ipython_kernel_ip_01(re_manager_cmd, ipython_kernel_ip):  # noqa: F811
    """
    CLI parameter '--ipython-kernel-ip': check different options for IP. Verify that the kernel
    is started with appropriate IP address.

    Run this test only for IP kernel worker.
    """
    # Start the manager
    params = []
    if ipython_kernel_ip is not None:
        params.extend([f"--ipython-kernel-ip={ipython_kernel_ip}"])
    re_manager_cmd(params)

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)

    resp, _ = zmq_single_request("config_get")
    assert resp["success"] is True

    connect_info = resp["config"]["ip_connect_info"]
    if ipython_kernel_ip in ("localhost", "127.0.0.1", None):
        assert connect_info["ip"] == "127.0.0.1"
    elif ipython_kernel_ip == "auto":
        assert connect_info["ip"] != "127.0.0.1", pprint.pformat(connect_info)
        assert re.search(r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$", connect_info["ip"]), pprint.pformat(connect_info)
    else:
        assert False, f"Unsupported value of ipython_kernel_ip: {ipython_kernel_ip!r}"

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)


_script_test_plan_invalid_1 = """
def test_plan_failing(dets=det1):
    # The default value is a detector, which can not be included in the plan description.
    yield from []
"""


# fmt: off
@pytest.mark.parametrize("ignore_invalid_plans", [False, True, None])
# fmt: on
def test_cli_ignore_invalid_plans_01(tmp_path, re_manager_cmd, ignore_invalid_plans):  # noqa: F811
    """
    CLI parameter '--ignore-invalid-plans'. Verify, that invalid plans (with signatures that are
    not accepted by the queue server) are properly handled during startup.
    """

    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=True)
    append_code_to_last_startup_file(pc_path, _script_test_plan_invalid_1)

    # Start the manager
    params = [f"--startup-dir={pc_path}"]
    if ignore_invalid_plans is not None:
        on_off = "ON" if ignore_invalid_plans else "OFF"
        params.extend([f"--ignore-invalid-plans={on_off}"])
    re_manager_cmd(params)

    resp2, _ = zmq_single_request("environment_open")
    assert resp2["success"] is True
    assert resp2["msg"] == ""

    if not ignore_invalid_plans:
        assert wait_for_condition(time=timeout_env_open, condition=condition_manager_idle)
        status = get_manager_status()
        assert status["worker_environment_exists"] is False
        assert status["worker_environment_state"] == "closed"
    else:
        assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)
        status = get_manager_status()
        assert status["worker_environment_exists"] is True
        assert status["worker_environment_state"] == "idle"

        resp, _ = zmq_single_request("plans_allowed", params={"user_group": _user_group})
        assert resp["success"] is True, pprint.pformat(resp)
        plans_allowed = resp["plans_allowed"]
        assert "test_plan_failing" not in plans_allowed

        resp9, _ = zmq_single_request("environment_close")
        assert resp9["success"] is True
        assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)


# fmt: off
@pytest.mark.parametrize("ignore_invalid_plans", [False, True, None])
# fmt: on
def test_cli_ignore_invalid_plans_02(tmp_path, re_manager_cmd, ignore_invalid_plans):  # noqa: F811
    """
    CLI parameter '--ignore-invalid-plans'. Verify, that invalid plans (with signatures that are
    not accepted by the queue server) are properly handled when uploading scripts.
    """
    pc_path = copy_default_profile_collection(tmp_path, copy_yaml=True)

    # Start the manager
    params = [f"--startup-dir={pc_path}"]
    if ignore_invalid_plans is not None:
        on_off = "ON" if ignore_invalid_plans else "OFF"
        params.extend([f"--ignore-invalid-plans={on_off}"])
    re_manager_cmd(params)

    resp, _ = zmq_single_request("environment_open")
    assert resp["success"] is True
    assert resp["msg"] == ""

    assert wait_for_condition(time=timeout_env_open, condition=condition_environment_created)
    status = get_manager_status()
    assert status["worker_environment_exists"] is True
    assert status["worker_environment_state"] == "idle"

    resp, _ = zmq_single_request("script_upload", params=dict(script=_script_test_plan_invalid_1))
    assert resp["success"] is True
    assert resp["msg"] == ""

    task_uid = resp["task_uid"]
    result = wait_for_task_result(10, task_uid)

    if not ignore_invalid_plans:
        assert result["success"] is False, pprint.pformat(result)
        assert "Failed to create description of plan 'test_plan_failing'" in result["msg"]
        assert "Failed to create description of plan 'test_plan_failing'" in result["traceback"]
    else:
        assert result["success"] is True, pprint.pformat(result)
        assert result["msg"] == ""
        assert result["traceback"] == ""

    assert wait_for_condition(time=10, condition=condition_manager_idle)

    resp, _ = zmq_single_request("plans_allowed", params={"user_group": _user_group})
    assert resp["success"] is True, pprint.pformat(resp)
    plans_allowed = resp["plans_allowed"]
    assert "test_plan_failing" not in plans_allowed

    resp9, _ = zmq_single_request("environment_close")
    assert resp9["success"] is True
    assert resp9["msg"] == ""

    assert wait_for_condition(time=3, condition=condition_environment_closed)
