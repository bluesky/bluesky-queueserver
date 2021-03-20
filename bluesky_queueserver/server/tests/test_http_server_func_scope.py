import pandas as pd
import os

from bluesky_queueserver.manager.tests._common import (  # noqa F401
    re_manager,
    re_manager_pc_copy,
    copy_default_profile_collection,
    append_code_to_last_startup_file,
)

from bluesky_queueserver.server.tests.conftest import (  # noqa F401
    SERVER_ADDRESS,
    SERVER_PORT,
    add_plans_to_queue,
    fastapi_server_fs,
    request_to_json,
    wait_for_environment_to_be_created,
    wait_for_queue_execution_to_complete,
    wait_for_manager_state_idle,
)


def test_http_server_queue_upload_spreasheet_1(re_manager, fastapi_server_fs, tmp_path, monkeypatch):  # noqa F811
    """
    Test for ``/queue/upload/spreadsheet`` API: generate .xlsx file, upload it to the server, verify
    the contents of the queue, run the queue and verify that the required number of plans were successfully
    completed.
    """
    monkeypatch.setenv(
        "BS_HTTPSERVER_CUSTOM_CODE_MODULE",
        "bluesky_queueserver.server.tests.http_custom_proc_functions",
        prepend=False,
    )
    fastapi_server_fs()

    # Create sample Excel file
    ss_fln = "spreadsheet.xlsx"
    ss_path = os.path.join(tmp_path, ss_fln)

    plan_params = [["count", 5, 1], ["count", 6, 0.5]]

    # Expected plans
    plans_expected = []
    for p in plan_params:
        plans_expected.append({"name": p[0], "args": [["det1", "det2"]], "kwargs": {"num": p[1], "delay": p[2]}})

    def create_excel(ss_path):
        df = pd.DataFrame(plan_params)
        df = df.set_axis(["name", "num", "delay"], axis=1)
        df.to_excel(ss_path, engine="openpyxl")
        return df

    def verify_excel(ss_path, df):
        df_read = pd.read_excel(ss_path, index_col=0, engine="openpyxl")
        assert df_read.equals(df), str(df_read)

    df = create_excel(ss_path)
    verify_excel(ss_path, df)

    # Send the Excel file to the server
    files = {"spreadsheet": open(ss_path, "rb")}
    resp1 = request_to_json("post", "/queue/upload/spreadsheet", files=files)
    assert resp1["success"] is True, str(resp1)

    # Verify that the queue contains correct plans
    resp2 = request_to_json("get", "/queue/get")
    assert resp2["success"] is True
    assert resp2["running_item"] == {}
    queue = resp2["queue"]
    assert len(queue) == len(plans_expected), str(queue)
    for p, p_exp in zip(queue, plans_expected):
        for k, v in p_exp.items():
            assert k in p
            assert v == p[k]

    resp3 = request_to_json("post", "/environment/open")
    assert resp3["success"] is True
    assert wait_for_environment_to_be_created(10)

    resp4 = request_to_json("post", "/queue/start")
    assert resp4["success"] is True
    assert wait_for_queue_execution_to_complete(60)

    resp5 = request_to_json("get", "/status")
    assert resp5["items_in_queue"] == 0
    assert resp5["items_in_history"] == len(plans_expected)

    resp6 = request_to_json("post", "/environment/close")
    assert resp6 == {"success": True, "msg": ""}
    assert wait_for_manager_state_idle(10)
