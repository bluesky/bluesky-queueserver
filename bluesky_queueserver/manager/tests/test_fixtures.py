import pytest

from bluesky_queueserver.manager.comms import zmq_single_request

from .common import re_manager_cmd, db_catalog  # noqa: F401

from .common import (
    wait_for_condition,
    condition_environment_created,
    condition_manager_idle,
    condition_environment_closed,
)

# User name and user group name used throughout most of the tests.
_user, _user_group = "Testing Script", "admin"


@pytest.mark.xfail(reason="For some reason the test fails when run on CI, but expected to pass locally")
def test_fixture_db_catalog(db_catalog):  # noqa F811
    """
    Basic test for the fixture `db_catalog`.
    """
    assert db_catalog["catalog_name"] == "qserver_tests"
    # Catalog does not necessarily need to be empty, since it will accumulate results of
    #   all the tests in the current session.
    list(db_catalog["catalog"])

    # Try to instantiate the Data Broker
    from databroker import Broker

    Broker.named(db_catalog["catalog_name"])

    # Try to access the catalog in 'standard' way
    from databroker import catalog

    catalog.force_reload()
    assert list(catalog[db_catalog["catalog_name"]]) == list(db_catalog["catalog"])


def test_fixture_re_manager_cmd_1(re_manager_cmd):  # noqa F811
    """
    Basic test for ``re_manager_cmd``.
    """
    # Create RE Manager with no parameters
    re_manager_cmd()
    # The second call is supposed to close the existing RE Manager and create a new one.
    re_manager_cmd([])


def test_fixture_re_manager_cmd_2(re_manager_cmd, db_catalog):  # noqa F811
    """
    Test for the fixture ``re_manager_cmd``: start RE Manager with command line parameters.
    Subscribe RE to databroker (created by ``db_catalog``, execute the plan and make sure
    that the run is recorded by the databroker by comparing Run UIDs from history and
    start document.
    """
    db_name = db_catalog["catalog_name"]
    re_manager_cmd(["--databroker-config", db_name])

    cat = db_catalog["catalog"]

    plan = {"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10], "item_type": "plan"}

    # Plan
    params1 = {"item": plan, "user": _user, "user_group": _user_group}
    resp1, _ = zmq_single_request("queue_item_add", params1)
    assert resp1["success"] is True, f"resp={resp1}"

    resp2, _ = zmq_single_request("status")
    assert resp2["items_in_queue"] == 1
    assert resp2["items_in_history"] == 0

    # Open the environment.
    resp3, _ = zmq_single_request("environment_open")
    assert resp3["success"] is True
    assert wait_for_condition(time=10, condition=condition_environment_created)

    resp4, _ = zmq_single_request("queue_start")
    assert resp4["success"] is True

    assert wait_for_condition(time=5, condition=condition_manager_idle)

    resp5, _ = zmq_single_request("status")
    assert resp5["items_in_queue"] == 0
    assert resp5["items_in_history"] == 1

    resp6, _ = zmq_single_request("history_get")
    history = resp6["items"]
    assert len(history) == 1

    uid = history[-1]["result"]["run_uids"][0]
    start_doc = cat[uid].metadata["start"]
    assert start_doc["uid"] == uid

    # Close the environment.
    resp7, _ = zmq_single_request("environment_close")
    assert resp7["success"] is True, f"resp={resp7}"
    assert wait_for_condition(time=5, condition=condition_environment_closed)
