"""
Unit tests for the Sardana/Run-Engine coexistence in the queueserver.

The Sardana branch must not interfere with the default Bluesky Run Engine path:
  * ``tango_url`` is opt-in (CLI / YAML config), not derived from ``TANGO_HOST``.
  * ``RunEngineWorker._sardana_enabled`` / ``RunEngineManager._sardana_enabled``
    flip only when ``tango_url`` is set in the worker/manager config dict.
  * Plan validation (``validate_plan``) in ``_prepare_item`` runs the same way in
    RE mode and Sardana mode - it is the only place ``allowed_plans`` permissions
    are enforced, and the Sardana macro schema is built to match its expected
    parameter/annotation format.
  * ``_sardana_existing_plans_and_devices`` builds dicts in the same schema as
    ``existing_plans_and_devices_from_nspace``.

The tests stub out Tango and the multiprocessing.Pipe so they run without a
running Sardana Pool, Redis, or ZMQ broker.
"""

import json
import multiprocessing
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from bluesky_queueserver.manager.config import Settings
from bluesky_queueserver.manager.manager import RunEngineManager
from bluesky_queueserver.manager.worker import RunEngineWorker

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_WORKER_BASE_CONFIG = {
    "update_existing_plans_devices": "NEVER",
    "use_ipython_kernel": False,
}

_MANAGER_BASE_CONFIG = {
    "lock_key_emergency": None,
    "use_ipython_kernel": False,
    "user_group_permissions_reload": "ON_STARTUP",
    "zmq_encoding": "json",
}


def _make_worker(extra_config=None):
    cfg = dict(_WORKER_BASE_CONFIG)
    if extra_config:
        cfg.update(extra_config)
    conn_parent, conn_child = multiprocessing.Pipe()
    try:
        return RunEngineWorker(conn=conn_parent, config=cfg)
    finally:
        # Connections are kept alive by the worker, but the child end is unused.
        conn_child.close()


def _make_manager(extra_config=None):
    cfg = dict(_MANAGER_BASE_CONFIG)
    if extra_config:
        cfg.update(extra_config)
    a1, a2 = multiprocessing.Pipe()
    b1, b2 = multiprocessing.Pipe()
    mgr = RunEngineManager(
        conn_watchdog=a1,
        conn_worker=b1,
        config=cfg,
        number_of_restarts=1,
    )
    # Keep references so the connection objects aren't garbage-collected.
    mgr._test_conns = (a1, a2, b1, b2)
    return mgr


def _settings_from_argv(argv, monkeypatch, *, strip_env=True):
    """Build a Settings object the way ``start_manager()`` does, with argv stubbed.

    When ``strip_env`` is True (default), QSERVER_CONFIG and TANGO_HOST are
    removed from the environment so the test does not depend on the host setup.
    """
    monkeypatch.setattr("sys.argv", ["start-re-manager", *argv])
    if strip_env:
        monkeypatch.delenv("QSERVER_CONFIG", raising=False)
        monkeypatch.delenv("TANGO_HOST", raising=False)
    # _build_arg_parser_and_parse is private but stable in this branch.
    from bluesky_queueserver.manager.start_manager import _build_arg_parser_and_parse

    parser, args = _build_arg_parser_and_parse(s_enc="")
    return Settings(parser=parser, args=args)


# ---------------------------------------------------------------------------
# tango_url Settings resolution
# ---------------------------------------------------------------------------


class TestTangoUrlSetting:
    def test_default_is_none(self, monkeypatch):
        """No CLI, no config, no TANGO_HOST -> Settings.tango_url is None."""
        settings = _settings_from_argv([], monkeypatch)
        assert settings.tango_url is None

    def test_tango_host_env_is_ignored(self, monkeypatch):
        """TANGO_HOST being set must NOT enable Sardana mode by itself."""
        monkeypatch.setenv("TANGO_HOST", "some-host:10000")
        settings = _settings_from_argv([], monkeypatch, strip_env=False)
        assert settings.sardana_enable is False
        assert settings.tango_url is None

    def test_cli_flag_sets_tango_url(self, monkeypatch):
        settings = _settings_from_argv(["--tango_url", "tango-db:10000"], monkeypatch)
        assert settings.tango_url == "tango-db:10000"


# ---------------------------------------------------------------------------
# RunEngineWorker gating
# ---------------------------------------------------------------------------


class TestWorkerSardanaGating:
    def test_disabled_without_tango_url(self):
        worker = _make_worker()
        assert worker._sardana_enabled is False
        assert worker.dev is None

    def test_enabled_with_tango_url(self):
        worker = _make_worker({"tango_url": "tango-db:10000"})
        assert worker._sardana_enabled is True
        # 'dev' is only populated during _worker_startup_code, not __init__.
        assert worker.dev is None

    def test_re_state_falls_back_to_runengine_in_re_mode(self):
        """In RE mode, re_state must read from self._RE, not self.dev."""
        worker = _make_worker()
        worker._RE = SimpleNamespace(state="idle")
        assert worker.re_state == "idle"

    def test_re_state_uses_door_in_sardana_mode(self):
        """In Sardana mode, re_state translates Tango DevState to qserver strings."""
        from bluesky_queueserver.manager import worker as worker_mod

        worker = _make_worker({"tango_url": "tango-db:10000"})

        dev = MagicMock()
        dev.State.return_value = worker_mod.DevState.RUNNING
        worker.dev = dev
        assert worker.re_state == "running"

        dev.State.return_value = worker_mod.DevState.STANDBY
        assert worker.re_state == "paused"

        dev.State.return_value = worker_mod.DevState.ON
        assert worker.re_state == "idle"


# ---------------------------------------------------------------------------
# RunEngineManager gating
# ---------------------------------------------------------------------------


class TestManagerSardanaGating:
    def test_disabled_without_tango_url_does_not_touch_tango(self):
        """Constructing the manager without tango_url must not import or call Tango."""
        with (
            patch("bluesky_queueserver.manager.manager.Database") as mock_db,
            patch("bluesky_queueserver.manager.manager.DeviceProxy") as mock_proxy,
        ):
            mgr = _make_manager()
        assert mgr._sardana_enabled is False
        assert mgr._tango_url is None
        assert mgr._measurement_groups == []
        mock_db.assert_not_called()
        mock_proxy.assert_not_called()

    def test_enabled_with_tango_url_queries_pool(self):
        """With tango_url set, the manager queries the Sardana Pool for MGs."""
        with (
            patch("bluesky_queueserver.manager.manager.Database") as mock_db_cls,
            patch("bluesky_queueserver.manager.manager.DeviceProxy") as mock_proxy_cls,
        ):
            mock_db = mock_db_cls.return_value
            mock_db.get_device_exported_for_class.return_value = ["pool/demo/1"]
            mock_pool = mock_proxy_cls.return_value
            mock_pool.MeasurementGroupList = [
                '{"name": "mg1", "elements": ["mot01", "ct01"]}',
            ]

            mgr = _make_manager({"tango_url": "tango-db:10000"})
            mgr._connect_sardana()

        assert mgr._sardana_enabled is True
        assert mgr._tango_url == "tango-db:10000"
        mock_db_cls.assert_called_once_with("tango-db", "10000")
        mock_db.get_device_exported_for_class.assert_called_once_with("pool*")
        assert mgr._measurement_groups == [
            {"name": "mg1", "elements": ["mot01", "ct01"]},
        ]

    def test_enabled_no_pool_found_is_not_fatal(self):
        """If no Sardana Pool exists at that TANGO_HOST, manager still constructs."""
        with (
            patch("bluesky_queueserver.manager.manager.Database") as mock_db_cls,
            patch("bluesky_queueserver.manager.manager.DeviceProxy") as mock_proxy_cls,
        ):
            mock_db_cls.return_value.get_device_exported_for_class.return_value = []
            mgr = _make_manager({"tango_url": "tango-db:10000"})
            mgr._connect_sardana()

        assert mgr._sardana_enabled is True
        assert mgr._pool == []
        assert mgr._dev is None
        assert mgr._measurement_groups == []
        mock_proxy_cls.assert_not_called()


# ---------------------------------------------------------------------------
# _prepare_item validation gating
# ---------------------------------------------------------------------------


class TestPrepareItemValidation:
    def _item(self):
        return {"name": "count", "args": [["det1"]], "item_type": "plan"}

    def test_validate_plan_called_in_re_mode(self):
        with (
            patch("bluesky_queueserver.manager.manager.Database"),
            patch("bluesky_queueserver.manager.manager.DeviceProxy"),
        ):
            mgr = _make_manager()

        with patch(
            "bluesky_queueserver.manager.manager.validate_plan",
            return_value=(True, ""),
        ) as mock_validate:
            mgr._prepare_item(
                item=self._item(),
                item_type="plan",
                user="u",
                user_group="primary",
                generate_new_uid=True,
            )
        mock_validate.assert_called_once()

    def test_validate_plan_called_in_sardana_mode(self):
        """
        'allowed_plans' membership is enforced only inside validate_plan() - skipping
        it entirely for Sardana would also skip permission enforcement, not just
        RE-specific argument checks. The Sardana macro schema uses the same
        parameter/annotation format as bluesky plans, so validate_plan applies to
        both modes the same way.
        """
        with (
            patch("bluesky_queueserver.manager.manager.Database") as mock_db_cls,
            patch("bluesky_queueserver.manager.manager.DeviceProxy"),
        ):
            mock_db_cls.return_value.get_device_exported_for_class.return_value = []
            mgr = _make_manager({"tango_url": "tango-db:10000"})

        with patch(
            "bluesky_queueserver.manager.manager.validate_plan",
            return_value=(True, ""),
        ) as mock_validate:
            item, _ = mgr._prepare_item(
                item=self._item(),
                item_type="plan",
                user="u",
                user_group="primary",
                generate_new_uid=True,
            )
        mock_validate.assert_called_once()
        assert item["user"] == "u"
        assert item["user_group"] == "primary"

    def test_validate_plan_rejects_disallowed_macro_in_sardana_mode(self):
        """validate_plan's 'allowed_plans' check must still reject unknown/forbidden
        macros in Sardana mode - this is the only place that enforces it."""
        with (
            patch("bluesky_queueserver.manager.manager.Database") as mock_db_cls,
            patch("bluesky_queueserver.manager.manager.DeviceProxy"),
        ):
            mock_db_cls.return_value.get_device_exported_for_class.return_value = []
            mgr = _make_manager({"tango_url": "tango-db:10000"})
        mgr._allowed_plans = {"primary": {}}  # no plans allowed

        with pytest.raises(RuntimeError, match="not in the list of allowed plans"):
            mgr._prepare_item(
                item=self._item(),
                item_type="plan",
                user="u",
                user_group="primary",
                generate_new_uid=True,
            )


# ---------------------------------------------------------------------------
# _sardana_existing_plans_and_devices helper
# ---------------------------------------------------------------------------


class TestSardanaExistingPlansAndDevices:
    """
    Macros live on the Sardana MacroServer device (attribute ``MacroList``)
    and devices live on per-type Pool attributes (``MotorList``,
    ``ExpChannelList``, ...). ``Database.get_device_exported_for_class`` is
    called twice - once for ``MacroServer`` and once for ``Pool`` - so the
    DB mock needs to dispatch on the requested class.
    """

    @staticmethod
    def _make_db(*, macroservers, pools):
        db = MagicMock()

        def by_class(cls):
            if cls == "MacroServer":
                return list(macroservers)
            if cls == "Pool":
                return list(pools)
            return []

        db.get_device_exported_for_class.side_effect = by_class
        return db

    @staticmethod
    def _make_pool(*, motors=(), exp_channels=(), io_registers=(), meas_groups=()):
        pool = MagicMock()
        pool.MotorList = list(motors)
        pool.PseudoMotorList = []
        pool.ExpChannelList = list(exp_channels)
        pool.PseudoCounterList = []
        pool.IORegisterList = list(io_registers)
        pool.MotorGroupList = []
        # MeasurementGroupList is set so a test can prove the helper ignores it,
        # but the helper itself does *not* iterate this attribute.
        pool.MeasurementGroupList = list(meas_groups)
        return pool

    @staticmethod
    def _make_ms(*, macros, info_blobs=None, info_side_effect=None):
        """Build a MacroServer mock with MacroList + a stubbed GetMacroInfo.

        - ``info_blobs`` is a list of JSON strings returned by GetMacroInfo.
        - ``info_side_effect`` lets a test raise from the bulk call.
        - If both are None, GetMacroInfo returns ``[]`` so the helper falls
          back to bare-stub plan entries.
        """
        ms = MagicMock()
        ms.MacroList = list(macros)
        if info_side_effect is not None:
            ms.command_inout.side_effect = info_side_effect
        else:
            ms.command_inout.return_value = list(info_blobs or [])
        return ms

    def test_schema_matches_existing_plans_format(self):
        worker = _make_worker({"tango_url": "tango-db:10000"})
        worker.dev = MagicMock()  # Door proxy - no longer consulted by the helper

        ms = self._make_ms(macros=["ascan", "ct"])  # no info_blobs -> bare-stub plans
        pool = self._make_pool(motors=['{"name": "mot01", "type": "Motor"}'])

        db = self._make_db(
            macroservers=["macroserver/demo/1"],
            pools=["pool/demo/1"],
        )

        with (
            patch("bluesky_queueserver.manager.worker.Database", return_value=db),
            patch(
                "bluesky_queueserver.manager.worker.DeviceProxy",
                side_effect=[pool, ms],
            ),
        ):
            plans, devices = worker._sardana_existing_plans_and_devices()

        assert set(plans) == {"ascan", "ct"}
        for name, p in plans.items():
            assert p["name"] == name
            assert p["module"] == "sardana.macroserver"
            assert p["properties"] == {"is_generator": True}
            assert p["parameters"] == []

        assert "mot01" in devices
        d = devices["mot01"]
        assert d["classname"] == "Motor"
        assert d["module"] == "sardana.pool"
        assert d["is_movable"] is True
        assert d["is_readable"] is True
        assert d["is_flyable"] is False

    def test_empty_lists_yield_empty_dicts(self):
        worker = _make_worker({"tango_url": "tango-db:10000"})

        ms = self._make_ms(macros=[])
        pool = self._make_pool()
        db = self._make_db(
            macroservers=["macroserver/demo/1"],
            pools=["pool/demo/1"],
        )

        with (
            patch("bluesky_queueserver.manager.worker.Database", return_value=db),
            patch(
                "bluesky_queueserver.manager.worker.DeviceProxy",
                side_effect=[pool, ms],
            ),
        ):
            plans, devices = worker._sardana_existing_plans_and_devices()
        assert plans == {}
        assert devices == {}

    def test_malformed_element_json_is_skipped(self):
        worker = _make_worker({"tango_url": "tango-db:10000"})

        ms = self._make_ms(macros=["ct"])
        # Three "Motor"-list entries: one not JSON, one missing 'name', one valid CT
        pool = self._make_pool(
            motors=[
                "not-json",
                '{"type": "Motor"}',  # missing 'name' -> skipped
            ],
            exp_channels=['{"name": "ct01", "type": "CTExpChannel"}'],
        )
        db = self._make_db(
            macroservers=["macroserver/demo/1"],
            pools=["pool/demo/1"],
        )

        with (
            patch("bluesky_queueserver.manager.worker.Database", return_value=db),
            patch(
                "bluesky_queueserver.manager.worker.DeviceProxy",
                side_effect=[pool, ms],
            ),
        ):
            plans, devices = worker._sardana_existing_plans_and_devices()

        assert list(devices) == ["ct01"]
        assert devices["ct01"]["is_movable"] is False
        assert devices["ct01"]["classname"] == "CTExpChannel"

    def test_measurement_groups_are_excluded_from_devices(self):
        """MeasurementGroups are surfaced via config_get, not as devices."""
        worker = _make_worker({"tango_url": "tango-db:10000"})

        ms = self._make_ms(macros=[])
        pool = self._make_pool(
            motors=['{"name": "mot01", "type": "Motor"}'],
            meas_groups=[
                '{"name": "mg_test", "type": "MeasurementGroup"}',
                '{"name": "mg_fast", "type": "MeasurementGroup"}',
            ],
        )
        db = self._make_db(
            macroservers=["macroserver/demo/1"],
            pools=["pool/demo/1"],
        )

        with (
            patch("bluesky_queueserver.manager.worker.Database", return_value=db),
            patch(
                "bluesky_queueserver.manager.worker.DeviceProxy",
                side_effect=[pool, ms],
            ),
        ):
            plans, devices = worker._sardana_existing_plans_and_devices()

        assert set(devices) == {"mot01"}
        assert "mg_test" not in devices
        assert "mg_fast" not in devices

    def test_macroserver_failure_does_not_raise(self):
        worker = _make_worker({"tango_url": "tango-db:10000"})

        # MacroList raises -> plans dict ends up empty, but device discovery still runs.
        ms = MagicMock()
        type(ms).MacroList = property(lambda self: (_ for _ in ()).throw(RuntimeError("comm failure")))
        pool = self._make_pool(motors=['{"name": "mot01", "type": "Motor"}'])
        db = self._make_db(
            macroservers=["macroserver/demo/1"],
            pools=["pool/demo/1"],
        )

        with (
            patch("bluesky_queueserver.manager.worker.Database", return_value=db),
            patch(
                "bluesky_queueserver.manager.worker.DeviceProxy",
                side_effect=[pool, ms],
            ),
        ):
            plans, devices = worker._sardana_existing_plans_and_devices()
        assert plans == {}
        assert set(devices) == {"mot01"}

    def test_macros_enriched_from_getmacroinfo(self):
        """GetMacroInfo entries are translated into qserver parameter dicts."""
        worker = _make_worker({"tango_url": "tango-db:10000"})

        ascan_info = json.dumps(
            {
                "name": "ascan",
                "module": "scan",
                "description": "Absolute scan.",
                "parameters": [
                    {
                        "name": "motor",
                        "type": "Moveable",
                        "description": "Moveable to move",
                        "default_value": None,
                        "min": None,
                        "max": None,
                    },
                    {
                        "name": "start_pos",
                        "type": "Float",
                        "description": "Scan start",
                        "default_value": None,
                        "min": None,
                        "max": None,
                    },
                    {
                        "name": "nr_interv",
                        "type": "Integer",
                        "description": "Intervals",
                        "default_value": None,
                        "min": 1,
                        "max": 1000,
                    },
                    {
                        "name": "integ_time",
                        "type": "Float",
                        "description": "Integ time (s)",
                        "default_value": 1.0,
                        "min": None,
                        "max": None,
                    },
                ],
            }
        )
        # GetMacroInfo can return results in a different order than the input.
        ms = self._make_ms(macros=["ascan", "ct"], info_blobs=[ascan_info])
        pool = self._make_pool(
            motors=[
                '{"name": "mot01", "type": "Motor"}',
                '{"name": "mot02", "type": "Motor"}',
            ],
        )
        db = self._make_db(
            macroservers=["macroserver/demo/1"],
            pools=["pool/demo/1"],
        )

        with (
            patch("bluesky_queueserver.manager.worker.Database", return_value=db),
            patch(
                "bluesky_queueserver.manager.worker.DeviceProxy",
                side_effect=[pool, ms],
            ),
        ):
            plans, _ = worker._sardana_existing_plans_and_devices()

        # ascan is fully populated
        ascan = plans["ascan"]
        assert ascan["module"] == "scan"
        assert ascan["description"] == "Absolute scan."
        param_names = [p["name"] for p in ascan["parameters"]]
        assert param_names == ["motor", "start_pos", "nr_interv", "integ_time"]

        # Daiquiri form-builder annotations: scalar types pass through as
        # Python names; element references gain a `devices` group reference.
        annotations = {p["name"]: p["annotation"] for p in ascan["parameters"]}
        assert annotations["start_pos"] == {"type": "float"}
        assert annotations["nr_interv"] == {"type": "int"}
        assert annotations["integ_time"] == {"type": "float"}
        assert annotations["motor"] == {
            "type": "Motors",
            "devices": {"Motors": ["mot01", "mot02"]},
        }

        # Bounds passed through as strings
        nr = next(p for p in ascan["parameters"] if p["name"] == "nr_interv")
        assert nr["min"] == "1"
        assert nr["max"] == "1000"

        # Default value passed through
        integ = next(p for p in ascan["parameters"] if p["name"] == "integ_time")
        assert "default" in integ

        # ct wasn't in info_blobs -> bare stub fallback
        ct = plans["ct"]
        assert ct["parameters"] == []
        assert ct["module"] == "sardana.macroserver"

    def test_getmacroinfo_failure_falls_back_to_bare_stubs(self):
        """If GetMacroInfo blows up, every macro still appears (just without params)."""
        worker = _make_worker({"tango_url": "tango-db:10000"})

        ms = self._make_ms(
            macros=["ascan", "ct"],
            info_side_effect=RuntimeError("MacroServer is sulking"),
        )
        pool = self._make_pool()
        db = self._make_db(
            macroservers=["macroserver/demo/1"],
            pools=["pool/demo/1"],
        )

        with (
            patch("bluesky_queueserver.manager.worker.Database", return_value=db),
            patch(
                "bluesky_queueserver.manager.worker.DeviceProxy",
                side_effect=[pool, ms],
            ),
        ):
            plans, _ = worker._sardana_existing_plans_and_devices()

        assert set(plans) == {"ascan", "ct"}
        for p in plans.values():
            assert p["parameters"] == []
            assert p["module"] == "sardana.macroserver"

    def test_element_param_types_emit_device_groups_for_daiquiri(self):
        """
        Element-typed macro parameters (Moveable, ExpChannel, MeasurementGroup,
        IORegister) become group references with a populated
        ``annotation.devices`` dict so a form builder like Daiquiri can render
        them as dropdowns of the right scope.
        """
        worker = _make_worker({"tango_url": "tango-db:10000"})

        info = json.dumps(
            {
                "name": "ct_at_motor",
                "parameters": [
                    {
                        "name": "motor",
                        "type": "Moveable",
                        "default_value": None,
                        "min": None,
                        "max": None,
                        "description": "Where to go",
                    },
                    {
                        "name": "counter",
                        "type": "ExpChannel",
                        "default_value": None,
                        "min": None,
                        "max": None,
                        "description": "Channel",
                    },
                    {
                        "name": "mg",
                        "type": "MeasurementGroup",
                        "default_value": None,
                        "min": None,
                        "max": None,
                        "description": "Active MG",
                    },
                    {
                        "name": "shutter",
                        "type": "IORegister",
                        "default_value": None,
                        "min": None,
                        "max": None,
                        "description": "Shutter register",
                    },
                ],
            }
        )
        ms = self._make_ms(macros=["ct_at_motor"], info_blobs=[info])
        # Build representative items in every relevant pool list. ``mg_test`` is
        # in MeasurementGroupList so it must appear in the group dict for the
        # form even though it never lands in ``existing_devices``.
        pool = self._make_pool(
            motors=['{"name": "mot01", "type": "Motor"}'],
            exp_channels=['{"name": "ct01", "type": "CTExpChannel"}'],
            io_registers=['{"name": "shutter1", "type": "IORegister"}'],
            meas_groups=['{"name": "mg_test", "type": "MeasurementGroup"}'],
        )
        db = self._make_db(
            macroservers=["macroserver/demo/1"],
            pools=["pool/demo/1"],
        )

        with (
            patch("bluesky_queueserver.manager.worker.Database", return_value=db),
            patch(
                "bluesky_queueserver.manager.worker.DeviceProxy",
                side_effect=[pool, ms],
            ),
        ):
            plans, devices = worker._sardana_existing_plans_and_devices()

        annotations = {p["name"]: p["annotation"] for p in plans["ct_at_motor"]["parameters"]}

        assert annotations["motor"] == {
            "type": "Motors",
            "devices": {"Motors": ["mot01"]},
        }
        assert annotations["counter"] == {
            "type": "ExpChannels",
            "devices": {"ExpChannels": ["ct01"]},
        }
        assert annotations["mg"] == {
            "type": "MeasurementGroups",
            "devices": {"MeasurementGroups": ["mg_test"]},
        }
        assert annotations["shutter"] == {
            "type": "IORegisters",
            "devices": {"IORegisters": ["shutter1"]},
        }

        # Sanity: the MeasurementGroup itself is still excluded from
        # existing_devices, even though it appears in the form's group list.
        assert "mg_test" not in devices
        assert set(devices) == {"mot01", "ct01", "shutter1"}

    def test_paramrepeat_type_list_becomes_var_positional(self):
        """
        Sardana macros like `mv` / `wm` declare repeating parameter groups
        via ParamRepeat: GetMacroInfo then returns the parameter's ``type``
        as a *list* of nested parameter dicts. The helper must not crash and
        must emit a sensible VAR_POSITIONAL entry instead.
        """
        worker = _make_worker({"tango_url": "tango-db:10000"})

        mv_info = json.dumps(
            {
                "name": "mv",
                "parameters": [
                    {
                        "name": "motor_pos_list",
                        "description": "List of motor/position pairs",
                        "default_value": None,
                        "min": 1,
                        "max": None,
                        "type": [
                            {
                                "name": "motor",
                                "type": "Moveable",
                                "description": "Motor to move",
                                "default_value": None,
                                "min": None,
                                "max": None,
                            },
                            {
                                "name": "pos",
                                "type": "Float",
                                "description": "Target position",
                                "default_value": None,
                                "min": None,
                                "max": None,
                            },
                        ],
                    },
                ],
            }
        )
        ms = self._make_ms(macros=["mv"], info_blobs=[mv_info])
        pool = self._make_pool()
        db = self._make_db(macroservers=["ms/1"], pools=["pool/1"])

        with (
            patch("bluesky_queueserver.manager.worker.Database", return_value=db),
            patch(
                "bluesky_queueserver.manager.worker.DeviceProxy",
                side_effect=[pool, ms],
            ),
        ):
            plans, _ = worker._sardana_existing_plans_and_devices()

        assert "mv" in plans
        param = plans["mv"]["parameters"][0]
        assert param["name"] == "motor_pos_list"
        assert param["kind"] == {"name": "VAR_POSITIONAL", "value": 2}
        # Union, not plain 'str': ParamRepeat groups like mv's motor/position pairs
        # alternate device-name strings and numeric values, so a plain 'str' type
        # would reject the numeric half under strict pydantic validation.
        assert param["annotation"] == {"type": "typing.Union[str, float, int]"}
        # Description survives (or falls back to the inner field names)
        assert "motor" in param["description"]
        assert param["min"] == "1"

    def test_unknown_param_type_falls_back_to_str(self):
        """Sardana types we haven't mapped (or new ones) become 'str'."""
        worker = _make_worker({"tango_url": "tango-db:10000"})

        info = json.dumps(
            {
                "name": "weird",
                "parameters": [
                    {
                        "name": "thing",
                        "type": "SomeBrandNewType",
                        "default_value": None,
                        "min": None,
                        "max": None,
                        "description": "",
                    },
                ],
            }
        )
        ms = self._make_ms(macros=["weird"], info_blobs=[info])
        pool = self._make_pool()
        db = self._make_db(macroservers=["ms/1"], pools=["pool/1"])

        with (
            patch("bluesky_queueserver.manager.worker.Database", return_value=db),
            patch(
                "bluesky_queueserver.manager.worker.DeviceProxy",
                side_effect=[pool, ms],
            ),
        ):
            plans, _ = worker._sardana_existing_plans_and_devices()

        assert plans["weird"]["parameters"][0]["annotation"]["type"] == "str"

    def test_tango_db_unreachable_is_not_fatal(self):
        worker = _make_worker({"tango_url": "tango-db:10000"})
        worker.dev = MagicMock()

        # Database constructor itself blows up -> the helper returns empty dicts.
        with patch(
            "bluesky_queueserver.manager.worker.Database",
            side_effect=RuntimeError("tango db unreachable"),
        ):
            plans, devices = worker._sardana_existing_plans_and_devices()

        assert plans == {}
        assert devices == {}
