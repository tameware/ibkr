# Usage: python -m unittest test_ibkr_app_support.py -v

import datetime
import unittest
from unittest.mock import MagicMock, Mock, call, patch
from zoneinfo import ZoneInfo

import argparse
import json
import tempfile
import threading
import types
from pathlib import Path

from ibkr_app_support import (
    PositionLedger,
    add_config_argument,
    add_ib_connection_arguments,
    add_logging_arguments,
    add_session_hours_arguments,
    cli_to_config,
    default_config_path,
    disconnect_cleanly,
    execution_belongs_to_client,
    handle_ledger_execution,
    max_sell_shares,
    ib_client_id_from_config,
    ib_error_is_status_info,
    idle_until_shutdown,
    ledger_path_for_strategy,
    load_config_file,
    load_merged_config,
    log_ib_error,
    log_startup_timezones,
    NbboCoalescer,
    NbboThrottle,
    make_stock_contract,
    log_session_transition,
    merge_config,
    open_order_belongs_to_client,
    order_status_clients_match,
    regular_session_open,
    run_bot,
    safe_cancel_order,
    session_wall_clock,
    should_suppress_ib_error,
    wait_for_ib_ready,
)

_SESSION_CFG = {
    "market_timezone": "America/New_York",
    "market_open_hour": 9,
    "market_open_minute": 30,
    "market_close_hour": 16,
}


class TestPositionLedger(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.config = {
            "symbol": "FDX",
            "sec_type": "STK",
            "client_id": 7,
            "ledgers_dir": self._tmp.name,
        }

    def tearDown(self):
        self._tmp.cleanup()

    def test_ledger_path_in_ledgers_dir(self):
        path = ledger_path_for_strategy("peg_primary", self.config)
        self.assertEqual(path.parent, Path(self._tmp.name))
        self.assertTrue(path.name.startswith("peg_primary_FDX_7"))

    def test_apply_fill_buy_and_sell(self):
        ledger = PositionLedger.open("market_maker", self.config)
        self.assertTrue(ledger.apply_fill("BUY", 100, 150.0, exec_id="e1"))
        self.assertEqual(ledger.qty, 100)
        self.assertAlmostEqual(ledger.avg_cost, 150.0)
        self.assertTrue(ledger.apply_fill("SELL", 40, 155.0, exec_id="e2"))
        self.assertEqual(ledger.qty, 60)
        self.assertAlmostEqual(ledger.avg_cost, 150.0)

    def test_apply_fill_skips_duplicate_exec_id(self):
        ledger = PositionLedger.open("peg_mid", self.config)
        self.assertTrue(ledger.apply_fill("BUY", 10, 50.0, exec_id="dup"))
        self.assertFalse(ledger.apply_fill("BUY", 10, 50.0, exec_id="dup"))
        self.assertEqual(ledger.qty, 10)

    def test_save_and_reload(self):
        ledger = PositionLedger.open("midprice", self.config)
        ledger.apply_fill("BUY", 25, 99.5, exec_id="x1")
        ledger.save()
        reloaded = PositionLedger.open("midprice", self.config)
        self.assertEqual(reloaded.qty, 25)
        self.assertAlmostEqual(reloaded.avg_cost, 99.5)

    def test_handle_ledger_execution_filters_client(self):
        ledger = PositionLedger.open("peg_best", self.config)
        execution = Mock()
        execution.clientId = 999
        execution.side = "BOT"
        execution.shares = 5
        execution.price = 10.0
        execution.execId = "bad"
        self.assertFalse(handle_ledger_execution(ledger, execution, 7))
        self.assertEqual(ledger.qty, 0)

    def test_max_sell_shares_zero_ledger_with_account_long(self):
        ledger = PositionLedger.open("peg_primary", self.config)
        ledger.record_ib_snapshot("U123", 500, 100.0)
        self.assertEqual(ledger.qty, 0)
        self.assertEqual(max_sell_shares(ledger), 0)

    def test_max_sell_shares_capped_by_ib_snapshot(self):
        ledger = PositionLedger.open("peg_primary", self.config)
        ledger.apply_fill("BUY", 100, 50.0)
        ledger.record_ib_snapshot("U123", 30, 50.0)
        self.assertEqual(max_sell_shares(ledger), 30)

    def test_apply_fill_sell_cannot_drive_ledger_negative(self):
        ledger = PositionLedger.open("peg_mid", self.config)
        ledger.apply_fill("BUY", 10, 50.0)
        ledger.apply_fill("SELL", 25, 51.0)
        self.assertEqual(ledger.qty, 0)


class TestLogStartupTimezones(unittest.TestCase):
    def test_logs_local_and_market_with_logger(self):
        logger = MagicMock()
        ny = ZoneInfo("America/New_York")
        now = datetime.datetime(2026, 5, 11, 15, 45, 0, tzinfo=ny)
        log_startup_timezones(_SESSION_CFG, logger=logger, now=now)
        self.assertEqual(logger.info.call_count, 2)
        self.assertIn("local timezone", logger.info.call_args_list[0][0][0])
        self.assertEqual(
            logger.info.call_args_list[1][0],
            (
                "Startup market_timezone=%s, now=%s",
                "America/New_York",
                "2026-05-11 15:45:00 EDT",
            ),
        )

    def test_logs_local_only_when_market_timezone_missing(self):
        logger = MagicMock()
        log_startup_timezones({}, logger=logger)
        logger.info.assert_called_once()
        self.assertIn("local timezone", logger.info.call_args[0][0])

    def test_prints_without_logger(self):
        ny = ZoneInfo("America/New_York")
        now = datetime.datetime(2026, 5, 11, 9, 0, 0, tzinfo=ny)
        with patch("builtins.print") as mock_print:
            log_startup_timezones(_SESSION_CFG, now=now)
        self.assertEqual(mock_print.call_count, 2)
        self.assertIn("market_timezone", mock_print.call_args_list[1][0][0])


class TestRegularSession(unittest.TestCase):
    def setUp(self):
        self.ny = ZoneInfo("America/New_York")

    def test_regular_session_open_during_hours(self):
        now = datetime.datetime(2026, 5, 11, 10, 30, 0, tzinfo=self.ny)
        self.assertTrue(regular_session_open(_SESSION_CFG, now=now))

    def test_regular_session_open_before_open(self):
        now = datetime.datetime(2026, 5, 11, 9, 29, 0, tzinfo=self.ny)
        self.assertFalse(regular_session_open(_SESSION_CFG, now=now))

    def test_regular_session_open_at_open(self):
        now = datetime.datetime(2026, 5, 11, 9, 30, 0, tzinfo=self.ny)
        self.assertTrue(regular_session_open(_SESSION_CFG, now=now))

    def test_regular_session_open_after_close(self):
        now = datetime.datetime(2026, 5, 11, 16, 0, 0, tzinfo=self.ny)
        self.assertFalse(regular_session_open(_SESSION_CFG, now=now))

    def test_session_wall_clock_naive_localizes(self):
        naive = datetime.datetime(2026, 5, 11, 10, 0, 0)
        now, tz_name, moh, mom, mch = session_wall_clock(_SESSION_CFG, now=naive)
        self.assertEqual(tz_name, "America/New_York")
        self.assertEqual(moh, 9)
        self.assertEqual(mom, 30)
        self.assertEqual(mch, 16)
        self.assertIsNotNone(now.tzinfo)

    def test_log_session_transition_startup_closed(self):
        logger = MagicMock()
        log_session_transition(
            logger,
            _SESSION_CFG,
            prev_in_hours=None,
            in_hours=False,
        )
        logger.info.assert_called_once()
        self.assertIn("closed at startup", logger.info.call_args[0][0])

    def test_log_session_transition_session_end(self):
        logger = MagicMock()
        log_session_transition(
            logger,
            _SESSION_CFG,
            prev_in_hours=True,
            in_hours=False,
        )
        logger.info.assert_called_once()
        self.assertIn("session ended", logger.info.call_args[0][0])

    def test_log_session_transition_no_op_when_open(self):
        logger = MagicMock()
        log_session_transition(
            logger,
            _SESSION_CFG,
            prev_in_hours=True,
            in_hours=True,
        )
        logger.info.assert_not_called()


class TestNbboThrottle(unittest.TestCase):
    def test_skips_when_interval_not_elapsed_and_key_unchanged(self):
        clock = Mock(side_effect=[10.0, 10.0, 10.5, 10.5])
        throttle = NbboThrottle(3.0, clock=clock)
        key = (50.0, 50.1)
        self.assertTrue(throttle.should_run(key))
        throttle.mark_ran(key)
        self.assertFalse(throttle.should_run(key))

    def test_runs_when_key_changes_inside_interval(self):
        clock = Mock(side_effect=[10.0, 10.0, 10.5])
        throttle = NbboThrottle(3.0, clock=clock)
        throttle.mark_ran((50.0, 50.1))
        self.assertTrue(throttle.should_run((50.0, 50.2)))

    def test_force_and_bypass(self):
        throttle = NbboThrottle(60.0, clock=lambda: 100.0)
        throttle.mark_ran((1.0, 2.0))
        self.assertTrue(throttle.should_run((1.0, 2.0), force=True))
        self.assertTrue(throttle.should_run((1.0, 2.0), bypass_if=lambda: True))
        self.assertFalse(throttle.should_run((1.0, 2.0)))


class TestNbboCoalescer(unittest.TestCase):
    def test_zero_interval_flushes_immediately(self):
        seen: list[str] = []
        NbboCoalescer(0, lambda: seen.append("flush")).schedule()
        self.assertEqual(seen, ["flush"])

    def test_defers_flush_until_timer_fires(self):
        seen: list[str] = []
        coalesce = NbboCoalescer(0.05, lambda: seen.append("flush"))
        with patch("ibkr_app_support.threading.Timer") as timer_cls:
            timer = MagicMock()
            timer_cls.return_value = timer
            coalesce.schedule()
            coalesce.schedule()
            self.assertEqual(timer_cls.call_count, 2)
            self.assertEqual(timer.cancel.call_count, 1)
            self.assertEqual(seen, [])
            timer_cls.call_args[0][1]()
            self.assertEqual(seen, ["flush"])

    def test_max_interval_flushes_during_continuous_ticks(self):
        seen: list[str] = []
        now = [0.0]

        def clock() -> float:
            return now[0]

        coalesce = NbboCoalescer(
            10.0,
            lambda: seen.append("flush"),
            max_interval_seconds=1.0,
            clock=clock,
        )
        with patch("ibkr_app_support.threading.Timer"):
            coalesce.schedule()
            now[0] = 0.4
            coalesce.schedule()
            self.assertEqual(seen, [])
            now[0] = 1.0
            coalesce.schedule()
        self.assertEqual(seen, ["flush"])

    def test_quiet_flush_defers_until_max_interval_elapsed(self):
        seen: list[str] = []
        now = [0.0]

        def clock() -> float:
            return now[0]

        coalesce = NbboCoalescer(
            0.05,
            lambda: seen.append("flush"),
            max_interval_seconds=1.0,
            clock=clock,
        )
        with patch("ibkr_app_support.threading.Timer") as timer_cls:
            timer = MagicMock()
            timer_cls.return_value = timer
            coalesce._do_flush()
            now[0] = 0.5
            coalesce.schedule()
            timer_cls.call_args[0][1]()
        self.assertEqual(seen, ["flush"])

    def test_continuous_stream_flushes_at_max_interval(self):
        seen: list[float] = []
        now = [0.0]

        def clock() -> float:
            return now[0]

        coalesce = NbboCoalescer(
            2.0,
            lambda: seen.append(clock()),
            max_interval_seconds=10.0,
            clock=clock,
        )
        with patch("ibkr_app_support.threading.Timer"):
            for step in range(150):
                now[0] = step * 0.1
                coalesce.schedule()
        self.assertEqual(len(seen), 1)
        self.assertAlmostEqual(seen[0], 10.0)


class TestMakeStockContract(unittest.TestCase):
    def test_builds_from_config(self):
        cfg = {
            "symbol": "IBM",
            "sec_type": "STK",
            "exchange": "SMART",
            "currency": "USD",
            "primary_exchange": "NYSE",
        }
        c = make_stock_contract(cfg)
        self.assertEqual(c.symbol, "IBM")
        self.assertEqual(c.secType, "STK")
        self.assertEqual(c.exchange, "SMART")
        self.assertEqual(c.currency, "USD")
        self.assertEqual(c.primaryExchange, "NYSE")

    def test_overrides_and_defaults(self):
        c = make_stock_contract({"symbol": "OZ"}, primary_exchange="NASDAQ")
        self.assertEqual(c.symbol, "OZ")
        self.assertEqual(c.secType, "STK")
        self.assertEqual(c.currency, "USD")
        self.assertEqual(c.exchange, "SMART")
        self.assertEqual(c.primaryExchange, "NASDAQ")


class TestLoadMergedConfig(unittest.TestCase):
    def test_load_merged_config_merges_cli_and_validates(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"host": "127.0.0.1", "port": 7496, "symbol": "FDX"}, f)
            path = f.name
        try:
            args = argparse.Namespace(config=path, port=7497, symbol=None)
            config = load_merged_config(args, required=["host", "port", "symbol"])
            self.assertEqual(config["host"], "127.0.0.1")
            self.assertEqual(config["port"], 7497)
            self.assertEqual(config["symbol"], "FDX")
        finally:
            Path(path).unlink()

    def test_load_merged_config_raises_on_missing_required(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"host": "127.0.0.1"}, f)
            path = f.name
        try:
            args = argparse.Namespace(config=path)
            with self.assertRaises(ValueError):
                load_merged_config(args, required=["host", "port"])
        finally:
            Path(path).unlink()


class TestArgparseHelpers(unittest.TestCase):
    def test_default_config_path(self):
        p = Path(tempfile.gettempdir()) / "bot_script.py"
        self.assertTrue(default_config_path(p).endswith("bot_script.json"))

    def test_add_config_argument_default(self):
        parser = argparse.ArgumentParser()
        add_config_argument(parser, "/foo/peg_primary.py")
        args = parser.parse_args([])
        self.assertEqual(args.config, "/foo/peg_primary.json")

    def test_add_ib_connection_arguments(self):
        parser = argparse.ArgumentParser()
        add_ib_connection_arguments(parser, include_client_id=True, include_account=True)
        args = parser.parse_args(
            [
                "--host",
                "localhost",
                "--port",
                "7497",
                "--client_id",
                "2",
                "--account",
                "DU123",
                "--symbol",
                "OZ",
            ]
        )
        self.assertEqual(args.host, "localhost")
        self.assertEqual(args.client_id, 2)
        self.assertEqual(args.account, "DU123")

    def test_add_logging_and_session_hours(self):
        parser = argparse.ArgumentParser()
        add_logging_arguments(parser)
        add_session_hours_arguments(parser)
        args = parser.parse_args(
            [
                "--log_dir",
                "logs",
                "--console",
                "--market_timezone",
                "America/New_York",
                "--market_open_hour",
                "9",
                "--market_open_minute",
                "30",
                "--market_close_hour",
                "16",
            ]
        )
        self.assertEqual(args.log_dir, "logs")
        self.assertTrue(args.console)
        self.assertEqual(args.market_timezone, "America/New_York")
        self.assertEqual(args.market_close_hour, 16)


class TestIbErrorFiltering(unittest.TestCase):
    _CFG = {
        "ignored_error_codes": [2104, 2106],
        "ignore_error_substrings": ["HMDS", "data farm connection is broken"],
    }

    def test_should_suppress_ignored_code(self):
        self.assertTrue(
            should_suppress_ib_error(self._CFG, 2104, "Market data farm connected")
        )

    def test_should_suppress_substring_in_message(self):
        self.assertTrue(
            should_suppress_ib_error(self._CFG, 10000, "Error: HMDS connection issue")
        )

    def test_should_suppress_data_farm_in_reject_json(self):
        self.assertTrue(
            should_suppress_ib_error(
                {},
                500,
                "other",
                advanced_order_reject='{"reason":"data farm offline"}',
            )
        )

    def test_should_not_suppress_critical(self):
        self.assertFalse(should_suppress_ib_error(self._CFG, 500, "Critical error"))

    def test_log_ib_error_warning(self):
        logger = MagicMock()
        log_ib_error(
            logger,
            self._CFG,
            req_id=1,
            error_time="t",
            error_code=1999,
            error_string="oops",
        )
        logger.warning.assert_called_once()
        logger.info.assert_not_called()

    def test_log_ib_error_status_info(self):
        logger = MagicMock()
        log_ib_error(
            logger,
            self._CFG,
            req_id=0,
            error_time="",
            error_code=2158,
            error_string="Farm OK",
        )
        logger.info.assert_called_once()
        logger.warning.assert_not_called()

    def test_ib_error_is_status_info(self):
        self.assertTrue(ib_error_is_status_info(2104))
        self.assertFalse(ib_error_is_status_info(500))


class TestDisconnectCleanly(unittest.TestCase):
    def test_cancels_mkt_data_sleeps_and_disconnects(self):
        client = MagicMock()
        client.isConnected.return_value = True
        client.serverVersion.return_value = 157
        logger = MagicMock()

        with patch("ibkr_app_support.time.sleep") as sleep:
            disconnect_cleanly(
                client,
                logger=logger,
                market_data_req_ids=[1001, 1002],
                settle_seconds=0.5,
            )

        client.cancelMktData.assert_any_call(1001)
        client.cancelMktData.assert_any_call(1002)
        sleep.assert_called_once_with(0.5)
        client.disconnect.assert_called_once()
        logger.info.assert_called_once_with("Disconnected cleanly.")

    def test_skips_mkt_data_when_not_connected(self):
        client = MagicMock()
        client.isConnected.return_value = False

        with patch("ibkr_app_support.time.sleep"):
            disconnect_cleanly(client, market_data_req_ids=[1])

        client.cancelMktData.assert_not_called()
        client.disconnect.assert_called_once()


class TestSafeCancelOrder(unittest.TestCase):
    def test_skips_when_disconnected(self):
        client = MagicMock()
        client.isConnected.return_value = False
        safe_cancel_order(client, 1)
        client.cancelOrder.assert_not_called()

    def test_skips_when_server_version_unset(self):
        client = MagicMock()
        client.isConnected.return_value = True
        client.serverVersion.return_value = None
        safe_cancel_order(client, 1)
        client.cancelOrder.assert_not_called()

    def test_legacy_empty_string_fallback(self):
        client = MagicMock()
        client.isConnected.return_value = True
        client.serverVersion.return_value = 157
        client.cancelOrder = MagicMock(side_effect=[TypeError(), TypeError(), None])
        safe_cancel_order(client, 99)
        client.cancelOrder.assert_has_calls([call(99), call(99, "")])


class TestRunBot(unittest.TestCase):
    def test_run_bot_success_runs_main_loop_and_stops(self):
        app = MagicMock()
        app.logger = MagicMock()
        app.isConnected.return_value = True
        app.api_ready = True
        ran = {"main": False}

        def main_loop():
            ran["main"] = True
            app.shutdown_flag = True

        config = {"host": "127.0.0.1", "port": 7497, "client_id": 1}
        code = run_bot(
            app,
            config,
            is_ready=lambda: True,
            main_loop=main_loop,
            connect_timeout_seconds=1.0,
        )
        self.assertEqual(code, 0)
        self.assertTrue(ran["main"])
        app.connect.assert_called_once_with("127.0.0.1", 7497, 1)
        app.stop.assert_called()

    def test_run_bot_returns_one_on_ready_timeout(self):
        app = MagicMock()
        app.logger = MagicMock()
        app.isConnected.return_value = False
        config = {"host": "127.0.0.1", "port": 7497, "client_id": 2}

        code = run_bot(
            app,
            config,
            is_ready=lambda: False,
            main_loop=lambda: None,
            connect_timeout_seconds=0.15,
        )
        self.assertEqual(code, 1)
        app.disconnect.assert_called_once()
        app.stop.assert_not_called()

    def test_run_bot_starts_extra_daemon_threads(self):
        app = MagicMock()
        app.logger = MagicMock()
        app.isConnected.return_value = True
        started = threading.Event()

        def extra():
            started.set()

        config = {"host": "127.0.0.1", "port": 7497}
        with patch("ibkr_app_support.threading.Thread") as thread_cls:
            thread_cls.return_value.start = MagicMock()
            run_bot(
                app,
                config,
                is_ready=lambda: True,
                main_loop=lambda: setattr(app, "shutdown_flag", True),
                extra_daemon_threads=[("Watchdog", extra)],
                connect_timeout_seconds=1.0,
            )
            self.assertGreaterEqual(thread_cls.call_count, 2)

    def test_idle_until_shutdown_exits_on_flag(self):
        app = MagicMock()
        app.shutdown_flag = False
        app.isConnected.return_value = True

        def set_flag():
            app.shutdown_flag = True

        timer = threading.Timer(0.05, set_flag)
        timer.start()
        self.addCleanup(timer.cancel)
        idle_until_shutdown(app, poll_seconds=0.02)

    def test_idle_until_shutdown_exits_on_disconnect(self):
        app = MagicMock()
        app.shutdown_flag = False
        app.isConnected.return_value = False
        idle_until_shutdown(app, poll_seconds=0.02)


class TestIbClientOwnership(unittest.TestCase):
    """Helpers for ignoring other API clients' orders and executions."""

    def test_ib_client_id_from_config(self):
        self.assertEqual(ib_client_id_from_config({"client_id": 7}), 7)
        self.assertEqual(ib_client_id_from_config({"client_id": "9"}), 9)
        self.assertEqual(ib_client_id_from_config({}), 1)
        self.assertEqual(ib_client_id_from_config({"client_id": None}), 1)

    def test_open_order_without_client_id_is_accepted(self):
        order = types.SimpleNamespace()
        self.assertTrue(open_order_belongs_to_client(order, 1))

    def test_open_order_filters_by_client_id(self):
        o = types.SimpleNamespace(clientId=3)
        self.assertTrue(open_order_belongs_to_client(o, 3))
        self.assertFalse(open_order_belongs_to_client(o, 4))

    def test_execution_without_client_id_is_accepted(self):
        ex = types.SimpleNamespace()
        self.assertTrue(execution_belongs_to_client(ex, 1))

    def test_execution_filters_by_client_id(self):
        ex = types.SimpleNamespace(clientId=901)
        self.assertTrue(execution_belongs_to_client(ex, 901))
        self.assertFalse(execution_belongs_to_client(ex, 1))

    def test_order_status_matches_client_id_strictly(self):
        self.assertTrue(order_status_clients_match(901, 901))
        self.assertFalse(order_status_clients_match(0, 901))
        self.assertFalse(order_status_clients_match("x", 1))


class TestWaitForIbReady(unittest.TestCase):
    def test_returns_true_when_ready_immediately(self):
        self.assertTrue(wait_for_ib_ready(lambda: True, timeout_seconds=1.0))

    def test_returns_false_on_timeout(self):
        self.assertFalse(
            wait_for_ib_ready(lambda: False, timeout_seconds=0.2, poll_seconds=0.05)
        )

    def test_returns_false_if_connection_drops_after_connect(self):
        state = {"up": True}

        def connected():
            return state["up"]

        def ready():
            return False

        def drop():
            state["up"] = False

        import threading

        timer = threading.Timer(0.05, drop)
        timer.start()
        self.addCleanup(timer.cancel)
        self.assertFalse(
            wait_for_ib_ready(
                ready,
                is_connected=connected,
                timeout_seconds=1.0,
                poll_seconds=0.02,
            )
        )


if __name__ == "__main__":
    unittest.main()
