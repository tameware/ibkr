# Usage: python -m unittest test_ibkr_app_support.py -v

import datetime
import unittest
from unittest.mock import MagicMock, call, patch
from zoneinfo import ZoneInfo

import argparse
import json
import tempfile
import threading
from pathlib import Path

from ibkr_app_support import (
    add_config_argument,
    add_ib_connection_arguments,
    add_logging_arguments,
    add_session_hours_arguments,
    cli_to_config,
    default_config_path,
    ib_error_is_status_info,
    idle_until_shutdown,
    load_config_file,
    load_merged_config,
    log_ib_error,
    log_session_transition,
    merge_config,
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
