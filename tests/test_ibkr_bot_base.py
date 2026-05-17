# Usage: python -m unittest discover -s tests -t . -v

import unittest
from unittest.mock import MagicMock, patch

from tests.ibapi_mocks import install_ibapi_mocks

install_ibapi_mocks(ticktype=False)

from ibkr_bot_base import IbkrBotApp, SnapshotResyncMixin


class _StubBot(IbkrBotApp):
    def __init__(self, config):
        super().__init__(config, logger_name="stub", default_log_file="stub.log")
        self.startup_called = False
        self.shutdown_quotes_called = False

    def startup(self) -> None:
        self.startup_called = True

    def shutdown_quotes(self) -> None:
        self.shutdown_quotes_called = True

    def market_data_req_ids(self):
        return (42,)


_RESYNC_CONFIG = {
    "symbol": "X",
    "sec_type": "STK",
    "currency": "USD",
    "exchange": "SMART",
}


class _ResyncStub(SnapshotResyncMixin, IbkrBotApp):
    def __init__(self, config=None):
        super().__init__(
            config if config is not None else _RESYNC_CONFIG,
            logger_name="resync_stub",
            default_log_file="resync.log",
        )
        self.open_symbol_buys = 0
        self.open_symbol_sells = 0
        self._init_snapshot_resync(self.config)
        self.sync_orders = MagicMock()
        self.reqPositions = MagicMock()
        self.reqOpenOrders = MagicMock()
        self.isConnected = MagicMock(return_value=True)

    def startup(self) -> None:
        pass

    def shutdown_quotes(self) -> None:
        pass


class TestSnapshotResyncMixin(unittest.TestCase):
    def setUp(self):
        self._log_patcher = patch(
            "ibkr_bot_base.build_logger", return_value=MagicMock()
        )
        self._log_patcher.start()
        self.addCleanup(self._log_patcher.stop)

    def test_trigger_resync_debounces(self):
        bot = _ResyncStub()
        bot.resync_debounce_seconds = 10.0
        with patch("ibkr_bot_base.time.monotonic", side_effect=[100.0, 101.0, 120.0]):
            bot.trigger_resync()
            self.assertTrue(bot.sync_requested)
            bot.reqPositions.assert_called_once()
            bot.reqOpenOrders.assert_called_once()
            bot.reqPositions.reset_mock()
            bot.reqOpenOrders.reset_mock()
            bot.trigger_resync()
            bot.reqPositions.assert_not_called()
            bot.sync_requested = False
            bot.trigger_resync(force=True)
            bot.reqPositions.assert_called_once()

    def test_maybe_sync_orders_after_snapshots(self):
        bot = _ResyncStub()
        bot.sync_requested = True
        bot.position_snapshot_complete = True
        bot.open_orders_snapshot_complete = True
        bot.maybe_sync_orders()
        bot.sync_orders.assert_called_once()
        self.assertFalse(bot.sync_requested)


class TestIbkrBotApp(unittest.TestCase):
    def setUp(self):
        self._log_patcher = patch(
            "ibkr_bot_base.build_logger", return_value=MagicMock()
        )
        self._log_patcher.start()
        self.addCleanup(self._log_patcher.stop)

    def test_next_valid_id_runs_startup(self):
        bot = _StubBot(
            {"symbol": "X", "sec_type": "STK", "currency": "USD", "exchange": "SMART"}
        )
        bot.nextValidId(100)
        self.assertTrue(bot.api_ready)
        self.assertEqual(bot.next_order_id, 100)
        self.assertTrue(bot.startup_called)

    def test_connection_closed_sets_shutdown(self):
        bot = _StubBot(
            {"symbol": "X", "sec_type": "STK", "currency": "USD", "exchange": "SMART"}
        )
        bot._api_ready = True
        bot.connectionClosed()
        self.assertFalse(bot.api_ready)
        self.assertTrue(bot.shutdown_flag)

    def test_stop_disconnects(self):
        bot = _StubBot(
            {"symbol": "X", "sec_type": "STK", "currency": "USD", "exchange": "SMART"}
        )
        with patch("ibkr_bot_base.disconnect_cleanly") as dc:
            bot.stop()
            dc.assert_called_once()
        self.assertTrue(bot.shutdown_quotes_called)
        self.assertTrue(bot._stop_called)


if __name__ == "__main__":
    unittest.main()
