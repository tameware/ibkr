# Usage: python -m unittest discover -s tests -t . -v

import unittest
from unittest.mock import MagicMock, patch

from tests.ibapi_mocks import install_ibapi_mocks

install_ibapi_mocks(ticktype=False)

from ibkr_bot_base import IbkrBotApp


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
