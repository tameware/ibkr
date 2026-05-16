# Usage: python -m unittest discover -s tests -t . -v

import sys
import unittest
from unittest.mock import MagicMock, patch

# Mock IB API before importing ibkr_bot_base
class MockEWrapper:
    pass


class MockEClient:
    def __init__(self, wrapper):
        pass


class MockContract:
    def __init__(self):
        self.symbol = ""
        self.secType = ""
        self.currency = ""
        self.exchange = ""
        self.primaryExchange = ""


mock_ibapi = MagicMock()
mock_ibapi.wrapper.EWrapper = MockEWrapper
mock_ibapi.client.EClient = MockEClient
mock_ibapi.contract.Contract = MockContract

sys.modules["ibapi"] = mock_ibapi
sys.modules["ibapi.client"] = mock_ibapi.client
sys.modules["ibapi.wrapper"] = mock_ibapi.wrapper
sys.modules["ibapi.contract"] = mock_ibapi.contract

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
