# Usage: python -m unittest discover -s tests -t . -v

import unittest
from unittest.mock import MagicMock, patch

from tests.ibapi_mocks import install_ibapi_mocks

install_ibapi_mocks(ticktype=False)

from ibkr_bot_base import (
    CONTRACT_DETAILS_REQ_ID,
    HIST_REQ_ID,
    ContractResolutionMixin,
    IbkrBotApp,
    OpenPriceBootstrapMixin,
    SnapshotResyncMixin,
)


class _StubBot(IbkrBotApp):
    def __init__(self, config):
        super().__init__(config, logger_name="stub", default_log_file="stub.log")
        self.startup_called = False
        self.shutdown_quotes_called = False
        self.subscribe_market_data = MagicMock()

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


class _OpenPriceStub(OpenPriceBootstrapMixin, IbkrBotApp):
    def __init__(self):
        super().__init__(
            _RESYNC_CONFIG,
            logger_name="open_price_stub",
            default_log_file="open_price.log",
        )
        self._init_open_price_bootstrap()
        self.ref_price = None
        self.reqHistoricalData = MagicMock()

    def startup(self) -> None:
        pass

    def shutdown_quotes(self) -> None:
        pass


class TestOpenPriceBootstrapMixin(unittest.TestCase):
    def setUp(self):
        self._log_patcher = patch(
            "ibkr_bot_base.build_logger", return_value=MagicMock()
        )
        self._log_patcher.start()
        self.addCleanup(self._log_patcher.stop)

    def test_historical_data_end_sets_open_only(self):
        bot = _OpenPriceStub()
        bar = MagicMock(open=150.0, close=151.0)
        bot._bars = [bar]
        bot.historicalDataEnd(HIST_REQ_ID, "", "")
        self.assertEqual(bot.open_price, 150.0)
        self.assertIsNone(bot.ref_price)

    def test_historical_data_end_falls_back_to_close(self):
        bot = _OpenPriceStub()
        bar = MagicMock(open=0, close=145.5)
        bot._bars = [bar]
        bot.historicalDataEnd(HIST_REQ_ID, "", "")
        self.assertEqual(bot.open_price, 145.5)

    def test_historical_data_ignores_wrong_req_id(self):
        bot = _OpenPriceStub()
        bar = MagicMock(open=150.0)
        bot.historicalData(9999, bar)
        self.assertEqual(bot._bars, [])


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


class _MktDataStub(ContractResolutionMixin, IbkrBotApp):
    def __init__(self, config):
        super().__init__(config, logger_name="mkt_stub", default_log_file="mkt.log")
        self._init_contract_resolution(config)
        self.reqContractDetails = MagicMock()
        self.reqMktData = MagicMock()
        self.reqMarketDataType = MagicMock()
        self.cancelMktData = MagicMock()
        self.isConnected = MagicMock(return_value=True)
        self.serverVersion = MagicMock(return_value=157)
        self.shutdown_flag = False

    def startup(self) -> None:
        pass

    def shutdown_quotes(self) -> None:
        pass

    def market_data_req_ids(self):
        return (3001,)


class TestContractResolutionMixin(unittest.TestCase):
    def setUp(self):
        self._log_patcher = patch(
            "ibkr_bot_base.build_logger", return_value=MagicMock()
        )
        self._log_patcher.start()
        self.addCleanup(self._log_patcher.stop)

    def test_contract_details_end_subscribes_after_resolve(self):
        bot = _MktDataStub(
            {"symbol": "OZ", "sec_type": "STK", "currency": "USD", "exchange": "SMART"}
        )
        details = MagicMock()
        details.contract.conId = 516097295
        details.contract.symbol = "OZ"
        details.contract.secType = "STK"
        details.contract.currency = "USD"
        details.contract.localSymbol = "OZ"
        details.contract.exchange = "SMART"
        details.contract.primaryExchange = "AMEX"
        bot.contractDetails(CONTRACT_DETAILS_REQ_ID, details)
        bot.contractDetailsEnd(CONTRACT_DETAILS_REQ_ID)
        bot.reqMktData.assert_called_once()
        self.assertEqual(bot.contract.conId, 516097295)
        mkt_contract = bot.reqMktData.call_args[0][1]
        self.assertEqual(mkt_contract.exchange, "AMEX")

    def test_market_data_contract_uses_primary_exchange(self):
        bot = _MktDataStub(
            {"symbol": "OZ", "sec_type": "STK", "currency": "USD", "exchange": "SMART"}
        )
        from ibapi.contract import Contract

        bot.contract = Contract()
        bot.contract.conId = 1
        bot.contract.symbol = "OZ"
        bot.contract.secType = "STK"
        bot.contract.currency = "USD"
        bot.contract.exchange = "SMART"
        bot.contract.primaryExchange = "AMEX"
        bot._market_data_contract = bot.contract
        routed = bot.market_data_contract()
        self.assertEqual(routed.exchange, "AMEX")

    def test_maybe_watchdog_recover_resubscribes_and_polls_snapshot(self):
        bot = _MktDataStub(
            {
                "symbol": "OZ",
                "sec_type": "STK",
                "currency": "USD",
                "exchange": "SMART",
                "market_data_stall_seconds": 0.0,
                "market_data_watchdog_resubscribe_seconds": 30.0,
                "market_data_snapshot_poll_seconds": 30.0,
            }
        )
        from ibapi.contract import Contract

        bot.contract = Contract()
        bot.contract.conId = 1
        bot.contract.symbol = "OZ"
        bot.contract.secType = "STK"
        bot.contract.currency = "USD"
        bot.contract.exchange = "SMART"
        bot.contract.primaryExchange = "AMEX"
        bot._market_data_contract = bot.contract
        bot._market_data_subscribed = True
        bot._market_data_subscribed_ts = 0.0
        bot._last_watchdog_resubscribe_ts = 0.0
        bot._last_snapshot_poll_ts = 0.0
        with patch("ibkr_bot_base.time.monotonic", return_value=100.0):
            bot.maybe_watchdog_recover_market_data(nbbo_ok=False)
        stream_calls = [
            c for c in bot.reqMktData.call_args_list if c[0][3] is False
        ]
        snap_calls = [c for c in bot.reqMktData.call_args_list if c[0][3] is True]
        self.assertEqual(len(stream_calls), 1)
        self.assertEqual(len(snap_calls), 1)
        self.assertEqual(stream_calls[0][0][1].exchange, "SMART")
        snap_cancel = [
            c for c in bot.cancelMktData.call_args_list if int(c[0][0]) == 1002
        ]
        self.assertEqual(snap_cancel, [])

    def test_snapshot_poll_skipped_while_in_flight(self):
        bot = _MktDataStub(
            {
                "symbol": "OZ",
                "sec_type": "STK",
                "currency": "USD",
                "exchange": "SMART",
            }
        )
        bot._market_data_snapshot_in_flight = True
        bot._market_data_snapshot_started_ts = 100.0
        with patch("ibkr_bot_base.time.monotonic", return_value=105.0):
            bot.poll_market_data_snapshot()
        bot.reqMktData.assert_not_called()

    def test_tick_snapshot_end_clears_in_flight(self):
        bot = _MktDataStub(
            {
                "symbol": "OZ",
                "sec_type": "STK",
                "currency": "USD",
                "exchange": "SMART",
                "market_data_snapshot_poll_seconds": 30.0,
            }
        )
        bot._market_data_snapshot_in_flight = True
        bot.tickSnapshotEnd(bot.market_data_snapshot_req_id)
        self.assertFalse(bot._market_data_snapshot_in_flight)

    def test_maybe_watchdog_recover_skips_when_nbbo_ok(self):
        bot = _MktDataStub(
            {
                "symbol": "OZ",
                "sec_type": "STK",
                "currency": "USD",
                "exchange": "SMART",
                "market_data_stall_seconds": 0.0,
            }
        )
        bot._market_data_subscribed = True
        bot._market_data_subscribed_ts = 0.0
        bot.maybe_watchdog_recover_market_data(nbbo_ok=True)
        bot.reqMktData.assert_not_called()


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

    def test_error_1102_resubscribes_market_data(self):
        bot = _StubBot(
            {
                "symbol": "X",
                "sec_type": "STK",
                "currency": "USD",
                "exchange": "SMART",
                "market_data_resubscribe_debounce_seconds": 0.0,
            }
        )
        bot.isConnected = MagicMock(return_value=True)
        bot.error(-1, 0, 1102, "Connectivity restored", "")
        bot.subscribe_market_data.assert_called_once()

    def test_error_1102_debounced(self):
        bot = _StubBot(
            {
                "symbol": "X",
                "sec_type": "STK",
                "currency": "USD",
                "exchange": "SMART",
                "market_data_resubscribe_debounce_seconds": 60.0,
            }
        )
        bot.isConnected = MagicMock(return_value=True)
        with patch("ibkr_bot_base.time.monotonic", side_effect=[100.0, 101.0]):
            bot.error(-1, 0, 1102, "Connectivity restored", "")
            bot.error(-1, 0, 1102, "Connectivity restored", "")
        bot.subscribe_market_data.assert_called_once()


if __name__ == "__main__":
    unittest.main()
