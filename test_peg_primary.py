# Usage: python -m unittest test_peg_primary.py -v

import datetime
import json
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch
from zoneinfo import ZoneInfo

# Mock IB API before importing peg_primary
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
        self.primaryExch = ""


class MockOrder:
    def __init__(self):
        self.action = ""
        self.orderType = ""
        self.totalQuantity = 0
        self.lmtPrice = 0.0
        self.auxPrice = 0.0
        self.exchange = ""
        self.tif = ""
        self.notHeld = False


mock_ibapi = MagicMock()
mock_ibapi.wrapper.EWrapper = MockEWrapper
mock_ibapi.client.EClient = MockEClient
mock_ibapi.contract.Contract = MockContract
mock_ibapi.order.Order = MockOrder

mock_pytz = MagicMock()


class MockTimezone:
    def localize(self, dt):
        return dt


mock_pytz.timezone = Mock(return_value=MockTimezone())

sys.modules["ibapi"] = mock_ibapi
sys.modules["ibapi.client"] = mock_ibapi.client
sys.modules["ibapi.wrapper"] = mock_ibapi.wrapper
sys.modules["ibapi.common"] = MagicMock()
sys.modules["ibapi.contract"] = mock_ibapi.contract
sys.modules["ibapi.order"] = mock_ibapi.order
sys.modules["ibapi.ticktype"] = MagicMock()
sys.modules["pytz"] = mock_pytz

from peg_primary import (
    Trader,
    build_arg_parser,
    cli_to_config,
    load_config_file,
    make_rel_order,
    merge_config,
    require_fields,
)


def _minimal_config(**overrides):
    base = {
        "host": "127.0.0.1",
        "port": 7496,
        "client_id": 1,
        "symbol": "TEST",
        "sec_type": "STK",
        "currency": "USD",
        "exchange": "SMART",
        "primary_exchange": "NYSE",
        "max_pos": 100,
        "loop_seconds": 1.0,
        "buy_delta": 0.05,
        "sell_delta": 0.05,
        "min_spread": 0.05,
        "rel_offset": 0.01,
        "market_timezone": "America/New_York",
        "market_open_hour": 9,
        "market_open_minute": 30,
        "market_close_hour": 16,
        "last_trade_min_size": 1.0,
        "tif": "DAY",
        "price_round_digits": 2,
        "ignored_error_codes": [],
        "ignore_error_substrings": [],
        "resync_debounce_seconds": 0.0,
    }
    base.update(overrides)
    return base


class TestHelpers(unittest.TestCase):
    def test_load_config_file_valid(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"host": "127.0.0.1", "port": 7497}, f)
            f.flush()
            path = f.name
        try:
            self.assertEqual(load_config_file(path), {"host": "127.0.0.1", "port": 7497})
        finally:
            Path(path).unlink()

    def test_load_config_file_invalid(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(["not", "a", "dict"], f)
            f.flush()
            path = f.name
        try:
            with self.assertRaises(ValueError):
                load_config_file(path)
        finally:
            Path(path).unlink()

    def test_cli_to_config(self):
        args = Namespace(
            config="x.json",
            host="localhost",
            port=7497,
            symbol="AAPL",
            none_value=None,
        )
        self.assertEqual(
            cli_to_config(args),
            {"host": "localhost", "port": 7497, "symbol": "AAPL"},
        )

    def test_merge_config(self):
        f = {"host": "127.0.0.1", "port": 7496, "symbol": "MSFT"}
        c = {"port": 7497, "symbol": "AAPL", "extra": "v"}
        m = merge_config(f, c)
        self.assertEqual(m["host"], "127.0.0.1")
        self.assertEqual(m["port"], 7497)
        self.assertEqual(m["symbol"], "AAPL")
        self.assertEqual(m["extra"], "v")

    def test_require_fields(self):
        require_fields({"a": 1, "b": 2}, ["a"])
        with self.assertRaises(ValueError):
            require_fields({"a": 1}, ["b"])

    def test_make_rel_order(self):
        o = make_rel_order(
            action="BUY",
            qty=50,
            limit_price=10.126,
            offset=0.01234,
            exchange="SMART",
            tif="DAY",
            price_round_digits=2,
            aux_round_digits=3,
        )
        self.assertEqual(o.action, "BUY")
        self.assertEqual(o.orderType, "REL")
        self.assertEqual(o.totalQuantity, 50)
        self.assertEqual(o.lmtPrice, 10.13)
        self.assertEqual(o.auxPrice, 0.012)
        self.assertEqual(o.exchange, "SMART")
        self.assertEqual(o.tif, "DAY")
        self.assertTrue(o.notHeld)


class TestAdjustedRelLimits(unittest.TestCase):
    def test_negative_deltas_meet_min_spread(self):
        """peg_mid.json-style negative deltas invert raw gap; still enforce width."""
        cfg = _minimal_config(buy_delta=-0.10, sell_delta=-0.10, min_spread=0.05)
        t = Trader(cfg)
        t.ref_price = 100.0
        buy_l, sell_l = t.adjusted_rel_limits()
        self.assertGreaterEqual(sell_l - buy_l, 0.05 - 1e-9)

    def test_widens_when_raw_gap_below_min(self):
        cfg = _minimal_config(buy_delta=0.02, sell_delta=0.02, min_spread=0.15)
        t = Trader(cfg)
        t.ref_price = 10.0
        buy_l, sell_l = t.adjusted_rel_limits()
        self.assertGreaterEqual(sell_l - buy_l, 0.15 - 1e-9)


class TestTrader(unittest.TestCase):
    def setUp(self):
        self.cfg = _minimal_config()
        self.t = Trader(self.cfg)

    def test_build_rel_order_uses_asymmetric_offsets(self):
        self.cfg["rel_offset_buy"] = 0.02
        self.cfg["rel_offset_sell"] = 0.03
        b = self.t.build_rel_order("BUY", 10, 100.0)
        self.assertEqual(b.auxPrice, 0.02)
        s = self.t.build_rel_order("SELL", 10, 100.0)
        self.assertEqual(s.auxPrice, 0.03)

    def test_sync_orders_flat_places_buy_only(self):
        self.t.ready_for_trading = True
        self.t.ref_price = 50.0
        self.t.nextOrderId = 100
        self.t.position_size = 0
        self.t.open_symbol_buys = 0
        self.t.open_symbol_sells = 0
        self.t.pending_buy = False
        self.t.pending_sell = False
        self.t.placeOrder = Mock()
        self.t.sync_orders()
        self.t.placeOrder.assert_called_once()
        oid, contract, order = self.t.placeOrder.call_args[0]
        self.assertEqual(oid, 100)
        self.assertEqual(order.action, "BUY")
        self.assertEqual(order.orderType, "REL")
        self.assertEqual(order.totalQuantity, 100)
        # buy_limit = ref - buy_delta = 50 - 0.05 = 49.95
        self.assertEqual(order.lmtPrice, 49.95)
        self.assertEqual(self.t.nextOrderId, 101)

    def test_sync_orders_partial_places_buy_and_sell(self):
        self.cfg["max_pos"] = 100
        self.t.ready_for_trading = True
        self.t.ref_price = 50.0
        self.t.nextOrderId = 200
        self.t.position_size = 75
        self.t.open_symbol_buys = 0
        self.t.open_symbol_sells = 0
        self.t.pending_buy = False
        self.t.pending_sell = False
        self.t.placeOrder = Mock()
        self.t.sync_orders()
        self.assertEqual(self.t.placeOrder.call_count, 2)
        calls = self.t.placeOrder.call_args_list
        buy_order = calls[0][0][2]
        sell_order = calls[1][0][2]
        self.assertEqual(buy_order.action, "BUY")
        self.assertEqual(buy_order.totalQuantity, 25)
        self.assertEqual(buy_order.lmtPrice, 49.95)
        self.assertEqual(sell_order.action, "SELL")
        self.assertEqual(sell_order.totalQuantity, 75)
        self.assertEqual(sell_order.lmtPrice, 50.05)
        self.assertEqual(self.t.nextOrderId, 202)

    def test_sync_orders_at_max_places_sell_only(self):
        self.cfg["max_pos"] = 100
        self.t.ready_for_trading = True
        self.t.ref_price = 50.0
        self.t.nextOrderId = 300
        self.t.position_size = 100
        self.t.open_symbol_buys = 0
        self.t.open_symbol_sells = 0
        self.t.pending_buy = False
        self.t.pending_sell = False
        self.t.placeOrder = Mock()
        self.t.sync_orders()
        self.t.placeOrder.assert_called_once()
        _, _, order = self.t.placeOrder.call_args[0]
        self.assertEqual(order.action, "SELL")
        self.assertEqual(order.totalQuantity, 100)
        self.assertEqual(order.lmtPrice, 50.05)

    def test_sync_orders_at_max_cancels_buy_first(self):
        self.cfg["max_pos"] = 100
        self.t.ready_for_trading = True
        self.t.ref_price = 50.0
        self.t.nextOrderId = 400
        self.t.position_size = 100
        self.t.buy_order_id = 88
        self.t.safe_cancel_order = Mock()
        self.t.placeOrder = Mock()
        self.t.sync_orders()
        self.t.safe_cancel_order.assert_called_once_with(88)
        self.t.placeOrder.assert_not_called()

    def test_sync_orders_cancels_opposite_before_buy(self):
        self.t.ready_for_trading = True
        self.t.ref_price = 40.0
        self.t.nextOrderId = 1
        self.t.position_size = 0
        self.t.sell_order_id = 55
        self.t.safe_cancel_order = Mock()
        self.t.placeOrder = Mock()
        self.t.sync_orders()
        self.t.safe_cancel_order.assert_called_once_with(55)
        self.t.placeOrder.assert_not_called()

    def test_trigger_resync_debounce(self):
        self.t.isConnected = Mock(return_value=True)
        self.t.request_positions_snapshot = Mock()
        self.t.request_open_orders_snapshot = Mock()
        self.t.resync_debounce_seconds = 10.0
        with patch("peg_primary.time.monotonic", side_effect=[100.0, 101.0, 120.0]):
            self.t.last_resync_request_ts = 0.0
            self.t.trigger_resync()
            self.assertTrue(self.t.sync_requested)
            c1 = self.t.request_positions_snapshot.call_count
            self.t.trigger_resync()
            self.assertEqual(self.t.request_positions_snapshot.call_count, c1)
            self.t.sync_requested = False
            self.t.trigger_resync()
            self.assertTrue(self.t.sync_requested)
            self.assertEqual(self.t.request_positions_snapshot.call_count, c1 + 1)

    def test_us_regular_hours(self):
        ny = ZoneInfo("America/New_York")
        during = datetime.datetime(2026, 5, 11, 10, 30, 0, tzinfo=ny)
        pre = datetime.datetime(2026, 5, 11, 9, 29, 0, tzinfo=ny)
        with patch("peg_primary.pytz.timezone", ZoneInfo):
            with patch("datetime.datetime") as mock_datetime_class:
                mock_datetime_class.now.return_value = during
                self.assertTrue(self.t.us_regular_hours())
            with patch("datetime.datetime") as mock_datetime_class:
                mock_datetime_class.now.return_value = pre
                self.assertFalse(self.t.us_regular_hours())

    def test_error_filters_ignored_code(self):
        self.cfg["ignored_error_codes"] = [2104]
        with patch("peg_primary.tprint") as tp:
            self.t.error(0, "", 2104, "Market data farm connected", "")
        tp.assert_not_called()

    def test_error_prints_other(self):
        with patch("peg_primary.tprint") as tp:
            self.t.error(1, "t", 1999, "oops", "")
        tp.assert_called_once()


class TestBuildArgParser(unittest.TestCase):
    def test_defaults(self):
        p = build_arg_parser()
        args = p.parse_args([])
        self.assertTrue(str(args.config).endswith("peg_primary.json"))


if __name__ == "__main__":
    unittest.main()
