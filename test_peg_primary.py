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
        "offset_pct": 0.25,
        "rel_offset": 0.01,
        "market_timezone": "America/New_York",
        "market_open_hour": 9,
        "market_open_minute": 30,
        "market_close_hour": 16,
        "last_trade_min_size": 1.0,
        "last_trade_safety_pct": 0.01,
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
        self.assertFalse(getattr(o, "notHeld", False))


class TestAdjustedRelLimits(unittest.TestCase):
    def test_offset_pct_from_nbbo_only(self):
        """With NBBO, limits come only from offset_pct (fraction of spread)."""
        cfg = _minimal_config(offset_pct=0.25)
        t = Trader(cfg)
        t.ref_price = 999.0
        t._bid = 99.9
        t._ask = 100.1
        buy_l, sell_l = t.adjusted_rel_limits()
        self.assertEqual(buy_l, 99.95)
        self.assertEqual(sell_l, 100.05)
        self.assertGreater(sell_l, buy_l)

    def test_last_trade_safety_caps_buy(self):
        """Buy ceiling cannot exceed last_trade * (1 + safety_pct)."""
        cfg = _minimal_config(offset_pct=0.4, last_trade_safety_pct=0.01)
        t = Trader(cfg)
        t._bid = 100.0
        t._ask = 110.0
        t.last_trade_price = 100.0
        buy_l, sell_l = t.adjusted_rel_limits()
        self.assertEqual(buy_l, 101.0)
        self.assertEqual(sell_l, 106.0)
        self.assertGreater(sell_l, buy_l)

    def test_last_trade_safety_raises_sell_floor(self):
        """Sell floor cannot go below last_trade * (1 - safety_pct)."""
        cfg = _minimal_config(offset_pct=0.1, last_trade_safety_pct=0.01)
        t = Trader(cfg)
        t._bid = 99.0
        t._ask = 101.0
        t.last_trade_price = 102.0
        buy_l, sell_l = t.adjusted_rel_limits()
        self.assertEqual(buy_l, 99.2)
        self.assertEqual(sell_l, 100.98)
        self.assertGreater(sell_l, buy_l)

    def test_offset_pct_ge_half_clamped_for_band(self):
        """offset_pct >= 0.5 is clamped; rounding may tie — profit nudge applies."""
        cfg = _minimal_config(offset_pct=0.99)
        t = Trader(cfg)
        t.ref_price = 100.0
        t._bid = 100.0
        t._ask = 101.0
        buy_l, sell_l = t.adjusted_rel_limits()
        self.assertGreater(sell_l, buy_l)


class TestTrader(unittest.TestCase):
    def setUp(self):
        self.cfg = _minimal_config()
        self.t = Trader(self.cfg)
        # NBBO so sync_orders / adjusted_rel_limits can run (matches offset_pct=0.25 → 49.95 / 50.05)
        self.t._bid = 49.90
        self.t._ask = 50.10
        self.t.last_trade_price = 50.0

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
        # bid=49.90 ask=50.10 spread 0.20 * offset_pct 0.25 = 0.05 → buy 49.95 / sell 50.05
        self.assertEqual(order.lmtPrice, 49.95)
        self.assertEqual(self.t.nextOrderId, 101)

    def test_sync_orders_skips_when_no_nbbo(self):
        self.t.ready_for_trading = True
        self.t.ref_price = 50.0
        self.t.nextOrderId = 100
        self.t._bid = None
        self.t._ask = None
        self.t.position_size = 0
        self.t.open_symbol_buys = 0
        self.t.open_symbol_sells = 0
        self.t.pending_buy = False
        self.t.pending_sell = False
        self.t.placeOrder = Mock()
        self.t.sync_orders()
        self.t.placeOrder.assert_not_called()

    def test_sync_updates_buy_lmt_when_cached_stale(self):
        """If working qty matches target but lmtPrice differs from NBBO-derived limit, amend."""
        self.t.ready_for_trading = True
        self.t.nextOrderId = 500
        self.t.position_size = 0
        self.t.buy_order_id = 77
        self.t.buy_order_qty = 100
        self.t.remaining_by_order[77] = 100
        self.t.order_working_lmt[77] = 48.0
        self.t.order_working_aux[77] = 0.01
        self.t.open_symbol_buys = 1
        self.t.placeOrder = Mock()
        self.t.sync_orders()
        self.t.placeOrder.assert_called_once()
        oid, _, order = self.t.placeOrder.call_args[0]
        self.assertEqual(oid, 77)
        self.assertEqual(order.lmtPrice, 49.95)

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

    def test_default_min_order_size_is_ten(self):
        cfg = _minimal_config()
        cfg.pop("min_order_size", None)
        t = Trader(cfg)
        self.assertEqual(t.min_order_size, 10)

    def test_sync_orders_partial_skips_buy_below_min_order_size(self):
        self.cfg["max_pos"] = 105
        self.cfg["min_order_size"] = 10
        self.t.ready_for_trading = True
        self.t.ref_price = 50.0
        self.t.nextOrderId = 200
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

    def test_sync_orders_partial_skips_sell_below_min_order_size(self):
        self.cfg["max_pos"] = 115
        self.cfg["min_order_size"] = 10
        self.t.ready_for_trading = True
        self.t.ref_price = 50.0
        self.t.nextOrderId = 200
        self.t.position_size = 8
        self.t.open_symbol_buys = 0
        self.t.open_symbol_sells = 0
        self.t.pending_buy = False
        self.t.pending_sell = False
        self.t.placeOrder = Mock()
        self.t.sync_orders()
        self.t.placeOrder.assert_called_once()
        _, _, order = self.t.placeOrder.call_args[0]
        self.assertEqual(order.action, "BUY")
        self.assertEqual(order.totalQuantity, 107)

    def test_sync_orders_flat_skips_buy_below_min_order_size(self):
        self.cfg["max_pos"] = 5
        self.cfg["min_order_size"] = 10
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
        self.t.placeOrder.assert_not_called()
        self.assertEqual(self.t.nextOrderId, 100)

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


class TestExecutionLogging(unittest.TestCase):
    def setUp(self):
        self.cfg = _minimal_config()
        self.t = Trader(self.cfg)
        self.t.trigger_resync = Mock()

    def _order_status(self, *, orderId, status, filled, remaining,
                      avgFillPrice=0.0, lastFillPrice=0.0):
        self.t.orderStatus(
            orderId, status, filled, remaining, avgFillPrice,
            0, 0, lastFillPrice, 0, "", 0.0,
        )

    def test_order_status_logs_filled_terminal(self):
        self.t.buy_order_id = 42
        with patch("peg_primary.tprint") as tp:
            self._order_status(orderId=42, status="Filled", filled=100,
                               remaining=0, avgFillPrice=49.95)
        msgs = [c.args[0] for c in tp.call_args_list]
        self.assertTrue(any("BUY 42 Filled" in m and "filled=100" in m
                            and "avgFillPrice=49.95" in m for m in msgs))
        self.assertIsNone(self.t.buy_order_id)
        self.t.trigger_resync.assert_called()

    def test_order_status_logs_cancelled_terminal(self):
        self.t.sell_order_id = 7
        with patch("peg_primary.tprint") as tp:
            self._order_status(orderId=7, status="Cancelled", filled=0,
                               remaining=0)
        msgs = [c.args[0] for c in tp.call_args_list]
        self.assertTrue(any("SELL 7 Cancelled" in m for m in msgs))
        self.assertIsNone(self.t.sell_order_id)

    def test_order_status_partial_fill_via_submitted(self):
        """IB sends partial fills as Submitted with cumulative filled > 0
        and remaining > 0. The script should log a partial-fill line."""
        self.t.buy_order_id = 99
        with patch("peg_primary.tprint") as tp:
            self._order_status(orderId=99, status="Submitted", filled=10,
                               remaining=90, avgFillPrice=50.0,
                               lastFillPrice=50.0)
        msgs = [c.args[0] for c in tp.call_args_list]
        self.assertTrue(any("BUY 99 partial fill" in m and "filled=10" in m
                            and "remaining=90" in m for m in msgs))
        self.assertEqual(self.t.filled_by_order[99], 10.0)
        self.t.trigger_resync.assert_called()

    def test_order_status_partial_fill_logged_only_on_delta(self):
        """Repeated Submitted callbacks with the same cumulative filled
        should not relog the partial fill."""
        self.t.buy_order_id = 99
        with patch("peg_primary.tprint") as tp:
            self._order_status(orderId=99, status="Submitted", filled=10,
                               remaining=90, avgFillPrice=50.0)
            self._order_status(orderId=99, status="Submitted", filled=10,
                               remaining=90, avgFillPrice=50.0)
        partial_msgs = [c.args[0] for c in tp.call_args_list
                        if "partial fill" in c.args[0]]
        self.assertEqual(len(partial_msgs), 1)

        with patch("peg_primary.tprint") as tp2:
            self._order_status(orderId=99, status="Submitted", filled=25,
                               remaining=75, avgFillPrice=50.1)
        partial_msgs = [c.args[0] for c in tp2.call_args_list
                        if "partial fill" in c.args[0]]
        self.assertEqual(len(partial_msgs), 1)
        self.assertIn("filled=25", partial_msgs[0])

    def test_order_status_no_partial_log_when_filled_zero(self):
        self.t.buy_order_id = 99
        with patch("peg_primary.tprint") as tp:
            self._order_status(orderId=99, status="Submitted", filled=0,
                               remaining=100)
        self.assertFalse(any("partial fill" in c.args[0]
                             for c in tp.call_args_list))

    def test_order_status_clears_filled_tracking_on_terminal(self):
        self.t.buy_order_id = 99
        self._order_status(orderId=99, status="Submitted", filled=10,
                           remaining=90)
        self.assertIn(99, self.t.filled_by_order)
        self._order_status(orderId=99, status="Filled", filled=100,
                           remaining=0, avgFillPrice=50.0)
        self.assertNotIn(99, self.t.filled_by_order)

    def _execution(self, **kwargs):
        e = MagicMock()
        defaults = {
            "orderId": 1, "execId": "ex-1", "side": "BOT", "shares": 25,
            "price": 49.97, "exchange": "ARCA", "cumQty": 25,
            "avgPrice": 49.97, "time": "20260513 17:00:00",
        }
        defaults.update(kwargs)
        for k, v in defaults.items():
            setattr(e, k, v)
        return e

    def _contract(self, symbol="TEST", sec_type="STK"):
        c = MockContract()
        c.symbol = symbol
        c.secType = sec_type
        return c

    def test_exec_details_logs_buy(self):
        with patch("peg_primary.tprint") as tp:
            self.t.execDetails(0, self._contract(),
                               self._execution(side="BOT", orderId=42,
                                               execId="x-42-1", shares=25,
                                               price=49.97, exchange="ARCA",
                                               cumQty=25, avgPrice=49.97))
        msgs = [c.args[0] for c in tp.call_args_list]
        self.assertEqual(len(msgs), 1)
        m = msgs[0]
        self.assertIn("Execution BUY", m)
        self.assertIn("orderId=42", m)
        self.assertIn("execId=x-42-1", m)
        self.assertIn("shares=25", m)
        self.assertIn("price=49.97", m)
        self.assertIn("exchange=ARCA", m)
        self.assertIn("cumQty=25", m)

    def test_exec_details_logs_sell(self):
        with patch("peg_primary.tprint") as tp:
            self.t.execDetails(0, self._contract(),
                               self._execution(side="SLD"))
        self.assertIn("Execution SELL", tp.call_args_list[0].args[0])

    def test_exec_details_filters_other_symbols(self):
        with patch("peg_primary.tprint") as tp:
            self.t.execDetails(0, self._contract(symbol="OTHER"),
                               self._execution())
        tp.assert_not_called()

    def test_exec_details_filters_other_sec_types(self):
        with patch("peg_primary.tprint") as tp:
            self.t.execDetails(0, self._contract(sec_type="OPT"),
                               self._execution())
        tp.assert_not_called()

    def test_terminal_status_clears_order_qty(self):
        self.t.buy_order_id = 5
        self.t.buy_order_qty = 100
        self._order_status(orderId=5, status="Filled", filled=100,
                           remaining=0, avgFillPrice=50.0)
        self.assertIsNone(self.t.buy_order_id)
        self.assertIsNone(self.t.buy_order_qty)

    def test_open_order_records_total_quantity(self):
        contract = MockContract()
        contract.symbol = "TEST"
        contract.secType = "STK"
        order = MockOrder()
        order.action = "BUY"
        order.totalQuantity = 75
        self.t.openOrder(123, contract, order, None)
        self.assertEqual(self.t.buy_order_id, 123)
        self.assertEqual(self.t.buy_order_qty, 75)

        sell_order = MockOrder()
        sell_order.action = "SELL"
        sell_order.totalQuantity = 40
        self.t.openOrder(124, contract, sell_order, None)
        self.assertEqual(self.t.sell_order_id, 124)
        self.assertEqual(self.t.sell_order_qty, 40)


class TestResizeOppositeAfterPartialFill(unittest.TestCase):
    """After a partial fill the opposite-side order's totalQuantity is stale.
    sync_orders should modify it in place (placeOrder with the same orderId)
    so the working remaining matches the new desired size."""

    def setUp(self):
        self.cfg = _minimal_config()
        self.t = Trader(self.cfg)
        self.t.ready_for_trading = True
        self.t.ref_price = 50.0
        self.t.nextOrderId = 500
        self.t._bid = 49.90
        self.t._ask = 50.10
        self.t.last_trade_price = 50.0
        self.t.placeOrder = Mock()
        self.t.safe_cancel_order = Mock()

    def test_partial_buy_fill_grows_existing_sell(self):
        """Position was 20 with SELL sized 20. BUY partially filled to 50.
        SELL should be amended to total 50 (filled 0 + desired 50)."""
        self.cfg["max_pos"] = 100
        self.t.position_size = 50
        self.t.buy_order_id = 10
        self.t.buy_order_qty = 80
        self.t.remaining_by_order[10] = 50
        self.t.sell_order_id = 11
        self.t.sell_order_qty = 20
        self.t.remaining_by_order[11] = 20
        self.t.open_symbol_buys = 1
        self.t.open_symbol_sells = 1

        self.t.sync_orders()

        self.t.placeOrder.assert_called_once()
        oid, _, order = self.t.placeOrder.call_args[0]
        self.assertEqual(oid, 11)
        self.assertEqual(order.action, "SELL")
        self.assertEqual(order.totalQuantity, 50)
        self.assertEqual(order.lmtPrice, 50.05)
        self.assertEqual(self.t.sell_order_qty, 50)

    def test_partial_sell_fill_grows_existing_buy(self):
        """Position dropped from 30 to 10 (SELL partial fill of 20). The
        existing BUY's remaining (70) is now smaller than desired (90).
        BUY should be amended to total 90 (filled 0 + desired 90)."""
        self.cfg["max_pos"] = 100
        self.t.position_size = 10
        self.t.buy_order_id = 20
        self.t.buy_order_qty = 70
        self.t.remaining_by_order[20] = 70
        self.t.sell_order_id = 21
        self.t.sell_order_qty = 30
        self.t.remaining_by_order[21] = 10
        self.t.open_symbol_buys = 1
        self.t.open_symbol_sells = 1

        self.t.sync_orders()

        self.t.placeOrder.assert_called_once()
        oid, _, order = self.t.placeOrder.call_args[0]
        self.assertEqual(oid, 20)
        self.assertEqual(order.action, "BUY")
        self.assertEqual(order.totalQuantity, 90)
        self.assertEqual(order.lmtPrice, 49.95)
        self.assertEqual(self.t.buy_order_qty, 90)

    def test_no_resize_when_remaining_matches_desired(self):
        """When the working size already matches desired (e.g., a same-side
        partial fill where remaining naturally equals max_pos - pos),
        sync_orders should not modify or cancel."""
        self.cfg["max_pos"] = 100
        self.t.position_size = 30
        self.t.buy_order_id = 30
        self.t.buy_order_qty = 100
        self.t.remaining_by_order[30] = 70
        self.t.sell_order_id = 31
        self.t.sell_order_qty = 30
        self.t.remaining_by_order[31] = 30
        self.t.open_symbol_buys = 1
        self.t.open_symbol_sells = 1

        self.t.sync_orders()

        self.t.placeOrder.assert_not_called()
        self.t.safe_cancel_order.assert_not_called()

    def test_resize_below_min_order_size_cancels(self):
        """Shrink to below min_order_size cancels the order."""
        self.cfg["max_pos"] = 96
        self.cfg["min_order_size"] = 10
        self.t.min_order_size = 10
        self.t.position_size = 95
        self.t.buy_order_id = 40
        self.t.buy_order_qty = 100
        self.t.remaining_by_order[40] = 5
        self.t.sell_order_id = 41
        self.t.sell_order_qty = 95
        self.t.remaining_by_order[41] = 95
        self.t.open_symbol_buys = 1
        self.t.open_symbol_sells = 1

        self.t.sync_orders()

        self.t.safe_cancel_order.assert_called_once_with(40)
        self.t.placeOrder.assert_not_called()
        self.assertIsNone(self.t.buy_order_id)
        self.assertIsNone(self.t.buy_order_qty)
        self.assertEqual(self.t.open_symbol_buys, 0)

    def test_flat_branch_resizes_existing_buy_back_to_max_pos(self):
        """Transition to flat (SELL fully filled). An existing BUY left over
        from partial state should be amended up to the flat target."""
        self.cfg["max_pos"] = 100
        self.t.position_size = 0
        self.t.buy_order_id = 50
        self.t.buy_order_qty = 70
        self.t.remaining_by_order[50] = 40
        self.t.sell_order_id = None
        self.t.sell_order_qty = None
        self.t.open_symbol_buys = 1
        self.t.open_symbol_sells = 0

        self.t.sync_orders()

        self.t.placeOrder.assert_called_once()
        oid, _, order = self.t.placeOrder.call_args[0]
        self.assertEqual(oid, 50)
        self.assertEqual(order.action, "BUY")
        # filled = 70 - 40 = 30; new_total = 30 + 100 = 130
        self.assertEqual(order.totalQuantity, 130)
        self.assertEqual(self.t.buy_order_qty, 130)

    def test_at_max_branch_resizes_existing_sell_up_to_pos(self):
        """Transition to at-max (BUY fully filled). An existing SELL left over
        from partial state should be amended up to the at-max target."""
        self.cfg["max_pos"] = 100
        self.t.position_size = 100
        self.t.buy_order_id = None
        self.t.buy_order_qty = None
        self.t.sell_order_id = 60
        self.t.sell_order_qty = 80
        self.t.remaining_by_order[60] = 80
        self.t.open_symbol_buys = 0
        self.t.open_symbol_sells = 1

        self.t.sync_orders()

        self.t.placeOrder.assert_called_once()
        oid, _, order = self.t.placeOrder.call_args[0]
        self.assertEqual(oid, 60)
        self.assertEqual(order.action, "SELL")
        self.assertEqual(order.totalQuantity, 100)
        self.assertEqual(self.t.sell_order_qty, 100)

    def test_resize_modifies_via_same_order_id(self):
        """Critical: modification uses the same orderId so IB amends rather
        than placing a second order."""
        self.cfg["max_pos"] = 100
        self.t.position_size = 50
        self.t.buy_order_id = 70
        self.t.buy_order_qty = 80
        self.t.remaining_by_order[70] = 50
        self.t.sell_order_id = 71
        self.t.sell_order_qty = 20
        self.t.remaining_by_order[71] = 20
        self.t.open_symbol_buys = 1
        self.t.open_symbol_sells = 1
        initial_next_id = self.t.nextOrderId

        self.t.sync_orders()

        oid, _, _ = self.t.placeOrder.call_args[0]
        self.assertEqual(oid, 71)
        self.assertEqual(self.t.nextOrderId, initial_next_id)

    def test_resize_skipped_when_remaining_not_yet_known(self):
        """Before any orderStatus has arrived for an order (no entry in
        remaining_by_order), don't try to resize."""
        self.cfg["max_pos"] = 100
        self.t.position_size = 50
        self.t.sell_order_id = 80
        self.t.sell_order_qty = 20
        self.t.open_symbol_buys = 0
        self.t.open_symbol_sells = 1

        self.t.sync_orders()

        sell_calls = [c for c in self.t.placeOrder.call_args_list
                      if c[0][0] == 80]
        self.assertEqual(sell_calls, [])


class TestBuildArgParser(unittest.TestCase):
    def test_defaults(self):
        p = build_arg_parser()
        args = p.parse_args([])
        self.assertTrue(str(args.config).endswith("peg_primary.json"))


class TestStartupOpenOrders(unittest.TestCase):
    def test_open_order_end_logs_snapshot_once(self):
        cfg = _minimal_config()
        t = Trader(cfg)
        t._startup_open_orders_logged = False
        contract = MockContract()
        contract.symbol = "TEST"
        contract.secType = "STK"
        order = MockOrder()
        order.action = "BUY"
        order.orderType = "REL"
        order.totalQuantity = 100
        order.lmtPrice = 48.0
        order.auxPrice = 0.01
        state = MagicMock()
        state.status = "Submitted"
        t.reqOpenOrders = Mock()
        with patch("peg_primary.tprint") as tp:
            t.request_open_orders_snapshot()
            t.openOrder(7, contract, order, state)
            t.openOrderEnd()
        lines = [str(c.args[0]) for c in tp.call_args_list]
        self.assertTrue(any("Open orders at startup for TEST" in x for x in lines))
        self.assertTrue(any("id=7" in x for x in lines))

        with patch("peg_primary.tprint") as tp2:
            t.openOrderEnd()
        self.assertFalse(
            any("Open orders at startup" in str(c.args[0]) for c in tp2.call_args_list)
        )


if __name__ == "__main__":
    unittest.main()
