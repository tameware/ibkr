# Usage: python -m unittest test_market_maker.py -v

# Coded by Cursor

import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import sys

# Mock IB API before importing market_maker
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
        self.conId = 0


class MockOrder:
    def __init__(self):
        self.action = ""
        self.orderType = ""
        self.totalQuantity = 0
        self.lmtPrice = 0.0
        self.tif = ""
        self.outsideRth = False


class MockOrderCancel:
    pass


mock_ibapi = MagicMock()
mock_ibapi.wrapper.EWrapper = MockEWrapper
mock_ibapi.client.EClient = MockEClient
mock_ibapi.contract.Contract = MockContract
mock_ibapi.order.Order = MockOrder
mock_ibapi.order_cancel.OrderCancel = MockOrderCancel

sys.modules["ibapi"] = mock_ibapi
sys.modules["ibapi.client"] = mock_ibapi.client
sys.modules["ibapi.wrapper"] = mock_ibapi.wrapper
sys.modules["ibapi.common"] = MagicMock()
sys.modules["ibapi.contract"] = mock_ibapi.contract
sys.modules["ibapi.order"] = mock_ibapi.order
sys.modules["ibapi.order_cancel"] = mock_ibapi.order_cancel

from market_maker import (  # noqa: E402
    LiveOrder,
    MarketMaker,
    QuoteState,
    build_arg_parser,
    cli_to_config,
    flatten_config_sections,
    load_config_file,
    merge_config,
)


class TestFlattenConfig(unittest.TestCase):
    """Test flatten_config_sections (nested JSON like market_maker.json)."""

    def test_flattens_sections_and_top_level_overrides(self):
        data = {
            "ibkr": {"host": "127.0.0.1", "port": 7496},
            "contract": {"symbol": "TEST", "currency": "USD"},
            "extra_top": 1,
        }
        out = flatten_config_sections(data)
        self.assertEqual(out["host"], "127.0.0.1")
        self.assertEqual(out["port"], 7496)
        self.assertEqual(out["symbol"], "TEST")
        self.assertEqual(out["currency"], "USD")
        self.assertEqual(out["extra_top"], 1)

    def test_hyphen_in_keys_normalized(self):
        data = {"section": {"primary-exchange": "NYSE"}}
        out = flatten_config_sections(data)
        self.assertEqual(out["primary_exchange"], "NYSE")

    def test_skips_underscore_prefixed_keys(self):
        data = {"s": {"_hidden": 1, "visible": 2}}
        out = flatten_config_sections(data)
        self.assertEqual(out, {"visible": 2})

    def test_duplicate_nested_key_raises(self):
        data = {
            "a": {"port": 1},
            "b": {"port": 2},
        }
        with self.assertRaises(ValueError) as ctx:
            flatten_config_sections(data)
        self.assertIn("port", str(ctx.exception))


class TestHelpers(unittest.TestCase):
    """Test load_config_file, cli_to_config, merge_config."""

    def test_load_config_file_valid(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"host": "127.0.0.1", "port": 7497}, f)
            path = f.name
        try:
            result = load_config_file(path)
            self.assertEqual(result, {"host": "127.0.0.1", "port": 7497})
        finally:
            Path(path).unlink()

    def test_load_config_file_invalid_not_object(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(["not", "object"], f)
            path = f.name
        try:
            with self.assertRaises(ValueError):
                load_config_file(path)
        finally:
            Path(path).unlink()

    def test_cli_to_config(self):
        args = Namespace(
            config="cfg.json",
            host="localhost",
            port=7497,
            symbol="AAPL",
            skipped=None,
        )
        result = cli_to_config(args)
        self.assertEqual(result, {"host": "localhost", "port": 7497, "symbol": "AAPL"})
        self.assertNotIn("config", result)
        self.assertNotIn("skipped", result)

    def test_merge_config(self):
        file_config = {"host": "127.0.0.1", "port": 7496, "symbol": "MSFT"}
        cli_config = {"port": 7497, "symbol": "AAPL", "extra": "x"}
        merged = merge_config(file_config, cli_config)
        self.assertEqual(merged["host"], "127.0.0.1")
        self.assertEqual(merged["port"], 7497)
        self.assertEqual(merged["symbol"], "AAPL")
        self.assertEqual(merged["extra"], "x")


class TestNbboTickImprove(unittest.TestCase):
    """NBBO one-tick improvement vs size and min quote width."""

    def setUp(self):
        self._log_patcher = patch.object(
            MarketMaker, "_build_logger", return_value=MagicMock()
        )
        self._log_patcher.start()
        self.addCleanup(self._log_patcher.stop)
        self.mm = MarketMaker(
            {
                "symbol": "FDX",
                "sec_type": "STK",
                "currency": "USD",
                "exchange": "SMART",
                "primary_exchange": "NYSE",
                "console": False,
                "min_quote_spread_cents": 50.0,
                "min_tick": 0.01,
            }
        )

    def test_improves_buy_when_bid_size_exceeds_order_and_width_ok(self):
        self.mm.quote.bid = 100.0
        self.mm.quote.ask = 102.0
        self.mm.quote.bid_size = 500
        self.mm.quote.ask_size = 40
        nb, ns = self.mm._nbbo_tick_improve_quotes(
            100.0, 101.0, buy_qty=100, sell_qty=50
        )
        self.assertAlmostEqual(nb, 100.01)
        self.assertAlmostEqual(ns, 101.0)

    def test_skips_buy_when_bid_size_not_greater_than_order(self):
        self.mm.quote.bid = 100.0
        self.mm.quote.ask = 102.0
        self.mm.quote.bid_size = 100
        self.mm.quote.ask_size = 40
        nb, ns = self.mm._nbbo_tick_improve_quotes(
            100.0, 101.0, buy_qty=100, sell_qty=50
        )
        self.assertAlmostEqual(nb, 100.0)
        self.assertAlmostEqual(ns, 101.0)

    def test_skips_buy_when_tick_would_narrow_below_min_cents(self):
        self.mm.quote.bid = 100.0
        self.mm.quote.ask = 101.0
        self.mm.quote.bid_size = 500
        self.mm.quote.ask_size = 500
        nb, ns = self.mm._nbbo_tick_improve_quotes(
            100.0, 100.50, buy_qty=100, sell_qty=50
        )
        self.assertAlmostEqual(nb, 100.0)
        self.assertAlmostEqual(ns, 100.50)

    def test_improves_sell_when_ask_size_exceeds_order(self):
        self.mm.quote.bid = 98.0
        self.mm.quote.ask = 100.0
        self.mm.quote.bid_size = 50
        self.mm.quote.ask_size = 400
        nb, ns = self.mm._nbbo_tick_improve_quotes(
            98.5, 100.0, buy_qty=50, sell_qty=80
        )
        self.assertAlmostEqual(nb, 98.5)
        self.assertAlmostEqual(ns, 99.99)

    def test_improves_sell_when_ask_size_still_zero_price_only_nbbo(self):
        """IB often sends bid/ask prices before tickSize; depth unknown => allow step-in."""
        self.mm.quote.bid = 50.12
        self.mm.quote.ask = 50.80
        self.mm.quote.bid_size = 0
        self.mm.quote.ask_size = 0
        nb, ns = self.mm._nbbo_tick_improve_quotes(
            50.12, 50.80, buy_qty=0, sell_qty=218
        )
        self.assertAlmostEqual(nb, 50.12)
        self.assertAlmostEqual(ns, 50.79)

    def test_skips_sell_when_ask_size_known_and_not_greater_than_order(self):
        self.mm.quote.bid = 50.12
        self.mm.quote.ask = 50.80
        self.mm.quote.ask_size = 200
        nb, ns = self.mm._nbbo_tick_improve_quotes(
            50.12, 50.80, buy_qty=0, sell_qty=218
        )
        self.assertAlmostEqual(nb, 50.12)
        self.assertAlmostEqual(ns, 50.80)


class TestMarketMakerStatics(unittest.TestCase):
    """Pure helpers on MarketMaker."""

    def test_round_cents(self):
        self.assertEqual(MarketMaker.round_down_cent(10.999), 10.99)
        self.assertEqual(MarketMaker.round_up_cent(10.001), 10.01)

    def test_build_contract(self):
        cfg = {
            "symbol": "IBM",
            "sec_type": "STK",
            "exchange": "SMART",
            "currency": "USD",
            "primary_exchange": "NYSE",
        }
        c = MarketMaker.build_contract(cfg)
        self.assertEqual(c.symbol, "IBM")
        self.assertEqual(c.secType, "STK")
        self.assertEqual(c.exchange, "SMART")
        self.assertEqual(c.currency, "USD")
        self.assertEqual(c.primaryExchange, "NYSE")


class TestMarketMakerCore(unittest.TestCase):
    """MarketMaker instance behavior with mocks."""

    def setUp(self):
        self._log_patcher = patch.object(
            MarketMaker, "_build_logger", return_value=MagicMock()
        )
        self._log_patcher.start()
        self.addCleanup(self._log_patcher.stop)

        self.base_config = {
            "symbol": "FDX",
            "sec_type": "STK",
            "currency": "USD",
            "exchange": "SMART",
            "primary_exchange": "NYSE",
            "console": False,
            "base_qty": 100,
            "max_position": 1000,
            "min_spread": 0.05,
            "inside_improve": 0.10,
            "inventory_penalty_per_100": 0.05,
            "target_roundtrip_capture": 0.30,
            "quote_refresh_seconds": 3.0,
            "max_market_stale_seconds": 60.0,
        }
        self.mm = MarketMaker(self.base_config)
        self.mm.cancelOrder = Mock()
        self.mm.placeOrder = Mock()
        self.mm.reqMarketDataType = Mock()
        self.mm.reqContractDetails = Mock()
        self.mm.reqPositions = Mock()
        self.mm.reqOpenOrders = Mock()
        self.mm.reqAllOpenOrders = Mock()
        self.mm.reqAutoOpenOrders = Mock()
        self.mm.reqMktData = Mock()
        self.mm.disconnect = Mock()
        self.mm.cancelMktData = Mock()

    def test_cfg_bool_from_strings(self):
        cfg_no_console = {**self.base_config, "console": False}
        cfg_off = {**cfg_no_console, "cancel_on_invalid_market": "false"}
        mm_off = MarketMaker(cfg_off)
        self.assertFalse(mm_off.cancel_on_invalid_market)

        cfg_on = {**cfg_no_console, "cancel_on_invalid_market": "yes"}
        mm_on = MarketMaker(cfg_on)
        self.assertTrue(mm_on.cancel_on_invalid_market)

    def test_next_id_increments(self):
        self.mm.next_order_id = 500
        self.assertEqual(self.mm.next_id(), 500)
        self.assertEqual(self.mm.next_order_id, 501)

    def test_next_id_raises_if_uninitialized(self):
        self.mm.next_order_id = None
        with self.assertRaises(RuntimeError):
            self.mm.next_id()

    def test_build_lmt_order(self):
        o = self.mm.build_lmt_order("BUY", 100, 12.345)
        self.assertEqual(o.action, "BUY")
        self.assertEqual(o.orderType, "LMT")
        self.assertEqual(o.totalQuantity, 100)
        self.assertEqual(o.lmtPrice, 12.35)
        self.assertEqual(o.tif, "DAY")
        self.assertFalse(o.outsideRth)

    def test_market_invalid_reason_missing_bid(self):
        self.mm.quote = QuoteState(bid=None, ask=100.0, last_update_ts=999999.0)
        self.assertEqual(self.mm.market_invalid_reason(), "missing bid")

    def test_market_invalid_reason_crossed(self):
        self.mm.quote = QuoteState(bid=101.0, ask=100.0, last_update_ts=999999.0)
        self.assertIn("crossed_or_locked", self.mm.market_invalid_reason() or "")

    def test_market_invalid_reason_stale(self):
        self.mm.quote = QuoteState(bid=100.0, ask=101.0, last_update_ts=0.0)
        with patch("market_maker.time.time", return_value=1000.0):
            reason = self.mm.market_invalid_reason()
        self.assertIsNotNone(reason)
        self.assertIn("stale", reason or "")

    def test_market_invalid_reason_spread_too_small(self):
        now = 1_000_000.0
        self.mm.quote = QuoteState(
            bid=100.0, ask=100.02, last_update_ts=now
        )
        with patch("market_maker.time.time", return_value=now):
            reason = self.mm.market_invalid_reason()
        self.assertIsNotNone(reason)
        self.assertIn("spread too small", reason or "")

    def test_compute_desired_quotes_flat_position(self):
        self.mm.quote.bid = 100.0
        self.mm.quote.ask = 101.0
        self.mm.position_qty = 0
        self.mm.avg_cost = 0.0
        buy_px, sell_px = self.mm.compute_desired_quotes()
        self.assertIsNotNone(buy_px)
        self.assertIsNotNone(sell_px)
        self.assertLess(buy_px, sell_px)

    def test_desired_sizes(self):
        self.mm.position_qty = 50
        self.mm.base_qty = 100
        self.mm.max_position = 200
        b, s = self.mm.desired_sizes()
        self.assertEqual(b, 100)
        self.assertEqual(s, 50)

    def test_desired_sizes_caps_buy_by_max_buy_shares_per_run(self):
        self.mm.max_buy_shares_per_run = 30
        self.mm.buy_shares_filled = 25
        self.mm.position_qty = 0
        b, _ = self.mm.desired_sizes()
        self.assertEqual(b, 5)

    def test_is_same_order(self):
        live = LiveOrder(
            order_id=1,
            side="BUY",
            price=10.0,
            qty=100,
            status="Submitted",
            remaining=100,
        )
        self.assertTrue(self.mm.is_same_order(live, "BUY", 100, 10.0))
        self.assertFalse(self.mm.is_same_order(live, "BUY", 99, 10.0))
        self.assertFalse(self.mm.is_same_order(None, "BUY", 100, 10.0))

    def test_can_open_new_long(self):
        self.mm.position_qty = 500
        self.mm.max_position = 500
        self.assertFalse(self.mm.can_open_new_long())
        self.mm.position_qty = 100
        self.assertTrue(self.mm.can_open_new_long())

    def test_should_keep_order_prefers_remaining(self):
        inc = LiveOrder(
            order_id=1,
            side="BUY",
            price=10.0,
            qty=100,
            remaining=50,
            status="Submitted",
        )
        cand = LiveOrder(
            order_id=2,
            side="BUY",
            price=10.0,
            qty=100,
            remaining=0,
            status="Filled",
        )
        self.assertTrue(self.mm._should_keep_order(inc, cand))

    def test_ib_cancel_order_two_arg_fallback(self):
        self.mm.cancelOrder = Mock(side_effect=[TypeError(), None])
        self.mm._ib_cancel_order(42)
        self.assertEqual(self.mm.cancelOrder.call_count, 2)

    def test_tick_price_updates_quote(self):
        self.mm.market_data_req_id = 1001
        with patch.object(self.mm, "maybe_manage_quotes"):
            self.mm.tickPrice(1001, 1, 50.25, Mock())
            self.mm.tickPrice(1001, 2, 50.75, Mock())
        self.assertEqual(self.mm.quote.bid, 50.25)
        self.assertEqual(self.mm.quote.ask, 50.75)

    def test_order_status_clears_buy_on_terminal(self):
        self.mm.buy_order = LiveOrder(
            order_id=10,
            side="BUY",
            price=1.0,
            qty=100,
            status="Submitted",
            remaining=100,
        )
        self.mm.orderStatus(
            10,
            "Filled",
            100,
            0,
            1.0,
            0,
            0,
            0.0,
            0,
            "",
            0.0,
        )
        self.assertIsNone(self.mm.buy_order)

    def test_maybe_manage_quotes_skips_when_disconnected(self):
        self.mm.connected_flag = False
        self.mm.open_orders_snapshot_done = True
        with patch.object(self.mm, "place_or_replace_buy") as pb, patch.object(
            self.mm, "place_or_replace_sell"
        ) as ps:
            self.mm.maybe_manage_quotes(force=True)
            pb.assert_not_called()
            ps.assert_not_called()

    def test_maybe_manage_quotes_runs_when_only_nbbo_size_changes_inside_throttle_window(self):
        """Old behavior throttled whenever dt < quote_refresh_seconds (unconditional return).

        That skipped a full quote cycle when only NBBO *sizes* changed (e.g. ask_sz 100→300)
        with the same bid/ask prices, so tick-improve never stepped a sell from 50.77 to 50.76.

        New behavior throttles only if (bid, ask, bid_sz, ask_sz) equals the snapshot from the
        last Quote decision; a size-only change must re-run."""
        ts = 2_000_000.0
        self.mm.connected_flag = True
        self.mm.shutdown_flag = False
        self.mm.open_orders_snapshot_done = True
        self.mm.position_qty = 218
        self.mm.avg_cost = 50.64
        self.mm.quote.bid = 50.0
        self.mm.quote.ask = 50.77
        self.mm.quote.bid_size = 100
        self.mm.quote.ask_size = 300
        self.mm.quote.last_update_ts = ts
        self.mm.quote_refresh_seconds = 3.0
        self.mm.last_quote_eval = ts - 0.5
        self.mm._last_quote_nbbo_snap = (50.0, 50.77, 100, 100)
        self.mm.next_order_id = 9001

        self.mm.place_or_replace_buy = Mock()
        self.mm.place_or_replace_sell = Mock()

        with patch("market_maker.time.time", return_value=ts), patch.object(
            self.mm, "hard_flatten_mode", return_value=False
        ), patch.object(self.mm, "flatten_only_mode", return_value=True), patch.object(
            self.mm, "compute_desired_quotes", return_value=(50.0, 51.0)
        ):
            self.mm.maybe_manage_quotes(force=False)

        self.mm.place_or_replace_sell.assert_called_once()
        sell_qty, sell_px = self.mm.place_or_replace_sell.call_args[0]
        self.assertEqual(sell_qty, 218)
        self.assertAlmostEqual(sell_px, 50.76)
        self.mm.place_or_replace_buy.assert_called_once_with(0, None)

    def test_maybe_manage_quotes_places_buy_when_flat(self):
        self.mm.connected_flag = True
        self.mm.shutdown_flag = False
        self.mm.open_orders_snapshot_done = True
        self.mm.next_order_id = 9001
        self.mm.quote.bid = 100.0
        self.mm.quote.ask = 101.0
        now = 1_000_000.0
        self.mm.quote.last_update_ts = now
        self.mm.position_qty = 0
        self.mm.last_quote_eval = 0.0
        with patch("market_maker.time.time", return_value=now), patch.object(
            self.mm, "hard_flatten_mode", return_value=False
        ), patch.object(self.mm, "flatten_only_mode", return_value=False), patch.object(
            self.mm, "place_or_replace_buy"
        ) as pb, patch.object(self.mm, "place_or_replace_sell") as ps:
            self.mm.maybe_manage_quotes(force=True)
            pb.assert_called_once()
            ps.assert_called_once()

    def test_flatten_only_mode(self):
        fixed = self.mm._market_now().replace(
            year=2026, month=5, day=10, hour=15, minute=45, second=0
        )
        with patch.object(MarketMaker, "_market_now", return_value=fixed):
            self.assertTrue(self.mm.flatten_only_mode())

    def test_hard_flatten_mode(self):
        fixed = self.mm._market_now().replace(
            year=2026, month=5, day=10, hour=15, minute=56, second=0
        )
        with patch.object(MarketMaker, "_market_now", return_value=fixed):
            self.assertTrue(self.mm.hard_flatten_mode())


class TestArgParser(unittest.TestCase):
    def test_build_arg_parser_defaults(self):
        parser = build_arg_parser()
        args = parser.parse_args([])
        self.assertTrue(str(args.config).endswith("market_maker.json"))

    def test_parse_overrides(self):
        parser = build_arg_parser()
        args = parser.parse_args(
            ["--symbol", "XYZ", "--port", "7497", "--base_qty", "50"]
        )
        self.assertEqual(args.symbol, "XYZ")
        self.assertEqual(args.port, 7497)
        self.assertEqual(args.base_qty, 50)


if __name__ == "__main__":
    unittest.main()
