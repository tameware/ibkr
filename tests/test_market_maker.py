# Usage: python -m unittest discover -s tests -t . -v

# Coded by Cursor

import datetime
import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch
from zoneinfo import ZoneInfo

from tests.ibapi_mocks import install_ibapi_mocks

install_ibapi_mocks(ticktype=False, order_cancel=True)

from ibkr_app_support import (
    NbboThrottle,
    cli_to_config,
    deep_merge_config_sections,
    flatten_config_sections,
    load_config_file,
    make_stock_contract,
    merge_config,
    seed_ledger_position,
)
from ibkr_app_support import regular_session_open  # noqa: E402

from market_maker import (  # noqa: E402
    LiveOrder,
    MarketMaker,
    QuoteState,
    build_arg_parser,
)

_SESSION_CFG = {
    "market_timezone": "America/New_York",
    "market_open_hour": 9,
    "market_open_minute": 30,
    "market_close_hour": 16,
}


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

    def test_skips_slash_slash_comment_keys(self):
        data = {
            "ibkr": {"host": "127.0.0.1", "//": "paper port note"},
            "strategy": {"max_pos": 100, "//": "strategy note"},
        }
        out = flatten_config_sections(data)
        self.assertEqual(out, {"host": "127.0.0.1", "max_pos": 100})
        self.assertNotIn("//", out)

    def test_load_config_file_allows_repeated_slash_slash_keys(self):
        raw = (
            '{"strategy": {"max_pos": 10, "//": "first note", '
            '"loop_seconds": 5, "//": "second note"}}'
        )
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(raw)
            path = f.name
        try:
            out = load_config_file(path)
        finally:
            Path(path).unlink(missing_ok=True)
        self.assertEqual(out["max_pos"], 10)
        self.assertEqual(out["loop_seconds"], 5)
        self.assertNotIn("//", out)

    def test_duplicate_nested_key_raises(self):
        data = {
            "a": {"port": 1},
            "b": {"port": 2},
        }
        with self.assertRaises(ValueError) as ctx:
            flatten_config_sections(data)
        self.assertIn("port", str(ctx.exception))

    def test_deep_merge_config_sections(self):
        base = {
            "ibkr": {"host": "127.0.0.1", "port": 7496},
            "strategy": {"loop_seconds": 10, "max_pos": 100},
        }
        override = {
            "ibkr": {"client_id": 4},
            "strategy": {"mid_delta": 0.01},
            "logging": {"log_file": "peg_best.log"},
        }
        merged = deep_merge_config_sections(base, override)
        self.assertEqual(merged["ibkr"]["host"], "127.0.0.1")
        self.assertEqual(merged["ibkr"]["client_id"], 4)
        self.assertEqual(merged["strategy"]["loop_seconds"], 10)
        self.assertEqual(merged["strategy"]["mid_delta"], 0.01)
        self.assertEqual(merged["logging"]["log_file"], "peg_best.log")

    def test_load_config_file_merges_base_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "base.json").write_text(
                json.dumps(
                    {
                        "ibkr": {"host": "127.0.0.1", "port": 7496},
                        "strategy": {"max_pos": 100},
                    }
                ),
                encoding="utf-8",
            )
            (root / "bot.json").write_text(
                json.dumps({"ibkr": {"client_id": 9}, "strategy": {"loop_seconds": 5}}),
                encoding="utf-8",
            )
            out = load_config_file(str(root / "bot.json"))
        self.assertEqual(out["host"], "127.0.0.1")
        self.assertEqual(out["port"], 7496)
        self.assertEqual(out["client_id"], 9)
        self.assertEqual(out["max_pos"], 100)
        self.assertEqual(out["loop_seconds"], 5)

    def test_load_config_file_skips_base_merge_when_loading_base(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "base.json"
            path.write_text(json.dumps({"ibkr": {"host": "10.0.0.1"}}), encoding="utf-8")
            out = load_config_file(str(path))
        self.assertEqual(out["host"], "10.0.0.1")

    def test_repo_peg_best_config_includes_base_defaults(self):
        out = load_config_file("config/peg_best.json")
        self.assertEqual(out["host"], "127.0.0.1")
        self.assertEqual(out["client_id"], 4)
        self.assertEqual(out["exchange"], "IBKRATS")
        self.assertEqual(out["log_file"], "peg_best.log")
        self.assertEqual(out["ignored_error_codes"], [2103, 2104, 2106, 2108, 2158])


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
        self._log_patcher = patch(
            "ibkr_bot_base.build_logger", return_value=MagicMock()
        )
        self._log_patcher.start()
        self.addCleanup(self._log_patcher.stop)
        self._ledger_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._ledger_dir.cleanup)
        self.mm = MarketMaker(
            {
                "ledgers_dir": self._ledger_dir.name,
                "symbol": "FDX",
                "sec_type": "STK",
                "currency": "USD",
                "exchange": "SMART",
                "primary_exchange": "NYSE",
                "console": False,
                "min_quote_spread_cents": 50.0,
                "min_tick": 0.01,
                **_SESSION_CFG,
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

    def test_skips_further_sell_improve_when_ask_sz_not_gt_order_even_if_inside_spread(self):
        """ask_depth_ok requires ask_sz > sell_qty (or unknown 0); thin top when already
        stepped in must not lower sell another tick (OZ: 218 shares, ask_sz=200 at 50.71)."""
        self.mm.quote.bid = 50.0
        self.mm.quote.ask = 50.71
        self.mm.quote.bid_size = 300
        self.mm.quote.ask_size = 200
        nb, ns = self.mm._nbbo_tick_improve_quotes(
            50.0, 50.71, buy_qty=0, sell_qty=218
        )
        self.assertAlmostEqual(nb, 50.0)
        self.assertAlmostEqual(ns, 50.71)


class TestMarketMakerStatics(unittest.TestCase):
    """Pure helpers on MarketMaker."""

    def test_round_cents(self):
        self.assertEqual(MarketMaker.round_down_cent(10.999), 10.99)
        self.assertEqual(MarketMaker.round_up_cent(10.001), 10.01)

    def test_make_stock_contract(self):
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


class TestMarketMakerCore(unittest.TestCase):
    """MarketMaker instance behavior with mocks."""

    def setUp(self):
        self._log_patcher = patch(
            "ibkr_bot_base.build_logger", return_value=MagicMock()
        )
        self._log_patcher.start()
        self.addCleanup(self._log_patcher.stop)

        self._ledger_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._ledger_dir.cleanup)
        self.base_config = {
            "ledgers_dir": self._ledger_dir.name,
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
            "mid_delta": 0.01,
            **_SESSION_CFG,
        }
        self.mm = MarketMaker(self.base_config)
        self._session_open_patcher = patch(
            "market_maker.regular_session_open", return_value=True
        )
        self._session_open_patcher.start()
        self.addCleanup(self._session_open_patcher.stop)
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

    def _seed_pos(self, qty: int, avg_cost: float = 100.0) -> None:
        seed_ledger_position(
            self.mm.ledger,
            self.mm,
            qty,
            qty_attr="position_size",
            avg_attr="avg_cost",
            avg_cost=avg_cost,
            clamp_qty_nonneg=True,
        )

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

    def test_us_regular_hours_during_session(self):
        ny = ZoneInfo("America/New_York")
        during = datetime.datetime(2026, 5, 11, 10, 30, 0, tzinfo=ny)
        self.assertTrue(regular_session_open(self.base_config, now=during))
        with patch("market_maker.regular_session_open", return_value=True) as mock_open:
            self.assertTrue(self.mm.us_regular_hours())
        mock_open.assert_called_once_with(self.mm.config)

    def test_us_regular_hours_outside_session(self):
        with patch("market_maker.regular_session_open", return_value=False) as mock_open:
            self.assertFalse(self.mm.us_regular_hours())
        mock_open.assert_called_once_with(self.mm.config)

    def test_maybe_manage_quotes_skips_outside_regular_session(self):
        ts = 3_000_000.0
        self.mm.connected_flag = True
        self.mm.shutdown_flag = False
        self.mm.open_orders_snapshot_done = True
        self.mm.quote.bid = 50.0
        self.mm.quote.ask = 51.0
        self.mm.quote.bid_size = 100
        self.mm.quote.ask_size = 100
        self.mm.quote.last_update_ts = ts
        self.mm.place_or_replace_buy = Mock()
        self.mm.place_or_replace_sell = Mock()

        with patch("market_maker.time.time", return_value=ts), patch(
            "market_maker.regular_session_open", return_value=False
        ):
            self.mm.maybe_manage_quotes(force=True)

        self.mm.place_or_replace_buy.assert_not_called()
        self.mm.place_or_replace_sell.assert_not_called()

    def test_poll_session_hours_cancels_when_session_closes(self):
        self.mm._prev_us_regular_hours = True
        self.mm.buy_order = LiveOrder(
            order_id=1, side="BUY", price=50.0, qty=100, remaining=100
        )
        with patch("market_maker.regular_session_open", return_value=False), patch.object(
            self.mm, "_cancel_working_quotes_flat_inventory"
        ) as mock_cancel, patch("market_maker.log_session_transition"):
            self.mm._poll_session_hours()
        mock_cancel.assert_called_once()
        self.assertFalse(self.mm._prev_us_regular_hours)

    def test_poll_session_hours_does_not_cancel_while_still_closed(self):
        self.mm._prev_us_regular_hours = False
        with patch("market_maker.regular_session_open", return_value=False), patch.object(
            self.mm, "_cancel_working_quotes_flat_inventory"
        ) as mock_cancel, patch("market_maker.log_session_transition"):
            self.mm._poll_session_hours()
        mock_cancel.assert_not_called()

    def test_compute_desired_quotes_flat_position(self):
        self.mm.quote.bid = 100.0
        self.mm.quote.ask = 101.0
        self._seed_pos(0, avg_cost=0.0)
        buy_px, sell_px = self.mm.compute_desired_quotes()
        self.assertIsNotNone(buy_px)
        self.assertIsNotNone(sell_px)
        self.assertLess(buy_px, sell_px)

    def test_desired_sizes(self):
        self._seed_pos(50)
        self.mm.base_qty = 100
        self.mm.max_position = 200
        b, s = self.mm.desired_sizes()
        self.assertEqual(b, 100)
        self.assertEqual(s, 50)

    def test_desired_sizes_caps_buy_by_max_buy_shares_per_run(self):
        self.mm.max_buy_shares_per_run = 30
        self.mm.buy_shares_filled = 25
        self._seed_pos(0)
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
        self._seed_pos(500)
        self.mm.max_position = 500
        self.assertFalse(self.mm.can_open_new_long())
        self._seed_pos(100)
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

    def test_should_keep_sell_adoption_prefers_larger_working_size(self):
        self._seed_pos(200)
        small = LiveOrder(
            order_id=1,
            side="SELL",
            price=50.0,
            qty=100,
            status="Submitted",
            filled=0,
            remaining=100,
        )
        big = LiveOrder(
            order_id=2,
            side="SELL",
            price=50.0,
            qty=200,
            status="Submitted",
            filled=0,
            remaining=200,
        )
        self.assertTrue(self.mm._should_keep_sell_adoption(big, small))
        self.assertFalse(self.mm._should_keep_sell_adoption(small, big))

    def test_needs_quote_despite_nbbo_throttle_no_sell_long(self):
        self._seed_pos(150)
        self.mm.sell_order = None
        self.assertTrue(self.mm._needs_quote_despite_nbbo_throttle())

    def test_needs_quote_despite_nbbo_throttle_undersized_sell(self):
        self._seed_pos(200)
        self.mm.sell_order = LiveOrder(
            order_id=3,
            side="SELL",
            price=50.0,
            qty=80,
            status="Submitted",
            filled=0,
            remaining=80,
        )
        self.assertTrue(self.mm._needs_quote_despite_nbbo_throttle())

    def test_needs_quote_despite_nbbo_throttle_ok_when_sell_covers_position(self):
        self._seed_pos(200)
        self.mm.sell_order = LiveOrder(
            order_id=3,
            side="SELL",
            price=50.0,
            qty=200,
            status="Submitted",
            filled=0,
            remaining=200,
        )
        self.assertFalse(self.mm._needs_quote_despite_nbbo_throttle())

    def test_maybe_manage_quotes_bypasses_throttle_when_long_without_working_sell(self):
        ts = 3_000_000.0
        self.mm.connected_flag = True
        self.mm.shutdown_flag = False
        self.mm.open_orders_snapshot_done = True
        self._seed_pos(200, avg_cost=48.0)
        self.mm.sell_order = None
        self.mm.quote.bid = 50.0
        self.mm.quote.ask = 51.0
        self.mm.quote.bid_size = 100
        self.mm.quote.ask_size = 100
        self.mm.quote.last_update_ts = ts
        self.mm.quote_refresh_seconds = 3.0
        self.mm._nbbo_throttle = NbboThrottle(3.0, clock=lambda: ts)
        self.mm._nbbo_throttle.mark_ran((50.0, 51.0, 100, 100))
        self.mm._nbbo_throttle._last_run_ts = ts - 0.5
        self.mm.next_order_id = 7001

        self.mm.place_or_replace_buy = Mock()
        self.mm.place_or_replace_sell = Mock()

        with patch("market_maker.time.time", return_value=ts), patch.object(
            self.mm, "_compute_desired_quotes_for", return_value=(50.0, 51.0)
        ):
            self.mm.maybe_manage_quotes(force=False)

        self.mm.place_or_replace_sell.assert_called_once()
        sell_qty, _ = self.mm.place_or_replace_sell.call_args[0]
        self.assertEqual(sell_qty, 200)
        self.mm.place_or_replace_buy.assert_called_once()
        buy_qty, _ = self.mm.place_or_replace_buy.call_args[0]
        self.assertGreater(buy_qty, 0)

    def test_safe_cancel_order_fallback_after_order_cancel_typeerror(self):
        from ibkr_app_support import safe_cancel_order

        self.mm.isConnected = Mock(return_value=True)
        self.mm.serverVersion = Mock(return_value=157)
        self.mm.cancelOrder = Mock(side_effect=[TypeError(), None])
        safe_cancel_order(self.mm, 42)
        self.assertEqual(self.mm.cancelOrder.call_count, 2)

    def test_safe_cancel_order_skips_when_not_connected(self):
        from ibkr_app_support import safe_cancel_order

        self.mm.isConnected = Mock(return_value=False)
        self.mm.cancelOrder = Mock()
        safe_cancel_order(self.mm, 99)
        self.mm.cancelOrder.assert_not_called()

    def test_safe_cancel_order_skips_when_server_version_none(self):
        from ibkr_app_support import safe_cancel_order

        self.mm.isConnected = Mock(return_value=True)
        self.mm.serverVersion = Mock(return_value=None)
        self.mm.cancelOrder = Mock()
        safe_cancel_order(self.mm, 99)
        self.mm.cancelOrder.assert_not_called()

    def test_place_or_replace_sell_reuses_order_id_when_price_changes(self):
        """IB modifies a limit in place when placeOrder uses the same order id (no cancel/replace)."""
        self.mm.next_order_id = 500
        self._seed_pos(218)
        self.mm.buy_order = None
        self.mm.sell_order = None
        self.mm.placeOrder = Mock()
        self.mm.cancelOrder = Mock()
        self.mm.place_or_replace_sell(218, 50.70)
        self.mm.place_or_replace_sell(218, 50.69)
        self.assertEqual(self.mm.sell_order.order_id, 500)
        self.assertEqual(self.mm.placeOrder.call_count, 2)
        oids = [c[0][0] for c in self.mm.placeOrder.call_args_list]
        self.assertEqual(oids, [500, 500])
        self.mm.cancelOrder.assert_not_called()

    def test_tick_price_updates_quote(self):
        self.mm.market_data_req_id = 1001
        with patch.object(self.mm, "maybe_manage_quotes"):
            self.mm.tickPrice(1001, 1, 50.25, Mock())
            self.mm.tickPrice(1001, 2, 50.75, Mock())
        self.assertEqual(self.mm.quote.bid, 50.25)
        self.assertEqual(self.mm.quote.ask, 50.75)

    def test_tick_size_triggers_maybe_manage_quotes(self):
        """NBBO size-only updates can flip tick-improve depth (e.g. ask_sz 200→300); must rerun."""
        self.mm.market_data_req_id = 1001
        self.mm.quote.bid = 50.05
        self.mm.quote.ask = 50.70
        self.mm.quote.bid_size = 200
        self.mm.quote.ask_size = 200
        with patch.object(self.mm, "maybe_manage_quotes") as mmq:
            self.mm.tickSize(1001, 0, 100)
        mmq.assert_called_once()
        self.assertEqual(self.mm.quote.bid_size, 100)

    def test_tick_size_refreshes_staleness_timestamps_when_nbbo_valid(self):
        """Stale checks use last_update_ts / last_nbbo_ok_ts; size-only ticks must refresh them."""
        self.mm.market_data_req_id = 1001
        self.mm.max_market_stale_seconds = 60.0
        self.mm.quote.bid = 50.05
        self.mm.quote.ask = 50.66
        self.mm.quote.bid_size = 100
        self.mm.quote.ask_size = 200
        self.mm.quote.last_update_ts = 1_000_000.0
        self.mm.last_nbbo_ok_ts = 1_000_000.0
        with patch.object(self.mm, "maybe_manage_quotes"):
            with patch("market_maker.time.time", return_value=2_000_000.0):
                self.mm.tickSize(1001, 3, 400)
        self.assertEqual(self.mm.quote.last_update_ts, 2_000_000.0)
        self.assertEqual(self.mm.last_nbbo_ok_ts, 2_000_000.0)
        with patch("market_maker.time.time", return_value=2_000_030.0):
            self.assertIsNone(self.mm.market_invalid_reason())

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
            self.mm.client_id,
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
        self._seed_pos(218, avg_cost=50.64)
        self.mm.quote.bid = 50.0
        self.mm.quote.ask = 50.77
        self.mm.quote.bid_size = 100
        self.mm.quote.ask_size = 300
        self.mm.quote.last_update_ts = ts
        self.mm.quote_refresh_seconds = 3.0
        self.mm._nbbo_throttle = NbboThrottle(3.0, clock=lambda: ts)
        self.mm._nbbo_throttle.mark_ran((50.0, 50.77, 100, 100))
        self.mm._nbbo_throttle._last_run_ts = ts - 0.5
        self.mm.next_order_id = 9001

        self.mm.place_or_replace_buy = Mock()
        self.mm.place_or_replace_sell = Mock()

        with patch("market_maker.time.time", return_value=ts), patch.object(
            self.mm, "_compute_desired_quotes_for", return_value=(50.0, 50.75)
        ):
            self.mm.maybe_manage_quotes(force=False)

        self.mm.place_or_replace_sell.assert_called_once()
        sell_qty, sell_px = self.mm.place_or_replace_sell.call_args[0]
        self.assertEqual(sell_qty, 218)
        self.assertAlmostEqual(sell_px, 50.76, delta=0.05)
        self.assertLessEqual(sell_px, 50.77)
        self.mm.place_or_replace_buy.assert_called_once()

    def test_maybe_manage_quotes_places_buy_when_flat(self):
        self.mm.connected_flag = True
        self.mm.shutdown_flag = False
        self.mm.open_orders_snapshot_done = True
        self.mm.next_order_id = 9001
        self.mm.quote.bid = 100.0
        self.mm.quote.ask = 101.0
        now = 1_000_000.0
        self.mm.quote.last_update_ts = now
        self._seed_pos(0, avg_cost=0.0)
        self.mm.last_quote_eval = 0.0
        with patch("market_maker.time.time", return_value=now), patch.object(
            self.mm, "place_or_replace_buy"
        ) as pb, patch.object(self.mm, "place_or_replace_sell") as ps:
            self.mm.maybe_manage_quotes(force=True)
            pb.assert_called_once()
            ps.assert_called_once()

    def test_stop_cancels_open_orders_when_flag_true(self):
        self.mm.cancel_open_orders_on_shutdown = True
        self.mm.buy_order = LiveOrder(order_id=1, side="BUY", price=10.0, qty=100)
        self.mm.sell_order = LiveOrder(order_id=2, side="SELL", price=11.0, qty=50)
        self.mm.isConnected = Mock(return_value=True)
        self.mm.serverVersion = Mock(return_value=157)
        with patch.object(self.mm, "cancel_live_order") as cancel:
            self.mm.stop()
            self.assertEqual(cancel.call_count, 2)

    def test_stop_skips_cancel_when_flag_false(self):
        self.mm.cancel_open_orders_on_shutdown = False
        self.mm.buy_order = LiveOrder(order_id=1, side="BUY", price=10.0, qty=100)
        self.mm.sell_order = LiveOrder(order_id=2, side="SELL", price=11.0, qty=50)
        self.mm.isConnected = Mock(return_value=True)
        self.mm.serverVersion = Mock(return_value=157)
        with patch.object(self.mm, "cancel_live_order") as cancel:
            self.mm.stop()
            cancel.assert_not_called()


class TestArgParser(unittest.TestCase):
    def test_build_arg_parser_defaults(self):
        parser = build_arg_parser()
        args = parser.parse_args([])
        self.assertEqual(str(args.config), str(Path("config") / "market_maker.json"))

    def test_parse_overrides(self):
        parser = build_arg_parser()
        args = parser.parse_args(
            [
                "--symbol",
                "XYZ",
                "--port",
                "7497",
                "--base_qty",
                "50",
                "--mid_delta=0.02",
            ]
        )
        self.assertEqual(args.symbol, "XYZ")
        self.assertEqual(args.port, 7497)
        self.assertEqual(args.base_qty, 50)
        self.assertEqual(args.mid_delta, 0.02)


if __name__ == "__main__":
    unittest.main()
