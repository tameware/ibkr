"""Tests for the MM-buy / PEG-BEST-sell hybrid bot."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

from tests.ibapi_mocks import install_ibapi_mocks
from tests.ledger_test_helpers import init_test_ledgers_dir

install_ibapi_mocks(ticktype=False, order_cancel=True, peg_best_constants=True)

from ibkr_app_support import seed_ledger_position  # noqa: E402
from mm_peg_best import MmPegBest, build_arg_parser  # noqa: E402
from market_maker import LiveOrder  # noqa: E402

_SESSION_CFG = {
    "market_timezone": "America/New_York",
    "market_open_hour": 9,
    "market_open_minute": 30,
    "market_close_hour": 16,
}


class TestMmPegBest(unittest.TestCase):
    def setUp(self):
        self._log_patcher = patch(
            "ibkr_bot_base.build_logger", return_value=MagicMock()
        )
        self._log_patcher.start()
        self.addCleanup(self._log_patcher.stop)

        self.ledgers_dir = init_test_ledgers_dir(self)
        self.base_config = {
            "ledgers_dir": self.ledgers_dir,
            "symbol": "OZ",
            "sec_type": "STK",
            "currency": "USD",
            "exchange": "SMART",
            "primary_exchange": "AMEX",
            "console": False,
            "base_qty": 100,
            "max_position": 300,
            "min_spread": 0.05,
            "quote_refresh_seconds": 3.0,
            "max_market_stale_seconds": 60.0,
            "mid_delta": 0.01,
            "price_round_digits": 2,
            "tif": "DAY",
            "sell_limit_multiplier": 0.98,
            "min_compete_size": 50,
            "mid_offset_whole": -0.01,
            "mid_offset_half": -0.005,
            "post_to_ats_seconds": 1,
            "peg_exchange": "IBKRATS",
            "client_id": 5,
            **_SESSION_CFG,
        }
        self.bot = MmPegBest(self.base_config)
        self._session_open_patcher = patch(
            "market_maker.regular_session_open", return_value=True
        )
        self._session_open_patcher.start()
        self.addCleanup(self._session_open_patcher.stop)
        self._progress_patcher = patch(
            "market_maker.session_progress_fraction", return_value=0.5
        )
        self._progress_patcher.start()
        self.addCleanup(self._progress_patcher.stop)

        self.bot.cancelOrder = Mock()
        self.bot.placeOrder = Mock()
        self.bot.reqMarketDataType = Mock()
        self.bot.reqContractDetails = Mock()
        self.bot.reqPositions = Mock()
        self.bot.reqOpenOrders = Mock()
        self.bot.reqAllOpenOrders = Mock()
        self.bot.reqAutoOpenOrders = Mock()
        self.bot.reqMktData = Mock()
        self.bot.disconnect = Mock()
        self.bot.cancelMktData = Mock()
        self.bot.next_order_id = 1000
        self.bot.connected_flag = True
        self.bot.shutdown_flag = False
        self.bot.open_orders_snapshot_done = True

    def _seed_pos(self, qty: int, avg_cost: float = 47.55) -> None:
        seed_ledger_position(
            self.bot.ledger,
            self.bot,
            qty,
            qty_attr="position_size",
            avg_attr="avg_cost",
            avg_cost=avg_cost,
            clamp_qty_nonneg=True,
        )

    def _set_nbbo(self, bid: float = 48.0, ask: float = 48.40, ts: float = 3_000_000.0):
        self.bot.quote.bid = bid
        self.bot.quote.ask = ask
        self.bot.quote.bid_size = 100
        self.bot.quote.ask_size = 100
        self.bot.quote.last_update_ts = ts
        self.bot._nbbo.bid = bid
        self.bot._nbbo.ask = ask
        self.bot._nbbo.satisfy_pair_for_quoting()

    def test_ledger_strategy_name_is_mm_peg_best(self):
        self.assertEqual(self.bot.ledger._data.get("strategy"), "mm_peg_best")

    def test_flat_places_lmt_buy_not_peg(self):
        ts = 3_000_000.0
        self._seed_pos(0, avg_cost=0.0)
        self._set_nbbo(ts=ts)
        with patch("market_maker.time.time", return_value=ts):
            self.bot.maybe_manage_quotes(force=True)

        self.bot.placeOrder.assert_called()
        oid, contract, order = self.bot.placeOrder.call_args[0]
        self.assertEqual(order.action, "BUY")
        self.assertEqual(order.orderType, "LMT")
        self.assertEqual(order.totalQuantity, 100)
        self.assertIsNone(self.bot.sell_order)

    def test_long_cancels_buy_and_places_peg_best_sell(self):
        ts = 3_000_000.0
        self._seed_pos(150, avg_cost=47.55)
        self._set_nbbo(bid=48.0, ask=48.40, ts=ts)
        self.bot.buy_order = LiveOrder(
            order_id=50, side="BUY", price=48.0, qty=100, status="Submitted"
        )
        self.bot.isConnected = Mock(return_value=True)
        self.bot.serverVersion = Mock(return_value=157)

        with patch("market_maker.time.time", return_value=ts):
            self.bot.maybe_manage_quotes(force=True)

        # Buy cancelled
        cancel_ids = [c.args[0] for c in self.bot.cancelOrder.call_args_list]
        self.assertIn(50, cancel_ids)

        peg_calls = [
            c
            for c in self.bot.placeOrder.call_args_list
            if c[0][2].orderType == "PEG BEST"
        ]
        self.assertEqual(len(peg_calls), 1)
        order = peg_calls[0][0][2]
        self.assertEqual(order.action, "SELL")
        self.assertEqual(order.totalQuantity, 150)
        self.assertEqual(order.exchange, "IBKRATS")
        self.assertTrue(order.notHeld)
        self.assertEqual(order.minCompeteSize, 50)
        self.assertEqual(order.midOffsetAtWhole, -0.01)
        self.assertEqual(order.midOffsetAtHalf, -0.005)
        self.assertEqual(order.postToAts, 1)
        # IB requires contract.exchange=IBKRATS for NotHeld (error 10297 otherwise).
        peg_contract = peg_calls[0][0][1]
        self.assertEqual(peg_contract.exchange, "IBKRATS")
        self.assertEqual(self.bot.contract.exchange, "SMART")
        # Protective limit: mid 48.20 * 0.98 = 47.24, raised by self-trade
        # floor mid + mid_delta = 48.21. Avg cost must not raise it further.
        self.assertAlmostEqual(order.lmtPrice, 48.21)

    def test_peg_sell_limit_not_floored_by_avg_cost(self):
        """PEG BEST may sell below avg cost; profit comes from cheaper buys."""
        self.bot.config["mid_delta"] = 0.0
        self.bot.config["min_profit_per_share"] = 0.03
        self.bot.config["commission_per_share"] = 0.005
        ts = 3_000_000.0
        self._seed_pos(100, avg_cost=47.91)
        self._set_nbbo(bid=46.37, ask=47.91, ts=ts)
        self.bot.isConnected = Mock(return_value=True)
        self.bot.serverVersion = Mock(return_value=157)

        with patch("market_maker.time.time", return_value=ts):
            self.bot.maybe_manage_quotes(force=True)

        peg_calls = [
            c
            for c in self.bot.placeOrder.call_args_list
            if c[0][2].orderType == "PEG BEST"
        ]
        self.assertEqual(len(peg_calls), 1)
        order = peg_calls[0][0][2]
        # mid 47.14 * 0.98 = 46.20, raised by self-trade floor mid+tick = 47.15.
        # Must not use avg_cost + required_edge (47.95).
        self.assertAlmostEqual(order.lmtPrice, 47.15)
        self.assertLess(order.lmtPrice, self.bot.avg_cost)
        self.assertLess(order.lmtPrice, self.bot.avg_cost + 0.04)

    def test_peg_sell_limit_change_cancels_then_places_new_id(self):
        """IB rejects in-place PEG BEST limit edits (error 105); cancel+replace."""
        ts = 3_000_000.0
        self._seed_pos(100, avg_cost=47.0)
        self._set_nbbo(bid=47.0, ask=48.0, ts=ts)
        self.bot.isConnected = Mock(return_value=True)
        self.bot.serverVersion = Mock(return_value=157)

        with patch("market_maker.time.time", return_value=ts):
            self.bot.maybe_manage_quotes(force=True)

        self.assertIsNotNone(self.bot.sell_order)
        first_id = self.bot.sell_order.order_id
        first_px = self.bot.sell_order.price
        self.bot.placeOrder.reset_mock()
        self.bot.cancelOrder.reset_mock()

        # Mid moves enough to change the protective limit.
        self._set_nbbo(bid=46.0, ask=47.0, ts=ts + 10)
        with patch("market_maker.time.time", return_value=ts + 10):
            self.bot.maybe_manage_quotes(force=True)

        cancel_ids = [c.args[0] for c in self.bot.cancelOrder.call_args_list]
        self.assertIn(first_id, cancel_ids)

        peg_calls = [
            c
            for c in self.bot.placeOrder.call_args_list
            if c[0][2].orderType == "PEG BEST"
        ]
        self.assertEqual(len(peg_calls), 1)
        new_id = peg_calls[0][0][0]
        self.assertNotEqual(new_id, first_id)
        self.assertNotAlmostEqual(peg_calls[0][0][2].lmtPrice, first_px)
        self.assertEqual(self.bot.sell_order.order_id, new_id)

    def test_lmt_buy_still_places_on_smart_contract(self):
        ts = 3_000_000.0
        self._seed_pos(0, avg_cost=0.0)
        self._set_nbbo(ts=ts)
        self.bot.isConnected = Mock(return_value=True)
        self.bot.serverVersion = Mock(return_value=157)
        with patch("market_maker.time.time", return_value=ts):
            self.bot.maybe_manage_quotes(force=True)
        buy_calls = [
            c
            for c in self.bot.placeOrder.call_args_list
            if c[0][2].orderType == "LMT" and c[0][2].action == "BUY"
        ]
        self.assertGreaterEqual(len(buy_calls), 1)
        self.assertEqual(buy_calls[0][0][1].exchange, "SMART")
        self.assertFalse(getattr(buy_calls[0][0][2], "notHeld", False))

    def test_quote_cycle_never_leaves_both_sides_working(self):
        ts = 3_000_000.0
        self._seed_pos(100, avg_cost=47.0)
        self._set_nbbo(ts=ts)
        with patch("market_maker.time.time", return_value=ts):
            self.bot.maybe_manage_quotes(force=True)

        self.assertIsNone(self.bot.buy_order)
        self.assertIsNotNone(self.bot.sell_order)
        self.assertEqual(self.bot.sell_order.side, "SELL")

    def test_flat_after_long_cancels_sell_and_resumes_lmt_buy(self):
        ts = 3_000_000.0
        self._seed_pos(100, avg_cost=47.0)
        self._set_nbbo(ts=ts)
        self.bot.isConnected = Mock(return_value=True)
        self.bot.serverVersion = Mock(return_value=157)
        with patch("market_maker.time.time", return_value=ts):
            self.bot.maybe_manage_quotes(force=True)
        self.assertIsNotNone(self.bot.sell_order)
        sell_id = self.bot.sell_order.order_id

        self._seed_pos(0, avg_cost=0.0)
        self.bot.placeOrder.reset_mock()
        with patch("market_maker.time.time", return_value=ts + 10):
            self.bot.maybe_manage_quotes(force=True)

        cancel_ids = [c.args[0] for c in self.bot.cancelOrder.call_args_list]
        self.assertIn(sell_id, cancel_ids)
        buy_calls = [
            c
            for c in self.bot.placeOrder.call_args_list
            if c[0][2].action == "BUY" and c[0][2].orderType == "LMT"
        ]
        self.assertGreaterEqual(len(buy_calls), 1)
        self.assertIsNone(self.bot.sell_order)

    def test_build_arg_parser_accepts_peg_and_mm_flags(self):
        parser = build_arg_parser()
        args = parser.parse_args(
            [
                "--sell_limit_multiplier",
                "0.97",
                "--peg_exchange",
                "IBKRATS",
                "--daily_volume_target",
                "400",
                "--min_compete_size",
                "25",
            ]
        )
        self.assertAlmostEqual(args.sell_limit_multiplier, 0.97)
        self.assertEqual(args.peg_exchange, "IBKRATS")
        self.assertEqual(args.daily_volume_target, 400)
        self.assertEqual(args.min_compete_size, 25)

    def test_default_config_path(self):
        parser = build_arg_parser()
        args = parser.parse_args([])
        self.assertEqual(str(args.config), str(Path("config") / "mm_peg_best.json"))


if __name__ == "__main__":
    unittest.main()
