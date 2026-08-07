"""Tests for the pure quote-decision engine (quote_engine.py).

The engine encodes the strategy goals directly:

* never quote a sell larger than the current position (never go short),
* never let a buy fill push the position above ``max_position``,
* floor every sell at avg_cost + round-trip commission + minimum profit,
* pace both sides toward a daily share-volume target, quoting passively
  when on schedule and stepping toward the mid when behind.
"""

from __future__ import annotations

import datetime
import unittest
from zoneinfo import ZoneInfo

from quote_engine import (
    DailyVolumeTracker,
    QuoteInputs,
    QuoteParams,
    decide_quotes,
    session_progress_fraction,
)

_NY = ZoneInfo("America/New_York")

_SESSION_CFG = {
    "market_timezone": "America/New_York",
    "market_open_hour": 9,
    "market_open_minute": 30,
    "market_close_hour": 16,
}


def _ny(hour: int, minute: int = 0) -> datetime.datetime:
    return datetime.datetime(2026, 8, 7, hour, minute, tzinfo=_NY)


def _params(**overrides) -> QuoteParams:
    defaults = dict(
        max_position=300,
        lot_size=100,
        min_order_size=10,
        daily_volume_target=400,
        min_profit_per_share=0.03,
        commission_per_share=0.005,
        tick=0.01,
    )
    defaults.update(overrides)
    return QuoteParams(**defaults)


def _inputs(**overrides) -> QuoteInputs:
    defaults = dict(
        bid=45.00,
        ask=45.30,
        position=0,
        avg_cost=0.0,
        bought_today=0,
        sold_today=0,
        session_progress=0.5,
    )
    defaults.update(overrides)
    return QuoteInputs(**defaults)


class TestSessionProgressFraction(unittest.TestCase):
    def test_before_open_is_zero(self):
        self.assertEqual(
            session_progress_fraction(_SESSION_CFG, now=_ny(8, 0)), 0.0
        )

    def test_at_open_is_zero(self):
        self.assertEqual(
            session_progress_fraction(_SESSION_CFG, now=_ny(9, 30)), 0.0
        )

    def test_halfway_through_session(self):
        # 9:30-16:00 is 390 minutes; halfway is 12:45.
        self.assertAlmostEqual(
            session_progress_fraction(_SESSION_CFG, now=_ny(12, 45)), 0.5
        )

    def test_after_close_is_one(self):
        self.assertEqual(
            session_progress_fraction(_SESSION_CFG, now=_ny(17, 0)), 1.0
        )


class TestDailyVolumeTracker(unittest.TestCase):
    def test_records_buys_and_sells_separately(self):
        t = DailyVolumeTracker("America/New_York")
        t.record_fill("BUY", 100, now=_ny(10, 0))
        t.record_fill("SELL", 40, now=_ny(10, 5))
        t.record_fill("BUY", 25, now=_ny(10, 10))
        self.assertEqual(t.bought_today(now=_ny(10, 15)), 125)
        self.assertEqual(t.sold_today(now=_ny(10, 15)), 40)

    def test_resets_on_new_market_day(self):
        t = DailyVolumeTracker("America/New_York")
        t.record_fill("BUY", 100, now=_ny(10, 0))
        next_day = _ny(10, 0) + datetime.timedelta(days=1)
        self.assertEqual(t.bought_today(now=next_day), 0)
        t.record_fill("SELL", 30, now=next_day)
        self.assertEqual(t.sold_today(now=next_day), 30)
        self.assertEqual(t.bought_today(now=next_day), 0)

    def test_ignores_nonpositive_quantities(self):
        t = DailyVolumeTracker("America/New_York")
        t.record_fill("BUY", 0, now=_ny(10, 0))
        t.record_fill("SELL", -5, now=_ny(10, 0))
        self.assertEqual(t.bought_today(now=_ny(10, 1)), 0)
        self.assertEqual(t.sold_today(now=_ny(10, 1)), 0)


class TestDecideQuotesSizes(unittest.TestCase):
    def test_flat_position_quotes_no_sell(self):
        q = decide_quotes(_params(), _inputs(position=0))
        self.assertEqual(q.sell_qty, 0)
        self.assertIsNone(q.sell_px)

    def test_never_sells_more_than_position(self):
        q = decide_quotes(_params(), _inputs(position=50, avg_cost=45.0))
        self.assertEqual(q.sell_qty, 50)

    def test_position_at_cap_quotes_no_buy(self):
        q = decide_quotes(_params(), _inputs(position=300, avg_cost=45.0))
        self.assertEqual(q.buy_qty, 0)
        self.assertIsNone(q.buy_px)

    def test_buy_qty_never_exceeds_room_below_cap(self):
        q = decide_quotes(_params(), _inputs(position=250, avg_cost=45.0))
        self.assertEqual(q.buy_qty, 50)

    def test_flat_position_buys_full_lot(self):
        q = decide_quotes(_params(), _inputs(position=0))
        self.assertEqual(q.buy_qty, 100)

    def test_sell_below_min_order_size_suppressed(self):
        q = decide_quotes(_params(), _inputs(position=5, avg_cost=45.0))
        self.assertEqual(q.sell_qty, 0)
        self.assertIsNone(q.sell_px)

    def test_buy_room_below_min_order_size_suppressed(self):
        q = decide_quotes(_params(), _inputs(position=295, avg_cost=45.0))
        self.assertEqual(q.buy_qty, 0)
        self.assertIsNone(q.buy_px)


class TestDecideQuotesBuyPricing(unittest.TestCase):
    def test_on_schedule_joins_bid(self):
        # Halfway through session, target 400 -> 200 due; already bought 200.
        q = decide_quotes(
            _params(), _inputs(bought_today=200, session_progress=0.5)
        )
        self.assertAlmostEqual(q.buy_px, 45.00)

    def test_ahead_of_schedule_joins_bid(self):
        q = decide_quotes(
            _params(), _inputs(bought_today=400, session_progress=0.5)
        )
        self.assertAlmostEqual(q.buy_px, 45.00)

    def test_slightly_behind_steps_quarter_spread(self):
        # Deficit 50 (<= lot): bid + 0.25 * 0.30 = 45.075 -> 45.07 (round down).
        q = decide_quotes(
            _params(), _inputs(bought_today=150, session_progress=0.5)
        )
        self.assertAlmostEqual(q.buy_px, 45.07)

    def test_far_behind_steps_forty_percent_of_spread(self):
        # Deficit 200 (> lot): bid + 0.40 * 0.50 = 45.20, below the
        # mid-edge cap (mid 45.25 - 0.04 = 45.21).
        q = decide_quotes(
            _params(),
            _inputs(ask=45.50, bought_today=0, session_progress=0.5),
        )
        self.assertAlmostEqual(q.buy_px, 45.20)

    def test_buy_capped_below_mid_by_required_edge(self):
        # Wide spread: bid=45.00 ask=46.00, mid=45.50, edge=0.04 -> cap 45.46.
        # Far behind would ask for bid + 0.40 = 45.40, still under cap; make
        # the spread even wider to hit the cap: bid=45.00 ask=47.00 ->
        # mid=46.00, cap=45.96, 40% step = 45.80 -> under. Use a huge deficit
        # step? The cap must bind when the step exceeds it.
        q = decide_quotes(
            _params(min_profit_per_share=0.50),
            _inputs(bid=45.00, ask=45.30, bought_today=0, session_progress=0.5),
        )
        # mid=45.15, edge = 2*0.005 + 0.50 = 0.51 -> cap 44.64 below bid;
        # buy falls back to joining the bid, never above the bid in that case.
        self.assertAlmostEqual(q.buy_px, 45.00)

    def test_buy_cap_binds_when_step_exceeds_it(self):
        # bid=45.00 ask=45.30: mid=45.15, edge=0.04 -> cap 45.11.
        # Far-behind step is 45.12 which exceeds the cap.
        q = decide_quotes(
            _params(), _inputs(bought_today=0, session_progress=0.5)
        )
        self.assertLessEqual(q.buy_px, 45.11)

    def test_buy_price_quantized_to_tick(self):
        q = decide_quotes(
            _params(), _inputs(bid=45.01, ask=45.28, bought_today=150)
        )
        self.assertAlmostEqual(q.buy_px, round(q.buy_px, 2))


class TestDecideQuotesSellPricing(unittest.TestCase):
    def test_on_schedule_joins_ask(self):
        q = decide_quotes(
            _params(),
            _inputs(position=100, avg_cost=45.0, sold_today=200,
                    session_progress=0.5),
        )
        self.assertAlmostEqual(q.sell_px, 45.30)

    def test_slightly_behind_steps_quarter_spread_down(self):
        # Deficit 50: ask - 0.25 * 0.30 = 45.225 -> 45.23 (round up).
        q = decide_quotes(
            _params(),
            _inputs(position=100, avg_cost=45.0, sold_today=150,
                    session_progress=0.5),
        )
        self.assertAlmostEqual(q.sell_px, 45.23)

    def test_far_behind_steps_forty_percent_down(self):
        # Deficit 200: ask - 0.40 * 0.30 = 45.18.
        q = decide_quotes(
            _params(),
            _inputs(position=100, avg_cost=45.0, sold_today=0,
                    session_progress=0.5),
        )
        self.assertAlmostEqual(q.sell_px, 45.18)

    def test_sell_never_below_profit_floor(self):
        # avg 45.20: floor = 45.20 + 0.01 + 0.03 = 45.24 > far-behind 45.18.
        q = decide_quotes(
            _params(),
            _inputs(position=100, avg_cost=45.20, sold_today=0,
                    session_progress=0.5),
        )
        self.assertAlmostEqual(q.sell_px, 45.24)

    def test_sell_floor_holds_even_above_the_ask(self):
        # Market dropped: avg 45.60 -> floor 45.64 sits above ask 45.30.
        q = decide_quotes(
            _params(),
            _inputs(position=100, avg_cost=45.60, sold_today=0,
                    session_progress=0.5),
        )
        self.assertAlmostEqual(q.sell_px, 45.64)

    def test_no_floor_when_avg_cost_unknown(self):
        q = decide_quotes(
            _params(),
            _inputs(position=100, avg_cost=0.0, sold_today=0,
                    session_progress=0.5),
        )
        self.assertAlmostEqual(q.sell_px, 45.18)

    def test_sell_stays_above_bid(self):
        # Aggressive step and low floor must not cross to the bid.
        q = decide_quotes(
            _params(),
            _inputs(bid=45.00, ask=45.04, position=100, avg_cost=40.0,
                    sold_today=0, session_progress=0.9),
        )
        self.assertGreaterEqual(q.sell_px, 45.01)

    def test_full_inventory_escalates_sell_urgency(self):
        # Sold on schedule, but position at cap -> quote as if far behind.
        q = decide_quotes(
            _params(),
            _inputs(position=300, avg_cost=45.0, sold_today=200,
                    session_progress=0.5),
        )
        self.assertAlmostEqual(q.sell_px, 45.18)

    def test_two_thirds_inventory_escalates_at_least_one_step(self):
        q = decide_quotes(
            _params(),
            _inputs(position=200, avg_cost=45.0, sold_today=200,
                    session_progress=0.5),
        )
        self.assertAlmostEqual(q.sell_px, 45.23)


class TestQuoteParamsFromConfig(unittest.TestCase):
    def test_reads_strategy_keys_with_defaults(self):
        cfg = {
            "max_position": 300,
            "base_qty": 100,
            "min_order_size": 10,
            "daily_volume_target": 400,
            "min_profit_per_share": 0.03,
            "commission_per_share": 0.005,
        }
        p = QuoteParams.from_config(cfg)
        self.assertEqual(p.max_position, 300)
        self.assertEqual(p.lot_size, 100)
        self.assertEqual(p.min_order_size, 10)
        self.assertEqual(p.daily_volume_target, 400)
        self.assertAlmostEqual(p.min_profit_per_share, 0.03)
        self.assertAlmostEqual(p.commission_per_share, 0.005)

    def test_defaults_when_config_empty(self):
        p = QuoteParams.from_config({})
        self.assertEqual(p.max_position, 300)
        self.assertEqual(p.lot_size, 100)
        self.assertEqual(p.daily_volume_target, 400)
        self.assertAlmostEqual(p.min_profit_per_share, 0.03)

    def test_buy_and_sell_pair_never_inverted_when_both_quoted(self):
        # Whatever the pacing state, a quoted pair must satisfy buy < sell.
        for bought in (0, 150, 200, 400):
            for sold in (0, 150, 200, 400):
                for pos in (50, 100, 200):
                    q = decide_quotes(
                        _params(),
                        _inputs(position=pos, avg_cost=45.0,
                                bought_today=bought, sold_today=sold,
                                session_progress=0.5),
                    )
                    if q.buy_qty > 0 and q.sell_qty > 0:
                        self.assertLess(
                            q.buy_px, q.sell_px,
                            msg=f"bought={bought} sold={sold} pos={pos}",
                        )


if __name__ == "__main__":
    unittest.main()
