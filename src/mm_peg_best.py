#!/usr/bin/env python3
"""Hybrid bot: market-maker LMT buys; PEG BEST sells whenever long.

Both sides may work at once. Buy sizing/pricing comes from ``quote_engine``
(capped by ``max_position``); sells use IBKRATS PEG BEST with a protective
limit (never below mid when mid_delta=0). The buy limit is always kept strictly
below the PEG BEST limit, and sides are repriced in an order that preserves
that gap, so the bot can never trade with itself.
PEG BEST limit changes cancel-and-replace when the protective limit moves by
more than ``peg_replace_deadband_ticks`` (IB rejects in-place edits, error 105).
"""

from __future__ import annotations

import argparse
import dataclasses
import sys
import time
from typing import Any, Dict, Optional

from ibapi.order import Order, COMPETE_AGAINST_BEST_OFFSET_UP_TO_MID

from ibkr_app_support import (
    PositionLedger,
    apply_order_account,
    clamp_sell_to_avoid_self_trade,
    idle_until_shutdown,
    load_merged_config,
    price_digits_from_config,
    price_move_exceeds_ticks,
    run_bot,
    stock_contract_with_exchange,
    sync_attrs_from_ledger,
)
from market_maker import (
    LiveOrder,
    MarketMaker,
    QuoteDecision,
    QuotePipelineInvalidPair,
    build_arg_parser as build_mm_arg_parser,
)


class MmPegBest(MarketMaker):
    """quote_engine LMT buy while room remains; PEG BEST sell while long.

    Both may work simultaneously; the buy limit stays strictly below the sell.
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize hybrid bot; rebind ledger to ``mm_peg_best``."""
        super().__init__(config)
        raw_acct = config.get("account", "") or ""
        self.ledger = PositionLedger.open(
            "mm_peg_best", config, client_id=self.client_id, account=raw_acct
        )
        if not self.ignore_ledger:
            sync_attrs_from_ledger(
                self.ledger,
                self,
                qty_attr="position_size",
                avg_attr="avg_cost",
                clamp_qty_nonneg=True,
            )
        self.sell_limit_multiplier = float(
            config.get("sell_limit_multiplier", 0.98)
        )
        self.min_compete_size = int(config.get("min_compete_size", 50))
        self.mid_offset_whole = float(config.get("mid_offset_whole", -0.01))
        self.mid_offset_half = float(config.get("mid_offset_half", -0.005))
        self.post_to_ats_seconds = int(config.get("post_to_ats_seconds", 1))
        self.peg_exchange = str(config.get("peg_exchange", "IBKRATS"))
        self.tif = str(config.get("tif", "DAY"))
        # Replace PEG BEST only when the protective limit moves by more than
        # this many ticks (default 1 stops 44.63↔44.64 cancel+replace thrash).
        self.peg_replace_deadband_ticks = max(
            0, int(config.get("peg_replace_deadband_ticks", 1))
        )
        self.logger.info(
            "mm_peg_best hybrid ledger_qty=%s avg=%.4f peg_exchange=%s "
            "peg_replace_deadband_ticks=%s",
            self.ledger.qty,
            self.ledger.avg_cost_per_share,
            self.peg_exchange,
            self.peg_replace_deadband_ticks,
        )

    def build_peg_best_sell(self, qty: int, limit_price: float) -> Order:
        """Build a PEG BEST sell with protective limit on ``peg_exchange``."""
        o = Order()
        o.action = "SELL"
        o.orderType = "PEG BEST"
        o.totalQuantity = int(qty)
        o.lmtPrice = round(float(limit_price), price_digits_from_config(self.config))
        o.exchange = self.peg_exchange
        o.tif = self.tif
        o.notHeld = True
        o.minCompeteSize = self.min_compete_size
        o.competeAgainstBestOffset = COMPETE_AGAINST_BEST_OFFSET_UP_TO_MID
        o.midOffsetAtWhole = self.mid_offset_whole
        o.midOffsetAtHalf = self.mid_offset_half
        o.postToAts = self.post_to_ats_seconds
        apply_order_account(o, self._order_account())
        return o

    def contract_for_place_order(self, order: Order):
        """Route PEG BEST via IBKRATS so NotHeld is accepted (IB error 10297)."""
        if getattr(order, "orderType", "") == "PEG BEST":
            return stock_contract_with_exchange(self.contract, self.peg_exchange)
        return self.contract

    def _peg_sell_protective_limit(self, bid: float, ask: float) -> float:
        """Protective sell limit from mid × multiplier (no avg-cost floor).

        PEG BEST may sell below average cost; edge is recovered on cheaper buys.
        """
        mid = (float(bid) + float(ask)) / 2.0
        digits = price_digits_from_config(self.config)
        limit = round(mid * self.sell_limit_multiplier, digits)
        return clamp_sell_to_avoid_self_trade(
            limit, self.config, bid=bid, ask=ask, mid=mid
        )

    def place_or_replace_peg_sell(self, qty: int, px: Optional[float]) -> None:
        """Place or replace a PEG BEST sell using ``sell_order`` tracking."""
        # Reuse LMT place/replace plumbing but swap the order builder for sells.
        prior_build = self.build_lmt_order

        def _build(action: str, q: int, price: float) -> Order:
            if action == "SELL":
                return self.build_peg_best_sell(q, price)
            return prior_build(action, q, price)

        self.build_lmt_order = _build  # type: ignore[method-assign]
        try:
            self.place_or_replace_sell(qty, px)
        finally:
            self.build_lmt_order = prior_build  # type: ignore[method-assign]

    def _replace_requires_new_order_id(self, order: Order) -> bool:
        """Cancel+replace PEG BEST sells; IB rejects in-place limit edits (105)."""
        return getattr(order, "orderType", "") == "PEG BEST"

    def _prices_match_for_replace(self, live: LiveOrder, px: float) -> bool:
        """PEG BEST sells: treat limit as unchanged within the tick deadband."""
        if live.side == "SELL":
            return not price_move_exceeds_ticks(
                live.price,
                px,
                min_tick=self._effective_min_tick(),
                ticks=self.peg_replace_deadband_ticks,
            )
        return super()._prices_match_for_replace(live, px)

    def openOrder(self, orderId, contract, order, orderState):
        """Adopt PEG BEST sells as well as LMT buys/sells."""
        ot = getattr(order, "orderType", "")
        if ot == "PEG BEST" and str(getattr(order, "action", "")).upper() == "SELL":
            order.orderType = "LMT"
            try:
                return super().openOrder(orderId, contract, order, orderState)
            finally:
                order.orderType = "PEG BEST"
        return super().openOrder(orderId, contract, order, orderState)

    def _compute_quote_decision(self, snap):
        """Buy-only decision from the engine (LMT sells never used).

        The base clamps the buy under the *working* sell from the snapshot; here
        the sell may be replaced first, so that clamp is stale. ``_quote_two_sided``
        clamps against the new PEG BEST limit instead.
        """
        result = super()._compute_quote_decision(
            dataclasses.replace(snap, sell_work_px=None)
        )
        if isinstance(result, QuotePipelineInvalidPair):
            return result
        return QuoteDecision(
            buy_qty=result.buy_qty,
            buy_px=result.buy_px,
            sell_qty=0,
            sell_px=None,
        )

    def _quote_engine_params(self):
        """Engine params without the stuck-sell buy pause.

        PEG BEST sells carry no avg-cost floor, so being long above the market
        never blocks the sell; keep buying lower to average down.
        """
        return dataclasses.replace(
            super()._quote_engine_params(), pause_buys_when_sell_stuck=False
        )

    def _buy_limit_strictly_below(
        self, buy_px: Optional[float], sell_limit: float
    ) -> Optional[float]:
        """Clamp ``buy_px`` at least one tick under ``sell_limit`` (no self-trade)."""
        if buy_px is None:
            return None
        if float(buy_px) < float(sell_limit):
            return float(buy_px)
        digits = price_digits_from_config(self.config)
        clamped = self._quantize_to_tick(
            float(sell_limit) - self._effective_min_tick()
        )
        clamped = round(clamped, digits)
        return clamped if clamped > 0 else None

    def _quote_two_sided(
        self,
        sellable: int,
        sell_limit: float,
        buy_qty: int,
        buy_px: Optional[float],
    ) -> None:
        """Place/replace both sides so buy < sell limit holds at every step.

        Whichever side is moving *away* from the other goes first: a buy that
        would sit at/above the new sell limit is lowered before the sell is
        replaced; otherwise the sell is (re)placed before the buy is raised.
        """
        buy_px = self._buy_limit_strictly_below(buy_px, sell_limit)
        buy_active = buy_qty > 0 and buy_px is not None
        working_buy = self.buy_order.price if self.buy_order else None

        def _buy() -> None:
            self.place_or_replace_buy(buy_qty, buy_px if buy_active else None)

        def _sell() -> None:
            self.place_or_replace_peg_sell(sellable, sell_limit)

        if working_buy is not None and working_buy >= sell_limit:
            _buy()
            _sell()
        else:
            _sell()
            _buy()

    def maybe_manage_quotes(self, force=False):
        """Long: PEG BEST sell plus quote_engine LMT buy (buy < sell limit)."""
        now = time.time()
        snap = self._capture_quote_mgmt_snapshot()

        if not snap.connected_flag or snap.shutdown_flag:
            return
        if not snap.open_orders_snapshot_done:
            return
        if not self.us_regular_hours():
            return

        inv_reason = self._market_invalid_reason_for_nbbo(
            snap.quote_bid, snap.quote_ask, snap.quote_luts
        )
        if self._abort_for_market_invalid_reason(inv_reason):
            return

        nbbo_key = (
            snap.quote_bid,
            snap.quote_ask,
            snap.quote_bid_sz,
            snap.quote_ask_sz,
        )
        if not self._nbbo_throttle.should_run(
            nbbo_key,
            force=force,
            bypass_if=lambda: self._needs_quote_despite_nbbo_throttle_with(
                snap.position_size, snap.sell_snap
            ),
        ):
            return

        with self.lock:
            self.last_quote_eval = now

        sellable = max(0, int(snap.max_sell))
        qb, qa = snap.quote_bid, snap.quote_ask
        if qb is None or qa is None:
            return

        pipeline = self._compute_quote_decision(snap)
        if isinstance(pipeline, QuotePipelineInvalidPair):
            msg = (
                "Computed invalid quotes buy=%s sell=%s; cancelling."
                if pipeline.after_compute
                else "Quotes invalid after NBBO clamp buy=%s sell=%s; cancelling."
            )
            self._abort_quotes_on_invalid_pair(
                pipeline.buy_px, pipeline.sell_px, msg
            )
            with self.lock:
                self._nbbo_throttle.mark_ran(nbbo_key)
            return
        buy_qty = pipeline.buy_qty
        buy_px = pipeline.buy_px if buy_qty > 0 else None

        if sellable > 0:
            limit = self._peg_sell_protective_limit(float(qb), float(qa))
            self._quote_two_sided(sellable, limit, buy_qty, buy_px)
        else:
            self.place_or_replace_peg_sell(0, None)
            self.place_or_replace_buy(buy_qty, buy_px)

        with self.lock:
            self._nbbo_throttle.mark_ran(nbbo_key)


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI for the hybrid bot (MM flags plus PEG BEST sell knobs)."""
    parser = build_mm_arg_parser()
    parser.description = (
        "IBKR hybrid: market-maker LMT buys when flat; PEG BEST sells when long"
    )
    # Replace default config path from market_maker.json
    for action in parser._actions:
        if getattr(action, "dest", None) == "config":
            action.default = __import__("pathlib").Path("config") / "mm_peg_best.json"
            break
    parser.add_argument("--sell_limit_multiplier", type=float)
    parser.add_argument("--min_compete_size", type=int)
    parser.add_argument("--mid_offset_whole", type=float)
    parser.add_argument("--mid_offset_half", type=float)
    parser.add_argument("--post_to_ats_seconds", type=int)
    parser.add_argument(
        "--peg_exchange",
        type=str,
        help="Exchange for PEG BEST sell orders (default: IBKRATS)",
    )
    parser.add_argument(
        "--peg_replace_deadband_ticks",
        type=int,
        help="Cancel+replace PEG BEST only when limit moves by more than this many ticks",
    )
    return parser


def main() -> None:
    """CLI entry point."""
    parser = build_arg_parser()
    args = parser.parse_args()
    config = load_merged_config(
        args,
        required=[
            "host",
            "port",
            "symbol",
            "sec_type",
            "currency",
            "exchange",
            "primary_exchange",
            "market_timezone",
            "market_open_hour",
            "market_open_minute",
            "market_close_hour",
            "sell_limit_multiplier",
            "min_compete_size",
            "mid_offset_whole",
            "mid_offset_half",
            "post_to_ats_seconds",
        ],
    )
    app = MmPegBest(config)
    sys.exit(
        run_bot(
            app,
            config,
            is_ready=lambda: app.api_ready,
            main_loop=lambda: idle_until_shutdown(app),
            extra_daemon_threads=[("Watchdog", app.watchdog_loop)],
            ready_label="nextValidId",
        )
    )


if __name__ == "__main__":
    main()
