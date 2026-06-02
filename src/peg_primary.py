"""IBKR Pegged-to-Primary (Relative / ``REL``) quoter.

Flat: one REL buy up to ``max_pos``. Partial inventory (``0 < pos < max_pos``):
REL buy for remaining capacity and REL sell for the current position. At or
above ``max_pos``: sell-only for the full position. Mutually exclusive buy/sell
when flat or capped; both sides may rest when partially filled.

Protective REL ``lmtPrice`` values use the NBBO **mid** only (``(bid+ask)/2``
rounded like ``ref_price`` in :meth:`tickPrice`): buy ceiling ``mid - d`` and
sell floor ``mid + d`` where ``d = max(mid_delta, one_tick)``. ``mid_delta``
defaults to ``0.10`` (``config/base.json``); raising ``d`` to at least one price tick
(``10 ** -price_round_digits``) keeps the rounded buy cap strictly below mid
and the sell floor strictly above mid.

REL ``auxPrice`` is ``(ask - bid) * offset_pct`` (``offset_pct`` clamped below
``0.5``), rounded with ``price_round_digits``.

The bot does not place, modify, or cancel orders until both bid and ask are valid
(positive and ask > bid). Orders are
not submitted when computed quantity is below ``min_order_size`` (default 10).

Quoting is **callback-driven**: ``tickPrice`` updates pending bid/ask and commits
the latest pair after ``nbbo_coalesce_seconds`` of quiet ticks, or at least every
``nbbo_coalesce_max_seconds`` during a continuous stream, then calls
``sync_orders`` (with throttling). Fill/position callbacks also resync. The
main thread only handles session
transitions, periodic snapshot refresh (``loop_seconds``), and shutdown.

On each connection, after the first ``reqOpenOrders`` snapshot completes, all
open orders for the configured symbol are logged, then ``sync_orders`` may
resize or re-peg existing working orders to match current NBBO / limits.

The TWS API exposes Pegged-to-Primary as ``orderType = "REL"`` with ``lmtPrice``
as the protective cap (buy ceiling / sell floor) and ``auxPrice`` as the offset
from the primary quote in **dollars**, here ``(ask - bid) * offset_pct``; IB
adjusts the working price with the NBBO; the client
does not need a tick-improve loop like ``market_maker.py``.

See: https://www.interactivebrokers.com/campus/ibkr-api-page/order-types/
(section *Relative (a.k.a. Pegged-to-Primary)*).
"""

from __future__ import annotations

import argparse
import datetime
import sys
import time
from typing import Any, Dict

from ibkr_app_support import (
    PositionLedger,
    add_config_argument,
    add_ib_connection_arguments,
    add_logging_arguments,
    add_never_sell_below_avg_cost_argument,
    add_session_hours_arguments,
    execution_belongs_to_client,
    handle_ledger_execution,
    handle_ledger_ib_position,
    ib_client_id_from_config,
    clamp_buy_to_avoid_self_trade,
    clamp_sell_to_avg_cost_floor,
    clamp_sell_to_avoid_self_trade,
    load_merged_config,
    max_sell_shares,
    NbboCoalesceSink,
    has_valid_nbbo,
    nbbo_coalesce_intervals_from_config,
    nbbo_mid_rounded,
    plan_working_order_reconcile,
    self_trade_limits_from_nbbo,
    NbboThrottle,
    log_session_transition,
    open_order_belongs_to_client,
    order_status_clients_match,
    regular_session_open,
    run_bot,
    safe_cancel_order,
    sync_attrs_from_ledger,
)
from ibkr_bot_base import (
    ContractResolutionMixin,
    IbkrBotApp,
    OpenPriceBootstrapMixin,
    SnapshotResyncMixin,
)
from ibapi.common import TickAttrib, TickerId
from ibapi.order import Order
from ibapi.ticktype import TickType

MKTDATA_REQ_ID = 3001


def make_rel_order(
    *,
    action: str,
    qty: int,
    limit_price: float,
    offset: float,
    exchange: str,
    tif: str,
    price_round_digits: int,
) -> Order:
    """Build a Pegged-to-Primary (REL) order with protective limit and aux offset."""
    o = Order()
    o.action = action
    o.orderType = "REL"
    o.totalQuantity = qty
    d = int(price_round_digits)
    o.lmtPrice = round(limit_price, d)
    o.auxPrice = round(offset, d)
    o.exchange = exchange
    o.tif = tif
    # Do not set ``notHeld`` on REL: IB rejects with 10297 "Not Held attribute is
    # invalid for this order" for many SMART / Pegged-to-Primary routes.
    return o


class Trader(
    ContractResolutionMixin,
    OpenPriceBootstrapMixin,
    SnapshotResyncMixin,
    IbkrBotApp,
):
    """Pegged-to-Primary (REL) quoter with NBBO-driven limit and size sync."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize :class:`Trader`."""
        super().__init__(
            config,
            logger_name="peg_primary",
            default_log_file="peg_primary.log",
        )

        self.nextOrderId: int | None = None

        self.position_size = 0
        self.avg_cost = 0.0
        self._init_open_price_bootstrap()

        self.buy_order_id: int | None = None
        self.sell_order_id: int | None = None
        self.buy_order_qty: int | None = None
        self.sell_order_qty: int | None = None

        self.ref_price = None
        self._nbbo = NbboCoalesceSink(
            self.config, self._on_coalesced_nbbo, logger=self.logger
        )

        self.remaining_by_order: Dict[int, Any] = {}
        self.filled_by_order: Dict[int, float] = {}
        self.open_symbol_buys = 0
        self.open_symbol_sells = 0
        self.pending_buy = False
        self.pending_sell = False
        self._init_snapshot_resync(config)
        self._init_contract_resolution(self.config)
        self.nbbo_sync_interval_seconds = float(self.config["loop_seconds"])
        self._nbbo_throttle = NbboThrottle(
            self.nbbo_sync_interval_seconds,
            clock=time.monotonic,
        )
        self._prev_us_regular_hours: bool | None = None
        _mos = int(self.config.get("min_order_size", 10))
        self.min_order_size = max(1, _mos)

        self._digits = int(self.config["price_round_digits"])
        self._tick = 10.0 ** (-self._digits)

        self._snapshot_open_order_lines: list[str] = []
        self._startup_open_orders_logged = False
        self.order_working_lmt: Dict[int, float] = {}
        self.order_working_aux: Dict[int, float] = {}

        self.ib_client_id = ib_client_id_from_config(config, default=1)
        self.ledger = PositionLedger.open(
            "peg_primary", config, client_id=self.ib_client_id
        )
        sync_attrs_from_ledger(
            self.ledger, self, qty_attr="position_size", avg_attr="avg_cost"
        )
        self.logger.info(
            "Position ledger %s qty=%s avg_cost_per_share=%.4f",
            self.ledger.path,
            self.ledger.qty,
            self.ledger.avg_cost_per_share,
        )

        _coalesce_quiet, _coalesce_max = nbbo_coalesce_intervals_from_config(self.config)
        self.logger.info(
            "REL quoter initialized symbol=%s exchange=%s "
            "(NBBO coalesce quiet=%.2fs min_interval=%.2fs; ib_client_id=%s)",
            self.config["symbol"],
            self.config["exchange"],
            _coalesce_quiet,
            _coalesce_max,
            self.ib_client_id,
        )

    def market_data_req_ids(self):
        """Market data request ids to cancel on shutdown."""
        return (MKTDATA_REQ_ID,)

    def assign_next_order_id(self, order_id: int) -> None:
        """Store the next usable order id from IB."""
        self.nextOrderId = order_id

    def shutdown_quotes(self) -> None:
        """Cancel working quotes and clear local order tracking."""
        self._nbbo.cancel()
        if self.cancel_open_orders_on_shutdown:
            for label, oid in (("BUY", self.buy_order_id), ("SELL", self.sell_order_id)):
                if oid is None:
                    continue
                safe_cancel_order(self, oid)
                self.logger.info("Cancel %s order id=%s on shutdown", label, oid)
        else:
            self.logger.info(
                "Leaving open orders at IBKR (cancel_open_orders_on_shutdown=false)"
            )

        self.buy_order_id = None
        self.sell_order_id = None
        self.pending_buy = False
        self.pending_sell = False

    def _offset_pct_fraction(self) -> float:
        """``offset_pct`` clamped to ``[0, 0.5)`` for REL ``auxPrice``."""
        return max(0.0, min(float(self.config["offset_pct"]), 0.5 - 1e-9))

    def build_rel_order(self, action: str, qty: int, limit_price: float) -> Order:
        """Build a REL order. ``auxPrice`` is ``(ask - bid) * offset_pct``.

        Requires a valid NBBO; callers must gate on :meth:`_has_valid_nbbo`.
        """
        assert self._has_valid_nbbo()
        spread = float(self._ask) - float(self._bid)
        aux_off = spread * self._offset_pct_fraction()
        return make_rel_order(
            action=action,
            qty=qty,
            limit_price=limit_price,
            offset=aux_off,
            exchange=self.config["exchange"],
            tif=self.config["tif"],
            price_round_digits=self._digits,
        )

    def _meets_min_order_size(self, qty: int) -> bool:
        """Do not submit REL orders smaller than ``min_order_size`` (default 10)."""
        return qty >= self.min_order_size

    def orderStatus(
        self,
        orderId,
        status,
        filled,
        remaining,
        avgFillPrice,
        permId,
        parentId,
        lastFillPrice,
        clientId,
        whyHeld,
        mktCapPrice,
    ):
        """IB callback: track fills and trigger resync on terminal status."""
        if not order_status_clients_match(clientId, self.ib_client_id):
            return
        self.remaining_by_order[orderId] = remaining

        try:
            filled_f = float(filled)
        except (TypeError, ValueError):
            filled_f = 0.0
        try:
            remaining_f = float(remaining)
        except (TypeError, ValueError):
            remaining_f = 0.0

        prev_filled = self.filled_by_order.get(orderId, 0.0)

        if status in ("Submitted", "PreSubmitted", "ApiPending", "PendingSubmit"):
            if orderId == self.buy_order_id:
                self.pending_buy = False
            if orderId == self.sell_order_id:
                self.pending_sell = False

            # IB reports partial fills via Submitted with cumulative filled increasing
            # and remaining > 0. Log only the delta vs. last seen filled.
            if filled_f > prev_filled and remaining_f > 0:
                side = (
                    "BUY"
                    if orderId == self.buy_order_id
                    else "SELL"
                    if orderId == self.sell_order_id
                    else "ORDER"
                )
                self.logger.info(
                    f"{side} {orderId} partial fill: filled={filled} remaining={remaining} "
                    f"avgFillPrice={avgFillPrice} lastFillPrice={lastFillPrice}"
                )
                self.filled_by_order[orderId] = filled_f
                self.trigger_resync()

        if status in ("Filled", "Cancelled", "Inactive", "ApiCancelled"):
            if orderId == self.buy_order_id:
                self.logger.info(
                    f"BUY {orderId} {status} filled={filled} avgFillPrice={avgFillPrice}"
                )
                self.pending_buy = False
                self.buy_order_id = None
                self.buy_order_qty = None
            if orderId == self.sell_order_id:
                self.logger.info(
                    f"SELL {orderId} {status} filled={filled} avgFillPrice={avgFillPrice}"
                )
                self.pending_sell = False
                self.sell_order_id = None
                self.sell_order_qty = None
            self.order_working_lmt.pop(orderId, None)
            self.order_working_aux.pop(orderId, None)
            self.remaining_by_order.pop(orderId, None)
            self.filled_by_order.pop(orderId, None)
            self.trigger_resync()

    def execDetails(self, reqId, contract, execution):
        """IB callback: apply fills to the position ledger."""
        if (
            contract.symbol != self.config["symbol"]
            or contract.secType != self.config["sec_type"]
        ):
            return
        if not execution_belongs_to_client(execution, self.ib_client_id):
            return

        if handle_ledger_execution(
            self.ledger, execution, self.ib_client_id, logger=self.logger
        ):
            sync_attrs_from_ledger(
                self.ledger, self, qty_attr="position_size", avg_attr="avg_cost"
            )

        raw_side = getattr(execution, "side", "")
        side = "BUY" if raw_side == "BOT" else "SELL" if raw_side == "SLD" else raw_side
        self.logger.info(
            f"Execution {side} orderId={execution.orderId} execId={execution.execId} "
            f"shares={execution.shares} price={execution.price} "
            f"exchange={execution.exchange} cumQty={execution.cumQty} "
            f"avgPrice={execution.avgPrice} time={execution.time} "
            f"ledger_qty={self.ledger.qty}"
        )

    @property
    def _bid(self) -> float | None:
        return self._nbbo.bid

    @_bid.setter
    def _bid(self, value: float | None) -> None:
        self._nbbo.bid = None if value is None else float(value)
        self._nbbo.satisfy_pair_for_quoting()

    @property
    def _ask(self) -> float | None:
        return self._nbbo.ask

    @_ask.setter
    def _ask(self, value: float | None) -> None:
        self._nbbo.ask = None if value is None else float(value)
        self._nbbo.satisfy_pair_for_quoting()

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib):
        """IB callback: stage bid/ask ticks for NBBO coalescing."""
        if not self.is_nbbo_market_data_req(reqId):
            return
        self.note_market_data_tick()
        self._nbbo.stage_tick_price(int(tickType), float(price))

    def tickSize(self, reqId: TickerId, tickType: TickType, size: Decimal) -> None:
        """IB callback: count size ticks for market-data stall recovery."""
        if not self.is_nbbo_market_data_req(reqId):
            return
        self.note_market_data_tick()

    def _on_coalesced_nbbo(self, bid: float, ask: float) -> None:
        """Update ``ref_price`` from coalesced bid/ask mid."""
        if self._has_valid_nbbo():
            self.maybe_sync_orders_from_nbbo()

    def _flush_pending_nbbo(self) -> None:
        """Apply staged bid/ask immediately (tests and manual flush)."""
        self._nbbo.flush_commit()

    def startup(self) -> None:
        """Subscribe to market data and request startup snapshots (from ``nextValidId``)."""
        self._startup_open_orders_logged = False
        self._nbbo.reset()
        self._market_data_subscribed = False
        self.request_positions_snapshot()
        self.request_today_open_or_prior_close()
        self.request_contract_details()
        self.request_open_orders_snapshot()
        self.logger.info(
            "Startup: requested positions, daily bars, market data, and open orders."
        )

    def openOrder(self, orderId, contract, order, orderState):
        """IB callback: track working orders for this symbol."""
        if contract.symbol == self.config["symbol"] and contract.secType == self.config["sec_type"]:
            if not open_order_belongs_to_client(order, self.ib_client_id):
                return
            try:
                qty = int(float(order.totalQuantity))
            except (TypeError, ValueError):
                qty = None
            try:
                lmt = float(order.lmtPrice)
                aux = float(getattr(order, "auxPrice", 0.0) or 0.0)
            except (TypeError, ValueError):
                lmt = float("nan")
                aux = 0.0
            if lmt == lmt:  # not NaN
                self.order_working_lmt[orderId] = lmt
                self.order_working_aux[orderId] = aux
            st = getattr(orderState, "status", "") or ""
            line = (
                f"  id={orderId} {order.action} {order.orderType} "
                f"totalQty={order.totalQuantity} lmtPrice={order.lmtPrice} "
                f"auxPrice={getattr(order, 'auxPrice', '')} status={st}"
            )
            self._snapshot_open_order_lines.append(line)
            if order.action == "BUY":
                self.open_symbol_buys += 1
                self.buy_order_id = orderId
                self.buy_order_qty = qty
            elif order.action == "SELL":
                self.open_symbol_sells += 1
                self.sell_order_id = orderId
                self.sell_order_qty = qty

    def on_open_orders_snapshot_start(self) -> None:
        """Reset open-order counters and peg_primary caches."""
        super().on_open_orders_snapshot_start()
        self._snapshot_open_order_lines = []
        self.order_working_lmt.clear()
        self.order_working_aux.clear()

    def on_open_orders_snapshot_end(self) -> None:
        """Mark open orders snapshot done and maybe sync."""
        if not self._startup_open_orders_logged:
            sym = self.config["symbol"]
            if self._snapshot_open_order_lines:
                self.logger.info(
                    f"Open orders at startup for {sym} ({self.config['sec_type']}): "
                    f"{len(self._snapshot_open_order_lines)}"
                )
                for ln in self._snapshot_open_order_lines:
                    self.logger.info(ln)
            else:
                self.logger.info(
                    f"Open orders at startup for {sym} ({self.config['sec_type']}): none"
                )
            self._startup_open_orders_logged = True
        super().on_open_orders_snapshot_end()

    def position(self, account, contract, pos, avgCost):
        """IB callback: reconcile IB position snapshot with ledger."""
        if contract.symbol == self.config["symbol"] and contract.secType == self.config["sec_type"]:
            handle_ledger_ib_position(
                self.ledger,
                account,
                int(pos),
                float(avgCost),
                contract=contract,
                logger=self.logger,
            )
            sync_attrs_from_ledger(
                self.ledger, self, qty_attr="position_size", avg_attr="avg_cost"
            )

    def us_regular_hours(self) -> bool:
        """True during configured regular session hours."""
        return regular_session_open(self.config)

    def _has_valid_nbbo(self) -> bool:
        """True when bid/ask are paired since reset and form a valid NBBO."""
        return self._nbbo.is_ready_for_quoting()

    def _nbbo_mid_rounded(self) -> float:
        """NBBO mid rounded to :attr:`_digits`. Caller must ensure :meth:`_has_valid_nbbo`."""
        return nbbo_mid_rounded(float(self._bid), float(self._ask), self.config)

    def adjusted_rel_limits(self) -> tuple[float, float]:
        """REL buy/sell ``lmtPrice`` from NBBO mid and ``mid_delta``.

        ``buy_limit = round(mid - d, digits)``, ``sell_limit = round(mid + d, digits)``
        with ``mid`` from :meth:`_nbbo_mid_rounded` and ``d = max(mid_delta, _tick)``
        so the buy cap stays strictly below ``mid`` and the sell floor strictly
        above ``mid``. Caller must ensure :meth:`_has_valid_nbbo` is true.
        """
        assert self._has_valid_nbbo()
        buy_limit, sell_limit = self_trade_limits_from_nbbo(
            float(self._bid), float(self._ask), self.config
        )
        sell_limit = clamp_sell_to_avg_cost_floor(
            sell_limit, self.avg_cost, self.config
        )
        return buy_limit, sell_limit

    def maybe_sync_orders_from_nbbo(self, *, force: bool = False) -> None:
        """Re-peg working REL orders when NBBO moves (API callback thread)."""
        if self.shutdown_flag:
            return
        if not self.us_regular_hours():
            return
        if not self.ready_for_trading or not self._has_valid_nbbo():
            return

        nbbo_key = (float(self._bid), float(self._ask))
        if not self._nbbo_throttle.should_run(nbbo_key, force=force):
            return

        self._nbbo_throttle.mark_ran(nbbo_key)

        mid = self._nbbo_mid_rounded()
        if mid != self.ref_price:
            self.logger.info(
                "ref_price updated: bid=%s ask=%s mid=%s",
                self._bid,
                self._ask,
                mid,
            )
            self.ref_price = mid

        self.sync_orders()

    def _clear_order_state(self, action: str) -> None:
        """Clear cached ids and counts for one order side."""
        if action == "BUY":
            oid = self.buy_order_id
            self.pending_buy = False
            self.buy_order_id = None
            self.buy_order_qty = None
            self.open_symbol_buys = 0
        else:
            oid = self.sell_order_id
            self.pending_sell = False
            self.sell_order_id = None
            self.sell_order_qty = None
            self.open_symbol_sells = 0
        if oid is not None:
            self.order_working_lmt.pop(oid, None)
            self.order_working_aux.pop(oid, None)

    def _maybe_resize_existing(
        self,
        *,
        action: str,
        oid: int,
        current_total_qty: int,
        desired_remaining: int,
        limit_price: float,
    ) -> bool:
        """Reconcile an existing working order to ``desired_remaining`` shares.

        IB partial fills change the position without changing the same-side
        order's working size (its ``remaining`` already reflects the desired
        target), but the opposite-side order's ``totalQuantity`` becomes
        stale. When the working ``remaining`` no longer matches what we
        want, modify the order in place via ``placeOrder`` with the same
        ``orderId`` (IB treats this as an amendment), preserving fills.

        Returns ``True`` when any side-effect (modify/cancel) was issued.
        """
        plan = plan_working_order_reconcile(
            self.remaining_by_order.get(oid),
            current_total_qty=current_total_qty,
            desired_remaining=desired_remaining,
            min_order_size=self.min_order_size,
        )
        if plan.kind == "noop":
            return False

        if plan.kind == "cancel":
            self.logger.info(
                f"Cancelling {action} id={oid} (desired remaining "
                f"{desired_remaining} < min_order_size {self.min_order_size}; "
                f"filled={plan.filled_now})"
            )
            safe_cancel_order(self, oid)
            self._clear_order_state(action)
            return True

        self.logger.info(
            f"Modifying {action} id={oid} totalQty {current_total_qty}->{plan.new_total_qty} "
            f"(filled={plan.filled_now} remaining {plan.remaining_now}->{desired_remaining} "
            f"lmtPrice={limit_price})"
        )
        order = self.build_rel_order(action, plan.new_total_qty, limit_price)
        self.placeOrder(oid, self.contract, order)
        self._note_order_cache(oid, order)
        if action == "BUY":
            self.buy_order_qty = plan.new_total_qty
        else:
            self.sell_order_qty = plan.new_total_qty
        return True

    def _note_order_cache(self, oid: int, order: Order) -> None:
        """Remember working lmt/aux for amend detection."""
        try:
            self.order_working_lmt[oid] = float(order.lmtPrice)
            self.order_working_aux[oid] = float(getattr(order, "auxPrice", 0.0) or 0.0)
        except (TypeError, ValueError):
            pass

    def _maybe_update_order_price(
        self, action: str, oid: int, total_qty: int, limit_price: float
    ) -> bool:
        """Amend working ``lmtPrice`` / ``auxPrice`` when NBBO-derived limits move but size is unchanged."""
        if oid not in self.order_working_lmt or oid not in self.order_working_aux:
            return False
        if self._has_valid_nbbo():
            if action == "BUY":
                limit_price = clamp_buy_to_avoid_self_trade(
                    limit_price,
                    self.config,
                    bid=self._bid,
                    ask=self._ask,
                )
            else:
                limit_price = clamp_sell_to_avoid_self_trade(
                    limit_price,
                    self.config,
                    bid=self._bid,
                    ask=self._ask,
                )
        new_order = self.build_rel_order(action, total_qty, limit_price)
        new_l = float(new_order.lmtPrice)
        new_a = float(new_order.auxPrice)
        old_l = float(self.order_working_lmt[oid])
        old_a = float(self.order_working_aux[oid])
        if round(old_l, self._digits) == round(new_l, self._digits) and round(
            old_a, self._digits
        ) == round(new_a, self._digits):
            return False
        mid = self._nbbo_mid_rounded()
        self.logger.info(
            f"Updating {action} id={oid} lmtPrice {old_l}->{new_l} "
            f"auxPrice {old_a}->{new_a} (totalQty={total_qty} "
            f"bid={self._bid} ask={self._ask} mid={mid})"
        )
        self.placeOrder(oid, self.contract, new_order)
        self._note_order_cache(oid, new_order)
        return True

    def sync_orders(self):
        """Place, cancel, or amend quotes to match position and limits."""
        if self.shutdown_flag or not self.ready_for_trading:
            return

        if self.nextOrderId is None:
            return

        if not self._has_valid_nbbo():
            return

        pos = self.position_size
        max_pos = int(self.config["max_pos"])

        buy_limit, sell_limit = self.adjusted_rel_limits()

        if pos <= 0:
            if self.sell_order_id is not None:
                self.logger.info(
                    "Cancelling SELL order id=%s (flat or flat target)",
                    self.sell_order_id,
                )
                sid = self.sell_order_id
                safe_cancel_order(self, sid)
                self.pending_sell = False
                self.sell_order_id = None
                self.sell_order_qty = None
                self.open_symbol_sells = 0
                self.order_working_lmt.pop(sid, None)
                self.order_working_aux.pop(sid, None)
                return

            if self.open_symbol_sells > 0:
                return

            buy_qty = max_pos
            if self.buy_order_id is not None and self.buy_order_qty is not None:
                oid = self.buy_order_id
                qty = self.buy_order_qty
                resized = self._maybe_resize_existing(
                    action="BUY",
                    oid=oid,
                    current_total_qty=qty,
                    desired_remaining=buy_qty,
                    limit_price=buy_limit,
                )
                if not resized:
                    self._maybe_update_order_price("BUY", oid, qty, buy_limit)
            elif (
                self._meets_min_order_size(buy_qty)
                and self.open_symbol_buys == 0
                and not self.pending_buy
            ):
                oid = self.nextOrderId
                self.nextOrderId += 1
                self.buy_order_id = oid
                self.buy_order_qty = buy_qty
                self.pending_buy = True
                order = self.build_rel_order("BUY", buy_qty, buy_limit)
                off = order.auxPrice
                self.logger.info(
                    "Placing REL (peg primary) BUY qty=%s lmtPrice=%s auxPrice=%s id=%s",
                    buy_qty,
                    buy_limit,
                    off,
                    oid,
                )
                self.placeOrder(oid, self.contract, order)
                self._note_order_cache(oid, order)
            return

        if pos >= max_pos:
            if self.buy_order_id is not None:
                self.logger.info(
                    "Cancelling BUY order id=%s (at or above max_pos)",
                    self.buy_order_id,
                )
                buy_oid = self.buy_order_id
                safe_cancel_order(self, buy_oid)
                self.pending_buy = False
                self.buy_order_id = None
                self.buy_order_qty = None
                self.open_symbol_buys = 0
                self.order_working_lmt.pop(buy_oid, None)
                self.order_working_aux.pop(buy_oid, None)
                return

            if self.open_symbol_buys > 0:
                return

            sell_qty = max_sell_shares(self.ledger)
            if self.sell_order_id is not None and self.sell_order_qty is not None:
                oid = self.sell_order_id
                qty = self.sell_order_qty
                resized = self._maybe_resize_existing(
                    action="SELL",
                    oid=oid,
                    current_total_qty=qty,
                    desired_remaining=sell_qty,
                    limit_price=sell_limit,
                )
                if not resized:
                    self._maybe_update_order_price("SELL", oid, qty, sell_limit)
            elif (
                self._meets_min_order_size(sell_qty)
                and self.open_symbol_sells == 0
                and not self.pending_sell
            ):
                oid = self.nextOrderId
                self.nextOrderId += 1
                self.sell_order_id = oid
                self.sell_order_qty = sell_qty
                self.pending_sell = True
                order = self.build_rel_order("SELL", sell_qty, sell_limit)
                off = order.auxPrice
                self.logger.info(
                    "Placing REL (peg primary) SELL qty=%s lmtPrice=%s auxPrice=%s id=%s",
                    sell_qty,
                    sell_limit,
                    off,
                    oid,
                )
                self.placeOrder(oid, self.contract, order)
                self._note_order_cache(oid, order)
            return

        # 0 < pos < max_pos: quote both add (room to max_pos) and exit (current position).
        buy_qty = max_pos - pos
        sell_qty = max_sell_shares(self.ledger)

        if self.buy_order_id is not None and self.buy_order_qty is not None:
            oid = self.buy_order_id
            qty = self.buy_order_qty
            resized = self._maybe_resize_existing(
                action="BUY",
                oid=oid,
                current_total_qty=qty,
                desired_remaining=buy_qty,
                limit_price=buy_limit,
            )
            if not resized:
                self._maybe_update_order_price("BUY", oid, qty, buy_limit)
        elif (
            self._meets_min_order_size(buy_qty)
            and self.open_symbol_buys == 0
            and not self.pending_buy
        ):
            oid = self.nextOrderId
            self.nextOrderId += 1
            self.buy_order_id = oid
            self.buy_order_qty = buy_qty
            self.pending_buy = True
            order = self.build_rel_order("BUY", buy_qty, buy_limit)
            off = order.auxPrice
            self.logger.info(
                "Placing REL (peg primary) BUY qty=%s lmtPrice=%s auxPrice=%s id=%s",
                buy_qty,
                buy_limit,
                off,
                oid,
            )
            self.placeOrder(oid, self.contract, order)
            self._note_order_cache(oid, order)

        if self.sell_order_id is not None and self.sell_order_qty is not None:
            oid = self.sell_order_id
            qty = self.sell_order_qty
            resized = self._maybe_resize_existing(
                action="SELL",
                oid=oid,
                current_total_qty=qty,
                desired_remaining=sell_qty,
                limit_price=sell_limit,
            )
            if not resized:
                self._maybe_update_order_price("SELL", oid, qty, sell_limit)
        elif (
            self._meets_min_order_size(sell_qty)
            and self.open_symbol_sells == 0
            and not self.pending_sell
        ):
            oid = self.nextOrderId
            self.nextOrderId += 1
            self.sell_order_id = oid
            self.sell_order_qty = sell_qty
            self.pending_sell = True
            order = self.build_rel_order("SELL", sell_qty, sell_limit)
            off = order.auxPrice
            self.logger.info(
                "Placing REL (peg primary) SELL qty=%s lmtPrice=%s auxPrice=%s id=%s",
                sell_qty,
                sell_limit,
                off,
                oid,
            )
            self.placeOrder(oid, self.contract, order)
            self._note_order_cache(oid, order)

    def run_until_shutdown(self) -> None:
        """Main thread: session logging, periodic snapshot refresh, disconnect detection."""
        last_periodic = 0.0
        interval = self.nbbo_sync_interval_seconds

        while not self.shutdown_flag:
            if not self.isConnected():
                if self.api_ready:
                    self.logger.warning(
                        "Lost connection to IBKR (check TWS/Gateway and API client settings)"
                    )
                else:
                    self.logger.error(
                        "Never received nextValidId from IBKR. "
                        "Check that TWS/IB Gateway is running, API is enabled, "
                        "host/port match config (paper often 7497), and client_id=%s "
                        "is not already in use.",
                        self.config.get("client_id", 1),
                    )
                break

            now = time.monotonic()
            in_hours = self.us_regular_hours()
            log_session_transition(
                self.logger,
                self.config,
                prev_in_hours=self._prev_us_regular_hours,
                in_hours=in_hours,
            )
            self._prev_us_regular_hours = in_hours

            if in_hours and (now - last_periodic) >= interval:
                last_periodic = now
                self.trigger_resync()

            self.maybe_recover_stalled_market_data()
            if self._market_data_subscribed and not self._nbbo.is_ready_for_quoting():
                self.maybe_watchdog_recover_market_data(nbbo_ok=False)
            time.sleep(1.0)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the argparse parser for this bot script."""
    parser = argparse.ArgumentParser(
        description="IBKR Pegged-to-Primary (REL) quoter — NBBO pegging is exchange-side"
    )
    add_config_argument(parser, __file__)
    add_ib_connection_arguments(parser)

    parser.add_argument("--max_pos", type=int)
    parser.add_argument("--loop_seconds", type=float)
    parser.add_argument("--min_order_size", type=int)
    add_session_hours_arguments(parser)
    parser.add_argument("--tif")
    parser.add_argument("--price_round_digits", type=int)
    parser.add_argument("--resync_debounce_seconds", type=float)
    parser.add_argument(
        "--nbbo_coalesce_seconds",
        type=float,
        help="Debounce bid/ask ticks: commit latest NBBO after this quiet period (default: resync_debounce_seconds)",
    )
    parser.add_argument(
        "--nbbo_coalesce_max_seconds",
        type=float,
        help="Force NBBO commit at least this often during continuous ticks (default: 1.0)",
    )
    parser.add_argument(
        "--mid_delta",
        type=float,
        help="Half-width around NBBO mid for REL caps (default 0.02; raised to >= one price tick)",
    )
    parser.add_argument(
        "--offset_pct",
        type=float,
        help="Fraction of NBBO spread for REL auxPrice only (effective max < 0.5)",
    )
    parser.add_argument(
        "--cancel_open_orders_on_shutdown",
        action=argparse.BooleanOptionalAction,
        help="Cancel tracked BUY/SELL quotes before disconnect (default: false)",
    )
    add_never_sell_below_avg_cost_argument(parser)

    add_logging_arguments(parser)
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
            "max_pos",
            "loop_seconds",
            "offset_pct",
            "market_timezone",
            "market_open_hour",
            "market_open_minute",
            "market_close_hour",
            "mid_delta",
            "tif",
            "price_round_digits",
            "ignored_error_codes",
            "ignore_error_substrings",
        ],
    )

    app = Trader(config)
    exit_code = run_bot(
        app,
        config,
        is_ready=lambda: app.api_ready,
        main_loop=app.run_until_shutdown,
        ready_label="nextValidId",
    )
    if exit_code != 0:
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
