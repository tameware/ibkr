#!/usr/bin/env python3

from __future__ import annotations

import argparse
import logging
import logging.handlers
import math
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional, Tuple, Union

from ibapi.common import OrderId, TickerId
from ibapi.contract import Contract
from ibapi.order import Order

from ibkr_app_support import (
    PositionLedger,
    add_config_argument,
    add_ib_connection_arguments,
    add_logging_arguments,
    add_never_sell_below_avg_cost_argument,
    add_session_hours_arguments,
    cfg_bool as _cfg_bool,
    execution_belongs_to_client,
    execution_fill_tuple,
    flatten_config_sections,
    has_valid_nbbo,
    handle_ledger_ib_position,
    is_valid_quote_pair,
    NbboCoalesceSink,
    ib_client_id_from_config,
    idle_until_shutdown,
    clamp_buy_to_avoid_self_trade,
    clamp_quote_prices_to_avoid_self_trade,
    clamp_sell_to_avg_cost_floor,
    clamp_sell_to_avoid_self_trade,
    load_merged_config,
    log_session_transition,
    max_sell_shares,
    NbboThrottle,
    open_order_belongs_to_client,
    order_status_clients_match,
    regular_session_open,
    run_bot,
    safe_cancel_order,
    sync_attrs_from_ledger,
)
from ibkr_bot_base import ContractResolutionMixin, IbkrBotApp


@dataclass
class QuoteState:
    bid: Optional[float] = None
    ask: Optional[float] = None
    bid_size: int = 0
    ask_size: int = 0
    last_update_ts: float = 0.0


@dataclass
class LiveOrder:
    order_id: int
    side: str
    price: float
    qty: int
    status: str = "PendingSubmit"
    filled: int = 0
    remaining: int = 0
    total_qty: int = 0


@dataclass(frozen=True)
class QuoteMgmtSnapshot:
    """Point-in-time state for one quote-management cycle (built under ``self.lock``)."""

    connected_flag: bool
    shutdown_flag: bool
    open_orders_snapshot_done: bool
    position_size: int
    gross_shares_traded: int
    avg_cost: float
    buy_shares_filled: int
    max_sell: int
    last_quote_eval: float
    last_nbbo_snap: Optional[Tuple[Optional[float], Optional[float], int, int]]
    quote_bid: Optional[float]
    quote_ask: Optional[float]
    quote_bid_sz: int
    quote_ask_sz: int
    quote_luts: float
    buy_work_px: Optional[float]
    sell_work_px: Optional[float]
    sell_snap: Optional[Tuple[str, int, int]]


@dataclass(frozen=True)
class QuoteDecision:
    buy_qty: int
    buy_px: float
    sell_qty: int
    sell_px: float


@dataclass(frozen=True)
class QuotePipelineInvalidPair:
    """Desired bid/ask pair is unusable; ``after_compute`` selects the warning template."""

    after_compute: bool
    buy_px: Optional[float]
    sell_px: Optional[float]


class MarketMaker(ContractResolutionMixin, IbkrBotApp):
    """Two-sided limit quoter with NBBO tick-improve and inventory skew."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize :class:`MarketMaker`."""
        super().__init__(
            config,
            logger_name="market_maker",
            default_log_file="market_maker.log",
        )

        self.host = str(config.get("host", "127.0.0.1"))
        self.port = int(config.get("port", 7496))
        self.client_id = ib_client_id_from_config(config, default=2)
        raw_acct = config.get("account", "") or ""
        self.account_filter = str(raw_acct).strip() or None

        self.base_qty = int(config.get("base_qty", 100))
        self.max_position = int(config.get("max_position", 1000))
        self.max_gross_shares = int(config.get("max_gross_shares", 1500))
        self.min_inventory_before_offering = int(
            config.get("min_inventory_before_offering", 100)
        )
        self.min_spread = float(config.get("min_spread", 0.20))
        self.inside_improve = float(config.get("inside_improve", 0.10))
        self.inventory_penalty_per_100 = float(
            config.get("inventory_penalty_per_100", 0.05)
        )
        self.target_roundtrip_capture = float(
            config.get("target_roundtrip_capture", 0.30)
        )
        self.quote_refresh_seconds = float(config.get("quote_refresh_seconds", 3.0))
        self._nbbo_throttle = NbboThrottle(
            self.quote_refresh_seconds,
            clock=time.time,
        )
        self.max_market_stale_seconds = float(
            config.get("max_market_stale_seconds", 15.0)
        )
        _mbd = config.get("max_buy_shares_per_run")
        self.max_buy_shares_per_run: Optional[int] = (
            None if _mbd is None else int(_mbd)
        )

        self.cancel_on_invalid_market = _cfg_bool(
            config, "cancel_on_invalid_market", True
        )
        self.require_live_data = _cfg_bool(config, "require_live_data", False)

        self.log_bid_ask_ticks = _cfg_bool(config, "log_bid_ask_ticks", False)
        self.log_invalid_market_reasons = _cfg_bool(
            config, "log_invalid_market_reasons", False
        )
        self.nbbo_watchdog_seconds = float(
            config.get("nbbo_watchdog_seconds", 30.0)
        )
        self.watchdog_repeat_seconds = float(
            config.get("watchdog_repeat_seconds", 60.0)
        )
        self.min_quote_spread_cents = float(
            config.get("min_quote_spread_cents", 50.0)
        )
        self.min_tick = float(config.get("min_tick", 0.01))

        self.lock = threading.RLock()
        self.connected_flag = False
        self.started = False

        self.next_order_id: Optional[int] = None
        self.market_data_req_id: TickerId = 1001
        self.contract_details_req_id: int = 2001
        self._init_contract_resolution(
            config,
            contract_details_req_id=self.contract_details_req_id,
        )

        self.quote = QuoteState()
        self._nbbo = NbboCoalesceSink(
            self.config, self._on_coalesced_nbbo, logger=self.logger
        )
        self.position_size = 0
        self.avg_cost = 0.0
        self.account = None
        self.contract_resolved = False
        self.market_data_type = None

        self.buy_order: Optional[LiveOrder] = None
        self.sell_order: Optional[LiveOrder] = None

        self.gross_shares_traded = 0
        self.realized_pnl = 0.0
        self.buy_shares_filled = 0

        self.last_quote_eval = 0.0
        self._last_quote_decision_log_key: Optional[
            Tuple[
                Optional[float],
                Optional[float],
                int,
                int,
                int,
                float,
                int,
                Optional[float],
                int,
                Optional[float],
            ]
        ] = None
        self._last_tick_size_log_key: Optional[
            Tuple[int, int, Optional[float], Optional[float], int, int]
        ] = None
        self.start_time = time.time()
        self.last_nbbo_ok_ts = 0.0
        self.last_watchdog_log_ts = 0.0

        self.target_conid: Optional[int] = None
        self.open_orders_snapshot_done = False
        self.adoption_phase = False
        self._prev_us_regular_hours: bool | None = None
        self._last_order_update_log_key: Dict[int, Tuple[int, int, int, float]] = {}

        self.ledger = PositionLedger.open(
            "market_maker", config, client_id=self.client_id, account=raw_acct
        )
        sync_attrs_from_ledger(
            self.ledger,
            self,
            qty_attr="position_size",
            avg_attr="avg_cost",
            clamp_qty_nonneg=True,
        )
        self.logger.info(
            "market_maker %s %s/%s client_id=%s ledger_qty=%s avg=%.4f",
            self.contract.symbol,
            self.contract.exchange,
            self.contract.primaryExchange,
            self.client_id,
            self.ledger.qty,
            self.ledger.avg_cost_per_share,
        )

    def market_data_req_ids(self):
        """Market data request ids to cancel on shutdown."""
        return (self.market_data_req_id,)

    def assign_next_order_id(self, order_id: int) -> None:
        """Store the next usable order id from IB."""
        with self.lock:
            self.next_order_id = order_id

    def on_api_ready(self, order_id: int) -> None:
        """Mark connected and run ``startup`` on first ``nextValidId``."""
        with self.lock:
            self.connected_flag = True
        self.logger.info("Connected to IBKR. nextValidId=%s", order_id)
        if not self.started:
            self.started = True
            self.startup()

    def on_connection_closed(self) -> None:
        """Clear ``connected_flag`` when IB closes the socket."""
        with self.lock:
            self.connected_flag = False

    def _mark_shutdown(self) -> None:
        """Set ``shutdown_flag`` for the main loop."""
        with self.lock:
            self.shutdown_flag = True

    def shutdown_quotes(self) -> None:
        """Cancel working quotes and clear local order tracking."""
        self._nbbo.cancel()
        if self.cancel_open_orders_on_shutdown:
            if self.buy_order:
                self.cancel_live_order(self.buy_order)
            if self.sell_order:
                self.cancel_live_order(self.sell_order)
        else:
            self.logger.info(
                "Leaving open orders at IBKR (cancel_open_orders_on_shutdown=false)"
            )

    def marketDataType(self, reqId, marketDataType):
        """IB callback: record live/delayed data type."""
        if not self.is_nbbo_market_data_req(reqId):
            return
        self.market_data_type = marketDataType
        self.logger.debug(
            "Market data type update reqId=%s type=%s", reqId, marketDataType
        )
        if self.require_live_data and marketDataType != 1:
            self.logger.warning("Live data required, but market data type is %s", marketDataType)

    def startup(self):
        """Subscribe to market data and request startup snapshots."""
        self._nbbo.reset()
        self._market_data_subscribed = False
        with self.lock:
            self.open_orders_snapshot_done = False
            self.adoption_phase = True
            self.buy_order = None
            self.sell_order = None

        self.request_contract_details()
        self.reqPositions()

        if self.client_id == 0:
            self.reqOpenOrders()          # binds currently open manual/TWS orders
            self.reqAutoOpenOrders(True)  # binds future manual/TWS orders
        else:
            self.reqAllOpenOrders()       # visibility only, not cancelable if id=0

        self.logger.debug(
            "Requested contract details, positions, open orders, and live market data."
        )

    def on_contract_resolved(self, contractDetails) -> None:
        """Record min tick and target conId after IB resolves the contract."""
        summary = contractDetails.contract
        mt = getattr(contractDetails, "minTick", None)
        new_min_tick: Optional[float] = None
        if mt is not None:
            try:
                v = float(mt)
                if v > 0:
                    new_min_tick = v
            except (TypeError, ValueError):
                pass

        with self.lock:
            self.contract_resolved = True
            self.target_conid = getattr(summary, "conId", None)
            if new_min_tick is not None:
                self.min_tick = new_min_tick

        self.logger.debug(
            "Resolved contract: conId=%s symbol=%s localSymbol=%s exchange=%s primaryExchange=%s currency=%s minTick=%s",
            getattr(summary, "conId", None),
            getattr(summary, "symbol", None),
            getattr(summary, "localSymbol", None),
            getattr(summary, "exchange", None),
            getattr(summary, "primaryExchange", None),
            getattr(summary, "currency", None),
            getattr(contractDetails, "minTick", None),
        )

    def _is_target_contract(self, contract) -> bool:
        """True if contract matches configured symbol/secType."""
        if getattr(contract, "secType", None) != self.contract.secType:
            return False
        if getattr(contract, "currency", None) != self.contract.currency:
            return False

        if self.target_conid is not None and getattr(contract, "conId", None):
            return contract.conId == self.target_conid

        return (
            getattr(contract, "symbol", None) == self.contract.symbol
            and getattr(contract, "primaryExchange", None) == self.contract.primaryExchange
        )

    def _should_keep_order(self, incumbent: LiveOrder, candidate: LiveOrder) -> bool:
        """True if open order should be adopted by this bot."""
        if incumbent.remaining > 0 and candidate.remaining <= 0:
            return True
        if candidate.remaining > 0 and incumbent.remaining <= 0:
            return False

        active_statuses = {"PreSubmitted", "PendingSubmit", "Submitted"}
        incumbent_active = incumbent.status in active_statuses
        candidate_active = candidate.status in active_statuses
        if incumbent_active and not candidate_active:
            return True
        if candidate_active and not incumbent_active:
            return False

        return incumbent.order_id < candidate.order_id

    def _cancel_adopted_extra(self, live: LiveOrder):
        """Cancel a duplicate adopted order that lost the adoption tie-break."""
        try:
            safe_cancel_order(self, live.order_id)
            self.logger.warning(
                "Cancelling extra adopted order id=%s side=%s px=%.2f qty=%s status=%s",
                live.order_id,
                live.side,
                live.price,
                live.qty,
                live.status,
            )
        except Exception as e:
            self.logger.exception(
                "Failed to cancel extra adopted order id=%s side=%s: %s",
                live.order_id,
                live.side,
                e,
            )

    def _on_coalesced_nbbo(self, bid: float, ask: float) -> None:
        """Update ``ref_price`` from coalesced bid/ask mid."""
        log_msg = None
        with self.lock:
            self.quote.bid = bid
            self.quote.ask = ask
            self._nbbo.satisfy_pair_for_quoting()
            now = time.time()
            self.quote.last_update_ts = now
            self.last_nbbo_ok_ts = now
            if self.log_bid_ask_ticks:
                log_msg = (
                    f"NBBO coalesced bid={bid:.2f} ask={ask:.2f} "
                    f"bid_sz={self.quote.bid_size} ask_sz={self.quote.ask_size}"
                )
        if log_msg:
            self.logger.info(log_msg)
        self.maybe_manage_quotes()

    def tickPrice(self, reqId, tickType, price, attrib):
        """IB callback: stage bid/ask ticks for NBBO coalescing."""
        if not self.is_nbbo_market_data_req(reqId):
            return
        self.note_market_data_tick()
        self._nbbo.stage_tick_price(int(tickType), float(price))

    def tickSize(self, reqId, tickType, size):
        """IB callback: update bid/ask size from market data."""
        if not self.is_nbbo_market_data_req(reqId):
            return
        self.note_market_data_tick()

        log_msg: Optional[str] = None
        updated = False
        with self.lock:
            if tickType == 0:
                self.quote.bid_size = int(size)
                updated = True
            elif tickType == 3:
                self.quote.ask_size = int(size)
                updated = True
            else:
                return

            # Size-only ticks still mean live NBBO data; refresh freshness for staleness / watchdog.
            now = time.time()
            self.quote.last_update_ts = now
            if (
                self.quote.bid is not None
                and self.quote.ask is not None
                and self.quote.bid < self.quote.ask
            ):
                self.last_nbbo_ok_ts = now

            if self.log_bid_ask_ticks:
                key = (
                    tickType,
                    int(size),
                    None
                    if self.quote.bid is None
                    else round(float(self.quote.bid), 6),
                    None
                    if self.quote.ask is None
                    else round(float(self.quote.ask), 6),
                    self.quote.bid_size,
                    self.quote.ask_size,
                )
                if key != self._last_tick_size_log_key:
                    self._last_tick_size_log_key = key
                    log_msg = (
                        f"NBBO size tick tickType={tickType} size={int(size)} "
                        f"bid={self.quote.bid} ask={self.quote.ask} "
                        f"bid_sz={self.quote.bid_size} ask_sz={self.quote.ask_size}"
                    )

        if log_msg:
            self.logger.info(log_msg)

        if updated:
            self.maybe_manage_quotes()

    def position(self, account, contract, position, avgCost):
        """IB callback: reconcile IB position snapshot with ledger."""
        if contract.symbol != self.contract.symbol or contract.secType != self.contract.secType:
            return
        if self.account_filter and account != self.account_filter:
            return

        with self.lock:
            self.account = account
            handle_ledger_ib_position(
                self.ledger,
                account,
                int(position),
                float(avgCost),
                contract=contract,
                logger=self.logger,
            )
            sync_attrs_from_ledger(
                self.ledger,
                self,
                qty_attr="position_size",
                avg_attr="avg_cost",
                clamp_qty_nonneg=True,
            )

        self.logger.debug(
            "Position update account=%s symbol=%s ledger_qty=%s "
            "ledger_avg_cost_per_share=%.4f ib_snapshot_qty=%s",
            account,
            contract.symbol,
            self.position_size,
            self.avg_cost,
            self.ledger.ib_snapshot_qty,
        )

    def positionEnd(self):
        """IB callback: position snapshot complete."""
        self.logger.debug("Initial position snapshot complete.")

    @staticmethod
    def _sell_working_remaining_from_ib(
        total_qty: int, filled: int, raw_remaining: Optional[int]
    ) -> int:
        """Shares still open on a sell, from IB ``openOrder`` / ``orderStatus`` fields.

        IB sometimes sets ``remaining`` equal to ``totalQuantity`` before ``filled`` is
        applied; prefer ``totalQuantity - filled`` when that yields a smaller value.
        """
        total = max(0, int(total_qty))
        filled_i = max(0, int(filled))
        implied = max(0, total - filled_i)
        if raw_remaining is None:
            return implied
        raw = max(0, int(raw_remaining))
        if filled_i > 0 and raw > implied:
            return implied
        if raw > total:
            return implied
        return raw

    def _log_order_qty_update_if_changed(
        self,
        order_id: int,
        side: str,
        *,
        prior_working: int,
        working: int,
        total_qty: int,
        filled: int,
        px: float,
    ) -> None:
        """INFO once per distinct working-size/price snapshot; DEBUG on repeats."""
        key = (int(working), int(total_qty), int(filled), round(float(px), 2))
        prev = self._last_order_update_log_key.get(int(order_id))
        if side == "SELL":
            msg = (
                "Order update id=%s side=SELL working_remaining %s->%s "
                "(totalQty=%s filled=%s) px=%.2f"
            )
            args = (order_id, prior_working, working, total_qty, filled, px)
        else:
            msg = "Order update id=%s side=%s qty %s->%s px=%.2f"
            args = (order_id, side, prior_working, working, px)
        if prev == key:
            self.logger.debug(msg, *args)
            return
        self._last_order_update_log_key[int(order_id)] = key
        self.logger.info(msg, *args)

    def openOrder(self, orderId, contract, order, orderState):
        """IB callback: track working orders for this symbol."""
        if not self._is_target_contract(contract):
            return
        if not open_order_belongs_to_client(order, self.client_id):
            return

        side = order.action.upper()
        if side not in {"BUY", "SELL"}:
            return
        if getattr(order, "orderType", "") != "LMT":
            return

        filled = int(getattr(orderState, "filled", 0) or 0)
        total_qty = int(float(order.totalQuantity))
        raw_remaining = getattr(orderState, "remaining", None)
        if side == "SELL":
            working = self._sell_working_remaining_from_ib(
                total_qty, filled, raw_remaining
            )
            live_qty = working
            remaining = working
        else:
            if raw_remaining is None:
                remaining = max(0, total_qty - filled)
            else:
                remaining = int(raw_remaining)
            live_qty = total_qty

        live = LiveOrder(
            order_id=orderId,
            side=side,
            price=float(order.lmtPrice) if getattr(order, "lmtPrice", None) is not None else 0.0,
            qty=live_qty,
            status=orderState.status,
            filled=filled,
            remaining=remaining,
            total_qty=total_qty,
        )

        if not self.adoption_phase:
            prior_qty: Optional[int] = None
            with self.lock:
                if side == "BUY" and self.buy_order and self.buy_order.order_id == orderId:
                    prior_qty = self.buy_order.qty
                    self.buy_order = live
                elif side == "SELL" and self.sell_order and self.sell_order.order_id == orderId:
                    prior_qty = self._sell_working_open_qty(self.sell_order)
                    self.sell_order = live
            if prior_qty is not None and prior_qty != live.qty:
                self._log_order_qty_update_if_changed(
                    orderId,
                    live.side,
                    prior_working=prior_qty,
                    working=live.qty,
                    total_qty=live.total_qty,
                    filled=live.filled,
                    px=live.price,
                )
            else:
                self.logger.debug(
                    "openOrder refresh id=%s side=%s working=%s totalQty=%s filled=%s "
                    "px=%.2f status=%s",
                    orderId,
                    live.side,
                    live.qty,
                    live.total_qty,
                    live.filled,
                    live.price,
                    live.status,
                )
            return

        extra_to_cancel = None

        with self.lock:
            if side == "BUY":
                incumbent = self.buy_order
                if incumbent is None:
                    self.buy_order = live
                    action = "adopted"
                elif self._should_keep_order(incumbent, live):
                    extra_to_cancel = live
                    action = "kept-incumbent-cancel-candidate"
                else:
                    extra_to_cancel = incumbent
                    self.buy_order = live
                    action = "replaced-incumbent-cancel-old"
            else:
                incumbent = self.sell_order
                if incumbent is None:
                    self.sell_order = live
                    action = "adopted"
                elif self._should_keep_sell_adoption(incumbent, live):
                    extra_to_cancel = live
                    action = "kept-incumbent-cancel-candidate"
                else:
                    extra_to_cancel = incumbent
                    self.sell_order = live
                    action = "replaced-incumbent-cancel-old"

        self.logger.debug(
            "openOrder adoption id=%s side=%s totalQty=%s filled=%s working=%s "
            "px=%.2f status=%s action=%s",
            orderId,
            live.side,
            int(order.totalQuantity),
            filled,
            live.qty,
            live.price,
            live.status,
            action,
        )

        if extra_to_cancel is not None and extra_to_cancel.order_id != live.order_id:
            self._cancel_adopted_extra(extra_to_cancel)

    def _log_session_working_order(self, label: str, live: Optional[LiveOrder]) -> None:
        if live is None:
            return
        if live.side == "SELL":
            self.logger.info(
                "Session working %s id=%s totalQty=%s filled=%s working_remaining=%s "
                "px=%.2f status=%s",
                label,
                live.order_id,
                live.total_qty or (live.filled + live.qty),
                live.filled,
                live.qty,
                live.price,
                live.status,
            )
        else:
            self.logger.info(
                "Session working %s id=%s qty=%s px=%.2f status=%s",
                label,
                live.order_id,
                live.qty,
                live.price,
                live.status,
            )

    def openOrderEnd(self):
        """IB callback: open-order snapshot complete."""
        with self.lock:
            self.open_orders_snapshot_done = True
            self.adoption_phase = False
            buy = self.buy_order
            sell = self.sell_order
        if buy is None and sell is None:
            self.logger.debug("Open-order snapshot complete (no working orders for symbol)")
            return
        self._log_session_working_order("buy", buy)
        self._log_session_working_order("sell", sell)

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
        if not order_status_clients_match(clientId, self.client_id):
            return
        with self.lock:
            target = None
            if self.buy_order and self.buy_order.order_id == orderId:
                target = self.buy_order
            elif self.sell_order and self.sell_order.order_id == orderId:
                target = self.sell_order

            if target:
                target.status = status
                target.filled = int(filled)
                if target.side == "SELL":
                    total = target.total_qty or (
                        int(target.filled) + max(int(target.qty), int(remaining))
                    )
                    working = self._sell_working_remaining_from_ib(
                        total, int(filled), int(remaining)
                    )
                    target.total_qty = total
                    target.qty = working
                    target.remaining = working
                else:
                    target.remaining = int(remaining)

                if status in {"Filled", "Cancelled", "ApiCancelled", "Inactive"} and int(
                    remaining
                ) == 0:
                    if target.side == "BUY":
                        self.buy_order = None
                    elif target.side == "SELL":
                        self.sell_order = None

        if status == "Filled":
            self.logger.info(
                "orderStatus id=%s Filled filled=%s avgFill=%.4f",
                orderId,
                filled,
                float(avgFillPrice or 0.0),
            )
        else:
            self.logger.debug(
                "orderStatus id=%s status=%s filled=%s remaining=%s avgFill=%.4f",
                orderId,
                status,
                filled,
                remaining,
                float(avgFillPrice or 0.0),
            )

    def execDetails(self, reqId, contract, execution):
        """IB callback: apply fills to the position ledger."""
        if not self._is_target_contract(contract):
            return
        if not execution_belongs_to_client(execution, self.client_id):
            return

        fill = execution_fill_tuple(execution)
        if fill is None:
            return
        side, shares, px = fill
        side_raw = str(getattr(execution, "side", "")).upper()

        with self.lock:
            if side_raw in ("SLD", "SELL") and self.ledger.qty > 0 and self.ledger.avg_cost > 0:
                sellable = min(self.ledger.qty, shares)
                self.realized_pnl += sellable * (px - self.ledger.avg_cost)

            self.gross_shares_traded += shares
            if side == "BUY":
                self.buy_shares_filled += shares

            exec_id = getattr(execution, "execId", None)
            if self.ledger.apply_fill(side, shares, px, exec_id=exec_id):
                sync_attrs_from_ledger(
                    self.ledger,
                    self,
                    qty_attr="position_size",
                    avg_attr="avg_cost",
                    clamp_qty_nonneg=True,
                )
                self.ledger.save()

        self.logger.info(
            "Fill %s %s @ %.4f pos=%s gross=%s pnl=%.2f",
            side_raw,
            shares,
            px,
            self.position_size,
            self.gross_shares_traded,
            self.realized_pnl,
        )

        self.maybe_manage_quotes(force=True)

    def next_id(self) -> int:
        """Allocate and return the next order id (thread-safe)."""
        with self.lock:
            if self.next_order_id is None:
                raise RuntimeError("next_order_id not initialized")
            oid = self.next_order_id
            self.next_order_id += 1
            return oid

    @staticmethod
    def round_down_cent(x: float) -> float:
        """Round a price down to the nearest cent."""
        return math.floor(x * 100.0) / 100.0

    @staticmethod
    def round_up_cent(x: float) -> float:
        """Round a price up to the nearest cent."""
        return math.ceil(x * 100.0) / 100.0

    def _effective_min_tick(self) -> float:
        """Minimum price increment from contract details (fallback 0.01)."""
        with self.lock:
            t = float(self.min_tick)
            return t if t > 0 else 0.01

    def _quantize_to_tick(self, x: float) -> float:
        """Round ``x`` to the nearest valid tick increment."""
        t = self._effective_min_tick()
        n = round(x / t)
        return round(n * t, 10)

    def _nbbo_tick_improve_with_nbbo(
        self,
        buy_px: float,
        sell_px: float,
        buy_qty: int,
        sell_qty: int,
        bid: float,
        ask: float,
        bid_size: int,
        ask_size: int,
    ) -> Tuple[float, float]:
        """Like ``_nbbo_tick_improve_quotes`` but uses explicit NBBO depth (thread-safe snapshot)."""
        tick = self._effective_min_tick()
        min_width = self.min_quote_spread_cents / 100.0

        bid_depth_ok = bid_size == 0 or bid_size > buy_qty
        ask_depth_ok = ask_size == 0 or ask_size > sell_qty

        if buy_qty > 0 and bid_depth_ok:
            cand_buy = self._quantize_to_tick(buy_px + tick)
            if (
                cand_buy < ask
                and cand_buy > bid
                and sell_px - cand_buy >= min_width
            ):
                buy_px = cand_buy
                self.logger.debug(
                    "NBBO tick improve BUY: bid_sz=%s buy_qty=%s -> buy_px=%.4f",
                    bid_size,
                    buy_qty,
                    buy_px,
                )

        if sell_qty > 0 and ask_depth_ok:
            cand_sell = self._quantize_to_tick(sell_px - tick)
            if (
                cand_sell > bid
                and cand_sell < ask
                and cand_sell - buy_px >= min_width
            ):
                sell_px = cand_sell
                self.logger.debug(
                    "NBBO tick improve SELL: ask_sz=%s sell_qty=%s -> sell_px=%.4f",
                    ask_size,
                    sell_qty,
                    sell_px,
                )

        return buy_px, sell_px

    def _nbbo_tick_improve_quotes(
        self,
        buy_px: float,
        sell_px: float,
        buy_qty: int,
        sell_qty: int,
    ) -> Tuple[float, float]:
        """Raise working bid / lower working ask by one minTick when NBBO size exceeds
        our quote size on that side, only if our bid–ask width stays >=
        min_quote_spread_cents (and quotes remain passive vs NBBO).

        If bid_size / ask_size are still zero (no tickSize yet from IB), treat depth as
        unknown and allow improvement so price-only NBBO streams still work."""
        bid = self.quote.bid
        ask = self.quote.ask
        if bid is None or ask is None:
            return buy_px, sell_px
        return self._nbbo_tick_improve_with_nbbo(
            buy_px,
            sell_px,
            buy_qty,
            sell_qty,
            bid,
            ask,
            self.quote.bid_size,
            self.quote.ask_size,
        )

    def build_lmt_order(self, action: str, qty: int, px: float) -> Order:
        """Build a DAY limit order at ``px``."""
        o = Order()
        o.action = action
        o.orderType = "LMT"
        o.totalQuantity = qty
        o.lmtPrice = round(px, 2)
        o.tif = "DAY"
        o.outsideRth = False
        return o

    def can_open_new_long(self) -> bool:
        """True if position/gross/per-run buy caps allow another buy."""
        with self.lock:
            if self.position_size >= self.max_position:
                return False
            if self.gross_shares_traded >= self.max_gross_shares:
                return False
            if self.max_buy_shares_per_run is not None:
                if self.buy_shares_filled >= self.max_buy_shares_per_run:
                    return False
            return True

    def max_sellable_qty(self) -> int:
        """Shares available to sell per ledger (non-negative qty)."""
        return max_sell_shares(self.ledger)

    def _market_invalid_reason_for_nbbo(
        self,
        bid: Optional[float],
        ask: Optional[float],
        last_update_ts: float,
    ) -> Optional[str]:
        """Return a reason string when NBBO is unusable for quoting, else None."""
        if bid is None:
            return "missing bid"
        if ask is None:
            return "missing ask"
        if not has_valid_nbbo(bid, ask):
            try:
                b = float(bid)
                a = float(ask)
            except (TypeError, ValueError):
                return "invalid nbbo"
            if b <= 0:
                return f"nonpositive bid {bid}"
            if a <= 0:
                return f"nonpositive ask {ask}"
            return f"crossed_or_locked bid={b:.2f} ask={a:.2f}"

        age = time.time() - last_update_ts
        if age > self.max_market_stale_seconds:
            return f"stale quote age={age:.1f}s max={self.max_market_stale_seconds:.1f}s"

        spread = ask - bid
        if spread < self.min_spread:
            return f"spread too small spread={spread:.2f} min={self.min_spread:.2f}"

        if self.require_live_data and self.market_data_type not in (None, 1):
            return f"market data type not live type={self.market_data_type}"

        return None

    def market_invalid_reason(self) -> Optional[str]:
        """Why current ``quote`` state cannot support quoting (or None if OK)."""
        if self._nbbo._awaiting_pair_after_reset:
            return "awaiting paired NBBO (bid and ask since reset)"
        return self._market_invalid_reason_for_nbbo(
            self.quote.bid,
            self.quote.ask,
            self.quote.last_update_ts,
        )

    def us_regular_hours(self) -> bool:
        """True during configured regular session hours."""
        return regular_session_open(self.config)

    def _poll_session_hours(self) -> None:
        """Log session transitions and cancel quotes when the regular session closes."""
        in_hours = self.us_regular_hours()
        log_session_transition(
            self.logger,
            self.config,
            prev_in_hours=self._prev_us_regular_hours,
            in_hours=in_hours,
        )
        if not in_hours and self._prev_us_regular_hours is not False:
            self._cancel_working_quotes_flat_inventory()
        self._prev_us_regular_hours = in_hours

    def _cancel_working_quotes_flat_inventory(self) -> None:
        """Cancel working buy; cancel sell only when flat or short (no inventory to lift)."""
        self.place_or_replace_buy(0, None)
        if self.position_size <= 0:
            self.place_or_replace_sell(0, None)

    def _invalid_market_should_cancel_working(self, reason: str) -> bool:
        """Only cancel working orders for structural invalidity, not missing NBBO at startup."""
        if not self.cancel_on_invalid_market:
            return False
        transient = (
            "missing bid",
            "missing ask",
            "awaiting paired NBBO",
        )
        return not any(reason.startswith(prefix) for prefix in transient)

    def _abort_for_market_invalid_reason(self, reason: Optional[str]) -> bool:
        """If ``reason`` is set, optionally cancel working quotes and log; return True."""
        if reason is None:
            return False
        if self._invalid_market_should_cancel_working(reason):
            self._cancel_working_quotes_flat_inventory()
        if self.log_invalid_market_reasons:
            self.logger.debug("Not quoting: %s", reason)
        return True

    def _abort_quotes_if_market_invalid(self) -> bool:
        """If the book is not quotable, optionally cancel and log; return True to skip the rest."""
        return self._abort_for_market_invalid_reason(self.market_invalid_reason())

    @staticmethod
    def _quotes_pair_is_valid(buy_px: Optional[float], sell_px: Optional[float]) -> bool:
        """Quotes pair is valid."""
        return is_valid_quote_pair(buy_px, sell_px)

    def _abort_quotes_on_invalid_pair(
        self,
        buy_px: Optional[float],
        sell_px: Optional[float],
        warning_msg: str,
    ) -> bool:
        """If the buy/sell quote pair is unusable, cancel working quotes, log, return True."""
        if self._quotes_pair_is_valid(buy_px, sell_px):
            return False
        self._cancel_working_quotes_flat_inventory()
        self.logger.warning(warning_msg, buy_px, sell_px)
        return True

    def market_is_valid(self) -> bool:
        """Market is valid."""
        return self.market_invalid_reason() is None

    def _compute_desired_quotes_for(
        self, bid: float, ask: float, position_size: int, avg_cost: float
    ) -> Tuple[Optional[float], Optional[float]]:
        """Compute desired quotes for."""
        spread = ask - bid

        buy_px = bid + min(self.inside_improve, spread * 0.25)
        sell_px = ask - min(self.inside_improve, spread * 0.25)

        long_hundreds = max(0, position_size) / 100.0
        skew = long_hundreds * self.inventory_penalty_per_100

        buy_px -= skew
        sell_px -= min(skew * 2.0, spread * 0.20)

        if position_size > 0 and avg_cost > 0:
            sell_floor = avg_cost + self.target_roundtrip_capture
            sell_px = max(sell_px, sell_floor)
        if position_size > 0:
            sell_px = clamp_sell_to_avg_cost_floor(sell_px, avg_cost, self.config)

        buy_px = min(buy_px, ask - 0.02)
        sell_px = max(sell_px, bid + 0.02)

        buy_px = self.round_down_cent(buy_px)
        sell_px = self.round_up_cent(sell_px)

        buy_px, sell_px = clamp_quote_prices_to_avoid_self_trade(
            buy_px, sell_px, self.config, bid=bid, ask=ask
        )

        if buy_px >= sell_px:
            return None, None

        return buy_px, sell_px

    def compute_desired_quotes(self):
        """Desired bid/ask prices from inventory skew and NBBO, or None if invalid."""
        bid = self.quote.bid
        ask = self.quote.ask
        if bid is None or ask is None:
            return None, None
        return self._compute_desired_quotes_for(bid, ask, self.position_size, self.avg_cost)

    def _desired_sizes_for(self, position_size: int, buy_shares_filled: int) -> Tuple[int, int]:
        """Return ``(buy_qty, sell_qty)`` for the given position and buy fill tally."""
        buy_qty = min(self.base_qty, max(0, self.max_position - position_size))
        if self.max_buy_shares_per_run is not None:
            room = max(0, self.max_buy_shares_per_run - buy_shares_filled)
            buy_qty = min(buy_qty, room)
        sell_qty = max_sell_shares(self.ledger)
        return buy_qty, sell_qty

    def desired_sizes(self):
        """Current ``(buy_qty, sell_qty)`` from live position state."""
        return self._desired_sizes_for(self.position_size, self.buy_shares_filled)

    def _sell_working_open_qty(self, live: LiveOrder) -> int:
        """Working size for a sell (prefer IB ``remaining``, else last ``qty``)."""
        if live.remaining > 0:
            return int(live.remaining)
        return int(live.qty)

    def _sell_order_total_quantity(
        self, desired_remaining: int, prior: Optional[LiveOrder]
    ) -> int:
        """IB ``totalQuantity`` for sells; amends must add shares already filled."""
        remaining = max(0, int(desired_remaining))
        if prior is None:
            return remaining
        return max(0, int(prior.filled)) + remaining

    def _needs_quote_despite_nbbo_throttle_with(
        self,
        position_size: int,
        sell_snap: Optional[Tuple[str, int, int]],
    ) -> bool:
        """Bypass NBBO throttle when inventory needs a sell quote refresh."""
        if position_size <= 0:
            return False
        if sell_snap is None:
            return True
        st, remaining, qty = sell_snap
        if st in {"Filled", "Cancelled", "ApiCancelled", "Inactive"}:
            return True
        if remaining <= 0 and st not in {
            "PreSubmitted",
            "PendingSubmit",
            "Submitted",
        }:
            return True
        open_qty = int(remaining) if remaining > 0 else int(qty)
        return open_qty < position_size

    def _needs_quote_despite_nbbo_throttle(self) -> bool:
        """If True, run a quote cycle even when bid/ask and NBBO sizes are unchanged.

        Covers: long inventory with no working sell, or a sell smaller than the position
        (e.g. after adding to the position, or stale adoption), so we place or bump size
        without waiting for an NBBO tick.
        """
        sell_snap = (
            (self.sell_order.status, self.sell_order.remaining, self.sell_order.qty)
            if self.sell_order
            else None
        )
        return self._needs_quote_despite_nbbo_throttle_with(self.position_size, sell_snap)

    def _should_keep_sell_adoption(self, incumbent: LiveOrder, candidate: LiveOrder) -> bool:
        """During ``reqOpenOrders`` adoption: prefer the sell that covers more shares."""
        pos = self.position_size
        ir = self._sell_working_open_qty(incumbent)
        cr = self._sell_working_open_qty(candidate)
        active_statuses = {"PreSubmitted", "PendingSubmit", "Submitted"}
        i_act = incumbent.status in active_statuses and ir > 0
        c_act = candidate.status in active_statuses and cr > 0
        if pos > 0 and i_act and c_act and ir != cr:
            return ir > cr
        return self._should_keep_order(incumbent, candidate)

    def is_same_order(self, live: Optional[LiveOrder], side: str, qty: int, px: float) -> bool:
        """True if working order already matches desired side, qty, and price."""
        if live is None:
            return False
        if live.side != side:
            return False

        if side == "SELL":
            live_open_qty = self._sell_working_open_qty(live)
        else:
            live_open_qty = live.qty
        if live_open_qty != qty:
            return False

        if abs(live.price - px) > 1e-9:
            return False
        if live.status in {"Cancelled", "ApiCancelled", "Inactive"}:
            return False
        return True

    def cancel_live_order(self, live: Optional[LiveOrder]):
        """Cancel a tracked working order and clear local state."""
        if live is None:
            return

        oid = live.order_id
        side = live.side
        px = live.price
        qty = live.qty

        try:
            safe_cancel_order(self, oid)

            with self.lock:
                if side == "BUY" and self.buy_order and self.buy_order.order_id == oid:
                    self.buy_order = None
                elif side == "SELL" and self.sell_order and self.sell_order.order_id == oid:
                    self.sell_order = None

            self.logger.info("Cancel order id=%s side=%s px=%.2f qty=%s", oid, side, px, qty)
        except Exception as e:
            self.logger.exception("Cancel failed for order id=%s side=%s: %s", oid, side, e)

    def _place_or_replace_lmt(
        self, side: Literal["BUY", "SELL"], qty: int, px: Optional[float]
    ) -> None:
        """Place or replace lmt."""
        if side == "BUY":
            prior = self.buy_order
        else:
            prior = self.sell_order
            qty = min(qty, self.max_sellable_qty())

        if qty <= 0 or px is None:
            if prior:
                self.cancel_live_order(prior)
            return

        bid = self.quote.bid
        ask = self.quote.ask
        if side == "BUY":
            px = clamp_buy_to_avoid_self_trade(px, self.config, bid=bid, ask=ask)
        else:
            px = clamp_sell_to_avoid_self_trade(px, self.config, bid=bid, ask=ask)

        if side == "BUY":
            if not self.can_open_new_long():
                if self.buy_order:
                    self.cancel_live_order(self.buy_order)
                return
            if self.sell_order and px >= self.sell_order.price:
                px = self.round_down_cent(self.sell_order.price - 0.01)
        elif self.buy_order and px <= self.buy_order.price:
            px = self.round_up_cent(self.buy_order.price + 0.01)

        if px <= 0 or qty <= 0:
            if prior:
                self.cancel_live_order(prior)
            return

        if self.is_same_order(prior, side, qty, px):
            return

        oid = prior.order_id if prior is not None else self.next_id()
        if prior is not None and side == "SELL":
            prior_qty = self._sell_working_open_qty(prior)
        else:
            prior_qty = prior.qty if prior is not None else None
        ib_qty = (
            self._sell_order_total_quantity(qty, prior)
            if side == "SELL"
            else qty
        )
        order = self.build_lmt_order(side, ib_qty, px)
        self.placeOrder(oid, self.contract, order)
        if side == "BUY":
            self.buy_order = LiveOrder(
                order_id=oid,
                side="BUY",
                price=px,
                qty=qty)
        else:
            filled = int(prior.filled) if prior is not None else 0
            self.sell_order = LiveOrder(
                order_id=oid,
                side="SELL",
                price=px,
                qty=qty,
                filled=filled,
                remaining=qty,
                total_qty=ib_qty,
            )
        if prior is None:
            self.logger.info(
                "Order placed id=%s side=%s qty=%s px=%.2f", oid, side, qty, px
            )
        elif prior_qty != qty:
            if side == "SELL" and int(prior.filled) > 0:
                self.logger.info(
                    "Order qty change id=%s side=%s remaining %s->%s "
                    "totalQuantity %s->%s px=%.2f",
                    oid,
                    side,
                    prior_qty,
                    qty,
                    int(prior.filled) + int(prior_qty),
                    ib_qty,
                    px,
                )
            else:
                self.logger.info(
                    "Order qty change id=%s side=%s qty %s->%s px=%.2f",
                    oid,
                    side,
                    prior_qty,
                    qty,
                    px,
                )
        else:
            self.logger.debug(
                "Order price change id=%s side=%s qty=%s px %.4f->%.4f",
                oid,
                side,
                qty,
                prior.price,
                px,
            )

    def place_or_replace_buy(self, qty: int, px: Optional[float]):
        """Place or replace buy."""
        self._place_or_replace_lmt("BUY", qty, px)

    def place_or_replace_sell(self, qty: int, px: Optional[float]):
        """Place or replace sell."""
        self._place_or_replace_lmt("SELL", qty, px)

    def _capture_quote_mgmt_snapshot(self) -> QuoteMgmtSnapshot:
        """Capture quote mgmt snapshot."""
        with self.lock:
            return QuoteMgmtSnapshot(
                connected_flag=self.connected_flag,
                shutdown_flag=self.shutdown_flag,
                open_orders_snapshot_done=self.open_orders_snapshot_done,
                position_size=self.position_size,
                gross_shares_traded=self.gross_shares_traded,
                avg_cost=self.avg_cost,
                buy_shares_filled=self.buy_shares_filled,
                max_sell=max_sell_shares(self.ledger),
                last_quote_eval=self.last_quote_eval,
                last_nbbo_snap=self._nbbo_throttle.last_key,
                quote_bid=self.quote.bid,
                quote_ask=self.quote.ask,
                quote_bid_sz=self.quote.bid_size,
                quote_ask_sz=self.quote.ask_size,
                quote_luts=self.quote.last_update_ts,
                buy_work_px=self.buy_order.price if self.buy_order else None,
                sell_work_px=self.sell_order.price if self.sell_order else None,
                sell_snap=(
                    (self.sell_order.status, self.sell_order.remaining, self.sell_order.qty)
                    if self.sell_order
                    else None
                ),
            )

    def _compute_quote_decision(
        self, snap: QuoteMgmtSnapshot
    ) -> Union[QuoteDecision, QuotePipelineInvalidPair]:
        """Turn a snapshot into final desired sizes/prices (no orders placed, no lock held)."""
        qb, qa = snap.quote_bid, snap.quote_ask
        if qb is None or qa is None:
            return QuotePipelineInvalidPair(True, qb, qa)

        buy_px, sell_px = self._compute_desired_quotes_for(
            float(qb), float(qa), snap.position_size, snap.avg_cost
        )
        bid = float(qb)
        ask = float(qa)

        if not self._quotes_pair_is_valid(buy_px, sell_px):
            return QuotePipelineInvalidPair(True, buy_px, sell_px)

        assert buy_px is not None and sell_px is not None

        if not self._quotes_pair_is_valid(buy_px, sell_px):
            return QuotePipelineInvalidPair(False, buy_px, sell_px)

        buy_qty, sell_qty = self._desired_sizes_for(snap.position_size, snap.buy_shares_filled)

        sell_qty = min(sell_qty, snap.max_sell)
        if snap.max_sell <= 0:
            sell_qty = 0

        if buy_qty > 0 and sell_qty > 0 and buy_px >= sell_px:
            sell_qty = 0

        buy_px, sell_px = self._nbbo_tick_improve_with_nbbo(
            buy_px,
            sell_px,
            buy_qty,
            sell_qty,
            bid,
            ask,
            snap.quote_bid_sz,
            snap.quote_ask_sz,
        )

        buy_px, sell_px = clamp_quote_prices_to_avoid_self_trade(
            buy_px, sell_px, self.config, bid=bid, ask=ask
        )

        if buy_qty > 0 and sell_qty > 0 and buy_px >= sell_px:
            sell_qty = 0

        sw = snap.sell_work_px
        if sw is not None and buy_qty > 0 and buy_px >= sw:
            buy_px = self.round_down_cent(sw - 0.01)

        bw = snap.buy_work_px
        if bw is not None and sell_qty > 0 and sell_px <= bw:
            sell_px = self.round_up_cent(bw + 0.01)

        if buy_qty > 0 and sell_qty > 0 and buy_px >= sell_px:
            self.logger.warning(
                "Final anti-self-trade guard triggered; suppressing sell quote."
            )
            sell_qty = 0

        return QuoteDecision(
            buy_qty=buy_qty,
            buy_px=buy_px,
            sell_qty=sell_qty,
            sell_px=sell_px,
        )

    def maybe_manage_quotes(self, force=False):
        """Evaluate NBBO and place/replace/cancel two-sided quotes."""
        now = time.time()
        snap = self._capture_quote_mgmt_snapshot()

        self.logger.debug(
            "maybe_manage_quotes force=%s connected=%s shutdown=%s pos=%s gross=%s "
            "bid=%s ask=%s bid_sz=%s ask_sz=%s",
            force,
            snap.connected_flag,
            snap.shutdown_flag,
            snap.position_size,
            snap.gross_shares_traded,
            snap.quote_bid,
            snap.quote_ask,
            snap.quote_bid_sz,
            snap.quote_ask_sz,
        )

        if not snap.connected_flag or snap.shutdown_flag:
            return

        if not snap.open_orders_snapshot_done:
            self.logger.debug(
                "Open-order snapshot not complete; skipping quote management this cycle."
            )
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

        pipeline = self._compute_quote_decision(snap)
        if isinstance(pipeline, QuotePipelineInvalidPair):
            msg = (
                "Computed invalid quotes buy=%s sell=%s; cancelling."
                if pipeline.after_compute
                else "Quotes invalid after NBBO clamp buy=%s sell=%s; cancelling."
            )
            self._abort_quotes_on_invalid_pair(pipeline.buy_px, pipeline.sell_px, msg)
            return

        buy_qty = pipeline.buy_qty
        buy_px = pipeline.buy_px
        sell_qty = pipeline.sell_qty
        sell_px = pipeline.sell_px

        self.place_or_replace_buy(buy_qty, buy_px if buy_qty > 0 else None)
        self.place_or_replace_sell(sell_qty, sell_px if sell_qty > 0 else None)

        with self.lock:
            self._nbbo_throttle.mark_ran(nbbo_key)

        log_bid = self.quote.bid
        log_ask = self.quote.ask
        log_bid_sz = self.quote.bid_size
        log_ask_sz = self.quote.ask_size
        log_pos = self.position_size
        log_avg = self.avg_cost
        decision_key = (
            None if log_bid is None else round(float(log_bid), 2),
            None if log_ask is None else round(float(log_ask), 2),
            int(log_bid_sz),
            int(log_ask_sz),
            int(log_pos),
            round(float(log_avg), 4),
            int(buy_qty),
            None if buy_qty <= 0 else round(float(buy_px), 2),
            int(sell_qty),
            None if sell_qty <= 0 else round(float(sell_px), 2),
        )
        msg = (
            "Quote decision nbbo=%.2f x %.2f (bid_sz=%s ask_sz=%s) pos=%s avg_cost=%.4f "
            "desired_buy=%s@%s desired_sell=%s@%s"
        )
        log_args = (
            log_bid,
            log_ask,
            log_bid_sz,
            log_ask_sz,
            log_pos,
            log_avg,
            buy_qty,
            buy_px,
            sell_qty,
            sell_px,
        )
        if decision_key != self._last_quote_decision_log_key:
            self._last_quote_decision_log_key = decision_key
        self.logger.debug(msg, *log_args)

    def watchdog_loop(self):
        """Background loop: session hours, stale-quote cancel, and quote refresh."""
        while not self.shutdown_flag:
            time.sleep(1.0)
            self._poll_session_hours()
            now = time.time()

            with self.lock:
                bid = self.quote.bid
                ask = self.quote.ask
                bid_sz = self.quote.bid_size
                ask_sz = self.quote.ask_size
                last_update_ts = self.quote.last_update_ts
                last_nbbo_ok_ts = self.last_nbbo_ok_ts
                market_data_type = self.market_data_type

                since_start = now - self.start_time

                if last_nbbo_ok_ts <= 0:
                    if (
                        since_start >= self.nbbo_watchdog_seconds
                        and (now - self.last_watchdog_log_ts) >= self.watchdog_repeat_seconds
                    ):
                        age = None if last_update_ts <= 0 else (now - last_update_ts)
                        self.logger.warning(
                            "NBBO watchdog: no valid bid/ask yet after %.1fs; bid=%s ask=%s "
                            "bid_sz=%s ask_sz=%s last_quote_age=%s marketDataType=%s",
                            since_start,
                            bid,
                            ask,
                            bid_sz,
                            ask_sz,
                            f"{age:.1f}s" if age is not None else "never",
                            market_data_type,
                        )
                        self.last_watchdog_log_ts = now
                    self.maybe_recover_stalled_market_data()
                    self.maybe_watchdog_recover_market_data(nbbo_ok=False)
                else:
                    age_ok = now - last_nbbo_ok_ts
                    if (
                        age_ok >= self.max_market_stale_seconds
                        and (now - self.last_watchdog_log_ts) >= self.watchdog_repeat_seconds
                    ):
                        self.logger.warning(
                            "NBBO watchdog: last valid NBBO is stale age=%.1fs bid=%s ask=%s "
                            "bid_sz=%s ask_sz=%s marketDataType=%s",
                            age_ok,
                            bid,
                            ask,
                            bid_sz,
                            ask_sz,
                            market_data_type,
                        )
                        self.last_watchdog_log_ts = now
                    self.maybe_watchdog_recover_market_data(nbbo_ok=False)

def build_arg_parser() -> argparse.ArgumentParser:
    """Build the argparse parser for this bot script."""
    parser = argparse.ArgumentParser(
        description="IBKR market-making bot (JSON config with CLI overrides)"
    )
    add_config_argument(parser, __file__)
    add_ib_connection_arguments(parser, include_account=True)

    parser.add_argument("--base_qty", type=int)
    parser.add_argument("--max_position", type=int)
    parser.add_argument("--max_gross_shares", type=int)
    parser.add_argument("--max_buy_shares_per_run", type=int)
    parser.add_argument("--min_inventory_before_offering", type=int)
    parser.add_argument("--min_spread", type=float)
    parser.add_argument("--inside_improve", type=float)
    parser.add_argument("--inventory_penalty_per_100", type=float)
    parser.add_argument("--target_roundtrip_capture", type=float)
    parser.add_argument("--quote_refresh_seconds", type=float)
    parser.add_argument("--max_market_stale_seconds", type=float)

    parser.add_argument(
        "--cancel_on_invalid_market",
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument(
        "--require_live_data",
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument(
        "--cancel_open_orders_on_shutdown",
        action=argparse.BooleanOptionalAction,
        help="Cancel tracked BUY/SELL quotes before disconnect (default: false)",
    )
    add_never_sell_below_avg_cost_argument(parser)

    parser.add_argument(
        "--log_bid_ask_ticks",
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument(
        "--log_invalid_market_reasons",
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument("--nbbo_watchdog_seconds", type=float)
    parser.add_argument("--watchdog_repeat_seconds", type=float)
    parser.add_argument("--min_quote_spread_cents", type=float)
    parser.add_argument("--min_tick", type=float)
    parser.add_argument(
        "--mid_delta",
        type=float,
        help="Half-width around NBBO mid for quote caps (default from JSON; >= one tick)",
    )

    add_session_hours_arguments(parser)
    add_logging_arguments(parser)
    return parser


def main():
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
        ],
    )
    app = MarketMaker(config)

    sys.exit(
        run_bot(
            app,
            config,
            is_ready=lambda: app.started,
            main_loop=lambda: idle_until_shutdown(app),
            extra_daemon_threads=[("Watchdog", app.watchdog_loop)],
            ready_label="nextValidId",
        )
    )


if __name__ == "__main__":
    main()