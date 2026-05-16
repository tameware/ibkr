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
    add_config_argument,
    add_ib_connection_arguments,
    add_logging_arguments,
    add_session_hours_arguments,
    cfg_bool as _cfg_bool,
    execution_belongs_to_client,
    flatten_config_sections,
    idle_until_shutdown,
    load_merged_config,
    log_session_transition,
    NbboThrottle,
    open_order_belongs_to_client,
    order_status_clients_match,
    regular_session_open,
    run_bot,
    safe_cancel_order,
)
from ibkr_bot_base import IbkrBotApp


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


@dataclass(frozen=True)
class QuoteMgmtSnapshot:
    """Point-in-time state for one quote-management cycle (built under ``self.lock``)."""

    connected_flag: bool
    shutdown_flag: bool
    open_orders_snapshot_done: bool
    position_qty: int
    gross_shares_traded: int
    avg_cost: float
    buy_shares_filled: int
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


class MarketMaker(IbkrBotApp):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(
            config,
            logger_name="market_maker",
            default_log_file="market_maker.log",
        )

        self.host = str(config.get("host", "127.0.0.1"))
        self.port = int(config.get("port", 7496))
        self.client_id = int(config.get("client_id", 901))
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

        self.log_bid_ask_ticks = _cfg_bool(config, "log_bid_ask_ticks", True)
        self.log_invalid_market_reasons = _cfg_bool(
            config, "log_invalid_market_reasons", True
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

        self.quote = QuoteState()
        self.position_qty = 0
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

        self.logger.info(
            "Bot initialized for %s on %s/%s",
            self.contract.symbol,
            self.contract.exchange,
            self.contract.primaryExchange,
        )

    def market_data_req_ids(self):
        return (self.market_data_req_id,)

    def assign_next_order_id(self, order_id: int) -> None:
        with self.lock:
            self.next_order_id = order_id

    def on_api_ready(self, order_id: int) -> None:
        with self.lock:
            self.connected_flag = True
        self.logger.info("Connected to IBKR. nextValidId=%s", order_id)
        if not self.started:
            self.started = True
            self.startup()

    def on_connection_closed(self) -> None:
        with self.lock:
            self.connected_flag = False

    def _mark_shutdown(self) -> None:
        with self.lock:
            self.shutdown_flag = True

    def shutdown_quotes(self) -> None:
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
        self.market_data_type = marketDataType
        self.logger.info("Market data type update reqId=%s type=%s", reqId, marketDataType)
        if self.require_live_data and marketDataType != 1:
            self.logger.warning("Live data required, but market data type is %s", marketDataType)

    def startup(self):
        with self.lock:
            self.open_orders_snapshot_done = False
            self.adoption_phase = True
            self.buy_order = None
            self.sell_order = None
    
        self.reqMarketDataType(1)
        self.reqContractDetails(self.contract_details_req_id, self.contract)
        self.reqPositions()
    
        if self.client_id == 0:
            self.reqOpenOrders()          # binds currently open manual/TWS orders
            self.reqAutoOpenOrders(True)  # binds future manual/TWS orders
        else:
            self.reqAllOpenOrders()       # visibility only, not cancelable if id=0
    
        self.reqMktData(self.market_data_req_id, self.contract, "", False, False, [])
        self.logger.info(
            "Requested contract details, positions, open orders, and live market data."
        )

    def contractDetails(self, reqId, contractDetails):
        if reqId != self.contract_details_req_id:
            return

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

        self.logger.info(
            "Resolved contract: conId=%s symbol=%s localSymbol=%s exchange=%s primaryExchange=%s currency=%s minTick=%s",
            getattr(summary, "conId", None),
            getattr(summary, "symbol", None),
            getattr(summary, "localSymbol", None),
            getattr(summary, "exchange", None),
            getattr(summary, "primaryExchange", None),
            getattr(summary, "currency", None),
            getattr(contractDetails, "minTick", None),
        )

    def contractDetailsEnd(self, reqId):
        if reqId == self.contract_details_req_id:
            self.logger.info("Contract details request complete for reqId=%s", reqId)

    def _is_target_contract(self, contract) -> bool:
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

    def tickPrice(self, reqId, tickType, price, attrib):
        if reqId != self.market_data_req_id:
            return

        updated = False
        log_msg = None

        with self.lock:
            if tickType == 1 and price > 0:
                self.quote.bid = float(price)
                self.quote.last_update_ts = time.time()
                updated = True
                if self.log_bid_ask_ticks:
                    log_msg = (
                        f"BID tick bid={self.quote.bid:.2f} ask={self.quote.ask} "
                        f"bid_sz={self.quote.bid_size} ask_sz={self.quote.ask_size}"
                    )
            elif tickType == 2 and price > 0:
                self.quote.ask = float(price)
                self.quote.last_update_ts = time.time()
                updated = True
                if self.log_bid_ask_ticks:
                    log_msg = (
                        f"ASK tick bid={self.quote.bid} ask={self.quote.ask:.2f} "
                        f"bid_sz={self.quote.bid_size} ask_sz={self.quote.ask_size}"
                    )

            if (
                self.quote.bid is not None
                and self.quote.ask is not None
                and self.quote.bid < self.quote.ask
            ):
                self.last_nbbo_ok_ts = time.time()

        if log_msg:
            self.logger.info(log_msg)

        if updated:
            self.maybe_manage_quotes()

    def tickSize(self, reqId, tickType, size):
        if reqId != self.market_data_req_id:
            return

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
        if contract.symbol != self.contract.symbol or contract.secType != self.contract.secType:
            return
        if self.account_filter and account != self.account_filter:
            return

        with self.lock:
            self.account = account
            self.position_qty = max(0, int(position))
            self.avg_cost = float(avgCost) if self.position_qty > 0 else 0.0

        self.logger.info(
            "Position update account=%s symbol=%s position=%s avgCost=%.4f",
            account,
            contract.symbol,
            self.position_qty,
            self.avg_cost,
        )

    def positionEnd(self):
        self.logger.info("Initial position snapshot complete.")

    def openOrder(self, orderId, contract, order, orderState):
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
        remaining = getattr(orderState, "remaining", None)
        if remaining is None:
            remaining = max(0, int(order.totalQuantity) - filled)
        remaining = int(remaining)

        live_qty = remaining if side == "SELL" else int(order.totalQuantity)

        live = LiveOrder(
            order_id=orderId,
            side=side,
            price=float(order.lmtPrice) if getattr(order, "lmtPrice", None) is not None else 0.0,
            qty=live_qty,
            status=orderState.status,
            filled=filled,
            remaining=remaining,
        )

        if not self.adoption_phase:
            with self.lock:
                if side == "BUY" and self.buy_order and self.buy_order.order_id == orderId:
                    self.buy_order = live
                elif side == "SELL" and self.sell_order and self.sell_order.order_id == orderId:
                    self.sell_order = live
            self.logger.info(
                "openOrder refresh id=%s side=%s qty=%s px=%.2f status=%s remaining=%s",
                orderId,
                live.side,
                live.qty,
                live.price,
                live.status,
                live.remaining,
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

        self.logger.info(
            "openOrder adoption id=%s side=%s qty=%s px=%.2f status=%s remaining=%s action=%s",
            orderId,
            live.side,
            live.qty,
            live.price,
            live.status,
            live.remaining,
            action,
        )

        if extra_to_cancel is not None and extra_to_cancel.order_id != live.order_id:
            self._cancel_adopted_extra(extra_to_cancel)

    def openOrderEnd(self):
        with self.lock:
            self.open_orders_snapshot_done = True
            self.adoption_phase = False
            self.logger.info(
                "Open orders snapshot complete. Adopted buy_order=%s sell_order=%s",
                self.buy_order,
                self.sell_order,
            )

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
                target.remaining = int(remaining)
                if target.side == "SELL":
                    target.qty = int(remaining)

                if status in {"Filled", "Cancelled", "ApiCancelled", "Inactive"} and int(
                    remaining
                ) == 0:
                    if target.side == "BUY":
                        self.buy_order = None
                    elif target.side == "SELL":
                        self.sell_order = None

        self.logger.info(
            "orderStatus id=%s status=%s filled=%s remaining=%s avgFill=%.4f lastFill=%.4f",
            orderId,
            status,
            filled,
            remaining,
            float(avgFillPrice or 0.0),
            float(lastFillPrice or 0.0),
        )

    def execDetails(self, reqId, contract, execution):
        if not self._is_target_contract(contract):
            return
        if not execution_belongs_to_client(execution, self.client_id):
            return

        shares = int(execution.shares)
        side = execution.side.upper()
        px = float(execution.price)

        with self.lock:
            self.gross_shares_traded += shares
            if side in ("BOT", "BUY"):
                self.buy_shares_filled += shares

            if side == "SLD" and self.position_qty > 0 and self.avg_cost > 0:
                sellable = min(self.position_qty, shares)
                self.realized_pnl += sellable * (px - self.avg_cost)

        self.logger.info(
            "execDetails side=%s shares=%s px=%.4f pos_snapshot=%s gross=%s realized=%.2f execId=%s",
            side,
            shares,
            px,
            self.position_qty,
            self.gross_shares_traded,
            self.realized_pnl,
            execution.execId,
        )

        self.maybe_manage_quotes(force=True)

    def next_id(self) -> int:
        with self.lock:
            if self.next_order_id is None:
                raise RuntimeError("next_order_id not initialized")
            oid = self.next_order_id
            self.next_order_id += 1
            return oid

    @staticmethod
    def round_down_cent(x: float) -> float:
        return math.floor(x * 100.0) / 100.0

    @staticmethod
    def round_up_cent(x: float) -> float:
        return math.ceil(x * 100.0) / 100.0

    def _effective_min_tick(self) -> float:
        with self.lock:
            t = float(self.min_tick)
            return t if t > 0 else 0.01

    def _quantize_to_tick(self, x: float) -> float:
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
                self.logger.info(
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
                self.logger.info(
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
        o = Order()
        o.action = action
        o.orderType = "LMT"
        o.totalQuantity = qty
        o.lmtPrice = round(px, 2)
        o.tif = "DAY"
        o.outsideRth = False
        return o

    def can_open_new_long(self) -> bool:
        with self.lock:
            if self.position_qty >= self.max_position:
                return False
            if self.gross_shares_traded >= self.max_gross_shares:
                return False
            if self.max_buy_shares_per_run is not None:
                if self.buy_shares_filled >= self.max_buy_shares_per_run:
                    return False
            return True

    def max_sellable_qty(self) -> int:
        return max(0, self.position_qty)

    def _market_invalid_reason_for_nbbo(
        self,
        bid: Optional[float],
        ask: Optional[float],
        last_update_ts: float,
    ) -> Optional[str]:
        if bid is None:
            return "missing bid"
        if ask is None:
            return "missing ask"
        if bid <= 0:
            return f"nonpositive bid {bid}"
        if ask <= 0:
            return f"nonpositive ask {ask}"
        if bid >= ask:
            return f"crossed_or_locked bid={bid:.2f} ask={ask:.2f}"

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
        return self._market_invalid_reason_for_nbbo(
            self.quote.bid,
            self.quote.ask,
            self.quote.last_update_ts,
        )

    def us_regular_hours(self) -> bool:
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
        if self.position_qty <= 0:
            self.place_or_replace_sell(0, None)

    def _abort_for_market_invalid_reason(self, reason: Optional[str]) -> bool:
        """If ``reason`` is set, optionally cancel working quotes and log; return True."""
        if reason is None:
            return False
        if self.cancel_on_invalid_market:
            self._cancel_working_quotes_flat_inventory()
        if self.log_invalid_market_reasons:
            self.logger.info("Not quoting: %s", reason)
        return True

    def _abort_quotes_if_market_invalid(self) -> bool:
        """If the book is not quotable, optionally cancel and log; return True to skip the rest."""
        return self._abort_for_market_invalid_reason(self.market_invalid_reason())

    @staticmethod
    def _quotes_pair_is_valid(buy_px: Optional[float], sell_px: Optional[float]) -> bool:
        return (
            buy_px is not None
            and sell_px is not None
            and buy_px < sell_px
        )

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
        return self.market_invalid_reason() is None

    def _compute_desired_quotes_for(
        self, bid: float, ask: float, position_qty: int, avg_cost: float
    ) -> Tuple[Optional[float], Optional[float]]:
        spread = ask - bid

        buy_px = bid + min(self.inside_improve, spread * 0.25)
        sell_px = ask - min(self.inside_improve, spread * 0.25)

        long_hundreds = max(0, position_qty) / 100.0
        skew = long_hundreds * self.inventory_penalty_per_100

        buy_px -= skew
        sell_px -= min(skew * 2.0, spread * 0.20)

        if position_qty > 0 and avg_cost > 0:
            sell_floor = avg_cost + self.target_roundtrip_capture
            sell_px = max(sell_px, sell_floor)

        buy_px = min(buy_px, ask - 0.02)
        sell_px = max(sell_px, bid + 0.02)

        buy_px = self.round_down_cent(buy_px)
        sell_px = self.round_up_cent(sell_px)

        if buy_px >= sell_px:
            return None, None

        return buy_px, sell_px

    def compute_desired_quotes(self):
        bid = self.quote.bid
        ask = self.quote.ask
        if bid is None or ask is None:
            return None, None
        return self._compute_desired_quotes_for(bid, ask, self.position_qty, self.avg_cost)

    def _desired_sizes_for(self, position_qty: int, buy_shares_filled: int) -> Tuple[int, int]:
        buy_qty = min(self.base_qty, max(0, self.max_position - position_qty))
        if self.max_buy_shares_per_run is not None:
            room = max(0, self.max_buy_shares_per_run - buy_shares_filled)
            buy_qty = min(buy_qty, room)
        sell_qty = max(0, position_qty)
        return buy_qty, sell_qty

    def desired_sizes(self):
        return self._desired_sizes_for(self.position_qty, self.buy_shares_filled)

    def _sell_working_open_qty(self, live: LiveOrder) -> int:
        """Working size for a sell (prefer IB ``remaining``, else last ``qty``)."""
        if live.remaining > 0:
            return int(live.remaining)
        return int(live.qty)

    def _needs_quote_despite_nbbo_throttle_with(
        self,
        position_qty: int,
        sell_snap: Optional[Tuple[str, int, int]],
    ) -> bool:
        if position_qty <= 0:
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
        return open_qty < position_qty

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
        return self._needs_quote_despite_nbbo_throttle_with(self.position_qty, sell_snap)

    def _should_keep_sell_adoption(self, incumbent: LiveOrder, candidate: LiveOrder) -> bool:
        """During ``reqOpenOrders`` adoption: prefer the sell that covers more shares."""
        pos = self.position_qty
        ir = self._sell_working_open_qty(incumbent)
        cr = self._sell_working_open_qty(candidate)
        active_statuses = {"PreSubmitted", "PendingSubmit", "Submitted"}
        i_act = incumbent.status in active_statuses and ir > 0
        c_act = candidate.status in active_statuses and cr > 0
        if pos > 0 and i_act and c_act and ir != cr:
            return ir > cr
        return self._should_keep_order(incumbent, candidate)

    def is_same_order(self, live: Optional[LiveOrder], side: str, qty: int, px: float) -> bool:
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
        if side == "BUY":
            prior = self.buy_order
        else:
            prior = self.sell_order
            qty = min(qty, self.max_sellable_qty())

        if qty <= 0 or px is None:
            if prior:
                self.cancel_live_order(prior)
            return

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
        order = self.build_lmt_order(side, qty, px)
        self.placeOrder(oid, self.contract, order)
        if side == "BUY":
            self.buy_order = LiveOrder(order_id=oid, side="BUY", price=px, qty=qty)
        else:
            self.sell_order = LiveOrder(
                order_id=oid,
                side="SELL",
                price=px,
                qty=qty,
                remaining=qty,
            )
        self.logger.info("%s working id=%s qty=%s px=%.2f", side, oid, qty, px)

    def place_or_replace_buy(self, qty: int, px: Optional[float]):
        self._place_or_replace_lmt("BUY", qty, px)

    def place_or_replace_sell(self, qty: int, px: Optional[float]):
        self._place_or_replace_lmt("SELL", qty, px)

    def _capture_quote_mgmt_snapshot(self) -> QuoteMgmtSnapshot:
        with self.lock:
            return QuoteMgmtSnapshot(
                connected_flag=self.connected_flag,
                shutdown_flag=self.shutdown_flag,
                open_orders_snapshot_done=self.open_orders_snapshot_done,
                position_qty=self.position_qty,
                gross_shares_traded=self.gross_shares_traded,
                avg_cost=self.avg_cost,
                buy_shares_filled=self.buy_shares_filled,
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
            float(qb), float(qa), snap.position_qty, snap.avg_cost
        )
        bid = float(qb)
        ask = float(qa)

        if not self._quotes_pair_is_valid(buy_px, sell_px):
            return QuotePipelineInvalidPair(True, buy_px, sell_px)

        assert buy_px is not None and sell_px is not None
        buy_px = max(buy_px, bid)
        sell_px = min(sell_px, ask)

        if not self._quotes_pair_is_valid(buy_px, sell_px):
            return QuotePipelineInvalidPair(False, buy_px, sell_px)

        buy_qty, sell_qty = self._desired_sizes_for(snap.position_qty, snap.buy_shares_filled)

        sell_qty = min(sell_qty, snap.position_qty)
        if snap.position_qty <= 0:
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
        now = time.time()
        snap = self._capture_quote_mgmt_snapshot()

        self.logger.debug(
            "maybe_manage_quotes force=%s connected=%s shutdown=%s pos=%s gross=%s "
            "bid=%s ask=%s bid_sz=%s ask_sz=%s",
            force,
            snap.connected_flag,
            snap.shutdown_flag,
            snap.position_qty,
            snap.gross_shares_traded,
            snap.quote_bid,
            snap.quote_ask,
            snap.quote_bid_sz,
            snap.quote_ask_sz,
        )

        if not snap.connected_flag or snap.shutdown_flag:
            return

        if not snap.open_orders_snapshot_done:
            self.logger.info(
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
                snap.position_qty, snap.sell_snap
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
        log_pos = self.position_qty
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
            self.logger.info(msg, *log_args)
        else:
            self.logger.debug(msg, *log_args)

    def watchdog_loop(self):
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

def build_arg_parser() -> argparse.ArgumentParser:
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

    add_session_hours_arguments(parser)
    add_logging_arguments(parser)
    return parser


def main():
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