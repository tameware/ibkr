#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import math
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

from ibapi.client import EClient
from ibapi.common import OrderId, TickerId
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.order_cancel import OrderCancel
from ibapi.wrapper import EWrapper


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


def _normalize_config_key(name: str) -> str:
    return name.replace("-", "_")


def flatten_config_sections(data: Dict[str, Any]) -> Dict[str, Any]:
    """Expand one level of section objects into a flat dict (underscore leaf keys, peg_best-style).

    Top-level scalars and lists stay as-is. Nested dict values merge their snake_case keys into
    the result; top-level keys (except those starting with ``_``) override section values.
    """
    section_flat: Dict[str, Any] = {}
    top_level: Dict[str, Any] = {}
    for key, value in data.items():
        sk = _normalize_config_key(str(key))
        if sk.startswith("_"):
            continue
        if isinstance(value, dict):
            for subkey, subval in value.items():
                nk = _normalize_config_key(str(subkey))
                if nk.startswith("_"):
                    continue
                if nk in section_flat:
                    raise ValueError(f"Duplicate config key {nk!r} (from nested sections)")
                section_flat[nk] = subval
        else:
            top_level[sk] = value
    merged = dict(section_flat)
    merged.update(top_level)
    return merged


def load_config_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Config file must contain a JSON object at the top level")
    return flatten_config_sections(data)


def cli_to_config(args: argparse.Namespace) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in vars(args).items():
        if key == "config" or value is None:
            continue
        result[key] = value
    return result


def merge_config(file_config: Dict[str, Any], cli_config: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(file_config)
    merged.update(cli_config)
    return merged


def _cfg_bool(config: Dict[str, Any], key: str, default: bool) -> bool:
    if key not in config:
        return default
    v = config[key]
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("0", "false", "no", "off", ""):
        return False
    if s in ("1", "true", "yes", "on"):
        return True
    raise ValueError(f"Invalid boolean for {key!r}: {v!r}")


class MarketMaker(EWrapper, EClient):
    def __init__(self, config: Dict[str, Any]):
        EClient.__init__(self, self)

        self.config = config
        self.logger = self._build_logger(config)

        self.host = str(config.get("host", "127.0.0.1"))
        self.port = int(config.get("port", 7496))
        self.client_id = int(config.get("client_id", 901))
        raw_acct = config.get("account", "") or ""
        self.account_filter = str(raw_acct).strip() or None

        self.contract = self.build_contract(config)

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
        self.max_market_stale_seconds = float(
            config.get("max_market_stale_seconds", 15.0)
        )
        _mbd = config.get("max_buy_shares_per_run")
        self.max_buy_shares_per_run: Optional[int] = (
            None if _mbd is None else int(_mbd)
        )

        self.end_new_positions_hour = int(config.get("end_new_positions_hour", 15))
        self.end_new_positions_minute = int(
            config.get("end_new_positions_minute", 40)
        )
        self.flatten_hour = int(config.get("flatten_hour", 15))
        self.flatten_minute = int(config.get("flatten_minute", 55))
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
        self.shutdown_flag = False
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
        self.start_time = time.time()
        self.last_nbbo_ok_ts = 0.0
        self.last_watchdog_log_ts = 0.0

        self.target_conid: Optional[int] = None
        self.open_orders_snapshot_done = False
        self.adoption_phase = False

        self.market_tz = ZoneInfo("America/New_York")
        self.logger.info("Market timezone set to %s", self.market_tz.key)

        self.logger.info(
            "Bot initialized for %s on %s/%s",
            self.contract.symbol,
            self.contract.exchange,
            self.contract.primaryExchange,
        )

    @staticmethod
    def _build_logger(config: Dict[str, Any]):
        log_dir = Path(str(config.get("log_dir", "logs")))
        log_file = str(config.get("log_file", "market_maker.log"))
        level_name = str(config.get("level", "INFO")).upper()
        max_bytes = int(config.get("max_bytes", 5_000_000))
        backup_count = int(config.get("backup_count", 10))
        use_console = _cfg_bool(config, "console", True)

        log_dir.mkdir(parents=True, exist_ok=True)

        logger = logging.getLogger("market_maker")
        logger.setLevel(getattr(logging, level_name, logging.INFO))
        logger.handlers.clear()
        logger.propagate = False

        fmt = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s %(threadName)s %(message)s"
        )

        fh = logging.handlers.RotatingFileHandler(
            log_dir / log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        fh.setFormatter(fmt)
        fh.setLevel(getattr(logging, level_name, logging.INFO))
        logger.addHandler(fh)

        if use_console:
            ch = logging.StreamHandler(sys.stdout)
            ch.setFormatter(fmt)
            ch.setLevel(getattr(logging, level_name, logging.INFO))
            logger.addHandler(ch)

        return logger

    @staticmethod
    def build_contract(config: Dict[str, Any]) -> Contract:
        c = Contract()
        c.symbol = str(config.get("symbol", "FDX"))
        c.secType = str(config.get("sec_type", "STK"))
        c.exchange = str(config.get("exchange", "SMART"))
        c.currency = str(config.get("currency", "USD"))
        c.primaryExchange = str(config.get("primary_exchange", "NYSE"))
        return c

    def error(self, reqId, errorCode, errorString, advancedOrderReject="", errorTime=""):
        if "data farm" in advancedOrderReject:
            return

        msg = f"reqId={reqId} code={errorCode} msg={errorString}"
        if advancedOrderReject:
            msg += f" reject={advancedOrderReject}"
        if errorTime:
            msg += f" time={errorTime}"

        if errorCode in {2104, 2106, 2158}:
            self.logger.info("IB status: %s", msg)
        else:
            self.logger.warning("IB error: %s", msg)

    def nextValidId(self, orderId: OrderId):
        with self.lock:
            self.next_order_id = orderId
            self.connected_flag = True
            self.logger.info("Connected to IBKR. nextValidId=%s", orderId)
            if not self.started:
                self.started = True
                self.startup()

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
            self._ib_cancel_order(live.order_id)
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
                    log_msg = f"BID tick bid={self.quote.bid:.2f} ask={self.quote.ask}"
            elif tickType == 2 and price > 0:
                self.quote.ask = float(price)
                self.quote.last_update_ts = time.time()
                updated = True
                if self.log_bid_ask_ticks:
                    log_msg = f"ASK tick bid={self.quote.bid} ask={self.quote.ask:.2f}"

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

        with self.lock:
            if tickType == 0:
                self.quote.bid_size = int(size)
            elif tickType == 3:
                self.quote.ask_size = int(size)

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
                elif self._should_keep_order(incumbent, live):
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

    def _nbbo_tick_improve_quotes(
        self,
        buy_px: float,
        sell_px: float,
        buy_qty: int,
        sell_qty: int,
    ) -> Tuple[float, float]:
        """Raise working bid / lower working ask by one minTick when NBBO size exceeds
        our quote size on that side, only if our bid–ask width stays >=
        min_quote_spread_cents (and quotes remain passive vs NBBO)."""
        bid = self.quote.bid
        ask = self.quote.ask
        if bid is None or ask is None:
            return buy_px, sell_px

        tick = self._effective_min_tick()
        min_width = self.min_quote_spread_cents / 100.0

        if buy_qty > 0 and self.quote.bid_size > buy_qty:
            cand_buy = self._quantize_to_tick(buy_px + tick)
            if (
                cand_buy < ask
                and cand_buy > bid
                and sell_px - cand_buy >= min_width
            ):
                buy_px = cand_buy
                self.logger.info(
                    "NBBO tick improve BUY: bid_size=%s > buy_qty=%s -> buy_px=%.4f",
                    self.quote.bid_size,
                    buy_qty,
                    buy_px,
                )

        if sell_qty > 0 and self.quote.ask_size > sell_qty:
            cand_sell = self._quantize_to_tick(sell_px - tick)
            if (
                cand_sell > bid
                and cand_sell < ask
                and cand_sell - buy_px >= min_width
            ):
                sell_px = cand_sell
                self.logger.info(
                    "NBBO tick improve SELL: ask_size=%s > sell_qty=%s -> sell_px=%.4f",
                    self.quote.ask_size,
                    sell_qty,
                    sell_px,
                )

        return buy_px, sell_px

    def build_lmt_order(self, action: str, qty: int, px: float) -> Order:
        o = Order()
        o.action = action
        o.orderType = "LMT"
        o.totalQuantity = qty
        o.lmtPrice = round(px, 2)
        o.tif = "DAY"
        o.outsideRth = False
        return o

    def _ib_cancel_order(self, order_id: int):
        try:
            self.cancelOrder(order_id, OrderCancel())
        except TypeError:
            self.cancelOrder(order_id)

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

    def market_invalid_reason(self) -> Optional[str]:
        if self.quote.bid is None:
            return "missing bid"
        if self.quote.ask is None:
            return "missing ask"
        if self.quote.bid <= 0:
            return f"nonpositive bid {self.quote.bid}"
        if self.quote.ask <= 0:
            return f"nonpositive ask {self.quote.ask}"
        if self.quote.bid >= self.quote.ask:
            return f"crossed_or_locked bid={self.quote.bid:.2f} ask={self.quote.ask:.2f}"

        age = time.time() - self.quote.last_update_ts
        if age > self.max_market_stale_seconds:
            return f"stale quote age={age:.1f}s max={self.max_market_stale_seconds:.1f}s"

        spread = self.quote.ask - self.quote.bid
        if spread < self.min_spread:
            return f"spread too small spread={spread:.2f} min={self.min_spread:.2f}"

        if self.require_live_data and self.market_data_type not in (None, 1):
            return f"market data type not live type={self.market_data_type}"

        return None

    def market_is_valid(self) -> bool:
        return self.market_invalid_reason() is None

    def compute_desired_quotes(self):
        bid = self.quote.bid
        ask = self.quote.ask
        spread = ask - bid

        buy_px = bid + min(self.inside_improve, spread * 0.25)
        sell_px = ask - min(self.inside_improve, spread * 0.25)

        long_hundreds = max(0, self.position_qty) / 100.0
        skew = long_hundreds * self.inventory_penalty_per_100

        buy_px -= skew
        sell_px -= min(skew * 2.0, spread * 0.20)

        if self.position_qty > 0 and self.avg_cost > 0:
            sell_floor = self.avg_cost + self.target_roundtrip_capture
            sell_px = max(sell_px, sell_floor)

        buy_px = min(buy_px, ask - 0.02)
        sell_px = max(sell_px, bid + 0.02)

        buy_px = self.round_down_cent(buy_px)
        sell_px = self.round_up_cent(sell_px)

        if buy_px >= sell_px:
            return None, None

        return buy_px, sell_px

    def desired_sizes(self):
        buy_qty = min(self.base_qty, max(0, self.max_position - self.position_qty))
        if self.max_buy_shares_per_run is not None:
            room = max(0, self.max_buy_shares_per_run - self.buy_shares_filled)
            buy_qty = min(buy_qty, room)
        sell_qty = self.max_sellable_qty()
        return buy_qty, sell_qty

    def is_same_order(self, live: Optional[LiveOrder], side: str, qty: int, px: float) -> bool:
        if live is None:
            return False
        if live.side != side:
            return False

        live_open_qty = live.remaining if side == "SELL" and live.remaining > 0 else live.qty
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
            self._ib_cancel_order(oid)

            with self.lock:
                if side == "BUY" and self.buy_order and self.buy_order.order_id == oid:
                    self.buy_order = None
                elif side == "SELL" and self.sell_order and self.sell_order.order_id == oid:
                    self.sell_order = None

            self.logger.info("Cancel order id=%s side=%s px=%.2f qty=%s", oid, side, px, qty)
        except Exception as e:
            self.logger.exception("Cancel failed for order id=%s side=%s: %s", oid, side, e)

    def place_or_replace_buy(self, qty: int, px: Optional[float]):
        if qty <= 0 or px is None:
            if self.buy_order:
                self.cancel_live_order(self.buy_order)
            return

        if not self.can_open_new_long():
            if self.buy_order:
                self.cancel_live_order(self.buy_order)
            return

        if self.sell_order and px >= self.sell_order.price:
            px = self.round_down_cent(self.sell_order.price - 0.01)

        if px <= 0:
            if self.buy_order:
                self.cancel_live_order(self.buy_order)
            return

        if self.is_same_order(self.buy_order, "BUY", qty, px):
            return

        oid = self.buy_order.order_id if self.buy_order else self.next_id()
        order = self.build_lmt_order("BUY", qty, px)
        self.placeOrder(oid, self.contract, order)
        self.buy_order = LiveOrder(order_id=oid, side="BUY", price=px, qty=qty)
        self.logger.info("BUY working id=%s qty=%s px=%.2f", oid, qty, px)

    def place_or_replace_sell(self, qty: int, px: Optional[float]):
        qty = min(qty, self.max_sellable_qty())
    
        if qty <= 0 or px is None:
            if self.sell_order:
                self.cancel_live_order(self.sell_order)
            return
    
        if self.buy_order and px <= self.buy_order.price:
            px = self.round_up_cent(self.buy_order.price + 0.01)
    
        if px <= 0 or qty <= 0:
            if self.sell_order:
                self.cancel_live_order(self.sell_order)
            return
    
        # snapshot of prior adopted order (may be id 0)
        prior = self.sell_order
    
        if self.is_same_order(prior, "SELL", qty, px):
            return
    
        new_oid = self.next_id()
        order = self.build_lmt_order("SELL", qty, px)
        self.placeOrder(new_oid, self.contract, order)
        self.sell_order = LiveOrder(
            order_id=new_oid,
            side="SELL",
            price=px,
            qty=qty,
            remaining=qty,
        )
        self.logger.info("SELL working id=%s qty=%s px=%.2f", new_oid, qty, px)
    
        # best-effort cancel of any adopted incumbent with a different id
        if prior is not None and prior.order_id != new_oid:
            try:
                self._ib_cancel_order(prior.order_id)
                self.logger.info(
                    "Requested cancel of adopted incumbent id=%s side=%s px=%.2f qty=%s",
                    prior.order_id,
                    prior.side,
                    prior.price,
                    prior.qty,
                )
            except Exception as e:
                # tolerate failures if prior id is not cancelable by this client
                self.logger.warning(
                    "Cancel of adopted incumbent id=%s failed: %s", prior.order_id, e
                )

    def _market_now(self) -> datetime:
        return datetime.now(self.market_tz)

    def flatten_only_mode(self) -> bool:
        now = self._market_now()
        return (now.hour > self.end_new_positions_hour) or (
            now.hour == self.end_new_positions_hour
            and now.minute >= self.end_new_positions_minute
        )

    def hard_flatten_mode(self) -> bool:
        now = self._market_now()
        return (now.hour > self.flatten_hour) or (
            now.hour == self.flatten_hour and now.minute >= self.flatten_minute
        )

    def force_flatten(self):
        with self.lock:
            pos = self.position_qty
            bid = self.quote.bid
            ask = self.quote.ask

        self.place_or_replace_buy(0, None)

        if pos <= 0 or bid is None or ask is None:
            self.place_or_replace_sell(0, None)
            return

        aggressive_sell = self.round_up_cent(max(bid + 0.01, ask - 0.01))
        self.place_or_replace_sell(pos, aggressive_sell)
        self.logger.info(
            "Force flatten mode active position=%s aggressive_sell=%.2f",
            pos,
            aggressive_sell,
        )

    def maybe_manage_quotes(self, force=False):
        now = time.time()

        with self.lock:
            self.logger.info(
                "maybe_manage_quotes force=%s connected=%s shutdown=%s snapshot_done=%s dt=%.2f pos=%s gross=%s bid=%s ask=%s",
                force,
                self.connected_flag,
                self.shutdown_flag,
                self.open_orders_snapshot_done,
                now - self.last_quote_eval,
                self.position_qty,
                self.gross_shares_traded,
                self.quote.bid,
                self.quote.ask,
            )

            if not self.connected_flag or self.shutdown_flag:
                return

            if not self.open_orders_snapshot_done:
                self.logger.info(
                    "Open-order snapshot not complete; skipping quote management this cycle."
                )
                return

            reason = self.market_invalid_reason()
            if reason is not None:
                if self.cancel_on_invalid_market:
                    self.place_or_replace_buy(0, None)
                    if self.position_qty <= 0:
                        self.place_or_replace_sell(0, None)
                if self.log_invalid_market_reasons:
                    self.logger.info("Not quoting: %s", reason)
                return

            if not force and (now - self.last_quote_eval) < self.quote_refresh_seconds:
                return
            self.last_quote_eval = now

            reason = self.market_invalid_reason()
            if reason is not None:
                if self.cancel_on_invalid_market:
                    self.place_or_replace_buy(0, None)
                    if self.position_qty <= 0:
                        self.place_or_replace_sell(0, None)
                if self.log_invalid_market_reasons:
                    self.logger.info("Not quoting: %s", reason)
                return

            if self.hard_flatten_mode():
                self.force_flatten()
                return

            buy_px, sell_px = self.compute_desired_quotes()

            bid = self.quote.bid
            ask = self.quote.ask

            if buy_px is None or sell_px is None or buy_px >= sell_px:
                self.place_or_replace_buy(0, None)
                if self.position_qty <= 0:
                    self.place_or_replace_sell(0, None)
                self.logger.warning(
                    "Computed invalid quotes buy=%s sell=%s; cancelling.",
                    buy_px,
                    sell_px,
                )
                return

            if bid is not None:
                buy_px = max(buy_px, bid)
            if ask is not None:
                sell_px = min(sell_px, ask)

            if buy_px is None or sell_px is None or buy_px >= sell_px:
                self.place_or_replace_buy(0, None)
                if self.position_qty <= 0:
                    self.place_or_replace_sell(0, None)
                self.logger.warning(
                    "Quotes invalid after NBBO clamp buy=%s sell=%s; cancelling.",
                    buy_px,
                    sell_px,
                )
                return

            buy_qty, sell_qty = self.desired_sizes()

            if self.flatten_only_mode():
                buy_qty = 0
                sell_qty = self.max_sellable_qty()

            sell_qty = min(sell_qty, self.position_qty)
            if self.position_qty <= 0:
                sell_qty = 0

            if buy_qty > 0 and sell_qty > 0 and buy_px >= sell_px:
                sell_qty = 0

            buy_px, sell_px = self._nbbo_tick_improve_quotes(
                buy_px, sell_px, buy_qty, sell_qty
            )

            if buy_qty > 0 and sell_qty > 0 and buy_px >= sell_px:
                sell_qty = 0

            if self.sell_order and buy_qty > 0 and buy_px >= self.sell_order.price:
                buy_px = self.round_down_cent(self.sell_order.price - 0.01)

            if self.buy_order and sell_qty > 0 and sell_px <= self.buy_order.price:
                sell_px = self.round_up_cent(self.buy_order.price + 0.01)

            if buy_qty > 0 and sell_qty > 0 and buy_px >= sell_px:
                self.logger.warning(
                    "Final anti-self-trade guard triggered; suppressing sell quote."
                )
                sell_qty = 0

        self.place_or_replace_buy(buy_qty, buy_px if buy_qty > 0 else None)
        self.place_or_replace_sell(sell_qty, sell_px if sell_qty > 0 else None)

        self.logger.info(
            "Quote decision nbbo=%.2f x %.2f pos=%s avg_cost=%.4f desired_buy=%s@%s desired_sell=%s@%s",
            self.quote.bid,
            self.quote.ask,
            self.position_qty,
            self.avg_cost,
            buy_qty,
            buy_px,
            sell_qty,
            sell_px,
        )

    def watchdog_loop(self):
        while not self.shutdown_flag:
            time.sleep(1.0)
            now = time.time()

            with self.lock:
                bid = self.quote.bid
                ask = self.quote.ask
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
                            "NBBO watchdog: no valid bid/ask yet after %.1fs; bid=%s ask=%s last_quote_age=%s marketDataType=%s",
                            since_start,
                            bid,
                            ask,
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
                            "NBBO watchdog: last valid NBBO is stale age=%.1fs bid=%s ask=%s marketDataType=%s",
                            age_ok,
                            bid,
                            ask,
                            market_data_type,
                        )
                        self.last_watchdog_log_ts = now

    def stop(self):
        with self.lock:
            self.shutdown_flag = True

        self.logger.info("Shutdown requested.")

        try:
            self.cancelMktData(self.market_data_req_id)
        except Exception as e:
            self.logger.warning("cancelMktData failed: %s", e)

        try:
            if self.buy_order:
                self.cancel_live_order(self.buy_order)
            if self.sell_order:
                self.cancel_live_order(self.sell_order)
        except Exception as e:
            self.logger.warning("Order cancel during shutdown failed: %s", e)

        time.sleep(1)

        try:
            self.disconnect()
        except Exception as e:
            self.logger.warning("Disconnect failed: %s", e)

        self.logger.info("Disconnected cleanly.")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="IBKR market-making bot (JSON config with CLI overrides)"
    )
    default_config_path = str(Path(__file__).with_suffix(".json"))
    parser.add_argument(
        "--config",
        default=default_config_path,
        help="Path to JSON config file (default: script name with .json suffix)",
    )

    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--client_id", type=int)
    parser.add_argument("--account")

    parser.add_argument("--symbol")
    parser.add_argument("--sec_type")
    parser.add_argument("--exchange")
    parser.add_argument("--currency")
    parser.add_argument("--primary_exchange")

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

    parser.add_argument("--end_new_positions_hour", type=int)
    parser.add_argument("--end_new_positions_minute", type=int)
    parser.add_argument("--flatten_hour", type=int)
    parser.add_argument("--flatten_minute", type=int)
    parser.add_argument(
        "--cancel_on_invalid_market",
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument(
        "--require_live_data",
        action=argparse.BooleanOptionalAction,
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

    parser.add_argument("--log_dir")
    parser.add_argument("--log_file")
    parser.add_argument("--level")
    parser.add_argument("--max_bytes", type=int)
    parser.add_argument("--backup_count", type=int)
    parser.add_argument(
        "--console",
        action=argparse.BooleanOptionalAction,
    )
    return parser


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    config_path = Path(args.config)
    file_config = load_config_file(str(config_path))
    cli_config = cli_to_config(args)
    config = merge_config(file_config, cli_config)
    app = MarketMaker(config)

    def handle_sig(*_):
        app.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    app.logger.info(
        "Connecting to IBKR host=%s port=%s client_id=%s",
        app.host,
        app.port,
        app.client_id,
    )
    app.connect(app.host, app.port, app.client_id)

    api_thread = threading.Thread(target=app.run, name="IBAPI", daemon=True)
    api_thread.start()

    watchdog_thread = threading.Thread(target=app.watchdog_loop, name="Watchdog", daemon=True)
    watchdog_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        handle_sig()


if __name__ == "__main__":
    main()