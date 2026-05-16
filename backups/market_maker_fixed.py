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
from typing import Any, Dict, Optional
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


class MarketMaker(EWrapper, EClient):
    def __init__(self, cfg: Dict[str, Any]):
        EClient.__init__(self, self)

        self.cfg = cfg
        self.logger = self._build_logger(cfg)

        self.host = str(cfg.get("host", "127.0.0.1"))
        self.port = int(cfg.get("port", 7496))
        self.clientid = int(cfg.get("clientid", 901))
        account = cfg.get("account")
        self.account_filter = str(account).strip() or None if account is not None else None

        self.contract = self.build_contract(cfg)

        self.base_qty = int(cfg.get("baseqty", 100))
        self.max_position = int(cfg.get("maxposition", 1000))
        self.max_gross_shares = int(cfg.get("maxgrossshares", 1500))
        self.min_inventory_before_offering = int(cfg.get("mininventorybeforeoffering", 100))
        self.min_spread = float(cfg.get("minspread", 0.20))
        self.inside_improve = float(cfg.get("insideimprove", 0.10))
        self.inventory_penalty_per_100 = float(cfg.get("inventorypenaltyper100", 0.05))
        self.target_roundtrip_capture = float(cfg.get("targetroundtripcapture", 0.30))
        self.quote_refresh_seconds = float(cfg.get("quoterefreshseconds", 3.0))
        self.max_market_stale_seconds = float(cfg.get("maxmarketstaleseconds", 15.0))

        self.end_new_positions_hour = int(cfg.get("endnewpositionshour", 15))
        self.end_new_positions_minute = int(cfg.get("endnewpositionsminute", 40))
        self.flatten_hour = int(cfg.get("flattenhour", 15))
        self.flatten_minute = int(cfg.get("flattenminute", 55))
        self.cancel_on_invalid_market = bool(cfg.get("canceloninvalidmarket", True))
        self.require_live_data = bool(cfg.get("requirelivedata", False))

        self.log_bid_ask_ticks = bool(cfg.get("logbidaskticks", True))
        self.log_invalid_market_reasons = bool(cfg.get("loginvalidmarketreasons", True))
        self.nbbo_watchdog_seconds = float(cfg.get("nbbowatchdogseconds", 30.0))
        self.watchdog_repeat_seconds = float(cfg.get("watchdogrepeatseconds", 60.0))

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

        self.last_quote_eval = 0.0
        self.start_time = time.time()
        self.last_nbbo_ok_ts = 0.0
        self.last_watchdog_log_ts = 0.0
        
        self.market_tz = ZoneInfo("America/New_York")
        self.logger.info("Market timezone set to %s", self.market_tz.key)
        
        self.logger.info(
            "Bot initialized for %s on %s/%s",
            self.contract.symbol,
            self.contract.exchange,
            self.contract.primaryExchange,
        )

    @staticmethod
    def _build_logger(cfg: Dict[str, Any]):
        log_dir = Path(str(cfg.get("logdir", "logs")))
        log_file = str(cfg.get("logfile", "marketmaker.log"))
        level_name = str(cfg.get("level", "INFO")).upper()
        max_bytes = int(cfg.get("maxbytes", 5_000_000))
        backup_count = int(cfg.get("backupcount", 10))
        use_console = bool(cfg.get("console", True))

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
    def build_contract(cfg: Dict[str, Any]) -> Contract:
        c = Contract()
        c.symbol = str(cfg.get("symbol", "FDX"))
        c.secType = str(cfg.get("sectype", "STK"))
        c.exchange = str(cfg.get("exchange", "SMART"))
        c.currency = str(cfg.get("currency", "USD"))
        c.primaryExchange = str(cfg.get("primaryexchange", "NYSE"))
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
            self.logger.warning(
                "Live data required, but market data type is %s", marketDataType
            )

    def startup(self):
        self.reqMarketDataType(1)
        self.reqContractDetails(self.contract_details_req_id, self.contract)
        self.reqPositions()
        self.reqOpenOrders()
        self.reqMktData(self.market_data_req_id, self.contract, "", False, False, [])
        self.logger.info(
            "Requested contract details, positions, open orders, and live market data."
        )

    def contractDetails(self, reqId, contractDetails):
        if reqId != self.contract_details_req_id:
            return
        summary = contractDetails.contract
        self.contract_resolved = True
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

    def tickPrice(self, reqId, tickType, price, attrib):
        self.logger.info("PATCHED tickPrice v2 reached")
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
        if contract.symbol != self.contract.symbol:
            return

        live = LiveOrder(
            order_id=orderId,
            side=order.action.upper(),
            price=float(order.lmtPrice) if getattr(order, "lmtPrice", None) is not None else 0.0,
            qty=int(order.totalQuantity),
            status=orderState.status,
        )

        with self.lock:
            if live.side == "BUY":
                self.buy_order = live
            elif live.side == "SELL":
                self.sell_order = live

        self.logger.info(
            "openOrder id=%s side=%s qty=%s px=%.2f status=%s",
            orderId,
            live.side,
            live.qty,
            live.price,
            orderState.status,
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

                if status in {"Filled", "Cancelled", "ApiCancelled", "Inactive"} and int(remaining) == 0:
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
        if contract.symbol != self.contract.symbol:
            return

        shares = int(execution.shares)
        side = execution.side.upper()
        px = float(execution.price)

        with self.lock:
            self.gross_shares_traded += shares

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
        return (
            self.position_qty < self.max_position
            and self.gross_shares_traded < self.max_gross_shares
        )

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
            return (
                f"stale quote age={age:.1f}s max={self.max_market_stale_seconds:.1f}s"
            )

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
        sell_qty = 0
        if self.position_qty >= self.min_inventory_before_offering:
            sell_qty = min(self.base_qty, self.max_sellable_qty())
        return buy_qty, sell_qty

    def is_same_order(
        self, live: Optional[LiveOrder], side: str, qty: int, px: float
    ) -> bool:
        if live is None:
            return False
        if live.side != side or live.qty != qty:
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

            self.logger.info(
                "Cancel order id=%s side=%s px=%.2f qty=%s", oid, side, px, qty
            )
        except Exception as e:
            self.logger.exception(
                "Cancel failed for order id=%s side=%s: %s", oid, side, e
            )

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

        if self.is_same_order(self.sell_order, "SELL", qty, px):
            return

        oid = self.sell_order.order_id if self.sell_order else self.next_id()
        order = self.build_lmt_order("SELL", qty, px)
        self.placeOrder(oid, self.contract, order)
        self.sell_order = LiveOrder(order_id=oid, side="SELL", price=px, qty=qty)
        self.logger.info("SELL working id=%s qty=%s px=%.2f", oid, qty, px)

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
            now.hour == self.flatten_hour
            and now.minute >= self.flatten_minute
        )

    def force_flatten(self):
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
        
    def maybe_manage_quotes(self, force: bool = False):
        self.logger.info("PATCHED maybe_manage_quotes v6 reached")
        now = time.time()
    
        self.logger.info(
            "maybe_manage_quotes force=%s connected=%s shutdown=%s dt=%.2f pos=%s gross=%s bid=%s ask=%s",
            force,
            self.connected_flag,
            self.shutdown_flag,
            now - self.last_quote_eval,
            self.position_qty,
            self.gross_shares_traded,
            self.quote.bid,
            self.quote.ask,
        )
    
        if not self.connected_flag or self.shutdown_flag:
            return
    
        reason = self.market_invalid_reason()
        if reason is not None:
            if self.cancel_on_invalid_market:
                self.place_or_replace_buy(0, None)
                self.place_or_replace_sell(0, None)
            if self.log_invalid_market_reasons:
                self.logger.info("Not quoting: %s", reason)
            return
    
        self.logger.info(
            "Market valid; proceeding to quote evaluation refresh=%.2f",
            self.quote_refresh_seconds,
        )
    
        was_never_evaluated = (self.last_quote_eval == 0.0)
        if (
            not force
            and not was_never_evaluated
            and (now - self.last_quote_eval) < self.quote_refresh_seconds
        ):
            self.logger.info(
                "Skipping quote evaluation due to refresh throttle dt=%.3f threshold=%.3f",
                now - self.last_quote_eval,
                self.quote_refresh_seconds,
            )
            return
    
        self.last_quote_eval = now
    
        reason = self.market_invalid_reason()
        if reason is not None:
            if self.cancel_on_invalid_market:
                self.place_or_replace_buy(0, None)
                self.place_or_replace_sell(0, None)
            if self.log_invalid_market_reasons:
                self.logger.info("Not quoting: %s", reason)
                return
    
        if self.hard_flatten_mode():
            self.logger.info("self.hard_flatten_mode()")
            self.force_flatten()
            return
    
        buy_px, sell_px = self.compute_desired_quotes()
        bid = self.quote.bid
        ask = self.quote.ask
    
        self.logger.info(
            "After compute_desired_quotes buy=%s sell=%s bid=%s ask=%s",
            buy_px,
            sell_px,
            bid,
            ask,
        )
    
        if buy_px is None or sell_px is None or buy_px >= sell_px:
            self.place_or_replace_buy(0, None)
            self.place_or_replace_sell(0, None)
            self.logger.warning(
                "Computed invalid quotes buy=%s sell=%s; cancelling.",
                buy_px,
                sell_px,
            )
            return
    
        if ask is not None:
            sell_px = min(sell_px, ask)
    
        if buy_px is None or sell_px is None or buy_px >= sell_px:
            self.place_or_replace_buy(0, None)
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
    
        if self.sell_order and buy_qty > 0 and buy_px >= self.sell_order.price:
            buy_px = self.round_down_cent(self.sell_order.price - 0.01)
    
        if self.buy_order and sell_qty > 0 and sell_px <= self.buy_order.price:
            sell_px = self.round_up_cent(self.buy_order.price + 0.01)
    
        if buy_qty > 0 and sell_qty > 0 and buy_px >= sell_px:
            self.logger.warning("Final anti-self-trade guard triggered; suppressing sell quote.")
            sell_qty = 0
    
        self.logger.info(
            "Submitting quotes desired_buy=%s@%s desired_sell=%s@%s",
            buy_qty,
            buy_px,
            sell_qty,
            sell_px,
        )
    
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


def loadconfig(filepath: str) -> Dict[str, Any]:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Config file must contain a JSON object at the top level")
    return data


def clitoconfig(args: argparse.Namespace) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in vars(args).items():
        if key == "config" or value is None:
            continue
        result[key] = value
    return result


def mergeconfig(fileconfig: Dict[str, Any], cliconfig: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(fileconfig)
    merged.update(cliconfig)
    return merged


def buildargparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parameterized IBKR market maker")
    defaultconfigpath = str(Path(__file__).with_suffix(".json"))
    parser.add_argument(
        "--config",
        default=defaultconfigpath,
        help="Path to JSON config file (default: script name with .json suffix)",
    )
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--clientid", "--client-id", dest="clientid", type=int)
    parser.add_argument("--account")
    parser.add_argument("--symbol")
    parser.add_argument("--sectype")
    parser.add_argument("--exchange")
    parser.add_argument("--currency")
    parser.add_argument(
        "--primaryexchange", "--primary-exchange", dest="primaryexchange"
    )
    parser.add_argument("--baseqty", "--base-qty", dest="baseqty", type=int)
    parser.add_argument(
        "--maxposition", "--max-position", dest="maxposition", type=int
    )
    parser.add_argument(
        "--maxgrossshares", "--max-gross-shares", dest="maxgrossshares", type=int
    )
    parser.add_argument(
        "--mininventorybeforeoffering",
        "--min-inventory-before-offering",
        dest="mininventorybeforeoffering",
        type=int,
    )
    parser.add_argument("--minspread", "--min-spread", dest="minspread", type=float)
    parser.add_argument(
        "--insideimprove", "--inside-improve", dest="insideimprove", type=float
    )
    parser.add_argument(
        "--inventorypenaltyper100",
        "--inventory-penalty-per-100",
        dest="inventorypenaltyper100",
        type=float,
    )
    parser.add_argument(
        "--targetroundtripcapture",
        "--target-roundtrip-capture",
        dest="targetroundtripcapture",
        type=float,
    )
    parser.add_argument(
        "--quoterefreshseconds",
        "--quote-refresh-seconds",
        dest="quoterefreshseconds",
        type=float,
    )
    parser.add_argument(
        "--maxmarketstaleseconds",
        "--max-market-stale-seconds",
        dest="maxmarketstaleseconds",
        type=float,
    )
    parser.add_argument(
        "--endnewpositionshour",
        "--end-new-positions-hour",
        dest="endnewpositionshour",
        type=int,
    )
    parser.add_argument(
        "--endnewpositionsminute",
        "--end-new-positions-minute",
        dest="endnewpositionsminute",
        type=int,
    )
    parser.add_argument("--flattenhour", "--flatten-hour", dest="flattenhour", type=int)
    parser.add_argument(
        "--flattenminute", "--flatten-minute", dest="flattenminute", type=int
    )
    parser.add_argument(
        "--canceloninvalidmarket",
        "--cancel-on-invalid-market",
        dest="canceloninvalidmarket",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    parser.add_argument(
        "--requirelivedata",
        "--require-live-data",
        dest="requirelivedata",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    parser.add_argument("--logdir", "--log-dir", dest="logdir")
    parser.add_argument("--logfile", "--log-file", dest="logfile")
    parser.add_argument("--level")
    parser.add_argument("--maxbytes", "--max-bytes", dest="maxbytes", type=int)
    parser.add_argument(
        "--backupcount", "--backup-count", dest="backupcount", type=int
    )
    parser.add_argument(
        "--console",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    parser.add_argument(
        "--logbidaskticks",
        "--log-bid-ask-ticks",
        dest="logbidaskticks",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    parser.add_argument(
        "--loginvalidmarketreasons",
        "--log-invalid-market-reasons",
        dest="loginvalidmarketreasons",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    parser.add_argument(
        "--nbbowatchdogseconds",
        "--nbbo-watchdog-seconds",
        dest="nbbowatchdogseconds",
        type=float,
    )
    parser.add_argument(
        "--watchdogrepeatseconds",
        "--watchdog-repeat-seconds",
        dest="watchdogrepeatseconds",
        type=float,
    )
    return parser


def main():
    parser = buildargparser()
    args = parser.parse_args()

    configpath = Path(args.config)
    fileconfig = loadconfig(str(configpath))
    cliconfig = clitoconfig(args)
    config = mergeconfig(fileconfig, cliconfig)

    app = MarketMaker(config)

    def handlesig(sig, frame):
        app.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handlesig)
    signal.signal(signal.SIGTERM, handlesig)

    app.logger.info(
        "Connecting to IBKR host=%s port=%s clientid=%s",
        app.host,
        app.port,
        app.clientid,
    )
    app.connect(app.host, app.port, app.clientid)

    apithread = threading.Thread(target=app.run, name="IBAPI", daemon=True)
    apithread.start()

    watchdogthread = threading.Thread(
        target=app.watchdog_loop, name="Watchdog", daemon=True
    )
    watchdogthread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        handlesig(None, None)


if __name__ == "__main__":
    main()