"""IBKR Pegged-to-Primary (Relative / ``REL``) quoter.

Flat: one REL buy up to ``max_pos``. Partial inventory (``0 < pos < max_pos``):
REL buy for remaining capacity and REL sell for the current position. At or
above ``max_pos``: sell-only for the full position. Mutually exclusive buy/sell
when flat or capped; both sides may rest when partially filled.

Protective REL ``lmtPrice`` values use the NBBO **mid** only (``(bid+ask)/2``
rounded like ``ref_price`` in :meth:`tickPrice`): buy ceiling ``mid - d`` and
sell floor ``mid + d`` where ``d = max(mid_delta, one_tick)``. ``mid_delta``
defaults to ``0.02``; raising ``d`` to at least one price tick
(``10 ** -price_round_digits``) keeps the rounded buy cap strictly below mid
and the sell floor strictly above mid.

REL ``auxPrice`` is ``(ask - bid) * offset_pct`` (``offset_pct`` clamped below
``0.5``), rounded with ``price_round_digits``.

The bot does not place, modify, or cancel orders until both bid and ask are valid
(positive and ask > bid). Orders are
not submitted when computed quantity is below ``min_order_size`` (default 10).

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
import json
import logging
import logging.handlers
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, Tuple

import pytz

from ibapi.client import EClient
from ibapi.common import TickAttrib, TickerId
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.ticktype import TickType
from ibapi.wrapper import EWrapper

HIST_REQ_ID = 1001
MKTDATA_REQ_ID = 3001


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


def _build_logger(config: Dict[str, Any]) -> logging.Logger:
    """Configure a dedicated logger (file + optional console), same style as ``market_maker``."""
    log_dir = Path(str(config.get("log_dir", "logs")))
    log_file = str(config.get("log_file", "peg_primary.log"))
    level_name = str(config.get("level", "INFO")).upper()
    max_bytes = int(config.get("max_bytes", 5_000_000))
    backup_count = int(config.get("backup_count", 10))
    use_console = _cfg_bool(config, "console", True)

    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("peg_primary")
    logger.setLevel(getattr(logging, level_name, logging.INFO))
    for h in list(logger.handlers):
        logger.removeHandler(h)
        h.close()
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


def load_config_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Config file must contain a JSON object at the top level")
    return data


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


def require_fields(config: Dict[str, Any], required_fields: list[str]) -> None:
    missing = [field for field in required_fields if field not in config]
    if missing:
        raise ValueError(f"Missing required configuration fields: {', '.join(missing)}")


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


class Trader(EWrapper, EClient):
    def __init__(self, config: Dict[str, Any]):
        EClient.__init__(self, self)
        self.config = config

        self.ready_for_trading = False
        self.nextOrderId: int | None = None

        self.contract = self.make_us_stock(
            self.config["symbol"],
            self.config["sec_type"],
            self.config["currency"],
            self.config["exchange"],
            self.config["primary_exchange"],
        )

        self.position_size = 0
        self.open_price = None
        self._bars: list[Any] = []

        self.buy_order_id: int | None = None
        self.sell_order_id: int | None = None
        self.buy_order_qty: int | None = None
        self.sell_order_qty: int | None = None

        self.ref_price = None
        self._bid = None
        self._ask = None

        self.remaining_by_order: Dict[int, Any] = {}
        self.filled_by_order: Dict[int, float] = {}
        self.open_symbol_buys = 0
        self.open_symbol_sells = 0
        self.pending_buy = False
        self.pending_sell = False
        self.position_snapshot_complete = False
        self.open_orders_snapshot_complete = False
        self.sync_requested = False
        self.last_resync_request_ts = 0.0
        self.resync_debounce_seconds = float(self.config.get("resync_debounce_seconds", 0.35))
        _mos = int(self.config.get("min_order_size", 10))
        self.min_order_size = max(1, _mos)

        self._digits = int(self.config["price_round_digits"])
        self._tick = 10.0 ** (-self._digits)

        self._snapshot_open_order_lines: list[str] = []
        self._startup_open_orders_logged = False
        self.order_working_lmt: Dict[int, float] = {}
        self.order_working_aux: Dict[int, float] = {}

        self.logger = _build_logger(config)
        self.logger.info(
            "REL quoter initialized symbol=%s exchange=%s",
            self.config["symbol"],
            self.config["exchange"],
        )

    def make_us_stock(
        self, symbol: str, sec_type: str, currency: str, exchange: str, primary_exchange: str
    ) -> Contract:
        c = Contract()
        c.symbol = symbol
        c.secType = sec_type
        c.currency = currency
        c.exchange = exchange
        c.primaryExch = primary_exchange
        return c

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
        if (
            contract.symbol != self.config["symbol"]
            or contract.secType != self.config["sec_type"]
        ):
            return

        raw_side = getattr(execution, "side", "")
        side = "BUY" if raw_side == "BOT" else "SELL" if raw_side == "SLD" else raw_side
        self.logger.info(
            f"Execution {side} orderId={execution.orderId} execId={execution.execId} "
            f"shares={execution.shares} price={execution.price} "
            f"exchange={execution.exchange} cumQty={execution.cumQty} "
            f"avgPrice={execution.avgPrice} time={execution.time}"
        )

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib):
        if reqId != MKTDATA_REQ_ID:
            return

        if tickType == 1:
            self._bid = price
        elif tickType == 2:
            self._ask = price

        if self._bid is not None and self._ask is not None and self._bid > 0 and self._ask > 0:
            mid = self._nbbo_mid_rounded()
            if mid != self.ref_price:
                self.logger.info(
                    f"ref_price updated: bid={self._bid} ask={self._ask} mid={mid}"
                )
                self.ref_price = mid

    def nextValidId(self, orderId: int):
        GENERIC_TICKS = ""

        self.logger.info("nextValidId: %s", orderId)
        self.nextOrderId = orderId
        self._startup_open_orders_logged = False

        self.request_positions_snapshot()
        self.request_today_open_or_prior_close()
        self.reqMktData(MKTDATA_REQ_ID, self.contract, GENERIC_TICKS, False, False, [])
        self.request_open_orders_snapshot()

    def openOrder(self, orderId, contract, order, orderState):
        if contract.symbol == self.config["symbol"] and contract.secType == self.config["sec_type"]:
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

    def openOrderEnd(self):
        self.open_orders_snapshot_complete = True
        self.ready_for_trading = self.position_snapshot_complete and self.open_orders_snapshot_complete
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
        self.maybe_sync_orders()

    def error(self, reqId, errorTime, errorCode, errorString, advancedOrderRejectJson=""):
        noisy_codes = {str(x) for x in self.config["ignored_error_codes"]}
        if str(errorCode) in noisy_codes:
            return

        ignored_substrings = [str(x) for x in self.config["ignore_error_substrings"]]
        error_text = str(errorString)
        if any(text in error_text for text in ignored_substrings):
            return

        self.logger.warning(
            "Error reqId=%s errorTime=%s errorCode=%s errorString=%s advancedOrderRejectJson=%s",
            reqId,
            errorTime,
            errorCode,
            error_text,
            advancedOrderRejectJson,
        )

    def position(self, account, contract, pos, avgCost):
        if contract.symbol == self.config["symbol"] and contract.secType == self.config["sec_type"]:
            self.position_size = int(pos)

    def positionEnd(self):
        self.position_snapshot_complete = True
        self.ready_for_trading = self.position_snapshot_complete and self.open_orders_snapshot_complete
        self.maybe_sync_orders()

    def request_today_open_or_prior_close(self):
        self.logger.info("Requesting daily bars for open/prior close")
        self._bars = []
        self.reqHistoricalData(
            HIST_REQ_ID,
            self.contract,
            "",
            "2 D",
            "1 day",
            "TRADES",
            1,
            1,
            False,
            [],
        )

    def historicalData(self, reqId, bar):
        if reqId != HIST_REQ_ID:
            return
        self._bars.append(bar)

    def historicalDataEnd(self, reqId, start, end):
        if reqId != HIST_REQ_ID:
            return

        if not self._bars:
            self.logger.info("No historical bars returned; cannot set open_price")
            return

        last_bar = self._bars[-1]

        if last_bar.open and last_bar.open > 0:
            self.open_price = last_bar.open
            self.logger.info("Today's open price: %s", self.open_price)
        else:
            self.open_price = last_bar.close
            self.logger.info("No valid open; using prior close: %s", self.open_price)

        if self.ref_price is None:
            self.ref_price = self.open_price

    def us_regular_hours(self) -> bool:
        ny = pytz.timezone(self.config["market_timezone"])
        now = datetime.datetime.now(tz=ny)
        market_open_hour = self.config["market_open_hour"]
        market_open_minute = self.config["market_open_minute"]
        market_close_hour = self.config["market_close_hour"]

        return (
            now.hour > market_open_hour
            or (now.hour == market_open_hour and now.minute >= market_open_minute)
        ) and now.hour < market_close_hour

    def _has_valid_nbbo(self) -> bool:
        bid = self._bid
        ask = self._ask
        return (
            bid is not None
            and ask is not None
            and float(bid) > 0
            and float(ask) > 0
            and float(ask) > float(bid)
        )

    def _nbbo_mid_rounded(self) -> float:
        """NBBO mid rounded to :attr:`_digits`. Caller must ensure :meth:`_has_valid_nbbo`."""
        return round((float(self._bid) + float(self._ask)) / 2.0, self._digits)

    def safe_cancel_order(self, order_id: int) -> None:
        try:
            self.cancelOrder(order_id)
        except TypeError:
            self.cancelOrder(order_id, "")

    def adjusted_rel_limits(self) -> tuple[float, float]:
        """REL buy/sell ``lmtPrice`` from NBBO mid and ``mid_delta``.

        ``buy_limit = round(mid - d, digits)``, ``sell_limit = round(mid + d, digits)``
        with ``mid`` from :meth:`_nbbo_mid_rounded` and ``d = max(mid_delta, _tick)``
        so the buy cap stays strictly below ``mid`` and the sell floor strictly
        above ``mid``. Caller must ensure :meth:`_has_valid_nbbo` is true.
        """
        assert self._has_valid_nbbo()
        mid_delta = max(float(self.config.get("mid_delta", 0.02)), self._tick)
        mid = self._nbbo_mid_rounded()
        buy_limit = round(mid - mid_delta, self._digits)
        sell_limit = round(mid + mid_delta, self._digits)
        return buy_limit, sell_limit

    def request_positions_snapshot(self):
        self.position_snapshot_complete = False
        self.position_size = 0
        self.reqPositions()

    def request_open_orders_snapshot(self):
        self.open_orders_snapshot_complete = False
        self.open_symbol_buys = 0
        self.open_symbol_sells = 0
        self._snapshot_open_order_lines = []
        self.order_working_lmt.clear()
        self.order_working_aux.clear()
        self.reqOpenOrders()

    def maybe_sync_orders(self):
        if self.sync_requested and self.position_snapshot_complete and self.open_orders_snapshot_complete:
            self.sync_requested = False
            self.sync_orders()

    def _clear_order_state(self, action: str) -> None:
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
        remaining_raw = self.remaining_by_order.get(oid)
        if remaining_raw is None:
            return False
        try:
            remaining_now = int(float(remaining_raw))
        except (TypeError, ValueError):
            return False

        if remaining_now == desired_remaining:
            return False

        filled_now = max(0, current_total_qty - remaining_now)

        if desired_remaining < self.min_order_size:
            self.logger.info(
                f"Cancelling {action} id={oid} (desired remaining "
                f"{desired_remaining} < min_order_size {self.min_order_size}; "
                f"filled={filled_now})"
            )
            self.safe_cancel_order(oid)
            self._clear_order_state(action)
            return True

        new_total = filled_now + desired_remaining
        self.logger.info(
            f"Modifying {action} id={oid} totalQty {current_total_qty}->{new_total} "
            f"(filled={filled_now} remaining {remaining_now}->{desired_remaining} "
            f"lmtPrice={limit_price})"
        )
        order = self.build_rel_order(action, new_total, limit_price)
        self.placeOrder(oid, self.contract, order)
        self._note_order_cache(oid, order)
        if action == "BUY":
            self.buy_order_qty = new_total
        else:
            self.sell_order_qty = new_total
        return True

    def _note_order_cache(self, oid: int, order: Order) -> None:
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

    def trigger_resync(self):
        if not self.isConnected():
            return

        now = time.monotonic()
        if now - self.last_resync_request_ts < self.resync_debounce_seconds:
            return

        self.last_resync_request_ts = now
        self.sync_requested = True
        self.request_positions_snapshot()
        self.request_open_orders_snapshot()
        self.maybe_sync_orders()

    def sync_orders(self):
        if not self.ready_for_trading:
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
                self.safe_cancel_order(sid)
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
                self.safe_cancel_order(buy_oid)
                self.pending_buy = False
                self.buy_order_id = None
                self.buy_order_qty = None
                self.open_symbol_buys = 0
                self.order_working_lmt.pop(buy_oid, None)
                self.order_working_aux.pop(buy_oid, None)
                return

            if self.open_symbol_buys > 0:
                return

            sell_qty = pos
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
        sell_qty = pos

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

    def run_loop(self):
        while True:
            if not self.isConnected():
                self.logger.info("Disconnected from IBKR; exiting run loop")
                break
            if self.us_regular_hours():
                self.sync_requested = True
                self.request_positions_snapshot()
                self.request_open_orders_snapshot()
                self.maybe_sync_orders()
            time.sleep(float(self.config["loop_seconds"]))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="IBKR Pegged-to-Primary (REL) quoter — NBBO pegging is exchange-side"
    )
    default_config_path = str(Path(__file__).with_suffix(".json"))
    parser.add_argument(
        "--config",
        default=default_config_path,
        help="Path to JSON config file (default: script name with .json suffix)",
    )

    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--symbol")
    parser.add_argument("--sec_type")
    parser.add_argument("--currency")
    parser.add_argument("--exchange")
    parser.add_argument("--primary_exchange")
    parser.add_argument("--max_pos", type=int)
    parser.add_argument("--loop_seconds", type=float)
    parser.add_argument("--min_order_size", type=int)
    parser.add_argument("--market_timezone")
    parser.add_argument("--market_open_hour", type=int)
    parser.add_argument("--market_open_minute", type=int)
    parser.add_argument("--market_close_hour", type=int)
    parser.add_argument("--tif")
    parser.add_argument("--price_round_digits", type=int)
    parser.add_argument("--resync_debounce_seconds", type=float)
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

    cli_flags: Tuple[Tuple[str, Dict[str, Any]], ...] = (
        ("--log_dir", {}),
        ("--log_file", {}),
        ("--level", {}),
        ("--max_bytes", {"type": int}),
        ("--backup_count", {"type": int}),
        ("--console", {"action": argparse.BooleanOptionalAction}),
    )
    for flag, kwargs in cli_flags:
        parser.add_argument(flag, **kwargs)
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    config_path = Path(args.config)
    file_config = load_config_file(str(config_path))
    cli_config = cli_to_config(args)
    config = merge_config(file_config, cli_config)

    required_fields = [
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
    ]
    require_fields(config, required_fields)

    app = Trader(config)
    client_id = int(config.get("client_id", 1))
    app.connect(config["host"], config["port"], client_id)

    thread = threading.Thread(target=app.run, daemon=True)
    thread.start()

    try:
        app.run_loop()
    except KeyboardInterrupt:
        app.logger.info("Stopping...")
    finally:
        app.disconnect()


if __name__ == "__main__":
    main()
