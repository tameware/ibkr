"""IBKR Pegged-to-Primary (Relative / ``REL``) quoter.

Flat: one REL buy up to ``max_pos``. Partial inventory (``0 < pos < max_pos``):
REL buy for remaining capacity and REL sell for the current position. At or
above ``max_pos``: sell-only for the full position. Mutually exclusive buy/sell
when flat or capped; both sides may rest when partially filled.

Protective REL ``lmtPrice`` values are derived from ``ref_price`` and deltas,
then adjusted so ``sell_limit - buy_limit >= min_spread`` (same idea as
``min_quote_spread_cents`` / working width in ``market_maker.py``). Orders are
not submitted when computed quantity is below ``min_order_size`` (default 10).

The TWS API exposes Pegged-to-Primary as ``orderType = "REL"`` with ``lmtPrice``
as the protective cap (buy ceiling / sell floor) and ``auxPrice`` as the offset
from the primary quote — IB adjusts the working price with the NBBO; the client
does not need a tick-improve loop like ``market_maker.py``.

See: https://www.interactivebrokers.com/campus/ibkr-api-page/order-types/
(section *Relative (a.k.a. Pegged-to-Primary)*).
"""

from __future__ import annotations

import argparse
import datetime
import json
import threading
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict

import pytz

from ibapi.client import EClient
from ibapi.common import TickAttrib, TickAttribLast, TickerId
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.ticktype import TickType
from ibapi.wrapper import EWrapper

HIST_REQ_ID = 1001
LAST_TRADE_REQ_ID = 2001
MKTDATA_REQ_ID = 3001


def tprint(msg: str) -> None:
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} {msg}")


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
    aux_round_digits: int,
) -> Order:
    o = Order()
    o.action = action
    o.orderType = "REL"
    o.totalQuantity = qty
    o.lmtPrice = round(limit_price, price_round_digits)
    o.auxPrice = round(offset, aux_round_digits)
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

    def _rel_offset_buy(self) -> float:
        return float(self.config.get("rel_offset_buy", self.config["rel_offset"]))

    def _rel_offset_sell(self) -> float:
        return float(self.config.get("rel_offset_sell", self.config["rel_offset"]))

    def build_rel_order(self, action: str, qty: int, limit_price: float) -> Order:
        off = self._rel_offset_buy() if action == "BUY" else self._rel_offset_sell()
        return make_rel_order(
            action=action,
            qty=qty,
            limit_price=limit_price,
            offset=off,
            exchange=self.config["exchange"],
            tif=self.config["tif"],
            price_round_digits=int(self.config["price_round_digits"]),
            aux_round_digits=int(self.config.get("aux_round_digits", self.config["price_round_digits"])),
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
                tprint(
                    f"{side} {orderId} partial fill: filled={filled} remaining={remaining} "
                    f"avgFillPrice={avgFillPrice} lastFillPrice={lastFillPrice}"
                )
                self.filled_by_order[orderId] = filled_f
                self.trigger_resync()

        if status in ("Filled", "Cancelled", "Inactive", "ApiCancelled"):
            if orderId == self.buy_order_id:
                tprint(
                    f"BUY {orderId} {status} filled={filled} avgFillPrice={avgFillPrice}"
                )
                self.pending_buy = False
                self.buy_order_id = None
                self.buy_order_qty = None
            if orderId == self.sell_order_id:
                tprint(
                    f"SELL {orderId} {status} filled={filled} avgFillPrice={avgFillPrice}"
                )
                self.pending_sell = False
                self.sell_order_id = None
                self.sell_order_qty = None
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
        tprint(
            f"Execution {side} orderId={execution.orderId} execId={execution.execId} "
            f"shares={execution.shares} price={execution.price} "
            f"exchange={execution.exchange} cumQty={execution.cumQty} "
            f"avgPrice={execution.avgPrice} time={execution.time}"
        )

    def tickByTickAllLast(
        self,
        reqId: int,
        tickType: int,
        time_value: int,
        price: float,
        size: Decimal,
        tickAttribLast: TickAttribLast,
        exchange: str,
        specialConditions: str,
    ):
        if reqId != LAST_TRADE_REQ_ID:
            return

        if size is None:
            return

        if float(size) >= float(self.config["last_trade_min_size"]):
            if self.ref_price != price:
                self.ref_price = price
                tprint(f"New ref_price from last trade: {price} size={size}")

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib):
        if reqId != MKTDATA_REQ_ID:
            return

        if tickType == 1:
            self._bid = price
        elif tickType == 2:
            self._ask = price

        if self._bid is not None and self._ask is not None and self._bid > 0 and self._ask > 0:
            mid = (self._bid + self._ask) / 2.0
            if mid != self.ref_price:
                tprint(f"ref_price updated: bid={self._bid} ask={self._ask} mid={mid}")
                self.ref_price = mid

    def nextValidId(self, orderId: int):
        GENERIC_TICKS = ""
        TICK_BY_TICK_TYPE = "Last"

        tprint(f"nextValidId: {orderId}")
        self.nextOrderId = orderId

        self.request_positions_snapshot()
        self.request_today_open_or_prior_close()
        self.reqMktData(MKTDATA_REQ_ID, self.contract, GENERIC_TICKS, False, False, [])
        self.request_open_orders_snapshot()

        self.reqTickByTickData(
            LAST_TRADE_REQ_ID,
            self.contract,
            TICK_BY_TICK_TYPE,
            0,
            False,
        )

    def openOrder(self, orderId, contract, order, orderState):
        if contract.symbol == self.config["symbol"] and contract.secType == self.config["sec_type"]:
            try:
                qty = int(float(order.totalQuantity))
            except (TypeError, ValueError):
                qty = None
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
        self.maybe_sync_orders()

    def error(self, reqId, errorTime, errorCode, errorString, advancedOrderRejectJson=""):
        noisy_codes = {str(x) for x in self.config["ignored_error_codes"]}
        if str(errorCode) in noisy_codes:
            return

        ignored_substrings = [str(x) for x in self.config["ignore_error_substrings"]]
        error_text = str(errorString)
        if any(text in error_text for text in ignored_substrings):
            return

        tprint(
            f"Error reqId={reqId} errorTime={errorTime} errorCode={errorCode} "
            f"errorString={error_text} advancedOrderRejectJson={advancedOrderRejectJson}"
        )

    def position(self, account, contract, pos, avgCost):
        if contract.symbol == self.config["symbol"] and contract.secType == self.config["sec_type"]:
            self.position_size = int(pos)

    def positionEnd(self):
        self.position_snapshot_complete = True
        self.ready_for_trading = self.position_snapshot_complete and self.open_orders_snapshot_complete
        self.maybe_sync_orders()

    def request_today_open_or_prior_close(self):
        tprint("Requesting daily bars for open/prior close")
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
            tprint("No historical bars returned; cannot set open_price")
            return

        last_bar = self._bars[-1]

        if last_bar.open and last_bar.open > 0:
            self.open_price = last_bar.open
            tprint(f"Today's open price: {self.open_price}")
        else:
            self.open_price = last_bar.close
            tprint(f"No valid open; using prior close: {self.open_price}")

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

    def safe_cancel_order(self, order_id: int) -> None:
        try:
            self.cancelOrder(order_id)
        except TypeError:
            self.cancelOrder(order_id, "")

    def adjusted_rel_limits(self) -> tuple[float, float]:
        """REL buy/sell ``lmtPrice`` pair from ``ref_price`` and deltas.

        ``buy_limit`` / ``sell_limit`` follow ``peg_mid`` style:
        ``buy_limit = ref - buy_delta``, ``sell_limit = ref + sell_delta``.
        Then widen symmetrically so ``sell_limit - buy_limit`` is at least
        ``min_spread`` (and non-negative when ``min_spread`` is 0), then round.
        If rounding tightens the gap, nudge ``sell_limit`` up by one price step.
        """
        ref = float(self.ref_price)
        digits = int(self.config["price_round_digits"])
        step = 10.0 ** (-digits)
        buy_limit = ref - float(self.config["buy_delta"])
        sell_limit = ref + float(self.config["sell_delta"])
        target_gap = max(0.0, float(self.config["min_spread"]))
        gap = sell_limit - buy_limit
        if gap < target_gap:
            deficit = target_gap - gap
            buy_limit -= deficit / 2.0
            sell_limit += deficit / 2.0

        buy_limit = round(buy_limit, digits)
        sell_limit = round(sell_limit, digits)
        for _ in range(10_000):
            if sell_limit - buy_limit + 1e-12 >= target_gap:
                break
            sell_limit = round(sell_limit + step, digits)
        return buy_limit, sell_limit

    def request_positions_snapshot(self):
        self.position_snapshot_complete = False
        self.position_size = 0
        self.reqPositions()

    def request_open_orders_snapshot(self):
        self.open_orders_snapshot_complete = False
        self.open_symbol_buys = 0
        self.open_symbol_sells = 0
        self.reqOpenOrders()

    def maybe_sync_orders(self):
        if self.sync_requested and self.position_snapshot_complete and self.open_orders_snapshot_complete:
            self.sync_requested = False
            self.sync_orders()

    def _clear_order_state(self, action: str) -> None:
        if action == "BUY":
            self.pending_buy = False
            self.buy_order_id = None
            self.buy_order_qty = None
            self.open_symbol_buys = 0
        else:
            self.pending_sell = False
            self.sell_order_id = None
            self.sell_order_qty = None
            self.open_symbol_sells = 0

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
            tprint(
                f"Cancelling {action} id={oid} (desired remaining "
                f"{desired_remaining} < min_order_size {self.min_order_size}; "
                f"filled={filled_now})"
            )
            self.safe_cancel_order(oid)
            self._clear_order_state(action)
            return True

        new_total = filled_now + desired_remaining
        tprint(
            f"Modifying {action} id={oid} totalQty {current_total_qty}->{new_total} "
            f"(filled={filled_now} remaining {remaining_now}->{desired_remaining} "
            f"lmtPrice={limit_price})"
        )
        order = self.build_rel_order(action, new_total, limit_price)
        self.placeOrder(oid, self.contract, order)
        if action == "BUY":
            self.buy_order_qty = new_total
        else:
            self.sell_order_qty = new_total
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

        if self.ref_price is None or self.nextOrderId is None:
            return

        pos = self.position_size
        max_pos = int(self.config["max_pos"])

        buy_limit, sell_limit = self.adjusted_rel_limits()

        if pos <= 0:
            if self.sell_order_id is not None:
                tprint(f"Cancelling SELL order id={self.sell_order_id} (flat or flat target)")
                self.safe_cancel_order(self.sell_order_id)
                self.pending_sell = False
                self.sell_order_id = None
                self.sell_order_qty = None
                self.open_symbol_sells = 0
                return

            if self.open_symbol_sells > 0:
                return

            buy_qty = max_pos
            if self.buy_order_id is not None and self.buy_order_qty is not None:
                self._maybe_resize_existing(
                    action="BUY",
                    oid=self.buy_order_id,
                    current_total_qty=self.buy_order_qty,
                    desired_remaining=buy_qty,
                    limit_price=buy_limit,
                )
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
                tprint(
                    f"Placing REL (peg primary) BUY qty={buy_qty} lmtPrice={buy_limit} "
                    f"auxPrice={off} id={oid}"
                )
                self.placeOrder(oid, self.contract, order)
            return

        if pos >= max_pos:
            if self.buy_order_id is not None:
                tprint(f"Cancelling BUY order id={self.buy_order_id} (at or above max_pos)")
                self.safe_cancel_order(self.buy_order_id)
                self.pending_buy = False
                self.buy_order_id = None
                self.buy_order_qty = None
                self.open_symbol_buys = 0
                return

            if self.open_symbol_buys > 0:
                return

            sell_qty = pos
            if self.sell_order_id is not None and self.sell_order_qty is not None:
                self._maybe_resize_existing(
                    action="SELL",
                    oid=self.sell_order_id,
                    current_total_qty=self.sell_order_qty,
                    desired_remaining=sell_qty,
                    limit_price=sell_limit,
                )
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
                tprint(
                    f"Placing REL (peg primary) SELL qty={sell_qty} lmtPrice={sell_limit} "
                    f"auxPrice={off} id={oid}"
                )
                self.placeOrder(oid, self.contract, order)
            return

        # 0 < pos < max_pos: quote both add (room to max_pos) and exit (current position).
        buy_qty = max_pos - pos
        sell_qty = pos

        if self.buy_order_id is not None and self.buy_order_qty is not None:
            self._maybe_resize_existing(
                action="BUY",
                oid=self.buy_order_id,
                current_total_qty=self.buy_order_qty,
                desired_remaining=buy_qty,
                limit_price=buy_limit,
            )
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
            tprint(
                f"Placing REL (peg primary) BUY qty={buy_qty} lmtPrice={buy_limit} "
                f"auxPrice={off} id={oid}"
            )
            self.placeOrder(oid, self.contract, order)

        if self.sell_order_id is not None and self.sell_order_qty is not None:
            self._maybe_resize_existing(
                action="SELL",
                oid=self.sell_order_id,
                current_total_qty=self.sell_order_qty,
                desired_remaining=sell_qty,
                limit_price=sell_limit,
            )
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
            tprint(
                f"Placing REL (peg primary) SELL qty={sell_qty} lmtPrice={sell_limit} "
                f"auxPrice={off} id={oid}"
            )
            self.placeOrder(oid, self.contract, order)

    def run_loop(self):
        while True:
            if not self.isConnected():
                tprint("Disconnected from IBKR; exiting run loop")
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
    parser.add_argument("--buy_delta", type=float)
    parser.add_argument("--sell_delta", type=float)
    parser.add_argument("--min_spread", type=float)
    parser.add_argument("--min_order_size", type=int)
    parser.add_argument("--rel_offset", type=float)
    parser.add_argument("--rel_offset_buy", type=float)
    parser.add_argument("--rel_offset_sell", type=float)
    parser.add_argument("--market_timezone")
    parser.add_argument("--market_open_hour", type=int)
    parser.add_argument("--market_open_minute", type=int)
    parser.add_argument("--market_close_hour", type=int)
    parser.add_argument("--last_trade_min_size", type=float)
    parser.add_argument("--tif")
    parser.add_argument("--price_round_digits", type=int)
    parser.add_argument("--aux_round_digits", type=int)
    parser.add_argument("--resync_debounce_seconds", type=float)
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
        "buy_delta",
        "sell_delta",
        "min_spread",
        "rel_offset",
        "market_timezone",
        "market_open_hour",
        "market_open_minute",
        "market_close_hour",
        "last_trade_min_size",
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
        tprint("Stopping...")
    finally:
        app.disconnect()


if __name__ == "__main__":
    main()
