from __future__ import annotations

import argparse
import datetime
import json
import pytz
import threading
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict

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


class Trader(EWrapper, EClient):
    def __init__(self, config: Dict[str, Any]):
        EClient.__init__(self, self)
        self.config = config

        self.ready_for_trading = False
        self.nextOrderId = None

        self.contract = self.make_us_stock(
            self.config["symbol"],
            self.config["sec_type"],
            self.config["currency"],
            self.config["exchange"],
            self.config["primary_exchange"],
        )

        self.position_size = 0
        self.open_price = None
        self._bars = []

        self.buy_order_id = None
        self.sell_order_id = None

        # ref_price tracks midpoint (or last trade) and is used to build
        # the MidPrice caps/floors.
        self.ref_price = None
        self._bid = None
        self._ask = None

        self.remaining_by_order = {}
        self.open_symbol_buys = 0
        self.open_symbol_sells = 0
        self.pending_buy = False
        self.pending_sell = False

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
        if status in ("Filled", "Cancelled"):
            if orderId == self.buy_order_id:
                tprint(f"BUY {orderId} {status} @ {avgFillPrice}")
                self.pending_buy = False
                self.buy_order_id = None
            if orderId == self.sell_order_id:
                tprint(f"SELL {orderId} {status} @ {avgFillPrice}")
                self.pending_sell = False
                self.sell_order_id = None

            self.open_symbol_buys = 0
            self.open_symbol_sells = 0
            self.reqOpenOrders()

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

        if tickType == 1:  # bid
            self._bid = price
        elif tickType == 2:  # ask
            self._ask = price

        if self._bid is not None and self._ask is not None and self._bid > 0 and self._ask > 0:
            mid = (self._bid + self._ask) / 2.0
            if mid != self.ref_price:
                tprint(f"ref_price updated: bid={self._bid} ask={self._ask} mid={mid}")
                self.ref_price = mid

    def make_us_stock(self, symbol: str, sec_type: str, currency: str, exchange: str, primary_exchange: str) -> Contract:
        c = Contract()
        c.symbol = symbol
        c.secType = sec_type
        c.currency = currency
        c.exchange = exchange
        c.primaryExch = primary_exchange
        return c

    def nextValidId(self, orderId):
        GENERIC_TICKS = ""
        TICK_BY_TICK_TYPE = "Last"

        tprint(f"nextValidId: {orderId}")
        self.nextOrderId = orderId

        self.reqPositions()
        self.request_today_open_or_prior_close()
        self.reqMktData(MKTDATA_REQ_ID, self.contract, GENERIC_TICKS, False, False, [])

        self.open_symbol_buys = 0
        self.open_symbol_sells = 0
        self.reqOpenOrders()

        self.reqTickByTickData(
            LAST_TRADE_REQ_ID,
            self.contract,
            TICK_BY_TICK_TYPE,
            0,   # number of ticks
            False,  # ignore size
        )

    def openOrder(self, orderId, contract, order, orderState):
        if contract.symbol == self.config["symbol"] and contract.secType == self.config["sec_type"]:
            if order.action == "BUY":
                self.open_symbol_buys += 1
                self.buy_order_id = orderId
            elif order.action == "SELL":
                self.open_symbol_sells += 1
                self.sell_order_id = orderId

    def openOrderEnd(self):
        self.ready_for_trading = True
        tprint(f"Open {self.config['symbol']} orders: buys={self.open_symbol_buys}, sells={self.open_symbol_sells}")

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
        return

    def request_today_open_or_prior_close(self):
        tprint("Requesting daily bars for open/prior close")
        self._bars = []
        self.reqHistoricalData(
            HIST_REQ_ID,
            self.contract,
            "",        # endDateTime
            "2 D",     # durationStr
            "1 day",   # barSizeSetting
            "TRADES",  # whatToShow
            1,         # useRTH
            1,         # formatDate
            False,     # keepUpToDate
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

    def us_regular_hours(self):
        ny = pytz.timezone(self.config["market_timezone"])
        now = datetime.datetime.now(tz=ny)
        market_open_hour = self.config["market_open_hour"]
        market_open_minute = self.config["market_open_minute"]
        market_close_hour = self.config["market_close_hour"]

        return (
            (now.hour > market_open_hour or (now.hour == market_open_hour and now.minute >= market_open_minute))
            and now.hour < market_close_hour
        )

    def make_midprice_order(self, action, qty, limit_price):
        o = Order()
        o.action = action
        o.orderType = "MIDPRICE"
        o.totalQuantity = qty
        o.lmtPrice = round(limit_price, int(self.config["price_round_digits"]))
        o.exchange = self.config["exchange"]
        o.tif = self.config["tif"]
        return o

    def sync_orders(self):
        if not self.ready_for_trading:
            return

        if self.ref_price is None or self.nextOrderId is None:
            return

        pos = self.position_size

        # MidPrice orders track midpoint and use lmtPrice as buy cap / sell floor.
        buy_limit = round(self.ref_price - float(self.config["buy_delta"]), int(self.config["price_round_digits"]))
        sell_limit = round(self.ref_price + float(self.config["sell_delta"]), int(self.config["price_round_digits"]))

        if pos <= 0:
            desired_side = "BUY"
            desired_qty = self.config["max_pos"]
            desired_limit = buy_limit
        else:
            desired_side = "SELL"
            desired_qty = pos
            desired_limit = sell_limit

        if desired_side == "BUY":
            if self.sell_order_id is not None:
                tprint(f"Cancelling opposite SELL order id={self.sell_order_id}")
                # self.cancelOrder(self.sell_order_id, "")
                self.cancelOrder(self.sell_order_id)
                self.pending_sell = False
                self.sell_order_id = None
                self.open_symbol_sells = 0
                return

            if self.open_symbol_sells > 0:
                return

            if desired_qty > 0 and self.open_symbol_buys == 0 and not self.pending_buy:
                oid = self.nextOrderId
                self.nextOrderId += 1
                self.buy_order_id = oid
                self.pending_buy = True

                order = self.make_midprice_order("BUY", desired_qty, desired_limit)
                tprint(f"Placing MIDPRICE BUY {desired_qty} cap={desired_limit}, id={oid}")
                self.placeOrder(oid, self.contract, order)
        else:
            if self.buy_order_id is not None:
                tprint(f"Cancelling opposite BUY order id={self.buy_order_id}")
                # self.cancelOrder(self.buy_order_id, "")
                self.cancelOrder(self.buy_order_id)
                self.pending_buy = False
                self.buy_order_id = None
                self.open_symbol_buys = 0
                return

            if self.open_symbol_buys > 0:
                return

            if desired_qty > 0 and self.open_symbol_sells == 0 and not self.pending_sell:
                oid = self.nextOrderId
                self.nextOrderId += 1
                self.sell_order_id = oid
                self.pending_sell = True

                order = self.make_midprice_order("SELL", desired_qty, desired_limit)
                tprint(f"Placing MIDPRICE SELL {desired_qty} floor={desired_limit}, id={oid}")
                self.placeOrder(oid, self.contract, order)

    def run_loop(self):
        while True:
            if not self.isConnected():
                tprint("Disconnected from IBKR; exiting run loop")
                break
            if self.us_regular_hours():
                self.reqPositions()
                self.sync_orders()
            time.sleep(self.config["loop_seconds"])


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parameterized IBKR MidPrice single-side trader")
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
    parser.add_argument("--market_timezone")
    parser.add_argument("--market_open_hour", type=int)
    parser.add_argument("--market_open_minute", type=int)
    parser.add_argument("--market_close_hour", type=int)
    parser.add_argument("--last_trade_min_size", type=float)
    parser.add_argument("--tif")
    parser.add_argument("--price_round_digits", type=int)
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
    CLIENT_ID = 1
    app.connect(config["host"], config["port"], CLIENT_ID)

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