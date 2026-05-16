from __future__ import annotations

import argparse
import threading
import time
from decimal import Decimal
from typing import Any, Dict

from ibapi.client import EClient
from ibapi.common import TickAttrib, TickAttribLast, TickerId
from ibapi.contract import Contract
from ibapi.order import Order, COMPETE_AGAINST_BEST_OFFSET_UP_TO_MID
from ibapi.ticktype import TickType
from ibapi.wrapper import EWrapper

from ibkr_app_support import (
    PositionLedger,
    add_config_argument,
    add_ib_connection_arguments,
    add_logging_arguments,
    add_session_hours_arguments,
    build_logger,
    handle_ledger_execution,
    handle_ledger_ib_position,
    ib_client_id_from_config,
    clamp_buy_to_avoid_self_trade,
    clamp_sell_to_avoid_self_trade,
    load_merged_config,
    log_ib_error,
    log_startup_timezones,
    max_sell_shares,
    make_stock_contract,
    regular_session_open,
    safe_cancel_order,
    should_suppress_ib_error,
    sync_attrs_from_ledger,
)

HIST_REQ_ID = 1001
LAST_TRADE_REQ_ID = 2001
MKTDATA_REQ_ID = 3001


class Trader(EWrapper, EClient):
    def __init__(self, config: Dict[str, Any]):
        EClient.__init__(self, self)
        self.config = config
        self.logger = build_logger(
            config, logger_name="peg_best", default_log_file="peg_best.log"
        )

        self.ready_for_trading = False
        self.nextOrderId = None

        self.contract = make_stock_contract(self.config)

        self.position_size = 0
        self.open_price = None
        self._bars = []

        self.buy_order_id = None
        self.sell_order_id = None

        # ref_price is used to calculate extreme limit prices,
        # to avoid bidding way too high or too low.
        # Does not directly affect PEG BEST orders otherwise.
        self.ref_price = None

        self._bid = None
        self._ask = None

        self.remaining_by_order = {}
        self.open_symbol_buys = 0
        self.open_symbol_sells = 0
        self.pending_buy = False
        self.pending_sell = False

        self.client_id = ib_client_id_from_config(config, default=4)
        self.ledger = PositionLedger.open(
            "peg_best", config, client_id=self.client_id
        )
        sync_attrs_from_ledger(self.ledger, self, qty_attr="position_size", avg_attr=None)

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
                self.logger.info(f"BUY {orderId} {status} @ {avgFillPrice}")
                self.pending_buy = False
                self.buy_order_id = None
                self.open_symbol_buys = 0
                self.open_symbol_sells = 0
                self.reqOpenOrders()
            elif orderId == self.sell_order_id:
                self.logger.info(f"SELL {orderId} {status} @ {avgFillPrice}")
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
                self.ref_price = mid

    def nextValidId(self, orderId):
        GENERIC_TICKS = ""
        TICK_BY_TICK_TYPE = "Last"

        self.logger.info(f"nextValidId: {orderId}")
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
            0,      # Number of ticks
            False,  # Ignore size
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
        self.logger.info(f"Open {self.config['symbol']} orders: buys={self.open_symbol_buys}, sells={self.open_symbol_sells}")

    def error(self, reqId, errorTime, errorCode, errorString, advancedOrderRejectJson=""):
        log_ib_error(
            self.logger,
            self.config,
            req_id=reqId,
            error_time=errorTime,
            error_code=errorCode,
            error_string=errorString,
            advanced_order_reject=advancedOrderRejectJson,
        )

    def execDetails(self, reqId, contract, execution):
        if (
            contract.symbol != self.config["symbol"]
            or contract.secType != self.config["sec_type"]
        ):
            return
        if handle_ledger_execution(
            self.ledger, execution, self.client_id, logger=self.logger
        ):
            sync_attrs_from_ledger(
                self.ledger, self, qty_attr="position_size", avg_attr=None
            )
            self.logger.info(
                f"Ledger {self.ledger.path.name} qty={self.ledger.qty} "
                f"after fill execId={getattr(execution, 'execId', '')}"
            )

    def position(self, account, contract, pos, avgCost):
        if contract.symbol == self.config["symbol"] and contract.secType == self.config["sec_type"]:
            handle_ledger_ib_position(
                self.ledger, account, int(pos), float(avgCost), logger=self.logger
            )
            sync_attrs_from_ledger(
                self.ledger, self, qty_attr="position_size", avg_attr=None
            )

    def positionEnd(self):
        return

    def request_today_open_or_prior_close(self):
        self.logger.info("Requesting daily bars for open/prior close")
        self._bars = []
        self.reqHistoricalData(
            HIST_REQ_ID,
            self.contract,
            "",         # endDateTime
            "2 D",      # durationStr
            "1 day",    # barSizeSetting
            "TRADES",   # whatToShow
            1,          # useRTH
            1,          # formatDate
            False,      # keepUpToDate
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
            self.logger.info(f"Today's open price: {self.open_price}")
        else:
            self.open_price = last_bar.close
            self.logger.info(f"No valid open; using prior close: {self.open_price}")

        if self.ref_price is None:
            self.ref_price = self.open_price

    def us_regular_hours(self):
        return regular_session_open(self.config)

    def make_pegbest_order(self, action, qty, limit_price):
        o = Order()
        o.action = action
        o.orderType = "PEG BEST"
        o.totalQuantity = qty
        o.lmtPrice = round(limit_price, int(self.config["price_round_digits"]))
        o.exchange = self.config["exchange"]
        o.tif = self.config["tif"]
        o.notHeld = True
        o.minCompeteSize = self.config["min_compete_size"]

        o.competeAgainstBestOffset = COMPETE_AGAINST_BEST_OFFSET_UP_TO_MID

        o.midOffsetAtWhole = self.config["mid_offset_whole"]
        o.midOffsetAtHalf = self.config["mid_offset_half"]
        o.postToAts = self.config["post_to_ats_seconds"]
        return o

    def sync_orders(self):
        if not self.ready_for_trading:
            return

        if self.ref_price is None or self.nextOrderId is None:
            return

        pos = self.position_size
        
        # Rounding to avoid this error:
        # errorCode=110 The price does not conform to the minimum price variation for this contract
        buy_limit = round(self.ref_price * self.config["buy_limit_multiplier"], int(self.config["price_round_digits"]))
        sell_limit = round(self.ref_price * self.config["sell_limit_multiplier"], int(self.config["price_round_digits"]))
        buy_limit = clamp_buy_to_avoid_self_trade(
            buy_limit, self.config, bid=self._bid, ask=self._ask, mid=self.ref_price
        )
        sell_limit = clamp_sell_to_avoid_self_trade(
            sell_limit, self.config, bid=self._bid, ask=self._ask, mid=self.ref_price
        )

        if pos <= 0:
            desired_side = "BUY"
            desired_qty = self.config["max_pos"]
            desired_limit = buy_limit
        else:
            desired_side = "SELL"
            desired_qty = max_sell_shares(self.ledger)
            desired_limit = sell_limit

        if desired_side == "BUY":
            if self.sell_order_id is not None:
                self.logger.info(f"Cancelling opposite SELL order id={self.sell_order_id}")
                safe_cancel_order(self, self.sell_order_id)
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

                order = self.make_pegbest_order("BUY", desired_qty, desired_limit)
                self.logger.info(f"Placing PEG BEST BUY {desired_qty} @ {desired_limit}, id={oid}")
                self.placeOrder(oid, self.contract, order)
        else:
            if self.buy_order_id is not None:
                self.logger.info(f"Cancelling opposite BUY order id={self.buy_order_id}")
                safe_cancel_order(self, self.buy_order_id)
                self.pending_buy = False
                self.buy_order_id = None
                self.open_symbol_buys = 0
                return

            if self.open_symbol_buys > 0:
                return

            if desired_qty <= 0 and self.sell_order_id is not None:
                self.logger.info(f"Cancelling SELL order id={self.sell_order_id} (no shares to sell)")
                safe_cancel_order(self, self.sell_order_id)
                self.pending_sell = False
                self.sell_order_id = None
                self.open_symbol_sells = 0
                return

            if desired_qty > 0 and self.open_symbol_sells == 0 and not self.pending_sell:
                oid = self.nextOrderId
                self.nextOrderId += 1
                self.sell_order_id = oid
                self.pending_sell = True

                order = self.make_pegbest_order("SELL", desired_qty, desired_limit)
                self.logger.info(f"Placing PEG BEST SELL {desired_qty} @ {desired_limit}, id={oid}")
                self.placeOrder(oid, self.contract, order)

    def run_loop(self):
        while True:
            if not self.isConnected():
                self.logger.info("Disconnected from IBKR; exiting run loop")
                break
            if self.us_regular_hours():
                self.reqPositions()
                self.sync_orders()
            time.sleep(self.config["loop_seconds"])


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parameterized IBKR PEG BEST single-side trader")
    add_config_argument(parser, __file__)
    add_ib_connection_arguments(parser, include_client_id=False)

    parser.add_argument("--max_pos", type=int)
    parser.add_argument("--loop_seconds", type=float)
    parser.add_argument("--buy_limit_multiplier", type=float)
    parser.add_argument("--sell_limit_multiplier", type=float)
    parser.add_argument("--min_compete_size", type=int)
    parser.add_argument("--mid_offset_whole", type=float)
    parser.add_argument("--mid_offset_half", type=float)
    parser.add_argument("--post_to_ats_seconds", type=int)
    add_session_hours_arguments(parser)
    parser.add_argument("--last_trade_min_size", type=float)
    parser.add_argument("--tif")
    parser.add_argument("--price_round_digits", type=int)
    add_logging_arguments(parser)
    return parser


def main() -> None:
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
            "buy_limit_multiplier",
            "sell_limit_multiplier",
            "min_compete_size",
            "mid_offset_whole",
            "mid_offset_half",
            "post_to_ats_seconds",
            "market_timezone",
            "market_open_hour",
            "market_open_minute",
            "market_close_hour",
            "last_trade_min_size",
            "tif",
            "price_round_digits",
            "ignored_error_codes",
            "ignore_error_substrings",
        ],
    )
    log_startup_timezones(config)

    app = Trader(config)
    app.connect(config["host"], config["port"], app.client_id)

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
