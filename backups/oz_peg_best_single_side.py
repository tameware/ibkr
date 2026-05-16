from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order

import threading
import time
import datetime
import pytz

SYMBOL = "OZ"
MAX_POS = 100

def tprint(msg: str) -> None:
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} {msg}")

class OZTrader(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

        self.ready_for_trading = False
        self.nextOrderId = None

        self.contract = self.make_us_stock(SYMBOL)

        self.position_size = 0

        # Reference price for the day: today's open if available, else prior close
        self.open_price = None
        self.hist_req_id = 1001
        self._bars = []

        # Track working orders so we can modify rather than create new ones
        self.buy_order_id = None
        self.sell_order_id = None

        self.ref_price = None   # opening or latest trade - used to set failsafes
        self.mktdata_req_id = 3001
        self._bid = None
        self._ask = None
        self.last_large_trade_req_id = 2001

        self.remaining_by_order = {}  # orderId -> remaining
        self.open_oz_buys = 0
        self.open_oz_sells = 0
        self.pending_buy = False
        self.pending_sell = False

    def orderStatus(
        self, orderId, status, filled, remaining,
        avgFillPrice, permId, parentId, lastFillPrice,
        clientId, whyHeld, mktCapPrice
    ):
        # Treat 'Filled' or 'Cancelled' as no longer existing
        if status in ("Filled", "Cancelled"):
            if orderId == self.buy_order_id:
                tprint(f"BUY {orderId} {status} @ {avgFillPrice}")
                self.pending_buy = False
                self.buy_order_id = None
            if orderId == self.sell_order_id:
                tprint(f"SELL {orderId} {status} @ {avgFillPrice}")
                self.pending_sell = False
                self.sell_order_id = None

            # We don't know action here, so resync by asking IB again
            self.open_oz_buys = 0
            self.open_oz_sells = 0
            self.reqOpenOrders()   # repopulate via openOrder callbacks
            
    from decimal import Decimal
    from ibapi.common import (
        TickAttribLast,
    )

    # tickByTickAllLast is activated by reqTickByTickData
    def tickByTickAllLast(
        self, reqId: int, tickType: int, time: int,
        price: float, size: Decimal,
        tickAttribLast: TickAttribLast,
        exchange: str, specialConditions: str
    ):
        if reqId != self.last_large_trade_req_id:
            return
    
        if size is None:
            return
    
        if float(size) >= 100.0:
            if self.ref_price != price:
                self.ref_price = price
                tprint(f"New ref_price from last trade: {price} size={size}")
        
    from ibapi.ticktype import (
        TickTypeEnum,  # for readable logging, optional
        TickType,
    )

    from ibapi.common import (
        TickerId,
        TickAttrib,
    )

    # Called by IBKR
    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib):
        if reqId != self.mktdata_req_id:
            return
    
        # 1 = bid, 2 = ask in IB tick types
        if tickType == 1:
            self._bid = price
        elif tickType == 2:
            self._ask = price
    
        if self._bid is not None and self._ask is not None and self._ask > 0:
            mid = (self._bid + self._ask) / 2.0
            if mid != self.ref_price:
                tprint(f"ref_price updated: bid={self._bid} ask={self._ask} mid={mid}")
            self.ref_price = mid
    
    # ----- Contract definition -----

    def make_us_stock(self, symbol):
        c = Contract()
        c.symbol = symbol
        c.secType = "STK"
        c.currency = "USD"
        # Contract exchange cannot stay SMART
        c.exchange = "IBKRATS"
        c.primaryExch = "AMEX"  # adjust to the actual primary listing for OZ
        return c

    # ----- TWS lifecycle -----

    # nextValidId is activated by IBKR itself right after your client successfully connects
    def nextValidId(self, orderId):
        tprint(f"nextValidId: {orderId}")
        self.nextOrderId = orderId

        # Initial position and reference price
        self.reqPositions()
        self.request_today_open_or_prior_close()

        # start streaming bid/ask for midprice
        # "" = no generic ticks; snapshot=False, regulatory snapshot=False
        # tprint("self.reqMktData")
        self.reqMktData(self.mktdata_req_id, self.contract, "", False, False, [])
    
        # sync open orders from IB
        self.open_oz_buys = 0
        self.open_oz_sells = 0
        self.reqOpenOrders()      # or self.reqAllOpenOrders()
    
        # start market data after we have a valid id
        self.reqTickByTickData(
            self.last_large_trade_req_id,
            self.contract,
            "Last",   # or "AllLast" if you want all print types
            0,        # number of past ticks; 0 = only live
            False     # ignore regulatory snapshots
        )

    def openOrder(self, orderId, contract, order, orderState):
        # We only care about OZ stock orders
        if contract.symbol == SYMBOL and contract.secType == "STK":
            if order.action == "BUY":
                self.open_oz_buys += 1
                self.buy_order_id = orderId
            elif order.action == "SELL":
                self.open_oz_sells += 1
                self.sell_order_id = orderId
                    
    def openOrderEnd(self):
        # Allow trading only after we’ve synced open orders
        self.ready_for_trading = True
        tprint(f"Open OZ orders: buys={self.open_oz_buys}, sells={self.open_oz_sells}")
        
    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson="", arg5=""):
        # Ignore “farm connection is OK” notifications and similar noise
        noisy_codes = {"2103", "2104", "2106", "2108", "2158", 2104, 2106, 2158}  # add/remove codes as you like
        if errorString in noisy_codes:
            return
    
        tprint(f"Error {reqId} {errorCode} {errorString} {advancedOrderRejectJson}")
    
    # ----- Positions (never short, track size) -----

    def position(self, account, contract, pos, avgCost):
        if contract.symbol == SYMBOL and contract.secType == "STK":
            self.position_size = int(pos)

    def positionEnd(self):
        pass
        # tprint("Current position:", self.position_size)
        # if self.ref_price is not None and self.nextOrderId is not None:
            # self.sync_orders()

    # ----- Historical data: open or prior close -----

    def request_today_open_or_prior_close(self):
        """
        Request 2 daily bars. Use today's open if valid; else prior close.
        """
        tprint("Requesting daily bars for open/prior close")
        self._bars = []
        self.reqHistoricalData(
            self.hist_req_id,
            self.contract,
            endDateTime="",          # now
            durationStr="2 D",       # today + yesterday
            barSizeSetting="1 day",
            whatToShow="TRADES",
            useRTH=1,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[]
        )

    def historicalData(self, reqId, bar):
        if reqId != self.hist_req_id:
            return
        self._bars.append(bar)

    # Called by IBKR
    def historicalDataEnd(self, reqId, start, end):
        if reqId != self.hist_req_id:
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

        # if self.nextOrderId is not None:
            # self.sync_orders()

        if self.ref_price is None:
            self.ref_price = self.open_price

    # ----- Time / helpers -----

    def us_regular_hours(self):
        ny = pytz.timezone("America/New_York")
        now = datetime.datetime.now(tz=ny)
        return ((now.hour > 9 or (now.hour == 9 and now.minute >= 30))
                and now.hour < 16)

    # PEG BEST builders

    def make_pegbest_order(self, action, qty, limit_price,
                           min_compete_size=100,
                           mid_offset_whole=0,
                           mid_offset_half=0):
        """
        Build an IBKRATS Pegged-to-Best order with UpToMid behavior.
        - orderType: PEG BEST
        - exchange: IBKRATS
        - competeAgainstBestOffset: UpToMid
        - midOffsetAtWhole / midOffsetAtHalf: midpoint offsets (Reg NMS compliant).
        """
        o = Order()
        o.action = action
        o.orderType = "PEG BEST"
        o.totalQuantity = qty
        o.lmtPrice = round(limit_price, 2)
        o.exchange = "IBKRATS"   # direct to IBKR ATS[web:35][web:48][web:51]
        o.tif = "DAY"
        o.notHeld = True

        # Optional size controls
        o.minCompeteSize = min_compete_size

        # UpToMid behavior: use the special constant and midpoint offsets
        # In Python API these constants/fields are exposed as attributes on Order.
        try:
            from ibapi.order import Order as OrderClass
            OrderClass.COMPETE_AGAINST_BEST_OFFSET_UP_TO_MID = float('inf')
            o.competeAgainstBestOffset = OrderClass.COMPETE_AGAINST_BEST_OFFSET_UP_TO_MID
        except Exception:
            tprint ("o.competeAgainstBestOffset = 0.02")
            o.competeAgainstBestOffset = 0.02

        o.midOffsetAtWhole = mid_offset_whole
        o.midOffsetAtHalf = mid_offset_half

        # NEW: PostToAts required for IBKRATS PEG BEST[web:109][web:133]
        REROUTE_AFTER_SECONDS = 1800 # 30 minutes
        o.postToAts = REROUTE_AFTER_SECONDS

        return o

    # ----- Core trading logic -----

    def sync_orders(self):
        """
        Enforce exactly one active order at a time:
        - If flat: work only a BUY for MAX_POS
        - If long: work only a SELL for current position
        """
        if not self.ready_for_trading:
            return
    
        if self.ref_price is None or self.nextOrderId is None:
            return
    
        pos = self.position_size
    
        FREQUENT_TRADES = True
        POSITIVE_SPREAD = False
    
        if FREQUENT_TRADES:
            buy_limit = round(self.ref_price * 1.02, 2)
            sell_limit = round(self.ref_price * 0.98, 2)
        elif POSITIVE_SPREAD:
            DELTA = 0.03
            buy_limit = round(self.ref_price - DELTA, 2)
            sell_limit = round(self.ref_price + DELTA, 2)
        else:
            buy_limit = round(self.ref_price, 2)
            sell_limit = round(self.ref_price, 2)
    
        mid_offset_whole = 0.02
        mid_offset_half = 0.015
        min_compete_size = 50
    
        if pos <= 0:
            desired_side = "BUY"
            desired_qty = MAX_POS
            desired_limit = buy_limit
        else:
            desired_side = "SELL"
            desired_qty = pos
            desired_limit = sell_limit
    
        if desired_side == "BUY":
            if self.sell_order_id is not None:
                tprint(f"Cancelling opposite SELL order id={self.sell_order_id}")
                self.cancelOrder(self.sell_order_id, "")
                self.pending_sell = False
                self.sell_order_id = None
                self.open_oz_sells = 0
                return
    
            if self.open_oz_sells > 0:
                return
    
            if desired_qty > 0 and self.open_oz_buys == 0 and not self.pending_buy:
                oid = self.nextOrderId
                self.nextOrderId += 1
                self.buy_order_id = oid
                self.pending_buy = True
    
                order = self.make_pegbest_order(
                    "BUY", desired_qty, desired_limit,
                    min_compete_size=min_compete_size,
                    mid_offset_whole=mid_offset_whole,
                    mid_offset_half=mid_offset_half
                )
                tprint(f"Placing PEG BEST BUY {desired_qty} @ {desired_limit}, id={oid}")
                self.placeOrder(oid, self.contract, order)
    
        else:
            if self.buy_order_id is not None:
                tprint(f"Cancelling opposite BUY order id={self.buy_order_id}")
                self.cancelOrder(self.buy_order_id, "")
                self.pending_buy = False
                self.buy_order_id = None
                self.open_oz_buys = 0
                return
    
            if self.open_oz_buys > 0:
                return
    
            if desired_qty > 0 and self.open_oz_sells == 0 and not self.pending_sell:
                oid = self.nextOrderId
                self.nextOrderId += 1
                self.sell_order_id = oid
                self.pending_sell = True
    
                order = self.make_pegbest_order(
                    "SELL", desired_qty, desired_limit,
                    min_compete_size=min_compete_size,
                    mid_offset_whole=mid_offset_whole,
                    mid_offset_half=mid_offset_half
                )
                tprint(f"Placing PEG BEST SELL {desired_qty} @ {desired_limit}, id={oid}")
                self.placeOrder(oid, self.contract, order)

    # ----- Main loop -----

    def run_loop(self):
        while True:
            if self.us_regular_hours():
                self.reqPositions()

                # if self.open_price is None:
                    # self.request_today_open_or_prior_close()

                self.sync_orders()

            SECONDS = 10
            time.sleep(SECONDS)


def main():
    LIVE = 7496
    PAPER = 7497

    app = OZTrader()
    
    app.connect("127.0.0.1", LIVE, clientId=1)

    t = threading.Thread(target=app.run, daemon=True)
    t.start()

    try:
        app.run_loop()
    except KeyboardInterrupt:
        tprint("Stopping...")
    finally:
        app.disconnect()


if __name__ == "__main__":
    main()
