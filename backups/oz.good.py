from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order

import threading
import time
import datetime
import pytz

SYMBOL = "OZ"
MAX_POS = 1000

class OZTrader(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

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
        self.live_orders = {}   # orderId -> (action, status)

    # ----- Callbacks -----

    def openOrder(self, orderId, contract, order, orderState):
        if contract.symbol != SYMBOL or contract.secType != "STK":
            return
        self.live_orders[orderId] = (order.action, orderState.status)
    
        if order.action == "BUY":
            self.buy_order_id = orderId
        elif order.action == "SELL":
            self.sell_order_id = orderId
    
    def orderStatus(self, orderId, status, filled, remaining,
                    avgFillPrice, permId, parentId, lastFillPrice,
                    clientId, whyHeld, mktCapPrice):
        if orderId in self.live_orders:
            action, _ = self.live_orders[orderId]
            self.live_orders[orderId] = (action, status)
    
            # When an order is fully cancelled or filled, clear our handle
            if status in ("Filled", "Cancelled", "Inactive"):
                if orderId == self.buy_order_id:
                    self.buy_order_id = None
                if orderId == self.sell_order_id:
                    self.sell_order_id = None
    
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

    def nextValidId(self, orderId):
        print("nextValidId:", orderId)
        self.nextOrderId = orderId

        # Initial position and reference price
        self.reqPositions()
        self.request_today_open_or_prior_close()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson="", arg5=""):
        print("Error", reqId, errorCode, errorString, advancedOrderRejectJson)

    # ----- Positions (never short, track size) -----

    def position(self, account, contract, pos, avgCost):
        if contract.symbol == SYMBOL and contract.secType == "STK":
            self.position_size = int(pos)

    def positionEnd(self):
        print("Current position:", self.position_size)
        if self.open_price is not None and self.nextOrderId is not None:
            self.sync_orders()

    # ----- Historical data: open or prior close -----

    def request_today_open_or_prior_close(self):
        """
        Request 2 daily bars. Use today's open if valid; else prior close.
        """
        print("Requesting daily bars for open/prior close")
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

    def historicalDataEnd(self, reqId, start, end):
        if reqId != self.hist_req_id:
            return

        if not self._bars:
            print("No historical bars returned; cannot set open_price")
            return

        last_bar = self._bars[-1]

        if last_bar.open and last_bar.open > 0:
            self.open_price = last_bar.open
            print(f"Using today's open price: {self.open_price}")
        else:
            self.open_price = last_bar.close
            print(f"No valid open; using prior close: {self.open_price}")

        if self.nextOrderId is not None:
            self.sync_orders()

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
        - midOffsetAtWhole / midOffsetAtHalf: midpoint offsets (Reg NMS compliant).[web:31][web:35][web:51][web:106][web:107]
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
        # In Python API these constants/fields are exposed as attributes on Order.[web:51][web:108][web:110]
        try:
            from ibapi.order import Order as OrderClass
            OrderClass.COMPETE_AGAINST_BEST_OFFSET_UP_TO_MID = float('inf')
            o.competeAgainstBestOffset = OrderClass.COMPETE_AGAINST_BEST_OFFSET_UP_TO_MID
        except Exception:
            print ("o.competeAgainstBestOffset = 0.02")
            o.competeAgainstBestOffset = 0.02

        o.midOffsetAtWhole = mid_offset_whole
        o.midOffsetAtHalf = mid_offset_half

        # NEW: PostToAts required for IBKRATS PEG BEST[web:109][web:133]
        # Placing PEG BEST SELL 300 @ 51.03, id=10
        # Error 10 1772932180452 10274 The 'PostToATS' order attribute may not be specified for this order or is invalid.
        o.postToAts = 1
        print ("o.postToAts = 1")
        # del o.postToAts

        return o

    # ----- Core trading logic -----

    if (True):
        def sync_orders(self):
            if self.open_price is None:
                return
        
            pos = self.position_size
            buy_qty = max(0, MAX_POS - pos)
            sell_qty = max(0, pos)
        
            ref = self.open_price
            buy_limit = round(ref * 1.01, 2)
            sell_limit = round(ref * 0.99, 2)
        
            mid_offset_whole = 0.02
            mid_offset_half = 0.015
            min_compete_size = 100
        
            # ---- BUY side ----
            if buy_qty > 0:
                order = self.make_pegbest_order(
                    "BUY", buy_qty, buy_limit,
                    min_compete_size=min_compete_size,
                    mid_offset_whole=mid_offset_whole,
                    mid_offset_half=mid_offset_half
                )
        
                if self.buy_order_id is None:
                    oid = self.nextOrderId
                    self.nextOrderId += 1
                    self.buy_order_id = oid
                    print(f"Placing PEG BEST BUY {buy_qty} @ {buy_limit}, id={oid}")
                    self.placeOrder(oid, self.contract, order)
                else:
                    order.orderId = self.buy_order_id
                    print(f"Modifying PEG BEST BUY {buy_qty} @ {buy_limit}, id={self.buy_order_id}")
                    self.placeOrder(self.buy_order_id, self.contract, order)
            else:
                if self.buy_order_id is not None:
                    print(f"Cancelling BUY id={self.buy_order_id}")
                    self.cancelOrder(self.buy_order_id)
                    self.buy_order_id = None
        
            # ---- SELL side ----
            if sell_qty > 0:
                order = self.make_pegbest_order(
                    "SELL", sell_qty, sell_limit,
                    min_compete_size=min_compete_size,
                    mid_offset_whole=mid_offset_whole,
                    mid_offset_half=mid_offset_half
                )
        
                if self.sell_order_id is None:
                    oid = self.nextOrderId
                    self.nextOrderId += 1
                    self.sell_order_id = oid
                    print(f"Placing PEG BEST SELL {sell_qty} @ {sell_limit}, id={oid}")
                    self.placeOrder(oid, self.contract, order)
                else:
                    order.orderId = self.sell_order_id
                    print(f"Modifying PEG BEST SELL {sell_qty} @ {sell_limit}, id={self.sell_order_id}")
                    self.placeOrder(self.sell_order_id, self.contract, order)
            else:
                if self.sell_order_id is not None:
                    print(f"Cancelling SELL id={self.sell_order_id}")
                    self.cancelOrder(self.sell_order_id)
                    self.sell_order_id = None
    else:
        def sync_orders(self):
            """
            Enforce:
            - Sell PEG BEST order for current position (if > 0)
            - Buy PEG BEST order for 1000 - position (if < 1000)
            Limits:
            - buy_limit = ref_price * 1.01
            - sell_limit = ref_price * 0.99
            Modify existing orders instead of creating new ones.
            """
            if self.open_price is None:
                return
    
            pos = self.position_size
            buy_qty = max(0, MAX_POS - pos)
            sell_qty = max(0, pos)
    
            ref = self.open_price
            buy_limit = round(ref * 1.01)
            sell_limit = round(ref * 0.99, 2)
    
            # Reasonable UpToMid offsets; you can tweak these per symbol.[web:31][web:35][web:51][web:106][web:107]
            mid_offset_whole = 0.02
            mid_offset_half = 0.015
            min_compete_size = 100
    
            # BUY side
            if buy_qty > 0:
                if self.buy_order_id is None:
                    oid = self.nextOrderId
                    self.nextOrderId += 1
                    self.buy_order_id = oid
    
                    order = self.make_pegbest_order(
                        "BUY", buy_qty, buy_limit,
                        min_compete_size=min_compete_size,
                        mid_offset_whole=mid_offset_whole,
                        mid_offset_half=mid_offset_half
                    )
                    print(f"Placing PEG BEST BUY {buy_qty} @ {buy_limit}, id={oid}")
                    self.placeOrder(oid, self.contract, order)
                else:
                    order = self.make_pegbest_order(
                        "BUY", buy_qty, buy_limit,
                        min_compete_size=min_compete_size,
                        mid_offset_whole=mid_offset_whole,
                        mid_offset_half=mid_offset_half
                    )
                    order.orderId = self.buy_order_id
                    print(f"Modifying PEG BEST BUY {buy_qty} @ {buy_limit}, id={self.buy_order_id}")
                    self.placeOrder(self.buy_order_id, self.contract, order)
            else:
                if self.buy_order_id is not None:
                    print(f"Cancelling BUY id={self.buy_order_id}")
                    self.cancelOrder(self.buy_order_id)
                    self.buy_order_id = None
    
            # SELL side
            if sell_qty > 0:
                if self.sell_order_id is None:
                    oid = self.nextOrderId
                    self.nextOrderId += 1
                    self.sell_order_id = oid
    
                    order = self.make_pegbest_order(
                        "SELL", sell_qty, sell_limit,
                        min_compete_size=min_compete_size,
                        mid_offset_whole=mid_offset_whole,
                        mid_offset_half=mid_offset_half
                    )
                    print(f"Placing PEG BEST SELL {sell_qty} @ {sell_limit}, id={oid}")
                    self.placeOrder(oid, self.contract, order)
                else:
                    order = self.make_pegbest_order(
                        "SELL", sell_qty, sell_limit,
                        min_compete_size=min_compete_size,
                        mid_offset_whole=mid_offset_whole,
                        mid_offset_half=mid_offset_half
                    )
                    order.orderId = self.sell_order_id
                    print(f"Modifying PEG BEST SELL {sell_qty} @ {sell_limit}, id={self.sell_order_id}")
                    self.placeOrder(self.sell_order_id, self.contract, order)
            else:
                if self.sell_order_id is not None:
                    print(f"Cancelling SELL id={self.sell_order_id}")
                    self.cancelOrder(self.sell_order_id)
                    self.sell_order_id = None

    # ----- Main loop -----

    def run_loop(self):
        while True:
            if self.us_regular_hours():
                self.reqPositions()

                if self.open_price is None:
                    self.request_today_open_or_prior_close()

                if self.open_price is not None:
                    self.sync_orders()

            time.sleep(10)


def main():
    app = OZTrader()
    app.connect("127.0.0.1", 7496, clientId=1)

    t = threading.Thread(target=app.run, daemon=True)
    t.start()

    try:
        app.run_loop()
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        app.disconnect()


if __name__ == "__main__":
    main()
