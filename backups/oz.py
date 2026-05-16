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

        # orderId -> (action, remaining_qty)
        self.oz_orders = {}
        # IDs that TWS considers currently open
        self.open_order_ids = set()

    # ----- Contract definition -----

    def make_us_stock(self, symbol):
        c = Contract()
        c.symbol = symbol
        c.secType = "STK"
        c.currency = "USD"
        # Route to IBKRATS for PEG BEST
        c.exchange = "IBKRATS"
        c.primaryExch = "AMEX"   # adjust to actual primary for OZ
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
            # Refresh open orders so we know what's live before syncing
            self.reqOpenOrders()
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

    # ----- Order tracking (open orders and remaining qty) -----

    def openOrder(self, orderId, contract, order, orderState):
        if contract.symbol != SYMBOL or contract.secType != "STK":
            return
        # Mark as open and initialize remaining as totalQuantity;
        # orderStatus will update remaining as fills happen.
        self.open_order_ids.add(orderId)
        self.oz_orders[orderId] = (order.action, int(order.totalQuantity))

    def openOrderEnd(self):
        """
        Ensure at most one BUY and one SELL order for OZ.
        Cancel extras and update buy_order_id / sell_order_id.
        """
        buy_ids = [oid for oid, (act, _) in self.oz_orders.items() if act == "BUY"]
        sell_ids = [oid for oid, (act, _) in self.oz_orders.items() if act == "SELL"]

        # Keep first BUY, cancel others
        if len(buy_ids) > 1:
            keep = buy_ids[0]
            for oid in buy_ids[1:]:
                print(f"Cancelling extra BUY id={oid}")
                self.cancelOrder(oid)
                self.oz_orders.pop(oid, None)
                self.open_order_ids.discard(oid)
            self.buy_order_id = keep
        elif len(buy_ids) == 1:
            self.buy_order_id = buy_ids[0]
        else:
            self.buy_order_id = None

        # Keep first SELL, cancel others
        if len(sell_ids) > 1:
            keep = sell_ids[0]
            for oid in sell_ids[1:]:
                print(f"Cancelling extra SELL id={oid}")
                self.cancelOrder(oid)
                self.oz_orders.pop(oid, None)
                self.open_order_ids.discard(oid)
            self.sell_order_id = keep
        elif len(sell_ids) == 1:
            self.sell_order_id = sell_ids[0]
        else:
            self.sell_order_id = None

    def orderStatus(self, orderId, status, filled, remaining,
                    avgFillPrice, permId, parentId, lastFillPrice,
                    clientId, whyHeld, mktCapPrice):
        if orderId in self.oz_orders:
            act, _ = self.oz_orders[orderId]
            self.oz_orders[orderId] = (act, int(remaining))

        # When order is no longer live, clear state
        if status in ("Filled", "Cancelled", "Inactive") or remaining == 0:
            self.open_order_ids.discard(orderId)
            self.oz_orders.pop(orderId, None)
            if orderId == self.buy_order_id:
                self.buy_order_id = None
            if orderId == self.sell_order_id:
                self.sell_order_id = None

    # ----- PEG BEST builder -----

    def make_pegbest_order(self, action, qty, limit_price,
                           min_compete_size=100,
                           mid_offset_whole=0,
                           mid_offset_half=0):
        """
        Build an IBKRATS Pegged-to-Best order (simplified).
        Adjust IBKRATS-specific flags here to match what works in your TWS.
        """
        o = Order()
        o.action = action
        o.orderType = "PEG BEST"
        o.totalQuantity = qty
        o.lmtPrice = round(limit_price, 2)
        o.exchange = "IBKRATS"
        o.tif = "DAY"
        o.transmit = True

        # Optional size controls
        o.minCompeteSize = min_compete_size

        # Simple numeric compete offset; adjust as needed
        o.competeAgainstBestOffset = 0.02

        # Midpoint offsets for UpToMid variant (if supported)
        o.midOffsetAtWhole = mid_offset_whole
        o.midOffsetAtHalf = mid_offset_half

        return o

    # ----- Core trading logic -----

    def sync_orders(self):
        """
        - Sell PEG BEST order for current position (if > 0)
        - Buy PEG BEST order for 1000 - position (if < 1000)
        - Use remaining quantity from orderStatus to decide when to modify.
        - Only modify if remaining != target, and only if orderId is still open.
        """
        if self.open_price is None:
            return

        pos = self.position_size
        target_buy_qty = max(0, MAX_POS - pos)   # 1000 - position
        target_sell_qty = max(0, pos)            # current position

        ref = self.open_price
        buy_limit = round(ref * 1.01, 2)
        sell_limit = round(ref * 0.99, 2)

        mid_offset_whole = 0.02
        mid_offset_half = 0.015
        min_compete_size = 100

        # --- BUY side: use remaining qty ---

        existing_buy_remaining = None
        if (self.buy_order_id is not None
                and self.buy_order_id in self.oz_orders
                and self.buy_order_id in self.open_order_ids):
            act, rem = self.oz_orders[self.buy_order_id]
            if act == "BUY":
                existing_buy_remaining = rem

        if target_buy_qty > 0:
            if (self.buy_order_id is not None
                    and self.buy_order_id in self.open_order_ids
                    and existing_buy_remaining == target_buy_qty):
                # Remaining already matches target; no modify
                pass
            else:
                buy_order = self.make_pegbest_order(
                    "BUY", target_buy_qty, buy_limit,
                    min_compete_size=min_compete_size,
                    mid_offset_whole=mid_offset_whole,
                    mid_offset_half=mid_offset_half
                )

                if self.buy_order_id is not None and self.buy_order_id in self.open_order_ids:
                    buy_order.orderId = self.buy_order_id
                    print(f"Modifying PEG BEST BUY remaining {target_buy_qty} @ {buy_limit}, id={self.buy_order_id}")
                    self.placeOrder(self.buy_order_id, self.contract, buy_order)
                else:
                    oid = self.nextOrderId
                    self.nextOrderId += 1
                    self.buy_order_id = oid
                    print(f"Placing PEG BEST BUY {target_buy_qty} @ {buy_limit}, id={oid}")
                    self.placeOrder(oid, self.contract, buy_order)
        else:
            if self.buy_order_id is not None and self.buy_order_id in self.open_order_ids:
                print(f"Cancelling BUY id={self.buy_order_id} (no buy needed)")
                self.cancelOrder(self.buy_order_id)
            self.buy_order_id = None

        # --- SELL side: use remaining qty ---

        existing_sell_remaining = None
        if (self.sell_order_id is not None
                and self.sell_order_id in self.oz_orders
                and self.sell_order_id in self.open_order_ids):
            act, rem = self.oz_orders[self.sell_order_id]
            if act == "SELL":
                existing_sell_remai
