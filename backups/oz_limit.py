from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order

import threading
import time
import math
import pytz # Might not be needed

HOST = "127.0.0.1"
PORT = 7496                     # 7497 paper, 7496 live
CLIENT_ID = 1                   # Was 21

SYMBOL = "OZ"
PRIMARY_EXCHANGE = "AMEX"
CURRENCY = "USD"

MAX_POSITION = 200
DELTA = 0.25                 # buy at mid-delta, sell at mid+delta
TICK_SIZE = 0.01
UPDATE_THRESHOLD = 0.01
POLL_SECONDS = 0.25

BUY_ABSOLUTE_CAP = 60.00
SELL_ABSOLUTE_FLOOR = 45.00

# ACCOUNT = None                # optional IB account string
PARTIAL_FILL_COOLDOWN = 3.0   # seconds to pause modifications after a partial fill


def tprint(msg: str) -> None:
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} {msg}")

def round_down_to_tick(px: float, tick: float) -> float:
    return math.floor(px / tick) * tick

def round_up_to_tick(px: float, tick: float) -> float:
    return math.ceil(px / tick) * tick

class MidDeltaMarketMaker(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

        self.next_order_id = None
        self.market_data_req_id = 1001

        self.bid = None
        self.ask = None

        self.current_position = 0
        self.position_snapshot_done = False

        self.buy_order_id = None
        self.sell_order_id = None

        self.buy_live = False
        self.sell_live = False

        self.last_buy_lmt = None
        self.last_sell_lmt = None
        self.last_buy_qty = None
        self.last_sell_qty = None

        self.buy_filled = 0
        self.buy_remaining = 0
        self.sell_filled = 0
        self.sell_remaining = 0

        self.buy_cooldown_until = 0.0
        self.sell_cooldown_until = 0.0

    # ---------- callbacks ----------

    def nextValidId(self, orderId: int):
        self.next_order_id = orderId
        print(f"nextValidId={orderId}")

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson="", errorTime=""):
        msg = f"ERROR reqId={reqId} code={errorCode} msg={errorString}"
        if advancedOrderRejectJson:
            msg += f" advancedOrderRejectJson={advancedOrderRejectJson}"
        if errorTime:
            msg += f" errorTime={errorTime}"
        print(msg)

    def tickPrice(self, reqId, tickType, price, attrib):
        if reqId != self.market_data_req_id:
            return
        if price <= 0:
            return

        if tickType == 1:      # bid
            self.bid = price
        elif tickType == 2:    # ask
            self.ask = price

    def position(self, account, contract, pos, avgCost):
        if contract.secType != "STK":
            return
        if contract.symbol != SYMBOL:
            return
        # if ACCOUNT is not None and account != ACCOUNT:
            # return

        self.current_position = int(pos)
        print(f"POSITION account={account} symbol={contract.symbol} pos={self.current_position} avgCost={avgCost}")

    def positionEnd(self):
        self.position_snapshot_done = True
        print("POSITION SNAPSHOT COMPLETE")

    def openOrder(self, orderId, contract, order, orderState):
        print(
            f"OPEN orderId={orderId} action={order.action} qty={order.totalQuantity} "
            f"lmt={getattr(order, 'lmtPrice', None)} status={orderState.status}"
        )

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice,
                    permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        print(
            f"STATUS orderId={orderId} status={status} filled={filled} "
            f"remaining={remaining} avgFill={avgFillPrice} lastFill={lastFillPrice}"
        )

        inactive_states = {"Filled", "Cancelled", "ApiCancelled", "Inactive"}
        is_live = status not in inactive_states

        now = time.time()

        if orderId == self.buy_order_id:
            self.buy_live = is_live
            self.buy_filled = filled
            self.buy_remaining = remaining

            if filled > 0 and remaining > 0:
                self.buy_cooldown_until = max(self.buy_cooldown_until, now + PARTIAL_FILL_COOLDOWN)

        elif orderId == self.sell_order_id:
            self.sell_live = is_live
            self.sell_filled = filled
            self.sell_remaining = remaining

            if filled > 0 and remaining > 0:
                self.sell_cooldown_until = max(self.sell_cooldown_until, now + PARTIAL_FILL_COOLDOWN)

    def execDetails(self, reqId, contract, execution):
        if contract.secType != "STK" or contract.symbol != SYMBOL:
            return

        side = execution.side.upper()
        shares = int(execution.shares)
        px = execution.price

        print(f"EXEC side={side} shares={shares} price={px}")

        if side == "BOT":
            self.current_position += shares
            self.buy_cooldown_until = max(self.buy_cooldown_until, time.time() + PARTIAL_FILL_COOLDOWN)
        elif side == "SLD":
            self.current_position -= shares
            self.sell_cooldown_until = max(self.sell_cooldown_until, time.time() + PARTIAL_FILL_COOLDOWN)

        self.current_position = max(0, min(MAX_POSITION, self.current_position))
        print(f"UPDATED POSITION={self.current_position}")

    # ---------- helpers ----------

    def make_stock_contract(self) -> Contract:
        c = Contract()
        c.symbol = SYMBOL
        c.secType = "STK"
        c.exchange = "SMART"
        c.currency = CURRENCY
        c.primaryExchange = PRIMARY_EXCHANGE
        return c

    def make_limit_order(self, action: str, qty: int, limit_price: float) -> Order:
        o = Order()
        o.action = action
        o.orderType = "LMT"
        o.totalQuantity = qty
        o.lmtPrice = limit_price
        o.tif = "DAY"
        o.outsideRth = False
        o.transmit = True
        return o

    def current_mid(self):
        if self.bid is None or self.ask is None:
            return None
        if self.bid <= 0 or self.ask <= 0 or self.ask < self.bid:
            return None
        return (self.bid + self.ask) / 2.0

    def desired_buy_qty(self):
        return max(0, MAX_POSITION - self.current_position)

    def desired_sell_qty(self):
        return max(0, self.current_position)

    def desired_buy_price(self):
        mid = self.current_mid()
        if mid is None:
            return None
        px = min(mid - DELTA, BUY_ABSOLUTE_CAP)
        px = round_down_to_tick(px, TICK_SIZE)
        return max(px, TICK_SIZE)

    def desired_sell_price(self):
        mid = self.current_mid()
        if mid is None:
            return None
        px = max(mid + DELTA, SELL_ABSOLUTE_FLOOR)
        px = round_up_to_tick(px, TICK_SIZE)
        return max(px, TICK_SIZE)

    def buy_in_cooldown(self):
        return time.time() < self.buy_cooldown_until

    def sell_in_cooldown(self):
        return time.time() < self.sell_cooldown_until

    # ---------- order management ----------

    def place_or_modify_buy(self, contract):
        qty = self.desired_buy_qty()

        if qty <= 0:
            if self.buy_order_id is not None and self.buy_live:
                print("CANCEL BUY (already at max position)")
                self.cancelOrder(self.buy_order_id, "")
                self.buy_live = False
            self.last_buy_qty = 0
            return

        desired = self.desired_buy_price()
        if desired is None:
            return

        if self.buy_order_id is None:
            self.buy_order_id = self.next_order_id
            self.next_order_id += 1

            order = self.make_limit_order("BUY", qty, desired)
            self.placeOrder(self.buy_order_id, contract, order)
            self.buy_live = True
            self.last_buy_lmt = desired
            self.last_buy_qty = qty
            print(f"PLACED BUY qty={qty} @ {desired:.2f}")
            return

        if self.buy_in_cooldown():
            print(f"SKIP BUY MODIFY (partial-fill cooldown {self.buy_cooldown_until - time.time():.2f}s)")
            return

        need_modify = (
            self.last_buy_lmt is None
            or self.last_buy_qty is None
            or abs(desired - self.last_buy_lmt) >= UPDATE_THRESHOLD
            or qty != self.last_buy_qty
            or not self.buy_live
        )

        if need_modify:
            order = self.make_limit_order("BUY", qty, desired)
            self.placeOrder(self.buy_order_id, contract, order)  # same orderId => modify
            self.buy_live = True
            self.last_buy_lmt = desired
            self.last_buy_qty = qty
            print(f"MODIFIED BUY qty={qty} @ {desired:.2f}")

    def place_or_modify_sell(self, contract):
        qty = self.desired_sell_qty()

        if qty <= 0:
            if self.sell_order_id is not None and self.sell_live:
                print("CANCEL SELL (no long position to sell)")
                self.cancelOrder(self.sell_order_id, "")
                self.sell_live = False
            self.last_sell_qty = 0
            return

        desired = self.desired_sell_price()
        if desired is None:
            return

        if self.sell_order_id is None:
            self.sell_order_id = self.next_order_id
            self.next_order_id += 1

            order = self.make_limit_order("SELL", qty, desired)
            self.placeOrder(self.sell_order_id, contract, order)
            self.sell_live = True
            self.last_sell_lmt = desired
            self.last_sell_qty = qty
            print(f"PLACED SELL qty={qty} @ {desired:.2f}")
            return

        if self.sell_in_cooldown():
            print(f"SKIP SELL MODIFY (partial-fill cooldown {self.sell_cooldown_until - time.time():.2f}s)")
            return

        need_modify = (
            self.last_sell_lmt is None
            or self.last_sell_qty is None
            or abs(desired - self.last_sell_lmt) >= UPDATE_THRESHOLD
            or qty != self.last_sell_qty
            or not self.sell_live
        )

        if need_modify:
            order = self.make_limit_order("SELL", qty, desired)
            self.placeOrder(self.sell_order_id, contract, order)  # same orderId => modify
            self.sell_live = True
            self.last_sell_lmt = desired
            self.last_sell_qty = qty
            print(f"MODIFIED SELL qty={qty} @ {desired:.2f}")

    def cancel_all_working(self):
        if self.buy_order_id is not None and self.buy_live:
            self.cancelOrder(self.buy_order_id, "")
        if self.sell_order_id is not None and self.sell_live:
            self.cancelOrder(self.sell_order_id, "")


def main():
    app = MidDeltaMarketMaker()
    app.connect(HOST, PORT, CLIENT_ID)

    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    while app.next_order_id is None:
        time.sleep(0.1)

    contract = app.make_stock_contract()

    app.reqPositions()
    print("Waiting for position snapshot...")
    while not app.position_snapshot_done:
        time.sleep(0.1)

    print(f"Initial position in {SYMBOL}: {app.current_position}")

    app.reqMktData(app.market_data_req_id, contract, "", False, False, [])
    print("Waiting for bid/ask...")
    while app.current_mid() is None:
        time.sleep(0.25)

    try:
        while True:
            mid = app.current_mid()
            if mid is not None:
                buy_qty = app.desired_buy_qty()
                sell_qty = app.desired_sell_qty()
                buy_px = app.desired_buy_price()
                sell_px = app.desired_sell_price()

                print(
                    f"bid={app.bid:.2f} ask={app.ask:.2f} mid={mid:.4f} pos={app.current_position} "
                    f"buy_qty={buy_qty} buy_px={buy_px:.2f} buy_cd={max(0, app.buy_cooldown_until-time.time()):.2f}s "
                    f"sell_qty={sell_qty} sell_px={sell_px:.2f} sell_cd={max(0, app.sell_cooldown_until-time.time()):.2f}s"
                )

                app.place_or_modify_buy(contract)
                app.place_or_modify_sell(contract)

            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print("Stopping...")
        app.cancel_all_working()
        time.sleep(1)
        app.disconnect()


if __name__ == "__main__":
    main()