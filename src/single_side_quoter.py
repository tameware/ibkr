"""Shared single-side quoter (flat: buy only; long: sell only)."""

from __future__ import annotations

import argparse
import sys
import time
from abc import abstractmethod
from decimal import Decimal
from typing import Any, Callable, Dict, Optional, Sequence

from ibapi.common import TickAttrib, TickAttribLast, TickerId
from ibapi.order import Order
from ibapi.ticktype import TickType

from ibkr_app_support import (
    PositionLedger,
    add_config_argument,
    add_ib_connection_arguments,
    add_logging_arguments,
    add_min_order_size_argument,
    add_never_sell_below_avg_cost_argument,
    add_session_hours_arguments,
    has_valid_nbbo,
    NbboCoalesceSink,
    handle_ledger_execution,
    handle_ledger_ib_position,
    ib_client_id_from_config,
    load_merged_config,
    log_startup_timezones,
    max_sell_shares,
    meets_min_order_size,
    min_order_size_from_config,
    regular_session_open,
    run_bot,
    safe_cancel_order,
    sync_attrs_from_ledger,
)
from ibkr_bot_base import (
    ContractResolutionMixin,
    IbkrBotApp,
    OpenPriceBootstrapMixin,
    SnapshotResyncMixin,
)

LAST_TRADE_REQ_ID = 2001
MKTDATA_REQ_ID = 3001

_BASE_REQUIRED_FIELDS = [
    "host",
    "port",
    "symbol",
    "sec_type",
    "currency",
    "exchange",
    "primary_exchange",
    "max_pos",
    "loop_seconds",
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


class SingleSideQuoter(
    ContractResolutionMixin,
    OpenPriceBootstrapMixin,
    SnapshotResyncMixin,
    IbkrBotApp,
):
    """Flat: buy up to ``max_pos``; long: sell inventory only (mutually exclusive sides)."""

    def __init__(
        self,
        config: Dict[str, Any],
        *,
        logger_name: str,
        default_log_file: str,
        ledger_name: str,
        default_client_id: int,
    ) -> None:
        """Initialize :class:`SingleSideQuoter`."""
        super().__init__(
            config,
            logger_name=logger_name,
            default_log_file=default_log_file,
        )

        self.nextOrderId: int | None = None
        self.position_size = 0
        self.avg_cost = 0.0
        self._init_open_price_bootstrap()

        self.buy_order_id: int | None = None
        self.sell_order_id: int | None = None
        self.ref_price = None
        self._nbbo = NbboCoalesceSink(
            self.config, self._on_coalesced_nbbo, logger=self.logger
        )

        self.remaining_by_order: Dict[int, Any] = {}
        self.open_symbol_buys = 0
        self.open_symbol_sells = 0
        self.pending_buy = False
        self.pending_sell = False
        self._init_snapshot_resync(config)
        self._init_contract_resolution(config)
        self.min_order_size = min_order_size_from_config(config)

        self.client_id = ib_client_id_from_config(
            config, default=default_client_id
        )
        self.ledger = PositionLedger.open(
            ledger_name, config, client_id=self.client_id
        )
        sync_attrs_from_ledger(
            self.ledger, self, qty_attr="position_size", avg_attr="avg_cost"
        )
        self.logger.info(
            "Position ledger %s qty=%s avg_cost_per_share=%.4f",
            self.ledger.path,
            self.ledger.qty,
            self.ledger.avg_cost_per_share,
        )

    def market_data_req_ids(self) -> Sequence[int]:
        """Market data request ids to cancel on shutdown."""
        return (MKTDATA_REQ_ID,)

    def assign_next_order_id(self, order_id: int) -> None:
        """Store the next usable order id from IB."""
        self.nextOrderId = order_id

    @property
    def _bid(self) -> float | None:
        return self._nbbo.bid

    @_bid.setter
    def _bid(self, value: float | None) -> None:
        self._nbbo.bid = None if value is None else float(value)
        self._nbbo.satisfy_pair_for_quoting()

    @property
    def _ask(self) -> float | None:
        return self._nbbo.ask

    @_ask.setter
    def _ask(self, value: float | None) -> None:
        self._nbbo.ask = None if value is None else float(value)
        self._nbbo.satisfy_pair_for_quoting()

    def _on_coalesced_nbbo(self, bid: float, ask: float) -> None:
        """Update ``ref_price`` from coalesced bid/ask mid."""
        mid = (bid + ask) / 2.0
        if mid != self.ref_price:
            self.ref_price = mid

    def _has_valid_nbbo(self) -> bool:
        return self._nbbo.is_ready_for_quoting()

    def shutdown_quotes(self) -> None:
        """Cancel working quotes and clear local order tracking."""
        self._nbbo.cancel()
        if self.cancel_open_orders_on_shutdown:
            for label, oid in (("BUY", self.buy_order_id), ("SELL", self.sell_order_id)):
                if oid is None:
                    continue
                safe_cancel_order(self, oid)
                self.logger.info("Cancel %s order id=%s on shutdown", label, oid)
        self.buy_order_id = None
        self.sell_order_id = None
        self.pending_buy = False
        self.pending_sell = False

    def startup(self) -> None:
        """Subscribe to market data and request startup snapshots."""
        self._nbbo.reset()
        self._market_data_subscribed = False
        self.request_positions_snapshot()
        self.request_today_open_or_prior_close()
        self.request_contract_details()
        self.request_open_orders_snapshot()
        self.reqTickByTickData(
            LAST_TRADE_REQ_ID,
            self.contract,
            "Last",
            0,
            False,
        )

    @abstractmethod
    def compute_order_limits(self) -> tuple[float, float]:
        """Return protective ``(buy_limit, sell_limit)`` from ``ref_price``."""

    @abstractmethod
    def make_quote_order(self, action: str, qty: int, limit_price: float) -> Order:
        """Build a BUY or SELL quote order for this strategy variant."""

    def format_place_order_log(
        self, *, side: str, qty: int, limit: float, order: Order, order_id: int
    ) -> str:
        """Human-readable log line for a new order submission."""
        order_type = getattr(order, "orderType", "?")
        return f"Placing {order_type} {side} {qty} @ {limit}, id={order_id}"

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
        """IB callback: track fills and trigger resync on terminal status."""
        tracked = orderId in (self.buy_order_id, self.sell_order_id)

        if status in ("Filled", "Cancelled", "Inactive", "ApiCancelled"):
            if orderId == self.buy_order_id:
                self.logger.info(f"BUY {orderId} {status} @ {avgFillPrice}")
                self.pending_buy = False
                self.buy_order_id = None
            elif orderId == self.sell_order_id:
                self.logger.info(f"SELL {orderId} {status} @ {avgFillPrice}")
                self.pending_sell = False
                self.sell_order_id = None
            if tracked:
                self.remaining_by_order.pop(orderId, None)
                self.trigger_resync()
        elif status == "PartiallyFilled" and tracked:
            self.logger.info(
                f"Order {orderId} partially filled: filled={filled} "
                f"remaining={remaining} avgFillPrice={avgFillPrice}"
            )
            self.trigger_resync()

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
        """IB callback: last-trade ticks (``ref_price`` comes from NBBO only)."""
        return

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib):
        """IB callback: stage bid/ask ticks for NBBO coalescing."""
        if not self.is_nbbo_market_data_req(reqId):
            return
        self.note_market_data_tick()
        self._nbbo.stage_tick_price(int(tickType), float(price))

    def openOrder(self, orderId, contract, order, orderState):
        """IB callback: track working orders for this symbol."""
        if contract.symbol == self.config["symbol"] and contract.secType == self.config["sec_type"]:
            if order.action == "BUY":
                self.open_symbol_buys += 1
                self.buy_order_id = orderId
            elif order.action == "SELL":
                self.open_symbol_sells += 1
                self.sell_order_id = orderId

    def on_open_orders_snapshot_end(self) -> None:
        """Mark open orders snapshot done and maybe sync."""
        self.logger.info(
            f"Open {self.config['symbol']} orders: "
            f"buys={self.open_symbol_buys}, sells={self.open_symbol_sells}"
        )
        super().on_open_orders_snapshot_end()

    def execDetails(self, reqId, contract, execution):
        """IB callback: apply fills to the position ledger."""
        if (
            contract.symbol != self.config["symbol"]
            or contract.secType != self.config["sec_type"]
        ):
            return
        if handle_ledger_execution(
            self.ledger, execution, self.client_id, logger=self.logger
        ):
            sync_attrs_from_ledger(
                self.ledger, self, qty_attr="position_size", avg_attr="avg_cost"
            )
            self.logger.info(
                f"Ledger {self.ledger.path.name} qty={self.ledger.qty} "
                f"avg_cost_per_share={self.ledger.avg_cost_per_share:.4f} "
                f"after fill execId={getattr(execution, 'execId', '')}"
            )

    def position(self, account, contract, pos, avgCost):
        """IB callback: reconcile IB position snapshot with ledger."""
        if contract.symbol == self.config["symbol"] and contract.secType == self.config["sec_type"]:
            handle_ledger_ib_position(
                self.ledger,
                account,
                int(pos),
                float(avgCost),
                contract=contract,
                logger=self.logger,
            )
            sync_attrs_from_ledger(
                self.ledger, self, qty_attr="position_size", avg_attr="avg_cost"
            )

    def us_regular_hours(self):
        """True during configured regular session hours."""
        return regular_session_open(self.config)

    def sync_orders(self):
        """Place, cancel, or amend quotes to match position and limits."""
        if not self.ready_for_trading:
            return
        if self.nextOrderId is None or not self._has_valid_nbbo():
            return
        if self.ref_price is None:
            return

        buy_limit, sell_limit = self.compute_order_limits()
        pos = self.position_size

        if pos <= 0:
            desired_side = "BUY"
            desired_qty = int(self.config["max_pos"])
            desired_limit = buy_limit
        else:
            desired_side = "SELL"
            desired_qty = max_sell_shares(self.ledger)
            desired_limit = sell_limit

        if desired_side == "BUY":
            if self.sell_order_id is not None:
                self.logger.info(
                    f"Cancelling opposite SELL order id={self.sell_order_id}"
                )
                safe_cancel_order(self, self.sell_order_id)
                self.pending_sell = False
                self.sell_order_id = None
                self.open_symbol_sells = 0
                return

            if self.open_symbol_sells > 0:
                return

            if (
                meets_min_order_size(desired_qty, self.min_order_size)
                and self.open_symbol_buys == 0
                and not self.pending_buy
            ):
                oid = self.nextOrderId
                self.nextOrderId += 1
                self.buy_order_id = oid
                self.pending_buy = True
                order = self.make_quote_order("BUY", desired_qty, desired_limit)
                self.logger.info(
                    self.format_place_order_log(
                        side="BUY",
                        qty=desired_qty,
                        limit=desired_limit,
                        order=order,
                        order_id=oid,
                    )
                )
                self.placeOrder(oid, self.contract, order)
        else:
            if self.buy_order_id is not None:
                self.logger.info(
                    f"Cancelling opposite BUY order id={self.buy_order_id}"
                )
                safe_cancel_order(self, self.buy_order_id)
                self.pending_buy = False
                self.buy_order_id = None
                self.open_symbol_buys = 0
                return

            if self.open_symbol_buys > 0:
                return

            if desired_qty <= 0 and self.sell_order_id is not None:
                self.logger.info(
                    f"Cancelling SELL order id={self.sell_order_id} (no shares to sell)"
                )
                safe_cancel_order(self, self.sell_order_id)
                self.pending_sell = False
                self.sell_order_id = None
                self.open_symbol_sells = 0
                return

            if (
                meets_min_order_size(desired_qty, self.min_order_size)
                and self.open_symbol_sells == 0
                and not self.pending_sell
            ):
                oid = self.nextOrderId
                self.nextOrderId += 1
                self.sell_order_id = oid
                self.pending_sell = True
                order = self.make_quote_order("SELL", desired_qty, desired_limit)
                self.logger.info(
                    self.format_place_order_log(
                        side="SELL",
                        qty=desired_qty,
                        limit=desired_limit,
                        order=order,
                        order_id=oid,
                    )
                )
                self.placeOrder(oid, self.contract, order)

    def run_until_shutdown(self) -> None:
        """Main loop: periodic resync while connected and in session."""
        while not self.shutdown_flag:
            if not self.isConnected():
                self.logger.info("Disconnected from IBKR; exiting run loop")
                break
            if self.us_regular_hours():
                self.trigger_resync()
            time.sleep(self.config["loop_seconds"])


def add_single_side_arguments(parser: argparse.ArgumentParser) -> None:
    """Add CLI flags shared by single-side quoter bots."""
    parser.add_argument("--max_pos", type=int)
    parser.add_argument("--loop_seconds", type=float)
    parser.add_argument("--resync_debounce_seconds", type=float)
    add_session_hours_arguments(parser)
    add_min_order_size_argument(parser)
    parser.add_argument("--last_trade_min_size", type=float)
    parser.add_argument("--tif")
    parser.add_argument("--price_round_digits", type=int)
    parser.add_argument(
        "--cancel_open_orders_on_shutdown",
        action=argparse.BooleanOptionalAction,
        help="Cancel tracked BUY/SELL quotes before disconnect (default: false)",
    )
    add_never_sell_below_avg_cost_argument(parser)
    add_logging_arguments(parser)


def run_single_side_main(
    *,
    description: str,
    script_file: str,
    trader_cls: type[SingleSideQuoter],
    extra_required: Sequence[str] = (),
    configure_parser: Optional[Callable[[argparse.ArgumentParser], None]] = None,
) -> None:
    """Parse config, construct trader, and run ``run_bot``."""
    parser = argparse.ArgumentParser(description=description)
    add_config_argument(parser, script_file)
    add_ib_connection_arguments(parser, include_client_id=False)
    add_single_side_arguments(parser)
    if configure_parser is not None:
        configure_parser(parser)

    args = parser.parse_args()
    required = list(_BASE_REQUIRED_FIELDS) + list(extra_required)
    config = load_merged_config(args, required=required)
    log_startup_timezones(config)

    app = trader_cls(config)
    exit_code = run_bot(
        app,
        config,
        is_ready=lambda: app.api_ready,
        main_loop=app.run_until_shutdown,
        ready_label="nextValidId",
    )
    if exit_code != 0:
        sys.exit(exit_code)
