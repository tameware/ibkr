"""Thin IB API app base: shared lifecycle hooks for peg_primary, market_maker, etc."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Sequence

from ibapi.client import EClient
from ibapi.wrapper import EWrapper

from ibkr_app_support import (
    build_logger,
    cfg_bool,
    disconnect_cleanly,
    log_ib_error,
    make_stock_contract,
)

HIST_REQ_ID = 1001


class OpenPriceBootstrapMixin:
    """Request daily bars at startup and set ``open_price`` / ``ref_price``."""

    def _init_open_price_bootstrap(self) -> None:
        self.open_price: float | None = None
        self._bars: list[Any] = []

    def request_today_open_or_prior_close(self) -> None:
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

    def historicalData(self, reqId: Any, bar: Any) -> None:
        if reqId != HIST_REQ_ID:
            return
        self._bars.append(bar)

    def historicalDataEnd(self, reqId: Any, start: Any, end: Any) -> None:
        if reqId != HIST_REQ_ID:
            return

        if not self._bars:
            self.logger.info("No historical bars returned; cannot set open_price")
            return

        last_bar = self._bars[-1]

        if last_bar.open and last_bar.open > 0:
            self.open_price = float(last_bar.open)
            self.logger.info("Today's open price: %s", self.open_price)
        else:
            self.open_price = float(last_bar.close)
            self.logger.info("No valid open; using prior close: %s", self.open_price)

        self.on_open_price_ready(self.open_price)

    def on_open_price_ready(self, open_price: float) -> None:
        """Default: seed ``ref_price`` when still unset."""
        if getattr(self, "ref_price", None) is None:
            self.ref_price = open_price


class SnapshotResyncMixin:
    """Debounce ``reqPositions`` / ``reqOpenOrders`` before calling ``sync_orders``."""

    def _init_snapshot_resync(self, config: Dict[str, Any]) -> None:
        self.ready_for_trading = False
        self.position_snapshot_complete = False
        self.open_orders_snapshot_complete = False
        self.sync_requested = False
        self.last_resync_request_ts = 0.0
        self.resync_debounce_seconds = float(
            config.get("resync_debounce_seconds", 0.35)
        )

    def on_open_orders_snapshot_start(self) -> None:
        """Reset per-symbol open-order counters before ``reqOpenOrders``."""
        self.open_symbol_buys = 0
        self.open_symbol_sells = 0

    def request_positions_snapshot(self) -> None:
        self.position_snapshot_complete = False
        self.reqPositions()

    def request_open_orders_snapshot(self) -> None:
        self.open_orders_snapshot_complete = False
        self.on_open_orders_snapshot_start()
        self.reqOpenOrders()

    def _update_ready_for_trading(self) -> None:
        self.ready_for_trading = (
            self.position_snapshot_complete and self.open_orders_snapshot_complete
        )

    def positionEnd(self) -> None:
        self.position_snapshot_complete = True
        self._update_ready_for_trading()
        self.maybe_sync_orders()

    def openOrderEnd(self) -> None:
        self.on_open_orders_snapshot_end()

    def on_open_orders_snapshot_end(self) -> None:
        self.open_orders_snapshot_complete = True
        self._update_ready_for_trading()
        self.maybe_sync_orders()

    def maybe_sync_orders(self) -> None:
        if (
            self.sync_requested
            and self.position_snapshot_complete
            and self.open_orders_snapshot_complete
        ):
            self.sync_requested = False
            self.sync_orders()

    def trigger_resync(self, *, force: bool = False) -> None:
        if self.shutdown_flag or not self.isConnected():
            return

        now = time.monotonic()
        if not force and now - self.last_resync_request_ts < self.resync_debounce_seconds:
            return

        self.last_resync_request_ts = now
        self.sync_requested = True
        self.request_positions_snapshot()
        self.request_open_orders_snapshot()
        self.maybe_sync_orders()


class IbkrBotApp(EWrapper, EClient, ABC):
    """Shared IBKR bot shell: config, contract, logging, errors, connect/teardown.

    Subclasses implement quoting (``startup``, order management) via hooks.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        *,
        logger_name: str,
        default_log_file: str,
    ) -> None:
        EClient.__init__(self, self)
        self.config = config
        self.contract = make_stock_contract(config)
        self.logger = build_logger(
            config,
            logger_name=logger_name,
            default_log_file=default_log_file,
        )
        self.shutdown_flag = False
        self._stop_called = False
        self._api_ready = False
        self.cancel_open_orders_on_shutdown = cfg_bool(
            config, "cancel_open_orders_on_shutdown", False
        )

    @property
    def api_ready(self) -> bool:
        return self._api_ready

    @api_ready.setter
    def api_ready(self, value: bool) -> None:
        self._api_ready = value

    def market_data_req_ids(self) -> Sequence[int]:
        """Request ids passed to ``cancelMktData`` on shutdown."""
        return ()

    @abstractmethod
    def shutdown_quotes(self) -> None:
        """Cancel or clear bot-managed working orders before disconnect."""

    def on_connection_closed(self) -> None:
        """Extra teardown when IB invokes ``connectionClosed``."""

    def on_api_ready(self, order_id: int) -> None:
        """Called once per connection after ``nextValidId`` (default: ``startup``)."""
        self.startup()

    @abstractmethod
    def startup(self) -> None:
        """Subscribe to market data and request startup snapshots."""

    def assign_next_order_id(self, order_id: int) -> None:
        """Store the next usable order id from IB."""
        self.next_order_id = order_id

    def _mark_shutdown(self) -> None:
        self.shutdown_flag = True

    def connectAck(self) -> None:
        self.logger.info("IBKR connectAck received")

    def connectionClosed(self) -> None:
        self._api_ready = False
        self.shutdown_flag = True
        self.logger.warning("IBKR connection closed (connectionClosed callback)")
        self.on_connection_closed()

    def nextValidId(self, orderId: int) -> None:
        self.logger.info("nextValidId: %s", orderId)
        self._api_ready = True
        self.assign_next_order_id(orderId)
        self.on_api_ready(orderId)

    def error(
        self,
        reqId: Any,
        errorTime: Any,
        errorCode: Any,
        errorString: Any,
        advancedOrderReject: str = "",
    ) -> None:
        log_ib_error(
            self.logger,
            self.config,
            req_id=reqId,
            error_time=errorTime,
            error_code=errorCode,
            error_string=errorString,
            advanced_order_reject=advancedOrderReject,
        )

    def stop(self) -> None:
        if self._stop_called:
            return
        self._stop_called = True
        self._mark_shutdown()
        self.logger.info("Shutdown requested.")

        try:
            self.shutdown_quotes()
        except Exception as e:
            self.logger.warning("Order cancel during shutdown failed: %s", e)

        disconnect_cleanly(
            self,
            logger=self.logger,
            market_data_req_ids=list(self.market_data_req_ids()),
        )
