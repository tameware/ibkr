"""Thin IB API app base: shared lifecycle hooks for peg_primary, market_maker, etc."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Sequence

from ibapi.client import EClient
from ibapi.wrapper import EWrapper

from ibkr_app_support import (
    IB_ERROR_CONNECTIVITY_RESTORED,
    build_logger,
    cfg_bool,
    disconnect_cleanly,
    ib_error_is_connectivity_restored,
    log_ib_error,
    make_stock_contract,
    stock_contract_from_details,
    stock_contract_on_primary_exchange,
    stock_contract_on_smart_exchange,
    subscribe_stock_nbbo_market_data,
)

HIST_REQ_ID = 1001
CONTRACT_DETAILS_REQ_ID = 2001
MARKET_DATA_SNAPSHOT_REQ_ID = 1002


class OpenPriceBootstrapMixin:
    """Request daily bars at startup and set ``open_price`` (not ``ref_price``)."""

    def _init_open_price_bootstrap(self) -> None:
        """Initialize ``open_price`` and historical bar buffer."""
        self.open_price: float | None = None
        self._bars: list[Any] = []

    def request_today_open_or_prior_close(self) -> None:
        """Request daily bars to seed ``open_price``."""
        self.logger.debug("Requesting daily bars for open/prior close")
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
        """IB callback: accumulate daily bar rows."""
        if reqId != HIST_REQ_ID:
            return
        self._bars.append(bar)

    def historicalDataEnd(self, reqId: Any, start: Any, end: Any) -> None:
        """IB callback: set ``open_price`` from the latest bar."""
        if reqId != HIST_REQ_ID:
            return

        if not self._bars:
            self.logger.debug("No historical bars returned; cannot set open_price")
            return

        last_bar = self._bars[-1]

        if last_bar.open and last_bar.open > 0:
            self.open_price = float(last_bar.open)
            self.logger.debug("Today's open price: %s", self.open_price)
        else:
            self.open_price = float(last_bar.close)
            self.logger.debug("No valid open; using prior close: %s", self.open_price)

        self.on_open_price_ready(self.open_price)

    def on_open_price_ready(self, open_price: float) -> None:
        """Hook after ``open_price`` is set; subclasses may extend."""


class SnapshotResyncMixin:
    """Debounce ``reqPositions`` / ``reqOpenOrders`` before calling ``sync_orders``."""

    def _init_snapshot_resync(self, config: Dict[str, Any]) -> None:
        """Initialize snapshot flags and resync debounce interval."""
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
        """Request a fresh ``reqPositions`` snapshot."""
        self.position_snapshot_complete = False
        self.reqPositions()

    def request_open_orders_snapshot(self) -> None:
        """Request a fresh ``reqOpenOrders`` snapshot."""
        self.open_orders_snapshot_complete = False
        self.on_open_orders_snapshot_start()
        self.reqOpenOrders()

    def _update_ready_for_trading(self) -> None:
        """Set ``ready_for_trading`` when both snapshots are done."""
        self.ready_for_trading = (
            self.position_snapshot_complete and self.open_orders_snapshot_complete
        )

    def positionEnd(self) -> None:
        """IB callback: position snapshot complete."""
        self.position_snapshot_complete = True
        self._update_ready_for_trading()
        self.maybe_sync_orders()

    def openOrderEnd(self) -> None:
        """IB callback: open-order snapshot complete."""
        self.on_open_orders_snapshot_end()

    def on_open_orders_snapshot_end(self) -> None:
        """Mark open orders snapshot done and maybe sync."""
        self.open_orders_snapshot_complete = True
        self._update_ready_for_trading()
        self.maybe_sync_orders()

    def maybe_sync_orders(self) -> None:
        """Run ``sync_orders`` after both snapshots if resync was requested."""
        if (
            self.sync_requested
            and self.position_snapshot_complete
            and self.open_orders_snapshot_complete
        ):
            self.sync_requested = False
            self.sync_orders()

    def trigger_resync(self, *, force: bool = False) -> None:
        """Debounced refresh of positions, open orders, then ``sync_orders``."""
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


class ContractResolutionMixin:
    """Resolve ``contract`` via ``reqContractDetails`` before ``reqMktData``."""

    def _init_contract_resolution(
        self,
        config: Dict[str, Any],
        *,
        contract_details_req_id: int = CONTRACT_DETAILS_REQ_ID,
    ) -> None:
        """Initialize contract-resolution state."""
        self.contract_details_req_id = contract_details_req_id
        self._market_data_subscribed = False
        self._market_data_contract: Any | None = None
        self._market_data_subscribed_ts = 0.0
        self._market_data_last_tick_ts = 0.0
        self._market_data_fallback_attempted = False
        self._market_data_route = "auto"
        self._market_data_use_delayed = False
        self._watchdog_resubscribe_count = 0
        self._last_watchdog_resubscribe_ts = 0.0
        self._last_snapshot_poll_ts = 0.0
        self._market_data_snapshot_in_flight = False
        self._market_data_snapshot_started_ts = 0.0
        self.market_data_snapshot_timeout_seconds = float(
            config.get("market_data_snapshot_timeout_seconds", 10.0)
        )
        self.market_data_stall_seconds = float(
            config.get("market_data_stall_seconds", 15.0)
        )
        self.market_data_watchdog_resubscribe_seconds = float(
            config.get("market_data_watchdog_resubscribe_seconds", 60.0)
        )
        self.market_data_snapshot_poll_seconds = float(
            config.get("market_data_snapshot_poll_seconds", 30.0)
        )
        self.market_data_allow_delayed_fallback = cfg_bool(
            config, "market_data_allow_delayed_fallback", True
        )
        self.market_data_snapshot_req_id = int(
            config.get("market_data_snapshot_req_id", MARKET_DATA_SNAPSHOT_REQ_ID)
        )

    def request_contract_details(self) -> None:
        """Request IB contract details for :attr:`contract`."""
        self._market_data_subscribed = False
        self.reqContractDetails(self.contract_details_req_id, self.contract)

    def contractDetails(self, reqId: Any, contractDetails: Any) -> None:
        """IB callback: replace :attr:`contract` with IB's resolved definition."""
        if int(reqId) != int(self.contract_details_req_id):
            return
        self.contract = stock_contract_from_details(contractDetails)
        self._market_data_contract = self.contract
        self.on_contract_resolved(contractDetails)

    def on_contract_resolved(self, contractDetails: Any) -> None:
        """Hook after :attr:`contract` is updated from ``contractDetails``."""

    def contractDetailsEnd(self, reqId: Any) -> None:
        """IB callback: subscribe to NBBO after the contract is resolved."""
        if int(reqId) != int(self.contract_details_req_id):
            return
        self.logger.debug(
            "Contract details complete reqId=%s conId=%s symbol=%s exchange=%s primaryExchange=%s",
            reqId,
            getattr(self.contract, "conId", None),
            getattr(self.contract, "symbol", None),
            getattr(self.contract, "exchange", None),
            getattr(self.contract, "primaryExchange", None),
        )
        self.ensure_market_data_subscription(reason="contract_resolved")

    def market_data_contract(self) -> Any:
        """Contract used for ``reqMktData`` (resolved from ``contractDetails``).

        When ``primaryExchange`` differs from ``exchange`` (e.g. SMART vs AMEX),
        subscribe on the primary listing — IB often delivers no bid/ask ticks on
        SMART for some symbols (OZ) even when TWS shows a live quote.

        :attr:`_market_data_route` ``smart`` / ``primary`` override routing during
        watchdog recovery (alternate SMART vs listing).
        """
        base = self._market_data_contract or self.contract
        route = str(getattr(self, "_market_data_route", "auto") or "auto")
        if route == "smart":
            return stock_contract_on_smart_exchange(base)
        if route == "primary":
            primary = str(getattr(base, "primaryExchange", "") or "").strip()
            if primary:
                return stock_contract_on_primary_exchange(base)
        primary = str(getattr(base, "primaryExchange", "") or "").strip()
        current = str(getattr(base, "exchange", "") or "").strip()
        if primary and primary != current:
            return stock_contract_on_primary_exchange(base)
        return base

    def market_data_snapshot_req_ids(self) -> Sequence[int]:
        """Request ids for one-shot ``reqMktData`` snapshot polls (watchdog fallback)."""
        if self.market_data_snapshot_poll_seconds <= 0:
            return ()
        return (self.market_data_snapshot_req_id,)

    def all_market_data_req_ids(self) -> Sequence[int]:
        """Streaming + snapshot ids (for ``cancelMktData`` on shutdown)."""
        ids: list[int] = []
        for req_id in self.market_data_req_ids():
            ids.append(int(req_id))
        for req_id in self.market_data_snapshot_req_ids():
            rid = int(req_id)
            if rid not in ids:
                ids.append(rid)
        return ids

    def is_nbbo_market_data_req(self, req_id: Any) -> bool:
        """True when ``req_id`` is a streaming or snapshot NBBO subscription."""
        rid = int(req_id)
        return rid in {int(x) for x in self.all_market_data_req_ids()}

    def note_market_data_tick(self) -> None:
        """Record that a market data callback arrived (for stall recovery)."""
        self._market_data_last_tick_ts = time.monotonic()

    def tickSnapshotEnd(self, reqId: int) -> None:
        """IB callback: snapshot ``reqMktData`` finished (allows another poll)."""
        if int(reqId) in {int(x) for x in self.market_data_snapshot_req_ids()}:
            self._market_data_snapshot_in_flight = False

    def maybe_recover_stalled_market_data(self) -> None:
        """Re-subscribe on the primary exchange if no ticks arrive after subscribe."""
        if (
            self.shutdown_flag
            or not self.isConnected()
            or not self._market_data_subscribed
            or self._market_data_fallback_attempted
        ):
            return
        now = time.monotonic()
        if now - self._market_data_subscribed_ts < self.market_data_stall_seconds:
            return
        if self._market_data_last_tick_ts > self._market_data_subscribed_ts:
            return
        primary = str(getattr(self.contract, "primaryExchange", "") or "").strip()
        current_exchange = str(getattr(self.market_data_contract(), "exchange", "") or "")
        if not primary or primary == current_exchange:
            return
        self._market_data_fallback_attempted = True
        self._market_data_contract = stock_contract_on_primary_exchange(self.contract)
        self.logger.warning(
            "No market data ticks after %.0fs; re-subscribing on primary exchange %s (conId=%s)",
            self.market_data_stall_seconds,
            primary,
            getattr(self.contract, "conId", None),
        )
        self.ensure_market_data_subscription(
            reason="primary_exchange_fallback",
            force=True,
        )

    def _market_data_type_for_subscribe(self) -> int | None:
        if self._market_data_use_delayed:
            return 3
        return None

    def _advance_market_data_route_for_recovery(self) -> None:
        """Toggle SMART vs primary listing for the next watchdog resubscribe."""
        base = self._market_data_contract or self.contract
        primary = str(getattr(base, "primaryExchange", "") or "").strip()
        if not primary:
            return
        route = str(self._market_data_route or "auto")
        if route in ("auto", "primary"):
            self._market_data_route = "smart"
        else:
            self._market_data_route = "primary"

    def poll_market_data_snapshot(self) -> None:
        """Request a one-shot NBBO snapshot (``reqMktData`` with ``snapshot=True``)."""
        snap_ids = list(self.market_data_snapshot_req_ids())
        if not snap_ids or self.shutdown_flag or not self.isConnected():
            return
        if self._market_data_snapshot_in_flight:
            elapsed = time.monotonic() - self._market_data_snapshot_started_ts
            if elapsed < self.market_data_snapshot_timeout_seconds:
                return
            self._market_data_snapshot_in_flight = False
        contract = self.market_data_contract()
        md_type = self._market_data_type_for_subscribe()
        if md_type is not None and hasattr(self, "reqMarketDataType"):
            self.reqMarketDataType(int(md_type))
        for req_id in snap_ids:
            subscribe_stock_nbbo_market_data(
                self,
                int(req_id),
                contract,
                live=md_type is None,
                cancel_first=False,
                snapshot=True,
            )
        self._market_data_snapshot_in_flight = True
        self._market_data_snapshot_started_ts = time.monotonic()
        self.logger.debug(
            "NBBO snapshot poll reqIds=%s exchange=%s primaryExchange=%s conId=%s",
            snap_ids,
            getattr(contract, "exchange", None),
            getattr(contract, "primaryExchange", None),
            getattr(contract, "conId", None),
        )

    def maybe_watchdog_recover_market_data(self, *, nbbo_ok: bool) -> None:
        """Force resubscribe and/or snapshot poll when NBBO is missing or stale."""
        if (
            nbbo_ok
            or self.shutdown_flag
            or not self.isConnected()
            or not self._market_data_subscribed
        ):
            return
        now = time.monotonic()
        if now - self._market_data_subscribed_ts < self.market_data_stall_seconds:
            return

        resub_interval = self.market_data_watchdog_resubscribe_seconds
        snap_interval = self.market_data_snapshot_poll_seconds

        if (
            resub_interval > 0
            and (now - self._last_watchdog_resubscribe_ts) >= resub_interval
        ):
            self._last_watchdog_resubscribe_ts = now
            self._watchdog_resubscribe_count += 1
            self._market_data_fallback_attempted = False
            self._advance_market_data_route_for_recovery()
            if (
                self.market_data_allow_delayed_fallback
                and self._watchdog_resubscribe_count % 3 == 0
            ):
                self._market_data_use_delayed = True
                self.logger.debug(
                    "Watchdog: enabling delayed market data (type 3) after %s resubscribes",
                    self._watchdog_resubscribe_count,
                )
            nbbo = getattr(self, "_nbbo", None)
            if nbbo is not None:
                nbbo.reset()
            contract = self.market_data_contract()
            self.logger.debug(
                "Watchdog: force re-subscribe route=%s exchange=%s (attempt %s)",
                self._market_data_route,
                getattr(contract, "exchange", None),
                self._watchdog_resubscribe_count,
            )
            self.ensure_market_data_subscription(
                reason="watchdog_resubscribe",
                force=True,
            )

        if snap_interval > 0 and (now - self._last_snapshot_poll_ts) >= snap_interval:
            self._last_snapshot_poll_ts = now
            self.poll_market_data_snapshot()

    def ensure_market_data_subscription(
        self,
        *,
        reason: str,
        force: bool = False,
    ) -> None:
        """Subscribe or refresh NBBO using the resolved market-data contract."""
        if self.shutdown_flag or not self.isConnected():
            return
        req_ids = list(self.market_data_req_ids())
        if not req_ids:
            return
        contract = self.market_data_contract()
        cancel_first = self._market_data_subscribed or force
        md_type = self._market_data_type_for_subscribe()
        for req_id in req_ids:
            subscribe_stock_nbbo_market_data(
                self,
                int(req_id),
                contract,
                live=md_type is None,
                cancel_first=cancel_first,
                market_data_type=md_type,
            )
        self._market_data_subscribed = True
        self._market_data_subscribed_ts = time.monotonic()
        if not force:
            self._market_data_fallback_attempted = False
        msg = (
            "NBBO market data subscribed (%s) reqIds=%s exchange=%s conId=%s delayed=%s"
        )
        args = (
            reason,
            req_ids,
            getattr(contract, "exchange", None),
            getattr(contract, "conId", None),
            self._market_data_use_delayed,
        )
        self.logger.debug(msg, *args)

    def subscribe_market_data(self) -> None:
        """Subscribe or refresh streaming NBBO (after resolve or on reconnect)."""
        self.ensure_market_data_subscription(reason="subscribe_market_data")


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
        """Initialize :class:`IbkrBotApp`."""
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
        self._last_market_data_resubscribe_ts = 0.0
        self.market_data_resubscribe_debounce_seconds = float(
            config.get("market_data_resubscribe_debounce_seconds", 1.0)
        )

    @property
    def api_ready(self) -> bool:
        """True after ``nextValidId`` (socket API ready to trade)."""
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

    def on_ib_connectivity_restored(self) -> None:
        """Re-subscribe to market data after TWS/Gateway error 1102."""
        if self.shutdown_flag or not self.isConnected():
            return
        if not self.market_data_req_ids():
            return

        now = time.monotonic()
        debounce = self.market_data_resubscribe_debounce_seconds
        if now - self._last_market_data_resubscribe_ts < debounce:
            return
        self._last_market_data_resubscribe_ts = now

        self.logger.info(
            "TWS connectivity restored (error %s); re-subscribing to market data",
            IB_ERROR_CONNECTIVITY_RESTORED,
        )
        nbbo = getattr(self, "_nbbo", None)
        if nbbo is not None:
            nbbo.reset()
        self.subscribe_market_data()

    def assign_next_order_id(self, order_id: int) -> None:
        """Store the next usable order id from IB."""
        self.next_order_id = order_id

    def _mark_shutdown(self) -> None:
        """Set ``shutdown_flag`` for the main loop."""
        self.shutdown_flag = True

    def connectAck(self) -> None:
        """IB callback: connection acknowledged."""
        self.logger.debug("IBKR connectAck received")

    def connectionClosed(self) -> None:
        """IB callback: mark disconnected and run teardown hook."""
        self._api_ready = False
        self.shutdown_flag = True
        self.logger.warning("IBKR connection closed (connectionClosed callback)")
        self.on_connection_closed()

    def nextValidId(self, orderId: int) -> None:
        """IB callback: API ready; assign order id and run ``startup``."""
        self.logger.debug("nextValidId: %s", orderId)
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
        """IB callback: log errors via shared filter rules."""
        log_ib_error(
            self.logger,
            self.config,
            req_id=reqId,
            error_time=errorTime,
            error_code=errorCode,
            error_string=errorString,
            advanced_order_reject=advancedOrderReject,
        )
        if ib_error_is_connectivity_restored(errorCode):
            try:
                self.on_ib_connectivity_restored()
            except Exception as e:
                self.logger.warning(
                    "Market data re-subscribe after connectivity restore failed: %s",
                    e,
                )

    def stop(self) -> None:
        """Cancel quotes, disconnect, and cancel market data subscriptions."""
        if self._stop_called:
            return
        self._stop_called = True
        self._mark_shutdown()
        self.logger.info("Shutdown requested.")

        try:
            self.shutdown_quotes()
        except Exception as e:
            self.logger.warning("Order cancel during shutdown failed: %s", e)

        cancel_ids = (
            list(self.all_market_data_req_ids())
            if hasattr(self, "all_market_data_req_ids")
            else list(self.market_data_req_ids())
        )
        disconnect_cleanly(
            self,
            logger=self.logger,
            market_data_req_ids=cancel_ids,
        )
