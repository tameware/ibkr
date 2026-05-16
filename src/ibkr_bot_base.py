"""Thin IB API app base: shared lifecycle hooks for peg_primary, market_maker, etc."""

from __future__ import annotations

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
