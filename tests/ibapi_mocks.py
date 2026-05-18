"""Shared IB API stubs for unit tests.

Call :func:`install_ibapi_mocks` before importing ``src`` modules that depend on
``ibapi`` (typically at the top of each test module).
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, Mock


class MockEWrapper:
    pass


class MockEClient:
    def __init__(self, wrapper):
        pass


class MockContract:
    def __init__(self):
        self.symbol = ""
        self.secType = ""
        self.currency = ""
        self.exchange = ""
        self.primaryExch = ""
        self.primaryExchange = ""
        self.conId = 0


class MockOrder:
    def __init__(self):
        self.action = ""
        self.orderType = ""
        self.totalQuantity = 0
        self.lmtPrice = 0.0
        self.auxPrice = 0.0
        self.exchange = ""
        self.tif = ""
        self.notHeld = False
        self.minCompeteSize = 0
        self.competeAgainstBestOffset = ""
        self.midOffsetAtWhole = 0.0
        self.midOffsetAtHalf = 0.0
        self.postToAts = 0
        self.outsideRth = False


class MockOrderCancel:
    pass


class MockTimezone:
    def localize(self, dt):
        return dt


def install_ibapi_mocks(
    *,
    ticktype: bool = True,
    pytz: bool = False,
    order_cancel: bool = False,
    peg_best_constants: bool = False,
) -> MagicMock:
    """Register mock ``ibapi`` (and related) modules in :data:`sys.modules`."""
    mock_ibapi = MagicMock()
    mock_ibapi.wrapper.EWrapper = MockEWrapper
    mock_ibapi.client.EClient = MockEClient
    mock_ibapi.contract.Contract = MockContract
    mock_ibapi.order.Order = MockOrder

    sys.modules["ibapi"] = mock_ibapi
    sys.modules["ibapi.client"] = mock_ibapi.client
    sys.modules["ibapi.wrapper"] = mock_ibapi.wrapper
    sys.modules["ibapi.contract"] = mock_ibapi.contract
    sys.modules["ibapi.order"] = mock_ibapi.order
    sys.modules["ibapi.common"] = MagicMock()

    if ticktype:
        sys.modules["ibapi.ticktype"] = MagicMock()

    if order_cancel:
        mock_ibapi.order_cancel = MagicMock()
        mock_ibapi.order_cancel.OrderCancel = MockOrderCancel
        sys.modules["ibapi.order_cancel"] = mock_ibapi.order_cancel

    if peg_best_constants:
        mock_ibapi.order.COMPETE_AGAINST_BEST_OFFSET_UP_TO_MID = (
            "COMPETE_AGAINST_BEST_OFFSET_UP_TO_MID"
        )

    if pytz:
        mock_pytz = MagicMock()
        mock_pytz.timezone = Mock(return_value=MockTimezone())
        sys.modules["pytz"] = mock_pytz

    return mock_ibapi


def seed_valid_nbbo(trader: object, mid: float, spread: float = 0.2) -> None:
    """Set bid/ask/ref_price on a trader for tests that call sync_orders."""
    half = spread / 2.0
    trader._bid = mid - half  # type: ignore[attr-defined]
    trader._ask = mid + half  # type: ignore[attr-defined]
    trader.ref_price = mid  # type: ignore[attr-defined]
