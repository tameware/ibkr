"""Pure quote-decision engine for the market maker.

Encodes the strategy goals directly:

* never quote a sell larger than the current position (never go short),
* never let a buy fill push the position above ``max_position``,
* floor every sell at avg_cost + round-trip commission + minimum profit,
  so a completed round trip is always net profitable,
* pace both sides toward a daily share-volume target: quote passively
  (join the bid / ask) when on schedule, and step toward the mid by
  fractions of the NBBO spread when behind.

Everything here is deterministic and free of IB API dependencies.
"""

from __future__ import annotations

import datetime
import math
from dataclasses import dataclass
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

# Fraction of the NBBO spread to step inside, indexed by urgency level.
_URGENCY_SPREAD_FRACTION = (0.0, 0.25, 0.40)


def session_progress_fraction(
    config: Dict[str, Any],
    now: Optional[datetime.datetime] = None,
) -> float:
    """Fraction of the regular session elapsed, clamped to [0, 1]."""
    tz = ZoneInfo(str(config["market_timezone"]))
    if now is None:
        now = datetime.datetime.now(tz=tz)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=tz)
    else:
        now = now.astimezone(tz)
    open_dt = now.replace(
        hour=int(config["market_open_hour"]),
        minute=int(config["market_open_minute"]),
        second=0,
        microsecond=0,
    )
    close_dt = now.replace(
        hour=int(config["market_close_hour"]), minute=0, second=0, microsecond=0
    )
    total = (close_dt - open_dt).total_seconds()
    if total <= 0:
        return 1.0
    elapsed = (now - open_dt).total_seconds()
    return min(1.0, max(0.0, elapsed / total))


class DailyVolumeTracker:
    """Per-side share volume for the current market-timezone day."""

    def __init__(self, market_timezone: str):
        self._tz = ZoneInfo(market_timezone)
        self._day: Optional[datetime.date] = None
        self._bought = 0
        self._sold = 0

    def _roll(self, now: Optional[datetime.datetime]) -> None:
        if now is None:
            now = datetime.datetime.now(tz=self._tz)
        day = now.astimezone(self._tz).date()
        if day != self._day:
            self._day = day
            self._bought = 0
            self._sold = 0

    def record_fill(
        self, side: str, qty: int, now: Optional[datetime.datetime] = None
    ) -> None:
        """Add a fill to today's totals (side ``BUY`` or ``SELL``)."""
        self._roll(now)
        qty = int(qty)
        if qty <= 0:
            return
        if side.upper() == "BUY":
            self._bought += qty
        else:
            self._sold += qty

    def bought_today(self, now: Optional[datetime.datetime] = None) -> int:
        self._roll(now)
        return self._bought

    def sold_today(self, now: Optional[datetime.datetime] = None) -> int:
        self._roll(now)
        return self._sold


@dataclass(frozen=True)
class QuoteParams:
    """Static strategy parameters (from config)."""

    max_position: int = 300
    lot_size: int = 100
    min_order_size: int = 10
    daily_volume_target: int = 400
    min_profit_per_share: float = 0.03
    commission_per_share: float = 0.005
    tick: float = 0.01

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "QuoteParams":
        return cls(
            max_position=int(config.get("max_position", 300)),
            lot_size=int(config.get("base_qty", 100)),
            min_order_size=int(config.get("min_order_size", 10)),
            daily_volume_target=int(config.get("daily_volume_target", 400)),
            min_profit_per_share=float(
                config.get("min_profit_per_share", 0.03)
            ),
            commission_per_share=float(
                config.get("commission_per_share", 0.005)
            ),
            tick=float(config.get("min_tick", 0.01)),
        )

    @property
    def required_edge(self) -> float:
        """Round-trip commission plus minimum profit, per share."""
        return 2.0 * self.commission_per_share + self.min_profit_per_share


@dataclass(frozen=True)
class QuoteInputs:
    """Point-in-time market and inventory state."""

    bid: float
    ask: float
    position: int
    avg_cost: float
    bought_today: int
    sold_today: int
    session_progress: float


@dataclass(frozen=True)
class QuoteProposal:
    """Desired quotes; price is None when the side is not quoted."""

    buy_qty: int
    buy_px: Optional[float]
    sell_qty: int
    sell_px: Optional[float]


def _quantize_down(px: float, tick: float) -> float:
    return round(math.floor(px / tick + 1e-9) * tick, 10)


def _quantize_up(px: float, tick: float) -> float:
    return round(math.ceil(px / tick - 1e-9) * tick, 10)


def _pacing_urgency(target_so_far: float, done: int, lot: int) -> int:
    """0 = on/ahead of schedule, 1 = slightly behind, 2 = far behind."""
    deficit = target_so_far - done
    if deficit <= 0:
        return 0
    if deficit <= lot:
        return 1
    return 2


def _buy_price(params: QuoteParams, inp: QuoteInputs, urgency: int) -> float:
    spread = inp.ask - inp.bid
    mid = (inp.bid + inp.ask) / 2.0
    px = inp.bid + _URGENCY_SPREAD_FRACTION[urgency] * spread
    # Keep enough room that selling at avg_cost + required_edge stays near
    # the mid; on tight spreads fall back to joining the bid.
    px = min(px, mid - params.required_edge)
    px = max(px, inp.bid)
    return _quantize_down(px, params.tick)


def _sell_price(params: QuoteParams, inp: QuoteInputs, urgency: int) -> float:
    spread = inp.ask - inp.bid
    px = inp.ask - _URGENCY_SPREAD_FRACTION[urgency] * spread
    if inp.avg_cost > 0:
        px = max(px, inp.avg_cost + params.required_edge)
    px = max(px, inp.bid + params.tick)
    return _quantize_up(px, params.tick)


def decide_quotes(params: QuoteParams, inp: QuoteInputs) -> QuoteProposal:
    """Compute desired buy/sell quotes from market, inventory, and pacing."""
    target_so_far = params.daily_volume_target * inp.session_progress

    buy_room = max(0, params.max_position - max(0, inp.position))
    buy_qty = min(params.lot_size, buy_room)
    if buy_qty < params.min_order_size:
        buy_qty = 0

    sell_qty = max(0, inp.position)
    if sell_qty < params.min_order_size:
        sell_qty = 0

    buy_px: Optional[float] = None
    if buy_qty > 0:
        buy_urgency = _pacing_urgency(
            target_so_far, inp.bought_today, params.lot_size
        )
        buy_px = _buy_price(params, inp, buy_urgency)

    sell_px: Optional[float] = None
    if sell_qty > 0:
        sell_urgency = _pacing_urgency(
            target_so_far, inp.sold_today, params.lot_size
        )
        # Full or heavy inventory blocks buying (and thus volume); escalate
        # selling so shares keep cycling.
        if inp.position >= params.max_position:
            sell_urgency = 2
        elif 3 * inp.position >= 2 * params.max_position:
            sell_urgency = max(sell_urgency, 1)
        sell_px = _sell_price(params, inp, sell_urgency)

    return QuoteProposal(
        buy_qty=buy_qty, buy_px=buy_px, sell_qty=sell_qty, sell_px=sell_px
    )
