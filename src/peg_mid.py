from __future__ import annotations

import argparse
from typing import Any, Dict

from ibapi.order import Order

from ibkr_app_support import add_config_argument, add_ib_connection_arguments
from two_sided_mid import TwoSidedMidTrader, add_two_sided_mid_arguments, run_two_sided_mid_main

_PEG_MID_REQUIRED = ["peg_mid_offset_whole", "peg_mid_offset_half"]


class Trader(TwoSidedMidTrader):
    """PEG MID two-sided quoter with whole/half-cent offsets."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize :class:`Trader`."""
        super().__init__(
            config,
            logger_name="peg_mid",
            default_log_file="peg_mid.log",
            ledger_name="peg_mid",
            default_client_id=3,
        )

    def make_quote_order(self, action: str, qty: int, limit_price: float) -> Order:
        """Build a BUY or SELL quote order for this strategy variant."""
        return self.make_peg_mid_order(action, qty, limit_price)

    def midpoint_is_half_penny(self) -> bool:
        """True when NBBO mid falls on a half-cent."""
        if self._bid is None or self._ask is None:
            return False
        midpoint_half_cents = int(round((self._bid + self._ask) * 100))
        return midpoint_half_cents % 2 == 1

    def current_peg_mid_offset(self) -> float:
        """PEG MID offset for whole- vs half-penny mid."""
        if self.midpoint_is_half_penny():
            return float(
                self.config.get(
                    "peg_mid_offset_half",
                    self.config.get("peg_mid_offset_whole", 0.0) + 0.005,
                )
            )
        return float(self.config.get("peg_mid_offset_whole", 0.0))

    def make_peg_mid_order(self, action: str, qty: int, limit_price: float) -> Order:
        """Build a PEG MID order with whole/half offsets."""
        o = Order()
        o.action = action
        o.orderType = "PEG MID"
        o.totalQuantity = qty
        o.lmtPrice = round(limit_price, int(self.config["price_round_digits"]))
        o.midOffsetAtWhole = float(self.config.get("peg_mid_offset_whole", 0.0))
        o.midOffsetAtHalf = float(self.config.get("peg_mid_offset_half", 0.005))
        o.exchange = self.config["exchange"]
        o.tif = self.config["tif"]
        o.notHeld = True
        return o

    def format_place_order_log(
        self, *, side: str, qty: int, limit: float, order: Order, order_id: int
    ) -> str:
        """Human-readable log line for a new order submission."""
        cap_floor = "cap" if side == "BUY" else "floor"
        return (
            f"Placing PEG MID {side} {qty} {cap_floor}={limit} "
            f"whole={order.midOffsetAtWhole} half={order.midOffsetAtHalf}, id={order_id}"
        )


def _configure_parser(parser: argparse.ArgumentParser) -> None:
    """Add strategy-specific CLI flags to the parser."""
    parser.add_argument("--peg_mid_offset_whole", type=float)
    parser.add_argument("--peg_mid_offset_half", type=float)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the argparse parser for this bot script."""
    parser = argparse.ArgumentParser(
        description="Parameterized IBKR ATS Pegged-to-Midpoint two-sided trader"
    )
    add_config_argument(parser, __file__)
    add_ib_connection_arguments(parser, include_client_id=False)
    add_two_sided_mid_arguments(parser)
    _configure_parser(parser)
    return parser


def main() -> None:
    """CLI entry point."""
    run_two_sided_mid_main(
        description="Parameterized IBKR ATS Pegged-to-Midpoint two-sided trader",
        script_file=__file__,
        trader_cls=Trader,
        extra_required=_PEG_MID_REQUIRED,
        configure_parser=_configure_parser,
    )


if __name__ == "__main__":
    main()
