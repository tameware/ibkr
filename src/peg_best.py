from __future__ import annotations

import argparse
from typing import Any, Dict

from ibapi.order import Order, COMPETE_AGAINST_BEST_OFFSET_UP_TO_MID

from ibkr_app_support import (
    add_config_argument,
    add_ib_connection_arguments,
    clamp_buy_to_avoid_self_trade,
    clamp_sell_to_avg_cost_floor,
    clamp_sell_to_avoid_self_trade,
)
from single_side_quoter import (
    SingleSideQuoter,
    add_single_side_arguments,
    run_single_side_main,
)

_PEG_BEST_REQUIRED = [
    "buy_limit_multiplier",
    "sell_limit_multiplier",
    "min_compete_size",
    "mid_offset_whole",
    "mid_offset_half",
    "post_to_ats_seconds",
]


class Trader(SingleSideQuoter):
    """PEG BEST single-side quoter (multiplier limits from ``ref_price``)."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize :class:`Trader`."""
        super().__init__(
            config,
            logger_name="peg_best",
            default_log_file="peg_best.log",
            ledger_name="peg_best",
            default_client_id=4,
        )

    def compute_order_limits(self) -> tuple[float, float]:
        """Return protective ``(buy_limit, sell_limit)`` from ``ref_price``."""
        digits = int(self.config["price_round_digits"])
        buy_limit = round(
            self.ref_price * float(self.config["buy_limit_multiplier"]), digits
        )
        sell_limit = round(
            self.ref_price * float(self.config["sell_limit_multiplier"]), digits
        )
        buy_limit = clamp_buy_to_avoid_self_trade(
            buy_limit, self.config, bid=self._bid, ask=self._ask, mid=self.ref_price
        )
        sell_limit = clamp_sell_to_avoid_self_trade(
            sell_limit, self.config, bid=self._bid, ask=self._ask, mid=self.ref_price
        )
        sell_limit = clamp_sell_to_avg_cost_floor(
            sell_limit, self.avg_cost, self.config
        )
        return buy_limit, sell_limit

    def make_quote_order(self, action: str, qty: int, limit_price: float) -> Order:
        """Build a BUY or SELL quote order for this strategy variant."""
        o = Order()
        o.action = action
        o.orderType = "PEG BEST"
        o.totalQuantity = qty
        o.lmtPrice = round(limit_price, int(self.config["price_round_digits"]))
        o.exchange = self.config["exchange"]
        o.tif = self.config["tif"]
        o.notHeld = True
        o.minCompeteSize = int(self.config["min_compete_size"])
        o.competeAgainstBestOffset = COMPETE_AGAINST_BEST_OFFSET_UP_TO_MID
        o.midOffsetAtWhole = self.config["mid_offset_whole"]
        o.midOffsetAtHalf = self.config["mid_offset_half"]
        o.postToAts = self.config["post_to_ats_seconds"]
        return o


def _configure_parser(parser: argparse.ArgumentParser) -> None:
    """Add strategy-specific CLI flags to the parser."""
    parser.add_argument("--buy_limit_multiplier", type=float)
    parser.add_argument("--sell_limit_multiplier", type=float)
    parser.add_argument("--min_compete_size", type=int)
    parser.add_argument("--mid_offset_whole", type=float)
    parser.add_argument("--mid_offset_half", type=float)
    parser.add_argument("--post_to_ats_seconds", type=int)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the argparse parser for this bot script."""
    parser = argparse.ArgumentParser(
        description="Parameterized IBKR PEG BEST single-side trader"
    )
    add_config_argument(parser, __file__)
    add_ib_connection_arguments(parser, include_client_id=False)
    add_single_side_arguments(parser)
    _configure_parser(parser)
    return parser


def main() -> None:
    """CLI entry point."""
    run_single_side_main(
        description="Parameterized IBKR PEG BEST single-side trader",
        script_file=__file__,
        trader_cls=Trader,
        extra_required=_PEG_BEST_REQUIRED,
        configure_parser=_configure_parser,
    )


if __name__ == "__main__":
    main()
