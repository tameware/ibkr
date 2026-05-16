from __future__ import annotations

import argparse
from typing import Any, Dict

from ibapi.order import Order

from ibkr_app_support import add_config_argument, add_ib_connection_arguments
from two_sided_mid import TwoSidedMidTrader, add_two_sided_mid_arguments, run_two_sided_mid_main


class Trader(TwoSidedMidTrader):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(
            config,
            logger_name="midprice",
            default_log_file="midprice.log",
            ledger_name="midprice",
            default_client_id=5,
        )

    def make_quote_order(self, action: str, qty: int, limit_price: float) -> Order:
        return self.make_midprice_order(action, qty, limit_price)

    def make_midprice_order(self, action: str, qty: int, limit_price: float) -> Order:
        o = Order()
        o.action = action
        o.orderType = "MIDPRICE"
        o.totalQuantity = qty
        o.lmtPrice = round(limit_price, int(self.config["price_round_digits"]))
        o.exchange = self.config["exchange"]
        o.tif = self.config["tif"]
        return o


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parameterized IBKR MidPrice two-sided trader"
    )
    add_config_argument(parser, __file__)
    add_ib_connection_arguments(parser, include_client_id=False)
    add_two_sided_mid_arguments(parser)
    return parser


def main() -> None:
    run_two_sided_mid_main(
        description="Parameterized IBKR MidPrice two-sided trader",
        script_file=__file__,
        trader_cls=Trader,
    )


if __name__ == "__main__":
    main()
