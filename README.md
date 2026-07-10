**Scripts to trade automatically using Interactive Brokers' API.**

Requires IB's TWS app and its IBJts library. The scripts are tested with library version 10.44.01. Bot code lives under `src/`; JSON configs under `config/`. Run from the repo root (e.g. `python src/peg_primary.py`). Parameters can be overridden on the command line.

*peg_best.py* places one order at a time, either buy or sell, using the PEG BEST order type. The order type usually trades at the midpoint between the NBBO bid and ask. If it placed both buy and sell orders it would often trade with itself. This could and perhaps should be avoided by placing limits for buys below the midpoint and sells above the midpoint. The script would trade less often, but be profitable more often.

*midprice.py* places orders with a user-specified delta around the midpoint between the NBBO bid and ask.

*peg_mid.py* also places orders with a user-specified delta around the midpoint between the NBBO bid and ask, with a different IB order type.

*market_maker.py* makes a market in a security. It tries to get just inside the NBBO spread for both buys and sells, narrowing the spread while another order chases it, without chasing itself.

To run the tests:

`python -m unittest discover -s tests -t .`
