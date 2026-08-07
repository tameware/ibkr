**Scripts to trade automatically using Interactive Brokers' API.**

Requires IB's TWS app and its IBJts library. The scripts are tested with library version 10.44.01. Bot code lives under `src/`; JSON configs under `config/`. Run from the repo root (e.g. `python src/peg_primary.py`). Parameters can be overridden on the command line.

*peg_best.py* places one order at a time, either buy or sell, using the PEG BEST order type. The order type usually trades at the midpoint between the NBBO bid and ask. If it placed both buy and sell orders it would often trade with itself. This could and perhaps should be avoided by placing limits for buys below the midpoint and sells above the midpoint. The script would trade less often, but be profitable more often.

*midprice.py* places orders with a user-specified delta around the midpoint between the NBBO bid and ask.

*peg_mid.py* also places orders with a user-specified delta around the midpoint between the NBBO bid and ask, with a different IB order type.

*market_maker.py* makes a market in a security. Quote decisions live in `quote_engine.py` and enforce three rules: never go short (sells never exceed the current position), never hold more than `max_position` shares (default 300), and never sell below average cost plus round-trip commission plus `min_profit_per_share`, so every completed round trip is profitable. To hit `daily_volume_target` shares per side per day, each side is paced against the session clock: on schedule it joins the NBBO bid/ask; behind schedule it steps toward the mid by fractions of the spread (buys stay below mid minus the required edge, sells never break the profit floor). Inventory near the cap escalates sell aggressiveness so shares keep cycling. Unsold inventory is held (up to the cap) rather than dumped at a loss into the close.

To run the tests:

`python -m unittest discover -s tests -t .`
