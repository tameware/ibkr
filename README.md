**Scripts to trade automatically using Interactive Brokers' API.**

Requires IB's TWS app and its IBJts library. The scripts are tested with library version 10.44.01. The parameters for each script are read from a json file and can be overriden on the command line.

*peg_best.py* places one order at a time, either buy or sell, using the PEG BEST order type. The order type usually trades at the midpoint between the NBBO bid and ask. If it placed both buy and sell orders it would often trade with itself. This could and perhaps should be avoided by placing limits for buys below the midpoint and sells above the midpoint. The script would trade less often, but be profitable more often.

*midprice.py* places orders with a user-specified delta around the midpoint between the NBBO bid and ask.
