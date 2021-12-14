#!/bin/sh

trap "" PIPE

$HOME/bin/book_extractor --pid
"ETH-USD:BTC-USD:MKR-USD:BCH-USD:COMP-USD:AAVE-USD:UNI-USD:CRV-USD:DOGE-USD:SHIB-USD:ZEC-USD:LTC-USD:ADA-USD:XLM-USD:BAL-USD:ALGO-USD" --level 2 --interval 60000000 --total 89280 --prefix $HOME/data/ob_62d_ --logfile $HOME/data/extraction2.log
