import pandas as pd

import os
import sys
import time
import ccxt

exchange = ccxt.binance()
exchange.enableRateLimit = True

fetch_markets = pd.DataFrame(exchange.fetchMarkets())
symbols = fetch_markets.loc[(fetch_markets['swap'] == True) & 
                 (fetch_markets['contract'] == True) & 
                 (fetch_markets['settle'] == 'USDT')]['symbol'].reset_index(drop=True)

markets = pd.DataFrame()
profits = pd.DataFrame()

timeframe = ["1m", 60000]
outdir = "./markets/" + timeframe[0]
if not os.path.exists(outdir):
    os.mkdir(outdir)

for index, symbol in symbols.items():
    
    filename = os.path.join(outdir, symbol.replace("/", "").split(":")[0] + '.csv')
    
    accumulator = []
    csv = None
    t = 0

    if os.path.exists(filename):
        # continue
        csv = pd.read_csv(filename)[:-1]
        t = int(csv["d"].tail(1).iloc[0])
        accumulator.append(csv)
        
    len_ohlcv = 0
    while True:
        
        time.sleep(int(exchange.rateLimit) / 1000)
        
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe[0], t + timeframe[1], 1000)
        
        if len(ohlcv) <= 0:
            break
        else:
            len_ohlcv += len(ohlcv)
        
        t = int(ohlcv[-1][0])
        
        accumulator.append(pd.DataFrame(
            ohlcv,
            columns=['d', 'o', 'h', 'l', 'c', 'v']
        ))
        
        print("👉", f"{index + 1}/{len(symbols)}", str(pd.to_datetime(t, unit='ms')), symbol, f"{len_ohlcv}")
    
    pd.concat(accumulator, ignore_index=True).to_csv(
        filename,
        mode='w',
        index=False,
    )

sys.exit(0)
