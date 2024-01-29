import asyncio
from time import time

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
from pandas_datareader import data as pdr

import yfinance as yf
yf.pdr_override()

from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket

class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    """Session class with caching and rate-limiting behavior. Accepts arguments for both
    LimiterSession and CachedSession.
    """

session = CachedLimiterSession(
  per_second = 10000,
  bucket_class = MemoryQueueBucket,
  backend = SQLiteCache("yfinance.cache")
)

# minimum and maximum stock price to appear in scanner (USD)
MIN_PRICE = 1
MAX_PRICE = 30

# list of symbols to always filter out 
SYMBOL_IGNORE = ['ATRI', 'TKNO', 'ZXYZ.A']

# build list of all stocks into symbol string
url = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"
df = pd.read_csv(url, sep="|")
symbols = df['Symbol'].astype(str).tolist()[:-1]
symbols = [x for x in symbols if x not in SYMBOL_IGNORE]

'''
TODO

While the get_data_yahoo method all-at-once is nice, it might be more
efficient to operate on each call in-place using asyncio IO like before.

I am guessing that it is just chaining together all calls to download...
but maybe we should operate on the data in place instead of getting all 
the data at once and then operating on it.  We need this to run as fast as
it can run, so doing things twice is unnecessary.

Probably going to create multiple scripts fo

Thinking that this code can run as a service or something at some interval.
It can host the tables at different URLs, find some python web framework to 
use for this....

'''


# get all historical one-day stock data into pandas (ignoring last value which is invalid (file creation date))
#data = pdr.get_data_yahoo(group_by="ticker", 
#                          tickers=' '.join(symbols), 
#                          interval="1m", 
#                          period="1d",
#                          session=session)
#latest = data.iloc[-1]


async def buildMovementList(symbol):
  try:
    thestock = latest[symbol]
    
    price = thestock['Adj Close']
    # filter out based on min and max price
    if MIN_PRICE <= price <= MAX_PRICE:
      volume = thestock['Volume']
      low = float(10000)
      high = float(0)
      for day in data[symbol].itertuples(index=True, name='Pandas'):
        if day.Low < low:
          low = day.Low
        if high < day.High:
          high = day.High
      deltapercent = 100 * (high - low)/low
      open = thestock['Open']
      close = thestock['Close']
      if(open == 0):
        deltaprice = 0
      else:
        deltaprice = 100 * (close - open) / open
      pair = [symbol, price, deltapercent, deltaprice, volume]
      movementlist.append(pair)
  except KeyError as e:
    pass


async def main():
  results = asyncio.gather(*(buildMovementList(symbol) for symbol in df['Symbol']))
  return results


if __name__ == '__main__':
    movementlist = []

    results = asyncio.run(main())
    
    #(movementlist)

    # debug info
    print("Matched Symbols = " + str(len(movementlist)))

    for item in movementlist:
      print(item)