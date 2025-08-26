import os, time, httpx
from datetime import datetime, timezone
from utils.db import tx

BASE = os.getenv("BINANCE_BASE","https://fapi.binance.com")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT").split(",")]
INTERVALS = [i.strip() for i in os.getenv("INTERVALS","1m,5m,15m,1h").split(",")]

def iso_ms_to_ts(ms):
    return datetime.fromtimestamp(ms/1000, tz=timezone.utc)

def upsert(symbol, interval, row):
    ot = iso_ms_to_ts(row[0]); ct = iso_ms_to_ts(row[6])
    open_, high, low, close = map(float, row[1:5])
    vol = float(row[5]); trades = int(row[8])
    tb_base = float(row[9]); tb_quote = float(row[10])
    with tx() as cur:
        cur.execute("""
        insert into candles(symbol, interval, open_time, open, high, low, close, volume, close_time, trades, taker_buy_base, taker_buy_quote)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        on conflict (symbol, interval, open_time) do update set
          open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close,
          volume=excluded.volume, close_time=excluded.close_time, trades=excluded.trades,
          taker_buy_base=excluded.taker_buy_base, taker_buy_quote=excluded.taker_buy_quote;
        """,(symbol, interval, ot, open_, high, low, close, vol, ct, trades, tb_base, tb_quote))

def run_once():
    with httpx.Client(timeout=20) as client:
        for sym in SYMBOLS:
            for itv in INTERVALS:
                url = f"{BASE}/fapi/v1/klines?symbol={sym}&interval={itv}&limit=1000"
                resp = client.get(url); resp.raise_for_status()
                data = resp.json()
                for row in data:
                    upsert(sym, itv, row)

if __name__=="__main__":
    while True:
        try:
            run_once()
        except Exception as e:
            print("candles_poll error:", e)
        time.sleep(20)
