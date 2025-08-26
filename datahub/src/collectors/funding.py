import os, time, httpx
from datetime import datetime, timezone
from utils.db import tx

BASE = os.getenv("BINANCE_BASE","https://fapi.binance.com")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT").split(",")]

def ts(ms): return datetime.utcfromtimestamp(ms/1000).replace(tzinfo=timezone.utc)

def upsert_funding(sym, ft, rate):
    with tx() as cur:
        cur.execute("""
        insert into funding_rates(symbol, funding_time, funding_rate)
        values (%s,%s,%s)
        on conflict (symbol, funding_time) do update set funding_rate=excluded.funding_rate;
        """,(sym, ft, rate))

def upsert_pred(sym, et, rate):
    with tx() as cur:
        cur.execute("""
        insert into funding_predictions(symbol, event_time, predicted_rate)
        values (%s,%s,%s)
        on conflict (symbol, event_time) do update set predicted_rate=excluded.predicted_rate;
        """,(sym, et, rate))

def run_once():
    with httpx.Client(timeout=15) as c:
        for s in SYMBOLS:
            r = c.get(f"{BASE}/fapi/v1/fundingRate", params={"symbol":s,"limit":100})
            r.raise_for_status()
            for item in r.json():
                upsert_funding(s, ts(int(item["fundingTime"])), float(item["fundingRate"]))
            r = c.get(f"{BASE}/fapi/v1/premiumIndex", params={"symbol":s})
            r.raise_for_status()
            j = r.json()
            fr_pred = float(j.get("lastFundingRate", 0.0))
            upsert_pred(s, datetime.now(timezone.utc), fr_pred)

if __name__=="__main__":
    while True:
        try:
            run_once()
        except Exception as e:
            print("funding error:", e)
        time.sleep(60)
