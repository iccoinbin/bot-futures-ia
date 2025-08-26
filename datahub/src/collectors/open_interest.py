import os, time, httpx
from datetime import datetime, timezone
from utils.db import tx

BASE = os.getenv("BINANCE_BASE","https://fapi.binance.com")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT").split(",")]

def run_once():
    now = datetime.now(timezone.utc)
    with httpx.Client(timeout=15) as c:
        for s in SYMBOLS:
            r = c.get(f"{BASE}/fapi/v1/openInterest", params={"symbol": s})
            r.raise_for_status()
            oi = float(r.json()["openInterest"])
            with tx() as cur:
                cur.execute("""
                insert into open_interest(symbol, event_time, open_interest)
                values (%s,%s,%s)
                on conflict (symbol, event_time) do nothing;
                """, (s, now, oi))

if __name__ == "__main__":
    while True:
        try:
            run_once()
        except Exception as e:
            print("open_interest error:", e)
        time.sleep(30)
