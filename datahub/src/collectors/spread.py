import os, time, httpx
from datetime import datetime, timezone
from utils.db import tx

BASE_F = os.getenv("BINANCE_BASE_F", "https://fapi.binance.com")
BASE_S = os.getenv("BINANCE_BASE_S", "https://api.binance.com")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT").split(",")]

def run_once():
    now = datetime.now(timezone.utc)
    with httpx.Client(timeout=15) as c:
        for s in SYMBOLS:
            # Preço perp (futuros)
            r_perp = c.get(f"{BASE_F}/fapi/v1/ticker/price", params={"symbol": s})
            r_perp.raise_for_status()
            perp_price = float(r_perp.json()["price"])
            # Preço spot
            r_spot = c.get(f"{BASE_S}/api/v3/ticker/price", params={"symbol": s})
            r_spot.raise_for_status()
            spot_price = float(r_spot.json()["price"])
            # Spread %
            spread_pct = ((perp_price - spot_price) / spot_price) * 100.0
            with tx() as cur:
                cur.execute("""
                insert into spread(symbol, event_time, perp_price, spot_price, spread_pct)
                values (%s,%s,%s,%s,%s)
                on conflict (symbol, event_time) do nothing;
                """, (s, now, perp_price, spot_price, spread_pct))

if __name__ == "__main__":
    while True:
        try:
            run_once()
        except Exception as e:
            print("spread error:", e)
        time.sleep(30)
