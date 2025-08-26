import os, time, math, datetime as dt
import pandas as pd
import httpx
from tenacity import retry, stop_after_attempt, wait_fixed
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
PG_DSN   = os.getenv("PG_DSN")
SYMBOLS  = [s.strip() for s in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT").split(",") if s.strip()]
BACK_D   = int(os.getenv("FUNDING_BACKFILL_DAYS","30"))

engine = create_engine(PG_DSN, pool_pre_ping=True)
BASE = "https://fapi.binance.com"
PATH = "/fapi/v1/fundingRate"
LIMIT = 1000

def to_utc(ms): return dt.datetime.utcfromtimestamp(ms/1000.0)

def latest_ts(symbol):
    with engine.begin() as c:
        row = c.execute(text(
            "SELECT ts FROM public.funding_rates WHERE symbol=:s ORDER BY ts DESC LIMIT 1"
        ), {"s":symbol}).fetchone()
    return row[0] if row else None

def upsert(df: pd.DataFrame):
    if df.empty: return
    import psycopg2
    from psycopg2.extras import execute_values
    dsn = PG_DSN.replace("postgresql+psycopg2://","postgresql://")
    cols = ["ts","symbol","funding_rate"]
    vals = df[cols].values.tolist()
    conn = psycopg2.connect(dsn); conn.autocommit=True
    with conn, conn.cursor() as cur:
        sql = """INSERT INTO public.funding_rates (ts,symbol,funding_rate) VALUES %s
                 ON CONFLICT (ts,symbol) DO UPDATE SET funding_rate=EXCLUDED.funding_rate;"""
        execute_values(cur, sql, vals, page_size=1000)
    conn.close()

@retry(stop=stop_after_attempt(5), wait=wait_fixed(1))
def fetch(symbol, start_ms=None, end_ms=None, limit=LIMIT):
    params = {"symbol":symbol, "limit":limit}
    if start_ms is not None: params["startTime"] = int(start_ms)
    if end_ms   is not None: params["endTime"]   = int(end_ms)
    r = httpx.get(BASE+PATH, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def parse(symbol, raw):
    if not raw: return pd.DataFrame()
    rows = []
    for k in raw:
        # keys: symbol, fundingRate, fundingTime, ...
        ts = to_utc(k["fundingTime"])
        fr = float(k["fundingRate"])
        rows.append({"ts": ts, "symbol": symbol, "funding_rate": fr})
    return pd.DataFrame(rows)

def backfill_symbol(symbol):
    now_ms = int(time.time()*1000)
    # funding a cada 8h -> janela grande ok
    step_ms = 15 * 24 * 60 * 60 * 1000  # 15 dias por página para segurança
    last = latest_ts(symbol)
    if last:
        start_ms = int((last + dt.timedelta(hours=8)).timestamp()*1000)
    else:
        start_ms = now_ms - BACK_D*24*60*60*1000

    while start_ms <= now_ms:
        end_ms = min(start_ms + step_ms, now_ms)
        data = fetch(symbol, start_ms=start_ms, end_ms=end_ms)
        df = parse(symbol, data)
        if df.empty:
            start_ms = end_ms + 1
            continue
        upsert(df)
        start_ms = int(df["ts"].max().timestamp()*1000) + 1

def run_once():
    for s in SYMBOLS:
        try:
            backfill_symbol(s)
            print(f"[OK] funding {s} up-to-date")
        except Exception as e:
            print(f"[ERR] funding {s}: {e}")

if __name__ == "__main__":
    print("[*] Funding rate collector iniciado...")
    run_once()
