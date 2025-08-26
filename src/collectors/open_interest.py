import os, time, datetime as dt
import pandas as pd
import httpx
from tenacity import retry, stop_after_attempt, wait_fixed
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
PG_DSN   = os.getenv("PG_DSN")
SYMBOLS  = [s.strip() for s in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT").split(",") if s.strip()]
PERIODS  = [p.strip() for p in os.getenv("OI_PERIODS","5m,15m,1h").split(",") if p.strip()]
BACK_D   = int(os.getenv("OI_BACKFILL_DAYS","30"))

engine = create_engine(PG_DSN, pool_pre_ping=True)

BASE = "https://fapi.binance.com"
PATH = "/futures/data/openInterestHist"  # ?symbol=BTCUSDT&period=5m&startTime=&endTime=&limit=

LIMIT = 500  # máx por chamada nesta rota

def to_utc(ms): return dt.datetime.utcfromtimestamp(ms/1000.0)

def latest_ts(symbol, tf):
    with engine.begin() as c:
        row = c.execute(text(
            "SELECT ts FROM public.open_interest WHERE symbol=:s AND timeframe=:tf ORDER BY ts DESC LIMIT 1"
        ), {"s":symbol, "tf":tf}).fetchone()
    return row[0] if row else None

def upsert(df: pd.DataFrame):
    if df.empty: return
    import psycopg2
    from psycopg2.extras import execute_values
    dsn = os.getenv("PG_DSN").replace("postgresql+psycopg2://","postgresql://")
    cols = ["ts","symbol","timeframe","open_interest"]
    vals = df[cols].values.tolist()
    conn = psycopg2.connect(dsn); conn.autocommit=True
    with conn, conn.cursor() as cur:
        sql = """INSERT INTO public.open_interest (ts,symbol,timeframe,open_interest) VALUES %s
                 ON CONFLICT (ts,symbol,timeframe) DO UPDATE SET open_interest=EXCLUDED.open_interest;"""
        execute_values(cur, sql, vals, page_size=1000)
    conn.close()

@retry(stop=stop_after_attempt(5), wait=wait_fixed(1))
def fetch(symbol, period, start_ms=None, end_ms=None, limit=LIMIT):
    params = {"symbol":symbol, "period":period, "limit":limit}
    if start_ms is not None: params["startTime"] = int(start_ms)
    if end_ms   is not None: params["endTime"]   = int(end_ms)
    r = httpx.get(BASE+PATH, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def parse(symbol, tf, raw):
    if not raw: return pd.DataFrame()
    rows = []
    for k in raw:
        # keys típicas: "symbol","sumOpenInterest","sumOpenInterestValue","timestamp"
        ts = to_utc(k["timestamp"])
        oi = float(k.get("sumOpenInterest", k.get("openInterest", 0.0)))
        rows.append({"ts": ts, "symbol": symbol, "timeframe": tf, "open_interest": oi})
    return pd.DataFrame(rows)

def backfill_symbol_tf(symbol, tf):
    now_ms = int(time.time()*1000)
    # janela por página ~ 7 dias para segurança (limite 500)
    step_ms = 7 * 24 * 60 * 60 * 1000
    last = latest_ts(symbol, tf)
    if last:
        # próximo ponto depois do último salvo
        start_ms = int((last + dt.timedelta(minutes=1)).timestamp()*1000)
    else:
        start_ms = now_ms - BACK_D * 24 * 60 * 60 * 1000

    while start_ms <= now_ms:
        end_ms = min(start_ms + step_ms, now_ms)
        raw = fetch(symbol, tf, start_ms=start_ms, end_ms=end_ms)
        df = parse(symbol, tf, raw)
        if df.empty:
            start_ms = end_ms + 1
            continue
        upsert(df)
        start_ms = int(df["ts"].max().timestamp()*1000) + 1

def run_once():
    for s in SYMBOLS:
        for tf in PERIODS:
            try:
                backfill_symbol_tf(s, tf)
                print(f"[OK] OI {s} {tf} up-to-date")
            except Exception as e:
                print(f"[ERR] OI {s} {tf}: {e}")

if __name__ == "__main__":
    print("[*] Open Interest collector iniciado...")
    run_once()
