import os, time, math, datetime as dt
import pandas as pd
import httpx
from tenacity import retry, stop_after_attempt, wait_fixed
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
PG_DSN        = os.getenv("PG_DSN")
CANDLES_TABLE = os.getenv("CANDLES_TABLE","candles")
SYMBOLS       = [s.strip() for s in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT").split(",") if s.strip()]
TIMEFRAMES    = [t.strip() for t in os.getenv("TIMEFRAMES","1m,5m,15m,1h").split(",") if t.strip()]
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS","7"))
SLEEP_SEC     = int(os.getenv("POLL_SLEEP_SEC","5"))

engine = create_engine(PG_DSN, pool_pre_ping=True)
BASE = "https://fapi.binance.com"  # Futures USD-M
PATH = "/fapi/v1/klines"

TF_MS = {"1m":60_000,"3m":180_000,"5m":300_000,"15m":900_000,"30m":1_800_000,"1h":3_600_000}
LIMIT = 1000

def to_utc(ms): return dt.datetime.utcfromtimestamp(ms/1000.0)

def latest_ts(symbol, tf):
    q = text(f"SELECT ts FROM {CANDLES_TABLE} WHERE symbol=:s AND timeframe=:tf ORDER BY ts DESC LIMIT 1")
    with engine.begin() as c:
        row = c.execute(q, {"s":symbol, "tf":tf}).fetchone()
    return row[0] if row else None

def upsert(df: pd.DataFrame):
    if df.empty: return
    import psycopg2
    from psycopg2.extras import execute_values
    cols = ["ts","symbol","timeframe","open","high","low","close","volume"]
    vals = df[cols].values.tolist()
    dsn = PG_DSN.replace("postgresql+psycopg2://","postgresql://")
    conn = psycopg2.connect(dsn); conn.autocommit=True
    with conn, conn.cursor() as cur:
        sql = f"""INSERT INTO {CANDLES_TABLE} ({",".join(cols)}) VALUES %s
                  ON CONFLICT (ts,symbol,timeframe) DO UPDATE SET
                  open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                  close=EXCLUDED.close, volume=EXCLUDED.volume;"""
        execute_values(cur, sql, vals, page_size=1000)
    conn.close()

@retry(stop=stop_after_attempt(5), wait=wait_fixed(1))
def fetch(symbol, tf, start_ms=None, end_ms=None, limit=LIMIT):
    params = {"symbol":symbol, "interval":tf, "limit":limit}
    if start_ms is not None: params["startTime"] = int(start_ms)
    if end_ms   is not None: params["endTime"]   = int(end_ms)
    r = httpx.get(BASE+PATH, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def parse(symbol, tf, raw):
    if not raw: return pd.DataFrame()
    rows = []
    for k in raw:
        # kline array: [openTime, open, high, low, close, volume, closeTime, ...]
        ts = to_utc(k[0])
        rows.append({
            "ts": ts,
            "symbol": symbol,
            "timeframe": tf,
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
        })
    return pd.DataFrame(rows)

def backfill_symbol_tf(symbol, tf):
    step = TF_MS[tf]
    now_ms = int(time.time()*1000)
    last = latest_ts(symbol, tf)
    if last:
        start_ms = int((last + dt.timedelta(milliseconds=step)).timestamp()*1000)
    else:
        start_ms = now_ms - BACKFILL_DAYS*24*60*60*1000  # dias para trás

    while start_ms <= now_ms:
        raw = fetch(symbol, tf, start_ms=start_ms, limit=LIMIT)
        if not raw: break
        df = parse(symbol, tf, raw)
        if df.empty: break
        upsert(df)
        start_ms = int(df["ts"].max().timestamp()*1000) + step

def run_once():
    # backfill/atualiza cada par e timeframe
    for s in SYMBOLS:
        for tf in TIMEFRAMES:
            try:
                backfill_symbol_tf(s, tf)
                print(f"[OK] {s} {tf} up-to-date")
            except Exception as e:
                print(f"[ERR] {s} {tf}: {e}")

def main():
    # 1) Backfill inicial
    run_once()
    # 2) Loop contínuo simples
    while True:
        time.sleep(SLEEP_SEC)
        run_once()

if __name__ == "__main__":
    print("[*] Candles collector (Futures USD-M) iniciado...")
    main()
