import os, time, datetime as dt
import pandas as pd
import httpx
from tenacity import retry, stop_after_attempt, wait_fixed
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
PG_DSN        = os.getenv("PG_DSN")
SYMBOLS       = [s.strip() for s in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT").split(",") if s.strip()]
TIMEFRAMES    = [t.strip() for t in os.getenv("TIMEFRAMES","1m,5m,15m,1h").split(",") if t.strip()]
BACK_DAYS     = int(os.getenv("SPOT_BACKFILL_DAYS","7"))

engine = create_engine(PG_DSN, pool_pre_ping=True)

FUT_BASE = "https://fapi.binance.com"
SPOT_BASE= "https://api.binance.com"
PATH_KL  = "/api/v3/klines"   # Spot
TF_MS = {"1m":60_000,"3m":180_000,"5m":300_000,"15m":900_000,"30m":1_800_000,"1h":3_600_000}
LIMIT = 1000

def to_utc(ms): return dt.datetime.utcfromtimestamp(ms/1000.0)

def latest_spot_ts(symbol, tf):
    with engine.begin() as c:
        row = c.execute(text(
            "SELECT ts FROM public.spot_candles WHERE symbol=:s AND timeframe=:tf ORDER BY ts DESC LIMIT 1"
        ), {"s":symbol,"tf":tf}).fetchone()
    return row[0] if row else None

def upsert_spot(df: pd.DataFrame):
    if df.empty: return
    import psycopg2
    from psycopg2.extras import execute_values
    cols = ["ts","symbol","timeframe","open","high","low","close","volume"]
    dsn = os.getenv("PG_DSN").replace("postgresql+psycopg2://","postgresql://")
    conn= psycopg2.connect(dsn); conn.autocommit=True
    with conn, conn.cursor() as cur:
        sql = """INSERT INTO public.spot_candles (ts,symbol,timeframe,open,high,low,close,volume) VALUES %s
                 ON CONFLICT (ts,symbol,timeframe) DO UPDATE SET
                 open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume;"""
        execute_values(cur, sql, df[cols].values.tolist(), page_size=1000)
    conn.close()

def upsert_spread(df: pd.DataFrame):
    if df.empty: return
    import psycopg2
    from psycopg2.extras import execute_values
    cols = ["ts","symbol","timeframe","perp_close","spot_close","spread_pct","spread_bps"]
    dsn = os.getenv("PG_DSN").replace("postgresql+psycopg2://","postgresql://")
    conn= psycopg2.connect(dsn); conn.autocommit=True
    with conn, conn.cursor() as cur:
        sql = """INSERT INTO public.spread_perp_spot (ts,symbol,timeframe,perp_close,spot_close,spread_pct,spread_bps) VALUES %s
                 ON CONFLICT (ts,symbol,timeframe) DO UPDATE SET
                 perp_close=EXCLUDED.perp_close, spot_close=EXCLUDED.spot_close,
                 spread_pct=EXCLUDED.spread_pct, spread_bps=EXCLUDED.spread_bps;"""
        execute_values(cur, sql, df[cols].values.tolist(), page_size=1000)
    conn.close()

@retry(stop=stop_after_attempt(5), wait=wait_fixed(1))
def fetch_spot(symbol, tf, start_ms=None, end_ms=None, limit=LIMIT):
    params={"symbol":symbol, "interval":tf, "limit":limit}
    if start_ms is not None: params["startTime"]=int(start_ms)
    if end_ms   is not None: params["endTime"]=int(end_ms)
    r=httpx.get(SPOT_BASE+PATH_KL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def parse_spot(symbol, tf, raw):
    if not raw: return pd.DataFrame()
    rows=[]
    for k in raw:
        rows.append({
            "ts": to_utc(k[0]),
            "symbol": symbol,
            "timeframe": tf,
            "open": float(k[1]),
            "high": float(k[2]),
            "low":  float(k[3]),
            "close":float(k[4]),
            "volume":float(k[5]),
        })
    return pd.DataFrame(rows)

def backfill_spot(symbol, tf):
    step=TF_MS[tf]
    now_ms=int(time.time()*1000)
    last=latest_spot_ts(symbol, tf)
    if last:
        start_ms=int((last + dt.timedelta(milliseconds=step)).timestamp()*1000)
    else:
        start_ms=now_ms - BACK_DAYS*24*60*60*1000
    while start_ms <= now_ms:
        raw=fetch_spot(symbol, tf, start_ms=start_ms, limit=LIMIT)
        if not raw: break
        df=parse_spot(symbol, tf, raw)
        if df.empty: break
        upsert_spot(df)
        start_ms=int(df["ts"].max().timestamp()*1000) + step

def compute_spread(symbol, tf):
    # junta futures.candles com spot_candles no mesmo ts e tf
    q = text("""
        WITH fut AS (
          SELECT ts, close AS perp_close
          FROM public.candles
          WHERE symbol = :s AND timeframe = :tf
        ),
        spot AS (
          SELECT ts, close AS spot_close
          FROM public.spot_candles
          WHERE symbol = :s AND timeframe = :tf
        ),
        joined AS (
          SELECT f.ts, :s AS symbol, :tf AS timeframe, f.perp_close, s.spot_close
          FROM fut f
          JOIN spot s ON s.ts = f.ts
        )
        SELECT * FROM joined
        ORDER BY ts DESC
        LIMIT 3000;
    """)
    with engine.begin() as c:
        df=pd.read_sql(q, c, params={"s":symbol, "tf":tf})
    if df.empty: return pd.DataFrame()
    df["spread_pct"]=(df["perp_close"] - df["spot_close"]) / df["spot_close"] * 100.0
    df["spread_bps"]=df["spread_pct"] * 100.0
    return df

def run_once():
    for s in SYMBOLS:
        for tf in TIMEFRAMES:
            try:
                backfill_spot(s, tf)
                sp = compute_spread(s, tf)
                upsert_spread(sp)
                print(f"[OK] spread {s} {tf}: {len(sp)} linhas upsert")
            except Exception as e:
                print(f"[ERR] spread {s} {tf}: {e}")

if __name__=="__main__":
    print("[*] Spread perp vs. spot collector iniciado...")
    run_once()
