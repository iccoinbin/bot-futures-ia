import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import ta

load_dotenv()
PG_DSN=os.getenv("PG_DSN")
CANDLES_TABLE=os.getenv("CANDLES_TABLE","candles")
FEATURES_TABLE=os.getenv("FEATURES_TABLE","features")
SYMBOLS=[s.strip() for s in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT").split(",") if s.strip()]
TIMEFRAMES=[t.strip() for t in os.getenv("TIMEFRAMES","1m,5m,15m,1h").split(",") if t.strip()]
LOOKBACK_ROWS=int(os.getenv("LOOKBACK_ROWS","3000"))
engine=create_engine(PG_DSN, pool_pre_ping=True)

SCHEMA=f"""
CREATE TABLE IF NOT EXISTS {FEATURES_TABLE}(
 ts TIMESTAMP NOT NULL,
 symbol TEXT NOT NULL,
 timeframe TEXT NOT NULL,
 ema20_slope DOUBLE PRECISION,
 ema50_slope DOUBLE PRECISION,
 vwap_slope DOUBLE PRECISION,
 adx14 DOUBLE PRECISION,
 atrp14 DOUBLE PRECISION,
 bb_width DOUBLE PRECISION,
 delta_aggr DOUBLE PRECISION,
 bid_ask_ratio DOUBLE PRECISION,
 z_ema20_slope DOUBLE PRECISION,
 z_ema50_slope DOUBLE PRECISION,
 z_vwap_slope DOUBLE PRECISION,
 z_adx14 DOUBLE PRECISION,
 z_atrp14 DOUBLE PRECISION,
 z_bb_width DOUBLE PRECISION,
 -- novos
 oi_5m DOUBLE PRECISION,
 spread_pct DOUBLE PRECISION,
 z_oi_5m DOUBLE PRECISION,
 z_spread_pct DOUBLE PRECISION,
 PRIMARY KEY (ts,symbol,timeframe)
);
"""
with engine.begin() as c: c.execute(text(SCHEMA))

def vwap(df):
    tp=(df["high"]+df["low"]+df["close"])/3.0
    pv=tp*df["volume"].fillna(0)
    return pv.cumsum()/df["volume"].fillna(0).cumsum().replace(0,np.nan)

def slope(s,k=3): return (s - s.shift(k))/float(k)
def zroll(s,win=500):
    m=s.rolling(win, min_periods=max(30,win//10)).mean()
    sd=s.rolling(win, min_periods=max(30,win//10)).std()
    return (s - m)/sd.replace(0,np.nan)

def load_candles(sym,tf,n):
    q=text(f"SELECT ts,open,high,low,close,volume FROM {CANDLES_TABLE} WHERE symbol=:s AND timeframe=:tf ORDER BY ts DESC LIMIT :n")
    with engine.begin() as c:
        df=pd.read_sql(q,c,params={"s":sym,"tf":tf,"n":n})
    if df.empty: return df
    return df.sort_values("ts").reset_index(drop=True)

def load_oi(sym, ts_min, ts_max):
    q=text("""
        SELECT ts, open_interest AS oi_5m
        FROM public.open_interest
        WHERE symbol=:s AND timeframe='5m' AND ts BETWEEN :a AND :b
        ORDER BY ts
    """)
    with engine.begin() as c:
        return pd.read_sql(q,c,params={"s":sym,"a":ts_min,"b":ts_max})

def load_spread(sym, tf, ts_min, ts_max):
    q=text("""
        SELECT ts, spread_pct
        FROM public.spread_perp_spot
        WHERE symbol=:s AND timeframe=:tf AND ts BETWEEN :a AND :b
        ORDER BY ts
    """)
    with engine.begin() as c:
        return pd.read_sql(q,c,params={"s":sym,"tf":tf,"a":ts_min,"b":ts_max})

def upsert(df: pd.DataFrame):
    if df.empty: return
    import psycopg2
    from psycopg2.extras import execute_values
    cols=["ts","symbol","timeframe",
          "ema20_slope","ema50_slope","vwap_slope","adx14",
          "atrp14","bb_width","delta_aggr","bid_ask_ratio",
          "z_ema20_slope","z_ema50_slope","z_vwap_slope",
          "z_adx14","z_atrp14","z_bb_width",
          "oi_5m","spread_pct","z_oi_5m","z_spread_pct"]
    vals=df[cols].where(pd.notnull(df),None).values.tolist()
    dsn=PG_DSN.replace("postgresql+psycopg2://","postgresql://")
    conn=psycopg2.connect(dsn); conn.autocommit=True
    with conn, conn.cursor() as cur:
        sql=f"""INSERT INTO {FEATURES_TABLE} ({",".join(cols)}) VALUES %s
        ON CONFLICT (ts,symbol,timeframe) DO UPDATE SET
         ema20_slope=EXCLUDED.ema20_slope, ema50_slope=EXCLUDED.ema50_slope, vwap_slope=EXCLUDED.vwap_slope, adx14=EXCLUDED.adx14,
         atrp14=EXCLUDED.atrp14, bb_width=EXCLUDED.bb_width, delta_aggr=EXCLUDED.delta_aggr, bid_ask_ratio=EXCLUDED.bid_ask_ratio,
         z_ema20_slope=EXCLUDED.z_ema20_slope, z_ema50_slope=EXCLUDED.z_ema50_slope, z_vwap_slope=EXCLUDED.z_vwap_slope,
         z_adx14=EXCLUDED.z_adx14, z_atrp14=EXCLUDED.z_atrp14, z_bb_width=EXCLUDED.z_bb_width,
         oi_5m=EXCLUDED.oi_5m, spread_pct=EXCLUDED.spread_pct,
         z_oi_5m=EXCLUDED.z_oi_5m, z_spread_pct=EXCLUDED.z_spread_pct;"""
        execute_values(cur, sql, vals, page_size=500)
    conn.close()

def build(df):
    out=df.copy()
    out["ema20"]=out["close"].ewm(span=20,adjust=False).mean()
    out["ema50"]=out["close"].ewm(span=50,adjust=False).mean()
    out["vwap"]=vwap(out)
    out["ema20_slope"]=slope(out["ema20"],3)
    out["ema50_slope"]=slope(out["ema50"],3)
    out["vwap_slope"]=slope(out["vwap"],3)
    try: out["adx14"]=ta.trend.adx(high=out["high"], low=out["low"], close=out["close"], window=14)
    except Exception: out["adx14"]=np.nan
    try:
        atr=ta.volatility.average_true_range(high=out["high"], low=out["low"], close=out["close"], window=14)
        out["atrp14"]=(atr/out["close"])*100.0
    except Exception: out["atrp14"]=np.nan
    try:
        bb_h=ta.volatility.bollinger_hband(out["close"],window=20,window_dev=2)
        bb_l=ta.volatility.bollinger_lband(out["close"],window=20,window_dev=2)
        bb_m=ta.volatility.bollinger_mavg(out["close"],window=20)
        out["bb_width"]=((bb_h-bb_l)/bb_m.replace(0,np.nan))*100.0
    except Exception: out["bb_width"]=np.nan
    # placeholders
    out["delta_aggr"]=np.nan; out["bid_ask_ratio"]=np.nan
    for c in ["ema20_slope","ema50_slope","vwap_slope","adx14","atrp14","bb_width"]:
        out[f"z_{c}"]=zroll(out[c],win=500)
    return out

def order(tf): return {"1m":1,"3m":3,"5m":5,"15m":15,"30m":30,"1h":60}.get(tf,999)

def main():
    for sym in SYMBOLS:
        for tf in sorted(TIMEFRAMES, key=order):
            df=load_candles(sym,tf,LOOKBACK_ROWS)
            if df.empty:
                print(f"[WARN] Sem candles para {sym} {tf}"); continue
            feats=build(df)

            # integra OI (5m) via merge_asof (último <= ts)
            oi = load_oi(sym, feats["ts"].min(), feats["ts"].max())
            if not oi.empty:
                feats = pd.merge_asof(feats.sort_values("ts"), oi.sort_values("ts"),
                                      on="ts", direction="backward")
            else:
                feats["oi_5m"]=np.nan

            # integra spread (match exato ts/tf)
            sp = load_spread(sym, tf, feats["ts"].min(), feats["ts"].max())
            if not sp.empty:
                feats = feats.merge(sp, on="ts", how="left")
            else:
                feats["spread_pct"]=np.nan

            # z-scores dos novos campos
            feats["z_oi_5m"]      = zroll(feats["oi_5m"], win=500)
            feats["z_spread_pct"] = zroll(feats["spread_pct"], win=500)

            final=feats[[
                "ts","ema20_slope","ema50_slope","vwap_slope","adx14","atrp14","bb_width",
                "delta_aggr","bid_ask_ratio",
                "z_ema20_slope","z_ema50_slope","z_vwap_slope","z_adx14","z_atrp14","z_bb_width",
                "oi_5m","spread_pct","z_oi_5m","z_spread_pct"
            ]].copy()
            final["symbol"]=sym; final["timeframe"]=tf
            final=final[["ts","symbol","timeframe",
                "ema20_slope","ema50_slope","vwap_slope","adx14","atrp14","bb_width",
                "delta_aggr","bid_ask_ratio",
                "z_ema20_slope","z_ema50_slope","z_vwap_slope","z_adx14","z_atrp14","z_bb_width",
                "oi_5m","spread_pct","z_oi_5m","z_spread_pct"]]

            upsert(final.tail(LOOKBACK_ROWS))
            print(f"[OK] {sym} {tf}: {len(final)} linhas processadas (com OI+Spread)")

if __name__=="__main__":
    main()
