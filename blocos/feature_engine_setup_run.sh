#!/usr/bin/env bash
set -euo pipefail

cd ~/bot-futures-ia

# venv + deps
python3 -m venv .venv >/dev/null 2>&1 || true
source .venv/bin/activate
pip install -U pip >/dev/null
pip install pandas numpy SQLAlchemy psycopg2-binary python-dotenv ta pandas_ta redis tqdm >/dev/null

# .env (sem sobrescrever existentes)
touch .env
grep -q "^PG_DSN=" .env || echo 'PG_DSN=postgresql+psycopg2://postgres:postgres@localhost:5432/bot_futures' >> .env
grep -q "^REDIS_URL=" .env || echo 'REDIS_URL=redis://localhost:6379/0' >> .env
grep -q "^CANDLES_TABLE=" .env || echo 'CANDLES_TABLE=candles' >> .env
grep -q "^FEATURES_TABLE=" .env || echo 'FEATURES_TABLE=features' >> .env
grep -q "^ORDERFLOW_TABLE=" .env || echo 'ORDERFLOW_TABLE=order_flow' >> .env
grep -q "^SYMBOLS=" .env || echo 'SYMBOLS=BTCUSDT,ETHUSDT' >> .env
grep -q "^TIMEFRAMES=" .env || echo 'TIMEFRAMES=1m,5m,15m,1h' >> .env
grep -q "^LOOKBACK_ROWS=" .env || echo 'LOOKBACK_ROWS=3000' >> .env

mkdir -p src/feature_engine scripts

# Feature Engine v1
cat > src/feature_engine/feature_engine_v1.py <<'PY'
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
ORDERFLOW_TABLE=os.getenv("ORDERFLOW_TABLE","order_flow")
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
 PRIMARY KEY (ts,symbol,timeframe)
);
"""
with engine.begin() as c: c.execute(text(SCHEMA))

def vwap(df):
    tp=(df["high"]+df["low"]+df["close"])/3.0
    pv=tp*df["volume"].fillna(0)
    cv=pv.cumsum(); vv=df["volume"].fillna(0).cumsum().replace(0,np.nan)
    return cv/vv

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

def maybe_of(sym,tf):
    try:
        with engine.begin() as c:
            exists=c.execute(text("SELECT to_regclass(:t)"),{"t":ORDERFLOW_TABLE}).scalar()
            if not exists: return None
            df=pd.read_sql(
                text(f"SELECT ts,delta_aggr,bid_ask_ratio FROM {ORDERFLOW_TABLE} WHERE symbol=:s AND timeframe=:tf ORDER BY ts ASC"),
                c, params={"s":sym,"tf":tf}
            )
            return None if df.empty else df
    except Exception: return None

def upsert(df):
    if df.empty: return
    import psycopg2
    from psycopg2.extras import execute_values
    cols=["ts","symbol","timeframe","ema20_slope","ema50_slope","vwap_slope","adx14","atrp14","bb_width","delta_aggr","bid_ask_ratio","z_ema20_slope","z_ema50_slope","z_vwap_slope","z_adx14","z_atrp14","z_bb_width"]
    vals=df[cols].where(pd.notnull(df),None).values.tolist()
    dsn=PG_DSN.replace("postgresql+psycopg2://","postgresql://")
    conn=psycopg2.connect(dsn); conn.autocommit=True
    with conn, conn.cursor() as cur:
        sql=f"""INSERT INTO {FEATURES_TABLE} ({",".join(cols)}) VALUES %s
        ON CONFLICT (ts,symbol,timeframe) DO UPDATE SET
         ema20_slope=EXCLUDED.ema20_slope, ema50_slope=EXCLUDED.ema50_slope, vwap_slope=EXCLUDED.vwap_slope, adx14=EXCLUDED.adx14,
         atrp14=EXCLUDED.atrp14, bb_width=EXCLUDED.bb_width, delta_aggr=EXCLUDED.delta_aggr, bid_ask_ratio=EXCLUDED.bid_ask_ratio,
         z_ema20_slope=EXCLUDED.z_ema20_slope, z_ema50_slope=EXCLUDED.z_ema50_slope, z_vwap_slope=EXCLUDED.z_vwap_slope,
         z_adx14=EXCLUDED.z_adx14, z_atrp14=EXCLUDED.z_atrp14, z_bb_width=EXCLUDED.z_bb_width;"""
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
            of=maybe_of(sym,tf)
            if of is not None:
                feats=feats.merge(of,on="ts",how="left")
                feats["delta_aggr"]=feats["delta_aggr_x"].combine_first(feats["delta_aggr_y"])
                feats["bid_ask_ratio"]=feats["bid_ask_ratio_x"].combine_first(feats["bid_ask_ratio_y"])
                feats.drop(columns=[c for c in feats.columns if c.endswith("_x") or c.endswith("_y")], inplace=True)
            final=feats[["ts","ema20_slope","ema50_slope","vwap_slope","adx14","atrp14","bb_width","delta_aggr","bid_ask_ratio",
                         "z_ema20_slope","z_ema50_slope","z_vwap_slope","z_adx14","z_atrp14","z_bb_width"]].copy()
            final["symbol"]=sym; final["timeframe"]=tf
            final=final[["ts","symbol","timeframe","ema20_slope","ema50_slope","vwap_slope","adx14","atrp14","bb_width","delta_aggr","bid_ask_ratio",
                         "z_ema20_slope","z_ema50_slope","z_vwap_slope","z_adx14","z_atrp14","z_bb_width"]]
            upsert(final.tail(LOOKBACK_ROWS))
            print(f"[OK] {sym} {tf}: {len(final)} linhas processadas")
if __name__=="__main__": main()
PY

# Runner
cat > scripts/run_feature_engine_v1.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate
export PYTHONUNBUFFERED=1
python src/feature_engine/feature_engine_v1.py
SH
chmod +x scripts/run_feature_engine_v1.sh

echo "[*] Iniciando rodada única da Feature Engine v1..."
./scripts/run_feature_engine_v1.sh
