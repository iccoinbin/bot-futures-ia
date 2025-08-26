import os
import sys
import time
import math
import argparse
import numpy as np
import pandas as pd
from datetime import timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ---- Config ----
FAST_EMA = 20
SLOW_EMA = 50
ADX_LEN  = 14
ATR_LEN  = 14
VOL_WIN  = 20
SLOPE_WIN = 10    # janelas p/ slope (variação normalizada)
VWAP_WIN  = 20    # vwap de janela (rolling), se não existir no candles

ADX_TREND_TH = 20.0
SLOPE_TH     = 0.0  # >0 levemente direcional; você pode elevar p/ 0.05 depois

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "botfutures")
DB_USER = os.getenv("DB_USER", "botfutures_user")
DB_PASS = os.getenv("DB_PASSWORD", "postgres")

CANDLES_TABLE = os.getenv("CANDLES_TABLE", "candles")  # ajuste se sua tabela tiver outro nome

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_pre_ping=True,
)

def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), "-", msg, flush=True)

def fetch_symbols_timeframes():
    q = text(f"""
        SELECT DISTINCT symbol, timeframe
        FROM {CANDLES_TABLE}
        ORDER BY symbol, timeframe
    """)
    return pd.read_sql(q, engine)

def last_feature_ts(symbol, timeframe):
    q = text("""
        SELECT MAX(ts) AS last_ts
          FROM features
         WHERE symbol=:s AND timeframe=:tf
    """)
    df = pd.read_sql(q, engine, params={"s": symbol, "tf": timeframe})
    return pd.to_datetime(df.loc[0, "last_ts"]) if df.shape[0] else None

def fetch_candles(symbol, timeframe, since_ts=None, pad_bars=200):
    # Carregamos com um "padding" para calcular indicadores que dependem de janelas
    where = "WHERE symbol=:s AND timeframe=:tf"
    params = {"s": symbol, "tf": timeframe}
    if since_ts is not None:
        where += " AND ts >= :since"
        params["since"] = pd.Timestamp(since_ts) - pd.Timedelta(minutes=0)  # ts é de fechamento
    q = text(f"""
        SELECT ts, open, high, low, close, volume
        FROM {CANDLES_TABLE}
        {where}
        ORDER BY ts
        """)
    df = pd.read_sql(q, engine, params=params)
    # manter um "padding" adicional para segurança ao calcular janelas
    if pad_bars and len(df) > (pad_bars + SLOW_EMA + VOL_WIN):
        df = df.iloc[-(pad_bars + SLOW_EMA + VOL_WIN + 10):].copy()
    return df

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    # EMAs
    df["ema_fast"] = df["close"].ewm(span=FAST_EMA, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=SLOW_EMA, adjust=False).mean()

    # VWAP de janela (se quiser VWAP cumulativo, trocar por cumulativo)
    pv = df["close"] * df["volume"].replace(0, np.nan)
    df["vwap"] = (pv.rolling(VWAP_WIN).sum() / df["volume"].replace(0, np.nan).rolling(VWAP_WIN).sum()).bfill()

    # ATR/ADX via cálculo manual simples (para evitar dependências extras se 'ta' falhar)
    # True Range
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        (df["high"] - df["low"]).abs(),
        (df["high"] - prev_close).abs(),
        (df["low"]  - prev_close).abs()
    ], axis=1).max(axis=1)
    df["atr"] = tr.rolling(ATR_LEN).mean()

    # DMs e DX/ADX
    up_move   = df["high"].diff()
    down_move = -df["low"].diff()
    plus_dm  = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    atr = df["atr"].replace(0, np.nan)

    plus_di  = 100 * pd.Series(plus_dm).rolling(ADX_LEN).sum() / atr.rolling(ADX_LEN).sum()
    minus_di = 100 * pd.Series(minus_dm).rolling(ADX_LEN).sum() / atr.rolling(ADX_LEN).sum()
    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di)).replace([np.inf, -np.inf], np.nan)
    df["adx"] = dx.rolling(ADX_LEN).mean()

    # Volatilidade: std dos log-retornos
    lr = np.log(df["close"] / df["close"].shift(1))
    df["vol_logret"] = lr.rolling(VOL_WIN).std()

    # Slopes normalizados (variação percentual sobre janela)
    def slope_pct(series, win):
        return (series - series.shift(win)) / (series.shift(win) + 1e-12)

    df["slope_ema_fast"] = slope_pct(df["ema_fast"], SLOPE_WIN)
    df["slope_vwap"]     = slope_pct(df["vwap"], SLOPE_WIN)

    # Regime
    cond_trend = (df["adx"] >= ADX_TREND_TH) & (df["slope_ema_fast"].abs() > SLOPE_TH)
    df["regime"] = np.where(cond_trend, "trend", "range")

    return df

def upsert_features(symbol, timeframe, fdf: pd.DataFrame):
    if fdf.empty:
        return 0
    cols = ["symbol","timeframe","ts","ema_fast","ema_slow","vwap","adx","atr","vol_logret","slope_ema_fast","slope_vwap","regime"]
    fdf = fdf[["ts","ema_fast","ema_slow","vwap","adx","atr","vol_logret","slope_ema_fast","slope_vwap","regime"]].copy()
    fdf.insert(0, "timeframe", timeframe)
    fdf.insert(0, "symbol", symbol)

    # Inserção em lotes
    with engine.begin() as conn:
        # cria tabela se não existir (defensivo)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS features (
              symbol           VARCHAR(20)   NOT NULL,
              timeframe        VARCHAR(10)   NOT NULL,
              ts               TIMESTAMPTZ   NOT NULL,
              ema_fast         DOUBLE PRECISION,
              ema_slow         DOUBLE PRECISION,
              vwap             DOUBLE PRECISION,
              adx              DOUBLE PRECISION,
              atr              DOUBLE PRECISION,
              vol_logret       DOUBLE PRECISION,
              slope_ema_fast   DOUBLE PRECISION,
              slope_vwap       DOUBLE PRECISION,
              regime           VARCHAR(10),
              created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
              updated_at       TIMESTAMPTZ
            );
        """))
        # upsert
        insert_sql = f"""
            INSERT INTO features ({",".join(cols)})
            VALUES ({",".join([f":{c}" for c in cols])})
            ON CONFLICT (symbol, timeframe, ts) DO UPDATE SET
              ema_fast=EXCLUDED.ema_fast,
              ema_slow=EXCLUDED.ema_slow,
              vwap=EXCLUDED.vwap,
              adx=EXCLUDED.adx,
              atr=EXCLUDED.atr,
              vol_logret=EXCLUDED.vol_logret,
              slope_ema_fast=EXCLUDED.slope_ema_fast,
              slope_vwap=EXCLUDED.slope_vwap,
              regime=EXCLUDED.regime,
              updated_at=NOW()
        """
        count = 0
        batch = []
        BATCH_SIZE = 1000
        for _, row in fdf.iterrows():
            params = {c: row[c] if c in fdf.columns else None for c in cols}
            batch.append(params)
            if len(batch) >= BATCH_SIZE:
                conn.execute(text(insert_sql), batch)
                count += len(batch)
                batch = []
        if batch:
            conn.execute(text(insert_sql), batch)
            count += len(batch)
        return count

def process_once(lookback_bars=3000):
    pairs = fetch_symbols_timeframes()
    if pairs.empty:
        log("Nenhum candle encontrado na tabela. Finalizando.")
        return

    for _, r in pairs.iterrows():
        s, tf = r["symbol"], r["timeframe"]
        try:
            last_ts = last_feature_ts(s, tf)
            df = fetch_candles(s, tf, since_ts=last_ts, pad_bars=lookback_bars)
            if df.empty:
                log(f"[{s} {tf}] nenhum candle novo.")
                continue
            df = add_indicators(df)
            # Mantém apenas linhas com indicadores válidos
            fdf = df.dropna(subset=["ema_fast","ema_slow","vwap","adx","atr","vol_logret","slope_ema_fast","slope_vwap","regime"]).copy()

            # Se já existia last_ts, pega somente > last_ts
            if last_ts is not None:
                fdf = fdf[fdf["ts"] > last_ts]

            inserted = upsert_features(s, tf, fdf)
            log(f"[{s} {tf}] upsert de {inserted} linhas.")
        except Exception as e:
            log(f"[{s} {tf}] ERRO: {e}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="Roda apenas uma vez e sai")
    ap.add_argument("--lookback", type=int, default=3000, help="Barras para recomputo (padding)")
    args = ap.parse_args()

    if args.once:
        process_once(lookback_bars=args.lookback)
        return

    # Loop contínuo simples
    while True:
        process_once(lookback_bars=args.lookback)
        time.sleep(30)

if __name__ == "__main__":
    main()
