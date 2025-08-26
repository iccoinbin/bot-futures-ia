import os, numpy as np, pandas as pd
from utils.db import tx
from ta.trend import ADXIndicator
from ta.volatility import AverageTrueRange, BollingerBands
import logging, sys
logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)])
logging.info('🚀 Iniciando Feature Engine v1')

SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS","BTCUSDT,ETHUSDT").split(",")]
INTERVALS = [i.strip() for i in os.getenv("INTERVALS","1m,5m,15m,1h").split(",")]

def load_candles(symbol, interval, lookback=1500):
    with tx() as cur:
        cur.execute("""
            select open_time, open, high, low, close, volume
            from candles
            where symbol=%s and interval=%s
            order by open_time desc limit %s
        """,(symbol, interval, lookback))
        rows = cur.fetchall()
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["open_time","open","high","low","close","volume"]).sort_values("open_time").reset_index(drop=True)
    # garantir float
    for col in ["open","high","low","close","volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    df = df.dropna(subset=["high","low","close"]).reset_index(drop=True)
    return df

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 50: return pd.DataFrame()

    # Tendência
    df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["ema_slope_20"] = df["ema20"].diff()
    pv = (df["close"]*df["volume"]).cumsum()
    vv = df["volume"].cumsum().replace(0,np.nan)
    df["vwap"] = pv / vv
    df["vwap_slope"] = df["vwap"].diff()

    # ADX 14
    adx = ADXIndicator(high=df["high"], low=df["low"], close=df["close"], window=14)
    df["adx_14"] = adx.adx()

    # Volatilidade
    atr = AverageTrueRange(high=df["high"], low=df["low"], close=df["close"], window=14).average_true_range()
    df["atrp_14"] = atr / df["close"]
    bb = BollingerBands(close=df["close"], window=20, window_dev=2)
    mavg = df["close"].rolling(20).mean()
    df["bb_width_20"] = (bb.bollinger_hband() - bb.bollinger_lband()) / mavg

    # Regime por quantis
    valid_atrp = df["atrp_14"].dropna()
    if valid_atrp.empty: return pd.DataFrame()
    q_low, q_high = valid_atrp.quantile([0.33, 0.66])
    def regime(x):
        if pd.isna(x): return np.nan
        if x <= q_low: return "low"
        if x <= q_high: return "mid"
        return "high"
    df["vol_regime"] = df["atrp_14"].apply(regime)

    # Placeholders de fluxo
    if "delta_aggressor_5m" not in df.columns: df["delta_aggressor_5m"] = np.nan
    if "bid_ask_ratio_5m" not in df.columns: df["bid_ask_ratio_5m"] = np.nan

    # Z-scores por regime
    zcols = ["ema_slope_20","vwap_slope","adx_14","atrp_14","bb_width_20","delta_aggressor_5m","bid_ask_ratio_5m"]
    for reg in ["low","mid","high"]:
        m = df["vol_regime"] == reg
        for col in zcols:
            mu = df.loc[m, col].mean()
            sd = df.loc[m, col].std()
            denom = sd if (pd.notna(sd) and sd and sd>0) else 1.0
            outcol = f"z_{'bb_width_20' if col=='bb_width_20' else col}"
            df.loc[m, outcol] = (df.loc[m, col] - mu) / denom

    # aliases para nomes esperados no upsert
    df["z_delta_aggr_5m"] = df["z_delta_aggressor_5m"] if "z_delta_aggressor_5m" in df else np.nan
    df["z_bidask_5m"] = df["z_bid_ask_ratio_5m"] if "z_bid_ask_ratio_5m" in df else np.nan

    return df

def upsert_features(symbol: str, interval: str, df: pd.DataFrame):
    cols = ["open_time","ema_slope_20","vwap_slope","adx_14","atrp_14","bb_width_20",
            "delta_aggressor_5m","bid_ask_ratio_5m","vol_regime",
            "z_ema_slope_20","z_vwap_slope","z_adx_14","z_atrp_14","z_bb_width_20","z_delta_aggr_5m","z_bidask_5m"]
    out = df[cols].dropna(subset=["ema_slope_20","vwap_slope","adx_14","atrp_14","bb_width_20"]).tail(800)
    if out.empty: return
    with tx() as cur:
        for _, r in out.iterrows():
            cur.execute("""
            insert into features (symbol, interval, open_time,
              ema_slope_20, vwap_slope, adx_14, atrp_14, bb_width_20,
              delta_aggressor_5m, bid_ask_ratio_5m, vol_regime,
              z_ema_slope_20, z_vwap_slope, z_adx_14, z_atrp_14, z_bb_width_20, z_delta_aggr_5m, z_bidask_5m)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            on conflict (symbol, interval, open_time) do update set
              ema_slope_20=excluded.ema_slope_20,
              vwap_slope=excluded.vwap_slope,
              adx_14=excluded.adx_14,
              atrp_14=excluded.atrp_14,
              bb_width_20=excluded.bb_width_20,
              delta_aggressor_5m=excluded.delta_aggressor_5m,
              bid_ask_ratio_5m=excluded.bid_ask_ratio_5m,
              vol_regime=excluded.vol_regime,
              z_ema_slope_20=excluded.z_ema_slope_20,
              z_vwap_slope=excluded.z_vwap_slope,
              z_adx_14=excluded.z_adx_14,
              z_atrp_14=excluded.z_atrp_14,
              z_bb_width_20=excluded.z_bb_width_20,
              z_delta_aggr_5m=excluded.z_delta_aggr_5m,
              z_bidask_5m=excluded.z_bidask_5m;
            """,(symbol, interval, r["open_time"],
                 r["ema_slope_20"], r["vwap_slope"], r["adx_14"], r["atrp_14"], r["bb_width_20"],
                 r["delta_aggressor_5m"], r["bid_ask_ratio_5m"], r["vol_regime"],
                 r["z_ema_slope_20"], r["z_vwap_slope"], r["z_adx_14"], r["z_atrp_14"],
                 r["z_bb_width_20"], r["z_delta_aggr_5m"], r["z_bidask_5m"]))
def run_once():
    for s in SYMBOLS:
        for itv in INTERVALS:
            df = load_candles(s, itv)
            if df.empty: continue
            fdf = compute_features(df)
            if fdf.empty: continue
            upsert_features(s, itv, fdf)

if __name__ == "__main__":
    run_once()
    logging.info('✅ Feature Engine v1 finalizada com sucesso')
