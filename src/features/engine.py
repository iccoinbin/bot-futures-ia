import asyncio
import asyncpg
import pandas as pd
import numpy as np
import pandas_ta as ta
from datetime import timezone, timedelta
from src.config.settings import S
from src.utils.db import get_pool

def to_frame(rows, cols):
    return pd.DataFrame(rows, columns=cols)

async def load_last_candles(pool, symbol: str, interval: str, lookback: int = 2500):
    q = """
    select
      open_time,
      cast(open   as double precision) as open,
      cast(high   as double precision) as high,
      cast(low    as double precision) as low,
      cast(close  as double precision) as close,
      cast(volume as double precision) as volume,
      cast(n_trades as integer)        as n_trades
    from md_candles
    where symbol=$1 and interval=$2
    order by open_time desc
    limit $3
    """
    async with pool.acquire() as con:
        rows = await con.fetch(q, symbol, interval, lookback)
    df = to_frame(rows, ["open_time","open","high","low","close","volume","n_trades"])
    if df.empty:
        return df
    df = df.sort_values("open_time").set_index("open_time")
    # força tipos
    for c in ["open","high","low","close","volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    df["n_trades"] = pd.to_numeric(df["n_trades"], errors="coerce").astype("int64", errors="ignore")
    return df

async def load_trades(pool, symbol: str, since_ts):
    q = """
    select
      trade_time,
      cast(price as double precision) as price,
      cast(qty   as double precision) as qty,
      is_buyer_maker
    from md_trades
    where symbol=$1 and trade_time >= $2
    order by trade_time asc
    """
    async with pool.acquire() as con:
        rows = await con.fetch(q, symbol, since_ts)
    df = to_frame(rows, ["trade_time","price","qty","is_buyer_maker"])
    if df.empty:
        return df
    df["price"] = pd.to_numeric(df["price"], errors="coerce").astype(float)
    df["qty"]   = pd.to_numeric(df["qty"], errors="coerce").astype(float)
    df["side"]  = np.where(df["is_buyer_maker"], -1, 1)  # -1 = venda agressora
    df["signed_qty"] = df["qty"] * df["side"]
    df = df.set_index("trade_time")
    return df


def compute_features_from_1m(df1m: pd.DataFrame, trades: pd.DataFrame):
    if df1m.empty:
        return pd.DataFrame()
    df1m = df1m.copy()
    # Garantia final de float
    for c in ["open","high","low","close","volume"]:
        df1m[c] = pd.to_numeric(df1m[c], errors="coerce").astype(float)

    out = pd.DataFrame(index=df1m.index)

    # Tendência
    ema20 = df1m["close"].ewm(span=20, adjust=False).mean()
    ema50 = df1m["close"].ewm(span=50, adjust=False).mean()
    out["ema20_slope"] = ema20.pct_change()*10000.0
    out["ema50_slope"] = ema50.pct_change()*10000.0

    # VWAP
    tp = (df1m["high"] + df1m["low"] + df1m["close"]) / 3.0
    vwap = (tp*df1m["volume"]).cumsum() / df1m["volume"].replace(0,np.nan).cumsum()
    out["vwap_slope"] = vwap.pct_change()*10000.0

    # --- Indicadores robustos a dados curtos ---
    # ADX
    adx_df = ta.adx(high=df1m["high"], low=df1m["low"], close=df1m["close"], length=14)
    if adx_df is None or "ADX_14" not in adx_df:
        out["adx14"] = pd.Series(np.nan, index=out.index)
    else:
        out["adx14"] = pd.to_numeric(adx_df["ADX_14"], errors="coerce").astype(float)

    # ATR
    atr_s = ta.atr(high=df1m["high"], low=df1m["low"], close=df1m["close"], length=14)
    if atr_s is None:
        atr_s = pd.Series(np.nan, index=out.index)
    atr_s = pd.to_numeric(atr_s, errors="coerce").astype(float)
    out["atr_pct"] = 100.0 * (atr_s / df1m["close"])

    # Bandas de Bollinger
    bb_df = ta.bbands(close=df1m["close"], length=20, std=2.0)
    if bb_df is None or "BBU_20_2.0" not in bb_df or "BBL_20_2.0" not in bb_df:
        out["bb_width"] = pd.Series(np.nan, index=out.index)
    else:
        bbu = pd.to_numeric(bb_df["BBU_20_2.0"], errors="coerce").astype(float)
        bbl = pd.to_numeric(bb_df["BBL_20_2.0"], errors="coerce").astype(float)
        out["bb_width"] = 100.0 * ((bbu - bbl) / df1m["close"])

    # Fluxo: delta agressor 1m
    if trades is not None and not trades.empty:
        t1m = trades["signed_qty"].resample("1min").sum().rename("delta_aggr_1m")
        out = out.join(t1m, how="left")
    else:
        out["delta_aggr_1m"] = np.nan

    # (Opcional) razão bid/ask – mantém como NaN por enquanto
    out["bid_ask_ratio"] = np.nan

    # Regimes por quantis de ATR%
    if out["atr_pct"].notna().sum() >= 3:
        q_low, q_high = out["atr_pct"].quantile([0.33, 0.66])
    else:
        q_low, q_high = np.nan, np.nan

    def regime(x):
        if pd.isna(x) or pd.isna(q_low) or pd.isna(q_high): return np.nan
        if x <= q_low: return "low"
        if x <= q_high: return "mid"
        return "high"
    out["vol_regime"] = out["atr_pct"].apply(regime)

    # Z-score por regime
    def zscore(s):
        m = s.rolling(200, min_periods=50).mean()
        sd = s.rolling(200, min_periods=50).std()
        return (s - m) / sd
    for col in ["ema20_slope","ema50_slope","vwap_slope","adx14","atr_pct","bb_width","delta_aggr_1m"]:
        out[f"z_{col}"] = out.groupby("vol_regime")[col].transform(zscore)

    return out


async def write_features(pool, symbol: str, interval: str, feat: pd.DataFrame):
    if feat is None or feat.empty:
        return

    # mantém só linhas com algum indicador válido
    keep_cols = ["ema20_slope","ema50_slope","vwap_slope","adx14","atr_pct","bb_width"]
    if feat[keep_cols].dropna(how="all").empty:
        return

    cols = ["ema20_slope","ema50_slope","vwap_slope","adx14","atr_pct","bb_width",
            "delta_aggr_1m","bid_ask_ratio","vol_regime",
            "z_ema20_slope","z_ema50_slope","z_vwap_slope",
            "z_adx14","z_atr_pct","z_bb_width","z_delta_aggr_1m"]
    for c in cols:
        if c not in feat.columns:
            feat[c] = np.nan

    def n2none(x):
        # numérico -> float | None
        if x is None:
            return None
        try:
            xv = float(x)
            if np.isnan(xv) or np.isinf(xv):
                return None
            return xv
        except Exception:
            return None

    rows = []
    for ts, r in feat.iterrows():
        vr = r.get("vol_regime")
        # vol_regime precisa ser string ou None
        if pd.isna(vr):
            vr = None
        else:
            vr = str(vr)

        rows.append((
            symbol, interval, ts.to_pydatetime().replace(tzinfo=timezone.utc),
            n2none(r["ema20_slope"]), n2none(r["ema50_slope"]), n2none(r["vwap_slope"]),
            n2none(r["adx14"]), n2none(r["atr_pct"]), n2none(r["bb_width"]),
            n2none(r["delta_aggr_1m"]), n2none(r["bid_ask_ratio"]),
            vr,
            n2none(r["z_ema20_slope"]), n2none(r["z_ema50_slope"]), n2none(r["z_vwap_slope"]),
            n2none(r["z_adx14"]), n2none(r["z_atr_pct"]), n2none(r["z_bb_width"]), n2none(r["z_delta_aggr_1m"])
        ))

    q = """
    insert into features(symbol,interval,ts,ema20_slope,ema50_slope,vwap_slope,adx14,atr_pct,bb_width,
                         delta_aggr_1m,bid_ask_ratio,vol_regime,z_ema20_slope,z_ema50_slope,z_vwap_slope,
                         z_adx14,z_atr_pct,z_bb_width,z_delta_aggr_1m)
    values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
    on conflict (symbol,interval,ts) do update set
      ema20_slope=excluded.ema20_slope, ema50_slope=excluded.ema50_slope, vwap_slope=excluded.vwap_slope,
      adx14=excluded.adx14, atr_pct=excluded.atr_pct, bb_width=excluded.bb_width,
      delta_aggr_1m=excluded.delta_aggr_1m, bid_ask_ratio=excluded.bid_ask_ratio,
      vol_regime=excluded.vol_regime, z_ema20_slope=excluded.z_ema20_slope,
      z_ema50_slope=excluded.z_ema50_slope, z_vwap_slope=excluded.z_vwap_slope,
      z_adx14=excluded.z_adx14, z_atr_pct=excluded.z_atr_pct, z_bb_width=excluded.z_bb_width,
      z_delta_aggr_1m=excluded.z_delta_aggr_1m;
    """
    async with pool.acquire() as con:
        await con.executemany(q, rows)
async def run_once():
    pool = await get_pool()
    for sym in S.symbols:
        df = await load_last_candles(pool, sym, "1m", 3000)
        if df.empty:
            print(f"[engine] Sem candles para {sym}")
            continue
        since = (df.index[-1] - pd.Timedelta(hours=48)).to_pydatetime()
        tr = await load_trades(pool, sym, since)
        feat = compute_features_from_1m(df, tr)
        await write_features(pool, sym, "1m", feat.last("48H"))
    await pool.close()

if __name__ == "__main__":
    asyncio.run(run_once())
