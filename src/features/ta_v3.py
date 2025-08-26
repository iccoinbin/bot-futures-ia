import pandas as pd
import numpy as np

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close_prev = df["close"].astype(float).shift(1)
    tr = pd.concat([
        (high - low).abs(),
        (high - close_prev).abs(),
        (low - close_prev).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def dx_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    # Wilder's DMI/ADX simplificado
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close_prev = df["close"].astype(float).shift(1)

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr1 = pd.concat([
        (high - low).abs(),
        (high - close_prev).abs(),
        (low - close_prev).abs()
    ], axis=1).max(axis=1)

    atr_ = tr1.rolling(period).mean()

    plus_di = 100 * pd.Series(plus_dm).rolling(period).sum() / atr_
    minus_di = 100 * pd.Series(minus_dm).rolling(period).sum() / atr_

    dx = ( (plus_di - minus_di).abs() / (plus_di + minus_di) ) * 100
    adx = dx.rolling(period).mean()
    return adx

def build_features(df: pd.DataFrame, ema_fast: int, ema_slow: int, atr_period: int, adx_period: int) -> pd.DataFrame:
    out = df.copy()
    out["ema_fast"] = ema(out["close"], ema_fast)
    out["ema_slow"] = ema(out["close"], ema_slow)
    out["atr"] = atr(out, atr_period)
    out["atr_pct"] = (out["atr"] / out["close"]).clip(lower=0)
    out["trend"] = (out["ema_fast"] > out["ema_slow"]).astype(int)
    out["adx"] = dx_adx(out, adx_period)
    out.dropna(inplace=True)
    return out
