import pandas as pd

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"].shift(1)
    tr = (high - low).abs()
    tr = tr.to_frame("tr")
    tr["h_pc"] = (df["high"] - close).abs()
    tr["l_pc"] = (df["low"] - close).abs()
    tr_max = tr.max(axis=1)
    return tr_max.rolling(period).mean()
