import pandas as pd
from .indicators import ema, atr

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema_fast"] = ema(df["close"], 21)
    df["ema_slow"] = ema(df["close"], 55)
    df["atr"] = atr(df, 14)
    df["trend"] = (df["ema_fast"] > df["ema_slow"]).astype(int)
    df.dropna(inplace=True)
    return df
