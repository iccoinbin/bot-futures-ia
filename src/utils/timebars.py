import pandas as pd

# Downsample 1m -> multi-interval (5m/15m/1h)
def resample_ohlcv(df_1m: pd.DataFrame, rule: str) -> pd.DataFrame:
    o = df_1m['open'].resample(rule).first()
    h = df_1m['high'].resample(rule).max()
    l = df_1m['low'].resample(rule).min()
    c = df_1m['close'].resample(rule).last()
    v = df_1m['volume'].resample(rule).sum()
    n = df_1m['n_trades'].resample(rule).sum()
    out = pd.DataFrame({'open':o,'high':h,'low':l,'close':c,'volume':v,'n_trades':n}).dropna()
    return out
