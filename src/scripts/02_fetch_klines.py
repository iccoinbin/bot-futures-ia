import pandas as pd
from datetime import datetime
from src.exchange.binance_client import client
from src.core.logger import logger

SYMBOL = "BTCUSDT"
INTERVAL = "1m"
LIMIT = 1000

if __name__ == "__main__":
    k = client.klines(symbol=SYMBOL, interval=INTERVAL, limit=LIMIT)
    cols = ["open_time","open","high","low","close","volume","close_time","qav","num_trades","taker_base","taker_quote","ignore"]
    df = pd.DataFrame(k, columns=cols)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
    out = "data/BTCUSDT_1m.csv"
    df.to_csv(out, index=False)
    logger.info(f"Salvo {len(df)} candles em {out}")
    print(f"OK: {out}")
