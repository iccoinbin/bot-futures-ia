import math
import pandas as pd
from datetime import datetime, timedelta, timezone
from src.exchange.binance_client import client

# Este script usa o cliente já existente (você já configurou no projeto base)
# Baixa histórico paginado para 'history_days' do settings_v3.yml

import yaml

def fetch_klines_range(symbol: str, interval: str, days: int) -> pd.DataFrame:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    df_all = []

    # Binance limita 'limit' por chamada (usamos 1000)
    limit = 1000
    cur = start
    while cur < end:
        # usar timestamp em ms
        start_ms = int(cur.timestamp() * 1000)
        k = client.klines(symbol=symbol, interval=interval, limit=limit, startTime=start_ms)
        if not k:
            break
        cols = ["open_time","open","high","low","close","volume","close_time","qav","num_trades","taker_base","taker_quote","ignore"]
        df = pd.DataFrame(k, columns=cols)
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
        df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
        df_all.append(df)
        # avança o cursor
        last_close = df["close_time"].iloc[-1]
        cur = last_close.to_pydatetime() + (last_close - df["open_time"].iloc[-1].to_pydatetime())

        if len(df) < limit:
            break

    out = pd.concat(df_all, ignore_index=True).drop_duplicates(subset=["open_time"])
    return out

if __name__ == "__main__":
    cfg = yaml.safe_load(open("config/settings_v3.yml"))
    sym = cfg["symbol"]
    tf = cfg["timeframe"]
    days = cfg["history_days"]

    df = fetch_klines_range(sym, tf, days)
    out = f"data/{sym}_{tf}_{days}d.csv"
    df.to_csv(out, index=False)
    print(f"OK: salvo {len(df)} candles em {out}")
