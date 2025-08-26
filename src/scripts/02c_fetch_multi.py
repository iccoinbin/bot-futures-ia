import yaml
from pathlib import Path
from src.scripts.fetch_klines_range import fetch_klines_range

if __name__ == "__main__":
    cfg = yaml.safe_load(open("config/settings_v31.yml"))
    symbols = cfg.get("symbols", ["BTCUSDT", "ETHUSDT"])
    tf = cfg.get("timeframe", "5m")
    days = cfg.get("history_days", 60)

    Path("data").mkdir(parents=True, exist_ok=True)

    for sym in symbols:
        df = fetch_klines_range(sym, tf, days)
        out = f"data/{sym}_{tf}_{days}d.csv"
        df.to_csv(out, index=False)
        print(f"OK: {sym} -> {len(df)} candles em {out}")
