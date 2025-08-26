import pandas as pd, yaml
from src.features.ta_v3 import build_features
from src.strategies.baseline_atr_v3 import baseline_atr_v3

if __name__ == "__main__":
    cfg = yaml.safe_load(open("config/settings_v3.yml"))
    sym, tf, days = cfg["symbol"], cfg["timeframe"], cfg["history_days"]
    in_file = f"data/{sym}_{tf}_{days}d.csv"
    df = pd.read_csv(in_file, parse_dates=["open_time","close_time"])

    # construir features
    inds = cfg["indicators"]
    df_feat = build_features(df, inds["ema_fast"], inds["ema_slow"], inds["atr_period"], inds["adx_period"])

    res = baseline_atr_v3(df_feat, cfg)
    print(f"PnL total: {res['pnl_total']:.2f} USDT | Trades: {res['num_trades']} | Wins: {res['wins']} | Losses: {res['losses']} | Avg: {res['avg_trade']:.4f}")
