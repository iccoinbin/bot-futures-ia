import pandas as pd
from src.backtest.backtester import sanity_backtest

IN_FILE = "data/BTCUSDT_1m_features.csv"

if __name__ == "__main__":
    df = pd.read_csv(IN_FILE, parse_dates=["open_time","close_time"])
    pnl = sanity_backtest(df, slippage_bps=5)
    print(f"SANITY BACKTEST — PnL bruto (1 contrato): {pnl:.2f}")
