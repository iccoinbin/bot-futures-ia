import pandas as pd
from src.strategies.baseline_atr import baseline_atr_strategy
import yaml

if __name__ == "__main__":
    df = pd.read_csv("data/BTCUSDT_1m_features.csv", parse_dates=["open_time","close_time"])

    cfg = yaml.safe_load(open("config/settings.yml"))
    tp_mult = cfg["execution"]["tp_atr_mult"]
    sl_mult = cfg["execution"]["sl_atr_mult"]
    risk_pct = cfg["risk"]["risk_per_trade_pct"]
    capital = cfg["risk"]["capital_usdt"]
    maker_bps = cfg["slippage"]["maker_bps"]
    taker_bps = cfg["slippage"]["taker_bps"]

    pnl, trades = baseline_atr_strategy(df, tp_mult, sl_mult, risk_pct, capital, maker_bps, taker_bps)

    wins = len([t for t in trades if t > 0])
    losses = len(trades) - wins
    avg = (sum(trades)/len(trades)) if trades else 0.0

    print(f"Baseline ATR Backtest — PnL total: {pnl:.2f} USDT")
    print(f"Trades: {len(trades)} | Wins: {wins} | Losses: {losses} | Avg/trade: {avg:.4f}")
