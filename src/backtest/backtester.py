import pandas as pd
from ..exec.executor_sim import ExecutorSim

def sanity_backtest(df: pd.DataFrame, slippage_bps: int = 5) -> float:
    exe = ExecutorSim(slippage_bps)
    position = 0
    entry_price = 0.0
    pnl = 0.0

    for _, row in df.iterrows():
        price = row["close"]
        trend = row["trend"]
        if position == 0:
            if trend == 1:
                fill = exe.market_fill("BUY", 1, price)
                position = 1
                entry_price = fill
            else:
                fill = exe.market_fill("SELL", 1, price)
                position = -1
                entry_price = fill
        else:
            if (position == 1 and trend == 0) or (position == -1 and trend == 1):
                if position == 1:
                    exit_fill = exe.market_fill("SELL", 1, price)
                    pnl += (exit_fill - entry_price)
                else:
                    exit_fill = exe.market_fill("BUY", 1, price)
                    pnl += (entry_price - exit_fill)
                position = 0
    return pnl
