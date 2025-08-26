import pandas as pd
from src.strategies.trend_v31 import run_trend
from src.strategies.meanrev_v31 import run_meanrev

def run_backtest_orchestrated(df_feat: pd.DataFrame, cfg: dict):
    # Segmenta por regime usando ADX: >= limiar => tendência; < limiar => range
    adx_min = cfg["filters"]["adx_trend_min"]
    df = df_feat.copy().reset_index(drop=True)
    regimes = (df["adx"] >= adx_min).astype(int)  # 1 trend, 0 range

    total_pnl=0.0; all_trades=[]; wins=0; losses=0

    # quebras de regime
    start=0
    for i in range(1, len(df)+1):
        if i==len(df) or regimes.iloc[i] != regimes.iloc[i-1]:
            block = df.iloc[start:i].copy()
            if len(block) > 10:
                if regimes.iloc[start]==1:
                    pnl, tr, w, l = run_trend(block, cfg)
                else:
                    pnl, tr, w, l = run_meanrev(block, cfg)
                total_pnl += pnl; all_trades.extend(tr); wins+=w; losses+=l
            start=i

    return {
        "pnl_total": total_pnl,
        "trades": all_trades,
        "wins": wins,
        "losses": losses,
        "num_trades": len(all_trades),
    }
