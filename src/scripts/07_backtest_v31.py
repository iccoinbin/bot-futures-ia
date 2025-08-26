import pandas as pd, yaml
from pathlib import Path
from src.features.ta_v31 import build_features
from src.strategies.orchestrator_v33 import run_backtest_orchestrated

def extract_trade_pnls(raw):
    vals = []
    for t in (raw or []):
        if isinstance(t, (int, float)):
            vals.append(float(t)); continue
        if isinstance(t, dict):
            for k in ("pnl","pnl_usdt","profit","pl","result","ret"):
                if k in t:
                    try:
                        vals.append(float(t[k])); break
                    except Exception:
                        pass
    return vals

if __name__ == "__main__":
    cfg = yaml.safe_load(open("config/settings_v31.yml"))
    symbols = cfg.get("symbols", ["BTCUSDT"])
    tf = cfg.get("timeframe", "5m")
    days = cfg.get("history_days", 60)
    inds = cfg["indicators"]

    for sym in symbols:
        in_file = f"data/{sym}_{tf}_{days}d.csv"
        if not Path(in_file).exists():
            print(f"{sym}: arquivo não encontrado: {in_file}. Rode o fetch antes.");
            continue

        df = pd.read_csv(in_file, parse_dates=["open_time","close_time"])
        df_feat = build_features(
            df, inds["ema_fast"], inds["ema_slow"], inds["atr_period"], inds["adx_period"], inds.get("vwap_window",20)
        )

        res = run_backtest_orchestrated(df_feat, cfg)

        trades_vals = extract_trade_pnls(res.get("trades") or res.get("trade_log") or res.get("executions"))
        wins = sum(1 for x in trades_vals if x > 0)
        losses = sum(1 for x in trades_vals if x <= 0)
        pos = sum(x for x in trades_vals if x > 0)
        neg_abs = -sum(x for x in trades_vals if x < 0)
        pf = (pos / (neg_abs if neg_abs > 1e-9 else 1e-9)) if trades_vals else 0.0
        avg = (sum(trades_vals)/len(trades_vals)) if trades_vals else 0.0

        print(f"{sym}: PnL {res.get('pnl_total',0.0):.2f} | Trades {len(trades_vals)} | Wins {wins} | Losses {losses} | Avg {avg:.4f} | PF {pf:.2f}")
