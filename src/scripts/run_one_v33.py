import yaml, pandas as pd
from pathlib import Path
from math import sqrt
from src.features.ta_v31 import build_features
from src.strategies.orchestrator_v33 import run_backtest_orchestrated

def extract_pnls(raw):
    vals=[]
    for t in (raw or []):
        if isinstance(t,(int,float)): vals.append(float(t)); continue
        if isinstance(t,dict):
            for k in ("pnl","pnl_usdt","profit","pl","result","ret"):
                if k in t:
                    try: vals.append(float(t[k])); break
                    except: pass
    return vals

def curve_stats(pnls):
    if not pnls: return 0.0, 0.0
    eq=0.0; peak=0.0; mdd=0.0
    for p in pnls:
        eq += p; peak = max(peak, eq); mdd = min(mdd, eq-peak)
    return eq, abs(mdd)

def sharpe_s(pnls):
    if not pnls: return 0.0
    import statistics as st
    mu = st.mean(pnls); sd = st.pstdev(pnls) or 1e-9
    return (mu/sd)*sqrt(252)

if __name__ == "__main__":
    base = yaml.safe_load(open("config/settings_v31.yml"))
    over = yaml.safe_load(open("config/presets/btc_trend_v33.yml"))
    # merge raso
    cfg = {**base, **over,
           "filters": {**base.get("filters",{}), **over.get("filters",{})},
           "execution": {**base.get("execution",{}), **over.get("execution",{})},
           "slippage_fees": {**base.get("slippage_fees",{}), **over.get("slippage_fees",{})},
          }
    sym = cfg["symbols"][0]; tf=cfg.get("timeframe","5m"); days=cfg.get("history_days",60)
    inds = cfg["indicators"]
    path = Path(f"data/{sym}_{tf}_{days}d.csv")
    if not path.exists():
        print(f"faltam dados: {path}"); raise SystemExit(1)

    df = pd.read_csv(path, parse_dates=["open_time","close_time"])
    feats = build_features(df, inds["ema_fast"], inds["ema_slow"], inds["atr_period"], inds["adx_period"], inds.get("vwap_window",20))
    res = run_backtest_orchestrated(feats, cfg)
    pnls = extract_pnls(res.get("trades"))
    pos = sum(x for x in pnls if x>0); neg = -sum(x for x in pnls if x<0)
    pf = (pos/(neg if neg>1e-9 else 1e-9)) if pnls else 0.0
    avg = (sum(pnls)/len(pnls)) if pnls else 0.0
    total, mdd = curve_stats(pnls)
    shp = sharpe_s(pnls)
    out = Path("reports"); out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{
        "symbol": sym, "trades": len(pnls),
        "wins": sum(1 for x in pnls if x>0),
        "losses": sum(1 for x in pnls if x<=0),
        "pnl_total": round(total,2), "avg": round(avg,4),
        "pf": round(pf,2), "mdd": round(mdd,2), "sharpe_s": round(shp,2),
        "preset": "btc_trend_v33"
    }]).to_csv(out/"result_btc_trend_v33.csv", index=False)
    print("[OK] salvo reports/result_btc_trend_v33.csv")
    print(f"BTCUSDT v33 => PnL {total:.2f} | Trades {len(pnls)} | PF {pf:.2f} | MDD {mdd:.2f} | Sharpe {shp:.2f}")
