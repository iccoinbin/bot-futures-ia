import yaml, pandas as pd
from pathlib import Path
from math import sqrt
from copy import deepcopy
from src.features.ta_v31 import build_features
from src.strategies.orchestrator_v31 import run_backtest_orchestrated

BASE = yaml.safe_load(open("config/settings_v31.yml"))

def merge(base, override):
    out = deepcopy(base)
    for k,v in override.items():
        if isinstance(v, dict) and k in out and isinstance(out[k], dict):
            out[k] = merge(out[k], v)
        else:
            out[k] = v
    return out

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
        eq += p
        peak = max(peak, eq)
        mdd = min(mdd, eq-peak)
    return eq, abs(mdd)

def sharpe_daily(pnls):
    if not pnls: return 0.0
    import statistics as st
    mean = st.mean(pnls)
    sd = st.pstdev(pnls) or 1e-9
    # aproxima diária: assume ~200 trades ~ 60 dias -> fator só para comparação
    return (mean/sd) * sqrt(252)

def run_one(cfg):
    rows=[]
    symbols = cfg.get("symbols", ["BTCUSDT"])
    tf = cfg.get("timeframe","5m")
    days = cfg.get("history_days",60)
    inds = cfg["indicators"]
    for sym in symbols:
        path = Path(f"data/{sym}_{tf}_{days}d.csv")
        if not path.exists():
            rows.append({"symbol": sym, "error": f"missing {path}"})
            continue
        df = pd.read_csv(path, parse_dates=["open_time","close_time"])
        feats = build_features(df, inds["ema_fast"], inds["ema_slow"], inds["atr_period"], inds["adx_period"], inds.get("vwap_window",20))
        res = run_backtest_orchestrated(feats, cfg)
        pnls = extract_pnls(res.get("trades") or res.get("trade_log") or res.get("executions"))
        pos = sum(x for x in pnls if x>0); neg = -sum(x for x in pnls if x<0)
        pf = (pos/(neg if neg>1e-9 else 1e-9)) if pnls else 0.0
        avg = (sum(pnls)/len(pnls)) if pnls else 0.0
        pnl_total, mdd = curve_stats(pnls)
        shp = sharpe_daily(pnls)
        rows.append({
            "symbol": sym,
            "trades": len(pnls),
            "wins": sum(1 for x in pnls if x>0),
            "losses": sum(1 for x in pnls if x<=0),
            "pnl_total": round(pnl_total,2),
            "avg": round(avg,4),
            "pf": round(pf,2),
            "mdd": round(mdd,2),
            "sharpe_s": round(shp,2),
        })
    return pd.DataFrame(rows)

def main():
    presets = [
        ("btc_trend_conservative", __import__("yaml").safe_load(open("config/presets/btc_trend_conservative.yml"))),
        ("eth_trend_conservative", __import__("yaml").safe_load(open("config/presets/eth_trend_conservative.yml"))),
        ("btc_mr", __import__("yaml").safe_load(open("config/presets/btc_mr.yml"))),
        ("eth_mr", __import__("yaml").safe_load(open("config/presets/eth_mr.yml"))),
        ("btc_trend", yaml.safe_load(open("config/presets/btc_trend.yml"))),
        ("eth_trend", yaml.safe_load(open("config/presets/eth_trend.yml"))),
    ]
    outdir = Path("reports"); outdir.mkdir(parents=True, exist_ok=True)
    all_frames=[]
    for name, over in presets:
        cfg = merge(BASE, over)
        df = run_one(cfg)
        df.to_csv(outdir/f"result_{name}.csv", index=False)
        print(f"[OK] salvo reports/result_{name}.csv")
        all_frames.append(df.assign(preset=name))
    if all_frames:
        big = pd.concat(all_frames, ignore_index=True)
        big.to_csv(outdir/"result_all.csv", index=False)
        print("[OK] salvo reports/result_all.csv")
        print(big)
if __name__ == "__main__":
    main()
