import yaml, pandas as pd, itertools
from pathlib import Path
from math import sqrt
from copy import deepcopy
from src.features.ta_v31 import build_features
from src.strategies.orchestrator_v33 import run_backtest_orchestrated

BASE = yaml.safe_load(open("config/settings_v31.yml"))
OVER = yaml.safe_load(open("config/presets/btc_trend_v33.yml"))

def merge(a,b):
    z=deepcopy(a)
    for k,v in b.items():
        if isinstance(v,dict) and k in z and isinstance(z[k],dict):
            z[k]=merge(z[k],v)
        else: z[k]=v
    return z

def pnls(raw):
    out=[]
    for t in (raw or []):
        if isinstance(t,(int,float)): out.append(float(t)); continue
        if isinstance(t,dict):
            for k in ("pnl","pnl_usdt","profit","pl","result","ret"):
                if k in t:
                    try: out.append(float(t[k])); break
                    except: pass
    return out

def mdd(p):
    eq=0; peak=0; dd=0
    for x in p:
        eq+=x; peak=max(peak,eq); dd=min(dd,eq-peak)
    return abs(dd)

def sharpe(p):
    if not p: return 0.0
    import statistics as st
    mu=st.mean(p); sd=st.pstdev(p) or 1e-9
    return (mu/sd)*sqrt(252)

def run_one(cfg):
    sym=cfg["symbols"][0]; tf=cfg["timeframe"]; days=cfg["history_days"]
    path=Path(f"data/{sym}_{tf}_{days}d.csv")
    if not path.exists(): return None
    df=pd.read_csv(path, parse_dates=["open_time","close_time"])
    ind=cfg["indicators"]
    feats=build_features(df, ind["ema_fast"], ind["ema_slow"], ind["atr_period"], ind["adx_period"], ind.get("vwap_window",20))
    res=run_backtest_orchestrated(feats,cfg)
    p=pnls(res.get("trades"))
    pos=sum(x for x in p if x>0); neg=-sum(x for x in p if x<0)
    pf=(pos/(neg if neg>1e-9 else 1e-9)) if p else 0.0
    return {
        "trades": len(p), "pnl_total": round(sum(p),2), "pf": round(pf,2),
        "mdd": round(mdd(p),2), "sharpe": round(sharpe(p),2)
    }

grid = {
  ("execution","tp_atr_mult"): [1.7, 1.8, 2.0],
  ("execution","sl_atr_mult"): [0.8, 0.9, 1.0],
  ("filters","adx_trend_min"): [16, 18, 20],
  ("filters","atrq_high"): [0.80, 0.85, 0.90]
}

rows=[]
keys=list(grid.keys())
for vals in itertools.product(*[grid[k] for k in keys]):
    over=deepcopy(OVER)
    for (sec,key), value in zip(keys, vals):
        over.setdefault(sec, {})[key]=value
    cfg=merge(BASE, over)
    r=run_one(cfg)
    if r:
        r.update({f"{sec}.{key}": val for (sec,key), val in zip(keys,vals)})
        rows.append(r)

out=pd.DataFrame(rows).sort_values(["pf","sharpe","pnl_total"], ascending=[False,False,False])
Path("reports").mkdir(exist_ok=True, parents=True)
out.to_csv("reports/grid_v33_btc.csv", index=False)
print(out.head(10).to_string(index=False))
print("\n[OK] salvo reports/grid_v33_btc.csv")
