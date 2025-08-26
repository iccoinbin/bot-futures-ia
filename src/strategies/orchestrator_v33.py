import pandas as pd
from src.strategies.trend_v31 import run_trend
from src.strategies.meanrev_v31 import run_meanrev

def _pnls_from_trades(trades):
    vals=[]
    for t in (trades or []):
        if isinstance(t,(int,float)): vals.append(float(t)); continue
        if isinstance(t,dict):
            for k in ("pnl","pnl_usdt","profit","pl","result","ret"):
                if k in t:
                    try: vals.append(float(t[k])); break
                    except: pass
    return vals

def _apply_risk_guards(df_all, trades, cfg):
    # df_all: dataframe completo (com close_time)
    # trades: lista de dicts {"pnl":..., "time":...}
    if not trades:
        return trades, 0.0, 0, 0

    r = cfg["risk"]
    cap = float(r.get("capital_usdt", 10000))
    day_lim  = cap * float(r.get("max_daily_loss_pct", 0.02))
    wk_lim   = cap * float(r.get("max_weekly_loss_pct", 0.06))
    mdd_lim  = cap * float(r.get("max_drawdown_pct", 0.12))

    # ordena trades por tempo
    tdf = pd.DataFrame(trades)
    if "time" not in tdf.columns:
        return trades, sum(_pnls_from_trades(trades)), sum(1 for x in _pnls_from_trades(trades) if x>0), sum(1 for x in _pnls_from_trades(trades) if x<=0)
    tdf["time"] = pd.to_datetime(tdf["time"])
    tdf = tdf.sort_values("time").reset_index(drop=True)

    eq = 0.0; peak = 0.0
    out = []
    day_start = None; day_pnl = 0.0
    week_pnl = 0.0

    for _, row in tdf.iterrows():
        ts = row["time"]
        pnl = float(row.get("pnl", 0.0))
        day = ts.date()
        iso_week = ts.isocalendar().week

        # reseta acumuladores de dia/semana quando virar
        if day_start is None:
            day_start = day
            cur_week = iso_week
        if day != day_start:
            day_pnl = 0.0
            day_start = day
        if iso_week != cur_week:
            week_pnl = 0.0
            cur_week = iso_week

        # aplica o trade e checa limites
        day_pnl  += pnl
        week_pnl += pnl
        eq += pnl
        peak = max(peak, eq)
        mdd = peak - eq  # drawdown atual em USDT

        if (abs(day_pnl) >= day_lim and day_pnl < 0) or \
           (abs(week_pnl) >= wk_lim and week_pnl < 0) or \
           (mdd >= mdd_lim):
            # corta aqui: não adiciona esse trade e encerra
            break

        out.append({"pnl": pnl, "time": ts})

    wins = sum(1 for t in out if t["pnl"]>0)
    losses = len(out)-wins
    total = sum(t["pnl"] for t in out)
    return out, total, wins, losses

def run_backtest_orchestrated(df_feat: pd.DataFrame, cfg: dict):
    adx_min = cfg["filters"]["adx_trend_min"]
    df = df_feat.copy().reset_index(drop=True)
    regimes = (df["adx"] >= adx_min).astype(int)  # 1 trend, 0 range

    raw_trades=[]; total_pnl=0.0; wins=0; losses=0
    start=0
    for i in range(1, len(df)+1):
        if i==len(df) or regimes.iloc[i] != regimes.iloc[i-1]:
            block = df.iloc[start:i].copy()
            if len(block)>10:
                if regimes.iloc[start]==1:
                    pnl, tr, w, l = run_trend(block, cfg)
                else:
                    pnl, tr, w, l = run_meanrev(block, cfg)
                raw_trades.extend(tr)
                total_pnl += pnl
                wins += w; losses += l
            start=i

    # aplica cortes de risco sobre os trades consolidados
    cut_trades, cut_total, cw, cl = _apply_risk_guards(df, raw_trades, cfg)
    return {
        "pnl_total": cut_total,
        "trades": cut_trades,
        "wins": cw,
        "losses": cl,
        "num_trades": len(cut_trades),
    }
