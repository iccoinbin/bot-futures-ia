import pandas as pd
from datetime import timedelta

def _slip(price: float, atr: float, mode: str, slip_bps_base: float, slip_frac_atr: float, side: int) -> float:
    # slippage simples: bps sobre preço + fração do ATR (meio-spread); aplica na direção da execução
    bump = price * (slip_bps_base/10000.0) + atr * slip_frac_atr
    return price + bump if side>0 else price - bump

def _fees(entry: float, exit: float, qty: float, maker_bps: float, taker_bps: float, mode: str) -> float:
    mk = maker_bps/10000.0
    tk = taker_bps/10000.0
    if mode == "maker_first":
        return entry*qty*mk + exit*qty*tk
    return entry*qty*tk + exit*qty*tk

def run_trend(df: pd.DataFrame, cfg: dict):
    ex = cfg["execution"]
    flt = cfg["filters"]
    fee = cfg["slippage_fees"]
    risk = cfg["risk"]

    tp_mult = ex["tp_atr_mult"]; sl_mult = ex["sl_atr_mult"]
    part_r  = ex["partial_at_r"]; trail_after = ex["trailing_after_r"]; trail_mult = ex["trailing_atr_mult"]
    mode    = ex["mode"]

    maker_bps = fee["maker_bps"]; taker_bps = fee["taker_bps"]
    slip_bps  = fee.get("slippage_bps", 0.0)
    slip_base = fee.get("slip_bps_base", slip_bps)
    slip_frac = fee.get("slip_bps_perc_of_atr", 0.10)

    adx_min  = flt["adx_trend_min"]
    use_q    = flt["use_atr_quantile"]; ql = flt["atrq_low"]; qh = flt["atrq_high"]
    block_m  = flt.get("block_funding_minutes", 0)

    cap      = risk["capital_usdt"]; risk_pct = risk["risk_per_trade_pct"]/100.0
    max_seq  = risk["max_consecutive_losses"]

    d = df.copy().reset_index(drop=True)
    # filtros de volatilidade
    if use_q and len(d) > 10:
        low = d["atr_pct"].quantile(ql); high = d["atr_pct"].quantile(qh)
        d = d[(d["atr_pct"]>=low) & (d["atr_pct"]<=high)]

    pnl_total = 0.0; trades=[]; losses_row=0
    pos=0; qty=0.0; ep=0.0; tp=None; sl=None; trail=None; et=None

    for _, r in d.iterrows():
        ts = r["close_time"]; price = float(r["close"]); atr=float(r["atr"]); adx=float(r["adx"])
        emaf=float(r["ema_fast"]); emas=float(r["ema_slow"])

        # funding block simples: evita alguns minutos de cada hora
        if block_m>0 and (ts.minute<block_m or ts.minute>=60-block_m):
            continue
        if pd.isna(atr) or atr<=0:
            continue
        # regime de tendência
        if adx < adx_min:  # sem regime
            continue

        # sizing por R (ATR*sl_mult)
        r_val = sl_mult*atr
        this_risk = cap*risk_pct
        plan_qty = (this_risk/r_val) if r_val>0 else 0.0

        # sinal: pullback à EMA rápida na direção da tendência
        trend_up = emaf>emas
        near_fast = abs(price - emaf) <= (0.25*atr)

        if pos==0:
            if plan_qty<=0 or not near_fast:
                continue
            if trend_up:
                pos=1; qty=plan_qty
                ep = price if mode=="maker_first" else _slip(price, atr, mode, slip_base, slip_frac, +1)
                tp = ep + tp_mult*atr; sl = ep - sl_mult*atr; et = ts; trail=None
            else:
                pos=-1; qty=plan_qty
                ep = price if mode=="maker_first" else _slip(price, atr, mode, slip_base, slip_frac, -1)
                tp = ep - tp_mult*atr; sl = ep + sl_mult*atr; et = ts; trail=None
            continue

        # gestão
        if pos==1:
            if part_r>0 and (price-ep) >= part_r*r_val and qty>0:
                half=qty*0.5
                ex_p = price if mode=="maker_first" else _slip(price, atr, mode, slip_base, slip_frac, -1)
                cost=_fees(ep, ex_p, half, maker_bps, taker_bps, mode)
                pnl = (ex_p-ep)*half - cost
                trades.append({"pnl": pnl, "time": ts})
                pnl_total += pnl; qty -= half; sl=min(sl, ep)
            if trail_after>0 and (price-ep) >= trail_after*r_val:
                trail = max(sl, price - trail_mult*atr)
            hit_tp = price>=tp; hit_sl = price<=sl; hit_tr = (trail is not None and price<=trail)
            if hit_tp or hit_sl or hit_tr:
                ex_p = price if mode=="maker_first" else _slip(price, atr, mode, slip_base, slip_frac, -1)
                cost=_fees(ep, ex_p, qty, maker_bps, taker_bps, mode)
                pnl=(ex_p-ep)*qty - cost
                trades.append({"pnl": pnl, "time": ts})
                pnl_total += pnl; losses_row = losses_row+1 if pnl<0 else 0
                pos=0; qty=0.0; tp=sl=trail=None
                if losses_row>=max_seq: break

        else: # pos == -1
            if part_r>0 and (ep-price) >= part_r*r_val and qty>0:
                half=qty*0.5
                ex_p = price if mode=="maker_first" else _slip(price, atr, mode, slip_base, slip_frac, +1)
                cost=_fees(ep, ex_p, half, maker_bps, taker_bps, mode)
                pnl = (ep-ex_p)*half - cost
                trades.append({"pnl": pnl, "time": ts})
                pnl_total += pnl; qty -= half; sl=max(sl, ep)
            if trail_after>0 and (ep-price) >= trail_after*r_val:
                trail = min(sl, price + trail_mult*atr)
            hit_tp = price<=tp; hit_sl = price>=sl; hit_tr = (trail is not None and price>=trail)
            if hit_tp or hit_sl or hit_tr:
                ex_p = price if mode=="maker_first" else _slip(price, atr, mode, slip_base, slip_frac, +1)
                cost=_fees(ep, ex_p, qty, maker_bps, taker_bps, mode)
                pnl=(ep-ex_p)*qty - cost
                trades.append({"pnl": pnl, "time": ts})
                pnl_total += pnl; losses_row = losses_row+1 if pnl<0 else 0
                pos=0; qty=0.0; tp=sl=trail=None
                if losses_row>=max_seq: break

    wins = sum(1 for t in trades if t["pnl"]>0)
    losses = len(trades)-wins
    return pnl_total, trades, wins, losses
