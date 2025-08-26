import pandas as pd
from datetime import timedelta
from src.backtest.costs_v3 import trade_cost, est_slippage, funding_cost

def _in_funding_block(ts, minutes: int) -> bool:
    # bloqueia N minutos antes/depois de cada virada de hora (funding simplificado)
    if minutes <= 0:
        return False
    minute = ts.minute
    return (minute < minutes) or (minute >= 60 - minutes)

def baseline_atr_v3(df: pd.DataFrame, cfg: dict) -> dict:
    tp_mult = cfg["execution"]["tp_atr_mult"]
    sl_mult = cfg["execution"]["sl_atr_mult"]
    partial_r = cfg["execution"]["partial_at_r"]
    trailing_after = cfg["execution"]["trailing_after_r"]
    trail_mult = cfg["execution"]["trailing_atr_mult"]
    mode = cfg["execution"]["mode"]

    maker_bps = cfg["slippage_fees"]["maker_bps"]
    taker_bps = cfg["slippage_fees"]["taker_bps"]
    slip_bps_base = cfg["slippage_fees"]["slip_bps_base"]
    slip_frac_atr = cfg["slippage_fees"]["slip_bps_perc_of_atr"]

    capital = cfg["risk"]["capital_usdt"]
    risk_pct = cfg["risk"]["risk_per_trade_pct"]
    max_consec = cfg["risk"]["max_consecutive_losses"]

    # filtros
    use_adx = cfg["filters"]["use_adx_filter"]
    adx_min = cfg["filters"]["adx_min"]
    use_atrq = cfg["filters"]["use_atr_quantile"]
    ql = cfg["filters"]["atrq_low"]
    qh = cfg["filters"]["atrq_high"]
    block_minutes = cfg["filters"]["block_funding_minutes"]

    # quantis de atr_pct (se habilitado)
    df = df.copy()
    if use_atrq:
        low = df["atr_pct"].quantile(ql)
        high = df["atr_pct"].quantile(qh)

    position = 0
    qty = 0.0
    entry_price = 0.0
    entry_time = None
    tp = None
    sl = None
    trail = None
    realized = 0.0
    consec_losses = 0

    trades = []

    for i, row in df.iterrows():
        ts = row["close_time"]
        price = float(row["close"])
        atr = float(row["atr"])
        trend = int(row["trend"])
        adx = float(row["adx"])
        atrp = float(row["atr_pct"])

        # aplicar bloqueio de funding (simplificado por hora)
        if _in_funding_block(ts, block_minutes):
            # opcionalmente fechar posição próxima do funding (não faremos aqui)
            continue

        # filtros de regime
        if use_adx and adx < adx_min:
            signal_ok = False
        else:
            signal_ok = True

        if use_atrq and (atrp < low or atrp > high):
            signal_ok = False

        # sizing (R = sl_mult*ATR)
        r_value = sl_mult * atr if atr > 0 else 0.0
        trade_risk = capital * (risk_pct / 100.0)
        planned_qty = (trade_risk / r_value) if r_value > 0 else 0.0

        if position == 0:
            if not signal_ok or planned_qty <= 0:
                continue
            # entrada por pullback: exige que o preço esteja perto da EMA rápida (aprox: close ~ ema_fast)
            # como não temos ema_fast aqui, supõe-se já no df (se precisar, inclua no cfg)
            try:
                ema_fast = float(row["ema_fast"])
            except Exception:
                ema_fast = price
            near_fast = abs(price - ema_fast) <= (0.2 * atr)  # tolerância 0.2*ATR

            if not near_fast:
                continue

            # abre conforme o trend
            qty = planned_qty
            entry_price = price + est_slippage(price, atr, slip_bps_base, slip_frac_atr) if mode=="taker" else price
            entry_time = ts
            if trend == 1:
                position = 1
                tp = entry_price + tp_mult * atr
                sl = entry_price - sl_mult * atr
            else:
                position = -1
                tp = entry_price - tp_mult * atr
                sl = entry_price + sl_mult * atr

            trail = None

        else:
            # gestão de posição
            unreal = 0.0
            if position == 1:
                unreal = (price - entry_price) * qty
                # ativa trailing após X R
                if trailing_after > 0 and (price - entry_price) >= trailing_after * r_value:
                    trail = max(sl if sl is not None else -1e9, price - trail_mult * atr)
                # parcial em 1R
                if partial_r > 0 and (price - entry_price) >= partial_r * r_value and qty > 0:
                    half = qty * 0.5
                    exit_p = price - est_slippage(price, atr, slip_bps_base, slip_frac_atr) if mode=="taker" else price
                    cost = trade_cost(entry_price, exit_p, half, maker_bps, taker_bps, mode)
                    realized += (exit_p - entry_price) * half - cost
                    qty -= half
                    # move SL para BE
                    sl = min(sl, entry_price)

                hit_tp = price >= tp
                hit_sl = price <= sl if sl is not None else False
                hit_trail = trail is not None and price <= trail

                if hit_tp or hit_sl or hit_trail or (use_adx and adx < adx_min):
                    exit_p = price - est_slippage(price, atr, slip_bps_base, slip_frac_atr) if mode=="taker" else price
                    hours = max(0.0, (ts - entry_time).total_seconds() / 3600.0)
                    fund = funding_cost(hours, exit_p * qty, cfg["funding"]["rate_per_hour"])
                    cost = trade_cost(entry_price, exit_p, qty, maker_bps, taker_bps, mode)
                    pnl_trade = (exit_p - entry_price) * qty - cost - fund
                    realized += pnl_trade
                    trades.append(pnl_trade)
                    consec_losses = consec_losses + 1 if pnl_trade < 0 else 0
                    position = 0
                    qty = 0.0
                    if consec_losses >= max_consec:
                        break

            else:  # position == -1
                unreal = (entry_price - price) * qty
                if trailing_after > 0 and (entry_price - price) >= trailing_after * r_value:
                    trail = min(sl if sl is not None else 1e12, price + trail_mult * atr)
                if partial_r > 0 and (entry_price - price) >= partial_r * r_value and qty > 0:
                    half = qty * 0.5
                    exit_p = price + est_slippage(price, atr, slip_bps_base, slip_frac_atr) if mode=="taker" else price
                    cost = trade_cost(entry_price, exit_p, half, maker_bps, taker_bps, mode)
                    realized += (entry_price - exit_p) * half - cost
                    qty -= half
                    sl = max(sl, entry_price)

                hit_tp = price <= tp
                hit_sl = price >= sl if sl is not None else False
                hit_trail = trail is not None and price >= trail

                if hit_tp or hit_sl or hit_trail or (use_adx and adx < adx_min):
                    exit_p = price + est_slippage(price, atr, slip_bps_base, slip_frac_atr) if mode=="taker" else price
                    hours = max(0.0, (ts - entry_time).total_seconds() / 3600.0)
                    fund = funding_cost(hours, exit_p * qty, cfg["funding"]["rate_per_hour"])
                    cost = trade_cost(entry_price, exit_p, qty, maker_bps, taker_bps, mode)
                    pnl_trade = (entry_price - exit_p) * qty - cost - fund
                    realized += pnl_trade
                    trades.append(pnl_trade)
                    consec_losses = consec_losses + 1 if pnl_trade < 0 else 0
                    position = 0
                    qty = 0.0
                    if consec_losses >= max_consec:
                        break

    res = {
        "pnl_total": realized,
        "trades": trades,
        "wins": len([t for t in trades if t > 0]),
        "losses": len([t for t in trades if t <= 0]),
        "avg_trade": (sum(trades)/len(trades)) if trades else 0.0,
        "num_trades": len(trades),
    }
    return res
