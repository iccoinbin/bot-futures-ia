import pandas as pd

def run_meanrev(df: pd.DataFrame, cfg: dict):
    ex = cfg["execution"]; flt = cfg["filters"]; fee = cfg["slippage_fees"]; risk = cfg["risk"]; ind = cfg["indicators"]
    w = ind.get("vwap_window", 20)
    tp_mult = ex["tp_atr_mult"]; sl_mult = ex["sl_atr_mult"]; mode=ex["mode"]
    maker_bps = fee["maker_bps"]; taker_bps=fee["taker_bps"]
    slip_base = fee.get("slip_bps_base", 0.02); slip_frac = fee.get("slip_bps_perc_of_atr", 0.10)
    cap=risk["capital_usdt"]; rpct=risk["risk_per_trade_pct"]/100.0
    adx_min = flt["adx_trend_min"]

    d = df.copy().reset_index(drop=True)
    try:
        d["vwap"] = (d["close"]*d["volume"]).rolling(w).sum()/d["volume"].rolling(w).sum()
    except Exception:
        d["vwap"] = d["close"].rolling(w).mean()
    d.dropna(inplace=True)
    if len(d) < 2:
        return 0.0, [], 0, 0

    trades=[]; pnl_total=0.0; wins=0; losses=0

    # i = candle de sinal; j=i+1 = candle de execução/validação
    for i in range(0, len(d)-1):
        r = d.iloc[i]
        if r["adx"] >= adx_min:  # só opera em range (ADX baixo)
            continue

        price=float(r["close"]); atr=float(r["atr"]); vwap=float(r["vwap"])
        if atr<=0: 
            continue
        r_val=sl_mult*atr; qty=(cap*rpct)/r_val if r_val>0 else 0.0
        if qty<=0: 
            continue

        side=0
        # desvio do VWAP com “margem” por vol
        if price < vwap*(1-0.10*float(r["atr_pct"])):   # abaixo → compra
            side = +1
        elif price > vwap*(1+0.10*float(r["atr_pct"])): # acima → venda
            side = -1
        else:
            continue

        # parâmetros de TP/SL
        if side>0:
            ep = price; tp = ep + tp_mult*atr; sl = ep - sl_mult*atr
        else:
            ep = price; tp = ep - tp_mult*atr; sl = ep + sl_mult*atr

        # candle seguinte
        j = i+1
        nxt = d.iloc[j]
        hi = float(nxt["high"]); lo = float(nxt["low"])

        hit_tp = (hi>=tp) if side>0 else (lo<=tp)
        hit_sl = (lo<=sl) if side>0 else (hi>=sl)

        # prioridade: quem ocorrer primeiro no candle? usamos ordem conservadora: SL antes de TP
        # (pior caso para evitar otimismo)
        outcome = "sl"
        if hit_sl and not hit_tp:
            outcome = "sl"
        elif hit_tp and not hit_sl:
            outcome = "tp"
        else:
            # se os dois ocorreram, assume SL (conservador)
            outcome = "sl"

        ex_price = tp if outcome=="tp" else sl

        # taxas (maker_first vs taker): para MR, assumimos taker na saída
        mk = maker_bps/10000.0; tk = taker_bps/10000.0
        fee_cost = (ep*qty*(mk if mode=="maker_first" else tk)) + (ex_price*qty*tk)

        if side>0:
            pnl = (ex_price-ep)*qty - fee_cost
        else:
            pnl = (ep-ex_price)*qty - fee_cost

        trades.append({"pnl": pnl, "time": nxt["close_time"], "side": side, "mode": "mr_nextbar"})
        pnl_total += pnl
        if pnl>0: wins+=1
        else: losses+=1

    return pnl_total, trades, wins, losses
