def trade_cost(entry: float, exit: float, qty: float, maker_bps: float, taker_bps: float, mode: str) -> float:
    maker = maker_bps / 10000.0
    taker = taker_bps / 10000.0
    if mode == "maker_first":
        return (entry * qty * maker) + (exit * qty * taker)
    else:
        return (entry * qty * taker) + (exit * qty * taker)

def est_slippage(price: float, atr: float, slip_bps_base: float, slip_frac_atr: float) -> float:
    bps = slip_bps_base / 10000.0
    atr_part = atr * slip_frac_atr
    return price * bps + atr_part

def funding_cost(hours_held: float, notional: float, rate_per_hour: float) -> float:
    return notional * rate_per_hour * hours_held
