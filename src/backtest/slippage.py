def taker_cost(price: float, bps: float) -> float:
    return price * (bps / 10000.0)
