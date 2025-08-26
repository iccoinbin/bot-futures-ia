class ExecutorSim:
    def __init__(self, slippage_bps: int = 5):
        self.slippage_bps = slippage_bps

    def market_fill(self, side: str, qty: float, price: float) -> float:
        slip = price * (self.slippage_bps / 10000)
        return price + slip if side == "BUY" else price - slip
