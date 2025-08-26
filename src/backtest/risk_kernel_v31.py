from dataclasses import dataclass

@dataclass
class RiskState:
    equity_start: float
    equity_now: float
    day_pl: float
    week_pl: float
    consec_losses: int
    max_equity: float

class RiskKernel:
    def __init__(self, cfg: dict):
        self.cfg = cfg

    def allow_new_trade(self, state: RiskState) -> bool:
        rk = self.cfg["risk"]
        if (state.day_pl / state.equity_start * 100) <= -rk["max_daily_loss_pct"]:
            return False
        if (state.week_pl / state.equity_start * 100) <= -rk["max_weekly_loss_pct"]:
            return False
        drawdown_pct = (state.equity_now - state.max_equity) / state.max_equity * 100
        if drawdown_pct <= -rk["max_drawdown_pct"]:
            return False
        if state.consec_losses >= rk["max_consecutive_losses"]:
            return False
        return True
