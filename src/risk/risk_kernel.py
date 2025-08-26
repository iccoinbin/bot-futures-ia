import yaml
from pathlib import Path

class RiskKernel:
    def __init__(self, cfg_path: str = "config/risk.yml"):
        self.cfg = yaml.safe_load(Path(cfg_path).read_text())

    def can_open(self, state) -> bool:
        c = self.cfg
        if state["daily_loss_pct"] <= -c["max_daily_loss_pct"]:
            return False
        if state["weekly_loss_pct"] <= -c["max_weekly_loss_pct"]:
            return False
        if state["drawdown_pct"] <= -c["max_drawdown_pct"]:
            return False
        if state["open_positions"] >= c["max_open_positions"]:
            return False
        if state["consecutive_losses"] >= c["pause_after_consecutive_losses"]:
            return False
        return True
