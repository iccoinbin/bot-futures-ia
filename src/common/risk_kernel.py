from __future__ import annotations
import json, os, datetime as dt
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Tuple, Optional, Dict, Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo(os.getenv("TZ", "America/Recife"))

def _now():
    return dt.datetime.now(TZ)

@dataclass
class RiskState:
    # acumuladores
    daily_pnl: float = 0.0
    weekly_pnl: float = 0.0
    equity_high_watermark: float = 0.0
    consecutive_losses: int = 0
    # metadados
    last_day: str = ""
    last_iso_week: str = ""
    kill_switch: bool = False

class RiskKernel:
    def __init__(self,
                 state_path: str,
                 max_daily_loss: float,
                 max_weekly_loss: float,
                 max_abs_dd: float,
                 max_consecutive_losses: int,
                 risk_per_trade_bp: int,
                 forbidden_windows: str,
                 hard_kill: bool=False):
        self.state_path = Path(state_path)
        self.max_daily_loss = float(max_daily_loss)
        self.max_weekly_loss = float(max_weekly_loss)
        self.max_abs_dd = float(max_abs_dd)
        self.max_consecutive_losses = int(max_consecutive_losses)
        self.risk_per_trade_bp = int(risk_per_trade_bp)
        self.forbidden_windows = self._parse_windows(forbidden_windows)
        self.state = self._load_state()
        if hard_kill:
            self.state.kill_switch = True
            self._save_state()

    # --------- factory ---------
    @classmethod
    def from_env(cls) -> "RiskKernel":
        sp = os.getenv("RISK_STATE_PATH", ".data/risk_state.json")
        return cls(
            state_path=sp,
            max_daily_loss=os.getenv("RISK_MAX_DAILY_LOSS_USDT", "100"),
            max_weekly_loss=os.getenv("RISK_MAX_WEEKLY_LOSS_USDT", "400"),
            max_abs_dd=os.getenv("RISK_MAX_ABS_DRAWDOWN_USDT", "800"),
            max_consecutive_losses=os.getenv("RISK_MAX_CONSECUTIVE_LOSSES", "4"),
            risk_per_trade_bp=os.getenv("RISK_RISK_PER_TRADE_BP", "35"),
            forbidden_windows=os.getenv("RISK_FORBIDDEN_WINDOWS", ""),
            hard_kill=os.getenv("RISK_KILL_SWITCH", "false").lower() == "true"
        )

    # --------- persistence ---------
    def _load_state(self) -> RiskState:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        if self.state_path.exists():
            try:
                data = json.loads(self.state_path.read_text())
                return RiskState(**data)
            except Exception:
                pass
        return RiskState()

    def _save_state(self):
        self.state_path.write_text(json.dumps(asdict(self.state), ensure_ascii=False, indent=2))

    # --------- windows ---------
    @staticmethod
    def _parse_windows(spec: str):
        # "HH:MM-HH:MM;HH:MM-HH:MM"
        wins = []
        for part in [s.strip() for s in spec.split(";") if s.strip()]:
            try:
                a,b = part.split("-")
                ah,am = map(int, a.split(":"))
                bh,bm = map(int, b.split(":"))
                wins.append(((ah,am),(bh,bm)))
            except Exception:
                continue
        return wins

    def _in_forbidden_window(self, ts: Optional[dt.datetime]=None) -> bool:
        ts = ts or _now()
        t = ts.time()
        for (ah,am),(bh,bm) in self.forbidden_windows:
            start = dt.time(ah,am)
            end   = dt.time(bh,bm)
            if start <= end:
                if start <= t <= end:
                    return True
            else:
                # janela que cruza meia-noite
                if t >= start or t <= end:
                    return True
        return False

    # --------- rollovers ---------
    def _roll_daily_weekly(self):
        now = _now()
        day = now.strftime("%Y-%m-%d")
        iso = f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"
        if self.state.last_day != day:
            self.state.daily_pnl = 0.0
            self.state.consecutive_losses = 0
            self.state.last_day = day
        if self.state.last_iso_week != iso:
            self.state.weekly_pnl = 0.0
            self.state.last_iso_week = iso

    # --------- public API ---------
    def pre_trade_check(self, equity_usdt: float, symbol: str, meta: Optional[Dict[str,Any]]=None) -> Tuple[bool,str,Dict[str,Any]]:
        self._roll_daily_weekly()
        meta = meta or {}
        reasons = []

        if self.state.kill_switch:
            reasons.append("kill_switch=ON")
        if self._in_forbidden_window():
            reasons.append("forbidden_window")
        if self.state.daily_pnl <= -abs(self.max_daily_loss):
            reasons.append(f"daily_limit_reached({self.state.daily_pnl:.2f}<=-{self.max_daily_loss})")
        if self.state.weekly_pnl <= -abs(self.max_weekly_loss):
            reasons.append(f"weekly_limit_reached({self.state.weekly_pnl:.2f}<=-{self.max_weekly_loss})")
        # MDD check: precisa do high watermark
        if self.state.equity_high_watermark == 0:
            self.state.equity_high_watermark = equity_usdt
        dd = self.state.equity_high_watermark - equity_usdt
        if dd >= abs(self.max_abs_dd):
            reasons.append(f"max_drawdown_reached({dd:.2f}>={self.max_abs_dd})")
        if self.state.consecutive_losses >= self.max_consecutive_losses:
            reasons.append(f"consecutive_losses>={self.max_consecutive_losses}")

        # tamanho máx. de risco por trade (em USDT)
        max_risk_usdt = equity_usdt * (self.risk_per_trade_bp/10000.0)
        info = {"max_risk_usdt": round(max_risk_usdt, 2), "equity": equity_usdt, "symbol": symbol}

        allowed = len(reasons) == 0
        if not allowed:
            info["blocked_reasons"] = reasons
        return allowed, (";".join(reasons) if reasons else "ok"), info

    def on_equity_update(self, equity_usdt: float):
        if equity_usdt > self.state.equity_high_watermark:
            self.state.equity_high_watermark = equity_usdt
            self._save_state()

    def on_fill(self, realized_pnl_usdt: float, is_closed_trade: bool, is_win: Optional[bool]=None):
        # Atualiza PnL diário/semanal e sequência de perdas
        self._roll_daily_weekly()
        self.state.daily_pnl  += realized_pnl_usdt
        self.state.weekly_pnl += realized_pnl_usdt
        if is_win is None:
            is_win = realized_pnl_usdt > 0
        if is_closed_trade:
            if is_win:
                self.state.consecutive_losses = 0
            else:
                self.state.consecutive_losses += 1
        self._save_state()

    def toggle_kill_switch(self, on: bool):
        self.state.kill_switch = bool(on)
        self._save_state()
