import os
import psycopg2
import psycopg2.extras
import datetime as dt
from typing import Optional, Tuple

class RiskGuard:
    """
    Guardião de risco para BACKTEST:
      - Lê limites de risk_limits_active (DB) uma vez no init.
      - Em runtime, você alimenta com (ts, equity_now) do backtest.
      - Ele mantém:
          * âncora diária (equity no início do dia UTC)
          * âncora semanal (equity na 2ª feira 00:00 UTC)
          * pico de equity (para MDD)
      - Retorna se deve bloquear novas ENTRADAS quando algum limite for violado.
    """

    def __init__(self, dsn: Optional[str] = None, env: str = "prod", project: str = "bot-futures-ia"):
        self.dsn = dsn or os.environ.get("DATABASE_URL", "")
        if not self.dsn:
            raise RuntimeError("DATABASE_URL não definido para RiskGuard.")

        self.env = env
        self.project = project

        self.limits = self._read_limits()
        if not self.limits:
            raise RuntimeError("risk_limits_active não encontrado para env/project.")

        # Estado interno para o backtest
        self._day_anchor_ts: Optional[dt.datetime] = None
        self._day_anchor_eq: Optional[float] = None

        self._week_anchor_ts: Optional[dt.datetime] = None
        self._week_anchor_eq: Optional[float] = None

        self._peak_eq: Optional[float] = None

    # ---------- Helpers de Data ----------
    @staticmethod
    def _to_utc(ts: dt.datetime) -> dt.datetime:
        if ts.tzinfo is None:
            return ts.replace(tzinfo=dt.timezone.utc)
        return ts.astimezone(dt.timezone.utc)

    @staticmethod
    def _start_of_day_utc(ts: dt.datetime) -> dt.datetime:
        ts = RiskGuard._to_utc(ts)
        return dt.datetime(ts.year, ts.month, ts.day, tzinfo=dt.timezone.utc)

    @staticmethod
    def _start_of_week_utc(ts: dt.datetime) -> dt.datetime:
        # ISO: Monday is 0
        ts = RiskGuard._to_utc(ts)
        dow = ts.weekday()
        sod = dt.datetime(ts.year, ts.month, ts.day, tzinfo=dt.timezone.utc)
        return sod - dt.timedelta(days=dow)

    # ---------- DB ----------
    def _read_limits(self):
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT notional_initial, leverage_max, risk_per_trade_pct,
                           max_daily_loss_pct, max_weekly_loss_pct, max_mdd_pct
                    FROM risk_limits_active
                    WHERE env=%s AND project=%s
                    LIMIT 1
                """, (self.env, self.project))
                row = cur.fetchone()
                if not row:
                    return None
                return dict(
                    notional_initial=float(row["notional_initial"]),
                    leverage_max=int(row["leverage_max"]),
                    risk_per_trade_pct=float(row["risk_per_trade_pct"]),
                    max_daily_loss_pct=float(row["max_daily_loss_pct"]),
                    max_weekly_loss_pct=float(row["max_weekly_loss_pct"]),
                    max_mdd_pct=float(row["max_mdd_pct"]),
                )

    # ---------- API do Backtester ----------
    def should_block(self, ts: dt.datetime, equity_now: float) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Decide se devemos bloquear novas entradas.
        Retorna (blocked, scope, reason).
        * scope: 'DAILY' | 'WEEKLY' | 'MDD' | None
        * reason: texto explicativo
        """
        ts = self._to_utc(ts)
        li = self.limits

        # Inicializa âncoras no início
        if self._peak_eq is None:
            self._peak_eq = equity_now

        # Reseta âncora diária quando muda o dia
        sod = self._start_of_day_utc(ts)
        if (self._day_anchor_ts is None) or (self._day_anchor_ts != sod):
            self._day_anchor_ts = sod
            # âncora diária é equity no início do dia; se não souber o valor anterior,
            # usa o equity do primeiro candle/ordem do dia
            if self._day_anchor_eq is None or self._day_anchor_ts != sod:
                self._day_anchor_eq = equity_now

        # Reseta âncora semanal quando muda a semana
        sow = self._start_of_week_utc(ts)
        if (self._week_anchor_ts is None) or (self._week_anchor_ts != sow):
            self._week_anchor_ts = sow
            if self._week_anchor_eq is None or self._week_anchor_ts != sow:
                self._week_anchor_eq = equity_now

        # Atualiza pico para MDD
        if self._peak_eq is None or equity_now > self._peak_eq:
            self._peak_eq = equity_now

        # Cálculos percentuais
        reason = None
        scope = None

        # Diário
        if self._day_anchor_eq and self._day_anchor_eq > 0:
            pct_day = 100.0 * (equity_now - self._day_anchor_eq) / self._day_anchor_eq
            if pct_day <= -li["max_daily_loss_pct"]:
                scope = "DAILY"
                reason = f"Perda diária {pct_day:.3f}% <= -{li['max_daily_loss_pct']}%"

        # Semanal (só avalia se diário ainda não travou)
        if scope is None and self._week_anchor_eq and self._week_anchor_eq > 0:
            pct_week = 100.0 * (equity_now - self._week_anchor_eq) / self._week_anchor_eq
            if pct_week <= -li["max_weekly_loss_pct"]:
                scope = "WEEKLY"
                reason = f"Perda semanal {pct_week:.3f}% <= -{li['max_weekly_loss_pct']}%"

        # MDD (só avalia se não travou ainda)
        if scope is None and self._peak_eq and self._peak_eq > 0:
            mdd_pct = 100.0 * (equity_now - self._peak_eq) / self._peak_eq
            if mdd_pct <= -li["max_mdd_pct"]:
                scope = "MDD"
                reason = f"MDD {mdd_pct:.3f}% <= -{li['max_mdd_pct']}%"

        if scope:
            return True, scope, reason
        return False, None, None
