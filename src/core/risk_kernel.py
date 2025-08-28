import os
import sys
import time
import argparse
import json
import datetime as dt
from typing import Optional, Tuple, List

import psycopg2
import psycopg2.extras

DEF_SLEEP_SEC = 60

def env_tznow():
    # Usa TZ do sistema; registros sempre em UTC no banco.
    return dt.datetime.now(dt.timezone.utc)

def pg_conn(dsn:str):
    return psycopg2.connect(dsn)

def fetch_one(cur, q, args=None):
    cur.execute(q, args or ())
    return cur.fetchone()

def fetch_all(cur, q, args=None):
    cur.execute(q, args or ())
    return cur.fetchall()

def table_exists(cur, schema: str, table: str) -> bool:
    row = fetch_one(cur, """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
    """, (schema, table))
    return bool(row)

def ensure_state_tables(cur):
    # risk_state: snapshot de bloqueio atual
    cur.execute("""
        CREATE TABLE IF NOT EXISTS risk_state (
          id SMALLINT PRIMARY KEY DEFAULT 1,
          env TEXT NOT NULL DEFAULT 'prod',
          project TEXT NOT NULL DEFAULT 'bot-futures-ia',
          blocked BOOLEAN NOT NULL DEFAULT FALSE,
          scope TEXT,               -- DAILY | WEEKLY | MDD
          reason TEXT,
          until_ts TIMESTAMPTZ,     -- quando desbloquear automaticamente (diário/semana)
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    # trigger updated_at
    cur.execute("""
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'risk_state_set_updated_at') THEN
            CREATE OR REPLACE FUNCTION risk_state_set_updated_at()
            RETURNS TRIGGER AS $f$
            BEGIN
              NEW.updated_at := now();
              RETURN NEW;
            END
            $f$ LANGUAGE plpgsql;
          END IF;
        END$$;
    """)
    cur.execute("""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'trg_risk_state_updated_at'
          ) THEN
            CREATE TRIGGER trg_risk_state_updated_at
            BEFORE UPDATE ON risk_state
            FOR EACH ROW EXECUTE FUNCTION risk_state_set_updated_at();
          END IF;
        END$$;
    """)
    # garante linha id=1
    cur.execute("""
        INSERT INTO risk_state (id, env, project, blocked)
        VALUES (1, 'prod', 'bot-futures-ia', FALSE)
        ON CONFLICT (id) DO NOTHING;
    """)

def insert_event(cur, env, project, etype, scope=None, equity=None, dd_pct=None, details=None):
    cur.execute("""
        INSERT INTO risk_events (env, project, event_type, scope, equity, drawdown_pct, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
    """, (env, project, etype, scope, equity, dd_pct, details))

def read_limits(cur, env, project):
    row = fetch_one(cur, """
        SELECT notional_initial, leverage_max, risk_per_trade_pct,
               max_daily_loss_pct, max_weekly_loss_pct, max_mdd_pct
        FROM risk_limits_active
        WHERE env=%s AND project=%s
        LIMIT 1
    """, (env, project))
    if not row:
        return None
    return {
        "notional_initial": float(row[0]),
        "leverage_max": int(row[1]),
        "risk_per_trade_pct": float(row[2]),
        "max_daily_loss_pct": float(row[3]),
        "max_weekly_loss_pct": float(row[4]),
        "max_mdd_pct": float(row[5]),
    }

def detect_equity_source(cur) -> Tuple[str, str]:
    """
    Detecta a melhor fonte disponível para equity/PnL:
      1) public.equity_snapshots (ts, equity)
      2) public.backtest_equity (ts, equity)
      3) public.pnl_ledger (ts, pnl)  -> integra para equity = notional_initial + sum(pnl)
    Retorna (mode, table_name).
    """
    candidates = [
        ("equity_series", "equity_snapshots", "equity"),
        ("equity_series", "backtest_equity", "equity"),
        ("pnl_ledger",    "pnl_ledger",      "pnl"),
    ]
    for mode, tbl, col in candidates:
        if table_exists(cur, "public", tbl):
            # valida colunas essenciais
            row = fetch_one(cur, f"""
                SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name=%s AND column_name IN ('ts', 'equity')
            """, (tbl,))
            if row and row[0] == 2:
                return mode, tbl
    return ("none", "")

def start_of_day_utc(now_utc: dt.datetime) -> dt.datetime:
    return dt.datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=dt.timezone.utc)

def start_of_week_utc(now_utc: dt.datetime) -> dt.datetime:
    # semana iniciando na segunda-feira (ISO)
    dow = now_utc.weekday()  # 0 = Monday
    sod = start_of_day_utc(now_utc)
    return sod - dt.timedelta(days=dow)

def load_equity_points(cur, mode: str, table: str, now_utc: dt.datetime, notional_initial: float) -> List[Tuple[dt.datetime, float]]:
    """
    Retorna lista [(ts_utc, equity_float)] nos últimos 30 dias.
    """
    since = now_utc - dt.timedelta(days=30)
    if mode == "equity_series":
        rows = fetch_all(cur, f"""
            SELECT ts, equity
            FROM {table}
            WHERE ts >= %s
            ORDER BY ts
        """, (since,))
        return [(r[0].astimezone(dt.timezone.utc), float(r[1])) for r in rows]
    elif mode == "pnl_ledger":
        rows = fetch_all(cur, f"""
            SELECT ts, pnl
            FROM {table}
            WHERE ts >= %s
            ORDER BY ts
        """, (since,))
        equity = notional_initial
        out = []
        for ts, pnl in rows:
            equity += float(pnl or 0.0)
            out.append((ts.astimezone(dt.timezone.utc), equity))
        return out
    else:
        return []

def eq_at_or_before(ts_list: List[Tuple[dt.datetime,float]], cutoff: dt.datetime, fallback: Optional[float]) -> Optional[float]:
    """
    Retorna equity no ponto <= cutoff (último conhecido).
    """
    last = None
    for ts, eq in ts_list:
        if ts <= cutoff:
            last = eq
        else:
            break
    return last if last is not None else fallback

def compute_stats(eq_series: List[Tuple[dt.datetime,float]], now_utc: dt.datetime, notional_initial: float):
    """
    Calcula:
      - equity_now
      - pct_day (desde início do dia)
      - pct_week (desde início da semana)
      - mdd_pct (drawdown percentual desde o pico dentro da janela)
    """
    if not eq_series:
        return None

    equity_now = eq_series[-1][1]
    sod = start_of_day_utc(now_utc)
    sow = start_of_week_utc(now_utc)

    eq_day0 = eq_at_or_before(eq_series, sod, fallback=notional_initial)
    eq_week0 = eq_at_or_before(eq_series, sow, fallback=notional_initial)

    pct_day = None
    if eq_day0 and eq_day0 > 0:
        pct_day = 100.0 * (equity_now - eq_day0) / eq_day0

    pct_week = None
    if eq_week0 and eq_week0 > 0:
        pct_week = 100.0 * (equity_now - eq_week0) / eq_week0

    # MDD: maior pico até agora e drawdown do ponto atual
    peak = max(eq for _, eq in eq_series)
    mdd_pct = None
    if peak and peak > 0:
        mdd_pct = 100.0 * (equity_now - peak) / peak  # negativo em drawdown

    return {
        "equity_now": equity_now,
        "pct_day": pct_day,
        "pct_week": pct_week,
        "mdd_pct": mdd_pct
    }

def maybe_unblock(cur, env, project, now_utc):
    # Auto-unblock se passou o until_ts
    row = fetch_one(cur, "SELECT blocked, scope, until_ts FROM risk_state WHERE id=1 AND env=%s AND project=%s", (env, project))
    if not row:
        return
    blocked, scope, until_ts = row
    if blocked and until_ts is not None and now_utc >= until_ts:
        cur.execute("""
            UPDATE risk_state
            SET blocked=FALSE, scope=NULL, reason=NULL, until_ts=NULL
            WHERE id=1 AND env=%s AND project=%s
        """, (env, project))
        insert_event(cur, env, project, "UNBLOCK", scope=None, details='{"auto":"time_elapsed"}')

def maybe_block(cur, env, project, scope: str, reason: str, equity_now: Optional[float], dd_pct: Optional[float], until_ts: Optional[dt.datetime]):
    # Seta bloqueio
    cur.execute("""
        UPDATE risk_state
        SET blocked=TRUE, scope=%s, reason=%s, until_ts=%s
        WHERE id=1 AND env=%s AND project=%s
    """, (scope, reason, until_ts, env, project))
    insert_event(cur, env, project, "VIOLATION", scope=scope, equity=equality(equity_now), dd_pct=dd_pct, details='{"reason": "'+reason.replace('"','\\"')+'"}')
    insert_event(cur, env, project, "BLOCK", scope=scope, equity=equality(equity_now), dd_pct=dd_pct, details='{"reason": "'+reason.replace('"','\\"')+'"}')

def equality(x):
    return None if x is None else float(x)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL", ""), help="Postgres DSN")
    parser.add_argument("--env", default="prod")
    parser.add_argument("--project", default="bot-futures-ia")
    parser.add_argument("--interval", type=int, default=DEF_SLEEP_SEC)
    args = parser.parse_args()

    if not args.dsn:
        print("ERROR: DATABASE_URL/--dsn não definido.", file=sys.stderr)
        sys.exit(2)

    while True:
        try:
            now_utc = env_tznow()
            with pg_conn(args.dsn) as conn:
                conn.autocommit = False
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    ensure_state_tables(cur)

                    limits = read_limits(cur, args.env, args.project)
                    if not limits:
                        insert_event(cur, args.env, args.project, "INFO", details='{"msg":"risk_limits_active ausente"}')
                        conn.commit()
                        time.sleep(args.interval); continue

                    mode, tbl = detect_equity_source(cur)
                    if mode == "none":
                        insert_event(cur, args.env, args.project, "INFO", details='{"msg":"Nenhuma fonte de equity encontrada"}')
                        conn.commit()
                        time.sleep(args.interval); continue

                    eq_series = load_equity_points(cur, mode, tbl, now_utc, limits["notional_initial"])
                    if len(eq_series) == 0:
                        insert_event(cur, args.env, args.project, "INFO", details='{"msg":"Fonte de equity vazia"}')
                        conn.commit()
                        time.sleep(args.interval); continue

                    stats = compute_stats(eq_series, now_utc, limits["notional_initial"])
                    insert_event(cur, args.env, args.project, "INFO", details=json.dumps({"dbg":"stats","mode": mode, "table": tbl, "equity_now": stats.get("equity_now"), "pct_day": stats.get("pct_day"), "pct_week": stats.get("pct_week"), "mdd_pct": stats.get("mdd_pct")}))
                    if not stats:
                        insert_event(cur, args.env, args.project, "INFO", details='{"msg":"Sem estatísticas válidas"}')
                        conn.commit()
                        time.sleep(args.interval); continue

                    # === Weekly & MDD checks (auto) ===
                    w = stats.get("pct_week")
                    if w is not None and w <= -limits["max_weekly_loss_pct"]:
                        reason = f"Perda semanal {w:.3f}% <= -{limits['max_weekly_loss_pct']:.1f}%"
                        maybe_block(cur, args.env, args.project, scope="WEEKLY", reason=reason,
                                    equity_now=stats.get("equity_now"), dd_pct=w, until_ts=None)

                    mdd = stats.get("mdd_pct")
                    if mdd is not None and mdd <= -limits["max_mdd_pct"]:
                        reason = f"MDD {mdd:.3f}% <= -{limits['max_mdd_pct']:.1f}%"
                        maybe_block(cur, args.env, args.project, scope="MDD", reason=reason,
                                    equity_now=stats.get("equity_now"), dd_pct=mdd, until_ts=None)

                    # Auto-unblock por tempo
                    maybe_unblock(cur, args.env, args.project, now_utc)

                    # Verifica bloqueio atual
                    row = fetch_one(cur, "SELECT blocked, scope FROM risk_state WHERE id=1 AND env=%s AND project=%s", (args.env, args.project))
                    blocked_now = bool(row[0]) if row else False

                    equity_now = stats["equity_now"]
                    pct_day = stats["pct_day"]
                    pct_week = stats["pct_week"]
                    mdd_pct = stats["mdd_pct"]  # negativo em drawdown (ex: -6.5)

                    # Condições de violação (perdas são valores negativos)
                    if not blocked_now:
                        # diário
                        if pct_day is not None and pct_day <= -limits["max_daily_loss_pct"]:
                            until = start_of_day_utc(now_utc) + dt.timedelta(days=1)
                            maybe_block(cur, args.env, args.project, "DAILY", f"Perda diária {pct_day:.3f}% <= -{limits['max_daily_loss_pct']}%", equity_now, None, until)

                        # semanal (pode bloquear mesmo que diário já tenha bloqueado; mas estado fica bloqueado de qualquer forma)
                        row = fetch_one(cur, "SELECT blocked FROM risk_state WHERE id=1 AND env=%s AND project=%s", (args.env, args.project))
                        blocked_now = bool(row[0]) if row else False
                        if not blocked_now and pct_week is not None and pct_week <= -limits["max_weekly_loss_pct"]:
                            sow = start_of_week_utc(now_utc)
                            until = sow + dt.timedelta(days=7)
                            maybe_block(cur, args.env, args.project, "WEEKLY", f"Perda semanal {pct_week:.3f}% <= -{limits['max_weekly_loss_pct']}%", equity_now, None, until)

                        # MDD (sem auto-unblock)
                        row = fetch_one(cur, "SELECT blocked FROM risk_state WHERE id=1 AND env=%s AND project=%s", (args.env, args.project))
                        blocked_now = bool(row[0]) if row else False
                        if not blocked_now and mdd_pct is not None and mdd_pct <= -limits["max_mdd_pct"]:
                            maybe_block(cur, args.env, args.project, "MDD", f"MDD {mdd_pct:.3f}% <= -{limits['max_mdd_pct']}%", equity_now, mdd_pct, None)

                    conn.commit()

        except Exception as e:
            try:
                with pg_conn(args.dsn) as conn:
                    with conn.cursor() as cur:
                        insert_event(cur, os.environ.get("ENV","prod") or "prod",
                                     os.environ.get("PROJECT","bot-futures-ia") or "bot-futures-ia",
                                     "INFO", details='{"error": "'+str(e).replace('"','\\"')+'"}')
                    conn.commit()
            except Exception:
                pass
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
