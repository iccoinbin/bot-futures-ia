#!/usr/bin/env python3
import os, sys, json, datetime as dt
import psycopg2, psycopg2.extras

ENV = os.environ.get("FEEDER_ENV", "prod")
PROJECT = os.environ.get("FEEDER_PROJECT", "bot-futures-ia")

def dsn_from_env() -> str:
    # Prioriza /etc/botfutures.env; fallback .env
    for path in ("/etc/botfutures.env", ".env"):
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip().startswith("DATABASE_URL"):
                        val = line.split("=",1)[1].strip().strip('"').strip("'")
                        return val.replace("postgresql+psycopg2://","postgresql://")
    raise RuntimeError("DATABASE_URL não encontrado em /etc/botfutures.env nem .env")

def table_exists(cur, schema, table) -> bool:
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema=%s AND table_name=%s
        """, (schema, table))
    return cur.fetchone() is not None

def last_snapshot_age(cur):
    cur.execute("SELECT MAX(ts) FROM equity_snapshots")
    ts = cur.fetchone()[0]
    if not ts: return None
    if ts.tzinfo is None: ts = ts.replace(tzinfo=dt.timezone.utc)
    return (dt.datetime.now(dt.timezone.utc) - ts).total_seconds()

def get_notional_initial(cur) -> float:
    cur.execute("""
        SELECT notional_initial
        FROM risk_limits_active
        WHERE env=%s AND project=%s
        LIMIT 1
    """, (ENV, PROJECT))
    row = cur.fetchone()
    return float(row[0]) if row else 1000.0

def equity_from_backtest(cur):
    if not table_exists(cur, "public", "backtest_equity"):
        return None, None
    cur.execute("SELECT ts, equity FROM backtest_equity ORDER BY ts DESC LIMIT 1")
    row = cur.fetchone()
    if not row: return None, None
    ts, eq = row
    if ts.tzinfo is None: ts = ts.replace(tzinfo=dt.timezone.utc)
    # considera “recente” se último ponto tem até 10 minutos
    age = (dt.datetime.now(dt.timezone.utc) - ts).total_seconds()
    return (float(eq), age)

def equity_from_ledger(cur, notional_initial: float):
    if not table_exists(cur, "public", "pnl_ledger"):
        return None
    cur.execute("SELECT COALESCE(SUM(pnl),0) FROM pnl_ledger")
    pnl_sum = float(cur.fetchone()[0] or 0.0)
    return notional_initial + pnl_sum

def main():
    dsn = dsn_from_env()
    now_utc = dt.datetime.now(dt.timezone.utc)
    with psycopg2.connect(dsn) as conn:
        cur = conn.cursor()
        # segura o ritmo se já tivemos snapshot há < 50s
        age = last_snapshot_age(cur)
        if age is not None and age < 50:
            print(json.dumps({"msg":"skip - snapshot muito recente","age":age}))
            return

        notional = get_notional_initial(cur)
        eq_bt, bt_age = equity_from_backtest(cur)
        if eq_bt is not None and bt_age is not None and bt_age <= 600:
            equity_now = eq_bt
            source = "backtest_equity"
        else:
            eq_led = equity_from_ledger(cur, notional)
            if eq_led is None:
                print(json.dumps({"msg":"skip - sem fonte de equity","env":ENV,"project":PROJECT}))
                return
            equity_now = eq_led
            source = "pnl_ledger"

        cur.execute("INSERT INTO equity_snapshots(ts, equity) VALUES (now(), %s)", (equity_now,))
        # loga um INFO para visibilidade
        cur.execute("""
            INSERT INTO risk_events(env, project, event_type, scope, equity, details)
            VALUES (%s,%s,'INFO',NULL,%s,%s)
        """, (ENV, PROJECT, equity_now, json.dumps({"dbg":"feeder","source":source})))
        conn.commit()
        print(json.dumps({"inserted":equity_now,"source":source}))
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(json.dumps({"error":str(e)}))
        sys.exit(1)
