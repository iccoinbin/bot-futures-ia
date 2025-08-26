import time, yaml, pandas as pd, argparse
from pathlib import Path
from datetime import datetime, timezone
import requests
from src.features.ta_v31 import build_features

def base_url(testnet: bool):
    return "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"

def fetch_klines(symbol: str, interval: str, limit: int, testnet: bool) -> pd.DataFrame:
    url = f"{base_url(testnet)}/fapi/v1/klines"
    r = requests.get(url, params={"symbol": symbol, "interval": interval, "limit": limit}, timeout=12)
    r.raise_for_status()
    cols = ["open_time","open","high","low","close","volume","close_time","qav","num_trades","taker_base","taker_quote","ignore"]
    df = pd.DataFrame(r.json(), columns=cols)
    df["open_time"]  = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    for c in ["open","high","low","close","volume","qav","taker_base","taker_quote"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("close_time").reset_index(drop=True)

def extract_pnls(trades):
    vals=[]
    for t in (trades or []):
        if isinstance(t,(int,float)): vals.append(float(t)); continue
        if isinstance(t,dict):
            for k in ("pnl","pnl_usdt","profit","pl","result","ret"):
                if k in t:
                    try: vals.append(float(t[k])); break
                    except: pass
    return vals

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", type=str, default=None)
    ap.add_argument("--timeframe", type=str, default=None)
    args = ap.parse_args()

    cfg = yaml.safe_load(open("config/settings_v31.yml"))
    rt  = cfg.setdefault("runtime", {})
    sym = (args.symbol or rt.get("symbol","BTCUSDT")).upper()
    tf  = (args.timeframe or rt.get("timeframe","5m")).lower()
    poll = int(rt.get("poll_interval_sec",10))
    hist_min = int(rt.get("history_minutes",600))
    testnet = bool(cfg.get("binance",{}).get("testnet",True))

    inds = cfg["indicators"]
    outdir = Path("logs"); outdir.mkdir(parents=True, exist_ok=True)
    outcsv = outdir / f"shadow_{sym}_{tf}.csv"
    last_bar = None

    print(f"[shadow] symbol={sym} tf={tf} testnet={testnet} poll={poll}s history={hist_min}m")
    while True:
        try:
            limit = max(200, hist_min // (1 if tf.endswith("m") else 5))
            raw = fetch_klines(sym, tf, limit, testnet)
            feats = build_features(raw, inds["ema_fast"], inds["ema_slow"], inds["atr_period"], inds["adx_period"], inds.get("vwap_window",20))
            # métrica simples de sanity: soma de PnLs do bloco
            from src.strategies.orchestrator_v33 import run_backtest_orchestrated
            res = run_backtest_orchestrated(feats, cfg)
            pnls = extract_pnls(res.get("trades"))
            pnl_total = float(sum(pnls))

            bar_close = raw.iloc[-1]["close_time"]
            if bar_close != last_bar:
                last_bar = bar_close
                row = {
                    "time_utc": datetime.now(timezone.utc).isoformat(),
                    "bar_close": bar_close.isoformat(),
                    "symbol": sym, "tf": tf,
                    "trades_batch": len(pnls),
                    "pnl_batch": round(pnl_total, 6),
                    "wins": sum(1 for x in pnls if x>0),
                    "losses": sum(1 for x in pnls if x<=0),
                }
                pd.DataFrame([row]).to_csv(outcsv, index=False, mode="a", header=not outcsv.exists())
                print(f"[shadow] {row['bar_close']} | trades {row['trades_batch']} | pnl {row['pnl_batch']}")
            else:
                print(f"[shadow] aguardando fechamento | bar {bar_close} | poll ativo")
        except Exception as e:
            print("[shadow][erro]", repr(e))
        time.sleep(poll)
