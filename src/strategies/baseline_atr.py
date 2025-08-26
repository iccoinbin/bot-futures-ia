import pandas as pd

def baseline_atr_strategy(df: pd.DataFrame, tp_mult: float, sl_mult: float, risk_pct: float, capital: float, maker_bps: float, taker_bps: float):
    """
    Estratégia baseline para backtest:
    - Direção: segue o sinal 'trend' (EMA21 > EMA55 compra; caso contrário, venda).
    - Gestão: TP=tp_mult*ATR, SL=sl_mult*ATR.
    - Tamanho: baseado em risco por trade (% do capital / SL em ATR).
    - Custos: aplica maker+taker em bps sobre entrada+saída.
    Observação: Simples e didática; não é para produção.
    """
    position = 0
    entry_price = 0.0
    pnl = 0.0
    trades = []

    tp = None
    sl = None

    for _, row in df.iterrows():
        price = float(row["close"])
        atr = float(row["atr"])
        trend = int(row["trend"])

        # tamanho do lote com base no risco e ATR
        qty = 0.0
        if atr > 0 and sl_mult > 0:
            qty = (capital * (risk_pct / 100.0)) / (sl_mult * atr)

        if position == 0:
            if qty <= 0:
                continue
            if trend == 1:  # compra
                entry_price = price
                position = 1
                tp = entry_price + (tp_mult * atr)
                sl = entry_price - (sl_mult * atr)
            else:  # venda
                entry_price = price
                position = -1
                tp = entry_price - (tp_mult * atr)
                sl = entry_price + (sl_mult * atr)
        else:
            if position == 1:
                hit_tp = price >= tp
                hit_sl = price <= sl
                invert = trend == 0
                if hit_tp or hit_sl or invert:
                    exit_price = price
                    profit = (exit_price - entry_price) * qty
                    cost = (maker_bps + taker_bps) / 10000.0 * (entry_price + exit_price) * qty
                    pnl += (profit - cost)
                    trades.append(profit - cost)
                    position = 0
            else:  # position == -1
                hit_tp = price <= tp
                hit_sl = price >= sl
                invert = trend == 1
                if hit_tp or hit_sl or invert:
                    exit_price = price
                    profit = (entry_price - exit_price) * qty
                    cost = (maker_bps + taker_bps) / 10000.0 * (entry_price + exit_price) * qty
                    pnl += (profit - cost)
                    trades.append(profit - cost)
                    position = 0

    return pnl, trades
