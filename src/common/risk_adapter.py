from __future__ import annotations
from typing import Callable, Dict, Any
from common.risk_kernel import RiskKernel

def place_oco_order_guarded(
    *, rk: RiskKernel, equity_usdt: float, symbol: str, side: str, qty: float,
    entry_px: float, stop_px: float, take_px: float,
    isolated: bool = True, reduce_only_exit: bool = True,
    sender: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    ok, reason, info = rk.pre_trade_check(equity_usdt, symbol)
    if not ok:
        return {"ok": False, "blocked": True, "reason": reason, "info": info}
    resp = sender(
        symbol=symbol, side=side, qty=qty,
        entry_px=entry_px, stop_px=stop_px, take_px=take_px,
        isolated=isolated, reduce_only_exit=reduce_only_exit,
    )
    return {"ok": True, "blocked": False, "resp": resp, "risk_info": info}
