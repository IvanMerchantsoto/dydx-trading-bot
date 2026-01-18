# func_kpis.py
from func_messaging import send_message
from constants import WALLET_ADDRESS

def _sf(x, d=0.0):
    try:
        return float(x)
    except Exception:
        return d

async def send_account_kpis(indexer):
    try:
        resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
        sub = resp.get("subaccount", {}) or {}

        equity = _sf(sub.get("equity"))
        free = _sf(sub.get("freeCollateral"))
        margin = _sf(sub.get("marginUsed"))

        positions = sub.get("openPerpetualPositions", {}) or sub.get("perpetualPositions", {}) or {}
        notional = 0.0
        unreal = 0.0
        open_legs = 0

        for m, p in positions.items():
            size = _sf(p.get("size"))
            if abs(size) <= 0:
                continue
            open_legs += 1
            unreal += _sf(p.get("unrealizedPnl"))
            px = _sf(p.get("oraclePrice")) or _sf(p.get("markPrice")) or _sf(p.get("entryPrice"))
            if px:
                notional += abs(size) * px

        msg = (
            "📊 *DYDX KPIs*\n"
            f"Equity: ${equity:,.2f}\n"
            f"Free collateral: ${free:,.2f}\n"
            f"Margin used: ${margin:,.2f}\n"
            f"Notional open (proxy): ${notional:,.2f}\n"
            f"Unrealized PnL: ${unreal:,.2f}\n"
            f"Open legs: {open_legs}"
        )
        send_message(msg)

        return {"equity": equity, "free": free, "margin": margin, "notional": notional, "unreal": unreal, "open_legs": open_legs}

    except Exception as e:
        send_message(f"⚠️ KPI error: {e}")
        return None
