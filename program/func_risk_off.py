import json
from datetime import datetime, timezone
from func_messaging import send_message
from constants import (
    WALLET_ADDRESS,
    RISK_SCORE_W_AGE, RISK_SCORE_W_ABS_Z, RISK_SCORE_W_UNREAL_PNL,
    RISK_OFF_MIN_AGE_HOURS
)
from func_public import get_candles_recent
from func_cointegration import calculate_zscore
from func_utils import format_number
from func_private import place_market_order
from v4_proto.dydxprotocol.clob.order_pb2 import Order
import asyncio

def _sf(x, d=0.0):
    try:
        return float(x)
    except Exception:
        return d

def _parse_opened_at(v):
    if not v:
        return None
    try:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(float(v), tz=timezone.utc)
        if isinstance(v, str):
            s = v.strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
    except Exception:
        return None
    return None

async def risk_off_close_worst_pair(node, indexer, wallet):
    # Load trades
    try:
        with open("bot_agents.json", "r") as f:
            trades = json.load(f) or []
    except Exception:
        return False

    if not trades:
        return False

    # market metadata
    markets_resp = await indexer.markets.get_perpetual_markets()
    markets = markets_resp.get("markets", {}) or {}

    # live positions + unreal pnl by market
    account_resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
    sub = account_resp.get("subaccount", {}) or {}
    positions = sub.get("openPerpetualPositions", {}) or sub.get("perpetualPositions", {}) or {}

    live_pos = {}
    unreal_by_market = {}
    for m, p in (positions or {}).items():
        size = _sf(p.get("size"))
        if abs(size) > 0:
            live_pos[m] = size
            unreal_by_market[m] = _sf(p.get("unrealizedPnl"))

    now_utc = datetime.now(timezone.utc)

    scored = []
    for t in trades:
        m1 = t.get("market_1"); m2 = t.get("market_2")
        if not m1 or not m2:
            continue
        if m1 not in live_pos or m2 not in live_pos:
            continue

        opened_at = _parse_opened_at(t.get("opened_at"))
        if not opened_at:
            continue
        age_hours = (now_utc - opened_at).total_seconds() / 3600.0
        if age_hours < float(RISK_OFF_MIN_AGE_HOURS):
            continue

        hedge_ratio = _sf(t.get("hedge_ratio"))
        if hedge_ratio == 0:
            continue

        # compute z_now
        z_now = None
        try:
            s1 = await get_candles_recent(indexer, m1)
            s2 = await get_candles_recent(indexer, m2)
            if len(s1) > 0 and len(s1) == len(s2):
                spread = s1 - (hedge_ratio * s2)
                z_now = float(calculate_zscore(spread).values.tolist()[-1])
        except Exception:
            z_now = None

        absz = abs(z_now) if z_now is not None else 0.0

        unreal = (unreal_by_market.get(m1, 0.0) + unreal_by_market.get(m2, 0.0))
        loss_penalty = max(0.0, -unreal)

        score = (RISK_SCORE_W_AGE * age_hours) + (RISK_SCORE_W_ABS_Z * absz) + (RISK_SCORE_W_UNREAL_PNL * loss_penalty)

        scored.append((score, t, age_hours, absz, unreal))

    if not scored:
        return False

    scored.sort(key=lambda x: x[0], reverse=True)
    score, t, age_hours, absz, unreal = scored[0]
    m1 = t["market_1"]; m2 = t["market_2"]

    send_message(
        "🛑 *RISK-OFF ACTIVADO*\n"
        f"Closing worst pair: {m1}/{m2}\n"
        f"Score: {score:.2f} | age={age_hours:.2f}h | abs(z)={absz:.2f} | unreal=${unreal:,.2f}"
    )

    # Close both legs reduce-only based on live sizes
    try:
        size_m1 = float(live_pos[m1])
        side_m1 = "SELL" if size_m1 > 0 else "BUY"
        qty_m1 = abs(size_m1)

        size_m2 = float(live_pos[m2])
        side_m2 = "SELL" if size_m2 > 0 else "BUY"
        qty_m2 = abs(size_m2)

        step_m1 = markets.get(m1, {}).get("stepSize")
        step_m2 = markets.get(m2, {}).get("stepSize")
        if step_m1:
            qty_m1 = float(format_number(qty_m1, step_m1))
        if step_m2:
            qty_m2 = float(format_number(qty_m2, step_m2))

        await place_market_order(
            node, indexer, wallet,
            m1, side_m1, qty_m1, markets[m1]["oraclePrice"],
            True, time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
        )
        await asyncio.sleep(0.5)
        await place_market_order(
            node, indexer, wallet,
            m2, side_m2, qty_m2, markets[m2]["oraclePrice"],
            True, time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
        )

        return True
    except Exception as e:
        send_message(f"⚠️ Risk-off close failed: {e}")
        return False
