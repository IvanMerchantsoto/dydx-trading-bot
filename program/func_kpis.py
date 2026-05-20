# func_kpis.py
"""
Cleaner account KPIs for dYdX v4.

What this does:
- reads the real account snapshot from dYdX
- values open positions with oracle prices
- compares exchange exposure vs bot_agents.json tracking
- sends ONE compact message
- avoids noisy / misleading fields such as marginUsed when it is unreliable
"""

from __future__ import annotations

from typing import Any, Dict, Optional
import json
import os

from func_messaging import send_message
from constants import WALLET_ADDRESS

JSON_PATH = os.path.join(os.path.dirname(__file__), "bot_agents.json")


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() in {"nan", "none", "null"}:
            return default
        return float(s)
    except Exception:
        return default


def _safe_div(a: float, b: float, default: float = 0.0) -> float:
    try:
        return a / b if b else default
    except Exception:
        return default


def _load_bot_agents() -> list:
    try:
        with open(JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


async def send_account_kpis(indexer, send_telegram: bool = True) -> Optional[Dict[str, float]]:
    """
    Fetch account KPIs from dYdX and optionally send a compact Telegram message.

    Returns:
        dict with metrics used by main.py
    """
    try:
        # 1) Subaccount snapshot
        resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
        sub = resp.get("subaccount", {}) if isinstance(resp, dict) else {}

        equity = _to_float(sub.get("equity"))
        free = _to_float(sub.get("freeCollateral"))

        # This endpoint can be unreliable on some sessions; keep it for return only.
        margin_reported = _to_float(sub.get("marginUsed"))

        # 2) Open positions
        positions = sub.get("openPerpetualPositions") or sub.get("perpetualPositions") or {}
        if not isinstance(positions, dict):
            positions = {}

        # 3) Market prices
        try:
            mk_resp = await indexer.markets.get_perpetual_markets()
            markets = mk_resp.get("markets", {}) if isinstance(mk_resp, dict) else {}
        except Exception:
            markets = {}

        notional = 0.0
        unreal = 0.0
        open_legs_exchange = 0
        open_markets_exchange = []

        DUST_SIZE = 1e-9

        for market, p in positions.items():
            if not isinstance(p, dict):
                continue

            size = _to_float(p.get("size"))
            if abs(size) <= DUST_SIZE:
                continue

            open_legs_exchange += 1
            open_markets_exchange.append(market)

            unreal += _to_float(
                p.get("unrealizedPnl")
                or p.get("unrealizedPnL")
                or p.get("unrealizedProfitLoss")
            )

            m = markets.get(market, {}) if isinstance(markets, dict) else {}
            px = _to_float(m.get("oraclePrice"))

            if px <= 0:
                px = _to_float(p.get("markPrice"))
            if px <= 0:
                px = _to_float(p.get("entryPrice"))

            if px > 0:
                notional += abs(size) * px

        # 4) Compare against bot_agents.json
        bot_agents = _load_bot_agents()
        tracked_pairs_json = len(bot_agents)

        tracked_markets_json = set()
        reconcile_required = 0
        orphan_count = 0

        for row in bot_agents:
            if not isinstance(row, dict):
                continue
            m1 = row.get("market_1")
            m2 = row.get("market_2")
            if m1:
                tracked_markets_json.add(m1)
            if m2:
                tracked_markets_json.add(m2)

            if bool(row.get("needs_reconcile")):
                reconcile_required += 1
            if str(row.get("pair_status", "")).upper() == "ORPHAN":
                orphan_count += 1

        tracked_markets_json_count = len(tracked_markets_json)
        open_pairs_exchange_est = open_legs_exchange / 2.0

        # 5) More useful derived metrics
        used_collateral_est = max(0.0, equity - free)
        used_collateral_ratio = _safe_div(used_collateral_est, equity, 0.0)

        # exchange vs JSON drift
        drift_legs = open_legs_exchange - tracked_markets_json_count
        drift_flag = abs(drift_legs) >= 2

        if send_telegram:
            msg = (
                "📊 *DYDX Snapshot*\n"
                f"Equity: ${equity:,.2f}\n"
                f"Free collateral: ${free:,.2f}\n"
                f"Used collateral (est): ${used_collateral_est:,.2f} ({used_collateral_ratio*100:.1f}% of equity)\n"
                f"Open notional (oracle): ${notional:,.2f}\n"
                f"Unrealized PnL: ${unreal:,.2f}\n"
                f"Exchange open legs: {open_legs_exchange}\n"
                f"Tracked pairs (JSON): {tracked_pairs_json}\n"
                f"Needs reconcile: {reconcile_required}\n"
            )

            if orphan_count > 0:
                msg += f"Orphans: {orphan_count}\n"

            if drift_flag:
                msg += f"⚠️ Drift exchange-vs-JSON: {drift_legs:+.0f} legs\n"

            send_message(msg)

        return {
            "equity": equity,
            "free": free,
            "margin_reported": margin_reported,
            "used_collateral_est": used_collateral_est,
            "used_collateral_ratio": used_collateral_ratio,
            "notional": notional,
            "unreal": unreal,
            "open_legs_exchange": float(open_legs_exchange),
            "open_pairs_exchange_est": float(open_pairs_exchange_est),
            "tracked_pairs_json": float(tracked_pairs_json),
            "tracked_markets_json": float(tracked_markets_json_count),
            "reconcile_required": float(reconcile_required),
            "orphan_count": float(orphan_count),
            "drift_legs": float(drift_legs),
        }

    except Exception as e:
        if send_telegram:
            send_message(f"⚠️ KPI error: {e}")
        return None