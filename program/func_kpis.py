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
from func_logging import log_event
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

        # ── Leverage and pair count (derived) ─────────────────────────────
        leverage_used = (notional / equity) if equity > 0 else 0.0

        # ── Bug #5 fix: emit kpi_snapshot as a structured JSON event ──────
        # This is the audit-time source of truth for equity/PnL trajectory.
        # audit_run.py and SOP procedure §7.1 consume this event.
        log_event({
            "type": "kpi_snapshot",
            "equity": equity,
            "free_collateral": free,
            "margin_reported": margin_reported,
            "used_collateral_est": used_collateral_est,
            "used_collateral_ratio": used_collateral_ratio,
            "notional_gross": notional,
            "leverage_used": leverage_used,
            "unrealized_pnl": unreal,
            "open_legs_exchange": int(open_legs_exchange),
            "open_pairs_exchange_est": float(open_pairs_exchange_est),
            "open_pairs_json": tracked_pairs_json,
            "tracked_markets_json": tracked_markets_json_count,
            "needs_reconcile": reconcile_required,
            "orphan_count": orphan_count,
            "drift_legs": drift_legs,
            "drift_flag": bool(drift_flag),
        }, print_terminal=False)

        if send_telegram:
            msg = (
                "📊 *DYDX Snapshot*\n"
                f"Equity: ${equity:,.2f}\n"
                f"Free collateral: ${free:,.2f}\n"
                f"Used collateral (est): ${used_collateral_est:,.2f} ({used_collateral_ratio*100:.1f}% of equity)\n"
                f"Open notional (oracle): ${notional:,.2f}\n"
                f"Leverage used: {leverage_used:.2f}x\n"
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
            "leverage_used": leverage_used,
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


async def send_positions_status(indexer) -> None:
    """
    2026-07-10 NUEVO: envía un mensaje Telegram con detalle POR PAR:
      - Markets del par
      - entry_z (z al abrir)
      - best_z (mejor z visto desde apertura — más cerca de 0 mejor)
      - z_now (aprox del PnL: si tenemos oracles, calc PnL)
      - unreal_pnl actual
      - edad

    Ayuda a decidir manualmente si esperar o cerrar cada par.

    Fuente:
      - bot_agents.json (entry_z, best_z, opened_at)
      - dYdX indexer (unrealizedPnl actual, positions)
    """
    from datetime import datetime, timezone

    try:
        with open(JSON_PATH, "r") as f:
            pairs = json.load(f) or []
        pairs = [p for p in pairs if isinstance(p, dict)]

        if not pairs:
            # No mandar mensaje si no hay pares abiertos (evita spam)
            return

        # Fetch positions del indexer
        try:
            resp = await indexer.account.get_subaccount(WALLET_ADDRESS.strip(), 0)
            sub = resp.get("subaccount", {}) or {}
            live_positions = sub.get("openPerpetualPositions", {}) or {}
            equity = _to_float(sub.get("equity"), 0.0)
            free = _to_float(sub.get("freeCollateral"), 0.0)
        except Exception:
            live_positions = {}
            equity = free = 0.0

        now = datetime.now(timezone.utc)
        lines = ["📍 *Estado de pares abiertos*", ""]

        total_unreal = 0.0
        for i, p in enumerate(pairs, 1):
            m1 = p.get("market_1", "?")
            m2 = p.get("market_2", "?")
            entry_z = _to_float(p.get("z_score"), 0.0)
            best_z = _to_float(p.get("best_z"), abs(entry_z))
            status = p.get("pair_status", "?")

            # Age
            opened_at_str = p.get("opened_at", "")
            age_str = "?"
            try:
                if opened_at_str:
                    opened = datetime.fromisoformat(opened_at_str.replace("Z", "+00:00"))
                    age_h = (now - opened).total_seconds() / 3600.0
                    if age_h < 1:
                        age_str = f"{age_h*60:.0f}min"
                    else:
                        age_str = f"{age_h:.1f}h"
            except Exception:
                pass

            # Unreal PnL de cada leg (sum)
            u1 = _to_float(live_positions.get(m1, {}).get("unrealizedPnl"), 0.0)
            u2 = _to_float(live_positions.get(m2, {}).get("unrealizedPnl"), 0.0)
            unreal = u1 + u2
            total_unreal += unreal

            emoji = "🟢" if unreal > 0 else ("🔴" if unreal < -0.5 else "🟡")
            lines.append(
                f"{emoji} *{m1}/{m2}* ({age_str}) [{status}]"
            )
            lines.append(
                f"   z_entry={entry_z:+.2f}  best_z={best_z:.2f}  "
                f"unreal=${unreal:+.2f}"
            )
            lines.append("")

        lines.append(f"💰 Equity: ${equity:.2f}  Free: ${free:.2f}")
        lines.append(f"📊 Total unreal: ${total_unreal:+.2f}")

        msg = "\n".join(lines)
        send_message(msg)

        log_event({
            "type": "positions_status_sent",
            "n_pairs": len(pairs),
            "total_unreal": round(total_unreal, 3),
            "equity": equity,
        }, print_terminal=False)

    except Exception as e:
        log_event({
            "type": "positions_status_error",
            "error": str(e),
        })