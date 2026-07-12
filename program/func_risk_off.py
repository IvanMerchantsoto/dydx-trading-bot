"""
func_risk_off.py
================
Cierra el "peor par" bajo condiciones de estrés real del portafolio.

Filosofía de scoring:
  El peor par = el que más pierde Y/O tiene el z más divergido.
  La EDAD no determina quién es el peor par — un par sano y viejo
  no debe cerrarse por haber sobrevivido mucho tiempo.

  score = W_UNREAL * abs(unreal_loss)   ← pérdida monetaria (domina)
        + W_ABS_Z  * abs_z              ← z divergido = tesis rota
        + stale_bonus                   ← pequeño bonus para pares > 24h sin converger

Guardrails:
  - RISK_OFF_MIN_AGE_HOURS: no cerrar pares abiertos hace menos de N horas
  - RISK_OFF_REQUIRE_NEGATIVE_UNREAL: no cerrar pares con PnL positivo salvo emergencia
  - Emergencia: abs_z >= RISK_OFF_EMERGENCY_ABS_Z OR age >= RISK_OFF_EMERGENCY_AGE_HOURS
"""

import json
import os
import asyncio
from datetime import datetime, timezone

from func_messaging import send_message
from func_logging import log_event
from constants import (
    WALLET_ADDRESS,
    RISK_SCORE_W_AGE,       # should be 0.0 — kept for config visibility
    RISK_SCORE_W_ABS_Z,
    RISK_SCORE_W_UNREAL_PNL,
    RISK_OFF_MIN_AGE_HOURS,
    TAKER_FEE_BPS,
    MARKET_MAX_SLIPPAGE_BPS_EXIT,
)
from func_public import get_candles_recent
from func_cointegration import calculate_zscore
from func_utils import format_number
from func_private import place_market_order
from v4_proto.dydxprotocol.clob.order_pb2 import Order

JSON_PATH = os.path.join(os.path.dirname(__file__), "bot_agents.json")

# ── Safety knobs ──────────────────────────────────────────────────────────────
# Only close positive-PnL pairs under emergency conditions.
RISK_OFF_REQUIRE_NEGATIVE_UNREAL = True

# Emergency override: even a profitable/young pair can be closed if:
#   - z is VERY diverged (structural break), or
#   - pair has been open an unusually long time (stale risk)
RISK_OFF_EMERGENCY_ABS_Z = 3.5
RISK_OFF_EMERGENCY_AGE_HOURS = 6.0

# Stale bonus: small score boost for pairs that have been open > 24h without converging.
# Represents "opportunity cost + increased risk of regime change".
# Keeps age OUT of the main score but adds a modest late-stage penalty.
_STALE_THRESHOLD_HOURS = 24.0
_STALE_BONUS_PER_EXTRA_HOUR = 0.5   # 0.5 pts per hour beyond 24h


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


def _load_trades():
    try:
        with open(JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_trades(trades):
    tmp = JSON_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(trades, f, indent=2)
    os.replace(tmp, JSON_PATH)


async def risk_off_close_worst_pair(node, indexer, wallet):
    """
    Close the truly worst pair under portfolio stress.

    Selection logic:
      1. Only LIVE pairs where both legs are on dYdX.
      2. Skip pairs younger than RISK_OFF_MIN_AGE_HOURS (protects fresh entries).
      3. Skip positive-unreal pairs unless emergency conditions.
      4. Score = loss × W_UNREAL + abs_z × W_ABS_Z + stale_bonus.
      5. Close the highest-scoring pair.
    """
    trades = _load_trades()
    if not trades:
        return False

    markets_resp = await indexer.markets.get_perpetual_markets()
    markets = markets_resp.get("markets", {}) or {}

    account_resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
    sub = account_resp.get("subaccount", {}) or {}
    positions = sub.get("openPerpetualPositions", {}) or sub.get("perpetualPositions", {}) or {}

    live_pos = {}
    unreal_by_market = {}
    for m, p in (positions or {}).items():
        size = _sf(p.get("size"))
        if abs(size) > 0:
            live_pos[m] = size
            unreal_by_market[m] = _sf(
                p.get("unrealizedPnl")
                or p.get("unrealizedPnL")
                or p.get("unrealizedProfitLoss")
            )

    now_utc = datetime.now(timezone.utc)
    scored = []

    for t in trades:
        m1 = t.get("market_1")
        m2 = t.get("market_2")
        trace_id = t.get("trace_id")

        if not m1 or not m2:
            continue
        if m1 not in live_pos or m2 not in live_pos:
            continue

        opened_at = _parse_opened_at(t.get("opened_at"))
        if not opened_at:
            continue

        age_hours = (now_utc - opened_at).total_seconds() / 3600.0

        # ── Age guard ─────────────────────────────────────────────────────
        if age_hours < float(RISK_OFF_MIN_AGE_HOURS):
            log_event({
                "type": "risk_off_age_skip",
                "trace_id": trace_id,
                "market_1": m1,
                "market_2": m2,
                "age_hours": round(age_hours, 4),
                "min_age_hours": RISK_OFF_MIN_AGE_HOURS,
            })
            continue

        hedge_ratio = _sf(t.get("hedge_ratio"))
        if hedge_ratio == 0:
            continue

        # ── Current z-score ───────────────────────────────────────────────
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

        # ── Unrealized PnL (sum of both legs) ─────────────────────────────
        unreal = unreal_by_market.get(m1, 0.0) + unreal_by_market.get(m2, 0.0)

        # ── Eligibility check ─────────────────────────────────────────────
        eligible = True
        emergency = False

        if RISK_OFF_REQUIRE_NEGATIVE_UNREAL and unreal >= 0:
            # Positive PnL pair — skip unless emergency
            if absz >= float(RISK_OFF_EMERGENCY_ABS_Z) or age_hours >= float(RISK_OFF_EMERGENCY_AGE_HOURS):
                emergency = True
                eligible = True
            else:
                eligible = False

        if not eligible:
            log_event({
                "type": "risk_off_skipped_positive_unreal",
                "trace_id": trace_id,
                "m1": m1, "m2": m2,
                "unreal": unreal,
                "absz": absz,
                "age_hours": round(age_hours, 3),
            })
            continue

        # ── Score: loss + z-divergence + stale bonus ─────────────────────
        # loss_penalty: how much money is being lost (0 if positive or emergency)
        loss_penalty = max(0.0, -unreal)

        # stale_bonus: small incentive to close pairs that have been open very long
        # Only kicks in after _STALE_THRESHOLD_HOURS — does NOT dominate the score.
        stale_bonus = max(0.0, (age_hours - _STALE_THRESHOLD_HOURS) * _STALE_BONUS_PER_EXTRA_HOUR)

        score = (
            float(RISK_SCORE_W_UNREAL_PNL) * loss_penalty   # e.g. 0.15 × $30 = 4.5
            + float(RISK_SCORE_W_ABS_Z) * absz               # e.g. 5.0 × 2.0 = 10.0
            + stale_bonus                                      # e.g. 0 until 24h, then small
        )

        scored.append({
            "score": score,
            "score_breakdown": {
                "loss_component": float(RISK_SCORE_W_UNREAL_PNL) * loss_penalty,
                "z_component": float(RISK_SCORE_W_ABS_Z) * absz,
                "stale_bonus": stale_bonus,
            },
            "trade": t,
            "trace_id": trace_id,
            "m1": m1,
            "m2": m2,
            "age_hours": age_hours,
            "absz": absz,
            "unreal": unreal,
            "emergency": emergency,
        })

    if not scored:
        log_event({
            "type": "risk_off_skip",
            "reason": "no_eligible_pairs",
            "live_pairs": len(trades),
        })
        return False

    # ── Select worst pair ─────────────────────────────────────────────────────
    scored.sort(key=lambda x: x["score"], reverse=True)
    worst = scored[0]

    m1 = worst["m1"]
    m2 = worst["m2"]
    trace_id = worst["trace_id"]
    age_hours = worst["age_hours"]
    absz = worst["absz"]
    unreal = worst["unreal"]
    score = worst["score"]
    emergency = worst["emergency"]
    breakdown = worst["score_breakdown"]

    msg = (
        f"RISK-OFF: closing {m1}/{m2}\n"
        f"Score: {score:.2f} "
        f"(loss={breakdown['loss_component']:.2f} "
        f"z={breakdown['z_component']:.2f} "
        f"stale={breakdown['stale_bonus']:.2f})\n"
        f"Age: {age_hours:.2f}h | abs(z): {absz:.2f} | Unreal: ${unreal:,.2f}"
    )
    if emergency:
        msg += "\nMode: EMERGENCY OVERRIDE"

    send_message(msg)

    log_event({
        "type": "risk_off_selected",
        "trace_id": trace_id,
        "market_1": m1,
        "market_2": m2,
        "score": score,
        "score_breakdown": breakdown,
        "age_hours": age_hours,
        "abs_z": absz,
        "unreal": unreal,
        "emergency": emergency,
    })

    # ── Execute close ─────────────────────────────────────────────────────────
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

        # Capture close-side market prices for PnL accounting (before placing orders)
        close_px_m1 = _sf(markets.get(m1, {}).get("oraclePrice"))
        close_px_m2 = _sf(markets.get(m2, {}).get("oraclePrice"))

        await place_market_order(
            node, indexer, wallet,
            m1, side_m1, qty_m1, markets[m1]["oraclePrice"],
            True, time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC,
            max_slippage_bps=MARKET_MAX_SLIPPAGE_BPS_EXIT,
        )
        await asyncio.sleep(0.5)
        await place_market_order(
            node, indexer, wallet,
            m2, side_m2, qty_m2, markets[m2]["oraclePrice"],
            True, time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC,
            max_slippage_bps=MARKET_MAX_SLIPPAGE_BPS_EXIT,
        )

        # Remove closed pair from JSON (atomic)
        remaining = []
        removed = False
        closed_record = None
        for row in trades:
            if row.get("market_1") == m1 and row.get("market_2") == m2 and not removed:
                removed = True
                closed_record = row
                continue
            remaining.append(row)
        _save_trades(remaining)

        # ── Synthetic trade_closed event (Bug #2 fix) ─────────────────────
        # risk_off MUST emit a trade_closed so its PnL is counted in
        # session_stats.net_pnl_est_sum. Without this, the bot's accounting
        # ignores all RISK_OFF closes (analysis of 2026-05-20 log: ~$0-50
        # of RISK_OFF closes silently disappeared from realized_pnl_run).
        try:
            from func_pnl import leg_pnl  # local import to avoid circular at module load

            entry1_side = closed_record.get("side_1") or closed_record.get("entry_side_1") if closed_record else None
            entry2_side = closed_record.get("side_2") or closed_record.get("entry_side_2") if closed_record else None
            entry1_price = _sf(closed_record.get("price_1") or closed_record.get("entry_price_1"), 0.0) if closed_record else 0.0
            entry2_price = _sf(closed_record.get("price_2") or closed_record.get("entry_price_2"), 0.0) if closed_record else 0.0
            entry1_size = _sf(closed_record.get("size_1") or closed_record.get("entry_size_1"), 0.0) if closed_record else 0.0
            entry2_size = _sf(closed_record.get("size_2") or closed_record.get("entry_size_2"), 0.0) if closed_record else 0.0

            pnl_gross_1 = pnl_gross_2 = 0.0
            if closed_record and entry1_side and entry2_side and entry1_price > 0 and entry2_price > 0:
                pnl_gross_1 = leg_pnl(entry1_side, entry1_price, close_px_m1, entry1_size)
                pnl_gross_2 = leg_pnl(entry2_side, entry2_price, close_px_m2, entry2_size)
            # Fallback: use indexer unrealizedPnl that we already have if entry data missing
            pnl_gross_total = pnl_gross_1 + pnl_gross_2 if (pnl_gross_1 or pnl_gross_2) else float(unreal)

            open_fees_paid = (_sf((closed_record or {}).get("fee_1", 0.0))
                              + _sf((closed_record or {}).get("fee_2", 0.0)))
            # Estimate close fees from notional × taker fee bps
            close_notional = (qty_m1 * close_px_m1 if close_px_m1 > 0 else 0.0) \
                           + (qty_m2 * close_px_m2 if close_px_m2 > 0 else 0.0)
            close_fees_est = close_notional * float(TAKER_FEE_BPS)

            net_pnl_est = pnl_gross_total - open_fees_paid - close_fees_est

            # fee_estimated flag from record (legacy positions or testnet)
            fee_estimated_flag = bool(
                (closed_record or {}).get("fee_1_estimated", False)
                or (closed_record or {}).get("fee_2_estimated", False)
            )

            log_event({
                "type": "trade_closed",
                "trace_id": trace_id,
                "market_1": m1,
                "market_2": m2,
                "close_reason": (
                    f"RISK_OFF: score={score:.2f} | loss_component={breakdown['loss_component']:.2f} "
                    f"| z_component={breakdown['z_component']:.2f} | stale_bonus={breakdown['stale_bonus']:.2f} "
                    f"| emergency={emergency} | unreal=${unreal:.2f} | abs_z={absz:.2f} | age={age_hours:.2f}h"
                ),
                "close_type_m1": "taker",
                "close_type_m2": "taker",
                "pnl_gross": pnl_gross_total,
                "open_fees": open_fees_paid,
                "close_fees_est": close_fees_est,
                "net_pnl_est": net_pnl_est,
                "fee_estimated": fee_estimated_flag,
                "pnl_provisional": fee_estimated_flag,
                "z_now": None,  # z was computed earlier in the scoring loop but not retained per-trade here
                "z_entry": _sf((closed_record or {}).get("z_score", 0.0)),
                "age_hours": age_hours,
                "synthetic": True,
                "source": "risk_off",
            })
        except Exception as tc_err:
            log_event({
                "type": "trade_closed_emit_error",
                "trace_id": trace_id,
                "market_1": m1, "market_2": m2,
                "source": "risk_off",
                "error": str(tc_err),
            })
            # Don't fail the close just because the event emission failed
            net_pnl_est = float(unreal)  # best-effort fallback for caller

        log_event({
            "type": "risk_off_closed",
            "trace_id": trace_id,
            "market_1": m1,
            "market_2": m2,
            "unreal": unreal,
            "score": score,
            "removed_from_json": removed,
        })

        send_message(
            f"🛑 RISK-OFF closed: {m1}/{m2}\n"
            f"Unreal: ${unreal:,.2f} | Net est (incl fees): ${net_pnl_est:+.2f}\n"
            f"Reason: score={score:.2f} loss={breakdown['loss_component']:.2f} z={breakdown['z_component']:.2f}"
        )
        # Return a dict so main.py can accumulate to session_stats
        return {
            "closed": True,
            "trace_id": trace_id,
            "market_1": m1,
            "market_2": m2,
            "net_pnl_est": float(net_pnl_est),
            "unreal": float(unreal),
        }

    except Exception as e:
        log_event({
            "type": "risk_off_close_failed",
            "trace_id": trace_id,
            "market_1": m1,
            "market_2": m2,
            "error": str(e),
        })
        send_message(f"Risk-off close FAILED: {m1}/{m2} | Error: {e}")
        return False
