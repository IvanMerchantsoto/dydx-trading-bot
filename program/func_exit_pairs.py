from func_messaging import send_message
from func_pnl import leg_pnl
from constants import (
    WALLET_ADDRESS,
    CLOSE_AT_ZSCORE_CROSS,
    USE_Z_TP, USE_Z_SL, USE_TIME_STOP,
    Z_TP, Z_SL_DELTA, TIME_STOP_HOURS,
    USE_MIN_PROFIT_TP,
    MIN_PROFIT_PCT as CONST_MIN_PROFIT_PCT,
    MIN_PROFIT_USD as CONST_MIN_PROFIT_USD,
    DROP_STALE_RECORDS,
    HARD_SL_USD, HARD_SL_PCT,
    TP_CONFIRM_CHECKS,
    MIN_HOLD_MINUTES_FOR_TP,
)
from func_utils import format_number
from func_public import get_candles_recent
from func_cointegration import calculate_zscore
from func_private import place_market_order, close_pair_maker_with_fallback
from constants import MAKER_EXIT_ENABLED, MAKER_EXIT_TIMEOUT_S
from func_logging import log_event
from v4_proto.dydxprotocol.clob.order_pb2 import Order

import json
import asyncio
import os
from datetime import datetime, timezone

JSON_PATH = os.path.join(os.path.dirname(__file__), "bot_agents.json")

# ─── Hard Stop-Loss ──────────────────────────────────────────────────────────
# Pulled from constants.py so they're in one place.
USE_HARD_SL = True
# HARD_SL_USD and HARD_SL_PCT imported from constants

# ─── TP behavior ─────────────────────────────────────────────────────────────
# MIN_HOLD_MINUTES_FOR_TP = 0  → no minimum hold time (from constants)
# TP_CONFIRM_CHECKS = 2        → z must be in TP zone 2 consecutive checks

USE_TP_HYSTERESIS = False
Z_TP_IN = float(Z_TP)
Z_TP_OUT = float(Z_TP) + 0.15

AUTO_CLOSE_SINGLE_LEG_ORPHANS = True
KEEP_RECONCILE_RECORDS = True

# Fee estimation for close leg (taker 0.05% per leg)
TAKER_FEE_BPS = 0.0005


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


def _has_pnl_fields(position: dict) -> bool:
    side_1 = position.get("side_1") or position.get("entry_side_1")
    side_2 = position.get("side_2") or position.get("entry_side_2")
    p1 = _sf(position.get("price_1") or position.get("entry_price_1"), 0.0)
    p2 = _sf(position.get("price_2") or position.get("entry_price_2"), 0.0)
    s1 = _sf(position.get("size_1") or position.get("entry_size_1"), 0.0)
    s2 = _sf(position.get("size_2") or position.get("entry_size_2"), 0.0)
    return (
        side_1 in ("BUY", "SELL")
        and side_2 in ("BUY", "SELL")
        and p1 > 0 and p2 > 0
        and s1 >= 0 and s2 >= 0
    )


def _compute_notional(position: dict) -> float:
    p1 = _sf(position.get("price_1") or position.get("entry_price_1"), 0.0)
    p2 = _sf(position.get("price_2") or position.get("entry_price_2"), 0.0)
    s1 = _sf(position.get("size_1") or position.get("entry_size_1"), 0.0)
    s2 = _sf(position.get("size_2") or position.get("entry_size_2"), 0.0)
    n1 = abs(p1 * s1) if p1 > 0 and s1 > 0 else 0.0
    n2 = abs(p2 * s2) if p2 > 0 and s2 > 0 else 0.0
    return n1 + n2


def _estimate_close_fees(live_pos: dict, markets: dict, m1: str, m2: str) -> float:
    """
    Estimate fees for closing both legs using current oracle prices.
    Uses TAKER_FEE_BPS (0.05%) as conservative estimate.
    """
    size_m1 = abs(_sf(live_pos.get(m1, 0.0)))
    size_m2 = abs(_sf(live_pos.get(m2, 0.0)))
    px_m1 = _sf(markets.get(m1, {}).get("oraclePrice"), 0.0)
    px_m2 = _sf(markets.get(m2, {}).get("oraclePrice"), 0.0)
    return (size_m1 * px_m1 + size_m2 * px_m2) * TAKER_FEE_BPS


def _profit_gate(
    pnl_gross: float,
    notional: float,
    open_fees_paid: float = 0.0,
    close_fees_est: float = 0.0,
) -> tuple[bool, float, float]:
    """
    Returns (passes_gate, min_required_gross, net_pnl_est).

    pnl_gross      : unrealized PnL proxy (no fees)
    notional       : total notional of both legs (for % threshold)
    open_fees_paid : actual fees paid at entry (from position record)
    close_fees_est : estimated fees to close (oracle price × TAKER_FEE_BPS)

    The gate requires gross PnL to exceed:
        max(MIN_PROFIT_USD, MIN_PROFIT_PCT × notional) + total_fees
    so that NET profit after all fees is ≥ the configured minimum.
    """
    total_fees = open_fees_paid + close_fees_est
    net_pnl = pnl_gross - total_fees

    if USE_MIN_PROFIT_TP:
        # Target net profit (before fees are added back)
        target_net = max(float(CONST_MIN_PROFIT_USD), float(CONST_MIN_PROFIT_PCT) * float(notional))
        # Gross must cover target net + all fees
        min_gross_required = target_net + total_fees
        passes = pnl_gross >= min_gross_required
        return passes, min_gross_required, net_pnl

    # If USE_MIN_PROFIT_TP is False: just require covering fees (break-even or better)
    passes = net_pnl >= 0.0
    return passes, total_fees, net_pnl


def _hard_sl_level(notional: float) -> float:
    if notional <= 0:
        return float(HARD_SL_USD)
    return max(float(HARD_SL_USD), float(HARD_SL_PCT) * float(notional))


def _format_close_side(size):
    return "SELL" if float(size) > 0 else "BUY"


async def _close_single_live_leg(node, indexer, wallet, markets, market, live_size, reason, trace_id=None):
    close_side = _format_close_side(live_size)
    close_size = abs(float(live_size))

    step = markets.get(market, {}).get("stepSize")
    if step:
        close_size = float(format_number(close_size, step))

    await place_market_order(
        node,
        indexer,
        wallet,
        market,
        close_side,
        close_size,
        markets[market]["oraclePrice"],
        True,
        time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
    )

    log_event({
        "type": "single_leg_cleanup",
        "trace_id": trace_id,
        "market": market,
        "close_side": close_side,
        "close_size": close_size,
        "reason": reason,
    })


async def manage_trade_exits(node, indexer, wallet):
    try:
        with open(JSON_PATH, "r") as f:
            open_positions_list = json.load(f)
    except Exception:
        return "complete"

    if not open_positions_list:
        return "complete"

    markets_resp = await indexer.markets.get_perpetual_markets()
    markets = markets_resp.get("markets", {}) or {}

    try:
        account_resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
        subaccount = account_resp.get("subaccount", {}) or {}
        positions = (
            subaccount.get("openPerpetualPositions", {})
            or subaccount.get("perpetualPositions", {})
            or {}
        )
    except Exception as e:
        log_event({
            "type": "exit_failure",
            "close_reason": "read_subaccount",
            "error": str(e),
        })
        return "error"

    # Build live position map: market → signed size
    live_pos = {}
    for m, pdata in (positions or {}).items():
        sz = _sf(pdata.get("size", 0.0))
        if abs(sz) > 0:
            live_pos[m] = sz

    now_utc = datetime.now(timezone.utc)
    save_output = []

    closed_count = 0
    kept_count = 0
    dropped_count = 0
    malformed_count = 0
    zcalc_error_count = 0
    exit_error_count = 0
    tp_blocked_profit_count = 0
    tp_blocked_missing_pnl_count = 0
    time_blocked_profit_count = 0   # kept for compatibility, usually 0 now
    tp_blocked_age_count = 0
    tp_blocked_confirm_count = 0
    tp_loss_exit_count = 0
    single_leg_cleanup_count = 0

    # Per-loop PnL accumulators (returned to main.py for session stats)
    tp_close_count = 0
    hard_sl_close_count = 0
    z_sl_close_count = 0
    net_pnl_est_sum = 0.0    # sum of net_pnl_est for all managed closes (not orphan legs)

    for position in open_positions_list:
        m1 = position.get("market_1")
        m2 = position.get("market_2")
        pair_status = position.get("pair_status")
        trace_id = position.get("trace_id")

        if not m1 or not m2:
            malformed_count += 1
            continue

        m1_live = m1 in live_pos
        m2_live = m2 in live_pos

        # ── Single-leg orphan cleanup ─────────────────────────────────────
        if m1_live != m2_live:
            if AUTO_CLOSE_SINGLE_LEG_ORPHANS:
                try:
                    live_market = m1 if m1_live else m2
                    live_size = live_pos[live_market]

                    await _close_single_live_leg(
                        node=node, indexer=indexer, wallet=wallet,
                        markets=markets, market=live_market,
                        live_size=live_size,
                        reason=f"single_leg_orphan pair={m1}/{m2} status={pair_status}",
                        trace_id=trace_id,
                    )

                    single_leg_cleanup_count += 1
                    closed_count += 1

                    send_message(
                        f"ORPHAN LEG CLOSED: {live_market}\n"
                        f"Pair: {m1}/{m2} | Status: {pair_status}"
                    )
                    log_event({
                        "type": "reconcile_closed_single_leg",
                        "trace_id": trace_id,
                        "market_1": m1,
                        "market_2": m2,
                        "pair_status": pair_status,
                        "m1_live": m1_live,
                        "m2_live": m2_live,
                    })
                    continue

                except Exception as e:
                    exit_error_count += 1
                    log_event({
                        "type": "exit_failure",
                        "trace_id": trace_id,
                        "market_1": m1,
                        "market_2": m2,
                        "close_reason": "single_leg_orphan_cleanup",
                        "error": str(e),
                    })
                    if KEEP_RECONCILE_RECORDS:
                        kept_count += 1
                        save_output.append(position)
                    else:
                        dropped_count += 1
                    continue
            else:
                if KEEP_RECONCILE_RECORDS:
                    kept_count += 1
                    save_output.append(position)
                else:
                    dropped_count += 1
                continue

        # ── Both legs gone → stale record ────────────────────────────────
        if (not m1_live) and (not m2_live):
            if DROP_STALE_RECORDS:
                dropped_count += 1
                continue
            else:
                kept_count += 1
                save_output.append(position)
                continue

        # ── Both legs live: evaluate exit conditions ──────────────────────
        z_entry = _sf(position.get("z_score", 0.0))

        opened_at = _parse_opened_at(position.get("opened_at"))
        age_min = None
        age_hours = None
        if opened_at is not None:
            age_min = (now_utc - opened_at).total_seconds() / 60.0
            age_hours = (now_utc - opened_at).total_seconds() / 3600.0

        # Current z-score
        z_now = None
        try:
            series_1 = await get_candles_recent(indexer, m1)
            series_2 = await get_candles_recent(indexer, m2)

            n = min(len(series_1), len(series_2))
            if n > 25:
                series_1 = series_1[-n:]
                series_2 = series_2[-n:]
                h = _sf(position.get("hedge_ratio"), 1.0)
                spread = series_1 - (h * series_2)
                z_now = float(calculate_zscore(spread).values.tolist()[-1])
        except Exception:
            z_now = None
            zcalc_error_count += 1

        # PnL calculation (proxy using oracle prices)
        can_pnl = _has_pnl_fields(position)

        entry1_side = position.get("side_1") or position.get("entry_side_1")
        entry2_side = position.get("side_2") or position.get("entry_side_2")
        entry1_price = _sf(position.get("price_1") or position.get("entry_price_1"), 0.0)
        entry2_price = _sf(position.get("price_2") or position.get("entry_price_2"), 0.0)
        entry1_size = _sf(position.get("size_1") or position.get("entry_size_1"), 0.0)
        entry2_size = _sf(position.get("size_2") or position.get("entry_size_2"), 0.0)

        exit1_price = _sf(markets.get(m1, {}).get("oraclePrice"), 0.0)
        exit2_price = _sf(markets.get(m2, {}).get("oraclePrice"), 0.0)

        pnl1 = pnl2 = pnl_gross = 0.0
        if can_pnl and exit1_price > 0 and exit2_price > 0:
            pnl1 = leg_pnl(entry1_side, entry1_price, exit1_price, entry1_size)
            pnl2 = leg_pnl(entry2_side, entry2_price, exit2_price, entry2_size)
            pnl_gross = pnl1 + pnl2

        total_notional = _compute_notional(position)

        # Fee accounting: open fees from record + close fee estimate.
        # Old positions opened before the fee-tracking fix have fee_1=fee_2=0.
        # In that case, estimate open fees using TAKER_FEE_BPS × open notional
        # so the profit gate isn't too lenient on legacy records.
        open_fees_paid = _sf(position.get("fee_1", 0.0)) + _sf(position.get("fee_2", 0.0))
        if open_fees_paid == 0.0 and total_notional > 0.0:
            # Estimate: ~0.05% per leg = 0.05% × (notional/2) × 2 legs = 0.05% × notional
            open_fees_paid = total_notional * TAKER_FEE_BPS
        close_fees_est = _estimate_close_fees(live_pos, markets, m1, m2)

        ok_profit, min_gross_required, net_pnl_est = _profit_gate(
            pnl_gross, total_notional, open_fees_paid, close_fees_est
        )

        is_close = False
        close_reason = None

        # ── 1. Hard monetary SL ───────────────────────────────────────────
        if (not is_close) and USE_HARD_SL and can_pnl:
            hard_level = _hard_sl_level(total_notional)
            if pnl_gross <= -hard_level:
                is_close = True
                close_reason = (
                    f"HARD_SL: pnl_gross={pnl_gross:.2f} <= -{hard_level:.2f} "
                    f"| net_est={net_pnl_est:.2f}"
                )

        # ── 2. Z-Score Stop-Loss ──────────────────────────────────────────
        if (not is_close) and USE_Z_SL and (z_now is not None):
            sl_level = abs(z_entry) + float(Z_SL_DELTA)
            if abs(z_now) >= sl_level:
                is_close = True
                close_reason = (
                    f"Z_SL: abs(z={z_now:.3f}) >= {sl_level:.3f} "
                    f"| pnl_gross={pnl_gross:.2f} | net_est={net_pnl_est:.2f}"
                )

        # ── 3. Take-Profit (z reversion + double confirmation + fee gate) ─
        tp_zone = False
        if USE_Z_TP and (z_now is not None):
            if USE_TP_HYSTERESIS:
                prev_in_zone = bool(position.get("tp_in_zone", False))
                if (not prev_in_zone) and abs(z_now) <= float(Z_TP_IN):
                    tp_zone = True
                elif prev_in_zone and abs(z_now) <= float(Z_TP_OUT):
                    tp_zone = True
                position["tp_in_zone"] = tp_zone
            else:
                tp_zone = abs(z_now) <= float(Z_TP)

        tp_confirm = int(position.get("tp_confirm", 0) or 0)

        if (not is_close) and tp_zone:
            # Optional minimum hold time (0 by default = disabled)
            if MIN_HOLD_MINUTES_FOR_TP and (age_min is not None) and (age_min < float(MIN_HOLD_MINUTES_FOR_TP)):
                tp_blocked_age_count += 1
                tp_confirm = 0
            else:
                tp_confirm += 1
        else:
            tp_confirm = 0

        position["tp_confirm"] = tp_confirm

        if (not is_close) and tp_zone:
            if tp_confirm < int(TP_CONFIRM_CHECKS):
                # Still building up confirmations — keep waiting
                tp_blocked_confirm_count += 1
            else:
                if not can_pnl:
                    tp_blocked_missing_pnl_count += 1
                else:
                    if ok_profit:
                        is_close = True
                        close_reason = (
                            f"TP: abs(z={z_now:.3f}) <= {Z_TP} "
                            f"confirms={tp_confirm} "
                            f"| pnl_gross={pnl_gross:.2f} "
                            f"| fees={open_fees_paid + close_fees_est:.2f} "
                            f"| net_est={net_pnl_est:.2f} "
                            f"| min_gross={min_gross_required:.2f}"
                        )
                    elif pnl_gross < 0.0:
                        # ── TP_LOSS_EXIT: z has mean-reverted but position is losing ──
                        # The spread thesis is done. Holding longer CANNOT improve gross PnL
                        # if z already reversed — it only bleeds more fees and exposes us to
                        # further price divergence. Cut the loss now.
                        is_close = True
                        tp_loss_exit_count += 1
                        close_reason = (
                            f"TP_LOSS_EXIT: z reverted abs(z={z_now:.3f}) <= {Z_TP} "
                            f"confirms={tp_confirm} "
                            f"| pnl_gross={pnl_gross:.2f} (negative — cutting loss) "
                            f"| fees={open_fees_paid + close_fees_est:.2f} "
                            f"| net_est={net_pnl_est:.2f}"
                        )
                        log_event({
                            "type": "tp_loss_exit",
                            "trace_id": trace_id,
                            "m1": m1, "m2": m2,
                            "pnl_gross": pnl_gross,
                            "net_est": net_pnl_est,
                            "open_fees": open_fees_paid,
                            "close_fees_est": close_fees_est,
                            "z_now": z_now,
                            "z_entry": z_entry,
                            "age_hours": age_hours,
                        })
                    else:
                        # pnl_gross > 0 but not enough to cover fees + min net profit yet.
                        # Keep waiting — the spread might improve further.
                        tp_blocked_profit_count += 1
                        log_event({
                            "type": "tp_blocked_fees",
                            "trace_id": trace_id,
                            "m1": m1, "m2": m2,
                            "pnl_gross": pnl_gross,
                            "net_est": net_pnl_est,
                            "min_gross_required": min_gross_required,
                            "open_fees": open_fees_paid,
                            "close_fees_est": close_fees_est,
                        })

        # ── 4. Time Stop (optional — disabled by default) ─────────────────
        if (not is_close) and USE_TIME_STOP and (age_hours is not None):
            if age_hours >= float(TIME_STOP_HOURS):
                # Unconditional: thesis invalidated after TIME_STOP_HOURS
                is_close = True
                close_reason = (
                    f"TIME_STOP: age={age_hours:.2f}h >= {TIME_STOP_HOURS:.2f}h "
                    f"| pnl_gross={pnl_gross:.2f} | net_est={net_pnl_est:.2f}"
                )

        if not is_close:
            kept_count += 1
            save_output.append(position)
            continue

        # ── Execute close ─────────────────────────────────────────────────
        try:
            size_m1 = float(live_pos[m1])
            close_side_m1 = "SELL" if size_m1 > 0 else "BUY"
            close_size_m1 = abs(size_m1)

            size_m2 = float(live_pos[m2])
            close_side_m2 = "SELL" if size_m2 > 0 else "BUY"
            close_size_m2 = abs(size_m2)

            step_m1 = markets.get(m1, {}).get("stepSize")
            step_m2 = markets.get(m2, {}).get("stepSize")
            if step_m1:
                close_size_m1 = float(format_number(close_size_m1, step_m1))
            if step_m2:
                close_size_m2 = float(format_number(close_size_m2, step_m2))

            # Determine if this is a TP exit (eligible for maker close to save fees)
            is_tp_exit = close_reason is not None and close_reason.startswith("TP:")
            use_maker = MAKER_EXIT_ENABLED and is_tp_exit

            log_event({
                "type": "trade_close_signal",
                "trace_id": trace_id,
                "market_1": m1,
                "market_2": m2,
                "close_reason": close_reason,
                "close_method": "maker_limit" if use_maker else "market_ioc",
                "z_now": z_now,
                "z_entry": z_entry,
                "pnl_gross": pnl_gross,
                "open_fees": open_fees_paid,
                "close_fees_est": close_fees_est,
                "net_pnl_est": net_pnl_est,
                "min_gross_required": min_gross_required,
                "age_hours": age_hours,
            })

            close_type_m1 = "taker"
            close_type_m2 = "taker"

            if use_maker:
                # ── TP: try POST_ONLY maker orders, fall back to MARKET ────
                res_m1, res_m2 = await close_pair_maker_with_fallback(
                    node, indexer, wallet,
                    m1, close_side_m1, close_size_m1,
                    m2, close_side_m2, close_size_m2,
                    markets,
                    timeout_s=float(MAKER_EXIT_TIMEOUT_S),
                    trace_id=trace_id,
                )
                close_type_m1 = res_m1.get("close_type", "taker_fallback")
                close_type_m2 = res_m2.get("close_type", "taker_fallback")

                # Accumulate real fees if available from maker fills
                actual_close_fees = (
                    _sf(res_m1.get("fee_est", 0.0)) + _sf(res_m2.get("fee_est", 0.0))
                )
                if actual_close_fees > 0:
                    close_fees_est = actual_close_fees
                    net_pnl_est = pnl_gross - (open_fees_paid + close_fees_est)

            else:
                # ── SL / HARD_SL: always MARKET for speed ─────────────────
                await place_market_order(
                    node, indexer, wallet,
                    m1, close_side_m1, close_size_m1, markets[m1]["oraclePrice"],
                    True,
                    time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
                )
                await asyncio.sleep(0.5)
                await place_market_order(
                    node, indexer, wallet,
                    m2, close_side_m2, close_size_m2, markets[m2]["oraclePrice"],
                    True,
                    time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
                )

            closed_count += 1

            # ── Accumulate per-type stats (for session summary) ───────────
            if close_reason and close_reason.startswith("TP"):
                tp_close_count += 1
            elif close_reason and "HARD_SL" in close_reason:
                hard_sl_close_count += 1
            elif close_reason and "Z_SL" in close_reason:
                z_sl_close_count += 1
            net_pnl_est_sum += net_pnl_est

            # ── Rich close notification ────────────────────────────────────
            age_str = f"{age_hours:.2f}h" if age_hours is not None else "?"
            z_str = f"{z_now:.3f}" if z_now is not None else "?"
            total_fees_str = f"{open_fees_paid + close_fees_est:.2f}"
            emoji = "✅" if net_pnl_est >= 0 else "🔴"
            maker_tag = " [maker]" if use_maker and "maker" in (close_type_m1, close_type_m2) else ""
            send_message(
                f"{emoji} CLOSED{maker_tag} {m1}/{m2}\n"
                f"Reason: {close_reason}\n"
                f"PnL gross: ${pnl_gross:.2f} | Fees: ~${total_fees_str} | Net est: ${net_pnl_est:.2f}\n"
                f"Age: {age_str} | Z now: {z_str} | Z entry: {z_entry:.3f}\n"
                f"Close: {close_type_m1}/{close_type_m2}"
            )

            log_event({
                "type": "trade_closed",
                "trace_id": trace_id,
                "market_1": m1,
                "market_2": m2,
                "close_reason": close_reason,
                "close_type_m1": close_type_m1,
                "close_type_m2": close_type_m2,
                "pnl_gross": pnl_gross,
                "open_fees": open_fees_paid,
                "close_fees_est": close_fees_est,
                "net_pnl_est": net_pnl_est,
                "z_now": z_now,
                "z_entry": z_entry,
                "age_hours": age_hours,
            })

        except Exception as e:
            exit_error_count += 1
            log_event({
                "type": "exit_failure",
                "trace_id": trace_id,
                "market_1": m1,
                "market_2": m2,
                "close_reason": close_reason,
                "error": str(e),
            })
            kept_count += 1
            save_output.append(position)

    with open(JSON_PATH, "w") as f:
        json.dump(save_output, f, indent=2)

    log_event({
        "type": "exit_summary",
        "closed": closed_count,
        "kept": kept_count,
        "dropped": dropped_count,
        "malformed": malformed_count,
        "zcalc_err": zcalc_error_count,
        "exit_err": exit_error_count,
        "tp_blocked_profit": tp_blocked_profit_count,
        "tp_blocked_fees": tp_blocked_profit_count,
        "tp_loss_exit": tp_loss_exit_count,
        "tp_blocked_missing_pnl": tp_blocked_missing_pnl_count,
        "tp_blocked_age": tp_blocked_age_count,
        "tp_blocked_confirm": tp_blocked_confirm_count,
        "time_blocked_profit": time_blocked_profit_count,
        "single_leg_cleanup": single_leg_cleanup_count,
        "saved": len(save_output),
        "use_time_stop": USE_TIME_STOP,
        "tp_confirm_required": TP_CONFIRM_CHECKS,
        "min_hold_minutes": MIN_HOLD_MINUTES_FOR_TP,
    })

    # Summary only if there were actions or errors worth reporting
    if single_leg_cleanup_count > 0 or exit_error_count > 0:
        send_message(
            f"Exit summary: closed={closed_count} orphan_cleanup={single_leg_cleanup_count} "
            f"errors={exit_error_count} tracked={len(save_output)}"
        )

    return {
        "status": "complete",
        "closed": closed_count,
        "tp_count": tp_close_count,
        "loss_exit_count": tp_loss_exit_count,
        "hard_sl_count": hard_sl_close_count,
        "z_sl_count": z_sl_close_count,
        "orphan_cleanup_count": single_leg_cleanup_count,
        "net_pnl_est_sum": net_pnl_est_sum,
        "saved": len(save_output),
    }
