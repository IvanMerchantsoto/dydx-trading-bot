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
    TRAIL_TP_ENABLED, TRAIL_Z_PULLBACK, TRAIL_Z_FLOOR,
    SL_COOLDOWN_ENABLED, SL_COOLDOWN_MIN_HOURS, SL_COOLDOWN_HALFLIFE_MULT,
)
from func_utils import format_number
from func_public import get_candles_recent, get_market_spread_bps
from func_cointegration import calculate_zscore
from func_private import place_market_order, close_pair_maker_with_fallback, get_real_fill_details
from constants import MAKER_EXIT_ENABLED, MAKER_EXIT_TIMEOUT_S
from func_logging import log_event
from v4_proto.dydxprotocol.clob.order_pb2 import Order

import json
import asyncio
import os
import numpy as np
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

# ── Post-SL cooldown writer ───────────────────────────────────────────────────
# Reutiliza el mismo pair_fail_cooldowns.json que usa func_entry_pairs.py,
# añadiendo un campo "sl_cooldown_until" con timestamp ISO para bloquear re-entrada.
_SL_COOLDOWN_PATH = os.path.join(os.path.dirname(__file__), "pair_fail_cooldowns.json")


def _pair_key_exit(m1: str, m2: str) -> str:
    return "/".join(sorted([str(m1), str(m2)]))


def _write_sl_cooldown(m1: str, m2: str, half_life: float):
    """Escribe un cooldown de re-entrada tras SL/HARD_SL para el par m1/m2."""
    if not SL_COOLDOWN_ENABLED:
        return
    try:
        try:
            with open(_SL_COOLDOWN_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}

        cooldown_hours = max(
            float(SL_COOLDOWN_MIN_HOURS),
            float(half_life) * float(SL_COOLDOWN_HALFLIFE_MULT) if half_life and half_life > 0 else 0.0
        )
        until_dt = datetime.now(timezone.utc).timestamp() + cooldown_hours * 3600.0
        until_iso = datetime.fromtimestamp(until_dt, tz=timezone.utc).isoformat().replace("+00:00", "Z")

        key = _pair_key_exit(m1, m2)
        rec = data.get(key, {})
        rec["sl_cooldown_until"] = until_iso
        rec["sl_cooldown_hours"] = round(cooldown_hours, 2)
        rec["sl_ts"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        data[key] = rec

        tmp = _SL_COOLDOWN_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, _SL_COOLDOWN_PATH)

        log_event({
            "type": "sl_cooldown_set",
            "pair": key,
            "cooldown_hours": round(cooldown_hours, 2),
            "until": until_iso,
        })
    except Exception as e:
        log_event({"type": "sl_cooldown_write_error", "pair": f"{m1}/{m2}", "error": str(e)})


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
    close_slippage_est: float = 0.0,
) -> tuple[bool, float, float]:
    """
    Returns (passes_gate, min_required_gross, net_pnl_executable).

    pnl_gross         : unrealized PnL proxy (using oracle prices, no fees)
    notional          : total notional of both legs
    open_fees_paid    : actual fees paid at entry
    close_fees_est    : estimated taker fees to close
    close_slippage_est: 2026-07-04 fix Bug #2. Estimated slippage on close.
                        pnl_gross uses ORACLE prices, but actual close crosses
                        the orderbook → pays bid/ask, worse than oracle by
                        ~half-spread × 2 legs. Without accounting for this,
                        the gate reports "profit gate pass" but real execution
                        results in NET LOSS.

    The gate requires "executable" PnL (gross minus all transaction costs)
    to exceed the target minimum profit.
    """
    total_costs = open_fees_paid + close_fees_est + close_slippage_est
    net_pnl_executable = pnl_gross - total_costs

    if USE_MIN_PROFIT_TP:
        target_net = max(float(CONST_MIN_PROFIT_USD), float(CONST_MIN_PROFIT_PCT) * float(notional))
        min_gross_required = target_net + total_costs
        passes = pnl_gross >= min_gross_required
        return passes, min_gross_required, net_pnl_executable

    # If USE_MIN_PROFIT_TP is False: just require covering all costs
    passes = net_pnl_executable >= 0.0
    return passes, total_costs, net_pnl_executable


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

        # Current z-score + absolute spread (for false-convergence detection)
        z_now = None
        spread_now = None  # 2026-07-03: valor absoluto del spread ahora
        _z_skip_reason = None
        try:
            series_1 = await get_candles_recent(indexer, m1)
            series_2 = await get_candles_recent(indexer, m2)

            n1, n2 = len(series_1), len(series_2)
            n = min(n1, n2)

            if n1 == 0 or n2 == 0:
                _z_skip_reason = f"empty_candles m1={n1} m2={n2}"
            elif n <= 25:
                _z_skip_reason = f"too_few_bars n={n} (need >25)"
            else:
                series_1 = series_1[-n:]
                series_2 = series_2[-n:]
                h = _sf(position.get("hedge_ratio"), 1.0)
                if h == 0:
                    _z_skip_reason = "hedge_ratio_zero"
                else:
                    spread = series_1 - (h * series_2)
                    spread_std = float(np.nanstd(spread))
                    zscore_series = calculate_zscore(spread)
                    z_last = zscore_series.values.tolist()[-1]

                    if spread_std < 1e-8:
                        _z_skip_reason = f"spread_std_near_zero={spread_std:.2e}"
                    elif z_last is None or (isinstance(z_last, float) and np.isnan(z_last)):
                        _z_skip_reason = "zscore_nan (window not yet filled)"
                    elif abs(z_last) > 10.0:
                        # Extreme z likely indicates a data/price spike — log but still use
                        log_event({
                            "type": "zscore_extreme_warning",
                            "trace_id": trace_id,
                            "m1": m1, "m2": m2,
                            "z_last": round(float(z_last), 3),
                            "spread_std": round(spread_std, 6),
                            "n_bars": n,
                        })
                        z_now = float(z_last)
                    else:
                        z_now = float(z_last)

                    # ── Spread quality diagnostics ─────────────────────────
                    # Log spread stats periodically for drift detection.
                    # Only when z_now is valid (not on error paths).
                    if z_now is not None:
                        spread_mean = float(np.nanmean(spread))
                        # 2026-07-03: absolute spread value for false-convergence detection
                        spread_now = float(spread[-1]) if len(spread) > 0 else None
                        log_event({
                            "type": "zscore_live",
                            "trace_id": trace_id,
                            "m1": m1, "m2": m2,
                            "z_now": round(z_now, 4),
                            "z_entry": round(z_entry, 4),
                            "spread_now": round(spread_now, 6) if spread_now is not None else None,
                            "spread_mean": round(spread_mean, 6),
                            "spread_std": round(spread_std, 6),
                            "n_bars": n,
                            "hedge_ratio": round(h, 6),
                        }, print_terminal=False)

        except Exception as _ze:
            z_now = None
            _z_skip_reason = f"exception: {_ze}"
            zcalc_error_count += 1

        if _z_skip_reason:
            log_event({
                "type": "zscore_unavailable",
                "trace_id": trace_id,
                "m1": m1, "m2": m2,
                "reason": _z_skip_reason,
            }, print_terminal=False)

        # ── Update best_z (lowest |z| seen since entry) ──────────────────
        # Tracked continuously so trailing TP has the full picture.
        if z_now is not None:
            current_best_z = _sf(position.get("best_z", abs(z_now) + 99.0))
            if abs(z_now) < current_best_z:
                position["best_z"] = abs(z_now)
            # (if z_now is None this loop, keep whatever best_z we had)

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
        # Bug #4 fix: track whether open fees were estimated. If yes, the cycle's
        # net_pnl is provisional and must be flagged as such in trade_closed.
        open_fees_estimated_fallback = False
        if open_fees_paid == 0.0 and total_notional > 0.0:
            open_fees_paid = total_notional * TAKER_FEE_BPS
            open_fees_estimated_fallback = True
        # Also propagate the per-leg fee_estimated flag set at open time (testnet)
        fee_estimated_at_open = bool(
            position.get("fee_1_estimated", False)
            or position.get("fee_2_estimated", False)
        )
        # Close fees are always estimated (we haven't sent the close order yet)
        close_fees_est = _estimate_close_fees(live_pos, markets, m1, m2)
        # pnl_provisional is True whenever ANY component of the fee was estimated.
        pnl_provisional = open_fees_estimated_fallback or fee_estimated_at_open or True
        # NOTE: close_fees_est is always estimated pre-close so pnl_provisional stays
        # True at this stage. After the close, we attempt to fetch real close fees
        # below and downgrade pnl_provisional to False if successful.

        # 2026-07-04 fix (Bug #2): estimar SLIPPAGE de close, no solo fees.
        # pnl_gross usa precios oracle. Al cerrar, cruzamos el book → precio
        # peor que oracle por ~half-spread por cada leg. Sin restarlo, el
        # profit gate dice OK cuando en realidad la ejecución neta es negativa.
        close_slippage_est = 0.0
        try:
            sp1_bps = await get_market_spread_bps(indexer, m1)
            sp2_bps = await get_market_spread_bps(indexer, m2)
            # Fallback: si no hay spread data, asume 20 bps (conservador)
            sp1_bps = sp1_bps if sp1_bps is not None else 20.0
            sp2_bps = sp2_bps if sp2_bps is not None else 20.0
            # Slippage per leg = half-spread × notional
            leg1_notional = abs(_sf(position.get("filled_usd_1", 0.0)))
            leg2_notional = abs(_sf(position.get("filled_usd_2", 0.0)))
            slip_1 = (sp1_bps / 2.0 / 10000.0) * leg1_notional
            slip_2 = (sp2_bps / 2.0 / 10000.0) * leg2_notional
            close_slippage_est = slip_1 + slip_2
        except Exception:
            # 2026-07-05 code-review fix: quitar el /2.0 espurio.
            # Fallback conservador: 20 bps × notional total (equivalente a
            # ~10 bps half-spread por cada leg, similar al path normal).
            close_slippage_est = 0.0020 * total_notional

        ok_profit, min_gross_required, net_pnl_est = _profit_gate(
            pnl_gross, total_notional, open_fees_paid, close_fees_est, close_slippage_est
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

        # ── 2b. ZERO-CROSSING TP (2026-06-30 fix) ───────────────────────────
        # Si z cruzó la media (cambio de signo vs entry), la tesis de
        # mean-reversion se cumplió COMPLETAMENTE — el spread no solo volvió
        # a la media, sino que la atravesó al otro lado.
        #
        # Esto es una señal MÁS FUERTE que |z|<=Z_TP y debe disparar TP
        # inmediatamente, SIN esperar TP_CONFIRM_CHECKS, porque:
        #   1. Es matemáticamente más informativo (cruce real de mean)
        #   2. Captura casos donde el spread "salta" la zona TP entre 2 muestras
        #
        # Caso real (LDO/POL 2026-06-30): entry z=+2.9447. Muestreos: +0.91 →
        # -1.78 en consecutivos de 30s. El bot NUNCA midió |z|<=0.7 porque
        # el z atravesó la zona TP entre 2 samples. Resultado: trade no
        # cerró a tiempo, riesgo de perder ~$1.43 de profit acumulado.
        #
        # Con este fix, el primer sample con z negativo (z_now=-1.78 vs
        # z_entry=+2.94) dispara TP_CROSSED_ZERO inmediatamente.
        #
        # Threshold de ±0.1 evita disparar por ruido cerca de z=0 cuando
        # el spread oscila justo en la media.
        if (not is_close) and USE_Z_TP and (z_now is not None):
            zero_crossed = (
                (z_entry >  0.1 and z_now < -0.1) or
                (z_entry < -0.1 and z_now >  0.1)
            )
            if zero_crossed:
                if can_pnl and pnl_gross > 0 and ok_profit:
                    is_close = True
                    close_reason = (
                        f"TP_CROSSED_ZERO: z_entry={z_entry:.3f} → z_now={z_now:.3f} "
                        f"(spread overshot mean) "
                        f"| pnl_gross={pnl_gross:.2f} | net_est={net_pnl_est:.2f} "
                        f"| min_gross={min_gross_required:.2f}"
                    )
                    log_event({
                        "type": "tp_zero_crossing_trigger",
                        "trace_id": trace_id,
                        "m1": m1, "m2": m2,
                        "z_entry": round(z_entry, 4),
                        "z_now": round(z_now, 4),
                        "pnl_gross": round(pnl_gross, 4),
                        "net_est": round(net_pnl_est, 4),
                    })
                elif can_pnl and pnl_gross > 0 and not ok_profit:
                    # Cruzó pero profit aún no cubre fees + min — esperar más
                    log_event({
                        "type": "tp_zero_crossing_blocked_profit",
                        "trace_id": trace_id,
                        "m1": m1, "m2": m2,
                        "z_entry": round(z_entry, 4),
                        "z_now": round(z_now, 4),
                        "pnl_gross": round(pnl_gross, 4),
                        "net_est": round(net_pnl_est, 4),
                        "min_required": round(min_gross_required, 4),
                    }, print_terminal=False)
                elif can_pnl and pnl_gross <= 0:
                    # 2026-07-03 fix: no HOLD ciego. Si zero-crossing con pérdida
                    # chica Y trade tiene edad razonable, cerrar. La política vieja
                    # de "HOLD hasta HARD_SL/Z_SL" hacía perder MUCHO más ($1.93 en
                    # UNI/SUI observado) que cerrar en el momento de convergencia.
                    MEAN_REVERTED_MAX_LOSS_ZC = -0.75
                    MIN_AGE_FOR_ZC_LOSS_EXIT_MIN = 30.0
                    age_ok = (age_min is not None and age_min >= MIN_AGE_FOR_ZC_LOSS_EXIT_MIN)
                    loss_ok = (pnl_gross >= MEAN_REVERTED_MAX_LOSS_ZC)
                    if age_ok and loss_ok:
                        is_close = True
                        close_reason = (
                            f"TP_ZERO_CROSSED_SMALL_LOSS: z_entry={z_entry:.3f} → "
                            f"z_now={z_now:.3f} (crossed mean), age={age_min:.0f}min, "
                            f"pnl_gross={pnl_gross:.3f} (accepting -${abs(pnl_gross):.2f} "
                            f"to avoid degradation)"
                        )
                        log_event({
                            "type": "tp_zero_crossing_loss_exit",
                            "trace_id": trace_id,
                            "m1": m1, "m2": m2,
                            "z_entry": round(z_entry, 4),
                            "z_now": round(z_now, 4),
                            "pnl_gross": round(pnl_gross, 4),
                            "age_min": round(age_min, 1) if age_min else None,
                        })
                    else:
                        log_event({
                            "type": "tp_zero_crossing_blocked_loss",
                            "trace_id": trace_id,
                            "m1": m1, "m2": m2,
                            "z_entry": round(z_entry, 4),
                            "z_now": round(z_now, 4),
                            "pnl_gross": round(pnl_gross, 4),
                            "age_min": round(age_min, 1) if age_min else None,
                            "gate_failed": ("age" if not age_ok else "loss_too_big"),
                        }, print_terminal=False)

        # ── 3. Take-Profit (z reversion + confirmation + fee gate) ──────────
        # Trailing TP: sigue el spread hasta su peak y cierra en el pullback.
        # Estándar: cierra cuando |z| ≤ Z_TP (umbral fijo).
        tp_zone = False
        if USE_Z_TP and (z_now is not None):
            # 2026-07-01: DYNAMIC per-pair exit threshold.
            # Cada par tiene su propio z_exit_threshold_dyn calculado en
            # cointegración (p30 de |z| histórico). Fallback a Z_TP global.
            _z_tp_dyn = float(position.get("z_exit_threshold_dyn", Z_TP) or Z_TP)

            if TRAIL_TP_ENABLED:
                best_z_val = _sf(position.get("best_z", abs(z_now)))
                # TP zone se activa cuando z ya llegó al umbral (best_z ≤ z_tp_dyn)
                # Y ahora ha rebotado TRAIL_Z_PULLBACK desde su mínimo.
                in_tp_zone = best_z_val <= _z_tp_dyn
                # Floor: si z llegó muy cerca de 0, cerrar inmediatamente
                floor_hit = best_z_val <= float(TRAIL_Z_FLOOR)
                # Pullback: z ha subido TRAIL_Z_PULLBACK desde el mejor z
                pulled_back = abs(z_now) >= best_z_val + float(TRAIL_Z_PULLBACK)
                tp_zone = in_tp_zone and (pulled_back or floor_hit)
                if tp_zone:
                    log_event({
                        "type": "trailing_tp_trigger",
                        "trace_id": trace_id,
                        "m1": m1, "m2": m2,
                        "z_now": z_now,
                        "best_z": best_z_val,
                        "z_entry": z_entry,
                        "floor_hit": floor_hit,
                        "pulled_back": pulled_back,
                    })
            elif USE_TP_HYSTERESIS:
                prev_in_zone = bool(position.get("tp_in_zone", False))
                if (not prev_in_zone) and abs(z_now) <= _z_tp_dyn:
                    tp_zone = True
                elif prev_in_zone and abs(z_now) <= (_z_tp_dyn + 0.15):
                    tp_zone = True
                position["tp_in_zone"] = tp_zone
            else:
                # 2026-07-01: usa el z_exit_threshold_dyn en vez del Z_TP global
                tp_zone = abs(z_now) <= _z_tp_dyn

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
                            f"TP: abs(z={z_now:.3f}) <= {_z_tp_dyn:.3f} (dyn) "
                            f"confirms={tp_confirm} "
                            f"| pnl_gross={pnl_gross:.2f} "
                            f"| fees={open_fees_paid + close_fees_est:.2f} "
                            f"| net_est={net_pnl_est:.2f} "
                            f"| min_gross={min_gross_required:.2f}"
                        )
                    elif pnl_gross < 0.0:
                        # ── SPREAD REVERSION CHECK (2026-07-03) ─────────────────
                        # Antes de aceptar TP_MEAN_REVERTED con pérdida chica,
                        # verificar que el spread absoluto REALMENTE convergió
                        # (no que solo el z bajó por shift del rolling mean).
                        #
                        # spread_reversion_ratio:
                        #   0.0 = spread no se movió del entry
                        #   1.0 = spread llegó al mean original (100% conv)
                        #   >1.0 = spread cruzó el mean (overshoot)
                        #   <0.0 = spread se alejó más (divergió)
                        spread_reversion_ratio = None
                        try:
                            s_entry = float(position.get("spread_at_entry", 0.0) or 0.0)
                            m_entry = float(position.get("mean_at_entry", 0.0) or 0.0)
                            distance_entry = s_entry - m_entry
                            if spread_now is not None and abs(distance_entry) > 1e-9:
                                distance_now = spread_now - m_entry
                                spread_reversion_ratio = 1.0 - (distance_now / distance_entry)
                        except Exception:
                            pass

                        # ── RE-HABILITADO 2026-07-03 (evidencia de bug real) ─────
                        # ANÁLISIS del log de UNI/SUI (24h):
                        #   - z llegó a 0.026 (mean reversion PERFECTA)
                        #   - 689 mediciones con |z| < TP threshold
                        #   - 406 zero-crossings detectados
                        #   - Bot NUNCA cerró porque pnl_gross siempre <0 (por fees)
                        #   - Peak equity $96.55 → actual $94.62 = perdió $1.93 por
                        #     no cerrar en el momento de convergencia
                        #
                        # Nueva regla (smart TP_LOSS_EXIT):
                        # Si z ha convergido claramente Y estamos en pérdida chica,
                        # cerrar aunque sea con pérdida (mejor que dejar degradar).
                        # Umbrales:
                        #   - |z_now| <= threshold_dyn + 20% (convergencia clara)
                        #   - Y pnl_gross en [-$0.75, 0] (pérdida limitada por fees+slippage)
                        #   - Y trade lleva al menos 30 min (evita whipsaw en apertura)
                        #
                        # Rationale: peor caso -$0.75 << peor caso HARD_SL (-$3).
                        # Además evita el fee bleed prolongado observado en UNI/SUI.
                        MEAN_REVERTED_MAX_LOSS = -0.75  # aceptable pérdida chica
                        MIN_AGE_FOR_LOSS_EXIT_MIN = 30.0
                        MIN_SPREAD_REVERSION = 0.5  # spread absoluto debe haber convergido >=50%
                        age_ok = (age_min is not None and age_min >= MIN_AGE_FOR_LOSS_EXIT_MIN)
                        loss_within_limit = (pnl_gross >= MEAN_REVERTED_MAX_LOSS)
                        z_clearly_converged = (abs(z_now) <= _z_tp_dyn * 1.2)
                        # spread_reversion_ok: None (no data) → asume True (backwards-compat).
                        # Data disponible → requiere convergencia absoluta real.
                        spread_reversion_ok = (
                            spread_reversion_ratio is None or
                            spread_reversion_ratio >= MIN_SPREAD_REVERSION
                        )

                        if age_ok and loss_within_limit and z_clearly_converged and spread_reversion_ok:
                            is_close = True
                            close_reason = (
                                f"TP_MEAN_REVERTED: z={z_now:.3f} converged "
                                f"(threshold {_z_tp_dyn:.3f}), spread_rev={spread_reversion_ratio}, "
                                f"age={age_min:.0f}min, pnl_gross={pnl_gross:.3f} "
                                f"(accepting small loss)"
                            )
                            log_event({
                                "type": "tp_mean_reverted",
                                "trace_id": trace_id,
                                "m1": m1, "m2": m2,
                                "z_now": round(z_now, 4),
                                "z_entry": round(z_entry, 4),
                                "z_threshold_dyn": _z_tp_dyn,
                                "spread_reversion_ratio": round(spread_reversion_ratio, 3) if spread_reversion_ratio is not None else None,
                                "pnl_gross": round(pnl_gross, 4),
                                "net_est": round(net_pnl_est, 4),
                                "age_min": round(age_min, 1) if age_min else None,
                            })
                        else:
                            # No cumple criterios: HOLD hasta HARD_SL/Z_SL
                            tp_blocked_profit_count += 1
                            log_event({
                                "type": "tp_blocked_loss",
                                "trace_id": trace_id,
                                "m1": m1, "m2": m2,
                                "pnl_gross": round(pnl_gross, 4),
                                "z_now": round(z_now, 4),
                                "spread_reversion_ratio": round(spread_reversion_ratio, 3) if spread_reversion_ratio is not None else None,
                                "age_min": round(age_min, 1) if age_min else None,
                                "gate_failed": (
                                    "age" if not age_ok else
                                    ("loss_too_big" if not loss_within_limit else
                                     ("z_not_converged" if not z_clearly_converged else
                                      ("false_convergence" if not spread_reversion_ok else "unknown")))
                                ),
                            }, print_terminal=False)
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
                # ── SL / HARD_SL / TP-without-maker: MARKET IOC for speed ──
                _close_res_m1 = await place_market_order(
                    node, indexer, wallet,
                    m1, close_side_m1, close_size_m1, markets[m1]["oraclePrice"],
                    True,
                    time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
                )
                await asyncio.sleep(0.5)
                _close_res_m2 = await place_market_order(
                    node, indexer, wallet,
                    m2, close_side_m2, close_size_m2, markets[m2]["oraclePrice"],
                    True,
                    time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
                )
                # Bug #7: capture client_ids so we can poll real fees after the close
                _close_cid_m1 = (_close_res_m1 or {}).get("order", {}).get("id")
                _close_cid_m2 = (_close_res_m2 or {}).get("order", {}).get("id")
                try:
                    if _close_cid_m1 and _close_cid_m2:
                        # Indexer typically lags 1-2s on fills, sleep before polling
                        await asyncio.sleep(1.5)
                        _det_m1 = await get_real_fill_details(indexer, _close_cid_m1, m1, max_fill_lookback=50)
                        _det_m2 = await get_real_fill_details(indexer, _close_cid_m2, m2, max_fill_lookback=50)
                        _real_fee_close = _sf(_det_m1.get("fee_total", 0.0)) + _sf(_det_m2.get("fee_total", 0.0))
                        _close_fee_estimated = bool(_det_m1.get("fee_estimated", True)) or bool(_det_m2.get("fee_estimated", True))
                        if _real_fee_close > 0:
                            close_fees_est = _real_fee_close
                            net_pnl_est = pnl_gross - (open_fees_paid + close_fees_est)
                            # If neither side is estimated and open also had real fees → no longer provisional
                            if not _close_fee_estimated and not fee_estimated_at_open:
                                pnl_provisional = False
                            log_event({
                                "type": "close_fees_polled",
                                "trace_id": trace_id,
                                "market_1": m1, "market_2": m2,
                                "fee_close_real": _real_fee_close,
                                "fee_close_estimated_flag": _close_fee_estimated,
                                "net_pnl_est_updated": net_pnl_est,
                            }, print_terminal=False)
                except Exception as _fe_err:
                    log_event({
                        "type": "close_fees_poll_error",
                        "trace_id": trace_id,
                        "market_1": m1, "market_2": m2,
                        "error": str(_fe_err),
                    }, print_terminal=False)

            # ── Post-close verification ──────────────────────────────────
            # Esperar 2s y verificar que ambas legs quedaron flat en el exchange.
            # Si alguna leg sigue abierta → loguear y reintentar el cierre.
            # Esto previene el escenario "WIF-USD huérfano" observado en el log.
            await asyncio.sleep(2.0)
            try:
                verify_resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
                verify_sub  = verify_resp.get("subaccount", {}) or {}
                verify_pos  = (
                    verify_sub.get("openPerpetualPositions", {})
                    or verify_sub.get("perpetualPositions", {})
                    or {}
                )
                residual_legs = []
                for check_m, check_side, check_size in [
                    (m1, close_side_m1, close_size_m1),
                    (m2, close_side_m2, close_size_m2),
                ]:
                    remaining_pos = verify_pos.get(check_m, {})
                    remaining_sz  = abs(_sf(remaining_pos.get("size", 0.0)))
                    step = markets.get(check_m, {}).get("stepSize")
                    min_sz = float(step) if step else 0.001
                    if remaining_sz > min_sz:
                        residual_legs.append((check_m, check_side, remaining_sz))

                if residual_legs:
                    log_event({
                        "type": "post_close_residual_detected",
                        "trace_id": trace_id,
                        "market_1": m1, "market_2": m2,
                        "close_reason": close_reason,
                        "residual_legs": [
                            {"market": m, "side": s, "size": sz}
                            for m, s, sz in residual_legs
                        ],
                    })
                    # Retry close for each residual leg
                    for res_m, res_side, res_sz in residual_legs:
                        try:
                            await place_market_order(
                                node, indexer, wallet,
                                res_m, res_side, res_sz,
                                markets[res_m]["oraclePrice"],
                                True,
                                time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
                            )
                            log_event({
                                "type": "post_close_retry_sent",
                                "trace_id": trace_id,
                                "market": res_m,
                                "side": res_side,
                                "size": res_sz,
                            })
                        except Exception as retry_err:
                            log_event({
                                "type": "post_close_retry_failed",
                                "trace_id": trace_id,
                                "market": res_m,
                                "error": str(retry_err),
                            })
                else:
                    log_event({
                        "type": "post_close_verified_flat",
                        "trace_id": trace_id,
                        "market_1": m1, "market_2": m2,
                    }, print_terminal=False)
            except Exception as verify_err:
                log_event({
                    "type": "post_close_verify_error",
                    "trace_id": trace_id,
                    "market_1": m1, "market_2": m2,
                    "error": str(verify_err),
                })

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
                # Bug #4 fix: explicit provenance flags so audit_run.py
                # can separate provisional PnL from confirmed PnL.
                "fee_estimated": bool(fee_estimated_at_open or open_fees_estimated_fallback),
                "open_fees_estimated_fallback": open_fees_estimated_fallback,
                "fee_estimated_at_open": fee_estimated_at_open,
                "pnl_provisional": pnl_provisional,
                "source": "manage_exits",
            })

            # ── Stdout exit log (visible en bot_stdout.log) ───────────────
            emoji_exit = "✅" if net_pnl_est >= 0 else "🔴"
            age_str_out = f"{age_hours:.2f}h" if age_hours is not None else "?"
            z_str_out   = f"{z_now:.3f}" if z_now is not None else "?"
            reason_short = (close_reason or "?")[:40]
            print(
                f"[EXIT] {emoji_exit} {m1}/{m2} | {reason_short} | "
                f"gross=${pnl_gross:.2f} net=${net_pnl_est:.2f} fees=${open_fees_paid+close_fees_est:.2f} | "
                f"z_entry={z_entry:.3f} z_now={z_str_out} | age={age_str_out}",
                flush=True
            )

            # ── Post-SL cooldown: bloquea re-entrada tras Z_SL o HARD_SL ──
            is_sl_exit = close_reason is not None and (
                "Z_SL" in close_reason or "HARD_SL" in close_reason
            )
            if is_sl_exit:
                half_life = _sf(position.get("half_life"), 0.0)
                _write_sl_cooldown(m1, m2, half_life)

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
