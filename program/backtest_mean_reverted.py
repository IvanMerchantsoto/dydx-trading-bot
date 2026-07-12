#!/usr/bin/env python3
"""
backtest_mean_reverted.py — Compara 2 lógicas de exit:

  MODO A (CURRENT / Jun 30 revert):
    - HARD_SL: max($2.50, 0.02×$130) = $2.60
    - STICKY TP (retroactivo via best_z)
    - Si pnl_gross < 0 y z revirtió → HOLD hasta HARD_SL

  MODO B (NUEVO — TP_MEAN_REVERTED estricto):
    - Todo lo de A +
    - Si pnl_gross en [-$0.50, 0] Y best_z <= 0.5 Y age >= 60min
      → cerrar aceptando la pérdida chica

Motivación (2026-07-11): observado tp_confirm=916 sin cerrar mientras
spread revertía perfecto (best_z=0.02) pero pnl<0. Sin acción se llega a
HARD_SL -$2.60. Con TP_MEAN_REVERTED, peor caso -$0.50.

Uso: python3 backtest_mean_reverted.py --bars 2160  # 3 meses
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from backtest import fetch_candles, CSV_PATH
from backtest_yearly import fetch_candles_with_ts

try:
    from constants import (
        ZSCORE_THRESH, Z_TP, Z_SL_DELTA, WINDOW as LIVE_WINDOW,
        MAX_OPEN_TRADES, TP_CONFIRM_CHECKS,
        HARD_SL_USD, HARD_SL_PCT,
        MIN_PROFIT_USD, MIN_PROFIT_PCT,
    )
except ImportError as e:
    print(f"Failed to import constants: {e}")
    sys.exit(1)


TAKER_FEE_BPS = 0.0005
SLIPPAGE_BPS_CLOSE = 0.0020
USD_PER_TRADE = 65.0

# NEW parameters for TP_MEAN_REVERTED
TP_MEAN_REVERTED_MAX_LOSS = -0.50
MIN_AGE_FOR_MEAN_REV_LOSS_MIN = 60.0
BEST_Z_MEAN_REV_MAX = 0.5


def simulate_pair(prices_m1, prices_m2, timestamps, hedge_ratio, enable_mean_reverted):
    """
    Same as backtest_current_logic but with optional TP_MEAN_REVERTED.
    Returns trades list.
    """
    p1 = np.array(prices_m1, dtype=float)
    p2 = np.array(prices_m2, dtype=float)
    if len(p1) < 2 or len(p2) < 2:
        return []
    n = min(len(p1), len(p2), len(timestamps))
    p1 = p1[-n:]
    p2 = p2[-n:]
    ts = timestamps[-n:]
    spread = p1 - hedge_ratio * p2
    MAX_BARS = 500

    trades = []
    state = "flat"
    entry_z = entry_p1 = entry_p2 = 0.0
    entry_idx = 0
    entry_ts = None
    entry_side = "short"
    open_fee = 0.0
    best_z = 99.0
    tp_confirm_count = 0
    tp_zone_reached = False

    for i in range(LIVE_WINDOW, n):
        w = spread[i - LIVE_WINDOW:i]
        s_mean, s_std = np.mean(w), np.std(w)
        if s_std < 1e-10:
            continue
        z = (spread[i] - s_mean) / s_std

        if state == "flat":
            if abs(z) >= ZSCORE_THRESH:
                state = "in_trade"
                entry_z = z
                entry_idx = i
                entry_p1 = p1[i]
                entry_p2 = p2[i]
                entry_ts = ts[i]
                best_z = abs(z)
                tp_confirm_count = 0
                tp_zone_reached = False
                entry_side = "short" if z > 0 else "long"
                open_fee = USD_PER_TRADE * 2 * TAKER_FEE_BPS

        elif state == "in_trade":
            best_z = min(best_z, abs(z))
            hold = i - entry_idx
            hold_min = hold * 60  # 1H candles → 60 min per bar

            sz1 = USD_PER_TRADE / entry_p1
            sz2 = USD_PER_TRADE / entry_p2
            if entry_side == "short":
                pnl1 = (entry_p1 - p1[i]) * sz1
                pnl2 = (p2[i] - entry_p2) * sz2
            else:
                pnl1 = (p1[i] - entry_p1) * sz1
                pnl2 = (entry_p2 - p2[i]) * sz2
            pnl_gross = pnl1 + pnl2

            close_fee = USD_PER_TRADE * 2 * TAKER_FEE_BPS
            notional_combined = 2 * USD_PER_TRADE
            close_slippage = SLIPPAGE_BPS_CLOSE * notional_combined
            total_costs = open_fee + close_fee + close_slippage
            net_pnl = pnl_gross - total_costs

            close_reason = None

            # HARD_SL
            hard_sl_level = max(float(HARD_SL_USD), float(HARD_SL_PCT) * notional_combined)
            if pnl_gross <= -hard_sl_level:
                close_reason = "HARD_SL"

            # Z_SL
            if close_reason is None:
                if abs(z) >= abs(entry_z) + Z_SL_DELTA:
                    close_reason = "Z_SL"

            # STICKY TP + profit gate + optionally TP_MEAN_REVERTED
            if close_reason is None:
                if abs(z) <= Z_TP or best_z <= Z_TP:
                    tp_zone_reached = True
                tp_zone = tp_zone_reached

                if tp_zone:
                    tp_confirm_count += 1
                    if tp_confirm_count >= TP_CONFIRM_CHECKS:
                        # Profit gate strict
                        target_net = max(float(MIN_PROFIT_USD), float(MIN_PROFIT_PCT) * notional_combined)
                        min_gross_required = target_net + total_costs

                        if pnl_gross >= min_gross_required:
                            close_reason = "TP"
                        elif enable_mean_reverted and pnl_gross < 0.0:
                            # TP_MEAN_REVERTED strict gate
                            age_ok = hold_min >= MIN_AGE_FOR_MEAN_REV_LOSS_MIN
                            loss_ok = pnl_gross >= TP_MEAN_REVERTED_MAX_LOSS
                            reverted_ok = best_z <= BEST_Z_MEAN_REV_MAX
                            if age_ok and loss_ok and reverted_ok:
                                close_reason = "TP_MEAN_REVERTED"
                else:
                    tp_confirm_count = 0

            # Time stop
            if close_reason is None and hold >= MAX_BARS:
                close_reason = "TIME_STOP"

            if close_reason:
                trades.append({
                    "entry_ts":   entry_ts,
                    "exit_ts":    ts[i],
                    "entry_z":    entry_z,
                    "exit_z":     z,
                    "best_z":     best_z,
                    "hold_bars":  hold,
                    "pnl_gross":  round(pnl_gross, 4),
                    "net_pnl":    round(net_pnl, 4),
                    "close_reason": close_reason,
                })
                state = "flat"
                tp_confirm_count = 0
                tp_zone_reached = False

    return trades


def apply_cap(all_trades, max_open):
    all_trades = sorted(all_trades, key=lambda t: t["entry_ts"])
    kept, dropped = [], []
    open_ts = []
    for t in all_trades:
        open_ts = [x for x in open_ts if x > t["entry_ts"]]
        if len(open_ts) < max_open:
            kept.append(t)
            open_ts.append(t["exit_ts"])
        else:
            dropped.append(t)
    return kept, dropped


def stats(trades):
    if not trades:
        return None
    n = len(trades)
    wins = [t for t in trades if t["net_pnl"] > 0]
    losses = [t for t in trades if t["net_pnl"] <= 0]
    net = sum(t["net_pnl"] for t in trades)
    sorted_t = sorted(trades, key=lambda x: x["exit_ts"])
    eq = np.cumsum([t["net_pnl"] for t in sorted_t])
    peak = np.maximum.accumulate(eq)
    dd = (eq - peak).min() if len(eq) else 0
    return {
        "n": n,
        "wr": len(wins) / n * 100,
        "net": net,
        "avg_w": np.mean([t["net_pnl"] for t in wins]) if wins else 0,
        "avg_l": np.mean([t["net_pnl"] for t in losses]) if losses else 0,
        "max_dd": dd,
        "reasons": {r: sum(1 for t in trades if t["close_reason"] == r)
                    for r in set(t["close_reason"] for t in trades)},
    }


def group_by_month(trades):
    by_m = defaultdict(list)
    for t in trades:
        ex = t["exit_ts"]
        dt = datetime.fromisoformat(ex.replace("Z", "+00:00")) if isinstance(ex, str) else ex
        by_m[dt.strftime("%Y-%m")].append(t)
    return by_m


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bars", type=int, default=2160)
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args()

    df = pd.read_csv(CSV_PATH)
    if "base_market" not in df.columns:
        col_map = {c: {"base": "base_market", "quote": "quote_market", "hedge_ratio": "hedge_ratio"}.get(c.lower(), c) for c in df.columns}
        df = df.rename(columns=col_map)
    if args.top:
        df = df.head(args.top)

    print("=" * 75)
    print(f"  COMPARACIÓN: SIN vs CON TP_MEAN_REVERTED")
    print(f"  Período: {args.bars} bars (~{args.bars/24:.0f}d)")
    print(f"  Pares:   {len(df)}, cap MAX_OPEN={MAX_OPEN_TRADES}")
    print("=" * 75)
    print(f"MODO A (current):   HOLD si pnl_gross<0")
    print(f"MODO B (new):       cerrar si pnl in [-$0.50, 0] AND best_z<=0.5 AND age>=60min")
    print("=" * 75)

    trades_A = []
    trades_B = []

    for i, row in df.iterrows():
        m1 = row["base_market"]
        m2 = row["quote_market"]
        hr = float(row.get("hedge_ratio", 1.0))
        raw_m1 = fetch_candles_with_ts(m1, args.bars, use_cache=not args.no_cache)
        raw_m2 = fetch_candles_with_ts(m2, args.bars, use_cache=not args.no_cache)
        if not raw_m1 or not raw_m2:
            print(f"  [{i+1}/{len(df)}] {m1}/{m2}... SKIP (sin datos)")
            continue
        d1 = {t: p for t, p in raw_m1}
        d2 = {t: p for t, p in raw_m2}
        common = sorted(set(d1.keys()) & set(d2.keys()))
        if len(common) < 50:
            print(f"  [{i+1}/{len(df)}] {m1}/{m2}... SKIP (< 50 bars)")
            continue
        prices_m1 = [d1[t] for t in common]
        prices_m2 = [d2[t] for t in common]

        # Modo A: sin TP_MEAN_REVERTED
        tA = simulate_pair(prices_m1, prices_m2, common, hr, enable_mean_reverted=False)
        for t in tA: t["pair"] = f"{m1}/{m2}"
        trades_A.extend(tA)

        # Modo B: con TP_MEAN_REVERTED
        tB = simulate_pair(prices_m1, prices_m2, common, hr, enable_mean_reverted=True)
        for t in tB: t["pair"] = f"{m1}/{m2}"
        trades_B.extend(tB)

        if tA and tB:
            netA = sum(t["net_pnl"] for t in tA)
            netB = sum(t["net_pnl"] for t in tB)
            print(f"  [{i+1}/{len(df)}] {m1}/{m2}... A={len(tA)}tr(${netA:+.1f}) vs B={len(tB)}tr(${netB:+.1f})")

    # Apply cap
    keptA, _ = apply_cap(trades_A, MAX_OPEN_TRADES)
    keptB, _ = apply_cap(trades_B, MAX_OPEN_TRADES)

    sA = stats(keptA)
    sB = stats(keptB)

    print()
    print("=" * 75)
    print(f"  RESULTADO GLOBAL con cap {MAX_OPEN_TRADES}")
    print("=" * 75)
    print(f"{'Metric':<22} {'MODO A (current)':>22}  {'MODO B (new)':>22}")
    print("-" * 75)
    print(f"{'Trades':<22} {sA['n']:>22}  {sB['n']:>22}")
    print(f"{'Winrate':<22} {sA['wr']:>21.1f}% {sB['wr']:>21.1f}%")
    print(f"{'Net PnL':<22} ${sA['net']:>+21.2f} ${sB['net']:>+21.2f}")
    print(f"{'Avg win':<22} ${sA['avg_w']:>+21.2f} ${sB['avg_w']:>+21.2f}")
    print(f"{'Avg loss':<22} ${sA['avg_l']:>+21.2f} ${sB['avg_l']:>+21.2f}")
    print(f"{'Max drawdown':<22} ${sA['max_dd']:>+21.2f} ${sB['max_dd']:>+21.2f}")
    diff = sB['net'] - sA['net']
    print(f"\n  DIFERENCIA (B - A): ${diff:+.2f}  ({diff/max(abs(sA['net']),1)*100:+.1f}%)")

    print(f"\n  DISTRIBUCIÓN close_reason")
    all_reasons = sorted(set(list(sA['reasons'].keys()) + list(sB['reasons'].keys())))
    for r in all_reasons:
        a = sA['reasons'].get(r, 0)
        b = sB['reasons'].get(r, 0)
        print(f"    {r:<20}  A={a:>4}  B={b:>4}  Δ={b-a:+d}")

    # Breakdown mensual
    print()
    print("=" * 75)
    print("  BREAKDOWN MENSUAL")
    print("=" * 75)
    print(f"{'Mes':<10}  {'A trades':>9}  {'A net':>10}  {'B trades':>9}  {'B net':>10}  {'Δ':>8}")
    print("-" * 65)
    byM_A = group_by_month(keptA)
    byM_B = group_by_month(keptB)
    all_months = sorted(set(list(byM_A.keys()) + list(byM_B.keys())))
    for m in all_months:
        a = sum(t["net_pnl"] for t in byM_A.get(m, []))
        b = sum(t["net_pnl"] for t in byM_B.get(m, []))
        print(f"{m:<10}  {len(byM_A.get(m,[])):>9}  ${a:>+9.2f}  {len(byM_B.get(m,[])):>9}  ${b:>+9.2f}  ${b-a:>+7.2f}")

    print()
    print("=" * 75)
    print("  RECOMENDACIÓN")
    print("=" * 75)
    if sB['net'] > sA['net'] + 20:
        print(f"  ✅ MODO B (TP_MEAN_REVERTED) es MEJOR: +${diff:.2f} sobre modo A")
        print(f"     Diferencia significativa. Deploy recomendado.")
    elif sB['net'] > sA['net']:
        print(f"  🟡 MODO B ligeramente mejor: +${diff:.2f}")
        print(f"     Diferencia marginal. Deploy con precaución.")
    else:
        print(f"  ❌ MODO B es PEOR: ${diff:.2f}")
        print(f"     TP_MEAN_REVERTED no ayuda en backtest. NO deploy.")


if __name__ == "__main__":
    main()
