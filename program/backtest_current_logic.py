#!/usr/bin/env python3
"""
backtest_current_logic.py — Backtest 100% fiel a la lógica live del bot.

Replica EXACTAMENTE (con tolerancia de 1h vs 30s por candles):
  - Entry: z-score >= ZSCORE_THRESH (2.7)
  - HARD_SL: max(HARD_SL_USD, HARD_SL_PCT × notional_combined)
    = max($2.50, 0.02 × $130) = $2.60
  - Z_SL: abs(z_now) >= abs(entry_z) + Z_SL_DELTA (1.5)
  - TP_CROSSED_ZERO: DESACTIVADO (matching código actual)
  - STICKY TP mode (retroactivo via best_z):
       tp_zone = (abs(z_now) <= Z_TP) OR (best_z <= Z_TP) OR (tp_zone_reached)
  - TP_CONFIRM_CHECKS = 2 consecutivos en tp_zone
  - Profit gate: pnl_gross >= MIN_PROFIT_USD (0.30) + fees + slippage_est
  - Fees: TAKER 5 bps × 2 legs × 2 sides (open + close)
  - Slippage close: ~20 bps × notional (half-spread × 2 legs)
  - Portfolio cap: MAX_OPEN_TRADES concurrentes
  - Time stop: MAX_BARS = 500

Uso: python3 backtest_current_logic.py --bars 2160  # 3 meses
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
        USE_MIN_PROFIT_TP,
    )
    print(f"[Config loaded from constants.py]")
    print(f"  ZSCORE_THRESH={ZSCORE_THRESH}  Z_TP={Z_TP}  Z_SL_DELTA={Z_SL_DELTA}")
    print(f"  HARD_SL_USD={HARD_SL_USD}  HARD_SL_PCT={HARD_SL_PCT}")
    print(f"  MIN_PROFIT_USD={MIN_PROFIT_USD}  MIN_PROFIT_PCT={MIN_PROFIT_PCT}")
    print(f"  TP_CONFIRM_CHECKS={TP_CONFIRM_CHECKS}  MAX_OPEN_TRADES={MAX_OPEN_TRADES}")
    print(f"  WINDOW={LIVE_WINDOW}")
except ImportError as e:
    print(f"Failed to import constants: {e}")
    sys.exit(1)


TAKER_FEE_BPS = 0.0005  # 0.05% per leg per fill
SLIPPAGE_BPS_CLOSE = 0.0020  # 20 bps close slippage (approx real)
USD_PER_TRADE = 65.0


def simulate_pair_faithful(prices_m1, prices_m2, timestamps, hedge_ratio):
    """Replicate live bot logic 1:1 per pair. Returns trades list."""
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
    tp_zone_reached = False  # STICKY flag

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
                open_fee = USD_PER_TRADE * 2 * TAKER_FEE_BPS  # $65 × 2 legs × 5bps

        elif state == "in_trade":
            best_z = min(best_z, abs(z))
            hold = i - entry_idx

            # PnL calculation (uses candle close ~= oracle-like)
            sz1 = USD_PER_TRADE / entry_p1
            sz2 = USD_PER_TRADE / entry_p2
            if entry_side == "short":
                pnl1 = (entry_p1 - p1[i]) * sz1
                pnl2 = (p2[i] - entry_p2) * sz2
            else:
                pnl1 = (p1[i] - entry_p1) * sz1
                pnl2 = (entry_p2 - p2[i]) * sz2
            pnl_gross = pnl1 + pnl2

            close_fee = USD_PER_TRADE * 2 * TAKER_FEE_BPS  # both legs at close
            notional_combined = 2 * USD_PER_TRADE  # $130
            close_slippage = SLIPPAGE_BPS_CLOSE * notional_combined  # 20 bps × $130 = $0.26
            total_costs = open_fee + close_fee + close_slippage
            net_pnl = pnl_gross - total_costs

            close_reason = None

            # ── 1. HARD_SL (matches live: max of USD and PCT×notional) ────
            hard_sl_level = max(float(HARD_SL_USD), float(HARD_SL_PCT) * notional_combined)
            if pnl_gross <= -hard_sl_level:
                close_reason = "HARD_SL"

            # ── 2. Z_SL ─────────────────────────────────────────────────
            if close_reason is None:
                sl_level = abs(entry_z) + Z_SL_DELTA
                if abs(z) >= sl_level:
                    close_reason = "Z_SL"

            # ── 3. STICKY TP (with retroactive best_z fix) ──────────────
            if close_reason is None:
                if abs(z) <= Z_TP or best_z <= Z_TP:
                    tp_zone_reached = True
                tp_zone = tp_zone_reached  # once reached, always True

                if tp_zone:
                    tp_confirm_count += 1
                    if tp_confirm_count >= TP_CONFIRM_CHECKS:
                        # Profit gate
                        target_net = max(float(MIN_PROFIT_USD), float(MIN_PROFIT_PCT) * notional_combined)
                        min_gross_required = target_net + total_costs
                        if pnl_gross >= min_gross_required:
                            close_reason = "TP"
                else:
                    tp_confirm_count = 0

            # ── 4. Time stop ────────────────────────────────────────────
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


def apply_max_concurrent_cap(all_trades, max_open):
    """Portfolio-level cap. Order by entry_ts, drop entries that exceed cap."""
    all_trades = sorted(all_trades, key=lambda t: t["entry_ts"])
    kept, dropped = [], []
    open_at_time = []  # list of (exit_ts)
    for t in all_trades:
        # Remove already-expired
        open_at_time = [x for x in open_at_time if x > t["entry_ts"]]
        if len(open_at_time) < max_open:
            kept.append(t)
            open_at_time.append(t["exit_ts"])
        else:
            dropped.append(t)
    return kept, dropped


def group_by_month(trades):
    by_month = defaultdict(list)
    for t in trades:
        exit_ts = t["exit_ts"]
        dt = datetime.fromisoformat(exit_ts.replace("Z", "+00:00")) if isinstance(exit_ts, str) else exit_ts
        by_month[dt.strftime("%Y-%m")].append(t)
    return by_month


def stats(trades):
    if not trades:
        return None
    n = len(trades)
    wins = [t for t in trades if t["net_pnl"] > 0]
    losses = [t for t in trades if t["net_pnl"] <= 0]
    net = sum(t["net_pnl"] for t in trades)
    equity = np.cumsum([t["net_pnl"] for t in sorted(trades, key=lambda x: x["exit_ts"])])
    peak = np.maximum.accumulate(equity)
    dd = (equity - peak).min() if len(equity) else 0
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bars", type=int, default=2160, help="720=1mo 2160=3mo 8760=1yr")
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument("--max-open", type=int, default=None,
                        help=f"Override MAX_OPEN (default={MAX_OPEN_TRADES})")
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args()

    max_concurrent = args.max_open if args.max_open is not None else MAX_OPEN_TRADES

    df = pd.read_csv(CSV_PATH)
    if "base_market" not in df.columns:
        col_map = {c: {"base": "base_market", "quote": "quote_market", "hedge_ratio": "hedge_ratio"}.get(c.lower(), c) for c in df.columns}
        df = df.rename(columns=col_map)
    if args.top:
        df = df.head(args.top)

    print()
    print("=" * 70)
    print(f"  BACKTEST 100% FIEL AL CÓDIGO ACTUAL")
    print(f"  Período: {args.bars} bars (~{args.bars/24:.0f}d, ~{args.bars/24/30:.1f}mo)")
    print(f"  Pares:   {len(df)}")
    print(f"  Cap:     MAX_OPEN={max_concurrent}")
    print("=" * 70)

    all_trades = []
    per_pair_stats = []

    for i, row in df.iterrows():
        m1 = row["base_market"]
        m2 = row["quote_market"]
        hr = float(row.get("hedge_ratio", 1.0))

        raw_m1 = fetch_candles_with_ts(m1, args.bars, use_cache=not args.no_cache)
        raw_m2 = fetch_candles_with_ts(m2, args.bars, use_cache=not args.no_cache)
        if not raw_m1 or not raw_m2:
            print(f"  [{i+1}/{len(df)}] {m1}/{m2}... SKIP (sin datos)")
            continue

        # Alinear timestamps
        d1 = {t: p for t, p in raw_m1}
        d2 = {t: p for t, p in raw_m2}
        common = sorted(set(d1.keys()) & set(d2.keys()))
        if len(common) < 50:
            print(f"  [{i+1}/{len(df)}] {m1}/{m2}... SKIP (solo {len(common)} bars comunes)")
            continue

        prices_m1 = [d1[t] for t in common]
        prices_m2 = [d2[t] for t in common]

        trades = simulate_pair_faithful(prices_m1, prices_m2, common, hr)
        for t in trades:
            t["pair"] = f"{m1}/{m2}"
        all_trades.extend(trades)

        if trades:
            net = sum(t["net_pnl"] for t in trades)
            wr = sum(1 for t in trades if t["net_pnl"] > 0) / len(trades) * 100
            print(f"  [{i+1}/{len(df)}] {m1}/{m2}... {len(trades)} trades WR={wr:.0f}% net=${net:+.2f}")
            per_pair_stats.append({"pair": f"{m1}/{m2}", "n": len(trades), "wr": wr, "net": round(net, 2)})
        else:
            print(f"  [{i+1}/{len(df)}] {m1}/{m2}... 0 trades")

    if not all_trades:
        print("Sin trades. Fin.")
        sys.exit(0)

    # Sin cap y con cap
    kept, dropped = apply_max_concurrent_cap(all_trades, max_concurrent)
    s_unc = stats(all_trades)
    s_cap = stats(kept)

    print()
    print("=" * 70)
    print(f"  RESULTADO GLOBAL — {args.bars/24:.0f} días")
    print("=" * 70)
    print(f"{'':22} {'Sin cap':>18}  {'Con cap ' + str(max_concurrent):>18}")
    print(f"{'Trades':<22} {s_unc['n']:>18}  {s_cap['n']:>18}")
    print(f"{'Winrate':<22} {s_unc['wr']:>17.1f}% {s_cap['wr']:>17.1f}%")
    print(f"{'Net PnL':<22} ${s_unc['net']:>17.2f} ${s_cap['net']:>17.2f}")
    print(f"{'Avg win':<22} ${s_unc['avg_w']:>17.2f} ${s_cap['avg_w']:>17.2f}")
    print(f"{'Avg loss':<22} ${s_unc['avg_l']:>17.2f} ${s_cap['avg_l']:>17.2f}")
    print(f"{'Max drawdown':<22} ${s_unc['max_dd']:>17.2f} ${s_cap['max_dd']:>17.2f}")
    print(f"\nTrades descartados por cap: {len(dropped)} ({len(dropped)/len(all_trades)*100:.1f}%)")

    # Distribución de motivos
    print(f"\n  DISTRIBUCIÓN close_reason (con cap)")
    for r, c in sorted(s_cap["reasons"].items(), key=lambda x: -x[1]):
        pct = c / s_cap["n"] * 100
        print(f"    {r:<15} {c:>5} ({pct:>5.1f}%)")

    # Breakdown mensual
    by_month = group_by_month(kept)
    print()
    print("=" * 70)
    print("  BREAKDOWN MENSUAL (con cap)")
    print("=" * 70)
    print(f"{'Mes':<10}  {'Trades':>7}  {'WR%':>6}  {'Net PnL':>12}  {'Cumul':>12}")
    print("-" * 55)
    cumul = 0
    for m in sorted(by_month.keys()):
        mts = by_month[m]
        wins = sum(1 for t in mts if t["net_pnl"] > 0)
        wr = wins / len(mts) * 100
        net = sum(t["net_pnl"] for t in mts)
        cumul += net
        emo = "🟢" if net > 0 else ("🔴" if net < -5 else "🟡")
        print(f"{m:<10}  {len(mts):>7}  {wr:>6.1f}  ${net:>+11.2f}  ${cumul:>+11.2f} {emo}")

    # Top / Bottom pares
    if per_pair_stats:
        print()
        print("=" * 70)
        print("  TOP 10 PARES")
        print("=" * 70)
        for p in sorted(per_pair_stats, key=lambda x: -x["net"])[:10]:
            print(f"  {p['pair']:<25}  {p['n']:>4}  WR={p['wr']:>5.1f}%  ${p['net']:>+9.2f}")

        print("\n  BOTTOM 5 PARES")
        for p in sorted(per_pair_stats, key=lambda x: x["net"])[:5]:
            print(f"  {p['pair']:<25}  {p['n']:>4}  WR={p['wr']:>5.1f}%  ${p['net']:>+9.2f}")

    # Extrapolación
    n_months = len(by_month) if by_month else 1
    monthly_avg = s_cap['net'] / n_months
    print()
    print("=" * 70)
    print("  RESUMEN")
    print("=" * 70)
    print(f"  Período:        {n_months} meses")
    print(f"  PnL total:      ${s_cap['net']:+.2f}")
    print(f"  PnL/mes prom:   ${monthly_avg:+.2f}")
    print(f"  Max drawdown:   ${s_cap['max_dd']:.2f}")
    print(f"  Ratio DD/PnL:   {abs(s_cap['max_dd']/max(s_cap['net'],1))*100:.1f}%")
    print(f"  Winrate:        {s_cap['wr']:.1f}%")


if __name__ == "__main__":
    main()
