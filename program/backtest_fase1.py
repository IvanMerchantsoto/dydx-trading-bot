#!/usr/bin/env python3
"""
backtest_fase1.py — Backtest de la config Fase 1 (Jun 30 style + notionals chicos)

Config:
  - USD_PER_TRADE = 30 (notional pair $60)
  - Z_TP = 0.7 (fácil de disparar)
  - HARD_SL = max($2.50, 2% × $60) = $2.50
  - TP_CROSSED_ZERO habilitado
  - TP_MEAN_REVERTED DESACTIVADO (HOLD si pnl<0)
  - MAX_OPEN = 10 (cap absoluto)
  - Blacklist: CRO, ZORA, AXL, HBAR (excluidos del CSV al inicio)

Uso: python3 backtest_fase1.py --bars 2160
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


# CONFIG FASE 1
Z_ENTRY_THRESH = 2.7
Z_TP = 0.7
Z_SL_DELTA = 1.5
HARD_SL_USD = 2.5
HARD_SL_PCT = 0.02
MIN_PROFIT_USD = 0.30
MIN_PROFIT_PCT = 0.0025
TP_CONFIRM_CHECKS = 2
WINDOW = 21
USD_PER_TRADE = 30.0  # Notional pair $60
MAX_OPEN = 10

TAKER_FEE_BPS = 0.0005
SLIPPAGE_BPS_CLOSE = 0.0020  # 20bps — asume solo pares líquidos (blacklist activa)

BLACKLIST = {"CRO-USD", "ZORA-USD", "AXL-USD", "HBAR-USD", "PENDLE-USD"}


def simulate_pair(prices_m1, prices_m2, timestamps, hedge_ratio):
    """
    Simula estrategia Fase 1:
    - TP_CROSSED_ZERO enabled
    - TP_MEAN_REVERTED disabled
    - HOLD si pnl_gross < 0 y z revirtió
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

    for i in range(WINDOW, n):
        w = spread[i - WINDOW:i]
        s_mean, s_std = np.mean(w), np.std(w)
        if s_std < 1e-10:
            continue
        z = (spread[i] - s_mean) / s_std

        if state == "flat":
            if abs(z) >= Z_ENTRY_THRESH:
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
            hard_sl_level = max(HARD_SL_USD, HARD_SL_PCT * notional_combined)
            if pnl_gross <= -hard_sl_level:
                close_reason = "HARD_SL"

            # Z_SL
            if close_reason is None:
                if abs(z) >= abs(entry_z) + Z_SL_DELTA:
                    close_reason = "Z_SL"

            # TP_CROSSED_ZERO (RELAXED gate — solo net > 0)
            # Fix Jun 30 bug: antes requería ok_profit ($0.30+ neto).
            # Ahora: si z cruzó cero Y net_pnl > 0 → cerrar.
            if close_reason is None:
                zero_crossed = (
                    (entry_z > 0.1 and z < -0.1) or
                    (entry_z < -0.1 and z > 0.1)
                )
                if zero_crossed and net_pnl > 0:
                    close_reason = "TP_CROSSED_ZERO"

            # STICKY TP + profit gate (main path)
            if close_reason is None:
                if abs(z) <= Z_TP or best_z <= Z_TP:
                    tp_zone_reached = True
                tp_zone = tp_zone_reached

                if tp_zone:
                    tp_confirm_count += 1
                    if tp_confirm_count >= TP_CONFIRM_CHECKS:
                        target_net = max(MIN_PROFIT_USD, MIN_PROFIT_PCT * notional_combined)
                        min_gross_required = target_net + total_costs
                        if pnl_gross >= min_gross_required:
                            close_reason = "TP"
                        # else: HOLD (TP_MEAN_REVERTED disabled in Fase 1)
                else:
                    tp_confirm_count = 0

            # Time stop
            if close_reason is None and hold >= MAX_BARS:
                close_reason = "TIME_STOP"

            if close_reason:
                trades.append({
                    "entry_ts": entry_ts,
                    "exit_ts": ts[i],
                    "entry_z": entry_z,
                    "exit_z": z,
                    "best_z": best_z,
                    "hold_bars": hold,
                    "pnl_gross": round(pnl_gross, 4),
                    "net_pnl": round(net_pnl, 4),
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
        "n": n, "wr": len(wins) / n * 100, "net": net,
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

    # Blacklist filter
    before = len(df)
    df = df[~df["base_market"].isin(BLACKLIST) & ~df["quote_market"].isin(BLACKLIST)]
    after = len(df)
    print(f"Blacklist filtró {before - after} pares. Quedan: {after}")

    if args.top:
        df = df.head(args.top)

    print()
    print("=" * 70)
    print(f"  BACKTEST FASE 1 — Jun 30 style + notionals chicos")
    print(f"  {args.bars} bars (~{args.bars/24:.0f}d) | {len(df)} pares")
    print("=" * 70)
    print(f"  USD_PER_TRADE: ${USD_PER_TRADE} (notional pair: $60)")
    print(f"  Z_TP: {Z_TP}")
    print(f"  HARD_SL: ${HARD_SL_USD}")
    print(f"  TP_CROSSED_ZERO: HABILITADO")
    print(f"  TP_MEAN_REVERTED: DESACTIVADO")
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
            continue
        d1 = {t: p for t, p in raw_m1}
        d2 = {t: p for t, p in raw_m2}
        common = sorted(set(d1.keys()) & set(d2.keys()))
        if len(common) < 50:
            continue
        prices_m1 = [d1[t] for t in common]
        prices_m2 = [d2[t] for t in common]

        trades = simulate_pair(prices_m1, prices_m2, common, hr)
        for t in trades:
            t["pair"] = f"{m1}/{m2}"
        all_trades.extend(trades)

        if trades:
            net = sum(t["net_pnl"] for t in trades)
            wr = sum(1 for t in trades if t["net_pnl"] > 0) / len(trades) * 100
            print(f"  [{i+1}/{len(df)}] {m1}/{m2}... {len(trades)} tr WR={wr:.0f}% ${net:+.1f}")
            per_pair_stats.append({"pair": f"{m1}/{m2}", "n": len(trades), "wr": wr, "net": round(net, 2)})

    kept, _ = apply_cap(all_trades, MAX_OPEN)
    s = stats(kept)

    if not s:
        print("Sin trades.")
        return

    print()
    print("=" * 70)
    print(f"  RESULTADO (con cap {MAX_OPEN})")
    print("=" * 70)
    print(f"  Trades:        {s['n']}")
    print(f"  Winrate:       {s['wr']:.1f}%")
    print(f"  Net PnL:       ${s['net']:+.2f}")
    print(f"  Avg win:       ${s['avg_w']:+.2f}")
    print(f"  Avg loss:      ${s['avg_l']:+.2f}")
    print(f"  Max drawdown:  ${s['max_dd']:.2f}")
    print(f"\n  Distribución cierres:")
    for r, c in sorted(s['reasons'].items(), key=lambda x: -x[1]):
        pct = c / s['n'] * 100
        print(f"    {r:<20}  {c:>4} ({pct:>5.1f}%)")

    # Mensual
    by_month = group_by_month(kept)
    print()
    print("=" * 70)
    print("  BREAKDOWN MENSUAL")
    print("=" * 70)
    print(f"{'Mes':<10}  {'Trades':>7}  {'WR%':>6}  {'Net PnL':>12}")
    print("-" * 45)
    for m in sorted(by_month.keys()):
        mts = by_month[m]
        wins = sum(1 for t in mts if t["net_pnl"] > 0)
        wr = wins / len(mts) * 100
        net = sum(t["net_pnl"] for t in mts)
        emo = "🟢" if net > 0 else ("🔴" if net < -5 else "🟡")
        print(f"{m:<10}  {len(mts):>7}  {wr:>6.1f}  ${net:>+11.2f} {emo}")

    n_months = len(by_month) or 1
    print()
    print(f"  PnL/mes promedio: ${s['net']/n_months:+.2f}")


if __name__ == "__main__":
    main()
