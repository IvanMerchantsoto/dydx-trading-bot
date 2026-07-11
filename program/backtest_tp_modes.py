#!/usr/bin/env python3
"""
backtest_tp_modes.py — Compara 3 modos de TP con params conservadores.

Motivación (2026-07-11): diagnóstico reveló que con TRAIL_TP_ENABLED=False +
Z_TP=0.4, el bot pierde oportunidades de close. 4 pares con best_z<=0.3
no cerraron porque el código solo mira z_now actual, no best_z histórico.

Modos comparados:
  A. FIXED   — tp_zone = abs(z_now) <= Z_TP (código actual)
  B. TRAIL   — usa best_z + pullback + floor (código original con trailing)
  C. STICKY  — una vez best_z <= Z_TP, tp_zone permanente hasta close

Todos usan HARD_SL_USD=$2.50 y demás params conservadores.

Uso: python3 backtest_tp_modes.py --top 30
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from backtest import fetch_candles, CSV_PATH, TAKER_FEE_BPS


BASE_PARAMS = {
    "zscore_thresh":    2.7,
    "z_tp":             0.4,
    "z_sl_delta":       1.5,
    "hard_sl_usd":      2.50,
    "trail_z_pullback": 0.3,
    "trail_z_floor":    0.15,
    "tp_confirm":       2,
    "usd_per_trade":    65.0,
    "z_score_window":   21,
}


def simulate_pair(prices_m1, prices_m2, hedge_ratio, params, tp_mode):
    """
    tp_mode: "FIXED", "TRAIL", "STICKY"
    """
    p1 = np.array(prices_m1, dtype=float)
    p2 = np.array(prices_m2, dtype=float)
    if len(p1) < 2 or len(p2) < 2:
        return []
    n = min(len(p1), len(p2))
    p1 = p1[-n:]
    p2 = p2[-n:]
    spread = p1 - hedge_ratio * p2

    window      = int(params["z_score_window"])
    USD         = float(params["usd_per_trade"])
    Z_ENTRY     = float(params["zscore_thresh"])
    Z_TP        = float(params["z_tp"])
    Z_SL_DELTA  = float(params["z_sl_delta"])
    HARD_SL_USD = float(params["hard_sl_usd"])
    PULLBACK    = float(params["trail_z_pullback"])
    FLOOR       = float(params["trail_z_floor"])
    TP_CONFIRM  = int(params["tp_confirm"])
    MAX_BARS    = 500

    trades = []
    state = "flat"
    entry_z = entry_p1 = entry_p2 = open_fee = 0.0
    entry_idx = 0
    entry_side = "short"
    best_z = 99.0
    tp_confirm_count = 0
    tp_zone_ever_reached = False  # for STICKY mode

    for i in range(window, n):
        w = spread[i - window:i]
        s_mean, s_std = np.mean(w), np.std(w)
        if s_std < 1e-10: continue
        z = (spread[i] - s_mean) / s_std

        if state == "flat":
            if abs(z) >= Z_ENTRY:
                state = "in_trade"
                entry_z = z
                entry_idx = i
                entry_p1 = p1[i]
                entry_p2 = p2[i]
                best_z = abs(z)
                tp_confirm_count = 0
                tp_zone_ever_reached = False
                entry_side = "short" if z > 0 else "long"
                open_fee = USD * 2 * TAKER_FEE_BPS

        elif state == "in_trade":
            best_z = min(best_z, abs(z))
            hold = i - entry_idx

            sz1 = USD / entry_p1
            sz2 = USD / entry_p2
            if entry_side == "short":
                pnl1 = (entry_p1 - p1[i]) * sz1
                pnl2 = (p2[i] - entry_p2) * sz2
            else:
                pnl1 = (p1[i] - entry_p1) * sz1
                pnl2 = (entry_p2 - p2[i]) * sz2
            pnl_gross = pnl1 + pnl2
            close_fee = USD * 2 * TAKER_FEE_BPS
            total_fees = open_fee + close_fee
            net_pnl = pnl_gross - total_fees

            close_reason = None

            # HARD_SL
            if pnl_gross <= -HARD_SL_USD:
                close_reason = "HARD_SL"

            # Z_SL
            if close_reason is None:
                if abs(z) >= abs(entry_z) + Z_SL_DELTA:
                    close_reason = "Z_SL"

            # TP MODE ─────────────────────────────────────────────────────
            if close_reason is None:
                if tp_mode == "FIXED":
                    tp_active = abs(z) <= Z_TP

                elif tp_mode == "TRAIL":
                    in_zone   = best_z <= Z_TP
                    floor_hit = best_z <= FLOOR
                    pulled_bk = abs(z) >= best_z + PULLBACK
                    tp_active = in_zone and (pulled_bk or floor_hit)

                elif tp_mode == "STICKY":
                    if abs(z) <= Z_TP:
                        tp_zone_ever_reached = True
                    tp_active = tp_zone_ever_reached

                else:
                    tp_active = False

                if tp_active:
                    tp_confirm_count += 1
                    if tp_confirm_count >= TP_CONFIRM:
                        if net_pnl > 0:
                            close_reason = "TP"
                else:
                    tp_confirm_count = 0

            # Time stop
            if close_reason is None and hold >= MAX_BARS:
                close_reason = "TIME_STOP"

            if close_reason:
                trades.append({
                    "entry_z": entry_z,
                    "exit_z": z,
                    "best_z_seen": best_z,
                    "hold_bars": hold,
                    "net_pnl": round(net_pnl, 4),
                    "pnl_gross": round(pnl_gross, 4),
                    "close_reason": close_reason,
                })
                state = "flat"
                tp_confirm_count = 0
                tp_zone_ever_reached = False

    return trades


def compute_metrics(trades, label):
    if not trades:
        return {"label": label, "n": 0, "wr": 0, "net": 0, "avg_w": 0, "avg_l": 0, "max_dd": 0}
    n = len(trades)
    wins = [t for t in trades if t["net_pnl"] > 0]
    losses = [t for t in trades if t["net_pnl"] <= 0]
    net = sum(t["net_pnl"] for t in trades)
    equity = np.cumsum([t["net_pnl"] for t in trades])
    peak = np.maximum.accumulate(equity)
    dd = (equity - peak).min() if len(equity) > 0 else 0
    return {
        "label": label,
        "n": n,
        "wr": len(wins) / n * 100,
        "net": net,
        "avg_w": np.mean([t["net_pnl"] for t in wins]) if wins else 0,
        "avg_l": np.mean([t["net_pnl"] for t in losses]) if losses else 0,
        "max_dd": dd,
        "reasons": {r: sum(1 for t in trades if t["close_reason"] == r) for r in set(t["close_reason"] for t in trades)},
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument("--bars", type=int, default=720)
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args()

    df = pd.read_csv(CSV_PATH)
    if "base_market" not in df.columns:
        col_map = {c: {"base": "base_market", "quote": "quote_market", "hedge_ratio": "hedge_ratio"}.get(c.lower(), c) for c in df.columns}
        df = df.rename(columns=col_map)
    if args.top:
        df = df.head(args.top)

    print("=" * 65)
    print(f"  BACKTEST TP MODES — {args.bars} barras (~{args.bars/24:.0f} días)")
    print(f"  {len(df)} pares | Params conservadores (HARD_SL $2.50, Z_TP 0.4)")
    print("=" * 65)

    # Correr los 3 modos
    modes = ["FIXED", "TRAIL", "STICKY"]
    all_trades = {m: [] for m in modes}

    for i, row in df.iterrows():
        m1 = row["base_market"]
        m2 = row["quote_market"]
        hr = float(row.get("hedge_ratio", 1.0))
        prices_m1 = fetch_candles(m1, "1HOUR", args.bars, use_cache=not args.no_cache)
        prices_m2 = fetch_candles(m2, "1HOUR", args.bars, use_cache=not args.no_cache)
        if not prices_m1 or not prices_m2:
            continue

        for mode in modes:
            trades = simulate_pair(prices_m1, prices_m2, hr, BASE_PARAMS, mode)
            all_trades[mode].extend(trades)

        # Progreso cada 5 pares
        if (i + 1) % 5 == 0:
            print(f"  [{i+1}/{len(df)}] procesados...", flush=True)

    # Comparar
    print("\n" + "=" * 65)
    print("  COMPARACIÓN DE MODOS")
    print("=" * 65)
    print(f"{'Metric':<20} {'FIXED':>15} {'TRAIL':>15} {'STICKY':>15}")
    print("-" * 65)

    metrics = {m: compute_metrics(all_trades[m], m) for m in modes}
    for field, label in [("n", "Trades"), ("wr", "Winrate %"), ("net", "Net PnL $"),
                          ("avg_w", "Avg win $"), ("avg_l", "Avg loss $"), ("max_dd", "Max DD $")]:
        row = f"{label:<20}"
        for m in modes:
            v = metrics[m][field]
            if field in ("wr",):
                row += f" {v:>14.1f}%"
            elif field in ("net", "max_dd", "avg_w", "avg_l"):
                row += f" ${v:>+13.2f}"
            else:
                row += f" {v:>15}"
        print(row)

    print("\n  DISTRIBUCIÓN close_reason")
    all_reasons = set()
    for m in modes:
        all_reasons.update(metrics[m]["reasons"].keys())
    for reason in sorted(all_reasons):
        row = f"  {reason:<18}"
        for m in modes:
            c = metrics[m]["reasons"].get(reason, 0)
            row += f" {c:>15}"
        print(row)

    # Recomendación
    print("\n" + "=" * 65)
    print("  RECOMENDACIÓN")
    print("=" * 65)
    best_mode = max(modes, key=lambda m: metrics[m]["net"])
    best_net = metrics[best_mode]["net"]
    print(f"  Ganador por PnL: {best_mode} con ${best_net:+.2f}")
    print(f"  Diferencia vs FIXED: ${best_net - metrics['FIXED']['net']:+.2f}")

    # Best modo por Sharpe-like (net/|max_dd|)
    def rr(m):
        dd = abs(metrics[m]["max_dd"])
        return metrics[m]["net"] / dd if dd > 0 else 0
    best_by_rr = max(modes, key=rr)
    print(f"  Ganador por PnL/DD: {best_by_rr} (ratio={rr(best_by_rr):.2f})")


if __name__ == "__main__":
    main()
