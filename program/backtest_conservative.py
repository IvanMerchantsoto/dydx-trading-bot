#!/usr/bin/env python3
"""
backtest_conservative.py — Backtest con parámetros CONSERVADORES para
diagnóstico ratio TP:SL fatal detectado 2026-07-10.

Cambios vs backtest.py original:
  - HARD_SL_USD = $2.50  (NUEVO: stop-loss absoluto en USD, no z-based)
  - Z_TP        = 0.4    (era 0.7, cerrar más rápido en profit)
  - Z_SL_DELTA  = 1.5    (mantiene z-based también, adicional)
  - TP_CROSSED_ZERO: N/A (nunca estuvo en backtest, así que ya "desactivado")

Objetivo: verificar si con HARD_SL más chico + TP más rápido, la estrategia
sale de ratio W:L = 1:19 (que actualmente pierde) a algo rentable.

Uso: python3 backtest_conservative.py --top 30
"""

import argparse
import numpy as np
import pandas as pd
import sys
from pathlib import Path

# Importar utilidades del backtest original
sys.path.insert(0, str(Path(__file__).parent))
from backtest import (
    fetch_candles, compute_metrics,
    CSV_PATH, TAKER_FEE_BPS, RT_FEE_MULT,
    LIVE_WINDOW,
)


# ═════════════════════════════════════════════════════════════════════════
# PARÁMETROS CONSERVADORES
# ═════════════════════════════════════════════════════════════════════════
CONSERVATIVE_PARAMS = {
    "zscore_thresh":    2.7,     # entry (mantiene)
    "z_tp":             0.4,     # ← cerrar más rápido (era 0.7)
    "z_sl_delta":       1.5,     # z-based SL (mantiene)
    "hard_sl_usd":      2.50,    # ← NUEVO: dollar SL
    "trail_tp":         False,   # sin trailing en conservative (más simple)
    "trail_z_pullback": 0.3,
    "trail_z_floor":    0.15,
    "tp_confirm":       2,
    "usd_per_trade":    65.0,
    "z_score_window":   21,
}


def simulate_pair_conservative(prices_m1, prices_m2, hedge_ratio, params):
    """
    Simula estrategia con:
      - HARD_SL absoluto en USD
      - Z_TP tighter
      - Sin TP_CROSSED_ZERO
    """
    p1 = np.array(prices_m1, dtype=float)
    p2 = np.array(prices_m2, dtype=float)

    if len(p1) < 2 or len(p2) < 2:
        return []

    n = min(len(p1), len(p2))
    p1 = p1[-n:]
    p2 = p2[-n:]

    spread = p1 - hedge_ratio * p2

    window      = int(params.get("z_score_window", 21))
    USD         = float(params.get("usd_per_trade", 65.0))
    Z_ENTRY     = float(params["zscore_thresh"])
    Z_TP        = float(params["z_tp"])
    Z_SL_DELTA  = float(params["z_sl_delta"])
    HARD_SL_USD = float(params.get("hard_sl_usd", 8.0))
    TRAIL       = bool(params.get("trail_tp", False))
    PULLBACK    = float(params.get("trail_z_pullback", 0.3))
    FLOOR       = float(params.get("trail_z_floor", 0.15))
    TP_CONFIRM  = int(params.get("tp_confirm", 2))
    MAX_BARS    = 500

    trades = []
    state      = "flat"
    entry_z    = 0.0
    entry_idx  = 0
    entry_p1   = 0.0
    entry_p2   = 0.0
    open_fee   = 0.0
    entry_side = "short"
    best_z     = 99.0
    tp_confirm_count = 0
    mae_running = 0.0

    for i in range(window, n):
        w_spread = spread[i - window:i]
        s_mean   = np.mean(w_spread)
        s_std    = np.std(w_spread)
        if s_std < 1e-10:
            continue
        z = (spread[i] - s_mean) / s_std

        if state == "flat":
            if abs(z) >= Z_ENTRY:
                state            = "in_trade"
                entry_z          = z
                entry_idx        = i
                entry_p1         = p1[i]
                entry_p2         = p2[i]
                best_z           = abs(z)
                tp_confirm_count = 0
                entry_side       = "short" if z > 0 else "long"
                open_fee         = USD * 2 * TAKER_FEE_BPS
                mae_running      = 0.0

        elif state == "in_trade":
            best_z = min(best_z, abs(z))
            hold   = i - entry_idx

            # PnL actual
            sz1 = USD / entry_p1
            sz2 = USD / entry_p2
            if entry_side == "short":
                pnl1 = (entry_p1 - p1[i]) * sz1
                pnl2 = (p2[i] - entry_p2) * sz2
            else:
                pnl1 = (p1[i] - entry_p1) * sz1
                pnl2 = (entry_p2 - p2[i]) * sz2
            pnl_gross  = pnl1 + pnl2
            close_fee  = USD * 2 * TAKER_FEE_BPS
            total_fees = open_fee + close_fee
            net_pnl    = pnl_gross - total_fees

            if pnl_gross < mae_running:
                mae_running = pnl_gross

            close_reason = None

            # ── 1. HARD_SL USD (NUEVO — check primero) ─────────────────────
            if pnl_gross <= -HARD_SL_USD:
                close_reason = "HARD_SL"

            # ── 2. Z Stop-Loss ─────────────────────────────────────────────
            if close_reason is None:
                sl_level = abs(entry_z) + Z_SL_DELTA
                if abs(z) >= sl_level:
                    close_reason = "Z_SL"

            # ── 3. Take-Profit ─────────────────────────────────────────────
            if close_reason is None:
                if TRAIL:
                    in_zone    = best_z <= Z_TP
                    floor_hit  = best_z <= FLOOR
                    pulled_bk  = abs(z) >= best_z + PULLBACK
                    tp_active  = in_zone and (pulled_bk or floor_hit)
                else:
                    tp_active = abs(z) <= Z_TP

                if tp_active:
                    tp_confirm_count += 1
                    if tp_confirm_count >= TP_CONFIRM:
                        # profit gate estricto: solo si net_pnl > 0
                        if net_pnl > 0:
                            close_reason = "TP"
                else:
                    tp_confirm_count = 0

            # ── 4. Time stop ───────────────────────────────────────────────
            if close_reason is None and hold >= MAX_BARS:
                close_reason = "TIME_STOP"

            if close_reason:
                trades.append({
                    "entry_z":     entry_z,
                    "exit_z":      z,
                    "hold_bars":   hold,
                    "pnl_gross":   round(pnl_gross, 4),
                    "fees":        round(total_fees, 4),
                    "net_pnl":     round(net_pnl, 4),
                    "mae":         round(mae_running, 4),
                    "close_reason": close_reason,
                    "entry_side":  entry_side,
                })
                state            = "flat"
                tp_confirm_count = 0
                mae_running      = 0.0

    return trades


def run_backtest_conservative(pairs_df, params, bars=720, use_cache=True):
    """Corre backtest en cada par con los params CONSERVADORES."""
    all_trades = []
    per_pair_metrics = []

    for i, row in pairs_df.iterrows():
        m1 = row["base_market"]
        m2 = row["quote_market"]
        hr = float(row.get("hedge_ratio", 1.0))

        prices_m1 = fetch_candles(m1, resolution="1HOUR", n_bars=bars, use_cache=use_cache)
        prices_m2 = fetch_candles(m2, resolution="1HOUR", n_bars=bars, use_cache=use_cache)

        if not prices_m1 or not prices_m2:
            print(f"  [{i+1}/{len(pairs_df)}] {m1}/{m2}... SKIP (sin datos)")
            continue

        trades = simulate_pair_conservative(prices_m1, prices_m2, hr, params)
        for t in trades:
            t["pair"] = f"{m1}/{m2}"
        all_trades.extend(trades)

        if trades:
            net = sum(t["net_pnl"] for t in trades)
            print(f"  [{i+1}/{len(pairs_df)}] {m1}/{m2} (hr={hr:.4f})... {len(trades)} trades | net=${net:.2f}")
            per_pair_metrics.append({
                "pair": f"{m1}/{m2}",
                "n_trades": len(trades),
                "net_pnl": round(net, 2),
                "wr": round(sum(1 for t in trades if t["net_pnl"] > 0) / len(trades) * 100, 1),
            })
        else:
            print(f"  [{i+1}/{len(pairs_df)}] {m1}/{m2}... 0 trades")

    return all_trades, per_pair_metrics


def main():
    parser = argparse.ArgumentParser(description="Backtest CONSERVADOR (HARD_SL $2.50 + Z_TP 0.4)")
    parser.add_argument("--top", type=int, default=None, help="Usar solo los N primeros pares del CSV")
    parser.add_argument("--pair", type=str, default=None, help='Par específico: "ADA-USD/MNT-USD"')
    parser.add_argument("--bars", type=int, default=720, help="Barras históricas (default: 720 = 30d)")
    parser.add_argument("--no-cache", action="store_true", help="Ignorar caché")
    args = parser.parse_args()

    # Cargar CSV
    pairs_df = pd.read_csv(CSV_PATH)
    if "base_market" not in pairs_df.columns:
        # normalize (old CSVs)
        col_map = {}
        for c in pairs_df.columns:
            cl = c.lower()
            if cl in ("base",): col_map[c] = "base_market"
            elif cl in ("quote",): col_map[c] = "quote_market"
            elif cl in ("hedge_ratio",): col_map[c] = "hedge_ratio"
        pairs_df = pairs_df.rename(columns=col_map)

    if args.pair:
        try:
            b, q = args.pair.split("/")
            pairs_df = pairs_df[(pairs_df["base_market"] == b) & (pairs_df["quote_market"] == q)]
        except Exception:
            print(f"Formato inválido: --pair '{args.pair}' (use 'BASE/QUOTE')")
            sys.exit(1)

    if args.top:
        pairs_df = pairs_df.head(args.top)

    print("=" * 60)
    print("  BACKTEST CONSERVATIVO")
    print(f"  {len(pairs_df)} pares | {args.bars} barras")
    print("=" * 60)
    print("Parámetros conservadores:")
    for k, v in sorted(CONSERVATIVE_PARAMS.items()):
        print(f"  {k} = {v}")
    print("=" * 60)

    trades, per_pair = run_backtest_conservative(
        pairs_df, CONSERVATIVE_PARAMS, bars=args.bars, use_cache=not args.no_cache
    )

    if not trades:
        print("Sin trades generados.")
        sys.exit(0)

    metrics = compute_metrics(trades, label="conservative")

    print("\n" + "=" * 60)
    print("  RESULTADO GLOBAL")
    print("=" * 60)
    print(f"  Trades:          {metrics['n_trades']}")
    print(f"  Win rate:        {metrics['win_rate']*100:.1f}%")
    print(f"  Profit factor:   {metrics['profit_factor']:.3f}")
    print(f"  EV / trade:      ${metrics['ev_per_trade']:.3f}")
    print(f"  Avg win:         ${metrics['avg_win']:.2f}")
    print(f"  Avg loss:        ${metrics['avg_loss']:.3f}")
    print(f"  Net PnL:         ${metrics['net_pnl']:.2f}")
    print(f"  Total fees:      ${metrics['total_fees']:.2f}")
    print(f"  Max drawdown:    ${metrics['max_drawdown']:.2f}")
    print(f"  Sharpe:          {metrics['sharpe']:.3f}")
    print(f"  Ratio Win/Loss:  {abs(metrics['avg_win']/metrics['avg_loss']):.2f}x")

    # Distribución close_reason
    reason_counts = {}
    for t in trades:
        r = t["close_reason"]
        reason_counts[r] = reason_counts.get(r, 0) + 1
    print("\n  Distribución motivos:")
    for r, c in sorted(reason_counts.items(), key=lambda x: -x[1]):
        print(f"    {r:<15}  {c}  ({c/len(trades)*100:.1f}%)")

    # Top 10 por PnL
    per_pair_sorted = sorted(per_pair, key=lambda x: -x["net_pnl"])[:10]
    print("\n" + "=" * 60)
    print("  TOP 10 PARES")
    print("=" * 60)
    print(f"{'Pair':<25} {'N':>4} {'WR%':>6} {'PnL':>10}")
    for p in per_pair_sorted:
        print(f"{p['pair']:<25} {p['n_trades']:>4} {p['wr']:>6.1f} {p['net_pnl']:>+10.2f}")

    # Bottom 5
    print("\n  BOTTOM 5 PARES")
    for p in sorted(per_pair, key=lambda x: x["net_pnl"])[:5]:
        print(f"{p['pair']:<25} {p['n_trades']:>4} {p['wr']:>6.1f} {p['net_pnl']:>+10.2f}")

    # Comparación versus backtest original (implícita)
    print("\n" + "=" * 60)
    print("  COMPARAR CON BACKTEST ORIGINAL")
    print("=" * 60)
    print(f"  Original (HARD_SL implícito, Z_TP 0.7): +$800 net 30d")
    print(f"  Conservative (HARD_SL $2.50, Z_TP 0.4): ${metrics['net_pnl']:.2f} net")
    print()
    if metrics["net_pnl"] > 0:
        print("  ✅ CONSERVATIVE ES RENTABLE")
    else:
        print("  ❌ CONSERVATIVE PIERDE EN BACKTEST TAMBIÉN")


if __name__ == "__main__":
    main()
