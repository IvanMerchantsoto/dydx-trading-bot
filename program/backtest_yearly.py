#!/usr/bin/env python3
"""
backtest_yearly.py — Backtest de 1 AÑO con:
  - Parámetros conservadores (HARD_SL $2.50, Z_TP 0.4)
  - Simulación de cap MAX_OPEN_TRADES a nivel PORTFOLIO
  - Breakdown mensual de PnL
  - Detección de meses malos vs buenos

Objetivo: validar si la estrategia funciona históricamente en distintos
regímenes de mercado (bull, bear, choppy).

Uso:
    python3 backtest_yearly.py                 # 1 año, todos los pares
    python3 backtest_yearly.py --bars 4380     # 6 meses
    python3 backtest_yearly.py --top 30        # top 30 pares
    python3 backtest_yearly.py --max-open 5    # simular MAX=5 en vez de 10

Requiere: pandas, numpy, statsmodels, requests
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

# Importar utilidades y constants
sys.path.insert(0, str(Path(__file__).parent))
from backtest import fetch_candles, CSV_PATH, TAKER_FEE_BPS

try:
    from constants import ZSCORE_THRESH, Z_TP, Z_SL_DELTA, WINDOW as LIVE_WINDOW, MAX_OPEN_TRADES
except ImportError:
    ZSCORE_THRESH = 2.7
    Z_TP = 0.4
    Z_SL_DELTA = 1.5
    LIVE_WINDOW = 21
    MAX_OPEN_TRADES = 10


CONSERVATIVE_PARAMS = {
    "zscore_thresh":    float(ZSCORE_THRESH),
    "z_tp":             float(Z_TP),           # 0.4
    "z_sl_delta":       float(Z_SL_DELTA),
    "hard_sl_usd":      2.50,
    "tp_confirm":       2,
    "usd_per_trade":    65.0,
    "z_score_window":   int(LIVE_WINDOW),
}


def simulate_pair_with_timestamps(prices_m1, prices_m2, timestamps, hedge_ratio, params):
    """
    Igual que simulate_pair_conservative pero también registra timestamp
    de entry y exit para poder agrupar por mes y aplicar cap concurrente.
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

    window      = int(params.get("z_score_window", 21))
    USD         = float(params.get("usd_per_trade", 65.0))
    Z_ENTRY     = float(params["zscore_thresh"])
    Z_TP_p      = float(params["z_tp"])
    Z_SL_DELTA_p = float(params["z_sl_delta"])
    HARD_SL_USD = float(params.get("hard_sl_usd", 2.5))
    TP_CONFIRM  = int(params.get("tp_confirm", 2))
    MAX_BARS    = 500

    trades = []
    state      = "flat"
    entry_z    = 0.0
    entry_idx  = 0
    entry_p1   = 0.0
    entry_p2   = 0.0
    entry_ts   = None
    open_fee   = 0.0
    entry_side = "short"
    best_z     = 99.0
    tp_confirm_count = 0

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
                entry_ts         = ts[i]
                best_z           = abs(z)
                tp_confirm_count = 0
                entry_side       = "short" if z > 0 else "long"
                open_fee         = USD * 2 * TAKER_FEE_BPS

        elif state == "in_trade":
            best_z = min(best_z, abs(z))
            hold   = i - entry_idx

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

            close_reason = None

            if pnl_gross <= -HARD_SL_USD:
                close_reason = "HARD_SL"
            if close_reason is None:
                sl_level = abs(entry_z) + Z_SL_DELTA_p
                if abs(z) >= sl_level:
                    close_reason = "Z_SL"
            if close_reason is None:
                if abs(z) <= Z_TP_p:
                    tp_confirm_count += 1
                    if tp_confirm_count >= TP_CONFIRM:
                        if net_pnl > 0:
                            close_reason = "TP"
                else:
                    tp_confirm_count = 0
            if close_reason is None and hold >= MAX_BARS:
                close_reason = "TIME_STOP"

            if close_reason:
                trades.append({
                    "entry_ts":   entry_ts,
                    "exit_ts":    ts[i],
                    "entry_z":    entry_z,
                    "exit_z":     z,
                    "hold_bars":  hold,
                    "net_pnl":    round(net_pnl, 4),
                    "pnl_gross":  round(pnl_gross, 4),
                    "close_reason": close_reason,
                })
                state = "flat"
                tp_confirm_count = 0

    return trades


def apply_max_concurrent_cap(all_trades, max_open):
    """
    Aplica cap de trades concurrentes a nivel PORTFOLIO.
    Ordena todos los trades por entry_ts. Si al momento de un entry
    hay >= max_open trades abiertos, ese trade se DESCARTA.

    Retorna (trades_kept, trades_dropped).
    """
    # Ordenar por entry_ts
    all_trades = sorted(all_trades, key=lambda t: t["entry_ts"])

    kept = []
    dropped = []

    # Simular timeline: iterar entries en orden y trackear los abiertos
    open_trades = []  # cada uno tiene "exit_ts"

    for t in all_trades:
        # Cerrar trades que ya expiraron antes de este entry
        open_trades = [ot for ot in open_trades if ot["exit_ts"] > t["entry_ts"]]

        if len(open_trades) < max_open:
            kept.append(t)
            open_trades.append(t)
        else:
            dropped.append(t)

    return kept, dropped


def group_by_month(trades):
    """Agrupa trades por mes usando exit_ts."""
    by_month = defaultdict(list)
    for t in trades:
        exit_ts = t["exit_ts"]
        if isinstance(exit_ts, str):
            dt = datetime.fromisoformat(exit_ts.replace("Z", "+00:00"))
        else:
            dt = exit_ts
        month_key = dt.strftime("%Y-%m")
        by_month[month_key].append(t)
    return by_month


def main():
    parser = argparse.ArgumentParser(description="Backtest 1 año con cap MAX_OPEN_TRADES y breakdown mensual")
    parser.add_argument("--bars", type=int, default=8760,
                        help="Barras (default 8760 = 1 año). 4380 = 6 meses.")
    parser.add_argument("--top", type=int, default=30, help="Top N pares del CSV")
    parser.add_argument("--max-open", type=int, default=None,
                        help=f"Cap concurrente (default: MAX_OPEN_TRADES={MAX_OPEN_TRADES})")
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args()

    max_concurrent = args.max_open if args.max_open is not None else MAX_OPEN_TRADES

    pairs_df = pd.read_csv(CSV_PATH)
    if "base_market" not in pairs_df.columns:
        col_map = {}
        for c in pairs_df.columns:
            cl = c.lower()
            if cl in ("base",): col_map[c] = "base_market"
            elif cl in ("quote",): col_map[c] = "quote_market"
            elif cl in ("hedge_ratio",): col_map[c] = "hedge_ratio"
        pairs_df = pairs_df.rename(columns=col_map)

    if args.top:
        pairs_df = pairs_df.head(args.top)

    print("=" * 65)
    print(f"  BACKTEST YEARLY — {args.bars} barras (~{args.bars/24/30:.1f} meses)")
    print(f"  {len(pairs_df)} pares | MAX_OPEN concurrent = {max_concurrent}")
    print("=" * 65)
    print("Parámetros:")
    for k, v in sorted(CONSERVATIVE_PARAMS.items()):
        print(f"  {k} = {v}")
    print("=" * 65)

    # ── Fetch data + simulate por par ─────────────────────────────────
    all_trades_uncapped = []
    pair_stats = []

    for i, row in pairs_df.iterrows():
        m1 = row["base_market"]
        m2 = row["quote_market"]
        hr = float(row.get("hedge_ratio", 1.0))

        # Necesitamos timestamps también
        prices_m1_raw = fetch_candles_with_ts(m1, args.bars, use_cache=not args.no_cache)
        prices_m2_raw = fetch_candles_with_ts(m2, args.bars, use_cache=not args.no_cache)

        if not prices_m1_raw or not prices_m2_raw:
            print(f"  [{i+1}/{len(pairs_df)}] {m1}/{m2}... SKIP (sin datos)")
            continue

        # Alinear por timestamp
        ts_dict_m1 = {t: p for t, p in prices_m1_raw}
        ts_dict_m2 = {t: p for t, p in prices_m2_raw}
        common_ts = sorted(set(ts_dict_m1.keys()) & set(ts_dict_m2.keys()))
        if len(common_ts) < 50:
            print(f"  [{i+1}/{len(pairs_df)}] {m1}/{m2}... SKIP (solo {len(common_ts)} bars comunes)")
            continue

        prices_m1 = [ts_dict_m1[t] for t in common_ts]
        prices_m2 = [ts_dict_m2[t] for t in common_ts]

        trades = simulate_pair_with_timestamps(
            prices_m1, prices_m2, common_ts, hr, CONSERVATIVE_PARAMS
        )
        for t in trades:
            t["pair"] = f"{m1}/{m2}"
        all_trades_uncapped.extend(trades)

        if trades:
            net = sum(t["net_pnl"] for t in trades)
            wins = sum(1 for t in trades if t["net_pnl"] > 0)
            wr = wins / len(trades) * 100
            print(f"  [{i+1}/{len(pairs_df)}] {m1}/{m2}... {len(trades)} trades | WR={wr:.0f}% | net=${net:+.2f}")
            pair_stats.append({
                "pair": f"{m1}/{m2}",
                "trades": len(trades),
                "wr": wr,
                "net_pnl": round(net, 2),
            })
        else:
            print(f"  [{i+1}/{len(pairs_df)}] {m1}/{m2}... 0 trades")

    if not all_trades_uncapped:
        print("Sin trades.")
        sys.exit(0)

    # ── Aplicar cap portfolio ────────────────────────────────────────
    kept, dropped = apply_max_concurrent_cap(all_trades_uncapped, max_concurrent)

    # ── Resultados globales sin cap y con cap ────────────────────────
    def stats(trades, label):
        if not trades:
            return None
        n = len(trades)
        wins = [t for t in trades if t["net_pnl"] > 0]
        losses = [t for t in trades if t["net_pnl"] <= 0]
        net = sum(t["net_pnl"] for t in trades)
        wr = len(wins) / n * 100
        avg_w = np.mean([t["net_pnl"] for t in wins]) if wins else 0
        avg_l = np.mean([t["net_pnl"] for t in losses]) if losses else 0
        # Max drawdown from equity curve
        sorted_by_exit = sorted(trades, key=lambda t: t["exit_ts"])
        equity_curve = np.cumsum([t["net_pnl"] for t in sorted_by_exit])
        peak = np.maximum.accumulate(equity_curve)
        drawdown = equity_curve - peak
        max_dd = drawdown.min() if len(drawdown) > 0 else 0
        return {
            "label": label, "n": n, "wr": wr, "net": net, "avg_w": avg_w, "avg_l": avg_l, "max_dd": max_dd
        }

    print("\n" + "=" * 65)
    print("  RESULTADO GLOBAL")
    print("=" * 65)

    s_unc = stats(all_trades_uncapped, "sin cap")
    s_cap = stats(kept, f"con cap {max_concurrent}")

    print(f"\n{'':25} {'Sin cap':>15}  {'Con cap ' + str(max_concurrent):>15}")
    print(f"{'Trades':<25} {s_unc['n']:>15}  {s_cap['n']:>15}")
    print(f"{'Winrate':<25} {s_unc['wr']:>14.1f}% {s_cap['wr']:>14.1f}%")
    print(f"{'Net PnL':<25} ${s_unc['net']:>14.2f} ${s_cap['net']:>14.2f}")
    print(f"{'Avg win':<25} ${s_unc['avg_w']:>14.2f} ${s_cap['avg_w']:>14.2f}")
    print(f"{'Avg loss':<25} ${s_unc['avg_l']:>14.2f} ${s_cap['avg_l']:>14.2f}")
    print(f"{'Max drawdown':<25} ${s_unc['max_dd']:>14.2f} ${s_cap['max_dd']:>14.2f}")
    print(f"\nTrades descartados por cap: {len(dropped)} ({len(dropped)/len(all_trades_uncapped)*100:.1f}%)")

    # ── Breakdown mensual (con cap) ──────────────────────────────────
    by_month = group_by_month(kept)
    print("\n" + "=" * 65)
    print("  BREAKDOWN MENSUAL (con cap)")
    print("=" * 65)
    print(f"{'Mes':<10}  {'Trades':>7}  {'WR%':>6}  {'Net PnL':>12}  {'Cumul':>12}")
    print("-" * 55)
    cumul = 0
    for month in sorted(by_month.keys()):
        month_trades = by_month[month]
        n = len(month_trades)
        wins = sum(1 for t in month_trades if t["net_pnl"] > 0)
        wr = wins / n * 100 if n else 0
        net = sum(t["net_pnl"] for t in month_trades)
        cumul += net
        emoji = "🟢" if net > 0 else ("🔴" if net < -5 else "🟡")
        print(f"{month:<10}  {n:>7}  {wr:>6.1f}  ${net:>+11.2f}  ${cumul:>+11.2f} {emoji}")

    # ── Top / Bottom pares ────────────────────────────────────────────
    if pair_stats:
        print("\n" + "=" * 65)
        print("  TOP 10 PARES")
        print("=" * 65)
        for p in sorted(pair_stats, key=lambda x: -x["net_pnl"])[:10]:
            print(f"  {p['pair']:<25}  {p['trades']:>4} trades  WR={p['wr']:>5.1f}%  ${p['net_pnl']:>+9.2f}")

        print("\n  BOTTOM 5 PARES")
        for p in sorted(pair_stats, key=lambda x: x["net_pnl"])[:5]:
            print(f"  {p['pair']:<25}  {p['trades']:>4} trades  WR={p['wr']:>5.1f}%  ${p['net_pnl']:>+9.2f}")

    # ── Extrapolación mensual promedio ────────────────────────────────
    n_months = len(by_month) if by_month else 1
    monthly_avg = s_cap['net'] / n_months
    print("\n" + "=" * 65)
    print("  RESUMEN EXTRAPOLADO")
    print("=" * 65)
    print(f"  Total período: {n_months} meses")
    print(f"  PnL total:     ${s_cap['net']:+.2f}")
    print(f"  PnL/mes prom:  ${monthly_avg:+.2f}")
    print(f"  Max drawdown:  ${s_cap['max_dd']:.2f}")
    print(f"  Ratio DD/PnL:  {abs(s_cap['max_dd']/max(s_cap['net'],1))*100:.1f}%")


def fetch_candles_with_ts(market, n_bars, use_cache=True):
    """
    Similar a fetch_candles pero retorna [(startedAt_iso, close_price), ...]
    para poder alinear por timestamp entre pares.
    """
    import json
    import time
    import requests
    from backtest import _cache_path, INDEXER

    cache_file = Path(str(_cache_path(market, "1HOUR", n_bars)) + ".ts.json")
    if use_cache and cache_file.exists():
        age_h = (time.time() - cache_file.stat().st_mtime) / 3600.0
        if age_h < 24.0:  # cache más largo para yearly
            try:
                with open(cache_file) as f:
                    return json.load(f)
            except Exception:
                pass

    to_dt = datetime.now(timezone.utc)
    all_candles = []
    current_to = to_dt
    batch_size = 100

    max_batches = (n_bars // batch_size) + 5
    for _ in range(max_batches):
        url = f"{INDEXER}/v4/candles/perpetualMarkets/{market}"
        params = {
            "resolution": "1HOUR",
            "limit": batch_size,
            "toISO": current_to.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        }
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            candles = resp.json().get("candles", [])
        except Exception as e:
            if not all_candles:
                return None
            break

        if not candles:
            break

        all_candles.extend(candles)
        oldest_in_batch = min(c["startedAt"] for c in candles)
        current_to = datetime.fromisoformat(oldest_in_batch.replace("Z", "+00:00")) - timedelta(seconds=1)

        if len(all_candles) >= n_bars + 50:
            break
        time.sleep(0.15)  # rate limit friendlier for large fetches

    if not all_candles:
        return None

    # Deduplicar y ordenar
    seen = set()
    unique = []
    for c in all_candles:
        k = c["startedAt"]
        if k not in seen:
            seen.add(k)
            unique.append(c)

    unique.sort(key=lambda x: x["startedAt"])
    result = [(c["startedAt"], float(c["close"])) for c in unique[-n_bars:]]

    try:
        with open(cache_file, "w") as f:
            json.dump(result, f)
    except Exception:
        pass

    return result


if __name__ == "__main__":
    main()
