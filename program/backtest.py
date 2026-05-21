#!/usr/bin/env python3
"""
backtest.py — Simulador offline de la estrategia stat-arb para dYdX v4.

Lee cointegrated_pairs.csv, descarga candles históricos del indexer,
simula la estrategia y puede hacer grid search de parámetros.

Uso:
    python3 backtest.py                          # parámetros actuales, todos los pares
    python3 backtest.py --grid                   # grid search (tarda ~5-10 min)
    python3 backtest.py --top 30                 # solo los 30 mejores pares del CSV
    python3 backtest.py --pair "ADA-USD/MNT-USD" # un solo par
    python3 backtest.py --grid --top 20          # grid en los 20 mejores pares

Requiere:
    pip install requests pandas numpy statsmodels tabulate --break-system-packages
"""

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
CSV_PATH   = SCRIPT_DIR / "cointegrated_pairs.csv"
CACHE_DIR  = SCRIPT_DIR / "backtest_cache"
CACHE_DIR.mkdir(exist_ok=True)

# ── dYdX indexer (testnet, same as constants.py) ──────────────────────────────
INDEXER = "https://indexer.v4testnet.dydx.exchange"

# ── Fee constants ─────────────────────────────────────────────────────────────
TAKER_FEE_BPS = 0.0005   # 0.05% per leg
RT_FEE_MULT   = 2 * TAKER_FEE_BPS  # round-trip = open + close, both legs

# ── Default simulation parameters (mirrors constants.py) ──────────────────────
DEFAULT_PARAMS = {
    "zscore_thresh":    2.0,
    "z_tp":             0.7,
    "z_sl_delta":       1.5,
    "trail_tp":         True,
    "trail_z_pullback": 0.3,
    "trail_z_floor":    0.15,
    "tp_confirm":       2,
    "usd_per_trade":    500.0,
    "z_score_window":   168,   # bars for rolling z-score (168h = 7 días)
}

# ── Grid search space ─────────────────────────────────────────────────────────
GRID = {
    "zscore_thresh":    [1.5, 2.0, 2.5, 3.0],
    "z_sl_delta":       [1.0, 1.5, 2.0, 2.5],
    "z_tp":             [0.3, 0.5, 0.7, 1.0],
    "trail_z_pullback": [0.2, 0.3, 0.5],
}

# ─────────────────────────────────────────────────────────────────────────────
# CANDLE FETCHING
# ─────────────────────────────────────────────────────────────────────────────

def _cache_path(market: str, resolution: str, n_bars: int) -> Path:
    safe = market.replace("/", "_").replace("-", "_")
    return CACHE_DIR / f"{safe}_{resolution}_{n_bars}.json"


def fetch_candles(market: str, resolution: str = "1HOUR", n_bars: int = 720,
                  use_cache: bool = True, cache_max_age_h: float = 6.0) -> Optional[list]:
    """
    Descarga hasta n_bars velas del indexer de dYdX.
    Cachea el resultado localmente para no re-descargar en el grid search.
    Retorna lista de floats (close prices), ordenada de más vieja a más nueva.
    Retorna None si hay error irrecuperable.
    """
    cache_file = _cache_path(market, resolution, n_bars)

    # Intentar leer caché si existe y no está vencida
    if use_cache and cache_file.exists():
        age_h = (time.time() - cache_file.stat().st_mtime) / 3600.0
        if age_h < cache_max_age_h:
            try:
                with open(cache_file) as f:
                    return json.load(f)
            except Exception:
                pass

    # Calcular rango de fechas: desde hace N horas hasta ahora
    to_dt   = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(hours=n_bars + 10)  # +10 para margen

    # dYdX indexer devuelve max 100 velas por request → paginar
    all_candles = []
    batch_size  = 100
    current_to  = to_dt

    for _ in range((n_bars // batch_size) + 2):
        url    = f"{INDEXER}/v4/candles/perpetualMarkets/{market}"
        params = {
            "resolution": resolution,
            "limit":      batch_size,
            "toISO":      current_to.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            candles = resp.json().get("candles", [])
        except Exception as e:
            if not all_candles:
                return None
            break

        if not candles:
            break

        all_candles.extend(candles)

        # Avanzar hacia atrás en el tiempo
        oldest_in_batch = min(c["startedAt"] for c in candles)
        current_to      = datetime.fromisoformat(oldest_in_batch.replace("Z", "+00:00"))
        current_to     -= timedelta(seconds=1)

        if current_to < from_dt or len(all_candles) >= n_bars + 50:
            break

        time.sleep(0.1)  # rate limit

    if not all_candles:
        return None

    # Deduplicar y ordenar de más viejo a más nuevo
    seen = set()
    unique = []
    for c in all_candles:
        k = c["startedAt"]
        if k not in seen:
            seen.add(k)
            unique.append(c)

    unique.sort(key=lambda x: x["startedAt"])

    closes = [float(c["close"]) for c in unique[-n_bars:]]

    if use_cache:
        try:
            with open(cache_file, "w") as f:
                json.dump(closes, f)
        except Exception:
            pass

    return closes


# ─────────────────────────────────────────────────────────────────────────────
# SIMULACIÓN DE UN PAR
# ─────────────────────────────────────────────────────────────────────────────

def simulate_pair(prices_m1: list, prices_m2: list, hedge_ratio: float,
                  params: dict) -> list:
    """
    Simula la estrategia en un par dado con precios históricos.
    Retorna lista de trades con campos:
        entry_z, exit_z, hold_bars, pnl_gross, fees, net_pnl, close_reason
    """
    p1 = np.array(prices_m1, dtype=float)
    p2 = np.array(prices_m2, dtype=float)

    if len(p1) < 2 or len(p2) < 2:
        return []

    n = min(len(p1), len(p2))
    p1 = p1[-n:]
    p2 = p2[-n:]

    spread = p1 - hedge_ratio * p2

    window      = int(params.get("z_score_window", 168))
    USD         = float(params.get("usd_per_trade", 500.0))
    Z_ENTRY     = float(params["zscore_thresh"])
    Z_TP        = float(params["z_tp"])
    Z_SL_DELTA  = float(params["z_sl_delta"])
    TRAIL       = bool(params.get("trail_tp", True))
    PULLBACK    = float(params.get("trail_z_pullback", 0.3))
    FLOOR       = float(params.get("trail_z_floor", 0.15))
    TP_CONFIRM  = int(params.get("tp_confirm", 2))
    MAX_BARS    = 500  # hard time stop (evita trades eternos en backtest)

    trades = []
    state      = "flat"
    entry_z    = 0.0
    entry_idx  = 0
    entry_p1   = 0.0
    entry_p2   = 0.0
    open_fee   = 0.0
    entry_side = "short"  # "short" = short m1 / long m2
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
                best_z           = abs(z)
                tp_confirm_count = 0
                entry_side       = "short" if z > 0 else "long"
                open_fee         = USD * 2 * TAKER_FEE_BPS

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

            close_reason = None

            # ── 1. Z Stop-Loss ────────────────────────────────────────────
            sl_level = abs(entry_z) + Z_SL_DELTA
            if abs(z) >= sl_level:
                close_reason = "Z_SL"

            # ── 2. Take-Profit (trailing o fijo) ─────────────────────────
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
                        # Profit gate: cierra en ganancia O si z revirtió pero PnL es negativo
                        if net_pnl > 0 or pnl_gross < 0:
                            close_reason = "TP" if net_pnl > 0 else "TP_LOSS"
                else:
                    tp_confirm_count = 0

            # ── 3. Hard time stop (solo backtest) ─────────────────────────
            if close_reason is None and hold >= MAX_BARS:
                close_reason = "TIME_STOP"

            if close_reason:
                trades.append({
                    "entry_z":    entry_z,
                    "exit_z":     z,
                    "hold_bars":  hold,
                    "pnl_gross":  round(pnl_gross, 4),
                    "fees":       round(total_fees, 4),
                    "net_pnl":    round(net_pnl, 4),
                    "close_reason": close_reason,
                    "entry_side": entry_side,
                })
                state            = "flat"
                tp_confirm_count = 0

    return trades


# ─────────────────────────────────────────────────────────────────────────────
# MÉTRICAS
# ─────────────────────────────────────────────────────────────────────────────

def compute_metrics(all_trades: list, label: str = "") -> dict:
    if not all_trades:
        return {
            "label": label, "n_trades": 0, "win_rate": 0, "profit_factor": 0,
            "avg_win": 0, "avg_loss": 0, "net_pnl": 0, "total_fees": 0,
            "max_drawdown": 0, "sharpe": 0, "ev_per_trade": 0,
            "tp_pct": 0, "sl_pct": 0, "time_pct": 0,
        }

    df = pd.DataFrame(all_trades)

    wins   = df[df["net_pnl"] > 0]
    losses = df[df["net_pnl"] <= 0]

    n          = len(df)
    win_rate   = len(wins) / n if n else 0
    gross_win  = wins["net_pnl"].sum()
    gross_loss = abs(losses["net_pnl"].sum())
    pf         = gross_win / gross_loss if gross_loss > 0 else (999.0 if gross_win > 0 else 0.0)
    avg_win    = wins["net_pnl"].mean() if len(wins) else 0.0
    avg_loss   = losses["net_pnl"].mean() if len(losses) else 0.0
    net_pnl    = df["net_pnl"].sum()
    total_fees = df["fees"].sum()
    ev         = net_pnl / n if n else 0.0

    # Sharpe (usando net_pnl por trade como retornos)
    if n > 1 and df["net_pnl"].std() > 0:
        sharpe = (df["net_pnl"].mean() / df["net_pnl"].std()) * np.sqrt(n)
    else:
        sharpe = 0.0

    # Max drawdown sobre curva de equity acumulada
    equity = df["net_pnl"].cumsum()
    peak   = equity.cummax()
    dd     = (equity - peak)
    max_dd = dd.min()

    # Distribución de motivos de cierre
    reason_pct = df["close_reason"].value_counts(normalize=True).to_dict()

    return {
        "label":         label,
        "n_trades":      n,
        "win_rate":      round(win_rate * 100, 1),
        "profit_factor": round(pf, 3),
        "avg_win":       round(avg_win, 3),
        "avg_loss":      round(avg_loss, 3),
        "net_pnl":       round(net_pnl, 2),
        "total_fees":    round(total_fees, 2),
        "max_drawdown":  round(max_dd, 2),
        "sharpe":        round(sharpe, 3),
        "ev_per_trade":  round(ev, 3),
        "tp_pct":        round(reason_pct.get("TP", 0) * 100, 1),
        "sl_pct":        round(reason_pct.get("Z_SL", 0) * 100, 1),
        "time_pct":      round(reason_pct.get("TIME_STOP", 0) * 100, 1),
    }


# ─────────────────────────────────────────────────────────────────────────────
# BACKTEST COMPLETO (todos los pares)
# ─────────────────────────────────────────────────────────────────────────────

def run_backtest(pairs_df: pd.DataFrame, params: dict,
                 n_bars: int = 720, verbose: bool = True) -> dict:
    """
    Corre la simulación para todos los pares en pairs_df.
    Retorna dict con métricas agregadas y lista de todos los trades.
    """
    all_trades = []
    pair_results = []
    n_pairs    = len(pairs_df)
    fetched    = 0
    skipped    = 0

    markets_cache = {}  # evitar re-descargar el mismo mercado

    for idx, row in pairs_df.iterrows():
        m1 = str(row.get("sym_1", row.get("market_1", ""))).strip()
        m2 = str(row.get("sym_2", row.get("market_2", ""))).strip()
        hr = float(row.get("hedge_ratio", 1.0))

        if not m1 or not m2:
            skipped += 1
            continue

        if verbose:
            print(f"  [{fetched+1}/{n_pairs}] {m1}/{m2} (hr={hr:.4f})...", end=" ", flush=True)

        # Descargar (o leer de caché)
        if m1 not in markets_cache:
            markets_cache[m1] = fetch_candles(m1, n_bars=n_bars)
        if m2 not in markets_cache:
            markets_cache[m2] = fetch_candles(m2, n_bars=n_bars)

        p1 = markets_cache[m1]
        p2 = markets_cache[m2]

        if p1 is None or p2 is None or len(p1) < 200 or len(p2) < 200:
            if verbose:
                print("SKIP (sin datos)")
            skipped += 1
            continue

        trades = simulate_pair(p1, p2, hr, params)
        fetched += 1

        if verbose:
            net = sum(t["net_pnl"] for t in trades)
            print(f"{len(trades)} trades | net=${net:.2f}")

        all_trades.extend(trades)
        if trades:
            m = compute_metrics(trades, label=f"{m1}/{m2}")
            m["pair"] = f"{m1}/{m2}"
            pair_results.append(m)

    metrics = compute_metrics(all_trades, label="TOTAL")
    metrics["pairs_run"]     = fetched
    metrics["pairs_skipped"] = skipped

    return {
        "metrics":      metrics,
        "all_trades":   all_trades,
        "pair_results": pair_results,
        "params":       params,
    }


# ─────────────────────────────────────────────────────────────────────────────
# GRID SEARCH
# ─────────────────────────────────────────────────────────────────────────────

def run_grid_search(pairs_df: pd.DataFrame, n_bars: int = 720) -> pd.DataFrame:
    """
    Prueba todas las combinaciones de parámetros en GRID.
    Retorna DataFrame ordenado por EV por trade descendente.
    """
    from itertools import product

    keys   = list(GRID.keys())
    values = list(GRID.values())
    combos = list(product(*values))

    print(f"\n{'='*60}")
    print(f"GRID SEARCH: {len(combos)} combinaciones × {len(pairs_df)} pares")
    print(f"{'='*60}\n")

    # Pre-descargar todos los mercados una sola vez
    markets_needed = set()
    for _, row in pairs_df.iterrows():
        markets_needed.add(str(row.get("sym_1", row.get("market_1", ""))).strip())
        markets_needed.add(str(row.get("sym_2", row.get("market_2", ""))).strip())

    print(f"Pre-descargando {len(markets_needed)} mercados...")
    markets_cache = {}
    for i, mkt in enumerate(sorted(markets_needed)):
        print(f"  [{i+1}/{len(markets_needed)}] {mkt}...", end=" ", flush=True)
        data = fetch_candles(mkt, n_bars=n_bars)
        if data and len(data) >= 200:
            markets_cache[mkt] = data
            print(f"{len(data)} bars")
        else:
            print("SKIP")
    print()

    results = []

    for ci, combo in enumerate(combos):
        p = dict(DEFAULT_PARAMS)  # base params
        for k, v in zip(keys, combo):
            p[k] = v

        label = " | ".join(f"{k}={v}" for k, v in zip(keys, combo))
        all_trades = []

        for _, row in pairs_df.iterrows():
            m1 = str(row.get("sym_1", row.get("market_1", ""))).strip()
            m2 = str(row.get("sym_2", row.get("market_2", ""))).strip()
            hr = float(row.get("hedge_ratio", 1.0))

            p1 = markets_cache.get(m1)
            p2 = markets_cache.get(m2)
            if p1 is None or p2 is None:
                continue

            trades = simulate_pair(p1, p2, hr, p)
            all_trades.extend(trades)

        m = compute_metrics(all_trades, label=label)
        for k, v in zip(keys, combo):
            m[k] = v
        results.append(m)

        if (ci + 1) % 20 == 0 or (ci + 1) == len(combos):
            print(f"  {ci+1}/{len(combos)} combos procesados...", flush=True)

    df = pd.DataFrame(results)
    df = df.sort_values("ev_per_trade", ascending=False)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────────────────────

def print_metrics(m: dict, title: str = ""):
    w = 52
    print(f"\n{'─'*w}")
    if title:
        print(f"  {title}")
    print(f"{'─'*w}")
    print(f"  Trades:         {m['n_trades']}")
    print(f"  Win rate:       {m['win_rate']}%")
    print(f"  Profit factor:  {m['profit_factor']}")
    print(f"  EV / trade:     ${m['ev_per_trade']}")
    print(f"  Avg win:        ${m['avg_win']}")
    print(f"  Avg loss:       ${m['avg_loss']}")
    print(f"  Net PnL:        ${m['net_pnl']}")
    print(f"  Total fees:     ${m['total_fees']}")
    print(f"  Max drawdown:   ${m['max_drawdown']}")
    print(f"  Sharpe:         {m['sharpe']}")
    print(f"  TP exits:       {m['tp_pct']}%")
    print(f"  SL exits:       {m['sl_pct']}%")
    print(f"  Time stops:     {m['time_pct']}%")
    print(f"{'─'*w}")


def print_grid_results(df: pd.DataFrame, top_n: int = 15):
    print(f"\n{'='*80}")
    print(f"  TOP {top_n} COMBINACIONES DE PARÁMETROS (por EV/trade)")
    print(f"{'='*80}")

    cols = ["zscore_thresh", "z_sl_delta", "z_tp", "trail_z_pullback",
            "n_trades", "win_rate", "profit_factor", "ev_per_trade",
            "net_pnl", "max_drawdown", "sharpe"]

    show_cols = [c for c in cols if c in df.columns]
    top       = df.head(top_n)[show_cols].copy()

    # Formatear para lectura
    top["win_rate"]      = top["win_rate"].apply(lambda x: f"{x}%")
    top["ev_per_trade"]  = top["ev_per_trade"].apply(lambda x: f"${x:.3f}")
    top["net_pnl"]       = top["net_pnl"].apply(lambda x: f"${x:.2f}")
    top["max_drawdown"]  = top["max_drawdown"].apply(lambda x: f"${x:.2f}")

    try:
        from tabulate import tabulate
        print(tabulate(top, headers="keys", tablefmt="simple", showindex=False))
    except ImportError:
        print(top.to_string(index=False))

    print()
    best = df.iloc[0]
    print("  MEJOR COMBINACIÓN:")
    grid_keys = list(GRID.keys())
    for k in grid_keys:
        if k in best:
            print(f"    {k} = {best[k]}")
    print(f"  → EV/trade=${best['ev_per_trade']:.3f} | WinRate={best['win_rate']}% | "
          f"PF={best['profit_factor']:.2f} | Net=${best['net_pnl']:.2f}")


def print_pair_breakdown(pair_results: list, top_n: int = 10):
    if not pair_results:
        return
    df = pd.DataFrame(pair_results).sort_values("net_pnl", ascending=False)
    print(f"\n{'='*70}")
    print(f"  TOP {top_n} PARES POR NET PNL")
    print(f"{'='*70}")
    cols = ["pair", "n_trades", "win_rate", "profit_factor", "ev_per_trade", "net_pnl"]
    show = [c for c in cols if c in df.columns]
    try:
        from tabulate import tabulate
        print(tabulate(df.head(top_n)[show], headers="keys", tablefmt="simple", showindex=False))
    except ImportError:
        print(df.head(top_n)[show].to_string(index=False))

    print(f"\n  BOTTOM {top_n} PARES (peores)")
    print(f"{'─'*70}")
    try:
        from tabulate import tabulate
        print(tabulate(df.tail(top_n)[show], headers="keys", tablefmt="simple", showindex=False))
    except ImportError:
        print(df.tail(top_n)[show].to_string(index=False))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def load_pairs(csv_path: Path, top_n: Optional[int] = None,
               single_pair: Optional[str] = None) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    # Normalizar nombres de columnas → siempre sym_1 / sym_2
    col_map = {}
    for c in df.columns:
        cl = c.strip().lower()
        if cl in ("sym_1", "market_1", "ticker_1", "base_market"):
            col_map[c] = "sym_1"
        elif cl in ("sym_2", "market_2", "ticker_2", "quote_market"):
            col_map[c] = "sym_2"
        elif cl in ("hedge_ratio",):
            col_map[c] = "hedge_ratio"
        elif cl in ("half_life",):
            col_map[c] = "half_life"
    df = df.rename(columns=col_map)

    # Eliminar filas con mercados vacíos o inválidos
    df = df[df["sym_1"].notna() & df["sym_2"].notna()]
    df = df[df["sym_1"].str.contains("-USD") & df["sym_2"].str.contains("-USD")]

    if single_pair:
        parts = single_pair.split("/")
        if len(parts) == 2:
            m1, m2 = parts[0].strip(), parts[1].strip()
            mask = (
                ((df["sym_1"] == m1) & (df["sym_2"] == m2)) |
                ((df["sym_1"] == m2) & (df["sym_2"] == m1))
            )
            df = df[mask]

    if top_n and not single_pair:
        df = df.head(top_n)

    return df.reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(description="dYdX stat-arb backtester")
    parser.add_argument("--grid",    action="store_true",  help="Correr grid search")
    parser.add_argument("--top",     type=int, default=None, help="Usar solo los N primeros pares del CSV")
    parser.add_argument("--pair",    type=str, default=None, help='Par específico: "ADA-USD/MNT-USD"')
    parser.add_argument("--bars",    type=int, default=720,  help="Número de barras históricas a descargar (default: 720 = 30 días)")
    parser.add_argument("--no-cache", action="store_true",  help="Ignorar caché y re-descargar todo")
    args = parser.parse_args()

    if not CSV_PATH.exists():
        print(f"ERROR: No se encontró {CSV_PATH}")
        print("Corre primero con FIND_COINTEGRATED=True para generar el CSV.")
        sys.exit(1)

    pairs_df = load_pairs(CSV_PATH, top_n=args.top, single_pair=args.pair)
    if pairs_df.empty:
        print("No se encontraron pares. Verifica --pair o --top.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  dYdX STAT-ARB BACKTESTER")
    print(f"  {len(pairs_df)} pares | {args.bars} barras por mercado")
    print(f"  Caché: {'desactivada' if args.no_cache else 'activa (~{:.0f}h)'.format(6)}")
    print(f"{'='*60}")

    if args.grid:
        grid_df = run_grid_search(pairs_df, n_bars=args.bars)
        print_grid_results(grid_df, top_n=15)

        # También corre con los mejores parámetros y muestra breakdown por par
        best = grid_df.iloc[0]
        best_params = dict(DEFAULT_PARAMS)
        for k in GRID:
            if k in best:
                best_params[k] = best[k]

        print(f"\nBacktest detallado con mejores parámetros...")
        result = run_backtest(pairs_df, best_params, n_bars=args.bars, verbose=False)
        print_metrics(result["metrics"], title="RESULTADO CON MEJORES PARÁMETROS")
        print_pair_breakdown(result["pair_results"])

        # Guardar resultados
        out_path = SCRIPT_DIR / "backtest_grid_results.csv"
        grid_df.to_csv(out_path, index=False)
        print(f"\n  Resultados guardados en: {out_path}")

    else:
        # Backtest simple con parámetros actuales
        params = dict(DEFAULT_PARAMS)
        print(f"\nParámetros:")
        for k, v in sorted(params.items()):
            if k in GRID or k in ("usd_per_trade", "tp_confirm", "z_score_window"):
                print(f"  {k} = {v}")
        print()

        result = run_backtest(pairs_df, params, n_bars=args.bars, verbose=True)

        print_metrics(result["metrics"], title="RESULTADO GLOBAL")
        print_pair_breakdown(result["pair_results"])

        # Guardar trades individuales
        if result["all_trades"]:
            trades_df = pd.DataFrame(result["all_trades"])
            out_path  = SCRIPT_DIR / "backtest_trades.csv"
            trades_df.to_csv(out_path, index=False)
            print(f"\n  Trades guardados en: {out_path}")

        # Comparar trail_tp=True vs False
        print(f"\n{'─'*52}")
        print("  COMPARACIÓN: Trailing TP vs TP fijo")
        print(f"{'─'*52}")
        for trail in [True, False]:
            p2 = dict(params)
            p2["trail_tp"] = trail
            r2 = run_backtest(pairs_df, p2, n_bars=args.bars, verbose=False)
            label = "Trailing TP" if trail else "TP fijo z≤0.7"
            m = r2["metrics"]
            print(f"  {label:15s}: EV=${m['ev_per_trade']:+.3f} | "
                  f"WR={m['win_rate']}% | PF={m['profit_factor']:.2f} | "
                  f"Net=${m['net_pnl']:.2f} | Trades={m['n_trades']}")


if __name__ == "__main__":
    main()
