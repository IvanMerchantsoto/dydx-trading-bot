#!/usr/bin/env python3
"""
backtest_wf.py — Backtest WALK-FORWARD honesto (auditoría fase 4).

Corrige los defectos del backtest anterior (Q1-Q6):
  Q3 look-ahead   : el hedge_ratio y la SELECCIÓN de pares se hacen SÓLO con la
                    ventana de ENTRENAMIENTO; el trading ocurre en la ventana de
                    TEST siguiente (out-of-sample real, rodante).
  Q1/Q5 costes    : modela fees + slippage + cruce de spread + funding (bps
                    configurables por pierna), no sólo fees al close.
  Q2 Sharpe       : Sharpe ANUALIZADO correcto (mean/std × √trades_por_año),
                    no el artefacto ×√n_trades.
  Q4 paridad      : replica la lógica de salida del live (HARD_SL monetario,
                    Z_SL, TP_CROSSED_ZERO, TP con doble confirmación + profit
                    gate) importando los MISMOS parámetros de constants.
  Q6 alineación   : las dos series se alinean por TIMESTAMP común, no por cola.

Paridad de selección: usa las MISMAS funciones que el bot vivo
(func_cointegration.calculate_cointegration / _half_life / _hurst_exponent /
calculate_zscore) sobre la ventana de entrenamiento.

Limitación honesta: los costes de spread/slippage son un PARÁMETRO (no hay
orderbook histórico). Ajusta --cost-bps-per-leg al spread real de los pares que
operas (mainnet altcoins: 40-200 bps). Con --cost-bps-per-leg 0 obtienes el
límite superior irreal (sólo para comparar con el backtest viejo).

SOLO LECTURA / offline (descarga candles del indexer con caché). No opera.

Uso:
    python3 backtest_wf.py --bars 2160 --train 336 --test 168 --cost-bps-per-leg 40
    python3 backtest_wf.py --top 40 --cost-bps-per-leg 60 --funding-bps-day 5
"""

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from constants import (
    ZSCORE_THRESH, Z_TP, Z_SL_DELTA, TP_CONFIRM_CHECKS,
    USD_PER_TRADE, HARD_SL_USD, HARD_SL_PCT,
    MAX_HALF_LIFE, WINDOW, HEDGE_RATIO_LOG_MAX, HURST_MAX,
    TAKER_FEE_BPS, INDEXER,
)
from func_cointegration import (
    calculate_cointegration, calculate_half_life,
    calculate_hurst_exponent, calculate_zscore,
)

SCRIPT_DIR = Path(__file__).parent
CSV_PATH = SCRIPT_DIR / "cointegrated_pairs.csv"
CACHE_DIR = SCRIPT_DIR / "backtest_cache"
CACHE_DIR.mkdir(exist_ok=True)
BARS_PER_YEAR = 24 * 365  # 1HOUR

_SESSION = requests.Session()  # reutiliza conexiones (mucho más rápido)


# ─────────────────────────────────────────────────────────────────────────────
# Candles con timestamp (para alineación correcta, Q6)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_candles_ts(market, n_bars=2160, cache_max_age_h=12.0):
    cache_file = CACHE_DIR / f"{market.replace('-', '_')}_1HOUR_{n_bars}_wf.json"
    if cache_file.exists():
        age_h = (time.time() - cache_file.stat().st_mtime) / 3600.0
        if age_h < cache_max_age_h:
            try:
                return json.load(open(cache_file))
            except Exception:
                pass

    to_dt = datetime.now(timezone.utc)
    from_dt = to_dt - timedelta(hours=n_bars + 10)
    out = {}
    current_to = to_dt
    base = INDEXER.rstrip("/")
    max_pages = (n_bars // 100) + 2
    empty_streak = 0
    for _ in range(max_pages):
        url = f"{base}/v4/candles/perpetualMarkets/{market}"
        params = {"resolution": "1HOUR", "limit": 100,
                  "toISO": current_to.strftime("%Y-%m-%dT%H:%M:%S.000Z")}
        candles = None
        # retry con backoff en 429/5xx/timeout
        for attempt in range(4):
            try:
                r = _SESSION.get(url, params=params, timeout=10)
                if r.status_code == 429 or r.status_code >= 500:
                    time.sleep(0.6 * (2 ** attempt))
                    continue
                r.raise_for_status()
                candles = r.json().get("candles", [])
                break
            except Exception:
                time.sleep(0.5 * (2 ** attempt))
                continue
        if not candles:
            empty_streak += 1
            if empty_streak >= 2:
                break   # el indexer ya no tiene más historia hacia atrás
            break
        for c in candles:
            out[c["startedAt"]] = float(c["close"])
        oldest = min(c["startedAt"] for c in candles)
        current_to = datetime.fromisoformat(oldest.replace("Z", "+00:00")) - timedelta(seconds=1)
        if current_to < from_dt:
            break
        time.sleep(0.05)
    if not out:
        return None
    series = sorted(out.items())  # [(ts, close)]
    try:
        json.dump(series, open(cache_file, "w"))
    except Exception:
        pass
    return series


def prefetch_markets(markets, n_bars, workers=6):
    """
    Descarga en paralelo (thread pool) los mercados ÚNICOS con barra de
    progreso. Cada mercado pagina internamente en serie; el paralelismo es
    entre mercados. Devuelve {market: series|None}.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    result = {}
    total = len(markets)
    done = 0
    print(f"Descargando {total} mercados únicos (≈{(n_bars//100)+1} páginas c/u, {workers} en paralelo)...", flush=True)
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_candles_ts, m, n_bars): m for m in markets}
        for fut in as_completed(futs):
            m = futs[fut]
            try:
                result[m] = fut.result()
            except Exception:
                result[m] = None
            done += 1
            if done % 5 == 0 or done == total:
                ok = sum(1 for v in result.values() if v)
                print(f"   {done}/{total} mercados  ({ok} OK, {time.time()-t0:.0f}s)", flush=True)
    return result


def align_by_ts(series_1, series_2):
    """Alinea dos series [(ts, close)] por timestamp común (Q6 fix)."""
    d1 = dict(series_1)
    d2 = dict(series_2)
    common = sorted(set(d1) & set(d2))
    ts = common
    p1 = np.array([d1[t] for t in common], dtype=float)
    p2 = np.array([d2[t] for t in common], dtype=float)
    return ts, p1, p2


def _zscore(spread, window):
    """
    Z-score rolling con ventana PARAMETRIZABLE (para el sweep de WINDOW).
    Idéntico en forma a func_cointegration.calculate_zscore pero con window
    explícito: z_t = (spread_t − mean_{t-window..t}) / std_{t-window..t}.
    Sin look-ahead (rolling causal). Devuelve np.array (NaN en el warm-up).
    """
    s = pd.Series(spread)
    mean = s.rolling(window=window).mean()
    std = s.rolling(window=window).std()
    return ((s - mean) / std).values


# ─────────────────────────────────────────────────────────────────────────────
# Selección de pares en la ventana de ENTRENAMIENTO (paridad con live)
# ─────────────────────────────────────────────────────────────────────────────
def train_select(p1_train, p2_train):
    """
    Replica el filtro de cointegración del bot sobre la ventana de train.
    Returns (passes: bool, hedge_ratio: float, half_life: float).
    """
    if len(p1_train) < 40 or len(p2_train) < 40:
        return False, None, None
    try:
        coint_flag, hedge_ratio, half_life, r_sq, p_val = calculate_cointegration(
            list(p1_train), list(p2_train)
        )
    except Exception:
        return False, None, None
    if coint_flag != 1:
        return False, None, None
    if half_life is None or np.isnan(half_life) or half_life <= 3 or half_life > MAX_HALF_LIFE:
        return False, None, None
    if hedge_ratio is None or hedge_ratio <= 0 or abs(np.log10(abs(hedge_ratio))) > HEDGE_RATIO_LOG_MAX:
        return False, None, None
    spread = np.array(p1_train) - hedge_ratio * np.array(p2_train)
    hurst = calculate_hurst_exponent(np.diff(spread))
    if not np.isnan(hurst) and hurst >= HURST_MAX:
        return False, None, None
    return True, float(hedge_ratio), float(half_life)


# ─────────────────────────────────────────────────────────────────────────────
# Simulación en la ventana de TEST (paridad de salida con func_exit_pairs)
# ─────────────────────────────────────────────────────────────────────────────
def simulate_test(p1_full, p2_full, test_start, test_end, hedge_ratio, half_life,
                  usd, cost_bps_leg, funding_bps_day, params):
    """
    Opera SÓLO en [test_start, test_end) con los parámetros dados (params), para
    poder barrer WINDOW / z_entry / Z_TP / stop_mode. El z usa ventana rodante
    causal (sin look-ahead). Costes: (fee+slippage+½spread)/pierna × 4 + funding.

    params: window, z_entry, z_tp, z_sl_delta, tp_confirm, hard_sl_usd,
            hard_sl_pct, stop_mode ('monetary'|'zonly'), time_stop_bars.
    """
    window = int(params["window"])
    z_entry_thr = float(params["z_entry"])
    z_tp = float(params["z_tp"])
    z_sl_delta = float(params["z_sl_delta"])
    tp_confirm_req = int(params["tp_confirm"])
    stop_mode = params.get("stop_mode", "monetary")
    time_stop_bars = int(params.get("time_stop_bars", 500))

    spread = p1_full - hedge_ratio * p2_full
    z_series = _zscore(spread, window)

    trades = []
    state = "flat"
    entry_z = 0.0
    entry_i = 0
    entry_p1 = entry_p2 = 0.0
    entry_side = "short"
    tp_confirm = 0
    best_z = 99.0
    notional = 2.0 * usd
    exec_cost = 4.0 * usd * (cost_bps_leg / 10_000.0)
    hard_level = max(float(params["hard_sl_usd"]), float(params["hard_sl_pct"]) * notional)

    lo = max(test_start, window + 1)
    for i in range(lo, test_end):
        z = z_series[i]
        if z is None or (isinstance(z, float) and np.isnan(z)):
            continue

        if state == "flat":
            if abs(z) >= z_entry_thr:
                state = "in_trade"
                entry_z = z
                entry_i = i
                entry_p1 = p1_full[i]
                entry_p2 = p2_full[i]
                entry_side = "short" if z > 0 else "long"
                best_z = abs(z)
                tp_confirm = 0
            continue

        # in_trade
        best_z = min(best_z, abs(z))
        hold = i - entry_i
        sz1 = usd / entry_p1
        sz2 = usd / entry_p2
        if entry_side == "short":   # short m1 / long m2
            pnl1 = (entry_p1 - p1_full[i]) * sz1
            pnl2 = (p2_full[i] - entry_p2) * sz2
        else:
            pnl1 = (p1_full[i] - entry_p1) * sz1
            pnl2 = (entry_p2 - p2_full[i]) * sz2
        pnl_gross = pnl1 + pnl2

        funding = funding_bps_day / 10_000.0 * notional * (hold / 24.0)
        net = pnl_gross - exec_cost - funding

        reason = None
        # 1) HARD_SL monetario — sólo en stop_mode='monetary'
        if stop_mode == "monetary" and pnl_gross <= -hard_level:
            reason = "HARD_SL"
        # 2) Z_SL (tesis rota) — siempre activo
        elif abs(z) >= abs(entry_z) + z_sl_delta:
            reason = "Z_SL"
        # 2b) TP_CROSSED_ZERO (gate relajado: net>0)
        elif ((entry_z > 0.1 and z < -0.1) or (entry_z < -0.1 and z > 0.1)) and net > 0:
            reason = "TP_CROSSED_ZERO"
        # 3) TP con confirmación + profit gate
        else:
            if abs(z) <= z_tp:
                tp_confirm += 1
            else:
                tp_confirm = 0
            if tp_confirm >= tp_confirm_req and net > 0:
                reason = "TP"
        if reason is None and hold >= time_stop_bars:
            reason = "TIME_STOP"

        if reason:
            trades.append({
                "entry_z": entry_z, "exit_z": z, "hold_bars": hold,
                "pnl_gross": pnl_gross, "exec_cost": exec_cost, "funding": funding,
                "net_pnl": net, "close_reason": reason, "entry_side": entry_side,
                "close_i": i,
            })
            state = "flat"
            tp_confirm = 0

    return trades


# ─────────────────────────────────────────────────────────────────────────────
# Métricas (Sharpe ANUALIZADO correcto, Q2 fix)
# ─────────────────────────────────────────────────────────────────────────────
def metrics(trades, usd, years):
    if not trades:
        return {"n": 0}
    df = pd.DataFrame(trades)
    n = len(df)
    net = df["net_pnl"]
    wins = net[net > 0]
    losses = net[net <= 0]
    gross_win = wins.sum()
    gross_loss = abs(losses.sum())
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")
    ev = net.mean()
    equity = net.cumsum()
    dd = (equity - equity.cummax()).min()

    # Sharpe ANUALIZADO: retornos por trade r=net/capital, escalados por la
    # frecuencia real de trades (trades/año). NO el ×√n_trades del viejo bug.
    r = net / usd
    trades_per_year = (n / years) if years > 0 else 0.0
    if r.std() > 0 and trades_per_year > 0:
        sharpe = (r.mean() / r.std()) * np.sqrt(trades_per_year)
    else:
        sharpe = 0.0

    reasons = df["close_reason"].value_counts(normalize=True).mul(100).round(1).to_dict()
    return {
        "n": n,
        "win_rate": round(len(wins) / n * 100, 1),
        "profit_factor": round(pf, 3),
        "ev_per_trade": round(ev, 4),
        "net_pnl": round(net.sum(), 2),
        "gross_pnl": round(df["pnl_gross"].sum(), 2),
        "total_exec_cost": round(df["exec_cost"].sum(), 2),
        "total_funding": round(df["funding"].sum(), 2),
        "max_drawdown": round(dd, 2),
        "sharpe_annual": round(sharpe, 3),
        "trades_per_year": round(trades_per_year, 1),
        "avg_hold_h": round(df["hold_bars"].mean(), 1),
        "reasons_pct": reasons,
    }


def build_params(args, window=None, z_entry=None, z_tp=None, stop_mode=None):
    return {
        "window": int(window if window is not None else (args.window or WINDOW)),
        "z_entry": float(z_entry if z_entry is not None else (args.z_entry or ZSCORE_THRESH)),
        "z_tp": float(z_tp if z_tp is not None else (args.z_tp or Z_TP)),
        "z_sl_delta": float(Z_SL_DELTA),
        "tp_confirm": int(TP_CONFIRM_CHECKS),
        "hard_sl_usd": float(HARD_SL_USD),
        "hard_sl_pct": float(HARD_SL_PCT),
        "stop_mode": stop_mode if stop_mode is not None else args.stop,
        "time_stop_bars": 500,
    }


def run_walk_forward(pairs, cache, unique_markets, args, params):
    """Walk-forward con los `params` dados. Devuelve dict con métricas y meta."""
    all_trades = []
    folds_used = 0
    selections = 0
    first_test_ts = None
    last_test_ts = None
    pairs_run = 0
    for _, row in pairs.iterrows():
        m1 = str(row.get("base_market", row.get("sym_1", ""))).strip()
        m2 = str(row.get("quote_market", row.get("sym_2", ""))).strip()
        if not m1 or not m2:
            continue
        s1 = cache.get(m1)
        s2 = cache.get(m2)
        if not s1 or not s2:
            continue
        ts, p1, p2 = align_by_ts(s1, s2)
        n = len(p1)
        if n < args.train + args.test + 5:
            continue
        pairs_run += 1
        start = 0
        while start + args.train + args.test <= n:
            tr0, tr1 = start, start + args.train
            te0, te1 = tr1, min(tr1 + args.test, n)
            passes, hr, hl = train_select(p1[tr0:tr1], p2[tr0:tr1])
            if passes:
                selections += 1
                trs = simulate_test(p1, p2, te0, te1, hr, hl,
                                    args.usd, args.cost_bps_per_leg, args.funding_bps_day, params)
                for t in trs:
                    t["pair"] = f"{m1}/{m2}"
                all_trades.extend(trs)
                if trs:
                    folds_used += 1
                    ci = trs[-1]["close_i"]
                    if 0 <= te0 < len(ts):
                        first_test_ts = ts[te0] if first_test_ts is None else min(first_test_ts, ts[te0])
                    if 0 <= ci < len(ts):
                        last_test_ts = ts[ci] if last_test_ts is None else max(last_test_ts, ts[ci])
            start += args.test

    years = 1.0
    if first_test_ts and last_test_ts:
        try:
            d0 = datetime.fromisoformat(first_test_ts.replace("Z", "+00:00"))
            d1 = datetime.fromisoformat(last_test_ts.replace("Z", "+00:00"))
            years = max((d1 - d0).total_seconds() / (365 * 86400), 1e-6)
        except Exception:
            pass
    m = metrics(all_trades, args.usd, years)
    n_unique = len(unique_markets)
    n_ok = sum(1 for v in cache.values() if v)
    return {"m": m, "years": years, "pairs_run": pairs_run, "selections": selections,
            "folds_used": folds_used, "first_test_ts": first_test_ts, "last_test_ts": last_test_ts,
            "n_unique": n_unique, "n_ok": n_ok, "n_dropped": n_unique - n_ok}


def _prefetch_for(pairs, args):
    unique_markets = set()
    for _, row in pairs.iterrows():
        m1 = str(row.get("base_market", row.get("sym_1", ""))).strip()
        m2 = str(row.get("quote_market", row.get("sym_2", ""))).strip()
        if m1 and m2:
            unique_markets.add(m1)
            unique_markets.add(m2)
    cache = prefetch_markets(sorted(unique_markets), args.bars, workers=args.workers)
    return unique_markets, cache


def run_sweep(pairs, cache, unique_markets, args):
    """Barre window × z_entry × z_tp × stop y rankea por NET PnL."""
    from itertools import product
    windows = [21, 84, 168, 504]
    z_entries = [2.7, 3.0]
    z_tps = [0.3, 0.7]
    stops = ["monetary", "zonly"]
    combos = list(product(windows, z_entries, z_tps, stops))
    print(f"SWEEP: {len(combos)} combinaciones (coste {args.cost_bps_per_leg}bps/pierna)\n")
    rows = []
    for (w, ze, zt, st) in combos:
        params = build_params(args, window=w, z_entry=ze, z_tp=zt, stop_mode=st)
        res = run_walk_forward(pairs, cache, unique_markets, args, params)
        m = res["m"]
        rows.append({
            "window": w, "z_entry": ze, "z_tp": zt, "stop": st,
            "n": m.get("n", 0), "wr": m.get("win_rate", 0), "pf": m.get("profit_factor", 0),
            "ev": m.get("ev_per_trade", 0), "net": m.get("net_pnl", 0),
            "sharpe": m.get("sharpe_annual", 0), "dd": m.get("max_drawdown", 0),
        })
    rows.sort(key=lambda r: r["net"], reverse=True)
    print(f"  {'win':>4} {'z_in':>4} {'z_tp':>4} {'stop':>8} {'n':>5} {'WR%':>6} {'PF':>6} "
          f"{'EV$':>8} {'NET$':>9} {'Shrp':>6} {'maxDD$':>8}")
    print("  " + "-" * 80)
    for r in rows:
        flag = " OK" if (r["net"] > 0 and r["ev"] > 0 and r["n"] >= 20) else ""
        print(f"  {r['window']:>4} {r['z_entry']:>4} {r['z_tp']:>4} {r['stop']:>8} "
              f"{r['n']:>5} {r['wr']:>6} {r['pf']:>6} {r['ev']:>8.4f} {r['net']:>9.2f} "
              f"{r['sharpe']:>6} {r['dd']:>8}{flag}")
    winners = [r for r in rows if r["net"] > 0 and r["ev"] > 0 and r["n"] >= 20]
    print()
    if winners:
        b = winners[0]
        print(f"  🎯 Mejor combo con EV>0 y n>=20: window={b['window']} z_entry={b['z_entry']} "
              f"z_tp={b['z_tp']} stop={b['stop']} → NET=${b['net']} EV=${b['ev']} WR={b['wr']}%")
        print(f"     (compáralo también a --cost-bps-per-leg 100 y 150 antes de decidir.)")
    else:
        print(f"  🛑 NINGUNA combinación da EV>0 con n>=20 a {args.cost_bps_per_leg}bps/pierna.")
        print(f"     A este coste la geometría no se arregla con parámetros → revisar")
        print(f"     tesis/mercado o probar coste menor (pares más líquidos).")
    print(f"\n  Recordatorio: gross al MID, ejecución perfecta. Es sensibilidad RELATIVA")
    print(f"  entre combos, NO prueba de GO. La verdad es reconcile_pnl.py en vivo.\n")


def main():
    ap = argparse.ArgumentParser(description="Backtest walk-forward honesto")
    ap.add_argument("--bars", type=int, default=2160, help="Barras totales a bajar (90d)")
    ap.add_argument("--train", type=int, default=336, help="Barras de entrenamiento por fold (14d)")
    ap.add_argument("--test", type=int, default=168, help="Barras de test por fold (7d)")
    ap.add_argument("--top", type=int, default=None, help="Sólo los N primeros pares del CSV")
    ap.add_argument("--usd", type=float, default=float(USD_PER_TRADE), help="USD por pierna")
    ap.add_argument("--cost-bps-per-leg", type=float, default=40.0,
                    help="Coste por pierna (fee+slippage+½spread) en bps. Mainnet altcoins ~40-200.")
    ap.add_argument("--funding-bps-day", type=float, default=3.0, help="Drag de funding en bps/día")
    ap.add_argument("--workers", type=int, default=6, help="Descargas concurrentes de candles")
    ap.add_argument("--sweep", action="store_true", help="Barrido window/z_entry/z_tp/stop")
    ap.add_argument("--stop", default="monetary", choices=["monetary", "zonly"],
                    help="single-run: monetary (HARD_SL) o zonly (sólo Z_SL, sin stop apretado)")
    ap.add_argument("--window", type=int, default=None, help="Override z-window (default constants.WINDOW)")
    ap.add_argument("--z-entry", type=float, default=None, help="Override umbral de entrada")
    ap.add_argument("--z-tp", type=float, default=None, help="Override umbral de TP")
    args = ap.parse_args()

    if not CSV_PATH.exists():
        print(f"ERROR: falta {CSV_PATH}")
        return
    pairs = pd.read_csv(CSV_PATH)
    if args.top:
        pairs = pairs.head(args.top)

    print(f"\n{'='*72}")
    print(f"  BACKTEST WALK-FORWARD  |  {len(pairs)} pares CSV  |  {'SWEEP' if args.sweep else 'single'}")
    print(f"  bars={args.bars} train={args.train} test={args.test} usd=${args.usd:.0f} "
          f"cost/leg={args.cost_bps_per_leg}bps")
    print(f"{'='*72}\n")

    unique_markets, cache = _prefetch_for(pairs, args)
    print(f"Prefetch listo.\n", flush=True)

    if args.sweep:
        run_sweep(pairs, cache, unique_markets, args)
        return

    params = build_params(args)
    print(f"Params: window={params['window']} z_entry={params['z_entry']} "
          f"z_tp={params['z_tp']} stop={params['stop_mode']}\n")
    res = run_walk_forward(pairs, cache, unique_markets, args, params)
    m = res["m"]
    years = res["years"]
    pairs_run = res["pairs_run"]
    selections = res["selections"]
    folds_used = res["folds_used"]
    first_test_ts = res["first_test_ts"]
    last_test_ts = res["last_test_ts"]
    n_unique = res["n_unique"]
    n_ok = res["n_ok"]
    n_dropped = res["n_dropped"]
    print(f"  Mercados únicos CSV:     {n_unique}")
    print(f"  Con datos suficientes:   {n_ok}   (descartados: {n_dropped})")
    if n_unique and n_dropped / n_unique > 0.30:
        print(f"  ⚠️  SESGO DE SUPERVIVENCIA: {n_dropped}/{n_unique} mercados ({100*n_dropped/n_unique:.0f}%)")
        print(f"      sin {args.bars}h de historia → EXCLUIDOS del backtest. Son en su")
        print(f"      mayoría los altcoins ilíquidos que más slippage generan en vivo.")
        print(f"      El resultado refleja sólo a los SUPERVIVIENTES líquidos. Baja --bars")
        print(f"      (p.ej. 720) para incluir más del universo real que opera el bot.")
    print(f"  Pares con datos:         {pairs_run}")
    print(f"  Selecciones (folds OK):  {selections}")
    print(f"  Folds con trades:        {folds_used}")
    print(f"  Periodo test cubierto:   {first_test_ts} → {last_test_ts}  ({years:.2f} años)")
    print(f"\n{'─'*72}\n  RESULTADO OUT-OF-SAMPLE (walk-forward, con costes)\n{'─'*72}")
    if m.get("n", 0) == 0:
        print("  0 trades. Sube --bars o baja --train, o revisa el CSV.")
        return
    print(f"  Trades:            {m['n']}  ({m['trades_per_year']}/año)")
    print(f"  Win rate:          {m['win_rate']}%")
    print(f"  Profit factor:     {m['profit_factor']}")
    print(f"  EV / trade:        ${m['ev_per_trade']}")
    print(f"  Gross PnL:         ${m['gross_pnl']}")
    print(f"  Exec cost total:   ${m['total_exec_cost']}")
    print(f"  Funding total:     ${m['total_funding']}")
    print(f"  NET PnL:           ${m['net_pnl']}")
    print(f"  Max drawdown:      ${m['max_drawdown']}")
    print(f"  Sharpe (anual):    {m['sharpe_annual']}   ← anualizado correcto (no ×√n_trades)")
    print(f"  Avg hold:          {m['avg_hold_h']}h")
    print(f"  Cierres:           {m['reasons_pct']}")

    # ── Coste de BREAK-EVEN: a qué bps/pierna el EV neto se vuelve 0 ───────
    # gross/trade se captura al MID; el coste real de ejecución (spread+slippage
    # cruzando el book con IOC) suele ser MUCHO mayor que el modelado.
    if m["n"] > 0:
        gross_per_trade = m["gross_pnl"] / m["n"]
        be_bps = gross_per_trade / (4.0 * args.usd) * 10_000.0
        print(f"\n  Coste de BREAK-EVEN: ≈{be_bps:.0f} bps/pierna "
              f"(por encima de esto el EV neto es NEGATIVO).")
        print(f"  Estás modelando {args.cost_bps_per_leg:.0f} bps/pierna. Tu universo real")
        print(f"  (altcoins mainnet) tiene spreads de 40-200 bps → corre también")
        print(f"  --cost-bps-per-leg {int(be_bps)+10} y 150 para ver la sensibilidad.")

    verdict_ok = m['net_pnl'] > 0 and m['ev_per_trade'] > 0
    print(f"\n  Veredicto (a {args.cost_bps_per_leg:.0f}bps): "
          f"{'✅ EV NETO > 0 OOS' if verdict_ok else '🛑 EV NETO ≤ 0 OOS — no desplegar'}")
    print(f"  ⚠️  RECORDATORIO: el gross se captura al MID con ejecución perfecta")
    print(f"      (sin legging ni whipsaw de 30s). La verdad es la reconciliación de")
    print(f"      fills reales (reconcile_pnl.py), no este backtest. Úsalo sólo para")
    print(f"      sensibilidad al coste y comparar parámetros, NO como prueba de GO.")
    print()


if __name__ == "__main__":
    main()
