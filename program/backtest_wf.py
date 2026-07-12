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


# ─────────────────────────────────────────────────────────────────────────────
# Selección de pares en la ventana de ENTRENAMIENTO (paridad con live)
# ─────────────────────────────────────────────────────────────────────────────
def train_select(p1_train, p2_train):
    """
    Replica el filtro de cointegración del bot sobre la ventana de train.
    Returns (passes: bool, hedge_ratio: float, half_life: float).
    """
    if len(p1_train) < max(40, WINDOW + 5) or len(p2_train) < max(40, WINDOW + 5):
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
                  usd, cost_bps_leg, funding_bps_day):
    """
    Opera SÓLO en [test_start, test_end). El z-score usa la ventana rodante
    WINDOW terminando en la barra actual (sólo datos pasados → sin look-ahead).
    Costes: (fee+slippage+spread) por pierna en entrada y salida + funding.
    """
    spread = p1_full - hedge_ratio * p2_full
    z_series = calculate_zscore(pd.Series(spread)).values

    trades = []
    state = "flat"
    entry_z = 0.0
    entry_i = 0
    entry_p1 = entry_p2 = 0.0
    entry_side = "short"
    tp_confirm = 0
    best_z = 99.0
    notional = 2.0 * usd
    # coste por round-trip completo (2 piernas × entrada+salida)
    exec_cost = 4.0 * usd * (cost_bps_leg / 10_000.0)
    hard_level = max(float(HARD_SL_USD), float(HARD_SL_PCT) * notional)

    lo = max(test_start, WINDOW + 1)
    for i in range(lo, test_end):
        z = z_series[i]
        if z is None or (isinstance(z, float) and np.isnan(z)):
            continue

        if state == "flat":
            if abs(z) >= ZSCORE_THRESH:
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
        # 1) HARD_SL monetario (paridad live)
        if pnl_gross <= -hard_level:
            reason = "HARD_SL"
        # 2) Z_SL
        elif abs(z) >= abs(entry_z) + Z_SL_DELTA:
            reason = "Z_SL"
        # 2b) TP_CROSSED_ZERO (gate relajado: net>0)
        elif ((entry_z > 0.1 and z < -0.1) or (entry_z < -0.1 and z > 0.1)) and net > 0:
            reason = "TP_CROSSED_ZERO"
        # 3) TP con doble confirmación + profit gate
        else:
            if abs(z) <= Z_TP:
                tp_confirm += 1
            else:
                tp_confirm = 0
            if tp_confirm >= TP_CONFIRM_CHECKS:
                if net > 0:
                    reason = "TP"
                # si pnl_gross<0 → HOLD (política jun30), no cerrar
        if reason is None and hold >= 500:
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
    args = ap.parse_args()

    if not CSV_PATH.exists():
        print(f"ERROR: falta {CSV_PATH}")
        return
    pairs = pd.read_csv(CSV_PATH)
    if args.top:
        pairs = pairs.head(args.top)

    print(f"\n{'='*72}")
    print(f"  BACKTEST WALK-FORWARD  |  {len(pairs)} pares CSV")
    print(f"  bars={args.bars} train={args.train} test={args.test} usd=${args.usd:.0f}")
    print(f"  cost/leg={args.cost_bps_per_leg}bps  funding={args.funding_bps_day}bps/día")
    print(f"  z_entry={ZSCORE_THRESH} z_tp={Z_TP} z_sl_delta={Z_SL_DELTA} "
          f"hard_sl=${HARD_SL_USD} window={WINDOW}")
    print(f"{'='*72}\n")

    # ── Prefetch de mercados únicos (con progreso, paralelo, backoff) ──────
    unique_markets = set()
    _pair_list = []
    for _, row in pairs.iterrows():
        m1 = str(row.get("base_market", row.get("sym_1", ""))).strip()
        m2 = str(row.get("quote_market", row.get("sym_2", ""))).strip()
        if m1 and m2:
            unique_markets.add(m1)
            unique_markets.add(m2)
            _pair_list.append((m1, m2))
    cache = prefetch_markets(sorted(unique_markets), args.bars, workers=args.workers)
    print(f"Prefetch listo. Corriendo walk-forward sobre {len(_pair_list)} pares...\n", flush=True)

    def get(m):
        return cache.get(m)

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
        s1 = get(m1)
        s2 = get(m2)
        if not s1 or not s2:
            continue
        ts, p1, p2 = align_by_ts(s1, s2)
        n = len(p1)
        if n < args.train + args.test + WINDOW + 5:
            continue
        pairs_run += 1

        # Walk-forward: rueda por ventanas de test no solapadas.
        start = 0
        while start + args.train + args.test <= n:
            tr0, tr1 = start, start + args.train
            te0, te1 = tr1, min(tr1 + args.test, n)
            passes, hr, hl = train_select(p1[tr0:tr1], p2[tr0:tr1])
            if passes:
                selections += 1
                # Simula sobre la serie completa pero acotado a [te0, te1);
                # el z usa ventana rodante (pasado) con el hedge_ratio de TRAIN.
                trs = simulate_test(p1, p2, te0, te1, hr, hl,
                                    args.usd, args.cost_bps_per_leg, args.funding_bps_day)
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

    # años cubiertos por el periodo de test
    years = 1.0
    if first_test_ts and last_test_ts:
        try:
            d0 = datetime.fromisoformat(first_test_ts.replace("Z", "+00:00"))
            d1 = datetime.fromisoformat(last_test_ts.replace("Z", "+00:00"))
            years = max((d1 - d0).total_seconds() / (365 * 86400), 1e-6)
        except Exception:
            pass

    m = metrics(all_trades, args.usd, years)
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
    print(f"\n  Veredicto: {'✅ EV NETO > 0 OOS con costes' if m['net_pnl'] > 0 and m['ev_per_trade'] > 0 else '🛑 EV NETO ≤ 0 OOS — no desplegar'}")
    print()


if __name__ == "__main__":
    main()
