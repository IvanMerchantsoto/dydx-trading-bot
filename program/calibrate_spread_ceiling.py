#!/usr/bin/env python3
"""
calibrate_spread_ceiling.py — Mide los spreads reales en dYdX mainnet
y recomienda un SPREAD_GATE_PER_LEG_CEILING_BPS basado en data, no en adivinar.

Qué hace:
  1. Lee cointegrated_pairs.csv
  2. Extrae los markets únicos (top 150 por quality si MAX_COINT_PAIRS_TO_SCAN configurado)
  3. Consulta el orderbook de cada market en dYdX
  4. Calcula spread bid-ask en bps por market
  5. Reporta distribución (p25/p50/p75/p90/p99)
  6. Sugiere ceiling óptimo según tu max_pct_of_edge y notional

Uso:
    cd ~/DYDX/program
    source ~/DYDX/.venv/bin/activate
    python3 calibrate_spread_ceiling.py
"""

import asyncio
import sys
from pathlib import Path
from statistics import median

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

import pandas as pd
import numpy as np
from func_connections import connect_dydx
from func_public import get_market_spread_bps
from func_cointegration import CSV_PATH as COINT_CSV_PATH


async def fetch_spread(indexer, market):
    """Get current spread in bps for one market. Returns None on error."""
    try:
        s = await get_market_spread_bps(indexer, market)
        return market, float(s) if s is not None else None
    except Exception as e:
        return market, None


def quality_score(row):
    try:
        r2 = float(row.get("r_squared", 0) or 0)
        hl = float(row.get("half_life", 24) or 24)
        hr = float(row.get("hurst", 0.5) or 0.5)
        return r2 * (24.0 / max(1.0, hl)) * (1.0 / max(0.1, hr))
    except Exception:
        return 0.0


def percentile(data, p):
    if not data:
        return 0
    return float(np.percentile(sorted(data), p))


async def main():
    # ── 1. Cargar pares cointegrados
    try:
        from constants import MAX_COINT_PAIRS_TO_SCAN
    except ImportError:
        MAX_COINT_PAIRS_TO_SCAN = 150

    df = pd.read_csv(COINT_CSV_PATH)
    print(f"Pares totales en CSV: {len(df)}")

    if len(df) > MAX_COINT_PAIRS_TO_SCAN > 0:
        df["_q"] = df.apply(quality_score, axis=1)
        df = df.sort_values("_q", ascending=False).head(MAX_COINT_PAIRS_TO_SCAN)
        print(f"Filtrado a top {MAX_COINT_PAIRS_TO_SCAN} por quality")

    # ── 2. Markets únicos
    markets = set()
    for _, row in df.iterrows():
        b = row.get("base_market") or row.get("sym_1")
        q = row.get("quote_market") or row.get("sym_2")
        if b: markets.add(str(b))
        if q: markets.add(str(q))
    markets = sorted(markets)
    print(f"Markets únicos a samplear: {len(markets)}")

    # ── 3. Conectar a dYdX
    print(f"\nConectando a dYdX mainnet...")
    node, indexer, wallet = await connect_dydx()
    print(f"✓ Conectado\n")

    # ── 4. Fetch spreads en paralelo (por lotes para no saturar)
    BATCH = 20
    spreads = {}
    for i in range(0, len(markets), BATCH):
        batch = markets[i:i+BATCH]
        tasks = [fetch_spread(indexer, m) for m in batch]
        results = await asyncio.gather(*tasks)
        for market, sp in results:
            spreads[market] = sp
        print(f"  Sampled {min(i+BATCH, len(markets))}/{len(markets)}...")

    # ── 5. Filtrar None y mostrar distribución
    valid = {m: s for m, s in spreads.items() if s is not None and s > 0}
    missing = [m for m, s in spreads.items() if s is None]

    if not valid:
        print("\n❌ No se obtuvieron spreads válidos. Algo está mal.")
        return

    values = sorted(valid.values())
    n = len(values)
    print(f"\n{'='*70}")
    print(f"Distribución de spreads bid-ask en mainnet ({n} markets):")
    print(f"{'='*70}")
    print(f"  min:  {values[0]:>8.1f} bps  ({sorted(valid.items(), key=lambda x:x[1])[0][0]})")
    print(f"  p10:  {percentile(values, 10):>8.1f} bps")
    print(f"  p25:  {percentile(values, 25):>8.1f} bps")
    print(f"  p50:  {percentile(values, 50):>8.1f} bps  (median)")
    print(f"  p75:  {percentile(values, 75):>8.1f} bps")
    print(f"  p90:  {percentile(values, 90):>8.1f} bps")
    print(f"  p95:  {percentile(values, 95):>8.1f} bps")
    print(f"  p99:  {percentile(values, 99):>8.1f} bps")
    print(f"  max:  {values[-1]:>8.1f} bps  ({sorted(valid.items(), key=lambda x:-x[1])[0][0]})")
    if missing:
        print(f"  (no data en {len(missing)} markets: {missing[:5]}{'...' if len(missing)>5 else ''})")

    # ── 6. Top markets más tight
    print(f"\n{'='*70}")
    print(f"Top 15 markets MÁS TIGHT (orden creciente de spread):")
    print(f"{'='*70}")
    for m, s in sorted(valid.items(), key=lambda x: x[1])[:15]:
        print(f"  {m:<30} {s:>8.1f} bps")

    print(f"\n{'='*70}")
    print(f"Top 10 markets MÁS WIDE (broken books / illiquid):")
    print(f"{'='*70}")
    for m, s in sorted(valid.items(), key=lambda x: -x[1])[:10]:
        print(f"  {m:<30} {s:>8.1f} bps")

    # ── 7. Calcular ceiling óptimo basado en MAX_PCT_OF_EDGE y notional
    print(f"\n{'='*70}")
    print(f"Cálculo de ceiling óptimo:")
    print(f"{'='*70}")

    try:
        from constants import (
            SPREAD_GATE_MAX_PCT_OF_EDGE,
            SPREAD_GATE_PER_LEG_CEILING_BPS,
            DYNAMIC_SIZING_MAX_USD,
            DYNAMIC_SIZING_PCT,
            USD_PER_TRADE,
        )
    except ImportError as e:
        print(f"  ⚠️  Error importing constants: {e}")
        return

    print(f"\n  Tu config actual:")
    print(f"    SPREAD_GATE_PER_LEG_CEILING_BPS = {SPREAD_GATE_PER_LEG_CEILING_BPS}")
    print(f"    SPREAD_GATE_MAX_PCT_OF_EDGE     = {SPREAD_GATE_MAX_PCT_OF_EDGE}")
    print(f"    DYNAMIC_SIZING_MAX_USD          = {DYNAMIC_SIZING_MAX_USD}")
    print(f"")

    # Cuántos markets caben en distintos ceilings
    print(f"  Cuántos markets pasarían el ceiling según el valor:")
    for ceiling in [50, 75, 100, 150, 200, 250, 300, 500, 1000]:
        passing = sum(1 for s in values if s <= ceiling)
        pct = passing / n * 100
        bar = '█' * int(pct / 3)
        print(f"    ceiling={ceiling:>5} bps → {passing:>3}/{n} markets ({pct:>5.1f}%) {bar}")

    # Recomendaciones específicas
    p_50 = percentile(values, 50)
    p_75 = percentile(values, 75)
    p_90 = percentile(values, 90)

    print(f"\n  Recomendaciones según objetivo:")
    print(f"  ┌────────────────────────────────────────────────────────────────")
    print(f"  │ Conservador (solo mejores 50%): ceiling = {p_50:.0f} bps")
    print(f"  │ Balanceado (75% del universo):  ceiling = {p_75:.0f} bps  ← RECOMENDADO")
    print(f"  │ Agresivo (90% del universo):    ceiling = {p_90:.0f} bps")
    print(f"  └────────────────────────────────────────────────────────────────")

    # Análisis económico
    print(f"\n  Análisis económico (sizing actual ${DYNAMIC_SIZING_MAX_USD}/leg, max_pct {SPREAD_GATE_MAX_PCT_OF_EDGE*100:.0f}%):")
    print(f"")
    print(f"  Para que un par PASE con expected_edge=$X:")
    print(f"    cost ≤ max_pct × edge")
    print(f"    (s1+s2) × notional / 10000 ≤ {SPREAD_GATE_MAX_PCT_OF_EDGE} × edge")
    print(f"    s1+s2 ≤ {SPREAD_GATE_MAX_PCT_OF_EDGE * 10000 / DYNAMIC_SIZING_MAX_USD:.0f} × edge")
    print(f"")
    for edge_usd in [0.5, 1.0, 2.0, 5.0]:
        max_combined = SPREAD_GATE_MAX_PCT_OF_EDGE * edge_usd * 10000 / DYNAMIC_SIZING_MAX_USD
        max_per_leg = max_combined / 2
        print(f"    Edge ${edge_usd:>4.1f} → combined ≤ {max_combined:>5.0f} bps → ~{max_per_leg:.0f} bps/leg")

    print(f"\n{'='*70}")
    print(f"Conclusión:")
    print(f"{'='*70}")
    print(f"  El BOTTLENECK real NO es el ceiling — es el max_pct_of_edge")
    print(f"  combinado con tu sizing pequeño de ${DYNAMIC_SIZING_MAX_USD}.")
    print(f"")
    print(f"  Con $30/leg y edges típicos de $0.5-2, NECESITAS pares con")
    print(f"  combined spread < 80-300 bps para que sean economicamente viables.")
    print(f"")
    print(f"  Aunque pongas ceiling muy alto, max_pct_of_edge sigue bloqueando")
    print(f"  los que no son rentables. Eso es CORRECTO.")
    print(f"")
    print(f"  Sugerencia FINAL: ceiling = {p_75:.0f} bps (p75 observado)")
    print(f"                    max_pct = 0.5 (ya en tu config)")


if __name__ == "__main__":
    asyncio.run(main())
