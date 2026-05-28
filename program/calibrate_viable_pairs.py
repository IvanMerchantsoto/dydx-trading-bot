#!/usr/bin/env python3
"""
calibrate_viable_pairs.py — Identifica cuántos pares de tu top 150 son
ECONÓMICAMENTE viables con tu sizing actual.

A diferencia de calibrate_spread_ceiling.py (que mira markets individuales),
este script cruza:
  - tu CSV de pares cointegrados (top 150 por quality)
  - los spreads reales actuales de cada market
  - tu config económica (max_pct_of_edge, sizing)

Y te dice: "de los 150 pares cointegrados que tienes, X son tradeables HOY
con tu config". Más relevante operacionalmente.

Uso:
    python3 calibrate_viable_pairs.py
"""

import asyncio
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

import pandas as pd
import numpy as np
from func_connections import connect_dydx
from func_public import get_market_spread_bps
from func_cointegration import CSV_PATH as COINT_CSV_PATH


async def fetch_spread(indexer, market):
    try:
        s = await get_market_spread_bps(indexer, market)
        return market, float(s) if s is not None else None
    except Exception:
        return market, None


def quality_score(row):
    try:
        r2 = float(row.get("r_squared", 0) or 0)
        hl = float(row.get("half_life", 24) or 24)
        hr = float(row.get("hurst", 0.5) or 0.5)
        return r2 * (24.0 / max(1.0, hl)) * (1.0 / max(0.1, hr))
    except Exception:
        return 0.0


async def main():
    from constants import (
        MAX_COINT_PAIRS_TO_SCAN,
        SPREAD_GATE_MAX_PCT_OF_EDGE,
        SPREAD_GATE_PER_LEG_CEILING_BPS,
        DYNAMIC_SIZING_MAX_USD,
    )

    # ── 1. Top 150 pairs
    df = pd.read_csv(COINT_CSV_PATH)
    if len(df) > MAX_COINT_PAIRS_TO_SCAN > 0:
        df["_q"] = df.apply(quality_score, axis=1)
        df = df.sort_values("_q", ascending=False).head(MAX_COINT_PAIRS_TO_SCAN).reset_index(drop=True)
    print(f"Pares en análisis: {len(df)}")

    # ── 2. Markets únicos
    markets = set()
    for _, row in df.iterrows():
        b = row.get("base_market") or row.get("sym_1")
        q = row.get("quote_market") or row.get("sym_2")
        if b: markets.add(str(b))
        if q: markets.add(str(q))
    markets = sorted(markets)

    # ── 3. Conectar
    print(f"Conectando a dYdX mainnet...")
    node, indexer, wallet = await connect_dydx()

    # ── 4. Spreads en paralelo
    BATCH = 20
    spreads = {}
    for i in range(0, len(markets), BATCH):
        batch = markets[i:i+BATCH]
        tasks = [fetch_spread(indexer, m) for m in batch]
        results = await asyncio.gather(*tasks)
        for m, sp in results:
            spreads[m] = sp
        print(f"  Sampled {min(i+BATCH, len(markets))}/{len(markets)}...")

    # ── 5. Analizar cada par
    notional = float(DYNAMIC_SIZING_MAX_USD)
    max_pct = float(SPREAD_GATE_MAX_PCT_OF_EDGE)
    ceiling = float(SPREAD_GATE_PER_LEG_CEILING_BPS)

    print(f"\n{'='*80}")
    print(f"Análisis de viabilidad económica por par")
    print(f"  Sizing:     ${notional:.0f}/leg")
    print(f"  max_pct:    {max_pct} ({max_pct*100:.0f}% del edge)")
    print(f"  ceiling:    {ceiling} bps/leg")
    print(f"{'='*80}")

    blocked_ceiling = 0
    blocked_missing = 0
    viable_pairs = []

    for _, row in df.iterrows():
        b = str(row.get("base_market") or row.get("sym_1") or "")
        q = str(row.get("quote_market") or row.get("sym_2") or "")
        s_b = spreads.get(b)
        s_q = spreads.get(q)

        if s_b is None or s_q is None:
            blocked_missing += 1
            continue

        # Bloqueado por ceiling?
        if s_b > ceiling or s_q > ceiling:
            blocked_ceiling += 1
            continue

        # Calcular spread combinado
        combined_bps = s_b + s_q
        spread_cost_usd = combined_bps * notional / 10000.0

        # Necesita edge mínimo para pasar max_pct
        min_edge_required = spread_cost_usd / max_pct
        viable_pairs.append({
            "base": b, "quote": q,
            "s_b": s_b, "s_q": s_q,
            "combined": combined_bps,
            "spread_cost": spread_cost_usd,
            "min_edge_required": min_edge_required,
        })

    # ── 6. Reporte
    print(f"\n  Total top {MAX_COINT_PAIRS_TO_SCAN}: {len(df)}")
    print(f"  ❌ Bloqueados por ceiling (spread > {ceiling}): {blocked_ceiling}")
    print(f"  ⚠️  Sin data de spread: {blocked_missing}")
    print(f"  ✅ Pasan el ceiling: {len(viable_pairs)}")

    if not viable_pairs:
        print("\nNingún par pasa el ceiling. Necesitas otro approach.")
        return

    # Ordenar por edge mínimo requerido
    viable_pairs.sort(key=lambda x: x["min_edge_required"])

    print(f"\n{'='*80}")
    print(f"Top 20 pares MÁS VIABLES (menor edge requerido para pasar gate):")
    print(f"{'='*80}")
    print(f"{'Par':<35} {'s_b':>6} {'s_q':>6} {'sum':>6} {'cost':>7} {'min_edge':>9}")
    print(f"{'-'*80}")
    for p in viable_pairs[:20]:
        pair_str = f"{p['base']}/{p['quote']}"
        print(f"  {pair_str:<33} {p['s_b']:>6.1f} {p['s_q']:>6.1f} {p['combined']:>6.1f} ${p['spread_cost']:>5.2f} ${p['min_edge_required']:>7.2f}")

    # ── 7. Estimación realista por edge esperado
    print(f"\n{'='*80}")
    print(f"Realmente tradeables según edge esperado:")
    print(f"{'='*80}")

    # Edge típico que el bot encuentra (de los logs): $0.5 - $5.0
    for edge_est in [0.5, 1.0, 2.0, 5.0]:
        viable_at_edge = sum(1 for p in viable_pairs if p["min_edge_required"] <= edge_est)
        print(f"  Si edge típico = ${edge_est:.1f} → {viable_at_edge}/{len(df)} pares pasan TODOS los gates")

    # ── 8. Markets más representados en pares viables
    from collections import Counter
    leg_counts = Counter()
    for p in viable_pairs:
        leg_counts[p['base']] += 1
        leg_counts[p['quote']] += 1
    print(f"\n  Markets más representados en pares viables (ordenado):")
    for m, c in leg_counts.most_common(10):
        sp = spreads.get(m, 0)
        print(f"    {m:<30} aparece en {c:>2} pares  (spread: {sp:.1f} bps)")


if __name__ == "__main__":
    asyncio.run(main())
