#!/usr/bin/env python3
"""
force_coint_refresh.py — Regenera cointegrated_pairs.csv on-demand.

Uso:
    python3 force_coint_refresh.py

⚠️ CUIDADO: no ejecutes esto si el bot principal está corriendo Y ya está
regenerando en background — vas a competir por rate limits del indexer.

Uso ideal:
  1. Detener el bot temporalmente (kill $(cat ~/DYDX/bot.pid))
  2. Correr este script (tarda ~5 min)
  3. Reiniciar el bot

Alternativa: dejar que el bot regenere solo cada 6h.
"""

import asyncio
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

from func_connections import connect_dydx
from func_public import construct_market_prices
from func_cointegration import store_cointegration_results, CSV_PATH


async def main():
    print(f"Refresh forzado del CSV en {CSV_PATH}")
    print(f"Conectando...")
    node, indexer, wallet = await connect_dydx()
    if not node or not indexer:
        print(f"❌ Conexión falló")
        return

    print(f"Fetching market prices...")
    t0 = time.time()
    df = await construct_market_prices(node, indexer)
    elapsed = time.time() - t0
    print(f"Fetch completado en {elapsed:.1f}s con {len(df.columns)} markets")

    if df.empty:
        print(f"❌ No markets fetched")
        return

    print(f"Calculando cointegración...")
    result = store_cointegration_results(df)
    if result == "saved":
        print(f"✅ CSV regenerated at {CSV_PATH}")
    else:
        print(f"⚠️ Unexpected result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
