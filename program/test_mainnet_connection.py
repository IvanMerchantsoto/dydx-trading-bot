#!/usr/bin/env python3
"""
test_mainnet_connection.py — Verificación de conexión a dYdX mainnet SIN operar.

Qué valida:
  1. Las URLs de NODE/INDEXER configuradas son accesibles
  2. La API_KEY (mnemonic) deriva al WALLET_ADDRESS configurado
  3. El indexer reconoce la wallet (responde con subaccount info)
  4. Hay markets disponibles para operar
  5. Equity, free collateral actual

NO envía órdenes. NO toca el bot principal.

Uso:
    python3 test_mainnet_connection.py
"""

import asyncio
import sys
import os
from pathlib import Path

# Reuse the existing bot machinery
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

from func_connections import connect_dydx
from constants import (
    MODE, NODE, INDEXER, WEBSOCKET, WALLET_ADDRESS,
    ZSCORE_THRESH, DYNAMIC_SIZING_PCT, MAX_OPEN_TRADES,
    HARD_SL_USD, USD_MIN_COLLATERAL, MAX_ENTRY_SPREAD_BPS,
)


def section(title):
    print(f"\n{'='*70}\n  {title}\n{'='*70}")


async def main():
    section("CONFIG CHECK")
    print(f"  MODE:                     {MODE}")
    print(f"  NODE:                     {NODE or '(VACIA — falta llenar)'}")
    print(f"  INDEXER:                  {INDEXER or '(VACIA — falta llenar)'}")
    print(f"  WEBSOCKET:                {WEBSOCKET or '(VACIA — falta llenar)'}")
    print(f"  WALLET_ADDRESS:           {WALLET_ADDRESS}")

    print(f"\n  Trading config (no se va a usar — solo lectura):")
    print(f"    ZSCORE_THRESH:          {ZSCORE_THRESH}")
    print(f"    DYNAMIC_SIZING_PCT:     {DYNAMIC_SIZING_PCT*100:.0f}%")
    print(f"    MAX_OPEN_TRADES:        {MAX_OPEN_TRADES}")
    print(f"    HARD_SL_USD:            ${HARD_SL_USD}")
    print(f"    USD_MIN_COLLATERAL:     ${USD_MIN_COLLATERAL}")
    print(f"    MAX_ENTRY_SPREAD_BPS:   {MAX_ENTRY_SPREAD_BPS}bps")

    if not NODE or not INDEXER or not WEBSOCKET:
        print("\n  ⚠️  URLs de mainnet incompletas. Llena NODE_MAINNET, INDEXER_MAINNET,")
        print("     WEBSOCKET_MAINNET en constants.py antes de continuar.")
        sys.exit(1)

    if MODE != "PRODUCTION":
        print("\n  ⚠️  MODE no es PRODUCTION. Este script está pensado para validar mainnet.")
        sys.exit(1)

    section("1. CONECTANDO A MAINNET")
    try:
        print(f"  Conectando a {NODE}...")
        node, indexer, wallet = await connect_dydx()
        print(f"  ✓ Conexión establecida")
    except Exception as e:
        print(f"  ✗ FAIL: {e}")
        print("\n  Posibles causas:")
        print("   - URL de NODE incorrecta")
        print("   - API_KEY (mnemonic) inválida o no cargada del .env")
        print("   - Sin internet o proxy bloqueando")
        sys.exit(1)

    section("2. VERIFICANDO WALLET")
    try:
        derived_addr = wallet.address if hasattr(wallet, 'address') else str(wallet)
        print(f"  Address derivada del mnemonic:  {derived_addr}")
        print(f"  Address configurada (constants): {WALLET_ADDRESS}")
        if derived_addr == WALLET_ADDRESS:
            print(f"  ✓ Coinciden — credenciales correctas")
        else:
            print(f"  ✗ NO COINCIDEN — esto es un problema:")
            print(f"     El mnemonic de tu .env apunta a otra wallet.")
            print(f"     Si la wallet derivada es tu mainnet wallet real,")
            print(f"     cambia WALLET_ADDRESS en constants.py a:")
            print(f"     WALLET_ADDRESS = \"{derived_addr}\"")
            sys.exit(1)
    except Exception as e:
        print(f"  ✗ Error verificando wallet: {e}")

    section("3. CONSULTANDO SUBACCOUNT EN INDEXER")
    try:
        resp = await indexer.account.get_subaccount(WALLET_ADDRESS.strip(), 0)
        sub = resp.get("subaccount", {}) if isinstance(resp, dict) else {}
        equity = float(sub.get("equity") or 0)
        free = float(sub.get("freeCollateral") or 0)
        positions = sub.get("openPerpetualPositions", {}) or {}

        print(f"  ✓ Indexer respondió")
        print(f"\n  Tu cuenta MAINNET:")
        print(f"    Equity:           ${equity:,.4f}")
        print(f"    Free collateral:  ${free:,.4f}")
        print(f"    Posiciones abiertas: {len(positions)}")

        if positions:
            print(f"\n  Posiciones actuales:")
            for m, p in positions.items():
                size = float(p.get("size", 0))
                if abs(size) > 1e-9:
                    print(f"    {m}: size={size}")

        if equity < 1.0:
            print(f"\n  ℹ️  Equity casi cero. Necesitas depositar los $100 USDC antes de arrancar.")
        elif equity < 50:
            print(f"\n  ℹ️  Equity ${equity:.2f} — bajo. Considera depositar más antes de operar.")
        else:
            print(f"\n  ✓ Equity suficiente para arrancar el bot.")

    except Exception as e:
        print(f"  ✗ Error consultando indexer: {e}")
        print("\n  Posibles causas:")
        print("   - URL de INDEXER incorrecta")
        print("   - Wallet address no es válida en mainnet")
        sys.exit(1)

    section("4. VERIFICANDO MARKETS DISPONIBLES")
    try:
        mk_resp = await indexer.markets.get_perpetual_markets()
        markets = mk_resp.get("markets", {}) or {}
        active = [m for m, d in markets.items() if d.get("status") == "ACTIVE"]
        print(f"  ✓ Markets totales: {len(markets)}, activos: {len(active)}")

        sample = sorted(active)[:10]
        print(f"\n  Primeros 10 markets activos: {sample}")

        # Spread sample en algunos populares
        from func_public import get_market_spread_bps
        popular = ["BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD", "LINK-USD"]
        print(f"\n  Spreads bid-ask en mainnet (referencia vs testnet):")
        for m in popular:
            if m in markets:
                try:
                    s = await get_market_spread_bps(indexer, m)
                    if s is not None:
                        print(f"    {m:<12s}  {s:>6.2f} bps  {'✓ tight' if s < 25 else '⚠️  amplio'}")
                except Exception:
                    print(f"    {m:<12s}  (no se pudo medir)")

    except Exception as e:
        print(f"  ✗ Error consultando markets: {e}")
        sys.exit(1)

    section("RESULTADO")
    print("  ✓ Conexión mainnet funcionando")
    print("  ✓ Credenciales correctas")
    print("  ✓ Indexer respondiendo")
    print("  ✓ Markets disponibles")
    print(f"\n  Listo para arrancar el bot principal con MODE=PRODUCTION.")
    print(f"  Antes asegúrate de:")
    print(f"   1. Equity ≥ \$100 USDC (actual: \${equity:,.2f})")
    print(f"   2. bot_agents.json vacío: echo \"[]\" > bot_agents.json")
    print(f"   3. PLACE_TRADES=True y MANAGE_EXITS=True en constants.py")
    print()


if __name__ == "__main__":
    asyncio.run(main())
