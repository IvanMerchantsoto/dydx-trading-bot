#!/usr/bin/env python3
"""
close_positions_limit.py — Cierra posiciones con LIMIT orders para MINIMIZAR SLIPPAGE.

Motivación (2026-07-12): CRO/FIL close via MARKET orders dio -$6+ de slippage
en par ilíquido. Con LIMIT orders al mid-price, esperamos slippage 0-2 bps.

Estrategia:
  1. Fetcha orderbook (bid, ask) de cada leg
  2. Coloca LIMIT order al MID (bid+ask)/2
  3. Espera N segundos (default 30s)
  4. Si no fill: cancela + coloca nueva orden más agresiva
  5. Repite hasta que fill o max_attempts
  6. Como último recurso: MARKET order (acepta slippage)

Uso:
    python3 close_positions_limit.py                    # Cierra TODAS las posiciones
    python3 close_positions_limit.py ICP-USD ASTER-USD  # Cierra un par específico
    python3 close_positions_limit.py --dry-run          # Simula sin ejecutar
"""

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from constants import WALLET_ADDRESS
from func_connections import connect_dydx
from func_utils import format_number
from func_private import place_limit_order, cancel_order_by_client_id, place_market_order
from dydx_v4_client.node.market import Order
from dydx_v4_client.indexer.rest.indexer_client import IndexerClient


# Config
LIMIT_WAIT_SECONDS = 30       # esperar antes de reintentar
MAX_ATTEMPTS = 4              # número de reintentos LIMIT antes de MARKET
SLIPPAGE_STEP_BPS = 20        # cada reintento agrega 20bps de tolerancia
FINAL_MARKET_FALLBACK = True  # último recurso: market order


async def get_orderbook(indexer, market):
    """Fetch orderbook to get best bid and ask."""
    try:
        ob = await indexer.markets.get_perpetual_market_orderbook(market)
        bids = ob.get("bids", [])
        asks = ob.get("asks", [])
        if not bids or not asks:
            return None, None, None
        best_bid = float(bids[0]["price"])
        best_ask = float(asks[0]["price"])
        mid = (best_bid + best_ask) / 2
        return best_bid, best_ask, mid
    except Exception as e:
        print(f"    [OB] error fetching orderbook: {e}")
        return None, None, None


async def get_position(indexer, market):
    """Fetch current position for a market."""
    try:
        resp = await indexer.account.get_subaccount(WALLET_ADDRESS.strip(), 0)
        sub = resp.get("subaccount", {}) or {}
        positions = sub.get("openPerpetualPositions", {}) or {}
        return positions.get(market)
    except Exception as e:
        print(f"    [POS] error: {e}")
        return None


async def get_market_info(indexer, market):
    """Fetch market info to know step_size and tick_size."""
    try:
        resp = await indexer.markets.get_perpetual_markets(market=market)
        m = resp.get("markets", {}).get(market)
        if not m:
            return None
        return {
            "stepSize": float(m.get("stepBaseQuantums", "1")) / (10 ** int(m.get("atomicResolution", 0)) * -1) if False else float(m["stepSize"]),
            "tickSize": float(m["tickSize"]),
            "minSize": float(m.get("stepSize", "1")),
        }
    except Exception as e:
        print(f"    [MKT] error: {e}")
        return None


async def close_single_position(node, indexer, wallet, market, dry_run=False):
    """
    Close a single market position using LIMIT orders with progressive aggression.
    """
    print(f"\n{'='*70}")
    print(f"  CERRANDO {market}")
    print(f"{'='*70}")

    # 1. Get current position
    pos = await get_position(indexer, market)
    if not pos or pos.get("status") != "OPEN":
        print(f"  ⚠️ No hay posición abierta en {market}")
        return {"market": market, "success": False, "reason": "no_position"}

    size = float(pos["size"])
    side_current = pos["side"]  # "LONG" or "SHORT"
    entry_price = float(pos.get("entryPrice", 0))
    unreal = float(pos.get("unrealizedPnl", 0))

    # 2. Determine close side (opposite of position)
    close_side = "SELL" if side_current == "LONG" else "BUY"
    close_size = abs(size)

    # 3. Get orderbook
    best_bid, best_ask, mid = await get_orderbook(indexer, market)
    if mid is None:
        print(f"  ❌ No orderbook data para {market}. Skip.")
        return {"market": market, "success": False, "reason": "no_orderbook"}

    spread_bps = (best_ask - best_bid) / mid * 10000
    print(f"  Position: {side_current} {size} @ entry ${entry_price}")
    print(f"  Unreal: ${unreal:+.3f}")
    print(f"  Orderbook: bid=${best_bid:.6f}  ask=${best_ask:.6f}  mid=${mid:.6f}  spread={spread_bps:.0f}bps")
    print(f"  Close: {close_side} {close_size}")

    # 4. Get market info for tick_size
    mkt = await get_market_info(indexer, market)
    if not mkt:
        print(f"  ❌ No market info para {market}. Skip.")
        return {"market": market, "success": False, "reason": "no_market_info"}
    tick = mkt["tickSize"]
    step = mkt["stepSize"]
    close_size_fmt = float(format_number(close_size, step))

    if dry_run:
        print(f"  🔬 DRY RUN — no order sent")
        return {"market": market, "success": True, "reason": "dry_run"}

    # 5. Loop: place LIMIT, wait, check fill, retry if needed
    remaining = close_size_fmt
    total_filled = 0.0
    last_client_id = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        # Compute limit price with progressive aggression
        # Attempt 1: mid price (best case)
        # Attempt 2: mid + 20bps toward opposite side (moderate)
        # Attempt 3: mid + 40bps (aggressive)
        # Attempt 4: mid + 60bps (very aggressive)
        aggression_bps = (attempt - 1) * SLIPPAGE_STEP_BPS
        if close_side == "SELL":
            # SELL: reduce price to be more competitive with buyers
            limit_price = mid * (1 - aggression_bps / 10000)
        else:
            # BUY: raise price to be more competitive with sellers
            limit_price = mid * (1 + aggression_bps / 10000)
        limit_price = float(format_number(limit_price, tick))

        print(f"\n  ── Attempt {attempt}/{MAX_ATTEMPTS} — LIMIT {close_side} {remaining} @ ${limit_price} (aggression={aggression_bps}bps)")

        try:
            resp = await place_limit_order(
                node, indexer, wallet, market, close_side,
                remaining, limit_price,
                reduce_only=True, post_only=False,  # NOT post-only so can take liquidity
                good_til_blocks=40,
            )
            client_id = (resp or {}).get("order", {}).get("id") or (resp or {}).get("client_id")
            last_client_id = client_id
            print(f"    Order placed: cid={client_id}")
        except Exception as e:
            print(f"    ❌ Error placing order: {e}")
            continue

        # Wait and check for fill
        await asyncio.sleep(LIMIT_WAIT_SECONDS)

        # Re-check position
        pos_now = await get_position(indexer, market)
        if pos_now is None or pos_now.get("status") != "OPEN":
            print(f"    ✅ POSITION CLOSED (position no longer open)")
            return {"market": market, "success": True, "attempts": attempt, "limit_price": limit_price}

        size_now = abs(float(pos_now["size"]))
        filled_this_round = remaining - size_now
        total_filled += filled_this_round
        remaining = size_now

        print(f"    Fill status: {filled_this_round:.4f} filled  |  {remaining:.4f} remaining")

        if remaining < step * 0.5:
            print(f"    ✅ POSITION CLOSED (residual < step)")
            return {"market": market, "success": True, "attempts": attempt}

        # Cancel remaining order before next attempt
        if last_client_id:
            try:
                await cancel_order_by_client_id(node, last_client_id)
                await asyncio.sleep(1.5)
            except Exception as e:
                print(f"    (cancel error: {e})")

        # Update orderbook for next attempt
        best_bid, best_ask, mid = await get_orderbook(indexer, market)
        if mid is None:
            print(f"    ⚠️ No orderbook para siguiente attempt")
            break

    # 6. Final fallback: MARKET order
    if FINAL_MARKET_FALLBACK and remaining > step * 0.5:
        print(f"\n  ⚠️ LIMIT attempts agotados. FALLBACK MARKET para {remaining} restantes...")
        try:
            oracle = mid  # use mid as reference
            await place_market_order(
                node, indexer, wallet, market, close_side,
                remaining, oracle, True,  # reduce_only
            )
            await asyncio.sleep(3)
            pos_final = await get_position(indexer, market)
            if pos_final is None or abs(float(pos_final.get("size", 0))) < step * 0.5:
                print(f"    ✅ Cerrado con MARKET fallback")
                return {"market": market, "success": True, "fallback": "market"}
        except Exception as e:
            print(f"    ❌ MARKET fallback failed: {e}")

    return {"market": market, "success": False, "remaining": remaining}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("markets", nargs="*", help="Markets to close (default: all open)")
    parser.add_argument("--dry-run", action="store_true", help="Simular sin ejecutar")
    parser.add_argument("--from-json", action="store_true", help="Use markets from bot_agents.json")
    args = parser.parse_args()

    print("Connecting to dYdX...")
    node, indexer, wallet = await connect_dydx()
    print("Connected.\n")

    # Determinar markets a cerrar
    if args.markets:
        markets_to_close = args.markets
    else:
        # Todas las posiciones abiertas del indexer
        resp = await indexer.account.get_subaccount(WALLET_ADDRESS.strip(), 0)
        sub = resp.get("subaccount", {}) or {}
        positions = sub.get("openPerpetualPositions", {}) or {}
        markets_to_close = list(positions.keys())

    if not markets_to_close:
        print("No hay posiciones abiertas. Fin.")
        return

    print(f"Markets a cerrar: {markets_to_close}")

    results = []
    for market in markets_to_close:
        r = await close_single_position(node, indexer, wallet, market, dry_run=args.dry_run)
        results.append(r)

    # Summary
    print("\n" + "=" * 70)
    print("  RESUMEN")
    print("=" * 70)
    success = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]
    print(f"  ✅ Cerrados OK: {len(success)}")
    for r in success:
        print(f"    {r['market']}  ({r.get('reason','')} attempts={r.get('attempts','?')})")
    if failed:
        print(f"  ❌ FALLARON: {len(failed)}")
        for r in failed:
            print(f"    {r['market']}  reason={r.get('reason','?')}")


if __name__ == "__main__":
    asyncio.run(main())
