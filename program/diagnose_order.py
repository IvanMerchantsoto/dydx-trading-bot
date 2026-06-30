#!/usr/bin/env python3
"""
diagnose_order.py — Test E2E de colocación de UNA orden y reporte detallado.

Propósito: distinguir entre 3 escenarios cuando el bot ve 100% NOT_FOUND:
  A) Orden rechazada en chain         → tx no aparece, no hay fill, no hay order
  B) Orden aceptada pero lag indexer  → tx aparece en chain pero indexer tarda
  C) Orden aceptada y fill instantaneo → todo OK, era timeout audit (8s)

Lo que hace:
  1. Conecta a mainnet (node + indexer).
  2. Lee equity inicial.
  3. Envía UNA orden MARKET IOC pequeña ($5 notional) en ETH-USD BUY.
  4. Captura tx_hash inmediatamente.
  5. Cada 2s durante 60s:
     - Consulta orders del subaccount → ¿aparece esta orden?
     - Consulta fills del subaccount → ¿hay un fill nuevo?
     - Consulta subaccount → ¿cambió equity / hay posición?
  6. Imprime timeline de lo que pasó.

Run:
    cd ~/DYDX/program
    source ~/DYDX/.venv/bin/activate
    python3 diagnose_order.py

Si la orden ejecuta → tienes una posición chica de ETH. Cerrarla manual:
    python3 -c "
import asyncio
from func_private import place_market_order
from func_connections import connect_dydx

async def close():
    node, indexer, wallet = await connect_dydx()
    # Si entró BUY 0.0015 ETH, cierras con SELL 0.0015 reduce_only=True
    await place_market_order(node, wallet, 'ETH-USD', 'SELL', 0.0015, reduce_only=True)
asyncio.run(close())
    "
"""

import asyncio
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

from func_connections import connect_dydx
from func_private import place_market_order
from constants import WALLET_ADDRESS

TEST_MARKET = "ETH-USD"
TEST_SIDE = "BUY"
TEST_USD = 5.0                  # notional ultra pequeño para no exponerse
POLL_INTERVAL_S = 2
POLL_DURATION_S = 60


def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


async def get_oracle_price(indexer, market):
    try:
        markets_resp = await indexer.markets.get_perpetual_markets()
        md = markets_resp.get("markets", {}).get(market, {})
        return float(md.get("oraclePrice") or 0.0)
    except Exception:
        return 0.0


async def get_subaccount_state(indexer, address):
    """Returns dict {equity, free, positions, num_positions}."""
    try:
        resp = await indexer.account.get_subaccounts(address)
        subs = resp.get("subaccounts") or []
        if not subs:
            return None
        sub = subs[0]
        positions = sub.get("openPerpetualPositions") or {}
        return {
            "equity": float(sub.get("equity") or 0),
            "free": float(sub.get("freeCollateral") or 0),
            "positions": positions,
            "num_positions": len(positions),
        }
    except Exception as e:
        return {"error": str(e)}


async def get_orders(indexer, address):
    try:
        resp = await indexer.account.get_subaccount_orders(address, 0)
        if isinstance(resp, list):
            return resp
        return resp.get("orders", []) or []
    except Exception as e:
        return [{"error": str(e)}]


async def get_fills(indexer, address):
    try:
        resp = await indexer.account.get_subaccount_fills(address, 0)
        if isinstance(resp, list):
            return resp
        return resp.get("fills", []) or []
    except Exception as e:
        return [{"error": str(e)}]


async def main():
    print(f"\n{'='*72}")
    print(f"DIAGNOSE_ORDER  —  {_now()}")
    print(f"{'='*72}\n")

    print(f"[1/6] Conectando a dYdX mainnet...")
    conn = await connect_dydx()
    # connect_dydx puede regresar (None, None, None) si falla
    if not conn or any(x is None for x in conn):
        print(f"\n      ❌ Conexión FALLÓ. Causas comunes:")
        print(f"         • Estás en red corporativa con Netskope/proxy SSL")
        print(f"           → Conéctate a hotspot del celular y reintenta")
        print(f"           → O corre este script en la VM (sin Netskope)")
        print(f"         • El NODE_MAINNET URL es incorrecto / caído")
        print(f"           → Prueba un endpoint alternativo en constants.py:")
        print(f"             - dydx-mainnet-grpc.kingnodes.com")
        print(f"             - dydx-grpc.lavenderfive.com")
        return
    node, indexer, wallet = conn
    print(f"      ✓ Node + indexer + wallet OK")
    print(f"      Address: {WALLET_ADDRESS}\n")

    print(f"[2/6] Estado inicial del subaccount...")
    pre = await get_subaccount_state(indexer, WALLET_ADDRESS)
    if not pre or pre.get("error"):
        print(f"      ❌ No se pudo leer subaccount: {pre}")
        return
    print(f"      Equity: ${pre['equity']:.2f}")
    print(f"      Free:   ${pre['free']:.2f}")
    print(f"      Positions: {pre['num_positions']}")
    if pre["positions"]:
        for m, p in pre["positions"].items():
            print(f"        - {m}: size={p.get('size')} side={p.get('side')}")
    print()

    print(f"[3/6] Obteniendo oracle price para {TEST_MARKET}...")
    oracle = await get_oracle_price(indexer, TEST_MARKET)
    if oracle <= 0:
        print(f"      ❌ Oracle price inválido: {oracle}")
        return
    print(f"      Oracle: ${oracle:,.2f}")

    raw_size = TEST_USD / oracle
    # Para ETH-USD el step size es 0.0001, así que redondeamos
    test_size = round(raw_size, 4)
    if test_size <= 0:
        print(f"      ❌ Test size {test_size} demasiado chico. Ajusta TEST_USD.")
        return
    notional = test_size * oracle
    print(f"      Test size: {test_size} ETH (~${notional:.2f})\n")

    pre_orders = await get_orders(indexer, WALLET_ADDRESS)
    pre_fills = await get_fills(indexer, WALLET_ADDRESS)
    pre_order_ids = {str(o.get("clientId")) for o in pre_orders if isinstance(o, dict)}
    pre_fill_ids = {str(f.get("id")) for f in pre_fills if isinstance(f, dict)}
    print(f"[4/6] Estado pre-orden:")
    print(f"      Orders existentes: {len(pre_orders)}")
    print(f"      Fills existentes (history):  {len(pre_fills)}")
    if pre_fills:
        last_fill = pre_fills[0] if isinstance(pre_fills[0], dict) else None
        if last_fill:
            print(f"      Last fill ts: {last_fill.get('createdAt')}")
    print()

    print(f"[5/6] >>> ENVIANDO MARKET {TEST_SIDE} {test_size} {TEST_MARKET} ~${notional:.2f} <<<")
    t_send = time.time()
    try:
        result = await place_market_order(
            node, wallet,
            market=TEST_MARKET,
            side=TEST_SIDE,
            size=test_size,
            reduce_only=False,
        )
    except Exception as e:
        print(f"      ❌❌❌ EXCEPCIÓN al enviar orden: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return
    t_sent = time.time()
    elapsed = t_sent - t_send
    print(f"      ✓ place_market_order regresó en {elapsed*1000:.0f} ms")
    print(f"      Return: {result}")
    tx_hash = result.get("tx_hash") if isinstance(result, dict) else None
    client_id = (
        result.get("order", {}).get("id") if isinstance(result, dict) else None
    )
    print(f"      tx_hash:   {tx_hash}")
    print(f"      client_id: {client_id}")
    if tx_hash == "unknown":
        print(f"      ⚠️  tx_hash = 'unknown' — la tx pudo no haberse broadcast OK")
    print()

    print(f"[6/6] Polling indexer cada {POLL_INTERVAL_S}s durante {POLL_DURATION_S}s...\n")
    print(f"      {'t+s':>5} | {'in_orders':>9} | {'in_fills':>8} | {'positions':>9} | {'equity':>8} | {'notes'}")
    print(f"      {'-'*5} | {'-'*9} | {'-'*8} | {'-'*9} | {'-'*8} | {'-'*30}")

    found_in_orders_at = None
    found_in_fills_at = None
    position_appeared_at = None
    final_orders = []
    final_fills = []

    t0 = time.time()
    while time.time() - t0 < POLL_DURATION_S:
        elapsed = int(time.time() - t0)
        orders = await get_orders(indexer, WALLET_ADDRESS)
        fills = await get_fills(indexer, WALLET_ADDRESS)
        state = await get_subaccount_state(indexer, WALLET_ADDRESS)

        new_orders = [
            o for o in orders
            if isinstance(o, dict) and str(o.get("clientId")) not in pre_order_ids
        ]
        new_fills = [
            f for f in fills
            if isinstance(f, dict) and str(f.get("id")) not in pre_fill_ids
        ]

        in_orders = len(new_orders)
        in_fills = len(new_fills)
        npos = state.get("num_positions", 0) if state else 0
        eq = state.get("equity", 0) if state else 0

        notes = ""
        if in_orders > 0 and not found_in_orders_at:
            found_in_orders_at = elapsed
            o0 = new_orders[0]
            notes = f"ORDER seen: status={o0.get('status')} cid={o0.get('clientId')}"
        if in_fills > 0 and not found_in_fills_at:
            found_in_fills_at = elapsed
            f0 = new_fills[0]
            notes = f"FILL seen: size={f0.get('size')} px={f0.get('price')} side={f0.get('side')}"
        if npos > pre["num_positions"] and not position_appeared_at:
            position_appeared_at = elapsed
            notes = f"POSITION created: now {npos}"

        print(f"      {elapsed:>5} | {in_orders:>9} | {in_fills:>8} | {npos:>9} | ${eq:>6.2f} | {notes}")

        final_orders = new_orders
        final_fills = new_fills

        if found_in_fills_at is not None and position_appeared_at is not None:
            # Llegamos a estado terminal — la orden ejecutó
            await asyncio.sleep(2)  # un poll más para confirmar
            break

        await asyncio.sleep(POLL_INTERVAL_S)

    # ── Final summary
    print()
    print(f"{'='*72}")
    print(f"DIAGNÓSTICO FINAL")
    print(f"{'='*72}")

    post = await get_subaccount_state(indexer, WALLET_ADDRESS)
    print(f"  Equity inicial:    ${pre['equity']:.4f}")
    print(f"  Equity final:      ${post['equity']:.4f}")
    print(f"  Cambio:            ${post['equity'] - pre['equity']:+.4f}")
    print(f"  Positions antes:   {pre['num_positions']}")
    print(f"  Positions despues: {post['num_positions']}")
    print(f"  Orden vista en /orders en t+{found_in_orders_at}s" if found_in_orders_at is not None else "  Orden NUNCA apareció en /orders")
    print(f"  Fill visto en /fills en t+{found_in_fills_at}s" if found_in_fills_at is not None else "  Fill NUNCA apareció en /fills")
    print()

    if found_in_fills_at is not None:
        print(f"  ✅ DIAGNÓSTICO: indexer SÍ vio el fill.")
        if found_in_fills_at > 8:
            print(f"     ⚠️  Tardó {found_in_fills_at}s (> 8s del audit window del bot).")
            print(f"     → El bug ES indexer lag. Ya subimos audit_retries a 20 (~60s).")
        else:
            print(f"     Llegó en {found_in_fills_at}s — el audit window NORMAL debería verlo.")
            print(f"     Si el bot reporta NOT_FOUND aquí, hay un bug en get_real_fill_details.")
        if final_fills:
            print(f"\n  Fill final:")
            import json
            print(json.dumps(final_fills[0], indent=4))

    elif found_in_orders_at is not None:
        # Orden vista pero no llena → KILLED_BY_FOK o pendiente
        print(f"  ⚠️  DIAGNÓSTICO: Orden ACEPTADA por chain pero NO FILLED.")
        print(f"     → Razón posible: IOC sin liquidez al precio enviado.")
        print(f"     Inspecciona la orden:")
        import json
        if final_orders:
            print(json.dumps(final_orders[0], indent=4))

    else:
        print(f"  ❌ DIAGNÓSTICO: La orden NUNCA llegó al indexer.")
        print(f"     Posibles causas:")
        print(f"       1. La tx fue rechazada por el NODE durante broadcast")
        print(f"          → check: tx_hash = '{tx_hash}'")
        print(f"          → if 'unknown', el node no aceptó la tx")
        print(f"       2. El NODE acepta pero el chain rechaza al validar")
        print(f"          (sequence mismatch, fees, account_number, etc.)")
        print(f"       3. El indexer está apuntando a otra red que el node")
        print(f"          (no probable porque equity = {pre['equity']})")
        print(f"\n     Próximo paso: busca el tx_hash en https://mintscan.io/dydx")
        print(f"     Si encuentras la tx → es el indexer (improbable)")
        print(f"     Si NO la encuentras → la tx no se broadcastó / chain la rechazó")


if __name__ == "__main__":
    asyncio.run(main())
