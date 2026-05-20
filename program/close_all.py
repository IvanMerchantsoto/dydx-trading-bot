"""
close_all.py — Cierre manual controlado de todas las posiciones abiertas en dYdX.

Reemplaza ABORT_ALL_POSITIONS=True. Úsalo SOLO cuando quieras limpiar manualmente.

Uso:
    python close_all.py           # cierra todo y limpia bot_agents.json
    python close_all.py --dry-run # muestra qué cerraría sin ejecutar
    python close_all.py --keep-json  # cierra en dYdX pero no toca bot_agents.json

Qué hace:
    1. Conecta a dYdX
    2. Lista posiciones abiertas con notional y PnL unrealized
    3. Pide confirmación (excepto con --yes)
    4. Cierra cada posición con reduce_only=True (MARKET IOC)
    5. Espera confirmación de fill
    6. Limpia bot_agents.json (elimina registros cuyas posiciones ya no existen)
    7. Envía resumen por Telegram
"""

import asyncio
import argparse
import json
import os
import sys

# ── ensure imports resolve from this directory ──
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from func_connections import connect_dydx
from func_messaging import send_message
from func_logging import log_event
from func_utils import format_number
from v4_proto.dydxprotocol.clob.order_pb2 import Order
from constants import WALLET_ADDRESS, UNMANAGED_IGNORE_MARKETS

JSON_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_agents.json")


def _sf(x, d=0.0):
    try:
        return float(x)
    except Exception:
        return d


async def get_all_open_positions(indexer):
    """Returns {market: {size, oraclePrice, unrealizedPnl, side, notional}}"""
    resp = await indexer.account.get_subaccount(WALLET_ADDRESS.strip(), 0)
    sub = resp.get("subaccount", {}) or {}
    positions = sub.get("openPerpetualPositions", {}) or sub.get("perpetualPositions", {}) or {}

    mk_resp = await indexer.markets.get_perpetual_markets()
    markets = mk_resp.get("markets", {}) or {}

    result = {}
    for mkt, pdata in (positions or {}).items():
        size = _sf(pdata.get("size"))
        if abs(size) < 1e-9:
            continue
        px = _sf(markets.get(mkt, {}).get("oraclePrice"))
        if px <= 0:
            px = _sf(pdata.get("markPrice") or pdata.get("entryPrice"))
        notional = abs(size) * px if px > 0 else 0.0
        unreal = _sf(pdata.get("unrealizedPnl") or pdata.get("unrealizedPnL") or 0)
        step = markets.get(mkt, {}).get("stepSize")
        result[mkt] = {
            "size": size,
            "side": "LONG" if size > 0 else "SHORT",
            "oracle_price": px,
            "notional": notional,
            "unrealized_pnl": unreal,
            "step_size": step,
        }
    return result, markets


async def close_position(node, indexer, wallet, market, pos_info, markets_data, dry_run=False):
    size = pos_info["size"]
    close_side = "SELL" if size > 0 else "BUY"
    close_size = abs(size)

    step = pos_info.get("step_size")
    if step:
        close_size = float(format_number(close_size, step))

    oracle_px = pos_info["oracle_price"]

    print(f"  {'[DRY RUN] ' if dry_run else ''}Closing {market}: {close_side} {close_size} "
          f"@ oracle ${oracle_px:.4f} | notional ${pos_info['notional']:.2f} "
          f"| unreal ${pos_info['unrealized_pnl']:+.2f}")

    if dry_run:
        return {"market": market, "closed": False, "dry_run": True}

    from func_private import place_market_order
    try:
        res = await place_market_order(
            node, indexer, wallet,
            market, close_side, close_size, oracle_px,
            reduce_only=True,
            time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC,
        )
        cid = (res or {}).get("order", {}).get("id")
        log_event({
            "type": "close_all_sent",
            "market": market,
            "side": close_side,
            "size": close_size,
            "oracle_px": oracle_px,
            "client_id": cid,
        })
        await asyncio.sleep(1.5)  # brief pause between orders
        return {"market": market, "closed": True, "client_id": cid}
    except Exception as e:
        print(f"    ERROR closing {market}: {e}")
        log_event({"type": "close_all_error", "market": market, "error": str(e)})
        return {"market": market, "closed": False, "error": str(e)}


def clean_bot_agents_json(closed_markets: set):
    """Remove JSON records whose markets have been closed."""
    try:
        with open(JSON_PATH, "r") as f:
            records = json.load(f)
    except Exception:
        records = []

    kept = []
    removed = 0
    for r in records:
        m1 = r.get("market_1", "")
        m2 = r.get("market_2", "")
        if m1 in closed_markets or m2 in closed_markets:
            removed += 1
        else:
            kept.append(r)

    tmp = JSON_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(kept, f, indent=2)
    os.replace(tmp, JSON_PATH)
    print(f"  bot_agents.json: removed {removed} records, kept {len(kept)}")
    return removed


async def run(args):
    dry_run = args.dry_run
    keep_json = args.keep_json
    skip_confirm = args.yes
    skip_markets = set(UNMANAGED_IGNORE_MARKETS or [])
    if args.skip:
        skip_markets.update(args.skip)

    print("Connecting to dYdX...", flush=True)
    node, indexer, wallet = await connect_dydx()
    print("Connected.", flush=True)

    positions, markets_data = await get_all_open_positions(indexer)

    if not positions:
        print("No open positions found. Nothing to do.")
        return

    # ── Summary ──
    total_notional = sum(p["notional"] for p in positions.values())
    total_unreal = sum(p["unrealized_pnl"] for p in positions.values())
    to_close = {m: p for m, p in positions.items() if m not in skip_markets}
    skipped = {m: p for m, p in positions.items() if m in skip_markets}

    print(f"\n{'='*60}")
    print(f"OPEN POSITIONS ({len(positions)} total | notional ${total_notional:,.2f} | unreal ${total_unreal:+.2f})")
    print(f"{'='*60}")
    for mkt, p in sorted(positions.items()):
        tag = " [SKIP - unmanaged]" if mkt in skip_markets else ""
        print(f"  {mkt:20s} {p['side']:5s} size={p['size']:>12.4f} "
              f"notional=${p['notional']:>8.2f}  unreal=${p['unrealized_pnl']:>+8.2f}{tag}")

    print(f"\nWill close: {len(to_close)} positions")
    if skipped:
        print(f"Will skip:  {len(skipped)} unmanaged markets: {sorted(skipped.keys())}")

    if not to_close:
        print("Nothing to close (all are in UNMANAGED_IGNORE_MARKETS).")
        return

    if dry_run:
        print("\n[DRY RUN] — No orders will be sent.\n")
    elif not skip_confirm:
        confirm = input(f"\nClose {len(to_close)} positions? [y/N] ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            return

    print(f"\n{'Simulating' if dry_run else 'Closing'} {len(to_close)} positions...")
    results = []
    for mkt, pos_info in sorted(to_close.items()):
        r = await close_position(node, indexer, wallet, mkt, pos_info, markets_data, dry_run=dry_run)
        results.append(r)

    closed_ok = [r["market"] for r in results if r.get("closed")]
    closed_fail = [r["market"] for r in results if not r.get("closed") and not r.get("dry_run")]

    print(f"\n{'='*60}")
    print(f"Results: {len(closed_ok)} closed OK | {len(closed_fail)} failed | {len(skipped)} skipped")
    if closed_fail:
        print(f"  Failed: {closed_fail}")

    # ── Clean JSON ──
    if not dry_run and not keep_json and closed_ok:
        print("\nCleaning bot_agents.json...")
        clean_bot_agents_json(set(closed_ok))

    # ── Telegram summary ──
    if not dry_run:
        emoji = "✅" if not closed_fail else "⚠️"
        msg_lines = [
            f"{emoji} close_all.py completado",
            f"Cerrados: {len(closed_ok)} | Fallidos: {len(closed_fail)} | Saltados: {len(skipped)}",
            f"Notional cerrado: ${sum(to_close[m]['notional'] for m in closed_ok if m in to_close):,.2f}",
            f"PnL unrealized al cierre: ${sum(to_close[m]['unrealized_pnl'] for m in closed_ok if m in to_close):+.2f}",
        ]
        if closed_fail:
            msg_lines.append(f"Fallidos: {', '.join(closed_fail)}")
        send_message("\n".join(msg_lines))

        log_event({
            "type": "close_all_complete",
            "closed_ok": closed_ok,
            "closed_fail": closed_fail,
            "skipped": sorted(skipped.keys()),
            "total_notional_closed": sum(to_close[m]["notional"] for m in closed_ok if m in to_close),
        })

    print("\nDone.")


def main():
    parser = argparse.ArgumentParser(description="Manually close all dYdX positions")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be closed without sending orders")
    parser.add_argument("--yes", "-y", action="store_true",
                        help="Skip confirmation prompt")
    parser.add_argument("--keep-json", action="store_true",
                        help="Don't modify bot_agents.json after closing")
    parser.add_argument("--skip", nargs="*", default=[],
                        help="Additional markets to skip (space-separated, e.g. BTC-USD ETH-USD)")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
