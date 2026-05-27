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
from constants import WALLET_ADDRESS, UNMANAGED_IGNORE_MARKETS, TAKER_FEE_BPS
from func_pnl import leg_pnl

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


def emit_synthetic_trade_closed_for_pair(record: dict, positions: dict, closed_markets: set):
    """
    Bug #6 fix: emit a trade_closed event for each pair whose markets are
    being closed by close_all.py. Without this, close_all PnL is invisible
    to audit_run.py and session_summary accounting.

    The pair is considered closed if AT LEAST one of its legs was closed.
    We compute pnl_gross using the entry prices stored in bot_agents.json
    and the oracle prices at close (captured before the orders went out).
    """
    m1 = record.get("market_1", "")
    m2 = record.get("market_2", "")
    if not m1 or not m2:
        return

    # Only emit if at least one leg was actually closed
    if m1 not in closed_markets and m2 not in closed_markets:
        return

    trace_id = record.get("trace_id")

    entry1_side = record.get("side_1") or record.get("entry_side_1")
    entry2_side = record.get("side_2") or record.get("entry_side_2")
    try:
        entry1_price = float(record.get("price_1") or record.get("entry_price_1") or 0)
        entry2_price = float(record.get("price_2") or record.get("entry_price_2") or 0)
        entry1_size  = float(record.get("size_1") or record.get("entry_size_1") or 0)
        entry2_size  = float(record.get("size_2") or record.get("entry_size_2") or 0)
    except Exception:
        entry1_price = entry2_price = entry1_size = entry2_size = 0.0

    close_px_m1 = positions.get(m1, {}).get("oracle_price", 0.0) if m1 in positions else entry1_price
    close_px_m2 = positions.get(m2, {}).get("oracle_price", 0.0) if m2 in positions else entry2_price

    pnl_gross_1 = pnl_gross_2 = 0.0
    if entry1_side and entry1_price > 0 and close_px_m1 > 0:
        pnl_gross_1 = leg_pnl(entry1_side, entry1_price, close_px_m1, entry1_size)
    if entry2_side and entry2_price > 0 and close_px_m2 > 0:
        pnl_gross_2 = leg_pnl(entry2_side, entry2_price, close_px_m2, entry2_size)
    pnl_gross_total = pnl_gross_1 + pnl_gross_2

    open_fees_paid = _sf(record.get("fee_1", 0.0)) + _sf(record.get("fee_2", 0.0))

    # Estimate close fees from the size that was actually closed (oracle × taker bps)
    close_notional = 0.0
    if m1 in closed_markets and close_px_m1 > 0:
        close_notional += abs(entry1_size) * close_px_m1
    if m2 in closed_markets and close_px_m2 > 0:
        close_notional += abs(entry2_size) * close_px_m2
    close_fees_est = close_notional * float(TAKER_FEE_BPS)

    net_pnl_est = pnl_gross_total - open_fees_paid - close_fees_est

    fee_estimated_at_open = bool(
        record.get("fee_1_estimated", False) or record.get("fee_2_estimated", False)
    )
    # Whether the pair was fully closed (both legs) or only one
    both_closed = (m1 in closed_markets and m2 in closed_markets)
    one_leg_only = not both_closed

    log_event({
        "type": "trade_closed",
        "trace_id": trace_id,
        "market_1": m1,
        "market_2": m2,
        "close_reason": "CLOSE_ALL: bulk close via close_all.py" + (
            f" (one_leg_only: m1_closed={m1 in closed_markets} m2_closed={m2 in closed_markets})"
            if one_leg_only else ""
        ),
        "close_type_m1": "taker",
        "close_type_m2": "taker",
        "pnl_gross": pnl_gross_total,
        "pnl_gross_leg_1": pnl_gross_1,
        "pnl_gross_leg_2": pnl_gross_2,
        "open_fees": open_fees_paid,
        "close_fees_est": close_fees_est,
        "net_pnl_est": net_pnl_est,
        "fee_estimated": fee_estimated_at_open,
        "pnl_provisional": True,  # always provisional: we lose track of orphan leg PnL otherwise
        "synthetic": True,
        "source": "close_all",
        "both_legs_closed": both_closed,
    })


def clean_bot_agents_json(closed_markets: set, positions: dict):
    """Remove JSON records whose markets have been closed.

    Also emits a synthetic trade_closed event per affected pair so that
    audit_run.py and session_summary include close_all PnL.
    """
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
            # Bug #6 fix: emit synthetic trade_closed before discarding
            try:
                emit_synthetic_trade_closed_for_pair(r, positions, closed_markets)
            except Exception as ee:
                log_event({
                    "type": "trade_closed_emit_error",
                    "trace_id": r.get("trace_id"),
                    "market_1": m1, "market_2": m2,
                    "source": "close_all",
                    "error": str(ee),
                })
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
        clean_bot_agents_json(set(closed_ok), positions)

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
