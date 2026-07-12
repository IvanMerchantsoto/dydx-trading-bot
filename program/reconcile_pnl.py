#!/usr/bin/env python3
"""
reconcile_pnl.py — Reconciliación del PnL REAL vs el PnL interno del bot.

Motivación (auditoría fase 1, E3):
  El PnL interno del bot (net_pnl_est en trade_closed) se calcula con precios
  ORÁCULO y fees a menudo estimadas → es sistemáticamente OPTIMISTA porque no
  ve el slippage IOC ni el cruce de spread. Esta herramienta calcula el PnL
  REAL a partir de los FILLS y FUNDING del exchange (fuente de verdad) y lo
  compara con lo que el bot cree que ganó/perdió.

Método (no requiere FIFO):
  trading_pnl = Σ(sell_notional) − Σ(buy_notional) − Σ(fees)
                + Σ_m (net_inventory_m × mark_price_m)     ← MtM del inventario abierto
  total_pnl   = trading_pnl + Σ(funding_payments)
  Cross-check = dYdX historical-pnl (equity/totalPnl calculado por el exchange)
              = equity_actual − equity_inicial − net_transfers

SOLO LECTURA. No envía órdenes. Sólo consulta el indexer (público) y lee logs.
Ejecutar en la VM (necesita red):
    python3 reconcile_pnl.py --days 7
    python3 reconcile_pnl.py --days 30 --log logs/bot_run.log.jsonl
"""

import argparse
import asyncio
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dydx_v4_client.indexer.rest.indexer_client import IndexerClient
from constants import INDEXER, WALLET_ADDRESS

SCRIPT_DIR = Path(__file__).parent


def _sf(x, d=0.0):
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _get(d, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and d.get(k) is not None:
            return d[k]
    return default


async def fetch_all_fills(indexer, address, days):
    """Pagina fills hacia atrás por createdBeforeOrAt hasta cubrir `days`."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    all_fills = []
    before = None
    seen = set()
    for _ in range(200):  # tope defensivo de páginas
        resp = await indexer.account.get_subaccount_fills(
            address=address, subaccount_number=0, limit=100,
            created_before_or_at=before,
        )
        fills = resp.get("fills", []) if isinstance(resp, dict) else (resp or [])
        if not fills:
            break
        new = 0
        oldest = None
        for f in fills:
            fid = str(_get(f, "id", "eventId", default=json.dumps(f, sort_keys=True)[:64]))
            if fid in seen:
                continue
            seen.add(fid)
            new += 1
            all_fills.append(f)
            ts = _get(f, "createdAt")
            if ts and (oldest is None or ts < oldest):
                oldest = ts
        if new == 0 or oldest is None:
            break
        # ¿ya pasamos el cutoff?
        try:
            if datetime.fromisoformat(oldest.replace("Z", "+00:00")) < cutoff:
                break
        except Exception:
            pass
        before = oldest
    # filtrar por ventana
    out = []
    for f in all_fills:
        ts = _get(f, "createdAt")
        try:
            if ts and datetime.fromisoformat(ts.replace("Z", "+00:00")) >= cutoff:
                out.append(f)
        except Exception:
            out.append(f)
    return out


async def fetch_funding(indexer, address, days):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    total = 0.0
    n = 0
    by_market = defaultdict(float)
    try:
        for page in range(1, 50):
            resp = await indexer.account.get_funding_payments(
                address=address, subaccount_id=0, limit=100, after_or_at=cutoff, page=page,
            )
            pays = resp.get("fundingPayments", []) if isinstance(resp, dict) else (resp or [])
            if not pays:
                break
            for p in pays:
                amt = _sf(_get(p, "payment", "amount"))
                total += amt
                by_market[_get(p, "ticker", "market", default="?")] += amt
                n += 1
            if len(pays) < 100:
                break
    except Exception as e:
        print(f"  [funding] no disponible: {e}")
    return total, n, dict(by_market)


async def fetch_marks(indexer):
    try:
        resp = await indexer.markets.get_perpetual_markets()
        m = resp.get("markets", {}) if isinstance(resp, dict) else {}
        return {k: _sf(v.get("oraclePrice")) for k, v in m.items()}
    except Exception:
        return {}


async def fetch_open_positions(indexer, address):
    try:
        resp = await indexer.account.get_subaccount(address, 0)
        sub = resp.get("subaccount", {}) or {}
        equity = _sf(sub.get("equity"))
        free = _sf(sub.get("freeCollateral"))
        positions = sub.get("openPerpetualPositions", {}) or sub.get("perpetualPositions", {}) or {}
        inv = {}
        for m, p in (positions or {}).items():
            sz = _sf(p.get("size"))
            if abs(sz) > 0:
                inv[m] = sz
        return equity, free, inv
    except Exception:
        return 0.0, 0.0, {}


async def fetch_exchange_pnl(indexer, address):
    """dYdX calcula su propio PnL en historical-pnl → cross-check definitivo."""
    try:
        resp = await indexer.account.get_subaccount_historical_pnls(address=address, subaccount_number=0)
        hist = resp.get("historicalPnl", []) if isinstance(resp, dict) else (resp or [])
        if not hist:
            return None
        hist_sorted = sorted(hist, key=lambda h: _get(h, "createdAt", default=""))
        first, last = hist_sorted[0], hist_sorted[-1]
        return {
            "first_equity": _sf(_get(first, "equity")),
            "last_equity": _sf(_get(last, "equity")),
            "last_total_pnl": _sf(_get(last, "totalPnl")),
            "last_net_transfers": _sf(_get(last, "netTransfers")),
            "first_at": _get(first, "createdAt"),
            "last_at": _get(last, "createdAt"),
            "n_points": len(hist_sorted),
        }
    except Exception as e:
        print(f"  [historical-pnl] no disponible: {e}")
        return None


def read_internal_pnl(log_paths, days):
    """Suma net_pnl_est de eventos trade_closed en los logs, dentro de la ventana."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    total = 0.0
    n = 0
    provisional = 0
    for lp in log_paths:
        if not os.path.exists(lp):
            continue
        with open(lp, errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line or '"trade_closed"' not in line:
                    continue
                try:
                    j = json.loads(line)
                except Exception:
                    continue
                if j.get("type") != "trade_closed":
                    continue
                ts = j.get("ts")
                if ts:
                    try:
                        if datetime.fromisoformat(ts.replace("Z", "+00:00")) < cutoff:
                            continue
                    except Exception:
                        pass
                total += _sf(j.get("net_pnl_est"))
                n += 1
                if j.get("pnl_provisional"):
                    provisional += 1
    return total, n, provisional


async def main():
    ap = argparse.ArgumentParser(description="Reconciliación PnL real (fills+funding) vs interno")
    ap.add_argument("--days", type=int, default=7, help="Ventana de reconciliación (días)")
    ap.add_argument("--log", action="append", default=None,
                    help="Ruta(s) a bot_run.log.jsonl (repetible). Default: los dos habituales.")
    args = ap.parse_args()

    log_paths = args.log or [
        str(SCRIPT_DIR / "bot_run.log.jsonl"),
        str(SCRIPT_DIR / "logs" / "bot_run.log.jsonl"),
    ]
    addr = WALLET_ADDRESS.strip()

    print(f"\n{'='*70}\n  RECONCILIACIÓN DE PnL — últimos {args.days} días\n  Wallet: {addr}\n{'='*70}")
    indexer = IndexerClient(INDEXER)

    print("\nDescargando fills, funding, posiciones y PnL del exchange...")
    fills = await fetch_all_fills(indexer, addr, args.days)
    funding_total, funding_n, funding_by_market = await fetch_funding(indexer, addr, args.days)
    marks = await fetch_marks(indexer)
    equity, free, inv = await fetch_open_positions(indexer, addr)
    exch = await fetch_exchange_pnl(indexer, addr)

    # ── PnL real desde fills ──────────────────────────────────────────────
    buy_notional = 0.0
    sell_notional = 0.0
    fees = 0.0
    net_size = defaultdict(float)   # market -> signed size neteado por fills
    cash_by_market = defaultdict(float)
    for f in fills:
        side = str(_get(f, "side", default="")).upper()
        size = _sf(_get(f, "size", "filledSize", "amount"))
        price = _sf(_get(f, "price", "fillPrice"))
        fee = _sf(_get(f, "fee", "feeAmount", "feeUsd"))
        mkt = _get(f, "market", "ticker", default="?")
        notional = size * price
        fees += fee
        if side == "BUY":
            buy_notional += notional
            net_size[mkt] += size
            cash_by_market[mkt] -= notional
        elif side == "SELL":
            sell_notional += notional
            net_size[mkt] -= size
            cash_by_market[mkt] += notional
        cash_by_market[mkt] -= fee

    # Mark-to-market del inventario abierto (usa posición real del subaccount)
    mtm = 0.0
    for m, sz in inv.items():
        px = marks.get(m, 0.0)
        mtm += sz * px
        cash_by_market[m] += sz * px

    trading_pnl = sell_notional - buy_notional - fees + mtm
    total_real_pnl = trading_pnl + funding_total

    internal_pnl, internal_n, provisional_n = read_internal_pnl(log_paths, args.days)

    # ── Reporte ───────────────────────────────────────────────────────────
    print(f"\n{'─'*70}\n  PnL REAL (fills + funding + MtM)\n{'─'*70}")
    print(f"  Fills procesados:        {len(fills)}")
    print(f"  Σ sell_notional:         ${sell_notional:,.2f}")
    print(f"  Σ buy_notional:          ${buy_notional:,.2f}")
    print(f"  Σ fees (reales):         ${fees:,.2f}")
    print(f"  MtM inventario abierto:  ${mtm:,.2f}  ({len(inv)} posiciones)")
    print(f"  Funding neto:            ${funding_total:,.2f}  ({funding_n} pagos)")
    print(f"  ──")
    print(f"  TRADING PnL:             ${trading_pnl:,.2f}")
    print(f"  TOTAL PnL REAL:          ${total_real_pnl:,.2f}")

    print(f"\n{'─'*70}\n  PnL INTERNO del bot (trade_closed.net_pnl_est)\n{'─'*70}")
    print(f"  Cierres contabilizados:  {internal_n}  (provisional={provisional_n})")
    print(f"  Σ net_pnl_est interno:   ${internal_pnl:,.2f}")

    print(f"\n{'─'*70}\n  DIVERGENCIA interno vs real\n{'─'*70}")
    diff = internal_pnl - total_real_pnl
    denom = abs(total_real_pnl) if abs(total_real_pnl) > 1e-9 else 1.0
    print(f"  interno − real:          ${diff:,.2f}  ({diff/denom*100:+.1f}%)")
    if abs(diff) > max(1.0, 0.20 * denom):
        print(f"  ⚠️  DIVERGENCIA > 20% — la contabilidad interna NO es fiable para")
        print(f"      decisiones. El interno omite slippage/spread no capturado.")
    else:
        print(f"  ✅ Dentro de tolerancia (20%).")

    if exch:
        print(f"\n{'─'*70}\n  CROSS-CHECK — historical-pnl del exchange (dYdX)\n{'─'*70}")
        exch_pnl = exch['last_equity'] - exch['first_equity'] - exch['last_net_transfers']
        print(f"  Ventana:                 {exch['first_at']} → {exch['last_at']} ({exch['n_points']} pts)")
        print(f"  Equity:                  ${exch['first_equity']:,.2f} → ${exch['last_equity']:,.2f}")
        print(f"  Net transfers:           ${exch['last_net_transfers']:,.2f}")
        print(f"  PnL exchange (eq−transf):${exch_pnl:,.2f}")
        print(f"  totalPnl (campo):        ${exch['last_total_pnl']:,.2f}")

    print(f"\n  Equity actual: ${equity:,.2f} | Free: ${free:,.2f}")

    # Top perdedores por mercado (cash neteado)
    losers = sorted(cash_by_market.items(), key=lambda kv: kv[1])[:10]
    if losers:
        print(f"\n{'─'*70}\n  TOP 10 mercados por PnL de caja neteado (incl. MtM y fees)\n{'─'*70}")
        for m, v in losers:
            print(f"    {m:<16} ${v:,.2f}   (inv abierto: {net_size.get(m,0):+.4f})")

    print()


if __name__ == "__main__":
    asyncio.run(main())
