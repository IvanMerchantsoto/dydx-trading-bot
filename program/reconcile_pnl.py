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
  Cross-check = dYdX historical-pnl (totalPnl calculado por el exchange).

2026-07-12: reescrito con `requests` directo contra el indexer (sin el SDK ni
httpx) para que corra con cualquier python. SOLO LECTURA, no opera.

Uso (en la VM):
    python3 reconcile_pnl.py --days 7
    python3 reconcile_pnl.py --days 30 --log logs/bot_run.log.jsonl
"""

import argparse
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).parent
_S = requests.Session()


def _read_const(name, default=None):
    """
    Lee una constante de tipo string de constants.py SIN importarlo (evita la
    dependencia de decouple/.env, para poder correr con cualquier python).
    Ignora la línea comentada de testnet (empieza con '#').
    """
    try:
        txt = (SCRIPT_DIR / "constants.py").read_text()
        m = re.search(rf'^{name}\s*=\s*"([^"]+)"', txt, re.M)
        return m.group(1) if m else default
    except Exception:
        return default


DEFAULT_ADDRESS = _read_const("WALLET_ADDRESS", "")
DEFAULT_INDEXER = _read_const("INDEXER_MAINNET", "https://indexer.dydx.trade")
BASE = DEFAULT_INDEXER.rstrip("/")  # se sobreescribe en main() con --indexer


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


def api(path, params=None):
    for attempt in range(4):
        try:
            r = _S.get(f"{BASE}{path}", params=params or {}, timeout=15)
            if r.status_code == 429 or r.status_code >= 500:
                import time as _t
                _t.sleep(0.6 * (2 ** attempt))
                continue
            r.raise_for_status()
            return r.json()
        except Exception:
            import time as _t
            _t.sleep(0.4 * (2 ** attempt))
    return {}


def fetch_all_fills(addr, days):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    out, before, seen = [], None, set()
    for _ in range(200):
        params = {"address": addr, "subaccountNumber": 0, "limit": 100}
        if before:
            params["createdBeforeOrAt"] = before
        resp = api("/v4/fills", params)
        fills = resp.get("fills", []) if isinstance(resp, dict) else []
        if not fills:
            break
        new, oldest = 0, None
        for f in fills:
            fid = str(_get(f, "id", "eventId", default=json.dumps(f, sort_keys=True)[:64]))
            if fid in seen:
                continue
            seen.add(fid); new += 1; out.append(f)
            ts = _get(f, "createdAt")
            if ts and (oldest is None or ts < oldest):
                oldest = ts
        if new == 0 or oldest is None:
            break
        try:
            if datetime.fromisoformat(oldest.replace("Z", "+00:00")) < cutoff:
                break
        except Exception:
            pass
        before = oldest
    res = []
    for f in out:
        ts = _get(f, "createdAt")
        try:
            if ts and datetime.fromisoformat(ts.replace("Z", "+00:00")) >= cutoff:
                res.append(f)
        except Exception:
            res.append(f)
    return res


def fetch_funding(addr, days):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    total, n, by_m = 0.0, 0, defaultdict(float)
    for page in range(1, 50):
        resp = api("/v4/fundingPayments",
                   {"address": addr, "subaccountNumber": 0, "limit": 100, "afterOrAt": cutoff, "page": page})
        pays = resp.get("fundingPayments", []) if isinstance(resp, dict) else []
        if not pays:
            break
        for p in pays:
            amt = _sf(_get(p, "payment", "amount"))
            total += amt; by_m[_get(p, "ticker", "market", default="?")] += amt; n += 1
        if len(pays) < 100:
            break
    return total, n, dict(by_m)


def fetch_marks():
    resp = api("/v4/perpetualMarkets")
    m = resp.get("markets", {}) if isinstance(resp, dict) else {}
    return {k: _sf(v.get("oraclePrice")) for k, v in m.items()}


def fetch_subaccount(addr):
    resp = api(f"/v4/addresses/{addr}/subaccountNumber/0")
    sub = resp.get("subaccount", {}) if isinstance(resp, dict) else {}
    equity = _sf(sub.get("equity")); free = _sf(sub.get("freeCollateral"))
    positions = sub.get("openPerpetualPositions", {}) or sub.get("perpetualPositions", {}) or {}
    inv = {m: _sf(p.get("size")) for m, p in (positions or {}).items() if abs(_sf(p.get("size"))) > 0}
    return equity, free, inv


def fetch_exchange_pnl(addr):
    resp = api("/v4/historical-pnl", {"address": addr, "subaccountNumber": 0, "limit": 1000})
    hist = resp.get("historicalPnl", []) if isinstance(resp, dict) else []
    if not hist:
        return None
    hs = sorted(hist, key=lambda h: _get(h, "createdAt", default=""))
    f, l = hs[0], hs[-1]
    return {
        "first_equity": _sf(_get(f, "equity")), "last_equity": _sf(_get(l, "equity")),
        "last_total_pnl": _sf(_get(l, "totalPnl")), "last_net_transfers": _sf(_get(l, "netTransfers")),
        "first_at": _get(f, "createdAt"), "last_at": _get(l, "createdAt"), "n_points": len(hs),
    }


def read_internal_pnl(log_paths, days):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    total, n, prov = 0.0, 0, 0
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
                total += _sf(j.get("net_pnl_est")); n += 1
                if j.get("pnl_provisional"):
                    prov += 1
    return total, n, prov


def main():
    global BASE
    ap = argparse.ArgumentParser(description="Reconciliación PnL real (fills+funding) vs interno")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--log", action="append", default=None)
    ap.add_argument("--address", default=DEFAULT_ADDRESS, help="Dirección dYdX (default: la de constants.py)")
    ap.add_argument("--indexer", default=DEFAULT_INDEXER, help="URL del indexer")
    args = ap.parse_args()
    BASE = args.indexer.rstrip("/")
    log_paths = args.log or [str(SCRIPT_DIR / "bot_run.log.jsonl"),
                             str(SCRIPT_DIR / "logs" / "bot_run.log.jsonl")]
    addr = (args.address or "").strip()
    if not addr:
        print("ERROR: no pude leer WALLET_ADDRESS de constants.py; pásala con --address dydx1...")
        return

    print(f"\n{'='*70}\n  RECONCILIACIÓN DE PnL — últimos {args.days} días\n  Wallet: {addr}\n{'='*70}")
    print("\nDescargando fills, funding, posiciones y PnL del exchange...")
    fills = fetch_all_fills(addr, args.days)
    funding_total, funding_n, _ = fetch_funding(addr, args.days)
    marks = fetch_marks()
    equity, free, inv = fetch_subaccount(addr)
    exch = fetch_exchange_pnl(addr)

    buy_notional = sell_notional = fees = 0.0
    net_size = defaultdict(float); cash_by_market = defaultdict(float)
    for f in fills:
        side = str(_get(f, "side", default="")).upper()
        size = _sf(_get(f, "size", "filledSize", "amount"))
        price = _sf(_get(f, "price", "fillPrice"))
        fee = _sf(_get(f, "fee", "feeAmount", "feeUsd"))
        mkt = _get(f, "market", "ticker", default="?")
        notional = size * price
        fees += fee
        if side == "BUY":
            buy_notional += notional; net_size[mkt] += size; cash_by_market[mkt] -= notional
        elif side == "SELL":
            sell_notional += notional; net_size[mkt] -= size; cash_by_market[mkt] += notional
        cash_by_market[mkt] -= fee

    mtm = 0.0
    for m, sz in inv.items():
        px = marks.get(m, 0.0); mtm += sz * px; cash_by_market[m] += sz * px

    trading_pnl = sell_notional - buy_notional - fees + mtm
    total_real_pnl = trading_pnl + funding_total
    internal_pnl, internal_n, prov_n = read_internal_pnl(log_paths, args.days)

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
    print(f"  Cierres contabilizados:  {internal_n}  (provisional={prov_n})")
    print(f"  Σ net_pnl_est interno:   ${internal_pnl:,.2f}")

    print(f"\n{'─'*70}\n  DIVERGENCIA interno vs real\n{'─'*70}")
    diff = internal_pnl - total_real_pnl
    denom = abs(total_real_pnl) if abs(total_real_pnl) > 1e-9 else 1.0
    print(f"  interno − real:          ${diff:,.2f}  ({diff/denom*100:+.1f}%)")
    if abs(diff) > max(1.0, 0.20 * denom):
        print(f"  ⚠️  DIVERGENCIA > 20% — la contabilidad interna NO es fiable.")
    else:
        print(f"  ✅ Dentro de tolerancia (20%).")

    if exch:
        print(f"\n{'─'*70}\n  CROSS-CHECK — historical-pnl del exchange (dYdX)\n{'─'*70}")
        print(f"  Ventana:                 {exch['first_at']} → {exch['last_at']} ({exch['n_points']} pts)")
        print(f"  Equity:                  ${exch['first_equity']:,.2f} → ${exch['last_equity']:,.2f}")
        print(f"  ➤ totalPnl (dYdX, autoritativo): ${exch['last_total_pnl']:,.2f}")
        print(f"    (Debe coincidir con TOTAL PnL REAL. NO uses equity−transfers:")
        print(f"     netTransfers no incluye depósitos previos → número inflado.)")

    print(f"\n  Equity actual: ${equity:,.2f} | Free: ${free:,.2f}")

    losers = sorted(cash_by_market.items(), key=lambda kv: kv[1])[:10]
    if losers:
        print(f"\n{'─'*70}\n  TOP 10 mercados por PnL de caja neteado (incl. MtM y fees)\n{'─'*70}")
        for m, v in losers:
            print(f"    {m:<16} ${v:,.2f}   (inv abierto: {net_size.get(m,0):+.4f})")
    print()


if __name__ == "__main__":
    main()
