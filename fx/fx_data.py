# fx/fx_data.py
"""
Precios FX DIARIOS gratis desde Frankfurter (tasas de referencia del BCE).
Keyless, pensado para acceso programático → NO bloquea IPs de datacenter
(a diferencia de stooq/Yahoo, que rechazan Google Cloud). Historia desde 1999.

  https://api.frankfurter.app/2007-01-01..2026-07-13?from=USD&to=EUR,GBP,JPY,...

Son tasas de referencia (un fix diario, días hábiles), no bid/ask tradeable —
PERFECTO para VALIDAR si la cointegración existe y persiste. Los precios
tradeables intradía vendrán del bróker en la Fase 2.
"""
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import requests

CACHE = Path(__file__).parent / "fx_cache"
CACHE.mkdir(exist_ok=True)
_S = requests.Session()
FRANK = "https://api.frankfurter.app"

# Divisas que aparecen en el universo (además de USD, que es la base).
CCYS = ["EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"]

DEFAULT_UNIVERSE = [
    "eurusd", "gbpusd", "usdjpy", "usdchf", "audusd", "usdcad", "nzdusd",
    "eurgbp", "eurjpy", "eurchf", "gbpjpy", "audjpy", "audnzd", "eurcad",
]


def _fetch_base_usd(start="2007-01-01", cache_h=24.0):
    """Serie temporal de tasas USD→{CCYS} (una llamada). Cachea el JSON crudo."""
    cf = CACHE / "frankfurter_usd.json"
    if cf.exists() and (time.time() - cf.stat().st_mtime) / 3600.0 < cache_h:
        try:
            j = json.loads(cf.read_text())
            if j.get("rates"):
                return j
        except Exception:
            pass
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    to = ",".join(CCYS)
    urls = [
        f"{FRANK}/{start}..{end}?from=USD&to={to}",
        f"{FRANK}/{start}..{end}?base=USD&symbols={to}",   # forma antigua, por si acaso
    ]
    last = None
    for url in urls:
        for attempt in range(3):
            try:
                r = _S.get(url, timeout=30)
                if r.status_code in (429, 500, 502, 503):
                    time.sleep(0.8 * (2 ** attempt)); continue
                r.raise_for_status()
                j = r.json()
                if j.get("rates"):
                    try:
                        cf.write_text(json.dumps(j))
                    except Exception:
                        pass
                    return j
            except Exception as e:
                last = e
                time.sleep(0.5 * (2 ** attempt))
    print(f"  [fx_data] Frankfurter falló: {last}")
    return None


def load_universe(symbols):
    """
    Devuelve {sym: [(date, close), ...]} derivando cada par de las tasas USD.
    price(base/quote) = (USD→quote) / (USD→base), con USD→USD = 1.
    """
    j = _fetch_base_usd()
    if not j:
        return {}
    rates = j.get("rates", {})   # {date: {CUR: val}}
    out = {}
    for sym in symbols:
        s = sym.lower().strip()
        base, quote = s[:3].upper(), s[3:].upper()
        rows = []
        for date in sorted(rates):
            r = rates[date]
            bv = 1.0 if base == "USD" else r.get(base)
            qv = 1.0 if quote == "USD" else r.get(quote)
            if bv and qv and bv > 0:
                rows.append((date, qv / bv))
        if len(rows) >= 300:
            out[s] = rows
        else:
            print(f"  [fx_data] {s}: sólo {len(rows)} días (divisa ausente en BCE?)")
    return out


def align_by_date(rows_1, rows_2):
    """Alinea dos series [(date, close)] por fecha común."""
    d1 = dict(rows_1); d2 = dict(rows_2)
    common = sorted(set(d1) & set(d2))
    p1 = np.array([d1[d] for d in common], dtype=float)
    p2 = np.array([d2[d] for d in common], dtype=float)
    return common, p1, p2
