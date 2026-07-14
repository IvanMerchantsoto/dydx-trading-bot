# fx/fx_data.py
"""
Descarga de precios FX DIARIOS gratis desde stooq (sin API key, sin cuenta).
URL: https://stooq.com/q/d/l/?s=eurusd&i=d  → CSV Date,Open,High,Low,Close,Volume

Suficiente para VALIDAR cointegración FX (años de historia). Los datos live
(tick/candle intradía) vendrán del bróker cuando la validación lo justifique.
"""
import csv
import io
import time
from pathlib import Path

import requests

CACHE = Path(__file__).parent / "fx_cache"
CACHE.mkdir(exist_ok=True)
_S = requests.Session()

# Universo por defecto: majors + crosses líquidos (spreads mínimos, historia larga).
DEFAULT_UNIVERSE = [
    "eurusd", "gbpusd", "usdjpy", "usdchf", "audusd", "usdcad", "nzdusd",
    "eurgbp", "eurjpy", "eurchf", "gbpjpy", "audjpy", "audnzd", "eurcad",
]


def fetch_daily(symbol, cache_h=24.0, min_rows=300):
    """
    Devuelve [(date_str, close_float), ...] ascendente, o None si falla.
    Cachea en fx_cache/{symbol}.csv.
    """
    symbol = symbol.lower().strip()
    cf = CACHE / f"{symbol}.csv"
    if cf.exists() and (time.time() - cf.stat().st_mtime) / 3600.0 < cache_h:
        try:
            rows = _parse(cf.read_text())
            if len(rows) >= min_rows:
                return rows
        except Exception:
            pass
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
    try:
        r = _S.get(url, timeout=25)
        r.raise_for_status()
        text = r.text
    except Exception as e:
        print(f"  [fx_data] fetch error {symbol}: {e}")
        return None
    rows = _parse(text)
    if len(rows) < min_rows:
        print(f"  [fx_data] {symbol}: sólo {len(rows)} filas (¿símbolo inválido o rate-limit?).")
        return rows if rows else None
    try:
        cf.write_text(text)
    except Exception:
        pass
    return rows


def _parse(text):
    out = []
    for row in csv.DictReader(io.StringIO(text)):
        d = row.get("Date")
        c = row.get("Close")
        if not d or c in (None, "", "N/D"):
            continue
        try:
            out.append((d, float(c)))
        except Exception:
            continue
    out.sort(key=lambda x: x[0])
    return out


def align_by_date(rows_1, rows_2):
    """Alinea dos series [(date, close)] por fecha común."""
    import numpy as np
    d1 = dict(rows_1); d2 = dict(rows_2)
    common = sorted(set(d1) & set(d2))
    p1 = np.array([d1[d] for d in common], dtype=float)
    p2 = np.array([d2[d] for d in common], dtype=float)
    return common, p1, p2
