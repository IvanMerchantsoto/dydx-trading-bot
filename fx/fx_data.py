# fx/fx_data.py
"""
Descarga de precios FX DIARIOS gratis desde Yahoo Finance (sin API key, sin
cuenta). Endpoint chart v8: keyless, años de historia, solo requiere User-Agent.

  https://query1.finance.yahoo.com/v8/finance/chart/EURUSD=X?range=max&interval=1d

Símbolos Yahoo FX = TICKER + "=X" (EURUSD=X, USDJPY=X, EURGBP=X, ...).
Suficiente para VALIDAR cointegración FX. Los datos live vendrán del bróker.
"""
import csv
import io
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

CACHE = Path(__file__).parent / "fx_cache"
CACHE.mkdir(exist_ok=True)
_S = requests.Session()
_UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                     "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"}

DEFAULT_UNIVERSE = [
    "eurusd", "gbpusd", "usdjpy", "usdchf", "audusd", "usdcad", "nzdusd",
    "eurgbp", "eurjpy", "eurchf", "gbpjpy", "audjpy", "audnzd", "eurcad",
]


def fetch_daily(symbol, cache_h=24.0, min_rows=300):
    """
    Devuelve [(date_str, close_float), ...] ascendente, o None si falla.
    Cachea en fx_cache/{symbol}.csv (Date,Close).
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

    ysym = symbol.upper() + "=X"
    rows = None
    last_err = None
    for host in ("query1", "query2"):
        url = (f"https://{host}.finance.yahoo.com/v8/finance/chart/{ysym}"
               f"?range=max&interval=1d")
        for attempt in range(3):
            try:
                r = _S.get(url, headers=_UA, timeout=25)
                if r.status_code in (429, 500, 502, 503):
                    time.sleep(0.8 * (2 ** attempt)); continue
                r.raise_for_status()
                j = r.json()
                res = j["chart"]["result"][0]
                ts = res["timestamp"]
                closes = res["indicators"]["quote"][0]["close"]
                rows = []
                for t, c in zip(ts, closes):
                    if c is None:
                        continue
                    d = datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d")
                    rows.append((d, float(c)))
                break
            except Exception as e:
                last_err = e
                time.sleep(0.5 * (2 ** attempt))
        if rows:
            break
    if not rows:
        print(f"  [fx_data] fetch error {symbol}: {last_err}")
        return None
    # dedup por fecha (Yahoo puede repetir la última) y ordenar
    dd = {}
    for d, c in rows:
        dd[d] = c
    rows = sorted(dd.items(), key=lambda x: x[0])
    if len(rows) < min_rows:
        print(f"  [fx_data] {symbol}: sólo {len(rows)} filas.")
        return rows if rows else None
    try:
        out = io.StringIO()
        w = csv.writer(out); w.writerow(["Date", "Close"])
        w.writerows(rows)
        cf.write_text(out.getvalue())
    except Exception:
        pass
    return rows


def _parse(text):
    out = []
    for row in csv.DictReader(io.StringIO(text)):
        d = row.get("Date")
        c = row.get("Close")
        if not d or c in (None, "", "N/D", "null"):
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
