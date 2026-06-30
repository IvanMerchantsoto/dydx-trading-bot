"""
diagnose_coint.py
=================
Diagnóstico rápido del proceso de cointegración SIN conectar a dYdX.
Usa el df_market_prices guardado en un archivo pickle (si existe),
o lo reconstruye leyendo el CSV actual de precios históricos.

Si el CSV está vacío, reabre desde la perspectiva de los filtros:
  ¿cuántos pares pasan el test de cointegración?
  ¿cuántos se caen por half_life?
  ¿con qué MAX_HALF_LIFE habría pares suficientes?

Uso:
    cd program/
    python diagnose_coint.py

    # O para ver cómo quedó el CSV actual:
    python diagnose_coint.py --csv-only
"""

import os
import sys
import argparse
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# ── bootstrap constants ───────────────────────────────────────────────────────
from decouple import config  # noqa — needed by constants

CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cointegrated_pairs.csv")
PRICES_PICKLE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "market_prices.pkl")


def inspect_csv():
    """Quick check of the saved CSV."""
    import pandas as pd

    if not os.path.exists(CSV_PATH):
        print("❌ cointegrated_pairs.csv NOT FOUND")
        return

    df = pd.read_csv(CSV_PATH)
    print(f"\n{'='*55}")
    print(f"  cointegrated_pairs.csv")
    print(f"  Rows: {len(df)}")
    print(f"{'='*55}")

    if len(df) == 0:
        print("⚠️  CSV IS EMPTY — no pairs passed all filters.")
        return

    cols = [c for c in ['base_market','quote_market','hedge_ratio','half_life'] if c in df.columns]
    hr = df['hedge_ratio'].abs() if 'hedge_ratio' in df.columns else None
    hl = df['half_life'] if 'half_life' in df.columns else None

    if hr is not None:
        print(f"\nHedge ratio |hr| distribution:")
        print(f"  > 100      : {(hr > 100).sum():4d}  ← should be ~0 with OLS fix")
        print(f"  10 – 100   : {((hr > 10) & (hr <= 100)).sum():4d}")
        print(f"  1 – 10     : {((hr >= 1) & (hr <= 10)).sum():4d}")
        print(f"  0.1 – 1    : {((hr >= 0.1) & (hr < 1)).sum():4d}")
        print(f"  < 0.1      : {(hr < 0.1).sum():4d}")

    if hl is not None:
        print(f"\nHalf-life distribution (hours):")
        print(f"  min={hl.min():.0f}h  p25={hl.quantile(0.25):.0f}h  "
              f"median={hl.median():.0f}h  p75={hl.quantile(0.75):.0f}h  max={hl.max():.0f}h")

    print(f"\nTop 15 pairs (fastest mean-reversion):")
    print(df[cols].head(15).to_string(index=False))


def simulate_filters(df_prices, max_hl_values=(24, 48, 72, 120)):
    """
    Run the cointegration scan on market prices data and count how many pairs
    pass at different MAX_HALF_LIFE thresholds.
    Helps answer: "if I set MAX_HALF_LIFE=48, would I get pairs?"
    """
    import statsmodels.api as sm
    from statsmodels.tsa.stattools import coint
    try:
        from statsmodels.tools.sm_exceptions import CollinearityWarning
        warnings.filterwarnings('ignore', category=CollinearityWarning)
    except ImportError:
        pass

    from constants import WINDOW

    def calc_half_life(spread):
        import pandas as pd
        df_s = pd.DataFrame(spread, columns=["spread"])
        lag = df_s.spread.shift(1)
        lag.iloc[0] = lag.iloc[1]
        ret = df_s.spread - lag
        ret.iloc[0] = ret.iloc[1]
        res = sm.OLS(ret, sm.add_constant(lag)).fit()
        return round(-np.log(2) / res.params.iloc[1], 0)

    markets = df_prices.columns.to_list()
    n_tested = 0
    n_coint = 0
    results = []  # (m1, m2, hr, hl)

    total_pairs = len(markets) * (len(markets) - 1) // 2
    print(f"\nScanning {len(markets)} markets → {total_pairs} pairs...")
    print("(this may take a few minutes)\n")

    for i, m1 in enumerate(markets[:-1]):
        s1 = np.array(df_prices[m1].values, dtype=np.float64)
        for m2 in markets[i+1:]:
            s2 = np.array(df_prices[m2].values, dtype=np.float64)

            if np.std(s1) == 0 or np.std(s2) == 0:
                continue

            n_tested += 1
            try:
                cr = coint(s1, s2)
                p_val = cr[1]
                t_stat = cr[0]
                crit = cr[2][1]

                if p_val < 0.05 and t_stat < crit:
                    n_coint += 1
                    model = sm.OLS(s1, sm.add_constant(s2)).fit()
                    hr = float(model.params.iloc[1])
                    spread = s1 - hr * s2
                    hl = calc_half_life(spread)
                    results.append((m1, m2, hr, hl))
            except Exception:
                continue

        # Progress every 10 markets
        if (i + 1) % 10 == 0:
            pct = (i + 1) / len(markets) * 100
            print(f"  Progress: {i+1}/{len(markets)} markets ({pct:.0f}%) — "
                  f"coint so far: {n_coint}", flush=True)

    print(f"\n{'='*55}")
    print(f"  FILTER SIMULATION RESULTS")
    print(f"{'='*55}")
    print(f"  Total pairs tested:  {n_tested}")
    print(f"  Passed coint test:   {n_coint}")

    if n_coint == 0:
        print("\n  ⚠️  No cointegrated pairs found at all.")
        print("  Check: is there sufficient price variation in the data?")
        print("         Is the data window long enough?")
        return

    hls = np.array([r[3] for r in results])
    hls_pos = hls[hls > 0]
    print(f"\n  Half-life distribution (coint-passing pairs):")
    if len(hls_pos):
        print(f"    negative/explosive: {(hls <= 0).sum()}")
        print(f"    0 – 3h (noise):     {((hls > 0) & (hls <= 3)).sum()}")
        for lo, hi in [(3, 24), (3, 48), (3, 72), (3, 120), (3, 999)]:
            cnt = ((hls > lo) & (hls <= hi)).sum()
            label = f"MAX_HL={hi}h" if hi < 999 else "any hl"
            print(f"    hl ({lo},{hi}] ({label}): {cnt} pairs")
        print(f"    min={hls_pos.min():.0f}h  p25={np.percentile(hls_pos,25):.0f}h  "
              f"median={np.median(hls_pos):.0f}h  p75={np.percentile(hls_pos,75):.0f}h  "
              f"max={hls_pos.max():.0f}h")

    print(f"\n  Top 10 pairs by half_life (fastest mean-reversion):")
    valid = sorted([r for r in results if 0 < r[3]], key=lambda x: x[3])
    for m1, m2, hr, hl in valid[:10]:
        print(f"    {m1:15s} / {m2:15s}  hr={hr:8.4f}  hl={hl:.0f}h")

    print(f"\n  RECOMMENDATION:")
    for max_hl in (24, 48, 72):
        cnt = ((hls > 3) & (hls <= max_hl)).sum()
        if cnt > 0:
            print(f"    Set MAX_HALF_LIFE = {max_hl} → {cnt} usable pairs")
            break
    else:
        print(f"    All half-lives > 72h. Consider extending WINDOW or checking data quality.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-only', action='store_true',
                        help='Only inspect the existing CSV, no new scan')
    args = parser.parse_args()

    inspect_csv()

    if args.csv_only:
        return

    # Try to load saved prices pickle first (fast)
    if os.path.exists(PRICES_PICKLE):
        import pandas as pd
        print(f"\nLoading market prices from {PRICES_PICKLE}...")
        df_prices = pd.read_pickle(PRICES_PICKLE)
        print(f"Loaded {len(df_prices.columns)} markets × {len(df_prices)} candles")
        simulate_filters(df_prices)
    else:
        print(f"\n⚠️  No prices pickle found at {PRICES_PICKLE}")
        print("To run the full filter simulation, save market prices first:")
        print("  In main.py, after construct_market_prices(), add:")
        print("    df_market_prices.to_pickle('market_prices.pkl')")
        print("  Then rerun the bot once and run this script.")
        print("\nFor now, run with --csv-only to inspect the current CSV:")
        print("  python diagnose_coint.py --csv-only")


if __name__ == "__main__":
    main()
