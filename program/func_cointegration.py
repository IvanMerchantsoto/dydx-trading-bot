import os
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')   # silencia divide-by-zero, etc.
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint
# Suppress CollinearityWarning — near-collinear pairs fail coint_flag anyway;
# the warning is noise in production logs.
try:
    from statsmodels.tools.sm_exceptions import CollinearityWarning
    warnings.filterwarnings('ignore', category=CollinearityWarning)
except ImportError:
    pass
from constants import MAX_HALF_LIFE, WINDOW

# Absolute path for the cointegrated pairs CSV — avoids fragile relative paths.
CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cointegrated_pairs.csv")


def calculate_half_life(spread):
    df_spread = pd.DataFrame(spread, columns=["spread"])
    spread_lag = df_spread.spread.shift(1)
    spread_lag.iloc[0]=spread_lag.iloc[1]
    spread_ret=df_spread.spread - spread_lag
    spread_ret.iloc[0] = spread_ret.iloc[1]
    spread_lag2 = sm.add_constant(spread_lag)
    model = sm.OLS(spread_ret, spread_lag2)
    res = model.fit()
    halflife = round(-np.log(2)/res.params.iloc[1],0)
    return halflife

# Calculate ZScore
def calculate_zscore(spread):
    spread_series = pd.Series(spread)
    mean = spread_series.rolling(center=False, window=WINDOW).mean()
    std = spread_series.rolling(center=False, window=WINDOW).std()
    x = spread_series.rolling(center=False, window=1).mean()
    szcore = (x-mean)/std
    return szcore

# Calculate Cointegration
def calculate_cointegration(series_1, series_2):
    series_1 = np.array(series_1).astype(np.float64)
    series_2 = np.array(series_2).astype(np.float64)

    if np.std(series_1) == 0 or np.std(series_2) == 0:
        return 0, 0, 0

    try:
        coint_flag = 0
        coint_res = coint(series_1, series_2)
        coint_t = coint_res[0]
        p_value = coint_res[1]
        critical_value = coint_res[2][1]

        # Hedge ratio via OLS without intercept: hr ≈ mean(s1)/mean(s2).
        # This is intentional for z-score pairs trading — the spread
        # s1 - hr*s2 oscillates near zero, producing meaningful half-lives
        # and z-scores. Adding an intercept shifts the β to a "true" OLS
        # slope but produces spreads with longer dynamics that break the
        # half-life filter and don't improve signal quality for this strategy.
        model = sm.OLS(series_1, series_2).fit()
        hedge_ratio = model.params[0]

        spread = series_1 - (hedge_ratio * series_2)
        half_life = calculate_half_life(spread)
        t_check = coint_t < critical_value
        coint_flag = 1 if p_value < 0.05 and t_check else 0
        return coint_flag, hedge_ratio, half_life
    except Exception:
        return 0, 0, 0

# Store cointegration results
def store_cointegration_results(df_market_prices):

    # Initialize
    markets = df_market_prices.columns.to_list()
    criteria_met_pairs = []

    # Diagnostic counters
    n_tested = 0
    n_coint_pass = 0       # passed p_value and t_check
    n_hl_negative = 0      # half_life <= 0 (explosive spread)
    n_hl_too_short = 0     # half_life <= 3h (too fast, likely noise)
    n_hl_too_long = 0      # half_life > MAX_HALF_LIFE
    half_lives_seen = []   # collect all valid half-lives for distribution

    # Find cointegrated pairs
    for index, base_market in enumerate(markets[:-1]):
        series_1 = df_market_prices[base_market].values.astype(float).tolist()

        for quote_market in markets[index+1:]:
            series_2 = df_market_prices[quote_market].values.astype(float).tolist()

            try:
                n_tested += 1
                coint_flag, hedge_ratio, half_life = calculate_cointegration(series_1, series_2)

                if coint_flag == 1:
                    n_coint_pass += 1
                    if half_life <= 0:
                        n_hl_negative += 1
                    elif half_life <= 3:
                        n_hl_too_short += 1
                    elif half_life > MAX_HALF_LIFE:
                        n_hl_too_long += 1
                        half_lives_seen.append(half_life)
                    else:
                        # Passes all filters
                        half_lives_seen.append(half_life)
                        criteria_met_pairs.append({
                            "base_market": base_market,
                            "quote_market": quote_market,
                            "hedge_ratio": hedge_ratio,
                            "half_life": half_life,
                        })
            except Exception as e:
                print(f"Error calculating cointegration results: {e}")
                continue

    # ── Diagnostic summary ────────────────────────────────────────────────────
    n_pairs_found = len(criteria_met_pairs)
    print(f"\n[COINT DIAGNOSTICS]")
    print(f"  Pairs tested:          {n_tested}")
    print(f"  Passed coint test:     {n_coint_pass}")
    print(f"  → HL negative/explsv:  {n_hl_negative}")
    print(f"  → HL ≤ 3h (noise):     {n_hl_too_short}")
    print(f"  → HL > {MAX_HALF_LIFE}h (slow):  {n_hl_too_long}")
    print(f"  → HL in (3,{MAX_HALF_LIFE}] ✓:  {n_pairs_found}")
    if half_lives_seen:
        arr = np.array(half_lives_seen)
        arr_pos = arr[arr > 0]
        if len(arr_pos):
            print(f"  HL distribution (coint-passing pairs, incl filtered):")
            print(f"    min={arr_pos.min():.0f}h  p25={np.percentile(arr_pos,25):.0f}h  "
                  f"median={np.median(arr_pos):.0f}h  p75={np.percentile(arr_pos,75):.0f}h  "
                  f"max={arr_pos.max():.0f}h")
    print()

    # ── Create and save DataFrame ─────────────────────────────────────────────
    # Sorted by half_life ascending: fastest mean-reverting pairs tried first.
    df_criteria_met = pd.DataFrame(criteria_met_pairs)
    if not df_criteria_met.empty:
        df_criteria_met.sort_values("half_life", ascending=True, inplace=True)
        df_criteria_met.reset_index(drop=True, inplace=True)
    df_criteria_met.to_csv(CSV_PATH, index=True)
    del df_criteria_met

    print(f"Cointegrated pairs successfully saved to {CSV_PATH}.")
    print(f"Total usable pairs: {n_pairs_found}")
    return "saved"






