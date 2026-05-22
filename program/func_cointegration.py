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
from constants import MAX_HALF_LIFE, WINDOW, HEDGE_RATIO_LOG_MAX, HURST_MAX, HURST_MIN_BARS

# Absolute path for the cointegrated pairs CSV — avoids fragile relative paths.
CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cointegrated_pairs.csv")


def calculate_hurst_exponent(spread) -> float:
    """
    Calcula el exponente de Hurst del spread vía análisis R/S (Rescaled Range).

    H < 0.5 → anti-persistente / mean-reverting  ← queremos esto para stat arb
    H ≈ 0.5 → random walk (sin memoria)
    H > 0.5 → persistente / trending (divergirá más)

    Retorna float en [0, 1], o np.nan si no hay suficientes datos.
    """
    ts = np.array(spread, dtype=float)
    n = len(ts)
    if n < HURST_MIN_BARS:
        return float('nan')

    # Usar fracciones del total para los lags de análisis
    lag_fractions = [0.05, 0.08, 0.12, 0.18, 0.25, 0.35, 0.45]
    lags = sorted(set(max(4, int(n * f)) for f in lag_fractions))
    lags = [l for l in lags if l <= n // 2]

    if len(lags) < 3:
        return float('nan')

    log_lags = []
    log_rs   = []

    for lag in lags:
        # Dividir en bloques no solapados de tamaño `lag`
        n_chunks = n // lag
        if n_chunks < 2:
            continue
        rs_chunk = []
        for k in range(n_chunks):
            chunk = ts[k * lag : (k + 1) * lag]
            mean_c = np.mean(chunk)
            std_c  = np.std(chunk)
            if std_c < 1e-10:
                continue
            devs = np.cumsum(chunk - mean_c)
            R    = devs.max() - devs.min()
            rs_chunk.append(R / std_c)
        if len(rs_chunk) >= 2:
            log_lags.append(np.log(lag))
            log_rs.append(np.log(np.mean(rs_chunk)))

    if len(log_lags) < 3:
        return float('nan')

    # H es la pendiente del log-log fit: log(R/S) = H·log(lag) + C
    poly = np.polyfit(log_lags, log_rs, 1)
    return float(np.clip(poly[0], 0.0, 1.0))


def calculate_half_life(spread):
    """
    Estima la half-life de mean reversion del spread via OLS AR(1).

    Modelo: Δspread_t = α + β·spread_{t-1} + ε
    Half-life = -ln(2) / β   (β debe ser negativo para mean-reversion)

    Retorna float (horas si spread es de barras horarias), o np.nan si:
    - β ≥ 0 (spread no mean-reverts)
    - Ajuste inválido (NaN / division by zero)

    NOTA: La versión anterior rellenaba spread_lag.iloc[0] con spread_lag.iloc[1]
    (para evitar NaN), lo que introducía un punto duplicado y sesgaba β hacia
    valores más negativos → subestimaba la half-life y aceptaba pares lentos.
    Ahora se usa dropna() correctamente.
    """
    ts = pd.Series(np.array(spread, dtype=float))
    spread_lag = ts.shift(1)
    spread_ret = ts - spread_lag

    # Descartar el primer par (NaN por el shift) sin introducir datos falsos
    valid = spread_lag.notna() & spread_ret.notna()
    y = spread_ret[valid].values
    X = spread_lag[valid].values

    if len(y) < 10:
        return float('nan')

    try:
        X_const = sm.add_constant(X)
        res = sm.OLS(y, X_const).fit()
        beta = res.params[1]
        if beta >= 0:
            # Spread no mean-reverts (β ≥ 0 → explosivo o random walk)
            return float('nan')
        halflife = round(-np.log(2) / beta, 0)
        return halflife
    except Exception:
        return float('nan')

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
        r_squared = float(model.rsquared)

        spread = series_1 - (hedge_ratio * series_2)
        half_life = calculate_half_life(spread)
        t_check = coint_t < critical_value
        coint_flag = 1 if p_value < 0.05 and t_check else 0
        return coint_flag, hedge_ratio, half_life, r_squared, p_value
    except Exception:
        return 0, 0, 0, 0.0, 1.0

# Store cointegration results
def store_cointegration_results(df_market_prices):

    # Initialize
    markets = df_market_prices.columns.to_list()
    criteria_met_pairs = []

    # Diagnostic counters
    n_tested = 0
    n_coint_pass = 0       # passed p_value and t_check
    n_hl_negative = 0      # half_life <= 0 (explosive spread) or nan
    n_hl_too_short = 0     # half_life <= 3h (too fast, likely noise)
    n_hl_too_long = 0      # half_life > MAX_HALF_LIFE
    n_hedge_filtered = 0   # hedge ratio outside [10^-LOG_MAX, 10^LOG_MAX]
    n_hurst_filtered = 0   # Hurst exponent >= HURST_MAX (spread is trending)
    half_lives_seen = []   # collect all valid half-lives for distribution
    hurst_values_seen = [] # for distribution diagnostic
    r_squared_seen = []    # R² distribution of hedge ratio OLS fits

    # Find cointegrated pairs
    for index, base_market in enumerate(markets[:-1]):
        series_1 = df_market_prices[base_market].values.astype(float).tolist()

        for quote_market in markets[index+1:]:
            series_2 = df_market_prices[quote_market].values.astype(float).tolist()

            try:
                n_tested += 1
                coint_flag, hedge_ratio, half_life, r_sq, p_val = calculate_cointegration(series_1, series_2)

                if coint_flag == 1:
                    n_coint_pass += 1
                    if not np.isnan(r_sq):
                        r_squared_seen.append(r_sq)

                    # ── Filter 1: Half-life ─────────────────────────────────
                    # calculate_half_life now returns nan when β≥0 (non-reverting)
                    if np.isnan(half_life) or half_life <= 0:
                        n_hl_negative += 1
                        continue
                    elif half_life <= 3:
                        n_hl_too_short += 1
                        continue
                    elif half_life > MAX_HALF_LIFE:
                        n_hl_too_long += 1
                        continue

                    # ── Filter 2: Hedge ratio sanity ────────────────────────
                    # Ratios extremos (ej: BTC/SHIB = 12.8B) indican que los dos
                    # activos tienen precios en unidades muy diferentes → el
                    # z-score no tiene significado económico real y el sizing
                    # resultante es impracticable.
                    if hedge_ratio <= 0 or abs(np.log10(abs(hedge_ratio))) > HEDGE_RATIO_LOG_MAX:
                        n_hedge_filtered += 1
                        continue

                    # ── Filter 3: Hurst exponent (mean-reversion check) ─────
                    # Aplicar sobre DIFERENCIAS del spread, no el nivel.
                    # Spreads cointegrados son AR(1) con phi≈1; en nivel todos
                    # parecen trending (H>0.8). Al diferenciar:
                    #   half_life=4h  → H_diff≈0.265  (fuerte mean-reversion)
                    #   half_life=24h → H_diff≈0.488  (borderline)
                    #   random walk   → H_diff≈0.579  (rechazado si ≥ HURST_MAX=0.52)
                    s1_arr = np.array(series_1, dtype=float)
                    s2_arr = np.array(series_2, dtype=float)
                    spread_arr = s1_arr - (hedge_ratio * s2_arr)
                    hurst = calculate_hurst_exponent(np.diff(spread_arr))

                    if not np.isnan(hurst):
                        hurst_values_seen.append(hurst)
                        if hurst >= HURST_MAX:
                            n_hurst_filtered += 1
                            continue

                    # ── Passes all filters ──────────────────────────────────
                    half_lives_seen.append(half_life)
                    criteria_met_pairs.append({
                        "base_market": base_market,
                        "quote_market": quote_market,
                        "hedge_ratio": hedge_ratio,
                        "half_life": half_life,
                        "hurst": round(hurst, 3) if not np.isnan(hurst) else None,
                        "r_squared": round(r_sq, 4) if not np.isnan(r_sq) else None,
                    })

            except Exception as e:
                print(f"Error calculating cointegration results: {e}")
                continue

    # ── Diagnostic summary ────────────────────────────────────────────────────
    n_pairs_found = len(criteria_met_pairs)
    print(f"\n[COINT DIAGNOSTICS]")
    print(f"  Pairs tested:              {n_tested}")
    print(f"  Passed coint test:         {n_coint_pass}")
    print(f"  → HL negative/explosive:   {n_hl_negative}")
    print(f"  → HL ≤ 3h (noise):         {n_hl_too_short}")
    print(f"  → HL > {MAX_HALF_LIFE}h (slow):      {n_hl_too_long}")
    print(f"  → Hedge ratio extreme:     {n_hedge_filtered}  (|log10(hr)| > {HEDGE_RATIO_LOG_MAX})")
    print(f"  → Hurst ≥ {HURST_MAX} (trending): {n_hurst_filtered}")
    print(f"  → Passed ALL filters ✓:    {n_pairs_found}")
    if half_lives_seen:
        arr = np.array(half_lives_seen)
        arr_pos = arr[arr > 0]
        if len(arr_pos):
            print(f"  HL distribution (final pairs):")
            print(f"    min={arr_pos.min():.0f}h  p25={np.percentile(arr_pos,25):.0f}h  "
                  f"median={np.median(arr_pos):.0f}h  p75={np.percentile(arr_pos,75):.0f}h  "
                  f"max={arr_pos.max():.0f}h")
    if hurst_values_seen:
        ha = np.array(hurst_values_seen)
        print(f"  Hurst distribution (coint-passing spreads):")
        print(f"    min={ha.min():.3f}  p25={np.percentile(ha,25):.3f}  "
              f"median={np.median(ha):.3f}  p75={np.percentile(ha,75):.3f}  "
              f"max={ha.max():.3f}")
    if r_squared_seen:
        ra = np.array(r_squared_seen)
        print(f"  R² distribution (hedge ratio OLS fit quality, coint-passing):")
        print(f"    min={ra.min():.3f}  p25={np.percentile(ra,25):.3f}  "
              f"median={np.median(ra):.3f}  p75={np.percentile(ra,75):.3f}  "
              f"max={ra.max():.3f}")
        low_r2 = (ra < 0.80).sum()
        if low_r2 > 0:
            print(f"    ⚠️  {low_r2} pares con R²<0.80 (hedge ratio poco confiable)")
    print()

    # ── Create and save DataFrame ─────────────────────────────────────────────
    # Sorted by half_life ascending: fastest mean-reverting pairs tried first.
    df_criteria_met = pd.DataFrame(criteria_met_pairs)
    if not df_criteria_met.empty:
        df_criteria_met.sort_values("half_life", ascending=True, inplace=True)
        df_criteria_met.reset_index(drop=True, inplace=True)
        # Ensure column order (hurst optional — old CSVs won't have it)
        cols = ["base_market", "quote_market", "hedge_ratio", "half_life", "hurst", "r_squared"]
        cols = [c for c in cols if c in df_criteria_met.columns]
        df_criteria_met = df_criteria_met[cols]
    df_criteria_met.to_csv(CSV_PATH, index=True)
    del df_criteria_met

    print(f"Cointegrated pairs successfully saved to {CSV_PATH}.")
    print(f"Total usable pairs: {n_pairs_found}")
    return "saved"






