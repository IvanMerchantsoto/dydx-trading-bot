# fx/coint.py
"""
Matemática de cointegración pura (sin dependencias del bot dYdX ni de .env).
Copia AUDITADA de las funciones de program/func_cointegration.py, para que el
proyecto FX sea totalmente independiente y reutilizable en el futuro bot FX.
"""
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint


def calculate_half_life(spread):
    """Half-life de reversión vía AR(1): Δs = α + β·s_{t-1}. HL = -ln2/β."""
    ts = pd.Series(np.asarray(spread, dtype=float))
    lag = ts.shift(1)
    ret = ts - lag
    valid = lag.notna() & ret.notna()
    y = ret[valid].values
    X = lag[valid].values
    if len(y) < 10:
        return float("nan")
    try:
        res = sm.OLS(y, sm.add_constant(X)).fit()
        beta = res.params[1]
        if beta >= 0:
            return float("nan")
        return float(round(-np.log(2) / beta, 1))
    except Exception:
        return float("nan")


def calculate_hurst_exponent(spread, min_bars=40):
    """Hurst vía R/S. Aplicar sobre DIFERENCIAS del spread. H<0.5 = mean-reverting."""
    ts = np.asarray(spread, dtype=float)
    n = len(ts)
    if n < min_bars:
        return float("nan")
    lag_fracs = [0.05, 0.08, 0.12, 0.18, 0.25, 0.35, 0.45]
    lags = sorted(set(max(4, int(n * f)) for f in lag_fracs))
    lags = [l for l in lags if l <= n // 2]
    if len(lags) < 3:
        return float("nan")
    log_lags, log_rs = [], []
    for lag in lags:
        nch = n // lag
        if nch < 2:
            continue
        rs = []
        for k in range(nch):
            ch = ts[k * lag:(k + 1) * lag]
            m = np.mean(ch); s = np.std(ch)
            if s < 1e-10:
                continue
            dev = np.cumsum(ch - m)
            rs.append((dev.max() - dev.min()) / s)
        if len(rs) >= 2:
            log_lags.append(np.log(lag)); log_rs.append(np.log(np.mean(rs)))
    if len(log_lags) < 3:
        return float("nan")
    return float(np.clip(np.polyfit(log_lags, log_rs, 1)[0], 0.0, 1.0))


def calculate_cointegration(series_1, series_2):
    """
    Engle-Granger + hedge ratio por OLS SIN intercepto (igual que el bot).
    Returns (coint_flag, hedge_ratio, half_life, r_squared, p_value).
    """
    s1 = np.asarray(series_1, dtype=float)
    s2 = np.asarray(series_2, dtype=float)
    if np.std(s1) == 0 or np.std(s2) == 0:
        return 0, 0.0, float("nan"), 0.0, 1.0
    try:
        res = coint(s1, s2)
        ct, p, cv = res[0], res[1], res[2][1]
        model = sm.OLS(s1, s2).fit()
        hr = float(model.params[0])
        r2 = float(model.rsquared)
        spread = s1 - hr * s2
        hl = calculate_half_life(spread)
        flag = 1 if (p < 0.05 and ct < cv) else 0
        return flag, hr, hl, r2, p
    except Exception:
        return 0, 0.0, float("nan"), 0.0, 1.0


def zscore(spread, window):
    """Z-score rolling causal (sin look-ahead), ventana parametrizable."""
    s = pd.Series(np.asarray(spread, dtype=float))
    mean = s.rolling(window=window).mean()
    std = s.rolling(window=window).std()
    return ((s - mean) / std).values
