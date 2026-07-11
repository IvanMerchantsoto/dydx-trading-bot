from func_messaging import send_message
from constants import (
    ZSCORE_THRESH, USD_PER_TRADE, USD_MIN_COLLATERAL, MAX_OPEN_TRADES, WINDOW,
    TAKER_FEE_BPS,
    FUNDING_GATE_ENABLED, FUNDING_MAX_COST_RATIO,
    OPPORTUNITY_SCORING,
    MIN_EDGE_FEE_MULTIPLE,
    MARKET_BLACKLIST,
    HEDGE_RATIO_LOG_MAX,
)
from func_utils import format_number
from func_public import get_candles_recent, get_market_spread_bps, get_funding_rates
from func_cointegration import calculate_zscore, CSV_PATH as COINT_CSV_PATH
from datetime import datetime, timezone
from constants import WALLET_ADDRESS
from func_bot_agent import BotAgent
from func_logging import log_event

import pandas as pd
import json
import os
import math
import uuid
import numpy as np

JSON_PATH = os.path.join(os.path.dirname(__file__), "bot_agents.json")

# ── Pair-level failure cooldown ───────────────────────────────────────────────
# Si un par falla PAIR_FAIL_COOLDOWN_THRESHOLD veces seguidas (fills=0 o partial
# en ambas legs), se marca en cooldown por PAIR_FAIL_COOLDOWN_HOURS horas.
# Esto evita spamear el mismo par ilíquido en cada ciclo.
PAIR_FAIL_COOLDOWN_PATH = os.path.join(os.path.dirname(__file__), "pair_fail_cooldowns.json")
PAIR_FAIL_COOLDOWN_THRESHOLD = 3    # fallos consecutivos antes de cooldown
PAIR_FAIL_COOLDOWN_HOURS = 4.0      # horas de cooldown tras threshold
PAIR_FAIL_RESET_ON_SUCCESS = True   # resetear contador si hay LIVE

# ── Market concentration limit ────────────────────────────────────────────────
# Máximo de pares abiertos que pueden compartir el mismo mercado.
# Evita que ARKM-USD (presente en 9+ pares del CSV) bloquee todas las señales
# en cuanto 1 par con ARKM está abierto.
#
# 2026-05-26: subido de 1 → 2. Análisis del log mostró que MAX=1 combinado con
# 13 pares manuales del usuario filtraba 47 de 144 pares del CSV (33%).
# MAX=2 permite mejor utilización del universe pero introduce correlación:
# si SOL-USD aparece en 2 pares activos, un movimiento adverso en SOL pega
# en ambos a la vez. Riesgo monitoreado via HARD_SL_USD por par.
MAX_TRADES_PER_MARKET = 2

# ── Runtime NaN cache (2026-06-02) ────────────────────────────────────────────
# Pairs cuyo z-score devolvió NaN se acumulan aquí. El pre-filter del CSV los
# salta en scans subsecuentes. Persiste durante la vida del proceso; se limpia
# en cada regeneración del CSV (FIND_COINTEGRATED=True) o restart del bot.
# Causa típica: market recién listado sin candles suficientes para regresión.
# Key format: "BASE-USD|QUOTE-USD" (mismo orden que el CSV).
_RUNTIME_NAN_PAIRS: set = set()


def runtime_nan_pairs_stats():
    """Diagnostic helper."""
    return {"size": len(_RUNTIME_NAN_PAIRS), "sample": list(_RUNTIME_NAN_PAIRS)[:10]}


def runtime_nan_pairs_clear():
    """Call this from main loop after CSV regeneration."""
    _RUNTIME_NAN_PAIRS.clear()


def _pair_key(m1, m2):
    return "/".join(sorted([str(m1), str(m2)]))


def _load_pair_fails():
    try:
        with open(PAIR_FAIL_COOLDOWN_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_pair_fails(data):
    tmp = PAIR_FAIL_COOLDOWN_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, PAIR_FAIL_COOLDOWN_PATH)


def _is_pair_in_fail_cooldown(m1, m2):
    data = _load_pair_fails()
    key = _pair_key(m1, m2)
    rec = data.get(key)
    if not rec:
        return False

    now_utc = datetime.now(timezone.utc)

    # ── Check post-SL cooldown (set by func_exit_pairs._write_sl_cooldown) ──
    sl_until_str = rec.get("sl_cooldown_until", "")
    if sl_until_str:
        try:
            sl_until = datetime.fromisoformat(sl_until_str.replace("Z", "+00:00"))
            if now_utc < sl_until:
                remaining_h = (sl_until - now_utc).total_seconds() / 3600.0
                log_event({
                    "type": "signal_skip",
                    "reason": "sl_cooldown",
                    "pair": key,
                    "remaining_hours": round(remaining_h, 2),
                    "until": sl_until_str,
                })
                return True
            else:
                # Expired — clear it
                rec.pop("sl_cooldown_until", None)
                rec.pop("sl_cooldown_hours", None)
                rec.pop("sl_ts", None)
                data[key] = rec
                _save_pair_fails(data)
        except Exception:
            pass

    # ── Check execution-failure cooldown (consecutive fails) ──────────────
    count = rec.get("consecutive_fails", 0)
    if count < PAIR_FAIL_COOLDOWN_THRESHOLD:
        return False
    ts_str = rec.get("last_fail_ts", "")
    try:
        last_fail = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        age_h = (now_utc - last_fail).total_seconds() / 3600.0
        if age_h >= PAIR_FAIL_COOLDOWN_HOURS:
            # Cooldown expired — reset counter
            data[key] = {"consecutive_fails": 0, "last_fail_ts": ts_str}
            _save_pair_fails(data)
            return False
        return True
    except Exception:
        return False


def _record_pair_fail(m1, m2):
    data = _load_pair_fails()
    key = _pair_key(m1, m2)
    rec = data.get(key, {"consecutive_fails": 0})
    rec["consecutive_fails"] = int(rec.get("consecutive_fails", 0)) + 1
    rec["last_fail_ts"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    data[key] = rec
    _save_pair_fails(data)
    return rec["consecutive_fails"]


def _record_pair_success(m1, m2):
    if not PAIR_FAIL_RESET_ON_SUCCESS:
        return
    data = _load_pair_fails()
    key = _pair_key(m1, m2)
    if key in data:
        data[key] = {"consecutive_fails": 0, "last_fail_ts": data[key].get("last_fail_ts", "")}
        _save_pair_fails(data)


def _sf(x, d=0.0):
    """Safe float conversion."""
    try:
        return float(x)
    except Exception:
        return d


async def _get_subaccount(indexer):
    resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
    return resp.get("subaccount", {}) or {}


async def _get_live_markets_with_position(indexer):
    """Return a set of markets that currently have a non-zero open position."""
    sub = await _get_subaccount(indexer)
    positions = sub.get("openPerpetualPositions", {}) or sub.get("perpetualPositions", {}) or {}
    live = set()
    for m, p in positions.items():
        if abs(_sf(p.get("size"))) > 0:
            live.add(m)
    return live


def _load_open_pairs_from_json():
    try:
        with open(JSON_PATH, "r") as f:
            data = json.load(f)
        return data if data else []
    except Exception:
        return []


def _count_open_pairs_from_json():
    return len(_load_open_pairs_from_json())


async def open_positions(
    node,
    indexer,
    wallet,
    max_new_trades=1,
    usd_per_trade=None,
    max_open_trades_override=None,
):
    """
    Entry manager — two-phase approach:

    PHASE 1: Collect and score ALL candidates that pass basic filters.
             Score = abs(z) × (1/half_life) × (100/spread_bps) — higher is better.
             Candidates are sorted by score when OPPORTUNITY_SCORING=True.

    PHASE 2: Open the top N candidates, applying funding gate and min-edge gate.

    Parameters
    ----------
    max_new_trades : int
        Maximum new pairs to open this batch.
    usd_per_trade : float or None
        Override USD_PER_TRADE (used for dynamic sizing from main.py).
    max_open_trades_override : int or None
        Override MAX_OPEN_TRADES (used for dynamic sizing from main.py).
    """
    import time as _time
    _scan_t0 = _time.time()
    _phase_t = {"setup": 0.0, "csv_load": 0.0, "markets": 0.0, "funding": 0.0,
                "prefilter": 0.0, "phase1": 0.0, "phase2": 0.0}
    _phase_last = _scan_t0

    def _mark(phase):
        nonlocal _phase_last
        _now = _time.time()
        _phase_t[phase] = _phase_t.get(phase, 0.0) + (_now - _phase_last)
        _phase_last = _now

    opened_count = 0

    # Effective parameters (may be overridden by dynamic sizing from main.py)
    eff_usd = float(usd_per_trade) if usd_per_trade is not None else float(USD_PER_TRADE)
    eff_max = int(max_open_trades_override) if max_open_trades_override is not None else int(MAX_OPEN_TRADES)

    # Round-trip fee for one pair at this trade size (open+close both legs at taker)
    fee_round_trip = 4.0 * eff_usd * TAKER_FEE_BPS

    # ── 1. Hard cap from JSON ─────────────────────────────────────────────
    open_pairs = _count_open_pairs_from_json()
    if open_pairs >= eff_max:
        print(f"[ENTRY] MAX_OPEN_TRADES reached ({open_pairs}/{eff_max}).")
        return 0

    # ── 2. Optional collateral gate (best-effort) ─────────────────────────
    try:
        sub = await _get_subaccount(indexer)
        free_collateral = _sf(sub.get("freeCollateral"))
        # Min collateral scales with trade size: at least 3 trade legs' worth
        # 2026-05-26: bajado de 3.0 a 1.5. Con $30/leg, eff_usd × 3 = $90 (casi todo
        # equity), bloqueando trades. eff_usd × 1.5 = $45 (50% del equity), permite
        # operar manteniendo buffer de margen.
        min_coll = max(float(USD_MIN_COLLATERAL), eff_usd * 1.5)
        if free_collateral and free_collateral < min_coll:
            print(f"[ENTRY] Free collateral ${free_collateral:,.2f} < min ${min_coll:,.2f}.")
            log_event({
                "type": "entry_blocked_collateral",
                "free_collateral": free_collateral,
                "min_collateral": min_coll,
            })
            return 0
    except Exception:
        pass

    # ── 3. Live markets with a position (avoid duplicating legs) ──────────
    live_markets = await _get_live_markets_with_position(indexer)

    # ── 4. Load cointegrated pairs CSV ────────────────────────────────────
    if not os.path.exists(COINT_CSV_PATH):
        print(f"[ENTRY] cointegrated_pairs.csv not found. Run FIND_COINTEGRATED=True first.")
        return 0
    df = pd.read_csv(COINT_CSV_PATH)
    _mark("setup")

    # ── 4b. Filter to top-N pairs by quality score (2026-05-26) ────────────
    # Mainnet has 449+ cointegrated pairs. Scanning all of them takes 18 min
    # per loop, which is operationally unviable for mean-reversion signals
    # that decay in minutes. We keep only the top N by a composite quality
    # score that rewards:
    #   - High R²        (clean linear cointegration fit)
    #   - Short half-life (fast mean-reversion → more trades/period)
    #   - Low Hurst      (strong mean-reversion behavior)
    #
    # Score formula: r_squared × (24 / max(1, half_life)) × (1 / max(0.1, hurst))
    # Higher = better. Top N kept for actual scanning.
    try:
        from constants import MAX_COINT_PAIRS_TO_SCAN
    except ImportError:
        MAX_COINT_PAIRS_TO_SCAN = 150  # safe default

    _csv_total = len(df)
    if _csv_total > int(MAX_COINT_PAIRS_TO_SCAN) and MAX_COINT_PAIRS_TO_SCAN > 0:
        def _coint_quality(row):
            try:
                r2 = float(row.get("r_squared", 0) or 0)
                hl = float(row.get("half_life", 24) or 24)
                hr = float(row.get("hurst", 0.5) or 0.5)
                return r2 * (24.0 / max(1.0, hl)) * (1.0 / max(0.1, hr))
            except Exception:
                return 0.0
        df = df.copy()
        df["_quality"] = df.apply(_coint_quality, axis=1)
        df = df.sort_values("_quality", ascending=False).head(int(MAX_COINT_PAIRS_TO_SCAN)).reset_index(drop=True)
        df = df.drop(columns=["_quality"])
        log_event({
            "type": "universe_filtered",
            "csv_total": _csv_total,
            "kept_top_n": int(MAX_COINT_PAIRS_TO_SCAN),
            "actual_kept": len(df),
        }, print_terminal=False)

    _mark("csv_load")
    # ── 5. Market metadata (one API call) ─────────────────────────────────
    markets_response = await indexer.markets.get_perpetual_markets()
    markets = markets_response.get("markets", {}) or {}
    _mark("markets")

    # ── 6. Fetch funding rates upfront (one API call for all markets) ──────
    funding_rates = {}
    if FUNDING_GATE_ENABLED:
        try:
            funding_rates = await get_funding_rates(indexer, list(markets.keys()))
            log_event({
                "type": "funding_rates_fetched",
                "markets_count": len(funding_rates),
            }, print_terminal=False)
        except Exception as fe:
            log_event({"type": "funding_rates_fetch_error", "error": str(fe)})

    # ── 7. Load existing open pairs state ─────────────────────────────────
    bot_agents = _load_open_pairs_from_json()

    def get_safe_min_size(m_name):
        m_data = markets.get(m_name, {})
        val = m_data.get("minOrderSize") or m_data.get("stepSize")
        return float(val) if val else 0.0

    # MAX_PRICE_RATIO: filtro contra pares con asimetría extrema de precio.
    # 50 era el valor inicial (commit 37659ea).
    #
    # 2026-05-26: subido de 50 a 200. Análisis del log 2026-05-25 mostró que
    # MAX=50 filtraba 33 de 144 pares del CSV (23%) — incluyendo pares con
    # precios diversos pero hedge_ratio sano (ya protegido por HEDGE_RATIO_LOG_MAX
    # = 3.5). MAX=200 permite pares hasta ratio 200 (ej. SOL $150 / mid-cap $1).
    # Pares con ratio > 200 (mostly BTC/altcoin nano-cap) siguen filtrados, lo
    # cual es correcto: ahí el min order size de dYdX hace imposible operar.
    # La protección real contra pares espurios sigue siendo HEDGE_RATIO_LOG_MAX.
    MAX_PRICE_RATIO = 200.0

    # ──────────────────────────────────────────────────────────────────────
    # 7b. PRE-FILTER del CSV (2026-06-02)
    # ──────────────────────────────────────────────────────────────────────
    # Análisis del log de 4 días (1012 scans) mostró que 54 pares estaban
    # SIEMPRE bloqueados por price_ratio (31,258 skips) y 41 pares SIEMPRE
    # devolvían z=NaN (24,151 skips). Cada uno consume:
    #   - 2 fetches de candles (cache hit, pero aún CPU)
    #   - cálculo de z-score
    #   - log_event(signal_skip)
    # Total: ~55K events de ruido en 4 días, ~5-10% del tiempo de scan.
    #
    # Filtramos UNA SOLA VEZ al inicio del scan:
    #   - Pares con price_ratio > MAX_PRICE_RATIO en este momento → fuera
    #   - Pares que históricamente devolvieron NaN (_RUNTIME_NAN_PAIRS) → fuera
    #
    # _RUNTIME_NAN_PAIRS es module-level, se llena durante el scan cuando
    # detectamos z=NaN, y persiste mientras el proceso vive. Se limpia en
    # cada reload del CSV (cuando FIND_COINTEGRATED regenera).
    _prefilter_dropped_price_ratio = 0
    _prefilter_dropped_nan         = 0
    _prefilter_dropped_no_market   = 0
    _df_filtered_rows = []
    for _, _row in df.iterrows():
        _b = _row["base_market"]
        _q = _row["quote_market"]
        _pair_key = f"{_b}|{_q}"
        if _pair_key in _RUNTIME_NAN_PAIRS:
            _prefilter_dropped_nan += 1
            continue
        if _b not in markets or _q not in markets:
            _prefilter_dropped_no_market += 1
            continue
        try:
            _bp = float(markets[_b]["oraclePrice"])
            _qp = float(markets[_q]["oraclePrice"])
            _ratio = max(_bp, _qp) / max(1e-12, min(_bp, _qp))
            if _ratio > MAX_PRICE_RATIO:
                _prefilter_dropped_price_ratio += 1
                continue
        except Exception:
            _prefilter_dropped_no_market += 1
            continue
        _df_filtered_rows.append(_row)
    if _df_filtered_rows:
        df = pd.DataFrame(_df_filtered_rows).reset_index(drop=True)
    else:
        df = df.iloc[0:0]   # empty but preserves columns

    log_event({
        "type": "csv_prefiltered",
        "csv_after_prefilter": len(df),
        "dropped_price_ratio": _prefilter_dropped_price_ratio,
        "dropped_nan_runtime": _prefilter_dropped_nan,
        "dropped_no_market": _prefilter_dropped_no_market,
        "runtime_nan_set_size": len(_RUNTIME_NAN_PAIRS),
    }, print_terminal=False)
    _mark("funding")  # funding is reflected here pre-prefilter; prefilter is fast
    _mark("prefilter")

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 1: Collect and score all candidates
    # ══════════════════════════════════════════════════════════════════════
    candidates = []

    # Counters for scan diagnostics (printed at end of Phase 1)
    _skip_blacklist     = 0
    _skip_invalid       = 0
    _skip_live          = 0
    _skip_concentration = 0
    _skip_cooldown      = 0
    _skip_hedge_ratio   = 0  # hedge ratio outside valid log10 range
    _skip_price_r       = 0
    _skip_candles       = 0
    _skip_low_z         = 0
    _skip_min_size      = 0
    _max_z_seen         = 0.0   # highest |z| seen (even if below threshold)
    _best_low_z_pair    = ""    # pair closest to threshold

    # Track how many candidates per market in this scan cycle
    # (prevents ARKM-USD appearing in 9 candidates when only 1 can be traded)
    _market_candidate_count: dict = {}

    for _, row in df.iterrows():
        base_market = row["base_market"]
        quote_market = row["quote_market"]
        hedge_ratio = float(row["hedge_ratio"])
        half_life = row.get("half_life")

        # ── Cheap filters (no API calls) ────────────────────────────────
        if base_market in MARKET_BLACKLIST or quote_market in MARKET_BLACKLIST:
            _skip_blacklist += 1
            log_event({"type": "signal_skip", "reason": "market_blacklisted",
                       "base": base_market, "quote": quote_market}, print_terminal=False)
            continue

        # ── Hedge ratio sanity check (belt-and-suspenders if CSV is stale) ──
        # Ratios extremos como BTC/SHIB (12.8B) no tienen sentido económico:
        # el sizing de la leg barata sería microscópico y el z-score espurio.
        if hedge_ratio <= 0 or abs(np.log10(abs(hedge_ratio))) > HEDGE_RATIO_LOG_MAX:
            _skip_hedge_ratio += 1
            log_event({"type": "signal_skip", "reason": "hedge_ratio_extreme",
                       "base": base_market, "quote": quote_market,
                       "hedge_ratio": hedge_ratio}, print_terminal=False)
            continue

        if base_market not in markets or quote_market not in markets:
            _skip_invalid += 1
            log_event({"type": "signal_skip", "reason": "invalid_market",
                       "base": base_market, "quote": quote_market}, print_terminal=False)
            continue
        if base_market in live_markets or quote_market in live_markets:
            _skip_live += 1
            continue

        # ── Market concentration limit ───────────────────────────────────
        # Si un mercado ya aparece MAX_TRADES_PER_MARKET veces como candidato
        # en este scan, saltamos pares adicionales con ese mercado.
        # Evita que ARKM-USD (presente en 9+ pares del CSV) bloquee todo
        # el capital cuando ya hay 1 candidato con ARKM.
        b_count = _market_candidate_count.get(base_market, 0)
        q_count = _market_candidate_count.get(quote_market, 0)
        if b_count >= MAX_TRADES_PER_MARKET or q_count >= MAX_TRADES_PER_MARKET:
            _skip_concentration += 1
            continue
        if _is_pair_in_fail_cooldown(base_market, quote_market):
            _skip_cooldown += 1
            log_event({"type": "signal_skip", "reason": "pair_fail_cooldown",
                       "base": base_market, "quote": quote_market}, print_terminal=False)
            continue

        try:
            base_price = float(markets[base_market]["oraclePrice"])
            quote_price = float(markets[quote_market]["oraclePrice"])
            min_base = get_safe_min_size(base_market)
            min_quote = get_safe_min_size(quote_market)
            base_step = markets[base_market]["stepSize"]
            quote_step = markets[quote_market]["stepSize"]
            tick_base = markets[base_market]["tickSize"]
        except Exception:
            _skip_invalid += 1
            continue

        price_ratio = max(base_price, quote_price) / max(1e-12, min(base_price, quote_price))
        if price_ratio > MAX_PRICE_RATIO:
            _skip_price_r += 1
            log_event({"type": "signal_skip", "reason": "price_ratio",
                       "base": base_market, "quote": quote_market,
                       "price_ratio": round(price_ratio, 1),
                       "base_price": base_price, "quote_price": quote_price},
                      print_terminal=False)
            continue

        # ── Candle fetch + z-score (main API cost per candidate) ─────────
        series_1 = await get_candles_recent(indexer, base_market)
        series_2 = await get_candles_recent(indexer, quote_market)

        if not (len(series_1) > 0 and len(series_1) == len(series_2)):
            _skip_candles += 1
            continue

        spread = series_1 - (hedge_ratio * series_2)

        try:
            z_score = float(calculate_zscore(spread).values.tolist()[-1])
        except Exception:
            _skip_candles += 1
            continue

        # 2026-05-26: guard against NaN/Inf z-score.
        # calculate_zscore divides (x - rolling_mean) / rolling_std. When the
        # spread is constant (or warmup window not full), std=0 → z=NaN.
        # Without this guard, NaN slipped past the |z| < THRESH check (because
        # NaN comparisons are always False), reached the candidates list, and
        # showed up as "edge=$0.00 < required=$5.00 z=nan" in the log over
        # and over for the same pair, wasting scan cycles.
        if not math.isfinite(z_score):
            _skip_candles += 1
            # 2026-06-02: añadir a runtime cache para skip futuro en pre-filter
            _RUNTIME_NAN_PAIRS.add(f"{base_market}|{quote_market}")
            log_event({
                "type": "signal_skip",
                "reason": "zscore_not_finite",
                "base": base_market,
                "quote": quote_market,
                "z_score": "nan" if math.isnan(z_score) else "inf",
                "runtime_nan_set_now": len(_RUNTIME_NAN_PAIRS),
            }, print_terminal=False)
            continue

        if abs(z_score) > _max_z_seen:
            _max_z_seen = abs(z_score)
            _best_low_z_pair = f"{base_market}/{quote_market}"

        if abs(z_score) < ZSCORE_THRESH:
            _skip_low_z += 1
            continue

        # ── Spread quality diagnostics ───────────────────────────────────
        spread_std = float(np.std(spread[-WINDOW:]) if len(spread) >= WINDOW else np.std(spread))
        spread_mean = float(np.mean(spread[-WINDOW:]) if len(spread) >= WINDOW else np.mean(spread))

        # ── Hedge ratio drift check (CSV vs live OLS) ────────────────────
        # Detecta si el hedge ratio del CSV sigue siendo válido hoy.
        # Un drift >30% indica que la relación de precios cambió — el z-score
        # puede estar mal escalado y la posición puede no ser market-neutral.
        try:
            _hr_live = float(np.polyfit(series_2.values, series_1.values, 1)[0])
            _hr_drift_pct = abs(_hr_live - hedge_ratio) / max(abs(hedge_ratio), 1e-10) * 100.0
            if _hr_drift_pct > 30.0:
                log_event({
                    "type": "hedge_ratio_drift_warning",
                    "base": base_market,
                    "quote": quote_market,
                    "hr_csv": round(hedge_ratio, 6),
                    "hr_live": round(_hr_live, 6),
                    "drift_pct": round(_hr_drift_pct, 2),
                    "z_score": round(z_score, 3),
                })
        except Exception:
            pass

        spread_s = pd.Series(spread)
        if len(spread_s) < WINDOW + 1:
            _skip_candles += 1
            continue

        spread_last = float(spread_s.iloc[-1])
        spread_mean_prev_s = spread_s.rolling(WINDOW).mean().shift(1)
        spread_mean_prev = float(spread_mean_prev_s.iloc[-1])

        if not math.isfinite(spread_mean_prev):
            _skip_candles += 1
            continue

        spread_dev = abs(spread_last - spread_mean_prev)

        base_quantity = eff_usd / base_price
        quote_quantity = eff_usd / quote_price
        approx_spread_usd = spread_dev * min(base_quantity, quote_quantity)

        base_side = "BUY" if z_score < 0 else "SELL"
        quote_side = "BUY" if z_score > 0 else "SELL"

        base_size_fmt = format_number(base_quantity, base_step)
        quote_size_fmt = format_number(quote_quantity, quote_step)

        failsafe_p = base_price * (1.02 if base_side == "BUY" else 0.98)
        accept_failsafe_base_price = format_number(failsafe_p, tick_base)

        if float(base_size_fmt) < min_base or float(quote_size_fmt) < min_quote:
            _skip_min_size += 1
            log_event({"type": "signal_skip", "reason": "min_size",
                       "base": base_market, "quote": quote_market,
                       "base_size": float(base_size_fmt), "min_base": min_base},
                      print_terminal=False)
            continue

        # ── Spread bps for scoring (lightweight orderbook call) ───────────
        spread_bps = await get_market_spread_bps(indexer, base_market)
        if spread_bps is None:
            spread_bps = 100.0  # conservative default when unavailable

        # ── Opportunity score ─────────────────────────────────────────────
        # Higher = better:  big signal × fast reversion × tight spread
        hl_f = max(1.0, float(half_life or 20.0))
        score = abs(z_score) * (1.0 / hl_f) * (100.0 / max(1.0, spread_bps))

        candidates.append({
            "base_market": base_market,
            "quote_market": quote_market,
            "hedge_ratio": hedge_ratio,
            "half_life": half_life,
            "z_score": z_score,
            "spread_dev": spread_dev,
            "approx_spread_usd": approx_spread_usd,
            "base_side": base_side,
            "quote_side": quote_side,
            "base_price": base_price,
            "quote_price": quote_price,
            "base_size_fmt": base_size_fmt,
            "quote_size_fmt": quote_size_fmt,
            "accept_failsafe_base_price": accept_failsafe_base_price,
            "spread_bps": spread_bps,
            "score": score,
            "spread_std": spread_std,
            "spread_mean": spread_mean,
        })

        # Actualizar contadores de concentración por mercado
        _market_candidate_count[base_market]  = _market_candidate_count.get(base_market, 0) + 1
        _market_candidate_count[quote_market] = _market_candidate_count.get(quote_market, 0) + 1

    # ── Sort by score ──────────────────────────────────────────────────────
    if OPPORTUNITY_SCORING and candidates:
        candidates.sort(key=lambda c: c["score"], reverse=True)

    # ── Phase 1 summary (always printed to terminal) ───────────────────────
    _total_csv = len(df)
    _phase1_summary = (
        f"[ENTRY] Phase1: {_total_csv} CSV pairs → "
        f"blacklist={_skip_blacklist} hedge={_skip_hedge_ratio} invalid={_skip_invalid} "
        f"live={_skip_live} concentration={_skip_concentration} "
        f"cooldown={_skip_cooldown} price_ratio={_skip_price_r} candles={_skip_candles} "
        f"low_z={_skip_low_z}(max|z|={_max_z_seen:.2f} @ {_best_low_z_pair}) "
        f"min_size={_skip_min_size} → candidates={len(candidates)}"
    )
    print(_phase1_summary, flush=True)

    log_event({
        "type": "entry_candidates_scored",
        "total_csv": _total_csv,
        "total_candidates": len(candidates),
        "skip_blacklist": _skip_blacklist,
        "skip_hedge_ratio": _skip_hedge_ratio,
        "skip_invalid": _skip_invalid,
        "skip_live": _skip_live,
        "skip_price_ratio": _skip_price_r,
        "skip_cooldown": _skip_cooldown,
        "skip_low_z": _skip_low_z,
        "skip_min_size": _skip_min_size,
        "max_z_seen": round(_max_z_seen, 3),
        "best_low_z_pair": _best_low_z_pair,
        "opportunity_scoring": OPPORTUNITY_SCORING,
        "fee_round_trip": round(fee_round_trip, 4),
        "eff_usd_per_trade": eff_usd,
        "top": [
            {
                "pair": f"{c['base_market']}/{c['quote_market']}",
                "score": round(c["score"], 4),
                "z": round(c["z_score"], 3),
                "hl": c["half_life"],
                "spread_bps": round(c["spread_bps"], 1),
                "approx_edge_usd": round(c["approx_spread_usd"], 2),
            }
            for c in candidates[:5]
        ],
    })

    _mark("phase1")
    # ══════════════════════════════════════════════════════════════════════
    # PHASE 2: Open top N candidates (with funding + min-edge gates)
    # ══════════════════════════════════════════════════════════════════════
    for cand in candidates:
        if opened_count >= max_new_trades:
            break
        if open_pairs >= eff_max:
            break

        base_market = cand["base_market"]
        quote_market = cand["quote_market"]
        z_score = cand["z_score"]
        half_life = cand["half_life"]
        base_side = cand["base_side"]
        quote_side = cand["quote_side"]
        approx_spread_usd = cand["approx_spread_usd"]

        # ── Gate A: Min edge ─────────────────────────────────────────────
        min_edge_required = MIN_EDGE_FEE_MULTIPLE * fee_round_trip
        if approx_spread_usd < min_edge_required:
            print(f"[ENTRY] SKIP {base_market}/{quote_market}: min_edge "
                  f"edge=${approx_spread_usd:.2f} < required=${min_edge_required:.2f} "
                  f"(fee_rt=${fee_round_trip:.2f}×{MIN_EDGE_FEE_MULTIPLE}x) "
                  f"z={z_score:.2f}", flush=True)
            log_event({
                "type": "signal_skip",
                "reason": "min_edge_gate",
                "base": base_market,
                "quote": quote_market,
                "approx_spread_usd": round(approx_spread_usd, 4),
                "min_edge_required": round(min_edge_required, 4),
                "fee_round_trip": round(fee_round_trip, 4),
                "score": round(cand["score"], 4),
            })
            continue

        # ── Gate B: Funding rate ─────────────────────────────────────────
        if FUNDING_GATE_ENABLED and funding_rates:
            fr_base = funding_rates.get(base_market, 0.0)
            fr_quote = funding_rates.get(quote_market, 0.0)

            # Expected hold ≈ one half_life. Funding periods = hold_hours / 8h.
            hold_hours = float(half_life or 8.0)
            funding_periods = max(1.0, hold_hours / 8.0)

            # Longs pay when rate > 0; shorts receive when rate > 0.
            base_funding = eff_usd * fr_base * funding_periods * (1.0 if base_side == "BUY" else -1.0)
            quote_funding = eff_usd * fr_quote * funding_periods * (1.0 if quote_side == "BUY" else -1.0)
            net_funding = base_funding + quote_funding  # positive = net cost to us

            log_event({
                "type": "funding_check",
                "base": base_market,
                "quote": quote_market,
                "fr_base": fr_base,
                "fr_quote": fr_quote,
                "base_funding_cost": round(base_funding, 4),
                "quote_funding_cost": round(quote_funding, 4),
                "net_funding_cost": round(net_funding, 4),
                "hold_hours": hold_hours,
                "expected_edge_usd": round(approx_spread_usd, 4),
                "ratio": round(net_funding / max(approx_spread_usd, 1e-9), 3),
            }, print_terminal=False)

            if net_funding > approx_spread_usd * FUNDING_MAX_COST_RATIO:
                print(f"[ENTRY] SKIP {base_market}/{quote_market}: funding_gate "
                      f"net_funding=${net_funding:.3f} > {FUNDING_MAX_COST_RATIO*100:.0f}% of "
                      f"edge=${approx_spread_usd:.2f} z={z_score:.2f}", flush=True)
                log_event({
                    "type": "signal_skip",
                    "reason": "funding_gate",
                    "base": base_market,
                    "quote": quote_market,
                    "net_funding_cost": round(net_funding, 4),
                    "expected_edge_usd": round(approx_spread_usd, 4),
                    "max_ratio": FUNDING_MAX_COST_RATIO,
                })
                continue

        # ── Open the trade ───────────────────────────────────────────────
        trace_id = uuid.uuid4().hex[:10]

        log_event({
            "type": "entry_signal",
            "trace_id": trace_id,
            "base": base_market,
            "quote": quote_market,
            "z": round(z_score, 4),
            "score": round(cand["score"], 4),
            "spread_bps": round(cand["spread_bps"], 1),
            "spread_dev": round(cand["spread_dev"], 6),
            "spread_std_window": round(cand.get("spread_std", float("nan")), 6),
            "spread_mean_window": round(cand.get("spread_mean", float("nan")), 6),
            "approx_spread_usd": round(approx_spread_usd, 4),
            "min_edge_required": round(min_edge_required, 4),
            "base_side": base_side,
            "quote_side": quote_side,
            "base_price": cand["base_price"],
            "quote_price": cand["quote_price"],
            "base_size": float(cand["base_size_fmt"]),
            "quote_size": float(cand["quote_size_fmt"]),
            "half_life": half_life,
            "hedge_ratio": cand["hedge_ratio"],
            "usd_per_trade": eff_usd,
        })

        print(f"[ENTRY] Opening trade for {base_market} and {quote_market} "
              f"(score={cand['score']:.4f} z={z_score:.3f} hl={half_life})...")

        # 2026-05-26: min_m1_fill_usd dinámico, escalado al tamaño del trade.
        # Default era 50.0 fijo, lo cual rompe operación pequeña ($30/leg → fill_gate
        # imposible). Ahora pide 30% del intended o $5, lo que sea mayor.
        # Sobre $30/leg: gate = $9 (no $50).
        # Sobre $1000/leg: gate = $300.
        # Sobre $50/leg: gate = $15.
        dynamic_min_fill = max(5.0, eff_usd * 0.30)

        bot_agent = BotAgent(
            node,
            indexer,
            market_1=base_market,
            market_2=quote_market,
            base_side=base_side,
            base_size=cand["base_size_fmt"],
            base_price=cand["base_price"],
            quote_side=quote_side,
            quote_size=cand["quote_size_fmt"],
            quote_price=cand["quote_price"],
            accept_failsafe_base_price=cand["accept_failsafe_base_price"],
            z_score=z_score,
            half_life=half_life,
            hedge_ratio=cand["hedge_ratio"],
            trace_id=trace_id,
            intended_usd_per_trade=eff_usd,
            expected_edge_usd=approx_spread_usd,   # 2026-05-26: needed for dynamic spread gate
            min_m1_fill_usd=dynamic_min_fill,      # 2026-05-26: scales with eff_usd
        )

        bot_open_dict = await bot_agent.open_trades(wallet)

        # Always log result
        if isinstance(bot_open_dict, dict):
            log_event({
                "type": "entry_result",
                "trace_id": trace_id,
                "base": base_market,
                "quote": quote_market,
                "pair_status": bot_open_dict.get("pair_status"),
                "comments": bot_open_dict.get("comments"),
                "order_id_m1": bot_open_dict.get("order_id_m1"),
                "order_id_m2": bot_open_dict.get("order_id_m2"),
                "filled_usd_1": _sf(bot_open_dict.get("filled_usd_1")),
                "filled_usd_2": _sf(bot_open_dict.get("filled_usd_2")),
                "fee_1": _sf(bot_open_dict.get("fee_1")),
                "fee_2": _sf(bot_open_dict.get("fee_2")),
                "score": round(cand["score"], 4),
            })

        # Save LIVE
        if isinstance(bot_open_dict, dict) and bot_open_dict.get("pair_status") == "LIVE":
            now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            bot_open_dict["opened_at"] = bot_open_dict.get("opened_at") or now_iso
            bot_open_dict["side_1"] = bot_open_dict.get("side_1") or base_side
            bot_open_dict["side_2"] = bot_open_dict.get("side_2") or quote_side
            bot_open_dict["price_1"] = bot_open_dict.get("price_1") or cand["base_price"]
            bot_open_dict["price_2"] = bot_open_dict.get("price_2") or cand["quote_price"]
            bot_open_dict["size_1"] = bot_open_dict.get("size_1") or _sf(bot_open_dict.get("filled_size_1"), _sf(cand["base_size_fmt"]))
            bot_open_dict["size_2"] = bot_open_dict.get("size_2") or _sf(bot_open_dict.get("filled_size_2"), _sf(cand["quote_size_fmt"]))
            bot_open_dict["entry_score"] = round(cand["score"], 4)  # for future analysis

            bot_agents.append(bot_open_dict)
            with open(JSON_PATH, "w") as f:
                json.dump(bot_agents, f, indent=2)

            opened_count += 1
            open_pairs += 1
            live_markets.add(base_market)
            live_markets.add(quote_market)

            # Reset failure counter for this pair
            _record_pair_success(base_market, quote_market)

            print(f"[ENTRY] Trade LIVE: {base_market} {base_side} / {quote_market} {quote_side}")

            filled_usd_1 = _sf(bot_open_dict.get("filled_usd_1"))
            filled_usd_2 = _sf(bot_open_dict.get("filled_usd_2"))
            fee_1 = _sf(bot_open_dict.get("fee_1"))
            fee_2 = _sf(bot_open_dict.get("fee_2"))
            fee_estimated = bot_open_dict.get("fee_1_estimated") or bot_open_dict.get("fee_2_estimated")
            total_fees = fee_1 + fee_2
            residual = _sf(bot_open_dict.get("residual_usd"))
            z = _sf(bot_open_dict.get("z_score"))
            hl = bot_open_dict.get("half_life")
            notional = filled_usd_1 + filled_usd_2
            fee_label = "~est" if fee_estimated else "real"

            send_message(
                f"TRADE LIVE\n"
                f"{base_market} {base_side} / {quote_market} {quote_side}\n"
                f"Notional: ${notional:,.2f} (m1=${filled_usd_1:,.2f}, m2=${filled_usd_2:,.2f})\n"
                f"Fees ({fee_label}): ${total_fees:.4f}\n"
                f"Residual: ${residual:.2f}\n"
                f"Z-score: {z:.3f} | Half-life: {hl}h | Score: {cand['score']:.4f}"
            )
            continue

        # SKIPPED (spread gate, PREPARE fail before COMMIT, etc.) — no exposure created
        if isinstance(bot_open_dict, dict) and bot_open_dict.get("pair_status") == "SKIPPED":
            skip_reason = bot_open_dict.get("comments", "")
            log_event({
                "type": "entry_skipped",
                "trace_id": trace_id,
                "base": base_market,
                "quote": quote_market,
                "reason": skip_reason,
            })
            # 2026-07-11 fix: SPREAD_CEILING/SPREAD_COST_EXCEEDS_EDGE seguirán
            # bloqueando por horas (spreads en pares específicos no cambian
            # rápido). Sin cooldown, el bot re-intenta el mismo par cada scan
            # y no llega a otros candidatos válidos.
            # Caso real observado: HBAR-USD 399bps > 250bps repetido cada
            # scan durante 60min sin cooldown.
            if "SPREAD_CEILING" in skip_reason or "SPREAD_COST_EXCEEDS_EDGE" in skip_reason:
                fail_count = _record_pair_fail(base_market, quote_market)
                if fail_count >= PAIR_FAIL_COOLDOWN_THRESHOLD:
                    log_event({
                        "type": "pair_fail_cooldown_set",
                        "base": base_market,
                        "quote": quote_market,
                        "consecutive_fails": fail_count,
                        "cooldown_hours": PAIR_FAIL_COOLDOWN_HOURS,
                        "trigger": "spread_gate_repeated",
                    })
            continue

        # Not LIVE: check for orphan exposure, gated on committed flag.
        # Only when MARKET orders were actually sent — otherwise we'd log false
        # orphans from pre-existing exposure in other sessions.
        committed = isinstance(bot_open_dict, dict) and bot_open_dict.get("committed", False)
        try:
            live_now = await _get_live_markets_with_position(indexer) if committed else set()
            orphan = committed and ((base_market in live_now) or (quote_market in live_now))
        except Exception:
            orphan = False

        if orphan and isinstance(bot_open_dict, dict):
            bot_open_dict["pair_status"] = "ORPHAN"
            bot_open_dict["needs_reconcile"] = True
            bot_open_dict["opened_at"] = bot_open_dict.get("opened_at") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            bot_agents.append(bot_open_dict)
            with open(JSON_PATH, "w") as f:
                json.dump(bot_agents, f, indent=2)
            send_message(f"⚠️ ORPHAN: {base_market}/{quote_market}. Guardado para cleanup.")
            log_event({
                "type": "orphan_saved",
                "trace_id": trace_id,
                "base": base_market,
                "quote": quote_market,
            })
            fail_count = _record_pair_fail(base_market, quote_market)
            if fail_count >= PAIR_FAIL_COOLDOWN_THRESHOLD:
                log_event({"type": "pair_fail_cooldown_set", "base": base_market,
                           "quote": quote_market, "consecutive_fails": fail_count,
                           "cooldown_hours": PAIR_FAIL_COOLDOWN_HOURS})
        else:
            reason = bot_open_dict.get("comments", "Unknown") if isinstance(bot_open_dict, dict) else "Unknown"
            print(f"[ENTRY] Trade FAILED for {base_market}/{quote_market}. Reason: {reason}")

            if isinstance(bot_open_dict, dict):
                f1 = _sf(bot_open_dict.get("filled_usd_1"))
                f2 = _sf(bot_open_dict.get("filled_usd_2"))
                one_leg_filled = (f1 > 0) != (f2 > 0)
                if one_leg_filled:
                    fail_count = _record_pair_fail(base_market, quote_market)
                    if fail_count >= PAIR_FAIL_COOLDOWN_THRESHOLD:
                        log_event({"type": "pair_fail_cooldown_set", "base": base_market,
                                   "quote": quote_market, "consecutive_fails": fail_count,
                                   "cooldown_hours": PAIR_FAIL_COOLDOWN_HOURS})

    print("[ENTRY] Entry scan completed.")
    if opened_count > 0:
        print(f"[ENTRY] {opened_count} new pairs opened this batch.")

    # 2026-06-02: emit scan_timing para audit_run.py
    _mark("phase2")
    _scan_total_s = _time.time() - _scan_t0
    log_event({
        "type": "scan_timing",
        "scan_total_s": round(_scan_total_s, 3),
        "phase_setup_s":     round(_phase_t.get("setup", 0), 3),
        "phase_csv_load_s":  round(_phase_t.get("csv_load", 0), 3),
        "phase_markets_s":   round(_phase_t.get("markets", 0), 3),
        "phase_funding_s":   round(_phase_t.get("funding", 0), 3),
        "phase_prefilter_s": round(_phase_t.get("prefilter", 0), 3),
        "phase_phase1_s":    round(_phase_t.get("phase1", 0), 3),
        "phase_phase2_s":    round(_phase_t.get("phase2", 0), 3),
        "candidates": len(candidates) if 'candidates' in locals() else 0,
        "opened": opened_count,
    }, print_terminal=False)

    return opened_count
