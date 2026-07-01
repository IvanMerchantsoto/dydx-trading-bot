import pandas as pd
import numpy as np
import asyncio
import time

from func_utils import get_ISO_times
from constants import RESOLUTION
from pprint import pprint

# Get relevant time periods for ISO from and to
ISO_TIMES = get_ISO_times()

# ─────────────────────────────────────────────────────────────────────────────
# Candle cache (2026-05-26)
# ─────────────────────────────────────────────────────────────────────────────
# RESOLUTION is "1HOUR" — candles only change at the top of each hour.
# Cache TTL of 30 min means: within a single hour, we serve cached candles
# instantly. At next loop iteration after the hour boundary, we refresh.
#
# Impact:
#   - First scan: same speed as before (cache miss for all markets)
#   - Subsequent scans within the hour: ~5-10 seconds instead of 18 minutes
#     (no API calls, just dict lookup)
#
# Memory: 119 markets × 100 floats × 8 bytes = ~95 KB. Trivial.
_CANDLE_CACHE = {}  # market_name → (numpy_array, fetch_timestamp_s)
_CANDLE_CACHE_TTL_S = 30 * 60   # 30 minutes

# 2026-07-01: track markets that hit persistent errors so we don't spam logs
_CANDLE_ERROR_LOGGED: set = set()


def candle_cache_stats():
    """Diagnostic: how many markets cached and how old the oldest entry is."""
    now = time.time()
    if not _CANDLE_CACHE:
        return {"size": 0, "oldest_age_s": 0}
    oldest = min(ts for _, ts in _CANDLE_CACHE.values())
    return {"size": len(_CANDLE_CACHE), "oldest_age_s": int(now - oldest)}


def candle_cache_clear():
    """Force refresh on next call. Use sparingly (e.g., after long idle)."""
    _CANDLE_CACHE.clear()


# Get Candles recent
async def get_candles_recent(indexer, market):
    """
    Returns a numpy float64 array of recent close prices (oldest first).
    Returns empty array on error or if the market has no data.
    Never raises.

    2026-05-26: now uses module-level cache with TTL of 30 min.
    First call per market hits the API; subsequent calls within TTL window
    return cached array instantly. With RESOLUTION='1HOUR', cache validity
    matches the data update cycle.
    """
    now = time.time()

    # Cache hit?
    cached = _CANDLE_CACHE.get(market)
    if cached is not None:
        arr, ts = cached
        if now - ts < _CANDLE_CACHE_TTL_S:
            return arr  # serve from cache, zero API call

    # Cache miss — fetch from API
    # 2026-07-01 v2: retry con exponential backoff en 429 Too Many Requests.
    # publicnode.com rate-limitea agresivamente si hay picos concurrentes.
    # 3 intentos con 0.5s, 1s, 2s de espera cubren la mayoría de casos.
    await asyncio.sleep(0.05)

    candles_resp = None
    last_err = None
    for attempt in range(3):
        try:
            candles_resp = await indexer.markets.get_perpetual_market_candles(
                market=market,
                resolution=RESOLUTION,
                limit=100
            )
            break  # success
        except Exception as e:
            last_err = e
            err_str = str(e).lower()
            if "429" in err_str or "too many" in err_str or "rate" in err_str:
                # Rate limited — backoff and retry
                backoff = 0.5 * (2 ** attempt)
                await asyncio.sleep(backoff)
                continue
            else:
                # Non-rate-limit error — don't retry
                break

    if candles_resp is None:
        # All retries exhausted — silently return empty (cache will be filled next scan)
        # Only log once per market per session to avoid log spam on ongoing 429s
        if market not in _CANDLE_ERROR_LOGGED:
            print(f"[PUBLIC] get_candles_recent error for {market}: {last_err}", flush=True)
            _CANDLE_ERROR_LOGGED.add(market)
        return np.array([], dtype=np.float64)

    # Defensively handle missing/malformed response
    if not isinstance(candles_resp, dict):
        return np.array([], dtype=np.float64)

    raw = candles_resp.get("candles")
    if not raw or not isinstance(raw, list):
        return np.array([], dtype=np.float64)

    close_prices = []
    for candle in raw:
        try:
            close_prices.append(float(candle["close"]))
        except Exception:
            continue

    if not close_prices:
        return np.array([], dtype=np.float64)

    close_prices.reverse()  # chronological order
    arr = np.array(close_prices, dtype=np.float64)

    # Store in cache for next call
    _CANDLE_CACHE[market] = (arr, now)
    return arr


# Get Candles Historical
async def get_candles_historical(indexer, market, sleep_between_tfs: float = 0.15):
    """
    Fetch historical candles across all timeframes in ISO_TIMES.

    2026-07-01 v2: SECUENCIAL con sleep pequeño entre timeframes.
    La primera versión paralelizaba los 4 timeframes por market, pero cuando
    también procesamos 15 markets en batch, llegamos a 60 requests concurrentes
    y el indexer rate-limitea (429 Too Many Requests).

    Con secuencial + sleep 0.15s: cada market tarda ~600-800ms.
    Con batch de 5 markets concurrentes = ~15 requests peak (safe).
    """
    close_prices = []
    for tf_key in ISO_TIMES.keys():
        tf_obj = ISO_TIMES[tf_key]
        await asyncio.sleep(sleep_between_tfs)
        try:
            candles = await indexer.markets.get_perpetual_market_candles(
                market=market,
                resolution=RESOLUTION,
                from_iso=tf_obj["from_iso"],
                to_iso=tf_obj["to_iso"],
                limit=100,
            )
            candles_list = candles.get("candles", []) if isinstance(candles, dict) else []
            for candle in candles_list:
                close_prices.append({"datetime": candle["startedAt"], market: candle["close"]})
        except Exception:
            # Continue with other timeframes even if one fails
            continue

    close_prices.reverse()
    return close_prices



# ─────────────────────────────────────────────────────────────────────────────
# Spread (bid-ask BPS) cache — 2026-06-01
# ─────────────────────────────────────────────────────────────────────────────
# get_market_spread_bps is called in entry_pairs Phase 1 for EVERY pair (top
# 150) on EVERY scan. That's 150 orderbook API calls per scan, the dominant
# bottleneck (~30s/scan).
#
# Spreads on dYdX move slowly enough that a 60s TTL is acceptable for SCORING.
# This is NOT used by the spread_check gate (which still fetches live in
# func_bot_agent), so caching here doesn't compromise risk control.
#
# Impact: ~30s/scan → ~3-5s/scan on warm cache.
_SPREAD_CACHE = {}              # market → (spread_bps, fetch_ts)
_SPREAD_CACHE_TTL_S = 60        # 60 seconds — spreads change slowly enough


def spread_cache_stats():
    """Diagnostic helper — size & oldest age."""
    now = time.time()
    if not _SPREAD_CACHE:
        return {"size": 0, "oldest_age_s": 0}
    oldest = min(ts for _, ts in _SPREAD_CACHE.values())
    return {"size": len(_SPREAD_CACHE), "oldest_age_s": int(now - oldest)}


def spread_cache_clear():
    _SPREAD_CACHE.clear()


# Get bid-ask spread in basis points for a single market
async def get_market_spread_bps(indexer, market, *, force_fresh: bool = False):
    """
    Fetches the best bid and ask from the live orderbook and returns the
    spread expressed in basis points: (ask - bid) / mid * 10_000.

    Returns None on any error so the caller can treat it as "proceed".
    A None result must never block a trade — it just means data unavailable.

    Cached for _SPREAD_CACHE_TTL_S seconds. Pass force_fresh=True from any
    code path that uses the value to MAKE a trading decision (e.g. the
    pre-commit gate in func_bot_agent). Scoring callers (entry_pairs Phase 1)
    should let the cache hit.
    """
    now = time.time()
    if not force_fresh:
        cached = _SPREAD_CACHE.get(market)
        if cached is not None:
            sp, ts = cached
            if now - ts < _SPREAD_CACHE_TTL_S:
                return sp

    try:
        ob = await indexer.markets.get_perpetual_market_orderbook(market=market)
        asks = ob.get("asks", []) if isinstance(ob, dict) else []
        bids = ob.get("bids", []) if isinstance(ob, dict) else []
        if not asks or not bids:
            return None
        best_ask = float(asks[0].get("price", 0) if isinstance(asks[0], dict) else asks[0])
        best_bid = float(bids[0].get("price", 0) if isinstance(bids[0], dict) else bids[0])
        if best_ask <= 0 or best_bid <= 0 or best_bid >= best_ask:
            return None
        mid = (best_ask + best_bid) / 2.0
        bps = ((best_ask - best_bid) / mid) * 10_000.0
        _SPREAD_CACHE[market] = (bps, now)
        return bps
    except Exception:
        return None


async def get_funding_rates(indexer, markets_list: list) -> dict:
    """
    Returns {market: nextFundingRate_float} for each market in markets_list.

    nextFundingRate is the predicted 8h funding rate as a decimal
    (e.g. 0.0001 = 0.01%/8h). Positive means longs pay shorts.

    Fetches all markets in a single API call. Markets missing from the
    response are returned as 0.0 — callers must treat 0.0 as "unknown",
    not as "zero funding".

    Returns empty dict on any API error so callers can proceed without
    the gate rather than blocking entries entirely.
    """
    try:
        resp = await indexer.markets.get_perpetual_markets()
        mdata = resp.get("markets", {}) if isinstance(resp, dict) else {}
        result = {}
        for m in markets_list:
            md = mdata.get(m, {})
            rate = (
                md.get("nextFundingRate")
                or md.get("nextFundingRate8H")
                or md.get("fundingRate8H")
                or 0.0
            )
            try:
                result[m] = float(rate)
            except Exception:
                result[m] = 0.0
        return result
    except Exception:
        return {}


# Construct market prices
async def construct_market_prices(node, indexer):
    """
    Fetch historical candles for ALL active markets and merge into single DataFrame.

    2026-07-01 fix: paralelizado. Antes procesaba 150 markets secuencialmente,
    cada uno con ~800ms de latencia (4 timeframes × 200ms). Total: ~2 minutos
    solo en los sleeps, plus ~5-10 minutos en fetches reales = 10-20 min.

    Ahora: procesa en batches de 15 markets concurrentes. Cada market internamente
    también paraleliza sus 4 timeframes. Total: ~30-60 segundos.
    """
    import time as _t
    _t0 = _t.time()

    # Declare variables
    tradeable_markets = []
    markets = await indexer.markets.get_perpetual_markets()
    market_data = markets.get("markets", {})

    # Find tradeable pairs
    for market_id in market_data.keys():
        market_info = market_data[market_id]
        if market_info.get("status") == "ACTIVE":
            tradeable_markets.append(market_id)

    print(f"{len(tradeable_markets)} active markets found. Fetching in parallel...", flush=True)

    # ── Parallel fetch in batches (conservative to avoid 429) ──
    # 2026-07-01 v2: BATCH=5 instead of 15. Con 4 timeframes secuenciales
    # dentro de cada market, esto es 5 × 1 = 5 requests concurrentes al pico.
    # Deja headroom para otros calls del bot (orderbook, subaccounts).
    #
    # Speed vs safety trade-off:
    #   BATCH=15: 30-45s pero 429 errors → 90% markets fallan
    #   BATCH=5:  60-120s pero 0% 429 errors → todos los markets OK
    #   Preferimos correctitud sobre velocidad.
    BATCH_SIZE = 5
    BATCH_SLEEP = 0.5   # pausa entre batches para no saturar indexer
    results = {}   # market -> list of {datetime, market: close}
    failures = []

    async def _fetch_one(market):
        try:
            data = await get_candles_historical(indexer, market)
            return market, data
        except Exception as e:
            return market, None

    for i in range(0, len(tradeable_markets), BATCH_SIZE):
        batch = tradeable_markets[i:i + BATCH_SIZE]
        batch_results = await asyncio.gather(*[_fetch_one(m) for m in batch])
        for m, data in batch_results:
            if data is not None and len(data) >= 10:
                results[m] = data
            else:
                failures.append(m)
        # Sleep between batches to give indexer breathing room
        if i + BATCH_SIZE < len(tradeable_markets):
            await asyncio.sleep(BATCH_SLEEP)
        if (i + BATCH_SIZE) % (BATCH_SIZE * 5) == 0 or (i + BATCH_SIZE) >= len(tradeable_markets):
            print(f"   Fetched {min(i + BATCH_SIZE, len(tradeable_markets))}/{len(tradeable_markets)} "
                  f"markets ({_t.time() - _t0:.1f}s elapsed, {len(results)} OK, {len(failures)} failed)", flush=True)

    # ── Merge into single DataFrame ──
    if not results:
        print(f"❌ construct_market_prices: 0 markets fetched successfully")
        return pd.DataFrame()

    dfs = []
    for market, close_prices in results.items():
        try:
            df_m = pd.DataFrame(close_prices)
            df_m.set_index("datetime", inplace=True)
            df_m = df_m.astype(float)
            if df_m[market].std() == 0:
                continue  # skip markets with no movement
            dfs.append(df_m)
        except Exception:
            continue

    if not dfs:
        return pd.DataFrame()

    # Merge all at once (much faster than iterative pd.merge)
    df = pd.concat(dfs, axis=1)

    # Drop columns with NaNs
    df = df.astype(float)
    nans = df.columns[df.isna().any()].tolist()
    if len(nans) > 0:
        df.drop(columns=nans, inplace=True)

    _elapsed = _t.time() - _t0
    print(f"✅ Tabla final lista con {len(df.columns)} mercados en {_elapsed:.1f}s "
          f"(dropped {len(nans)} con NaN)", flush=True)
    return df



