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
    # Reduced sleep from 0.2 → 0.05 because cache hits are now most calls,
    # so we'd hammer the API far less even at lower delay.
    await asyncio.sleep(0.05)

    try:
        candles_resp = await indexer.markets.get_perpetual_market_candles(
            market=market,
            resolution=RESOLUTION,
            limit=100
        )
    except Exception as e:
        print(f"[PUBLIC] get_candles_recent error for {market}: {e}", flush=True)
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
async def get_candles_historical(indexer, market):

    # Define output
    close_prices = []

    # Extract historical price data for each timeframe
    for timeframe in ISO_TIMES.keys():

        # Confirm times needed
        tf_obj = ISO_TIMES[timeframe]
        from_iso = tf_obj["from_iso"]
        to_iso = tf_obj["to_iso"]

        # Protect API
        await asyncio.sleep(0.2)

        # Get data
        candles = await indexer.markets.get_perpetual_market_candles(
            market=market,
            resolution=RESOLUTION,
            from_iso=from_iso,
            to_iso=to_iso,
            limit=100
        )

        # Structure data
        for candle in candles["candles"]:
            close_prices.append({"datetime": candle["startedAt"],market: candle["close"]})

        # Construct and return DataFrame
    close_prices.reverse()
    return close_prices



# Get bid-ask spread in basis points for a single market
async def get_market_spread_bps(indexer, market):
    """
    Fetches the best bid and ask from the live orderbook and returns the
    spread expressed in basis points: (ask - bid) / mid * 10_000.

    Returns None on any error so the caller can treat it as "proceed".
    A None result must never block a trade — it just means data unavailable.
    """
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
        return ((best_ask - best_bid) / mid) * 10_000.0
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

    # Declare variables
    tradeable_markets =[]
    markets= await indexer.markets.get_perpetual_markets()
    market_data = markets.get("markets", {})

    # Find tradeable pais
    for market_id in market_data.keys():
        market_info = market_data[market_id]
        if market_info.get("status")=="ACTIVE":
            tradeable_markets.append(market_id)

    print(f"{len(tradeable_markets)} active markets found.")

    # Set initial DateFrame
    if tradeable_markets:
        close_prices = await get_candles_historical(indexer, tradeable_markets[0])
        df = pd.DataFrame(close_prices)
        df.set_index("datetime", inplace=True)

        # Append other prices to DataFrame
        # You can limit the amount to loop through here to save time in development UAT
        for market in tradeable_markets[1:]:
            try:
                close_prices_add = await get_candles_historical(indexer, market)
                if not close_prices_add or len(close_prices_add) < 10:
                    continue
                df_add = pd.DataFrame(close_prices_add)
                df_add.set_index("datetime", inplace=True)
                df_add = df_add.astype(float)

                if df_add[market].std() == 0:
                    print(f"   ⚠️ Saltando {market}: Sin movimiento de precio.")
                    continue

                df = pd.merge(df, df_add, how="outer", on="datetime",copy=False)
            except Exception as e:
                print(f"Error calculating cointegration results: {e}")
                continue

        # Check any columns with NaNs
        df = df.astype(float)
        nans = df.columns[df.isna().any()].tolist()
        if len(nans)>0:
            df.drop(columns=nans, inplace=True)

        print(f"✅ Tabla final lista con {len(df.columns)} mercados seleccionados.")

        # Return result
        return df



