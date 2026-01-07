import pandas as pd
import numpy as np
import asyncio
import time

from func_utils import get_ISO_times
from constants import RESOLUTION
from pprint import pprint

# Get relevant time periods for ISO from and to
ISO_TIMES = get_ISO_times()

# Get Candles recent
async def get_candles_recent(indexer, market):

    # Define output
    close_prices = []

    # Protect API
    await asyncio.sleep(0.2)

    # Get data
    candles = await indexer.markets.get_perpetual_market_candles(
        market=market,
        resolution=RESOLUTION,
        limit=100
    )

    # Structure data
    for candle in candles["candles"]:
        close_prices.append(candle["close"])

    # Construct and return DataFrame
    close_prices.reverse()
    prices_result = np.array(close_prices).astype(np.float64)
    return prices_result


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



