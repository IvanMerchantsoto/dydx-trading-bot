import asyncio

from func_connections import connect_dydx
from func_private import abort_all_positions
from func_public import construct_market_prices
from constants import (
ABORT_ALL_POSITIONS,
FIND_COINTEGRATED
)

async def main():

    # Connect to client
    try:
        print("Connecting to client...")
        node, indexer = await connect_dydx()
    except Exception as e:
        print("Error connecting to client: ", e)
        exit(1)

    # Abort al open positions
    if ABORT_ALL_POSITIONS:
        try:
            print("Closing all positions...")
            close_orders = await abort_all_positions(node, indexer)
        except Exception as e:
            print("Error closing all positions: ", e)
            exit(1)

    # Find cointegrated pairs
    if FIND_COINTEGRATED:
        try:
            print("Fetching market prices, please allow 3 minutes...")
            df_market_prices = construct_market_prices(node)
        except Exception as e:
            print("Error constructing market prices: ", e)
            exit(1)


if __name__ == "__main__":
    asyncio.run(main())

