import asyncio

from func_connections import connect_dydx
from func_private import abort_all_positions
from func_public import construct_market_prices
from func_entry_pairs import open_positions
from func_cointegration import store_cointegration_results
from func_exit_pairs import manage_trade_exits
from func_messaging import send_message
from constants import (
ABORT_ALL_POSITIONS,
FIND_COINTEGRATED,
PLACE_TRADES,
MANAGE_EXITS
)

async def main():

    send_message(f"Bot Launched Successfully")

    # Connect to client
    try:
        print("Connecting to client...")
        node, indexer, wallet= await connect_dydx()
    except Exception as e:
        print("Error connecting to client: ", e)
        send_message(f"Failed to connect to DYDX.")
        exit(1)

    # Abort al open positions
    if ABORT_ALL_POSITIONS:
        try:
            print("Closing all positions...")
            close_orders = await abort_all_positions(node, indexer)
        except Exception as e:
            print("Error closing all positions: ", e)
            send_message(f"Failed to abort all positions.")
            exit(1)

    # Find cointegrated pairs
    if FIND_COINTEGRATED:
        # Construct Market Prices
        try:
            print("Fetching market prices, please allow 3 minutes...")
            df_market_prices = await construct_market_prices(node, indexer)
        except Exception as e:
            print("Error constructing market prices: ", e)
            send_message(f"Failed to construct market prices.")
            exit(1)

        # Store cointegrated pairs
        try:
            print("Storing cointegrated pairs...")
            stores_result = store_cointegration_results(df_market_prices)
            if stores_result != "saved":
                print("Error saving cointegrated pairs.")
                exit(1)
        except Exception as e:
            print("Error cointegrating pairs: ", e)
            send_message(f"Failed to cointegrate pairs.")
            exit(1)

    while True:
            # Manage exits
        if MANAGE_EXITS:
            try:
                print("Managing exits...")
                await manage_trade_exits(node, indexer, wallet)
            except Exception as e:
                print("Error managing exiting positions: ", e)
                send_message(f"Failed managing exiting positions.")
                exit(1)

        # Store cointegrated pairs
        if PLACE_TRADES:
            try:
                print("Finding trading opportunities...")
                await open_positions(node, indexer, wallet)
            except Exception as e:
                print("Error trading pairs: ", e)
                send_message(f"Failed opening trades.")
                exit(1)


if __name__ == "__main__":
    asyncio.run(main())

