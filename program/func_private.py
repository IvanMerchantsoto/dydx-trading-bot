import random
import time
from datetime import datetime, timedelta
from func_utils import format_number
from pprint import pprint
from constants import API_KEY, WALLET_ADDRESS

from dydx_v4_client import MAX_CLIENT_ID, OrderFlags
from v4_proto.dydxprotocol.clob.order_pb2 import Order

from dydx_v4_client.indexer.rest.constants import OrderType
from dydx_v4_client.indexer.rest.indexer_client import IndexerClient
from dydx_v4_client.network import TESTNET
from dydx_v4_client.node.client import NodeClient
from dydx_v4_client.node.market import Market
from dydx_v4_client.wallet import Wallet



# Place market order
async def place_market_order(node, indexer, market, side, size, price, reduce_only):
    MARKET_ID="BTC-USD"

    # Get position ID
    market = Market(
        (await indexer.markets.get_perpetual_markets(MARKET_ID))["markets"][MARKET_ID]
    )
    wallet = await Wallet.from_mnemonic(node, API_KEY, WALLET_ADDRESS)

    order_id = market.order_id(
        WALLET_ADDRESS, 0, random.randint(0, MAX_CLIENT_ID), OrderFlags.SHORT_TERM
    )

    # Get expiration time
    server_time = node.public.get_time()
    expiration = datetime.fromisoformat(server_time.data["iso"].replace("Z", "")) + timedelta(seconds=70)

    # Place an order
    placed_order = market.order(
        order_id=order_id,
        order_type=OrderType.MARKET,
        post_only=False,
        side=side,
        size=size,
        price=price,  # Recommend set to oracle price - 5% or lower for SELL, oracle price + 5% for BUY
        limit_fee = '0.015',
        expiration_epoch_seconds=expiration.timestamp(),
        time_in_force = "FOK",
        reduce_only=reduce_only
    )

    # Return result
    return placed_order.data


# Abort all open positions
async def abort_all_positions(node, indexer):

    wallet = await Wallet.from_mnemonic(node, API_KEY, WALLET_ADDRESS)

    # --------------------------------------------------------------------------
    # FASE 1: BATCH CANCEL
    # --------------------------------------------------------------------------

    try:
        response = await indexer.account.get_subaccount_orders(
            address=WALLET_ADDRESS, subaccount_number=0, status="OPEN"
        )
        open_orders = response if isinstance(response, list) else response.get("orders", [])

        if open_orders:
            print(f"{len(open_orders)} orders found. Canceling orders...")

            orders_by_pair = {}
            for order in open_orders:
                pair_id = int(order.get('clobPairId', 0))
                client_id = int(order.get('clientId', 0))
                if pair_id not in orders_by_pair: orders_by_pair[pair_id] = []
                orders_by_pair[pair_id].append(client_id)

            batches = [OrderBatch(clob_pair_id=pid, client_ids=cids) for pid, cids in orders_by_pair.items()]


            current_block = await node.latest_block_height()
            await node.batch_cancel_orders(
                wallet=wallet,
                subaccount_id=SubaccountId(owner=WALLET_ADDRESS, number=0),
                order_batches=batches,
                good_til_block=current_block + 20
            )
        else:
            print(f"No open orders found.")

    except Exception as e:
        print(f"Error cancelling orders: {e}")

    # --------------------------------------------------------------------------
    # FASE 2: CERRAR POSICIONES
    # --------------------------------------------------------------------------

    closed_markets = []

    try:
        account_resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
        subaccount = account_resp.get('subaccount', {})


        positions = subaccount.get('openPerpetualPositions', {})


        if not positions:
            positions = subaccount.get('perpetualPositions', {})
        # --------------------------


        active_positions = {m: d for m, d in positions.items() if float(d['size']) != 0}

        if not active_positions:
            print("No open positions found.")
            return []

        print(f"{len(active_positions)} opened positions. Closing positions...")

        for market_name, position_data in active_positions.items():
            size = float(position_data['size'])


            try:
                # 1. Obtener objeto Mercado
                m_data = await indexer.markets.get_perpetual_markets(market_name)
                market_obj = Market(m_data["markets"][market_name])

                # 2. Cerrar con close_position
                await node.close_position(
                    wallet=wallet,
                    address=WALLET_ADDRESS,
                    subaccount_number=0,
                    market=market_obj,
                    reduce_by=None,
                    client_id=random.randint(0, MAX_CLIENT_ID)
                )

                closed_markets.append(market_name)

            except Exception as e:
                print(f"Error closing position {market_name}: {e}")

            time.sleep(1)  # Pausa para evitar rate limits

    except Exception as e:
        print(f"Error closing positions: {e}")

    print("All positions closed.")
    return closed_markets