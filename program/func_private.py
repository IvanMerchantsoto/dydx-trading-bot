import random
import time
import asyncio
import json

from dydx_v4_client import MAX_CLIENT_ID, OrderFlags
from dydx_v4_client.indexer.rest.constants import OrderType
from dydx_v4_client.node.market import Market
from dydx_v4_client.wallet import Wallet
from v4_proto.dydxprotocol.clob.order_pb2 import Order

from constants import API_KEY, WALLET_ADDRESS, USD_PER_TRADE

# Get existing open positions
async def is_open_positions(indexer, market):
       # Protect API
    await asyncio.sleep(0.2)

    try:
        account_resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
        subaccount = account_resp.get("subaccount", {})

        positions = subaccount.get("openPerpetualPositions", {})


        if market in positions:
            size = float(positions[market].get("size", 0))
            return abs(size) > 0

        return False

    except Exception as e:
        print(f"Error consulting positions in {market}: {e}")
        return False


# Check order status
async def check_order_status(indexer, order_id):

    await asyncio.sleep(3)
    try:
        response = await indexer.account.get_subaccount_orders(
            address=WALLET_ADDRESS.strip(),
            subaccount_number=0
        )

        if response is None:
            print("Warning: Indexer is None.")
            return "UNKNOWN"

        orders = response if isinstance(response, list) else response.get("orders", [])

        for order in orders:
            if str(order.get("clientId")) == str(order_id):
                return order.get("status")

        return "NOT_FOUND"
    except Exception as e:
        print(f"Error in Indexer: {e}")
        return "FAILED"


# Place market order
async def place_market_order(node, indexer, wallet, market, side, size, price, reduce_only, time_in_force_type):
    try:
        if wallet is None:
            wallet = await Wallet.from_mnemonic(node, API_KEY.strip(), WALLET_ADDRESS.strip())

        # 2. Obtener datos del mercado (necesario para el ID y el Oracle)
        m_data = await indexer.markets.get_perpetual_markets(market)
        market_data = m_data["markets"][market]
        oracle_price = float(market_data["oraclePrice"])
        market_obj = Market(market_data)
        step_size = float(market_data["stepSize"])

        if side == "BUY":
            execution_price = float(oracle_price * 1.05)
            dydx_side = Order.Side.SIDE_BUY
        else:
            execution_price = float(oracle_price * 0.95)
            dydx_side = Order.Side.SIDE_SELL

        # 4. Configurar IDs
        client_id = random.randint(0, MAX_CLIENT_ID)
        # Nota: Usamos la secuencia actual de la memoria de la wallet
        order_id = market_obj.order_id(
            WALLET_ADDRESS.strip(), 0, client_id, OrderFlags.SHORT_TERM
        )

        current_block = await node.latest_block_height()

        # 5. Crear la orden
        placed_order = market_obj.order(
            order_id=order_id,
            order_type=OrderType.MARKET,
            post_only=False,
            side=dydx_side,
            size=float(size),
            price=execution_price,
            time_in_force=time_in_force_type,
            reduce_only=reduce_only,
            good_til_block=current_block + 20,
        )

        print(f">>> [TX] Enviando {side} {market} Size: {size} Price: {price}...")

        # 6. Enviar transacción
        transaction = await node.place_order(
            wallet=wallet,
            order=placed_order,
        )

        wallet.sequence += 1

        placed_order_result = {
            "order": {
                "id": str(client_id),
                "status": "PENDING",
                "market": market
            },
            "tx_hash": getattr(transaction, 'tx_hash', 'unknown')
        }

        return placed_order_result

    except Exception as e:
        print(f"Error enviando orden: {e}")
        return None


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

            await asyncio.sleep(1)  # Pausa para evitar rate limits

    except Exception as e:
        print(f"Error closing positions: {e}")

    # Override json file with empty trades
    bot_agents = []
    with open("bot_agents.json", "w") as f:
        json.dump(bot_agents, f)

    print("All positions closed.")
    return closed_markets


async def get_real_fill_details(indexer, client_id, market, max_retries=5):
    """
    Consulta el Indexer para saber el estado FINAL y el tamaño REAL ejecutado.
    CORREGIDO: Suma fills reales si la orden fue CANCELADA (IOC).
    """

    await asyncio.sleep(2)  # Espera inicial vital

    for _ in range(max_retries):
        try:
            # Buscamos la orden
            orders_resp = await indexer.account.get_subaccount_orders(
                address=WALLET_ADDRESS,
                subaccount_number=0,
                limit=10,
                ticker=market
            )
            orders = orders_resp if isinstance(orders_resp, list) else orders_resp.get("orders", [])

            target_order = None
            for o in orders:
                if str(o.get("clientId")) == str(client_id):
                    target_order = o
                    break

            if target_order:
                # --- AQUÍ EMPIEZA LA CORRECCIÓN ---
                status = target_order.get("status")
                original_size = float(target_order.get("size", 0))
                order_id = target_order.get("id")  # Necesitamos el ID del servidor

                filled_size = 0.0

                # LÓGICA HÍBRIDA:
                if status == "CANCELED":
                    # Si está cancelada (típico de IOC parcial), remainingSize miente.
                    # Hay que pedir los FILLS explícitamente.
                    try:
                        # Nota: Ajusta los parámetros según tu versión exacta del SDK de dYdX
                        fills_resp = await indexer.account.get_subaccount_fills(
                            address=WALLET_ADDRESS,
                            subaccount_number=0,
                            ticker=market,
                            limit=20
                        )
                        all_fills = fills_resp if isinstance(fills_resp, list) else fills_resp.get("fills", [])

                        # 2. Filtramos en Python los fills que pertenecen a NUESTRA order_id
                        # (Convertimos a string ambos para asegurar comparación correcta)
                        my_fills = [f for f in all_fills if str(f.get("orderId")) == str(order_id)]

                        # 3. Sumamos lo que realmente se ejecutó
                        filled_size = sum(float(f.get("size", 0)) for f in my_fills)

                    except Exception as e:
                        print(f"Error obteniendo fills: {e}. Asumiendo 0.")
                        filled_size = 0.0
                else:
                    # Si es FILLED u OPEN, la resta matemática suele funcionar bien
                    remaining = float(target_order.get("remainingSize", 0))
                    filled_size = original_size - remaining

                # ----------------------------------

                # Determinamos etiqueta humana
                fill_status = "UNKNOWN"
                print(f"STATUS = {status}")

                if status == "FILLED":
                    fill_status = "FILLED"
                elif status == "CANCELED":
                    if filled_size > 0:
                        fill_status = "PARTIALLY_FILLED_IOC"
                    else:
                        fill_status = "KILLED_BY_FOK"
                else:
                    fill_status = status

                return {
                    "found": True,
                    "status_label": fill_status,
                    "raw_status": status,
                    "filled_size": filled_size,
                    "filled_usd_est": filled_size * float(target_order.get("price", 0))  # Aprox
                }
            else:
                print(f".", end="", flush=True)
                await asyncio.sleep(1)  # Espera antes de reintentar si no encuentra orden

        except Exception as e:
            print(f"Error polling order: {e}")
            await asyncio.sleep(1)

    return {"found": False, "status_label": "NOT_FOUND", "filled_size": 0, "filled_usd_est": 0}