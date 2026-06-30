import random

from decouple import config
from dydx_v4_client.node.client import NodeClient
from dydx_v4_client.indexer.rest.indexer_client import IndexerClient
from dydx_v4_client.network import make_testnet
from dydx_v4_client.wallet import Wallet
from constants import (
WALLET_ADDRESS,
API_KEY,
NODE,
INDEXER,
WEBSOCKET,
)

CUSTOM_TESTNET = make_testnet(
        node_url= NODE,
        rest_indexer= INDEXER,
        websocket_indexer= WEBSOCKET
)
# Connect to DYDX
async def connect_dydx():
        node = None
        indexer = None
        wallet = None

        try:
            #print("🔌 Conectando al Nodo (Ejecución)...")
            node = await NodeClient.connect(CUSTOM_TESTNET.node)

            #print("👁️ Conectando al Indexer (Datos)...")
            indexer = IndexerClient(CUSTOM_TESTNET.rest_indexer)

            wallet = await Wallet.from_mnemonic(node, API_KEY, WALLET_ADDRESS)

            # Verificación rápida de que la cuenta existe
            response = await indexer.account.get_subaccounts(WALLET_ADDRESS)
            # Return Indexer
            subaccounts = response["subaccounts"]
            if subaccounts:
                print(f"Connection established. Subaccounts: {len(subaccounts)}")
            else:
                print("Error connecting to dydx.")

        except Exception as e:
                print(f"Error in the connection. {e}")
                # Si falla, devolvemos None para que el main sepa que hubo error.
                # 2026-06-02: corregido — antes regresaba (None, None) 2-tuple,
                # ahora regresa (None, None, None) consistente con el path OK.
                return None, None, None

        return node, indexer, wallet