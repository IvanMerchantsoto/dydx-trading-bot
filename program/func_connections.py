import random

from decouple import config
from dydx_v4_client.node.client import NodeClient
from dydx_v4_client.indexer.rest.indexer_client import IndexerClient
from dydx_v4_client.network import make_testnet, make_mainnet
from dydx_v4_client.wallet import Wallet
from constants import (
WALLET_ADDRESS,
API_KEY,
NODE,
INDEXER,
WEBSOCKET,
MODE,
)

# ──────────────────────────────────────────────────────────────────────────────
# 2026-06-02: BUG CRÍTICO ARREGLADO.
#
# Antes el código SIEMPRE usaba make_testnet(...) con las URLs de mainnet.
# Pero make_testnet hardcodea chain_id="dydx-testnet-4" en el NodeConfig.
# Esto causaba que todas las txs se firmaran con chain_id testnet y al
# llegar a mainnet eran RECHAZADAS con code=4 "signature verification failed".
#
# Síntoma observado: 4 días en mainnet, 0 trades, 100% NOT_FOUND.
# Causa raíz: chain_id mismatch en la firma vs el chain real.
#
# Fix: usar make_mainnet en producción (chain_id="dydx-mainnet-1").
# ──────────────────────────────────────────────────────────────────────────────
if MODE == "PRODUCTION":
    CUSTOM_NETWORK = make_mainnet(
        node_url=NODE,
        rest_indexer=INDEXER,
        websocket_indexer=WEBSOCKET,
    )
else:
    CUSTOM_NETWORK = make_testnet(
        node_url=NODE,
        rest_indexer=INDEXER,
        websocket_indexer=WEBSOCKET,
    )

# Backwards-compat alias (algún código viejo puede referenciar CUSTOM_TESTNET)
CUSTOM_TESTNET = CUSTOM_NETWORK


# Connect to DYDX
async def connect_dydx():
        node = None
        indexer = None
        wallet = None

        try:
            #print("🔌 Conectando al Nodo (Ejecución)...")
            node = await NodeClient.connect(CUSTOM_NETWORK.node)

            #print(f"👁️ Conectando al Indexer (Datos)... chain_id={CUSTOM_NETWORK.node.chain_id}")
            indexer = IndexerClient(CUSTOM_NETWORK.rest_indexer)

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