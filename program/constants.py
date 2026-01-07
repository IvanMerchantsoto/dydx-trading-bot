from decouple import config

# SELECT MODE
MODE = "DEVELOPMENT"

# Close all open positions and orders
ABORT_ALL_POSITIONS= False

# Find cointegrated pairs
FIND_COINTEGRATED = False

# Place Trades
PLACE_TRADES = True

# Resolution
RESOLUTION = "1HOUR"

# Status Window (days)
WINDOW = 21

# Thresholds - Opening
MAX_HALF_LIFE = 24
ZSCORE_THRESH = 1.5
USD_PER_TRADE = 1000
USD_MIN_COLLATERAL = 20000 #CLOSE TO BALANCE

# Thresholds - Closing
CLOSE_AT_ZSCORE_CROSS = True

# Wallet Address
WALLET_ADDRESS = "dydx1napkzyjp3rauk5p787r9sfhvs74r8e357a30n5"

# WALLET PRIVATE KEY
API_KEY=config("API_KEY")

# URL ADDRESS TESTNET
NODE_TESTNET= "test-dydx-grpc.kingnodes.com"
INDEXER_TESTNET= "https://indexer.v4testnet.dydx.exchange"
WEBSOCKET_TESTNET= "wss://indexer.v4testnet.dydx.exchange/v4/ws"

# URL ADDRESS MAINNET
NODE_MAINNET= ""
INDEXER_MAINNET= ""
WEBSOCKET_MAINNET= ""

# URL Selection
NODE = NODE_MAINNET if MODE == "PRODUCTION" else NODE_TESTNET
INDEXER = INDEXER_MAINNET if MODE == "PRODUCTION" else INDEXER_TESTNET
WEBSOCKET = WEBSOCKET_MAINNET if MODE == "PRODUCTION" else WEBSOCKET_TESTNET