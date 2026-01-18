from decouple import config

# ===== Loop / Cadencia =====
EXIT_CHECK_SECONDS = 30       # cada cuánto evalúas exits
KPI_SECONDS = 600             # cada cuánto mandas KPIs (10 min)
BATCH_OPEN_TRADES = 3         # abrir N trades y luego forzar revisión
MAX_OPEN_TRADES = 6           # hard cap de pares abiertos (recomendado)

# ===== Exit rules =====
USE_MIN_PROFIT_TP = True
MIN_PROFIT_PCT = 0.02
MIN_PROFIT_USD = 20.0
DROP_STALE_RECORDS = True
USE_Z_TP = True
USE_Z_SL = True
USE_TIME_STOP = True
Z_TP = 0.5
Z_SL_DELTA = 1.0
TIME_STOP_HOURS = 12

# ===== Risk-Off =====
RISK_OFF_ENABLED = True

# Disparadores de risk-off
RISK_OFF_FREE_COLLATERAL_TRIGGER = 7000   # si free collateral baja de esto -> risk-off
RISK_OFF_FORCE_IF_OPEN_TRADES_GE = 6      # si ya estás en el hard cap -> risk-off
RISK_OFF_MIN_AGE_HOURS = 2                # no mates trades recién abiertos (opcional)

# Score weights (ajusta)
RISK_SCORE_W_AGE = 1.0
RISK_SCORE_W_ABS_Z = 3.0
RISK_SCORE_W_UNREAL_PNL = 0.001  # penaliza pérdidas si hay unreal pnl (escala)

# SELECT MODE
MODE = "DEVELOPMENT"

# Close all open positions and orders
ABORT_ALL_POSITIONS= False

# Find cointegrated pairs
FIND_COINTEGRATED = False

# Manage exits
MANAGE_EXITS = True

# Place Trades
PLACE_TRADES = True

# Resolution
RESOLUTION = "1HOUR"

# Status Window (days)
WINDOW = 21

# Thresholds - Opening
MAX_HALF_LIFE = 24
ZSCORE_THRESH = 1.5
USD_PER_TRADE = 2000
USD_MIN_COLLATERAL = 7000 #CLOSE TO BALANCE

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