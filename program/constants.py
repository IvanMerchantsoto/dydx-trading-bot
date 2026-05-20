from decouple import config

# ===== Loop / Cadencia =====
EXIT_CHECK_SECONDS = 30       # cada cuánto evalúas exits (segundos)
KPI_SECONDS = 600             # cada cuánto mandas KPIs (10 min)
SESSION_SUMMARY_SECONDS = 300 # cada cuánto mandas resumen de rentabilidad de sesión (5 min)
BATCH_OPEN_TRADES = 3         # abrir N trades y luego forzar revisión
MAX_OPEN_TRADES = 15          # hard cap de pares abiertos (era 8, subido para testing)

# ===== Exit rules =====
USE_MIN_PROFIT_TP = True
# MIN_PROFIT_USD y MIN_PROFIT_PCT son el beneficio NETO mínimo DESPUÉS de fees.
# La lógica calcula automáticamente fees pagadas en apertura + estimado de cierre
# y los suma al umbral, así que estos valores son la "ganancia real mínima".
MIN_PROFIT_PCT = 0.00025  # 0.025% del notional como ganancia neta mínima
MIN_PROFIT_USD = 0.50     # o al menos $0.50 neto — lo que sea mayor
# Con $1000/leg ($2000 notional), fees round-trip ≈ $2.00:
#   min_gross_required = fees + max($0.50, $2000×0.025%) = $2 + $0.50 = $2.50
# Antes: MIN_PROFIT_PCT=0.15% → PCT gate=$3.00 → min_gross=$5.00
#   → bloqueaba trades con gross $2.90-$4.74 que SÍ eran rentables
# Ahora: cualquier trade con gross > $2.50 (net > $0.50) puede cerrar por TP
DROP_STALE_RECORDS = True

USE_Z_TP = True
Z_TP = 0.7           # cierra cuando abs(z) vuelve a ≤ 0.7 (casi media)

USE_Z_SL = True
Z_SL_DELTA = 1.5     # SL cuando abs(z_now) >= abs(z_entry) + 1.5
                     # Ej: entrada z=1.5 → SL en z=3.0

# TIME_STOP desactivado por decisión del usuario:
# se confía en Z-SL y Hard SL monetario para todo cierre por riesgo.
USE_TIME_STOP = False
TIME_STOP_HOURS = 24   # dejado aquí por si se reactiva (no se usa si USE_TIME_STOP=False)

# ===== TP Confirmation =====
# El usuario eligió "doble confirmación sin hold mínimo":
# - No hay tiempo mínimo de tenencia antes de TP
# - Z debe estar en zona TP durante 2 checks consecutivos (~60s con EXIT_CHECK_SECONDS=30)
# - Esto evita cierres por velas de ruido de 1-2 minutos
TP_CONFIRM_CHECKS = 2        # checks consecutivos requeridos en zona TP
MIN_HOLD_MINUTES_FOR_TP = 0  # 0 = sin restricción de tiempo mínimo

# ===== Risk-Off =====
RISK_OFF_ENABLED = True

# Disparadores de risk-off
RISK_OFF_FREE_COLLATERAL_TRIGGER = 7000   # si free collateral < esto → risk-off

# IMPORTANTE: igual a MAX_OPEN_TRADES. Si fuera menor, risk-off dispararía
# antes de llegar al hard cap y cerraría posiciones sanas.
RISK_OFF_FORCE_IF_OPEN_TRADES_GE = 15    # igualado a MAX_OPEN_TRADES

# Edad mínima antes de que risk-off pueda cerrar un par.
# 0.5h = 30 minutos. Protege aperturas muy recientes de cierres forzados inmediatos,
# pero permite cerrar posiciones jóvenes que ya estén en pérdida real.
RISK_OFF_MIN_AGE_HOURS = 0.5

# ===== Risk-Off Score Weights =====
# El "peor par" se define principalmente por:
#   1. Pérdida monetaria real (unrealized negativo) — peso mayor
#   2. Divergencia del z-score — señal de tesis rota
#   3. Stale bonus (solo si > 24h abierto) — calculado inline en func_risk_off.py
#
# ANTES: W_AGE=1.0 dominaba → el par más viejo siempre era "peor", independientemente
# de su PnL. INCORRECTO: un par sano y viejo no debe cerrarse solo por ser viejo.
# AHORA: edad no tiene peso; solo pérdida + divergencia de z.
RISK_SCORE_W_AGE = 0.0          # eliminado: edad no determina peor par
RISK_SCORE_W_ABS_Z = 5.0        # z-divergence es la segunda señal más importante
RISK_SCORE_W_UNREAL_PNL = 0.15  # $1 de pérdida ≈ 0.15 puntos de score
                                  # (antes era 0.001 — la pérdida no importaba!)

# Hard Stop-Loss monetario (en func_exit_pairs.py)
# _hard_sl_level = max(HARD_SL_USD, HARD_SL_PCT × notional)
#
# Scaling logic (dynamic sizing):
#   $550/leg = $1100 notional → max($20, 0.6%×$1100=$6.60) = $20  (1.82% loss)
#   $1000/leg = $2000 notional → max($20, 0.6%×$2000=$12) = $20   (1.00% loss)
#   $2000/leg = $4000 notional → max($20, 0.6%×$4000=$24) = $24   (0.60% loss)
#   $3000/leg = $6000 notional → max($20, 0.6%×$6000=$36) = $36   (0.60% loss)
#
# Previous: $35 floor → $550/leg pair could lose 3.2% before triggering.
# Now: $20 floor → tighter protection on small positions, scales up on large ones.
# Note: TP_LOSS_EXIT (new) cuts losing positions when z reverts BEFORE HARD_SL triggers.
HARD_SL_USD = 20.0
HARD_SL_PCT = 0.006

# SELECT MODE
MODE = "DEVELOPMENT"

# ===== Fee constants =====
# Keep in sync with func_private.py TAKER_FEE_BPS.
# Central definition so entry and exit modules share the same value.
TAKER_FEE_BPS = 0.0005    # 0.05% per leg (taker)
MAKER_FEE_BPS = 0.0002    # 0.02% per leg (maker) — used for savings estimate in logs

# ===== Maker Exits (TP) =====
# Use POST_ONLY limit orders for Take-Profit exits to pay maker fee (≤0.02%)
# instead of taker (0.05%). Falls back to MARKET if not filled within timeout.
# SL / HARD_SL always use MARKET for reliability — speed > cost when stopping loss.
#
# Disabled in DEVELOPMENT: testnet orderbooks are too thin for POST_ONLY to fill
# within the timeout window (0/N fills observed — all fall back to taker anyway,
# wasting 45s and adding complexity). Enable in PRODUCTION where books are deep.
MAKER_EXIT_ENABLED = MODE == "PRODUCTION"
MAKER_EXIT_TIMEOUT_S = 45       # seconds to wait for POST_ONLY fill before MARKET fallback

# ===== Funding Rate Gate =====
# Before opening a pair, check the predicted 8h funding rate for both legs.
# If net funding cost over expected hold > FUNDING_MAX_COST_RATIO × expected edge → skip.
# nextFundingRate from dYdX indexer is per 8h as a decimal (e.g. 0.0001 = 0.01%/8h).
# Set FUNDING_GATE_ENABLED=False to disable completely.
FUNDING_GATE_ENABLED = True
FUNDING_MAX_COST_RATIO = 0.5    # skip if net funding cost > 50% of expected edge

# ===== Opportunity Scoring =====
# When True, all candidates in a scan cycle are scored and sorted before opening.
# Score = abs(z) × (1/half_life) × (100/spread_bps) — higher is better.
# Opens the best N pairs instead of the first N found in the CSV.
OPPORTUNITY_SCORING = True

# ===== Min Edge Gate =====
# Require approx_spread_usd > MIN_EDGE_FEE_MULTIPLE × round_trip_fees before entering.
# round_trip_fees = 4 × usd_per_trade × TAKER_FEE_BPS  (open+close both legs at taker).
# With $1000/leg: round_trip=$2, min_edge=$4. Prevents trading marginal signals.
MIN_EDGE_FEE_MULTIPLE = 2.0

# ===== Dynamic Sizing =====
# Compute USD_PER_TRADE and MAX_OPEN_TRADES dynamically from equity each loop.
# Prevents over/under-deployment as account grows or shrinks.
# USD_PER_TRADE stays between DYNAMIC_SIZING_MIN_USD and DYNAMIC_SIZING_MAX_USD.
DYNAMIC_SIZING = True
DYNAMIC_SIZING_PCT = 0.04         # 4% of equity per leg
                                  # With $13k equity: 4% = $520 → rounds to $500/leg
                                  # 10 pairs × $500 × 2 = $10,000 gross (~0.77× equity)
                                  # Previous: 2.5% → $350/leg → 14 pairs → 0.75× equity
                                  # Now: slightly larger positions, fewer max pairs, same leverage
DYNAMIC_SIZING_MIN_USD = 200.0    # floor: $200/leg
DYNAMIC_SIZING_MAX_USD = 3000.0   # cap:   $3000/leg
# Max open trades = floor(equity / (usd_per_trade × 2.5)), hard-capped at MAX_OPEN_TRADES.
# 2.5× buffer ensures initial margin + SL headroom for all open pairs simultaneously.
# Example at $13k equity, $500/leg: floor(13000 / (500×2.5)) = floor(13000/1250) = 10 pairs

# ===== Drawdown Circuit Breaker =====
# Halt new entries if session PnL drops below DRAWDOWN_CIRCUIT_BREAKER_PCT of
# the equity measured at session start. Entries resume after DRAWDOWN_HALT_HOURS.
# Only counts positions opened in the current session (uses unrealizedPnL delta).
DRAWDOWN_CIRCUIT_BREAKER_ENABLED = True
DRAWDOWN_CIRCUIT_BREAKER_PCT = 0.03   # halt if session loss exceeds 3% of start equity
DRAWDOWN_HALT_HOURS = 4.0             # halt duration in hours

# ===== Pre-COMMIT Liquidity Gate =====
# Skip COMMIT if bid-ask spread on either leg exceeds this threshold.
# Prevents fee waste from flattening when one leg fills but the other can't.
#
# Testnet (DEVELOPMENT): 200 bps — orderbooks are intentionally thin,
#   spreads of 50-250 bps are normal and don't predict mainnet fill quality.
#   Still blocks truly illiquid outliers (ATH-USD at 257 bps, etc.)
#
# Mainnet (PRODUCTION): 25 bps — liquid pairs have 1-20 bps.
#   Anything wider predicts partial fills on one leg.
#
# Set to 0 to disable entirely.
MAX_ENTRY_SPREAD_BPS = 200 if MODE == "DEVELOPMENT" else 25

# Close all open positions and orders
# PELIGRO: True cierra TODO a market price en el startup — paga taker fees por cada posición
# y realiza cualquier pérdida latente. Solo actívalo manualmente antes de un restart de limpieza.
# En operación normal DEBE ser False.
ABORT_ALL_POSITIONS = False

# Find cointegrated pairs
FIND_COINTEGRATED = False

# Auto-refresh cointegration: regenerate cointegrated_pairs.csv if the file
# is older than this many hours, even when FIND_COINTEGRATED = False.
# Set to 0 to disable auto-refresh (not recommended — stale pairs are dangerous).
# 6h = refresh roughly 4x per day to catch new divergences in evolving markets.
COINTEGRATION_REFRESH_HOURS = 6

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
                       # El exit por Z_SL y HARD_SL gestiona el riesgo de tardanza
ZSCORE_THRESH = 1.5
USD_PER_TRADE = 1000
USD_MIN_COLLATERAL = 7000

# Thresholds - Closing
CLOSE_AT_ZSCORE_CROSS = True

# Wallet Address
WALLET_ADDRESS = "dydx1napkzyjp3rauk5p787r9sfhvs74r8e357a30n5"

# WALLET PRIVATE KEY
API_KEY = config("API_KEY")

# URL ADDRESS TESTNET
NODE_TESTNET = "test-dydx-grpc.kingnodes.com"
INDEXER_TESTNET = "https://indexer.v4testnet.dydx.exchange"
WEBSOCKET_TESTNET = "wss://indexer.v4testnet.dydx.exchange/v4/ws"

# URL ADDRESS MAINNET
NODE_MAINNET = ""
INDEXER_MAINNET = ""
WEBSOCKET_MAINNET = ""

# URL Selection
NODE = NODE_MAINNET if MODE == "PRODUCTION" else NODE_TESTNET
INDEXER = INDEXER_MAINNET if MODE == "PRODUCTION" else INDEXER_TESTNET
WEBSOCKET = WEBSOCKET_MAINNET if MODE == "PRODUCTION" else WEBSOCKET_TESTNET

UNMANAGED_CLOSE_MAX_ATTEMPTS = 2
UNMANAGED_ALERT_COOLDOWN_SECONDS = 300

# Markets con posición atascada que el abort no puede cerrar en testnet.
# Ignorarlos evita que bloqueen entradas indefinidamente.
# Vaciar cuando se cierren manualmente desde la UI de dYdX.
UNMANAGED_IGNORE_MARKETS = {
    "LDO-USD", "COMP-USD", "STX-USD", "ARKM-USD",
    # Posiciones atascadas por falta de liquidez en testnet (2026-05-19).
    # No aplica en producción — los orderbooks de mainnet son profundos.
    # Vaciar esta lista cuando se cierren manualmente desde la UI de dYdX.
    "ATH-USD", "BLAST-USD", "OP-USD", "PUMP-USD", "ZK-USD", "ZORA-USD",
}