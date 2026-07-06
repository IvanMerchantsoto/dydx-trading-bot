from decouple import config

# ===== Loop / Cadencia =====
EXIT_CHECK_SECONDS = 30       # cada cuánto evalúas exits (segundos)
KPI_SECONDS = 600             # cada cuánto mandas KPIs (10 min)
SESSION_SUMMARY_SECONDS = 300 # cada cuánto mandas resumen de rentabilidad de sesión (5 min)
BATCH_OPEN_TRADES = 3         # abrir N trades y luego forzar revisión
MAX_OPEN_TRADES = 5           # 2026-07-06: subido de 3 a 5 con equity $641.
                              # Estrategia diversificación: 5 pares × $65/leg =
                              # $650 notional (~igual al de 3×$100), pero:
                              #   - Variance portfolio: 33% → 20%
                              #   - Sharpe esperado sube
                              #   - Ley de grandes números favorece más pares
                              # Precauciones activas:
                              #   - MAX_TRADES_PER_MARKET=2 (concentración)
                              #   - HARD_SL $8 por par → worst case -$40 con 5
                              #     (aún manejable, 6% del equity total)
                              #   - Pre-flight edge/ceiling check
                              # Si va bien 2 semanas, considerar subir a 6-8.

# ===== Exit rules =====
USE_MIN_PROFIT_TP = True
# MIN_PROFIT_USD y MIN_PROFIT_PCT son el beneficio NETO mínimo DESPUÉS de fees.
# La lógica calcula automáticamente fees pagadas en apertura + estimado de cierre
# y los suma al umbral, así que estos valores son la "ganancia real mínima".
# 2026-05-26: bajado a $0.10 para operación $100 mainnet.
# Con notional combinado ~$60, MIN_PROFIT_PCT=0.0025 → $0.15 net mínimo.
# Más bajo que esto significa cerrar trades por puro ruido.
MIN_PROFIT_PCT = 0.0025   # 0.25% del notional como ganancia neta mínima
# 2026-07-06: subido de $0.10 a $0.30 con equity $641 y sizing $65/leg.
# Notional/pair = $130, 0.25% × $130 = $0.325 → nuevo piso $0.30.
MIN_PROFIT_USD = 0.30
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
RISK_OFF_FREE_COLLATERAL_TRIGGER = 25     # 2026-05-26: ajustado a operación $100.
                                           # Si free collateral < $25, dispara risk-off (cierra peor par).
                                           # Cuando equity crezca a $500+: subir a $100.

# IMPORTANTE: igual a MAX_OPEN_TRADES. Si fuera menor, risk-off dispararía
# antes de llegar al hard cap y cerraría posiciones sanas.
#
# 2026-05-26: con MAX_OPEN_TRADES=1 en operación $100, ponerlo en 1 hace que
# risk-off se evalúe cada vez que abres tu único trade. Aunque el risk_off
# tiene guardrails (edad, pérdida real, etc.), gasta API calls innecesarias.
# Lo subo a 99 efectivamente para que SOLO se dispare por el trigger de
# free collateral bajo (RISK_OFF_FREE_COLLATERAL_TRIGGER), no por hit del cap.
RISK_OFF_FORCE_IF_OPEN_TRADES_GE = 99    # efectivamente desactivado para $100

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
# 2026-05-26: ajustado para operación $100 mainnet.
# $3 USD = 3% del equity total = stop loss tolerable por trade individual.
# Sobre $60 notional combinado: 3/60 = 5% → razonable.
# Cuando subas a $500 equity: subir a $10. A $5,000: $20.
HARD_SL_USD = 8.0             # 2026-07-06: 3 → 8 con equity $641 y sizing $65/leg.
                              # Notional pair = $130. 5% × $130 = $6.5.
                              # $8 es piso absoluto (max de HARD_SL_PCT × notional y USD).
                              # Worst case con 5 pares: -$40 (6% del equity total).
HARD_SL_PCT = 0.05            # 5% del notional combinado (efectivo si notional > $160)

# SELECT MODE
# 2026-05-26: cambiado a PRODUCTION para operar $100 USD en mainnet.
# Esto cambia automáticamente: NODE/INDEXER/WEBSOCKET a mainnet,
# MAX_ENTRY_SPREAD_BPS ceiling 500→75, y MAKER_EXIT_ENABLED a True.
MODE = "PRODUCTION"

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
#
# 2026-05-26: AJUSTADO PARA OPERACIÓN $100 EN MAINNET.
# Con $100 equity:
#   30% × $100 = $30/leg
#   max_open = floor(100 / (30 × 2.5)) = 1 pair
#   Notional combinado = $60 (60% del equity, conservador para mainnet)
# Cuando equity crezca a $500+:
#   30% × $500 = $150/leg, max_open hasta 2 pairs
#   (subir DYNAMIC_SIZING_PCT a 0.20 cuando equity > $300 para diversificar)
DYNAMIC_SIZING = True
# 2026-07-06: Escalado a equity $641 (post-depósito $550) con enfoque
# DIVERSIFICACIÓN. En vez de 3 pares grandes ($100/leg), usamos 5 pares
# más chicos ($65/leg). Mismo notional total (~$650) pero:
#   - Variance del portfolio: 33% → 20% (40% menos drawdown esperado)
#   - Más "at bats" → convergencia estadística más rápida
#   - Mejor Sharpe ratio esperado
#   - Cada trade contribuye menos al riesgo total
# Ratio fees/edge se mantiene igual (escalan juntos).
DYNAMIC_SIZING_PCT = 0.10         # 10% of equity per leg  → $64 con $641
DYNAMIC_SIZING_MIN_USD = 40.0     # floor: $40/leg
DYNAMIC_SIZING_MAX_USD = 80.0     # cap:   $80/leg (redondeo step $8)
# Cuando subas a $500 de equity y quieras escalar:
#   DYNAMIC_SIZING_PCT = 0.20
#   DYNAMIC_SIZING_MIN_USD = 50.0
#   DYNAMIC_SIZING_MAX_USD = 500.0
# Cuando subas a $5,000:
#   DYNAMIC_SIZING_PCT = 0.08
#   DYNAMIC_SIZING_MIN_USD = 200.0
#   DYNAMIC_SIZING_MAX_USD = 3000.0
# Max open trades = floor(equity / (usd_per_trade × 2.5)), hard-capped at MAX_OPEN_TRADES.
# 2.5× buffer ensures initial margin + SL headroom for all open pairs simultaneously.
# Example at $13k equity, $500/leg: floor(13000 / (500×2.5)) = floor(13000/1250) = 10 pairs

# ===== Drawdown Circuit Breaker =====
# Halt new entries if session PnL drops below DRAWDOWN_CIRCUIT_BREAKER_PCT of
# the equity measured at session start. Entries resume after DRAWDOWN_HALT_HOURS.
# Only counts positions opened in the current session (uses unrealizedPnL delta).
DRAWDOWN_CIRCUIT_BREAKER_ENABLED = True
# 2026-05-26: subido de 3% a 20% para operación $100 mainnet.
# A 3% sobre $100 = $3 → se dispararía con un solo trade malo.
# A 20% sobre $100 = $20 → halt cuando pierdes una quinta parte del capital.
# Cuando subas a $500+ equity: bajar a 5% ($25 halt).
# Cuando subas a $5,000+: bajar a 3% original.
DRAWDOWN_CIRCUIT_BREAKER_PCT = 0.20
DRAWDOWN_HALT_HOURS = 4.0             # halt duration in hours

# ===== Pre-COMMIT Spread Gate (DYNAMIC) =====
# Replaces the fixed MAX_ENTRY_SPREAD_BPS. Decides whether the cost of crossing
# the bid-ask spread of both legs (entry + exit) is acceptable RELATIVE to the
# expected edge of the specific trade.
#
# Math:
#   spread_cost_usd = (spread_bps_leg1 + spread_bps_leg2) × notional_per_leg / 10000
#       (this is the total cost of crossing both books on entry AND exit,
#        derived from: 2 legs × 2 sides × half_spread × notional)
#
#   Decision logic (3 phases, applied in order):
#     1. FLOOR pass: if both legs have spread ≤ SPREAD_GATE_PER_LEG_FLOOR_BPS,
#        permit unconditionally (the spread is so tight that even with tiny edge
#        the cost is negligible).
#     2. CEILING block: if any leg's spread > SPREAD_GATE_PER_LEG_CEILING_BPS,
#        reject unconditionally regardless of edge (book is structurally broken
#        — market maker pulled, news event, etc.).
#     3. EDGE-PROPORTIONAL: spread_cost_usd ≤ SPREAD_GATE_MAX_PCT_OF_EDGE × expected_edge_usd.
#        This is the dynamic core: allow proportionally bigger spreads for
#        proportionally bigger expected edges.
#
# Example numbers (with default 0.30, floor=25, ceiling=500/75):
#   Trade A: edge=$207, leg1=210bps, leg2=20bps, notional=$2,550
#     phase 1: leg2 ≤ 25 ✓ but leg1 > 25 ✗ → not unconditional
#     phase 2: leg1=210 ≤ 500 (testnet) ✓ → not blocked
#     phase 3: cost = (210+20)×2550/10000 = $58.65;  max = 0.30×$207 = $62.10
#              $58.65 ≤ $62.10 → PASS
#   Trade B: edge=$207, leg1=417bps, leg2=20bps
#     phase 3: cost = $111;  max = $62 → FAIL
#   Trade C: edge=$80, leg1=15bps, leg2=10bps
#     phase 1: both ≤ 25 → PASS (no edge calculation needed)
#   Trade D: edge=$1000, leg1=600bps, leg2=10bps  (mainnet)
#     phase 2: leg1 > 75 (mainnet ceiling) → FAIL (book is broken regardless of edge)
SPREAD_GATE_MAX_PCT_OF_EDGE = 0.50      # spread cost ≤ 50% of expected edge
                                         # 2026-05-28: subido de 0.30 a 0.50.
                                         # Mainnet altcoins (AVAX, ETC, LINK, etc.) tienen
                                         # spreads 80-200bps reales. Con 0.30 y $30/leg, casi
                                         # todo edge se va en spread cost. 0.50 permite
                                         # captura neta positiva ajustada.
SPREAD_GATE_PER_LEG_FLOOR_BPS = 25      # both legs below → always permit
SPREAD_GATE_PER_LEG_CEILING_BPS = 500 if MODE == "DEVELOPMENT" else 250
                                         # 2026-05-28: mainnet 250 (data-driven).
                                         # calibrate_spread_ceiling.py ejecutado en mainnet
                                         # reveló distribución BIMODAL:
                                         #   - Cohorte real (50%):   1.5 - 83 bps
                                         #   - Borderline (16%):     83 - 250 bps
                                         #   - Cohorte rota (34%):   1000 - 4000+ bps
                                         # No hay markets entre 250-1000 bps. Ceiling 250
                                         # acepta toda la cohorte real + borderline,
                                         # bloquea estructuralmente los books rotos.
                                         #
                                         # Tight markets observados: ETH (1.5), SOL (2.4),
                                         # XRP (5.4), LINK (14.3), LTC (23), DOT (24),
                                         # ADA (25), APT (32), AVAX (38), UNI (34), SUI (34).
                                         # Estos son los pares "viables" económicamente.
                                         #
                                         #   testnet 500 (thin books normal aquí)
                                         #   mainnet 250 (real cohort, blocks broken books)

# Backward-compat: kept as informational constant, NO longer used by the bot logic.
# Pre-2026-05-26 the spread check used this fixed value. Replaced by the dynamic
# gate above. Keep here only for legacy modules that might still import it.
MAX_ENTRY_SPREAD_BPS = SPREAD_GATE_PER_LEG_CEILING_BPS

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
# 2026-07-04: bajado de 24h → 8h. WINDOW=21 barras (1HOUR) = 21h de memoria.
# Con MAX=24h teníamos incongruencia: half_life ≈ rolling window causa que
# el rolling mean "camine" con el spread, produciendo FALSA CONVERGENCIA
# (z baja pero el spread real no se mueve → PnL negativo aunque z=0).
# Regla: WINDOW debería cubrir 3× half-life típico.
#   Con WINDOW=21h: MAX_HALF_LIFE ≤ 7-8h para cubrir 3×.
# Con este cambio: menos pares cointegrados pero MÁS RÁPIDOS a revertir
# → menos tiempo esperando → menos fee bleed → menos falsa convergencia.
MAX_HALF_LIFE = 8
                       # El exit por Z_SL y HARD_SL gestiona el riesgo de tardanza
ZSCORE_THRESH = 2.7    # 2026-05-22: bajado de 3.0 a 2.7 tras backtest OOS comparativo.
                       #
                       # Backtest (60d, 70/30 split, top 30 pares, defaults de constants):
                       #                     COMBINED                       OOS (últimos 18d)
                       #   z   n   WR   PF   EV    Net      Sharpe  DD    oos_n WR    PF    EV     Net
                       #   2.5 1378 73.3% 2.00 $7.84 $10,797 8.27   -$524  431  77.5% 3.81 $16.20 $6,980
                       #   2.7 1179 74.1% 2.21 $9.15 $10,791 8.68   -$420  373  78.8% 3.85 $17.19 $6,412  ← elegido
                       #   2.8 1083 73.8% 2.23 $9.39 $10,173 8.47   -$484  346  78.6% 3.87 $17.61 $6,094
                       #   3.0 908  73.6% 2.25 $9.68 $8,786  7.70   -$561  302  77.8% 3.92 $17.49 $5,282
                       #
                       # z=2.7 domina en Pareto: mejor Net, WR, Sharpe y DD.
                       # OOS es MEJOR que IS (no overfit; régimen reciente favorece mean-reversion).
                       # Trade marginal entre 2.8→2.7 tiene EV=$11.78 (OOS), bien sobre costo round-trip.
                       # Trade marginal entre 2.7→2.5 baja a EV=$9.79 con PF degradando — no vale el churn.
                       # ZSCORE_THRESH anterior fijo en 3.0 capturaba solo 8% de escaneos vs ~20% con 2.7
                       # (en log del bot 2026-05-19/22). 2.5× más oportunidades, mismo nivel de calidad.
USD_PER_TRADE = 65           # 2026-07-06: fallback si dynamic falla. $65/leg (config $641 equity).
USD_MIN_COLLATERAL = 70      # 2026-07-06: subido a $70 (~1.1× sizing). Bot pausa si free < $70.
                              # Con $100 equity, deja $70 de buffer para variaciones.
                              # Cuando subas equity a $500+: subir a $150.

# ===== Universe Size Limit (2026-05-26) =====
# Mainnet typically yields 400+ cointegrated pairs. Scanning all takes 15-20min
# per loop, which is operationally unviable for mean-reversion signals that
# decay in minutes. Limit to top N by quality score (R² × short-HL × low-Hurst).
# After candle cache warms up, scans are <30s regardless. But primer scan
# benefits hugely from this filter.
#   150 → primer scan ~5 min, scans cacheados ~3-5s
#   100 → primer scan ~3 min, mismo cache behavior
#   500 → primer scan ~15 min (efectivamente sin filtro)
#     0 → desactivado (sin filtro, escanea todos)
MAX_COINT_PAIRS_TO_SCAN = 150

# Thresholds - Closing
CLOSE_AT_ZSCORE_CROSS = True

# ===== Trailing Take-Profit =====
# En lugar de cerrar el TP en z=Z_TP fijo, se sigue el spread hasta el peak
# y se cierra cuando rebota TRAIL_Z_PULLBACK unidades desde el mejor z visto.
# Esto captura más del overshoot natural de spreads cointegrados.
#
# Ejemplo: entramos a z=2.0. Z baja a 0.3 (best_z=0.3), luego rebota a 0.6:
#   → cerramos en 0.6 en lugar de 0.7 → capturamos 1.4 unidades en vez de 1.3
# Ejemplo: Z baja a 0.1 (floor_hit=True) → cerramos inmediatamente sin esperar rebote.
TRAIL_TP_ENABLED = False  # Backtest: fixed TP z≤0.7 es significativamente mejor (+$280 diferencia en 5 pares)
TRAIL_Z_PULLBACK = 0.3   # cerrar cuando |z| rebota 0.3 desde el mejor z visto
TRAIL_Z_FLOOR = 0.15     # si |z| llega aquí, cerrar inmediatamente (no esperar rebote)

# ===== Post-SL Cooldown =====
# Cuando Z_SL o HARD_SL cierra un par, se bloquea re-entrada por un mínimo de
# SL_COOLDOWN_MIN_HOURS, o hasta half_life × SL_COOLDOWN_HALFLIFE_MULT (el mayor).
# Evita el patrón observado: PENDLE/ETHFI cerrado por SL y re-abierto 9 min después.
SL_COOLDOWN_ENABLED = True
SL_COOLDOWN_MIN_HOURS = 2.0          # mínimo 2h de cooldown tras SL
SL_COOLDOWN_HALFLIFE_MULT = 0.75     # o 75% del half_life si es mayor

# Wallet Address
#testnet: WALLET_ADDRESS = "dydx1napkzyjp3rauk5p787r9sfhvs74r8e357a30n5"
WALLET_ADDRESS = "dydx1svqmveffvuan4p3w6r3kgc474hn07nqdrdue48"

# WALLET PRIVATE KEY
API_KEY = config("API_KEY")

# URL ADDRESS TESTNET
NODE_TESTNET = "test-dydx-grpc.kingnodes.com"
INDEXER_TESTNET = "https://indexer.v4testnet.dydx.exchange"
WEBSOCKET_TESTNET = "wss://indexer.v4testnet.dydx.exchange/v4/ws"

# URL ADDRESS MAINNET
# 2026-05-26: INDEXER sin /v4 al final — la librería dydx-v4-client lo agrega
# automáticamente. Si pones /v4 aquí, queda /v4/v4/... y devuelve 404.
#
# 2026-06-01: NODE cambiado de "dydx-ops-grpc.kingnodes.com" a
# "dydx-grpc.publicnode.com". Hipótesis del bug "4 días sin trades":
# el endpoint "ops" pertenece a una red distinta de la mainnet real
# (dydx-mainnet-1). El indexer mainnet veía los fondos pero NUNCA veía
# las órdenes porque iban a otra cadena → 100% NOT_FOUND en audit.
# publicnode.com es endpoint público oficial de dYdX mainnet.
# Backups (descomenta si publicnode falla):
#   "dydx-mainnet-grpc.kingnodes.com"     ← kingnodes pero "mainnet", no "ops"
#   "dydx-grpc.lavenderfive.com"          ← community pública
NODE_MAINNET = "dydx-grpc.publicnode.com"
INDEXER_MAINNET = "https://indexer.dydx.trade"
WEBSOCKET_MAINNET = "wss://indexer.dydx.trade/v4/ws"

# URL Selection
NODE = NODE_MAINNET if MODE == "PRODUCTION" else NODE_TESTNET
INDEXER = INDEXER_MAINNET if MODE == "PRODUCTION" else INDEXER_TESTNET
WEBSOCKET = WEBSOCKET_MAINNET if MODE == "PRODUCTION" else WEBSOCKET_TESTNET

UNMANAGED_CLOSE_MAX_ATTEMPTS = 2
UNMANAGED_ALERT_COOLDOWN_SECONDS = 300

# Markets con posición atascada que el abort no puede cerrar en testnet.
# Ignorarlos evita que bloqueen entradas indefinidamente.
# Vaciar cuando se cierren manualmente desde la UI de dYdX.

# ===== Pair Quality Filters =====
# Aplicados durante la generación del CSV (func_cointegration.py) Y en el scan
# de entrada (func_entry_pairs.py) como segunda línea de defensa.
#
# HEDGE RATIO: pares con ratio fuera del rango [10^-LOG_MAX, 10^LOG_MAX]
# son casi siempre espurios — el tamaño de posición resultante es microscópico
# o astronómico, y el z-score no tiene significado económico real.
# Log10 = 3.5 → rango válido: [0.000316, 3162]
# Ejemplos filtrados: BTC/SHIB (hedge=12.8B), ETH/BLUR (88K), BTC/POL (822K)
# Ejemplos permitidos: WIF/ENA (1.81), AVAX/ENS (1.44), LINK/MANA (107)
HEDGE_RATIO_LOG_MAX = 3.5    # máximo log10 del |hedge ratio|

# HURST EXPONENT: filtra spreads que NO son mean-reverting.
# IMPORTANTE: se aplica a las DIFERENCIAS del spread (diff = spread[t] - spread[t-1]).
# Sobre las diferencias, un spread mean-reverting tiene autocorrelación negativa → H < 0.5.
# Un random walk tiene diferencias i.i.d. → H ≈ 0.5-0.58.
#
# Calibración empírica sobre spreads AR(1) sintéticos a 1H:
#   half_life=4h  → H_diff=0.265   <- PASA (claramente mean-reverting)
#   half_life=24h → H_diff=0.488   <- PASA (barely, al límite)
#   Random walk   → H_diff=0.579   <- RECHAZADO
# Threshold 0.52 acepta todos los spreads cointegrados con HL <= MAX_HALF_LIFE=24h.
HURST_MAX = 0.52             # rechazar spread si H_diff >= este valor (sobre diferencias)
HURST_MIN_BARS = 40          # mínimo de barras para calcular Hurst confiablemente

# ===== Market Blacklist =====
# Mercados que jamás deben usarse como leg en ningún par.
# Criterio de inclusión: profit_factor < 0.5 en backtests de ≥3 trades,
# o mercados conocidos por liquidez anómala / pegging no estable.
#
# Backtests (grid search 192 combos, 30 pares, 30 días, z=3):
#   PENDLE-USD: 5 pares en bottom-10, pérdida combinada ≈ -$1,148
#   ETC-USD/MNT-USD: 0% WR, -$32 (solo 2 trades, evidencia débil)
#
# Vaciar solo si cointegración se rehace y el mercado muestra PF>1.0.
MARKET_BLACKLIST = {
    "PENDLE-USD",   # tóxico: 5 pares en bottom-10, -$1,148 combinado (backtest 2026-05)
}

UNMANAGED_IGNORE_MARKETS = set()
# 2026-05-26: vaciado al pasar a mainnet. Los 10 markets que estaban aquí eran
# todos posiciones atascadas en testnet sin liquidez. En mainnet no aplica.
# Si en mainnet aparece exposición que el bot no puede cerrar (raro), agregar
# manualmente aquí.