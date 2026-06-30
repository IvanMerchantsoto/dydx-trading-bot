# CHANGELOG — dYdX v4 Stat-Arb Bot

Registro de todos los cambios implementados en cada sesión. Ordenado de más reciente a más antiguo.
Para hacer rollback, revertir los commits de la sección correspondiente.

---

## [2026-05-21] Sesión 3 — Capas 1/2/3: Calidad de Pares + Backtest IS/OOS + Infra

### Motivación
El bot corría pero no era rentable. Análisis del log `bot_stdout-4.log` reveló:
- 25 "drift_legs" (posiciones huérfanas en dYdX sin match en `bot_agents.json`) acumulando -$400+ en unrealized PnL
- La pierna WIF-USD quedó abierta después del cierre de MNT/WIF porque no había verificación post-cierre
- El filtro de hedge ratio en `cointegrated_pairs.csv` era inexistente: 21 de 114 pares tenían ratios imposibles (BTC/SHIB = 12.8B, BTC/PEPE = 19.7B)
- El filtro de Hurst se aplicaba sobre el spread en NIVEL (incorrecto → todos los spreads AR(1) parecen trending con H≈0.85-0.97)
- No había forma de validar overfitting del backtest

### Archivos modificados

#### `program/constants.py`
**Sección añadida:** `# ===== Pair Quality Filters =====`

```python
HEDGE_RATIO_LOG_MAX = 3.5    # Rechazar si |log10(hedge_ratio)| > 3.5 → rango válido: [0.000316, 3162]
HURST_MAX = 0.52             # Rechazar spread si H_diff >= 0.52 (trending/random walk)
HURST_MIN_BARS = 40          # Mínimo de barras para calcular Hurst con confianza
```

**Calibración de HURST_MAX (documentada en el archivo):**
- Aplicar Hurst sobre DIFERENCIAS del spread, no el nivel
- half_life=4h  → H_diff≈0.265 (fuerte mean-reversion, pasa filtro)
- half_life=24h → H_diff≈0.488 (borderline, pasa filtro)
- random walk   → H_diff≈0.579 (rechazado: 0.579 ≥ 0.52)
- AR(1) nivel   → H_diff≈0.82-0.97 (INCORRECTO — nunca usar nivel)

**Para hacer rollback:** eliminar las 3 constantes del bloque `# ===== Pair Quality Filters =====`

---

#### `program/func_cointegration.py`
**Cambio 1:** Nueva función `calculate_hurst_exponent(spread) -> float`

Implementa análisis R/S (Rescaled Range) de Hurst. Retorna float en [0,1], o `nan` si n < HURST_MIN_BARS.
- Usa fracciones del total de barras como lags (robusto a series de distinto tamaño)
- Fit log-log de R/S vs lag → pendiente = H
- Recortado a [0.0, 1.0]

**Cambio 2:** Filtro de hedge ratio en `store_cointegration_results`

```python
# Filter 2: Hedge ratio sanity
if hedge_ratio <= 0 or abs(np.log10(abs(hedge_ratio))) > HEDGE_RATIO_LOG_MAX:
    n_hedge_filtered += 1
    continue
```

**Cambio 3 (CRÍTICO):** Filtro de Hurst en `store_cointegration_results`

```python
# CORRECTO: diferenciar antes de calcular Hurst
hurst = calculate_hurst_exponent(np.diff(spread_arr))
```

Bug previo: se pasaba `spread_arr` (nivel) en lugar de `np.diff(spread_arr)`. Esto hacía que el filtro fuera inútil — todos los spreads AR(1) dan H>0.8 en nivel, independientemente de si revertan.

**Cambio 4:** Columna `hurst` añadida al CSV de salida

**Cambio 5:** Diagnósticos ampliados — ahora imprime 5 contadores de filtro + distribuciones de half_life y Hurst:
```
[COINT DIAGNOSTICS]
  Pairs tested:              ...
  Passed coint test:         ...
  → HL negative/explosive:   ...
  → HL ≤ 3h (noise):         ...
  → HL > Xh (slow):          ...
  → Hedge ratio extreme:     ...
  → Hurst ≥ 0.52 (trending): ...
  → Passed ALL filters ✓:    ...
```

**Para hacer rollback:** revertir a la versión sin `calculate_hurst_exponent`, sin filtros 2/3, sin columna `hurst` en CSV.

---

#### `program/func_entry_pairs.py`
**Cambio:** Filtro de hedge ratio en tiempo de ejecución (primera verificación antes de cualquier otra)

```python
if hedge_ratio <= 0 or abs(np.log10(abs(hedge_ratio))) > HEDGE_RATIO_LOG_MAX:
    _skip_hedge_ratio += 1
    log_event({"type": "signal_skip", "reason": "hedge_ratio_extreme", ...}, print_terminal=False)
    continue
```

Esto protege en runtime incluso si el CSV tiene pares que pasaron sin el nuevo filtro de cointegración (CSV viejo).

**Cambio adicional:** El filtro `price_ratio` ahora loguea los valores exactos (base, quote, price_ratio) en lugar de contar silenciosamente. Facilita diagnóstico de qué pares se bloquean.

**Para hacer rollback:** eliminar el bloque `if hedge_ratio <= 0 or ...` y revertir el log del price_ratio filter.

---

#### `program/func_exit_pairs.py`
**Cambio:** Bloque de verificación post-cierre añadido

Después de ejecutar las órdenes de cierre (SL/TP/HARD_SL), el bot:
1. Espera 2 segundos (`asyncio.sleep(2.0)`)
2. Consulta posiciones reales en dYdX via `indexer.account.get_subaccount`
3. Si alguna leg tiene size residual > min_size → reintenta la orden de cierre (reduce_only MARKET)
4. Loguea resultado: `post_close_verified_flat` o `post_close_residual_detected` + `post_close_retry_sent`
5. Si la consulta falla → loguea `post_close_verify_error` (no es fatal, continúa)

Este cambio resuelve el caso WIF-USD observado en `bot_stdout-4.log` donde la leg quedó abierta después de cerrar MNT/WIF.

**Para hacer rollback:** eliminar el bloque `# ── Post-close verification ──` completo.

---

#### `program/backtest.py`
**Cambio:** Train/test split (In-Sample / Out-of-Sample)

Parámetro nuevo: `test_split: float = 0.0` en `run_backtest()`

Uso en CLI:
```bash
python3 backtest.py --bars 1440 --test-split 0.3
# → 70% In-Sample (primeros 42 días) / 30% Out-of-Sample (últimos 18 días con warmup)
```

Nueva función `print_split_comparison(result)`:
- Muestra tabla IS vs OOS con: Trades, Win Rate, Profit Factor, EV/trade, Net PnL, Max DD, Sharpe
- Calcula EV decay: si > 40% → "⚠️ POSIBLE OVERFIT", si ≤ 40% → "✅ Generaliza bien"

**Para hacer rollback:** eliminar `--test-split` del argparse, eliminar `print_split_comparison`, revertir firma de `run_backtest()`.

---

### Cómo regenerar `cointegrated_pairs.csv` con los nuevos filtros

Los nuevos filtros (hedge ratio + Hurst) sólo afectan la generación del CSV.
Para actualizar el CSV, establecer en `.env`:
```
FIND_COINTEGRATED=True
```
Correr `python3 main.py` una sola vez, esperar a que genere el CSV, luego cambiar a:
```
FIND_COINTEGRATED=False
```

El nuevo CSV tendrá la columna `hurst` y habrá excluido:
- Pares con hedge ratio fuera de `[10^-3.5, 10^3.5]` = `[0.000316, 3162]`
- Pares con spread trending (H_diff ≥ 0.52)
- Pares en MARKET_BLACKLIST (ej: PENDLE)

---

### Checklist de validación antes de operar con dinero real

- [ ] Regenerar `cointegrated_pairs.csv` con `FIND_COINTEGRATED=True`
- [ ] Verificar que los diagnósticos de cointegración muestren 0 pares con hedge ratio extremo
- [ ] Correr `python3 backtest.py --test-split 0.3` y confirmar EV OOS > 0
- [ ] Verificar que el CSV no tenga PENDLE ni pares con hedge_ratio < 0.000316 o > 3162
- [ ] Correr `python3 -m pytest tests/ -v` y confirmar que todos los tests pasan
- [ ] Hacer una corrida en DEVELOPMENT con `MAX_OPEN_TRADES=2` y verificar logs de `post_close_verified_flat`
- [ ] Confirmar que `bot_agents.json` no tiene registros ORPHAN o NEEDS_RECONCILE al inicio

---

## [Sesiones previas — 2026-05-xx]

Los cambios de sesiones anteriores están documentados en el historial de git y en el código fuente.
Los archivos modificados en sesiones previas incluyen:
- `main.py` — dynamic sizing, drawdown circuit breaker, batch logic, auto-refresh cointegración
- `func_bot_agent.py` — flujo PREPARE→COMMIT→AUDIT→DECISION, ORPHAN handling
- `func_private.py` — place_market_order, polling de fills, cancel/abort
- `func_exit_pairs.py` — TP/SL/time-stop/hard-SL, profit gate fee-aware, maker exits
- `func_risk_off.py` — recalibración de score (pérdida domina, edad=0), RISK_OFF_MIN_AGE_HOURS
- `func_entry_pairs.py` — opportunity scoring, funding gate, min edge gate, dynamic sizing
- `func_public.py` — get_funding_rates(), manejo de candles vacíos
- `func_logging.py` — JSONL con trace_id
- `func_kpis.py` — KPIs de cuenta, sesión summary
- `func_messaging.py` — fix POST, mensajes agrupados
- `func_position_guard.py` — assert_safe_to_open, reconcile_bot_vs_dydx
- `constants.py` — múltiples nuevas constantes (ver historial de git)
- `close_all.py` — script de cierre controlado (nuevo archivo)
- `tests/` — suite de tests offline

Para ver el detalle de cada sesión anterior, consultar `git log --oneline` en el repositorio.
