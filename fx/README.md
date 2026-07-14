# FX track — validar ANTES de arriesgar

Proyecto paralelo al bot dYdX (`program/`), **totalmente aislado**: no importa
nada de `program/`, no toca su config ni su estado. El bot dYdX sigue corriendo
sin cambios.

## Objetivo

Responder, **gratis y sin bróker/cuenta/capital**, la pregunta que hundió a
dYdX: *¿la cointegración de pares persiste y deja EV>0 neto de costes?* En FX
majors los spreads son ~1-4 bps (vs 40-200 en altcoins) y hay años de historia
diaria → aquí SÍ se puede validar de verdad.

## Fase 1 — validación (esto es lo que hay ahora)

```bash
# usar el venv (necesita pandas/numpy/statsmodels/requests)
~/DYDX/.venv/bin/python fx/fx_validate.py
# o con costes/params distintos:
~/DYDX/.venv/bin/python fx/fx_validate.py --cost-bps-per-leg 3 --z-entry 2.0
```

Datos: diarios gratis de stooq (sin API key). Salida:
1. **Tasa de persistencia de cointegración** (dYdX ≈ 5%; buscamos ≫ eso).
2. **EV/PnL out-of-sample** con costes FX realistas (spread + carry).
3. Veredicto: si persiste Y EV>0 → pasar a Fase 2; si no, paramos aquí (gratis).

Archivos:
- `coint.py` — matemática de cointegración auditada (sin deps del bot).
- `fx_data.py` — descarga FX diaria de stooq + caché.
- `fx_validate.py` — walk-forward honesto + veredicto.

## Fase 2 — solo si Fase 1 valida (aún NO)

Cuenta **DEMO** de bróter (OANDA v20 recomendado: API REST limpia, cuenta
práctica gratis, historia intradía). Adaptador de ejecución que reutiliza el
core + herramientas de auditoría (reconcile, kill-switch). Paper-trade medido
con reconcile antes de un solo dólar real.

> Regla que traemos de la auditoría dYdX: el juez es el PnL real reconciliado,
> nunca el backtest ni la contabilidad interna.
