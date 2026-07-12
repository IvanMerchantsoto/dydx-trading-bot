#!/usr/bin/env python3
"""
test_audit_fixes.py — Tests OFFLINE de las correcciones de la auditoría (F1-F3).

Cubre exactamente los defectos que el informe de fase 1 dijo que "deberían
fallar" antes del fix:
  E2  precio taker acotado (no banda ±5%) + protección de tope vs oráculo
  E1  gestión local de sequence activada
  E3  precio de entrada = fill real (avg_entry_price)  [comportamiento en entry]
  R2/R4  kill-switch de pérdida absoluta persistente
  R3  MAX_TRADES_PER_MARKET == 1

Correr:  ../.venv/bin/python test_audit_fixes.py
"""
import os
import sys
import tempfile

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"[PASS] {name}  {detail}")
    else:
        FAIL += 1
        print(f"[FAIL] {name}  {detail}")


# ── E2: precio taker acotado ────────────────────────────────────────────────
from func_private import _bounded_taker_price
import constants as C

# BUY dentro del book: límite = ask×(1+tol), source book
px, src = _bounded_taker_price("BUY", 100.0, 99.5, 100.5, 40)
check("E2 buy within book uses ask+tol", abs(px - 100.5 * 1.004) < 1e-6 and src == "book",
      f"px={px:.4f} src={src}")

# BUY con book roto (ask 8% arriba, tope 500bps): límite CAPADO por oráculo,
# quedando POR DEBAJO del ask → la IOC no llenará (protección E2).
px, src = _bounded_taker_price("BUY", 100.0, 99.0, 108.0, 40)
check("E2 broken book capped below ask (no fill)", px <= 100.0 * (1 + C.MARKET_SLIPPAGE_ORACLE_CAP_BPS/1e4) + 1e-9 and px < 108.0,
      f"px={px:.4f} (ask=108, cap={100*(1+C.MARKET_SLIPPAGE_ORACLE_CAP_BPS/1e4):.2f})")

# SELL dentro del book: límite = bid×(1-tol)
px, src = _bounded_taker_price("SELL", 100.0, 99.5, 100.5, 40)
check("E2 sell within book uses bid-tol", abs(px - 99.5 * 0.996) < 1e-6, f"px={px:.4f}")

# Sin book: fallback a oráculo
px, src = _bounded_taker_price("BUY", 100.0, None, None, 40)
check("E2 no book falls back to oracle", src == "oracle" and abs(px - 100.4) < 1e-6, f"px={px:.4f} src={src}")

# El límite NUNCA debe ser la vieja banda ±5% del oráculo con touch tight.
check("E2 no more fixed 5% band", abs(_bounded_taker_price("BUY", 100.0, 99.9, 100.1, 40)[0] - 105.0) > 1.0,
      "límite tight ≪ oracle×1.05")

# ── E1: gestión local de sequence activada ──────────────────────────────────
check("E1 LOCAL_SEQUENCE_MANAGEMENT enabled", C.LOCAL_SEQUENCE_MANAGEMENT is True)

# ── R3: concentración ────────────────────────────────────────────────────────
check("R3 MAX_TRADES_PER_MARKET == 1", C.MAX_TRADES_PER_MARKET == 1, f"={C.MAX_TRADES_PER_MARKET}")

# slippage: entry < exit < flatten < cap
check("E2 slippage tiers ordenados",
      C.MARKET_MAX_SLIPPAGE_BPS_ENTRY < C.MARKET_MAX_SLIPPAGE_BPS_EXIT
      < C.MARKET_MAX_SLIPPAGE_BPS_FLATTEN <= C.MARKET_SLIPPAGE_ORACLE_CAP_BPS,
      f"{C.MARKET_MAX_SLIPPAGE_BPS_ENTRY}/{C.MARKET_MAX_SLIPPAGE_BPS_EXIT}/"
      f"{C.MARKET_MAX_SLIPPAGE_BPS_FLATTEN}/cap{C.MARKET_SLIPPAGE_ORACLE_CAP_BPS}")

# ── Universo: techo DURO de liquidez en entrada ──────────────────────────────
check("LIQ MAX_ENTRY_LEG_SPREAD_BPS == 40", C.MAX_ENTRY_LEG_SPREAD_BPS == 40,
      f"={C.MAX_ENTRY_LEG_SPREAD_BPS}")
check("LIQ ceiling >= floor", C.MAX_ENTRY_LEG_SPREAD_BPS >= C.SPREAD_GATE_PER_LEG_FLOOR_BPS)
import func_bot_agent as BA
check("LIQ ceiling efectivo = min(config, 40)",
      abs(BA._EFFECTIVE_ENTRY_CEILING_BPS - min(float(C.SPREAD_GATE_PER_LEG_CEILING_BPS), 40.0)) < 1e-9,
      f"={BA._EFFECTIVE_ENTRY_CEILING_BPS}")

# ── R2/R4: kill-switch persistente ───────────────────────────────────────────
import func_kill_switch as K
# aislar el estado en un tmp file Y silenciar log_event para NO contaminar el
# log de producción con eventos kill_switch_triggered de prueba (se veían en
# pilot_check con high-water $600/$560 que eran de estos tests).
K.log_event = lambda *a, **k: None
_tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
_tmp.close()
K.STATE_PATH = _tmp.name
try:
    K.reset_kill_switch(600.0)
    h0, _ = K.evaluate(600.0)
    check("R2 kill-switch no dispara en high-water", h0 is False)

    h1, _ = K.evaluate(595.0)   # -$5, bajo el límite $40/8%
    check("R2 kill-switch no dispara con pérdida pequeña", h1 is False)

    h2, info2 = K.evaluate(555.0)   # -$45 desde 600 → > $40
    check("R2 kill-switch dispara sobre límite absoluto", h2 is True and info2.get("triggered"),
          f"dd=${info2.get('drawdown_usd')}")

    check("R4 kill-switch halt persiste (idempotente)", K.is_halted() is True and K.evaluate(560.0)[0] is True)

    # recuperación de un equity subiendo NO debe des-detenerlo (requiere reset manual)
    check("R4 no auto-reset al recuperar", K.evaluate(600.0)[0] is True)

    K.reset_kill_switch(560.0)
    check("R4 reset manual limpia halt y re-ancla high-water",
          K.is_halted() is False and K.get_state().get("high_water_equity") == 560.0)

    # el % también dispara: high-water 560, -8% = -$44.8 → equity 515 dispara
    K.reset_kill_switch(560.0)
    h3, info3 = K.evaluate(514.0)
    check("R2 kill-switch dispara por % del high-water", h3 is True, f"dd%={info3.get('drawdown_pct')}")
finally:
    try:
        os.unlink(_tmp.name)
        if os.path.exists(_tmp.name + ".tmp"):
            os.unlink(_tmp.name + ".tmp")
    except Exception:
        pass

print(f"\n{'='*56}\n  Results: {PASS} passed, {FAIL} failed\n{'='*56}")
sys.exit(1 if FAIL else 0)
