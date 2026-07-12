# func_kill_switch.py
"""
Kill-switch de pérdida absoluta con high-water PERSISTENTE (auditoría R2/R4).

Problema que resuelve:
  El drawdown circuit breaker (main.py) sólo frena ENTRADAS y su baseline
  (equity_session_start) se resiembra en CADA arranque del proceso. Un
  crash-loop (o reinicios frecuentes en la VM) resetea el baseline y la
  pérdida acumulada nunca dispara nada.

Diseño:
  - Mantiene un "high-water equity" en disco (risk_state.json) que SOBREVIVE
    a los reinicios. La pérdida se mide contra ese máximo histórico, no contra
    el equity del arranque.
  - Cuando el equity cae por debajo del high-water más de
    KILL_SWITCH_MAX_DRAWDOWN_USD (absoluto) o _PCT (relativo) — lo que ocurra
    primero — se marca `halted=True` de forma persistente.
  - Estando `halted`, el caller (main.py) CIERRA todas las posiciones y bloquea
    nuevas entradas hasta un reset MANUAL (reset_kill_switch), evitando que un
    reinicio "olvide" que debía estar detenido.

Este módulo es PURO estado (no ejecuta órdenes). El cierre lo hace main.py
con close_markets_actual para mantener la responsabilidad de red en un lugar.
"""

import json
import os
from datetime import datetime, timezone

from func_logging import log_event
from constants import (
    KILL_SWITCH_ENABLED,
    KILL_SWITCH_MAX_DRAWDOWN_USD,
    KILL_SWITCH_MAX_DRAWDOWN_PCT,
)

STATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "risk_state.json")


def _now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_state() -> dict:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    tmp = STATE_PATH + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, STATE_PATH)
    except Exception as e:
        log_event({"type": "kill_switch_state_write_error", "error": str(e)}, print_terminal=False)


def is_halted() -> bool:
    """True si el kill-switch está disparado (persistente entre reinicios)."""
    if not KILL_SWITCH_ENABLED:
        return False
    return bool(_load_state().get("halted", False))


def get_state() -> dict:
    return _load_state()


def reset_kill_switch(current_equity: float = 0.0) -> dict:
    """
    Reset MANUAL tras revisar la causa. Re-ancla el high-water al equity actual
    y limpia el flag halted. Debe llamarse a mano (nunca automáticamente).
    """
    state = _load_state()
    state["halted"] = False
    state["halted_reason"] = None
    state["reset_at"] = _now_iso()
    if current_equity and current_equity > 0:
        state["high_water_equity"] = float(current_equity)
    _save_state(state)
    log_event({"type": "kill_switch_reset", "high_water_equity": state.get("high_water_equity")})
    return state


def evaluate(current_equity: float) -> tuple:
    """
    Actualiza el high-water y evalúa el kill-switch.

    Returns (halted: bool, info: dict).
      halted=True  → el caller DEBE cerrar todo y bloquear entradas.
    Idempotente: si ya estaba halted, sigue devolviendo True sin re-disparar.
    """
    if not KILL_SWITCH_ENABLED:
        return False, {"enabled": False}

    state = _load_state()

    # Si ya está detenido, mantenerlo detenido (persistente).
    if state.get("halted", False):
        return True, {"already_halted": True, "reason": state.get("halted_reason"),
                      "high_water_equity": state.get("high_water_equity")}

    if not current_equity or current_equity <= 0:
        return False, {"reason": "no_equity"}

    high_water = float(state.get("high_water_equity", 0.0) or 0.0)
    if current_equity > high_water:
        high_water = current_equity
    state["high_water_equity"] = high_water
    state["last_equity"] = float(current_equity)
    state["last_update"] = _now_iso()

    drawdown_usd = high_water - current_equity
    drawdown_pct = (drawdown_usd / high_water) if high_water > 0 else 0.0

    trigger_usd = drawdown_usd >= float(KILL_SWITCH_MAX_DRAWDOWN_USD)
    trigger_pct = drawdown_pct >= float(KILL_SWITCH_MAX_DRAWDOWN_PCT)

    info = {
        "high_water_equity": round(high_water, 2),
        "current_equity": round(current_equity, 2),
        "drawdown_usd": round(drawdown_usd, 2),
        "drawdown_pct": round(drawdown_pct * 100, 3),
        "limit_usd": float(KILL_SWITCH_MAX_DRAWDOWN_USD),
        "limit_pct": float(KILL_SWITCH_MAX_DRAWDOWN_PCT) * 100,
    }

    if trigger_usd or trigger_pct:
        reason = (
            f"drawdown ${drawdown_usd:.2f} ({drawdown_pct*100:.1f}%) desde high-water "
            f"${high_water:.2f} — límite ${KILL_SWITCH_MAX_DRAWDOWN_USD:.0f} / "
            f"{KILL_SWITCH_MAX_DRAWDOWN_PCT*100:.0f}%"
        )
        state["halted"] = True
        state["halted_reason"] = reason
        state["halted_at"] = _now_iso()
        _save_state(state)
        info["triggered"] = True
        info["reason"] = reason
        log_event({"type": "kill_switch_triggered", **info})
        return True, info

    _save_state(state)
    info["triggered"] = False
    return False, info
