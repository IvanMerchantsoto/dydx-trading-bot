#!/usr/bin/env python3
"""
fx_validate.py — ¿Tiene EDGE la reversión de pares cointegrados en FX?

Responde la pregunta que mató a dYdX, GRATIS y sin bróter/cuenta/capital:
  1) ¿La cointegración PERSISTE en FX majors? (en dYdX solo ~5% de folds la
     mantenían → señal espuria). Métrica clave = % de folds que re-pasan
     cointegración al re-testear en cada ventana (walk-forward honesto).
  2) ¿Hay EV neto > 0 out-of-sample con costes FX realistas
     (spread ~1-4 bps + carry)?

Datos: diarios gratis de stooq (años de historia). SOLO LECTURA, no opera.
NO toca el bot de dYdX (directorio aislado).

Uso:
    python3 fx/fx_validate.py
    python3 fx/fx_validate.py --cost-bps-per-leg 3 --z-entry 2.0 --train 252 --test 63
"""
import argparse
import itertools
from datetime import datetime

import numpy as np
import pandas as pd

from fx_data import DEFAULT_UNIVERSE, load_universe, align_by_date
from coint import calculate_cointegration, calculate_hurst_exponent, zscore

# Filtros de selección (por fold), análogos al bot pero en días.
HL_MIN_D, HL_MAX_D = 2.0, 60.0     # half-life en días
HURST_MAX = 0.55
HEDGE_LOG_MAX = 3.5


def train_select(p1_tr, p2_tr):
    if len(p1_tr) < 60:
        return False, None, None
    flag, hr, hl, r2, p = calculate_cointegration(p1_tr, p2_tr)
    if flag != 1:
        return False, None, None
    if hl is None or np.isnan(hl) or hl < HL_MIN_D or hl > HL_MAX_D:
        return False, None, None
    if hr <= 0 or abs(np.log10(abs(hr))) > HEDGE_LOG_MAX:
        return False, None, None
    spread = np.asarray(p1_tr) - hr * np.asarray(p2_tr)
    hu = calculate_hurst_exponent(np.diff(spread))
    if not np.isnan(hu) and hu >= HURST_MAX:
        return False, None, None
    return True, float(hr), float(hl)


def simulate(p1, p2, te0, te1, hr, usd, cost_bps_leg, carry_bps_day, prm):
    spread = p1 - hr * p2
    zs = zscore(spread, prm["window"])
    trades = []
    state = "flat"; ez = 0.0; ei = 0; ep1 = ep2 = 0.0; side = "short"; conf = 0
    notional = 2.0 * usd
    exec_cost = 4.0 * usd * (cost_bps_leg / 10_000.0)
    lo = max(te0, prm["window"] + 1)
    for i in range(lo, te1):
        z = zs[i]
        if z is None or (isinstance(z, float) and np.isnan(z)):
            continue
        if state == "flat":
            if abs(z) >= prm["z_entry"]:
                state = "in"; ez = z; ei = i; ep1 = p1[i]; ep2 = p2[i]
                side = "short" if z > 0 else "long"; conf = 0
            continue
        hold = i - ei
        s1 = usd / ep1; s2 = usd / ep2
        if side == "short":
            pnl = (ep1 - p1[i]) * s1 + (p2[i] - ep2) * s2
        else:
            pnl = (p1[i] - ep1) * s1 + (ep2 - p2[i]) * s2
        carry = carry_bps_day / 10_000.0 * notional * hold
        net = pnl - exec_cost - carry
        reason = None
        if abs(z) >= abs(ez) + prm["z_sl_delta"]:
            reason = "Z_SL"
        elif ((ez > 0.1 and z < -0.1) or (ez < -0.1 and z > 0.1)) and net > 0:
            reason = "TP_CROSS0"
        else:
            conf = conf + 1 if abs(z) <= prm["z_tp"] else 0
            if conf >= prm["tp_confirm"] and net > 0:
                reason = "TP"
        if reason is None and hold >= prm["time_stop"]:
            reason = "TIME"
        if reason:
            trades.append({"net": net, "gross": pnl, "hold": hold, "reason": reason})
            state = "flat"; conf = 0
    return trades


def main():
    ap = argparse.ArgumentParser(description="Validador de cointegración FX (walk-forward, gratis)")
    ap.add_argument("--train", type=int, default=252, help="Días de entrenamiento por fold (~1y)")
    ap.add_argument("--test", type=int, default=63, help="Días de test por fold (~1 trimestre)")
    ap.add_argument("--window", type=int, default=20, help="Ventana z (días)")
    ap.add_argument("--z-entry", type=float, default=2.0)
    ap.add_argument("--z-tp", type=float, default=0.5)
    ap.add_argument("--z-sl-delta", type=float, default=1.5)
    ap.add_argument("--tp-confirm", type=int, default=1)
    ap.add_argument("--time-stop", type=int, default=90, help="Time stop en días")
    ap.add_argument("--usd", type=float, default=1000.0)
    ap.add_argument("--cost-bps-per-leg", type=float, default=2.0, help="FX majors ~0.5-2, crosses ~2-4")
    ap.add_argument("--carry-bps-day", type=float, default=1.0, help="Drag de swap/carry (bps/día)")
    ap.add_argument("--symbols", default=None, help="CSV de símbolos (default: majors+crosses)")
    args = ap.parse_args()

    prm = {"window": args.window, "z_entry": args.z_entry, "z_tp": args.z_tp,
           "z_sl_delta": args.z_sl_delta, "tp_confirm": args.tp_confirm, "time_stop": args.time_stop}
    universe = [s.strip() for s in args.symbols.split(",")] if args.symbols else DEFAULT_UNIVERSE

    print(f"\n{'='*72}\n  VALIDADOR COINTEGRACIÓN FX (walk-forward diario, gratis)\n{'='*72}")
    print(f"  universo={len(universe)}  train={args.train}d test={args.test}d z_win={args.window}d")
    print(f"  z_entry={args.z_entry} z_tp={args.z_tp}  cost={args.cost_bps_per_leg}bps/pierna "
          f"carry={args.carry_bps_day}bps/día\n")

    # Descarga (una sola llamada al BCE/Frankfurter, cacheada)
    data = load_universe(universe)
    for sym in universe:
        s = sym.lower().strip()
        print(f"   {s}: {len(data.get(s, [])) } días")
    syms = [s.lower().strip() for s in universe if s.lower().strip() in data]
    if len(syms) < 2:
        print("  ❌ No hay datos suficientes.")
        return

    # Walk-forward sobre todos los pares
    all_trades = []
    fold_opportunities = 0
    selections = 0
    pairs_tested = 0
    persist_by_pair = {}
    for a, b in itertools.combinations(syms, 2):
        dates, p1, p2 = align_by_date(data[a], data[b])
        n = len(p1)
        if n < args.train + args.test + 5:
            continue
        pairs_tested += 1
        opp = 0; sel = 0
        start = 0
        while start + args.train + args.test <= n:
            tr0, tr1 = start, start + args.train
            te0, te1 = tr1, min(tr1 + args.test, n)
            opp += 1; fold_opportunities += 1
            passes, hr, hl = train_select(p1[tr0:tr1], p2[tr0:tr1])
            if passes:
                sel += 1; selections += 1
                all_trades.extend(simulate(p1, p2, te0, te1, hr, args.usd,
                                           args.cost_bps_per_leg, args.carry_bps_day, prm))
            start += args.test
        if opp:
            persist_by_pair[f"{a}/{b}"] = (sel, opp)

    # ── Métrica CLAVE: persistencia de cointegración ──
    persist_rate = (selections / fold_opportunities * 100) if fold_opportunities else 0.0
    print(f"\n{'─'*72}\n  1) PERSISTENCIA DE COINTEGRACIÓN (la pregunta que mató a dYdX)\n{'─'*72}")
    print(f"  Pares testeados:            {pairs_tested}")
    print(f"  Oportunidades de fold:      {fold_opportunities}")
    print(f"  Folds que RE-pasan coint.:  {selections}")
    print(f"  ➤ Tasa de persistencia:     {persist_rate:.1f}%   (dYdX altcoins ≈ 5%)")
    top = sorted(persist_by_pair.items(), key=lambda kv: kv[1][0] / max(1, kv[1][1]), reverse=True)[:8]
    print(f"  Pares más persistentes:")
    for pr, (s, o) in top:
        print(f"     {pr:<14} {s}/{o} folds ({100*s/o:.0f}%)")

    # ── OOS con costes ──
    print(f"\n{'─'*72}\n  2) RENTABILIDAD OOS (walk-forward, con costes FX)\n{'─'*72}")
    if not all_trades:
        print("  0 trades — sin señal tradeable (o datos insuficientes).")
        return
    df = pd.DataFrame(all_trades)
    n = len(df); net = df["net"]
    wins = net[net > 0]; losses = net[net <= 0]
    wr = len(wins) / n * 100
    pf = wins.sum() / abs(losses.sum()) if len(losses) and losses.sum() != 0 else float("inf")
    ev = net.mean()
    # Sharpe anualizado aproximado (retorno por trade × trades/año). Los folds
    # cubren ~ (fold_opportunities×test) días naturales del histórico.
    span_days = fold_opportunities * args.test if fold_opportunities else 252
    years = max(span_days / 365.0, 1e-6)
    r = net / args.usd
    sharpe = (r.mean() / r.std() * np.sqrt(n / years)) if r.std() > 0 else 0.0
    print(f"  Trades:         {n}   ({n/years:.0f}/año aprox)")
    print(f"  Win rate:       {wr:.1f}%")
    print(f"  Profit factor:  {pf:.2f}")
    print(f"  EV / trade:     ${ev:.4f}")
    print(f"  NET total:      ${net.sum():.2f}   (sobre ${args.usd:.0f}/pierna)")
    print(f"  Sharpe (aprox): {sharpe:.2f}")
    print(f"  Avg hold:       {df['hold'].mean():.1f} días")
    print(f"  Cierres:        {df['reason'].value_counts(normalize=True).mul(100).round(0).to_dict()}")

    # ── Veredicto ──
    print(f"\n{'─'*72}\n  VEREDICTO\n{'─'*72}")
    edge = (ev > 0 and net.sum() > 0 and n >= 30)
    persists = persist_rate >= 20.0
    if persists and edge:
        print(f"  ✅ FX PROMETE: cointegración persiste ({persist_rate:.0f}%) Y EV>0 con costes.")
        print(f"     Siguiente paso: cuenta DEMO de bróker (OANDA) + adaptador de ejecución")
        print(f"     y re-validar en vivo (paper) con reconcile antes de cualquier capital.")
    elif persists and not edge:
        print(f"  ⚠️  Cointegración persiste ({persist_rate:.0f}%) pero EV≤0 a {args.cost_bps_per_leg}bps.")
        print(f"     Prueba costes menores / otros pares / otra geometría antes de decidir.")
    elif not persists:
        print(f"  🛑 Cointegración NO persiste ({persist_rate:.0f}%) — mismo problema que dYdX.")
        print(f"     La reversión de pares FX tampoco sería un edge fiable aquí.")
    if n < 30:
        print(f"  (n={n} < 30: muestra chica, trátalo como indicativo, no concluyente.)")
    print(f"\n  Recordatorio: gross al cierre diario, ejecución idealizada. Es sensibilidad")
    print(f"  relativa; el juez real será el paper-trade en vivo con reconcile.\n")


if __name__ == "__main__":
    main()
