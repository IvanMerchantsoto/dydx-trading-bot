#!/usr/bin/env python3
"""
pilot_check.py — Verificador de salud del PILOTO (post-auditoría).

Revisa el log del bot y comprueba que las correcciones de la auditoría se
activaron correctamente en los trades del piloto:
  E1  legging / sequence : ¿rechazos de cadena? ¿re-syncs? ¿retries de pierna?
                           ¿fills con AMBAS piernas o una sola?
  E2  slippage           : ¿el precio salió del book o del oráculo? ¿qué slippage?
  E3  PnL veraz          : ¿se recomputó pnl_gross con fills reales? delta slippage
  Liq techo 40bps        : ¿cuántas entradas bloqueó el techo de liquidez?
  Kill-switch            : estado persistente (risk_state.json)

Además traza el ciclo de vida COMPLETO del último trade LIVE.

SOLO LECTURA. Correr en la VM:
    python3 pilot_check.py
    python3 pilot_check.py --log logs/bot_run.log.jsonl --trades 10
"""

import argparse
import json
import os
from collections import Counter, defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent


def load_events(paths):
    evs = []
    for p in paths:
        if not os.path.exists(p):
            continue
        with open(p, errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    evs.append(json.loads(line))
                except Exception:
                    continue
    evs.sort(key=lambda j: j.get("ts", ""))
    return evs


def sec(t):
    print(f"\n{'='*72}\n  {t}\n{'='*72}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", action="append", default=None)
    ap.add_argument("--trades", type=int, default=10, help="Nº de trade_closed a detallar")
    args = ap.parse_args()
    paths = args.log or [str(SCRIPT_DIR / "bot_run.log.jsonl"),
                         str(SCRIPT_DIR / "logs" / "bot_run.log.jsonl")]

    evs = load_events(paths)
    if not evs:
        print("No hay eventos. ¿Ruta del log correcta?")
        return
    types = Counter(e.get("type") for e in evs)
    ts0 = next((e.get("ts") for e in evs if e.get("ts")), "?")
    ts1 = next((e.get("ts") for e in reversed(evs) if e.get("ts")), "?")

    sec("1. RANGO Y VOLUMEN")
    print(f"  Eventos: {len(evs):,}   Rango: {ts0} → {ts1}")
    print(f"  Restarts (session_equity_seeded): {types.get('session_equity_seeded',0)}")

    # ── E1: salud de ejecución / sequence ────────────────────────────────
    sec("2. E1 — LEGGING / SEQUENCE (debe estar limpio)")
    rej = [e for e in evs if e.get("type") == "order_chain_rejected"]
    rej_codes = Counter(e.get("tx_code") for e in rej)
    print(f"  order_chain_rejected:   {len(rej)}   codes={dict(rej_codes)}")
    for e in rej[:3]:
        print(f"     - {e.get('market')} code={e.get('tx_code')} log={(str(e.get('raw_log'))[:90])}")
    print(f"  sequence_resync:        {types.get('sequence_resync',0)}   "
          f"(self-heal; alto = problema de sequence)")
    print(f"  sequence_resync_error:  {types.get('sequence_resync_error',0)}")
    print(f"  commit_leg_retry:       {types.get('commit_leg_retry',0)}   "
          f"(retry de pierna rechazada)")
    fills = [e for e in evs if e.get("type") == "fills"]
    combos = Counter((e.get("status_1"), e.get("status_2")) for e in fills)
    print(f"  fills combos (status_1,status_2):")
    for c, n in combos.most_common():
        both = c[0] == "FILLED" and c[1] == "FILLED"
        one_leg = ("FILLED" in c) and not both
        tag = "  ✅ ambas" if both else ("  ⚠️ UNA sola pierna (legging)" if one_leg else "")
        print(f"     {c}: {n}{tag}")
    er = Counter(e.get("pair_status") for e in evs if e.get("type") == "entry_result")
    print(f"  entry_result: {dict(er)}")
    residuals = [e for e in evs if e.get("type") == "residual"]
    if residuals:
        deviations = [float(e.get("ratio_deviation_pct", 0) or 0) for e in residuals]
        print(f"  hedge ratio deviation: max={max(deviations):.2f}%  "
              f"avg={sum(deviations)/len(deviations):.2f}%  n={len(deviations)}")

    # ── E2: slippage / precio ────────────────────────────────────────────
    sec("3. E2 — PRECIO ACOTADO (book vs oráculo)")
    txm = [e for e in evs if e.get("type") == "tx_market"]
    src = Counter(e.get("px_source") for e in txm)
    slips = Counter(e.get("max_slippage_bps") for e in txm)
    print(f"  tx_market: {len(txm)}   px_source={dict(src)}   (book = usó orderbook)")
    print(f"  slippage bps usados: {dict(slips)}")

    # ── Liquidez ─────────────────────────────────────────────────────────
    sec("4. TECHO DE LIQUIDEZ 40bps + gates de entrada")
    sc = [e for e in evs if e.get("type") == "spread_check"]
    sc_block = [e for e in sc if e.get("passed") is False or e.get("ok") is False]
    rules = Counter(e.get("rule") for e in sc)
    print(f"  spread_check: {len(sc)}   por regla: {dict(rules)}")
    ceil_blocks = [e for e in sc if "CEILING" in str(e.get("reason", "")) or e.get("rule") == "ceiling_block"]
    print(f"  bloqueos por ceiling (liquidez): {len(ceil_blocks)}")
    ss = Counter(e.get("reason") for e in evs if e.get("type") == "signal_skip")
    if ss:
        print(f"  signal_skip top: {dict(Counter(dict(ss.most_common(6))))}")

    # ── Kill-switch ──────────────────────────────────────────────────────
    sec("5. KILL-SWITCH (estado persistente)")
    ks_path = SCRIPT_DIR / "risk_state.json"
    if ks_path.exists():
        st = json.load(open(ks_path))
        print(f"  risk_state.json: halted={st.get('halted')}  "
              f"high_water=${st.get('high_water_equity')}  last=${st.get('last_equity')}")
        if st.get("halted"):
            print(f"  🛑 HALTED: {st.get('halted_reason')}  (entradas BLOQUEADAS hasta reset)")
    else:
        print("  risk_state.json no existe todavía (se crea en el primer KPI).")
    for e in [e for e in evs if e.get("type") == "kill_switch_triggered"]:
        print(f"  ⚠️ kill_switch_triggered: {e.get('reason')}")

    # ── E3 + cierres ─────────────────────────────────────────────────────
    sec("6. E3 — CIERRES y PnL reconciliado a fills")
    closes = [e for e in evs if e.get("type") == "trade_closed"]
    recon = [e for e in evs if e.get("type") == "pnl_gross_reconciled_to_fills"]
    entry_recon = [e for e in evs if e.get("type") == "entry_fill_reconciled"]
    print(f"  trade_closed: {len(closes)}   pnl_gross_reconciled_to_fills: {len(recon)}")
    print(f"  entry_fill_reconciled (fills tardíos): {len(entry_recon)}")
    tot_net = sum(float(e.get("net_pnl_est", 0) or 0) for e in closes)
    print(f"  Σ net_pnl_est interno (log): ${tot_net:+.4f}")
    print(f"  ⚠️ El interno NO es la verdad: corre reconcile_pnl.py --days 1")
    for e in closes[-args.trades:]:
        print(f"     - {e.get('market_1')}/{e.get('market_2')}  "
              f"reason={str(e.get('close_reason'))[:48]}")
        print(f"         gross=${float(e.get('pnl_gross',0) or 0):+.4f}  "
              f"mark_decision=${float(e.get('pnl_gross_mark_at_decision',0) or 0):+.4f}  "
              f"exec_decision=${float(e.get('pnl_gross_executable_at_decision',0) or 0):+.4f}  "
              f"net_est=${float(e.get('net_pnl_est',0) or 0):+.4f}  "
              f"fees=${float(e.get('open_fees',0) or 0)+float(e.get('close_fees_est',0) or 0):.4f}  "
              f"prov={e.get('pnl_provisional')}")
    for e in recon[-args.trades:]:
        print(f"     [slippage real] {e.get('market_1')}/{e.get('market_2')}: "
              f"gross_oracle=${e.get('pnl_gross_oracle')} → gross_fills=${e.get('pnl_gross_real_fills')} "
              f"(delta ${e.get('slippage_delta')})")

    # ── Thesis expiry / convergence mismatch ─────────────────────────────
    sec("7. TESIS AGOTADA — CONVERGENCIA Y TIME STOP")
    cge = [e for e in evs if e.get("type") == "converged_loss_evaluated"]
    print(f"  converged_loss_evaluated: {len(cge)}")
    print(f"  CONVERGED_LOSS closes:    "
          f"{sum('CONVERGED_LOSS' in str(e.get('close_reason','')) for e in closes)}")
    print(f"  ADAPTIVE_TIME_STOP closes: "
          f"{sum('ADAPTIVE_TIME_STOP' in str(e.get('close_reason','')) for e in closes)}")
    for e in cge[-5:]:
        print(f"     - {e.get('m1')}/{e.get('m2')} progress={e.get('progress')} "
              f"age={e.get('age_hours')}h pnl=${e.get('pnl_gross')} "
              f"spreads={e.get('spread_1_bps')}/{e.get('spread_2_bps')}bps "
              f"liquid={e.get('liquid_enough')}")

    # ── Ciclo de vida del último trade LIVE ──────────────────────────────
    sec("8. CICLO DE VIDA DEL ÚLTIMO TRADE LIVE")
    live_traces = [e.get("trace_id") for e in evs
                   if e.get("type") == "entry_result" and e.get("pair_status") == "LIVE"]
    if not live_traces:
        print("  No hay entry_result LIVE en el log.")
    else:
        tid = live_traces[-1]
        print(f"  trace_id={tid}")
        want = {"entry_signal", "open_start", "spread_check", "tx_market", "commit_sent",
                "commit_leg_retry", "order_chain_rejected", "sequence_resync", "audit",
                "fills", "min_fill_gate", "open_live", "entry_result",
                "entry_fill_reconciled",
                "zscore_live", "trade_close_signal", "close_fees_polled",
                "pnl_gross_reconciled_to_fills", "post_close_verified_flat",
                "post_close_residual_detected", "trade_closed"}
        for e in [x for x in evs if x.get("trace_id") == tid and x.get("type") in want]:
            t = e.get("type")
            extra = ""
            if t == "spread_check":
                extra = f"rule={e.get('rule')} s1={e.get('spread_1_bps')} s2={e.get('spread_2_bps')} passed={e.get('passed')}"
            elif t == "tx_market":
                extra = f"{e.get('side')} {e.get('market')} src={e.get('px_source')} slip={e.get('max_slippage_bps')} seq={e.get('seq')}"
            elif t == "fills":
                extra = f"s1={e.get('status_1')} s2={e.get('status_2')}"
            elif t == "audit":
                extra = f"{e.get('market')} #{e.get('attempt')} {e.get('status')} filled={e.get('filled_size')}"
            elif t == "trade_closed":
                extra = f"{str(e.get('close_reason'))[:50]} net=${e.get('net_pnl_est')}"
            elif t == "trade_close_signal":
                extra = f"{str(e.get('close_reason'))[:50]}"
            print(f"     {e.get('ts','')[:19]}  {t:<28} {extra}")

    print()


if __name__ == "__main__":
    main()
