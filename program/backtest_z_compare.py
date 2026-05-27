#!/usr/bin/env python3
"""
backtest_z_compare.py
=====================
Compara ZSCORE_THRESH = {2.5, 2.7, 2.8, 3.0} sobre el mismo universe y ventana
de datos, dejando el resto de parámetros en sus valores actuales de constants.py.

Objetivo: decidir empíricamente si el bot debe relajar el threshold de entrada
para aumentar la frecuencia de trading (utilización de capital) sin destruir
el edge por trade.

Uso:
    cd /home/ivan_merchant_sh/DYDX/program   (o donde esté el proyecto)
    python3 backtest_z_compare.py

Opciones:
    --bars N       cuántas barras (1H) por mercado. Default 720 (30 días).
    --top N        usar solo los N primeros pares del CSV (más rápido).
    --test-split F validar OOS con F fracción al final (ej. 0.3 → 70/30).

Outputs:
    - Tabla comparativa en stdout con EV/WR/PF/n_trades/fee_drag/sharpe para cada z.
    - backtest_z_compare_results.csv con cada fila siendo una corrida.
"""

import argparse
import sys
from pathlib import Path

# Reuse the existing backtest module
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

import backtest as bt  # noqa: E402


def run_one(label: str, zscore_thresh: float, pairs_df, n_bars: int, test_split: float):
    """Run backtest for a single zscore_thresh, keep everything else default."""
    params = dict(bt.DEFAULT_PARAMS)
    params["zscore_thresh"] = float(zscore_thresh)
    print(f"\n{'='*70}")
    print(f"  Backtest z={zscore_thresh}  ({label})")
    print(f"{'='*70}")
    result = bt.run_backtest(pairs_df, params, n_bars=n_bars, verbose=False,
                             test_split=test_split)
    m = result["metrics"]
    m["zscore_thresh"] = float(zscore_thresh)
    m["label"] = label

    # If we have OOS, also collect those metrics
    if test_split > 0 and "oos_metrics" in result:
        oos = result["oos_metrics"]
        m["oos_n_trades"]      = oos["n_trades"]
        m["oos_win_rate"]      = oos["win_rate"]
        m["oos_profit_factor"] = oos["profit_factor"]
        m["oos_net_pnl"]       = oos["net_pnl"]
        m["oos_ev_per_trade"]  = oos["ev_per_trade"]
    return m


def fmt_row(m: dict) -> str:
    return (
        f"  z={m['zscore_thresh']:>4.2f}  "
        f"n={m['n_trades']:>4d}  "
        f"WR={m['win_rate']:>5.1f}%  "
        f"PF={m['profit_factor']:>5.2f}  "
        f"EV=${m['ev_per_trade']:>+7.3f}  "
        f"net=${m['net_pnl']:>+9.2f}  "
        f"fees=${m['total_fees']:>7.2f}  "
        f"fee_drag={m['fee_drag_pct']:>5.1f}%  "
        f"DD=${m['max_drawdown']:>+8.2f}  "
        f"Sharpe={m['sharpe']:>+5.2f}  "
        f"hold={m['avg_hold_bars']:>4.1f}h  "
        f"MAE={m['mae_avg']:>+6.2f}/p95={m['mae_p95']:>+6.2f}  "
        f"TP%={m['tp_pct']:>5.1f}  SL%={m['sl_pct']:>5.1f}"
    )


def main():
    p = argparse.ArgumentParser(description="Compare ZSCORE_THRESH = {2.5, 2.7, 2.8, 3.0}")
    p.add_argument("--bars", type=int, default=720,
                   help="Barras (1H) por mercado. Default 720 = 30 días.")
    p.add_argument("--top", type=int, default=None,
                   help="Solo los N primeros pares del CSV (para iteración rápida).")
    p.add_argument("--test-split", type=float, default=0.0,
                   help="Fracción OOS al final (0=off, 0.3=70/30).")
    p.add_argument("--thresholds", type=str, default="2.5,2.7,2.8,3.0",
                   help="Lista de z thresholds separados por coma.")
    args = p.parse_args()

    if not bt.CSV_PATH.exists():
        print(f"ERROR: No se encontró {bt.CSV_PATH}")
        sys.exit(1)

    pairs_df = bt.load_pairs(bt.CSV_PATH, top_n=args.top)
    if pairs_df.empty:
        print("No se encontraron pares.")
        sys.exit(1)

    thresholds = [float(x.strip()) for x in args.thresholds.split(",")]
    print(f"\nBacktest comparativo:")
    print(f"  Universe : {len(pairs_df)} pares")
    print(f"  Barras   : {args.bars} ({args.bars/24:.0f} días)")
    print(f"  Test split: {args.test_split} ({'OOS activo' if args.test_split>0 else 'off'})")
    print(f"  Thresholds: {thresholds}")
    print(f"  Resto    : leído de constants.py vía backtest.DEFAULT_PARAMS")

    results = []
    for z in thresholds:
        label = f"z>={z}"
        m = run_one(label, z, pairs_df, args.bars, args.test_split)
        results.append(m)
        print(fmt_row(m))

    # ── Comparative summary ──
    print(f"\n{'='*100}")
    print("  RESUMEN COMPARATIVO  (mejor opción debe maximizar EV*n_trades con PF aceptable y baja MAE)")
    print(f"{'='*100}")
    print(f"  {'z':>5}  {'n':>5}  {'WR':>7}  {'PF':>6}  {'EV':>9}  {'NET':>10}  "
          f"{'fees':>9}  {'drag':>7}  {'DD':>10}  {'Sharpe':>7}  {'hold':>5}  {'TP%':>6}  {'SL%':>6}")
    print("-" * 100)
    for m in results:
        print(f"  {m['zscore_thresh']:>4.2f}  {m['n_trades']:>5d}  "
              f"{m['win_rate']:>6.1f}%  {m['profit_factor']:>6.2f}  "
              f"${m['ev_per_trade']:>+7.3f}  ${m['net_pnl']:>+9.2f}  "
              f"${m['total_fees']:>7.2f}  {m['fee_drag_pct']:>5.1f}%  "
              f"${m['max_drawdown']:>+8.2f}  {m['sharpe']:>+6.2f}  "
              f"{m['avg_hold_bars']:>4.1f}h  {m['tp_pct']:>5.1f}%  {m['sl_pct']:>5.1f}%")

    if args.test_split > 0:
        print(f"\n  OOS comparison (last {args.test_split*100:.0f}% of data — unseen during 'training'):")
        print(f"  {'z':>5}  {'oos_n':>6}  {'oos_WR':>8}  {'oos_PF':>8}  {'oos_EV':>9}  {'oos_NET':>10}")
        print("-" * 70)
        for m in results:
            if "oos_n_trades" in m:
                print(f"  {m['zscore_thresh']:>4.2f}  {m['oos_n_trades']:>6d}  "
                      f"{m['oos_win_rate']:>7.1f}%  {m['oos_profit_factor']:>7.2f}  "
                      f"${m['oos_ev_per_trade']:>+7.3f}  ${m['oos_net_pnl']:>+9.2f}")

    # ── Save CSV ──
    try:
        import pandas as pd
        out = SCRIPT_DIR / "backtest_z_compare_results.csv"
        pd.DataFrame(results).to_csv(out, index=False)
        print(f"\n  Resultados completos guardados en: {out}")
    except Exception as e:
        print(f"\n  (No se pudo guardar CSV: {e})")

    # ── Recommendation rationale ──
    print(f"\n{'='*100}")
    print("  CÓMO INTERPRETAR")
    print(f"{'='*100}")
    print("  • Si bajar z aumenta n_trades pero baja PF mucho → estás entrando en señales débiles.")
    print("  • Si bajar z aumenta n_trades manteniendo PF ≥ 1.5 y EV > round-trip-cost → relajar tiene sentido.")
    print("  • fee_drag > 50% indica que la estrategia gasta más en fees que en captura — peligrosa.")
    print("  • Compara IS vs OOS si usaste --test-split: si OOS_PF << IS_PF, el parámetro está overfitted.")
    print("  • La elección 'óptima' maximiza NET_PNL en OOS, no EV ni n_trades por separado.")


if __name__ == "__main__":
    main()
