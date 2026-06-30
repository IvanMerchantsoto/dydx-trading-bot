"""
inspect_pairs.py
================
Inspecciona el cointegrated_pairs.csv generado.
Muestra distribución de hedge ratios, half-lives y top pares.

Uso:
    cd program/
    python inspect_pairs.py
"""

import os
import pandas as pd
import numpy as np

CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cointegrated_pairs.csv")

if not os.path.exists(CSV_PATH):
    print(f"No se encontró {CSV_PATH}")
    print("Corre el bot con FIND_COINTEGRATED=True primero.")
    exit(1)

df = pd.read_csv(CSV_PATH)
print(f"\nCSV: {CSV_PATH}")
print(f"Total pares cointegrados: {len(df)}")
print(f"Columnas: {list(df.columns)}\n")

if len(df) == 0:
    print("⚠️  El CSV está vacío — ningún par pasó los filtros.")
    print("  Verifica: WINDOW, MAX_HALF_LIFE, ZSCORE_THRESH")
    exit(0)

hr = df['hedge_ratio'].abs()
print("── Distribución hedge_ratio (abs) ─────────────────────────")
print(f"  |hr| > 100      (sospechoso): {(hr > 100).sum():4d}  ← deberían ser ~0 con OLS fix")
print(f"  |hr| 10–100     (revisable):  {((hr > 10) & (hr <= 100)).sum():4d}")
print(f"  |hr| 1–10       (bueno):      {((hr >= 1) & (hr <= 10)).sum():4d}")
print(f"  |hr| 0.1–1      (bueno):      {((hr >= 0.1) & (hr < 1)).sum():4d}")
print(f"  |hr| < 0.1      (pequeño):    {(hr < 0.1).sum():4d}")

hl = df['half_life']
print("\n── Half-life (horas) ───────────────────────────────────────")
print(f"  min={hl.min():.0f}h  median={hl.median():.0f}h  mean={hl.mean():.1f}h  max={hl.max():.0f}h")
print(f"  Pares con hl ≤ 8h (muy rápidos):  {(hl <= 8).sum()}")
print(f"  Pares con hl 9–16h:                {((hl > 8) & (hl <= 16)).sum()}")
print(f"  Pares con hl 17–24h:               {(hl > 16).sum()}")

print("\n── Top 20 pares (menor half-life = más rápido) ─────────────")
cols = ['base_market', 'quote_market', 'hedge_ratio', 'half_life']
print(df[cols].head(20).to_string(index=False))

# Flag any remaining suspicious ratios
suspect = df[hr > 100]
if len(suspect) > 0:
    print(f"\n⚠️  ATENCIÓN: {len(suspect)} pares con |hedge_ratio| > 100 (revisar manualmente):")
    print(suspect[cols].to_string(index=False))
else:
    print("\n✅ Todos los hedge_ratios son razonables (|hr| ≤ 100)")
