#!/usr/bin/env python3
"""Comprueba los criterios de aceptacion sobre el Parquet resultante.

Lectura directa con Pandas (no Dask): es una verificacion independiente del
pipeline, para no dar por bueno el resultado usando las mismas herramientas que
lo produjeron.
"""

from __future__ import annotations

import os
import sys

import pandas as pd

DATA_ROOT = os.environ.get("DATA_ROOT", "/shared-data")
PROCESSED_DIR = f"{DATA_ROOT}/processed"
EXPECTED_ROWS = int(os.environ.get("NUM_ROWS", 300_000))

CANONICAL_PATTERN = r"^CUST-\d{5}$"
ANOMALY_TOKEN = "CUST-00000-ANOMALY"


def main() -> int:
    if not os.path.isdir(PROCESSED_DIR):
        print(f"[X] No existe {PROCESSED_DIR}. Ejecuta antes el pipeline.")
        return 1

    df = pd.read_parquet(PROCESSED_DIR, engine="pyarrow")

    codes = df["customer_code"]
    canonicos = int(codes.str.match(CANONICAL_PATTERN).sum())
    anomalias = int((codes == ANOMALY_TOKEN).sum())
    nulos = int(codes.isna().sum())
    mojibake = int(df["city_notes"].str.contains("Ã", na=False).sum())
    tel_ok = int(df["phone"].notna().sum())

    print(f"Filas                : {len(df):,}")
    print(f"Codigos canonicos    : {canonicos:,}")
    print(f"Anomalias marcadas   : {anomalias:,}")
    print(f"Codigos nulos        : {nulos:,}")
    print(f"Restos de mojibake   : {mojibake:,}")
    print(f"Telefonos validos    : {tel_ok:,}")
    print(f"Telefonos nulos      : {len(df) - tel_ok:,}")
    print()
    print(df[["transaction_id", "customer_code", "city_notes", "phone"]].head(6).to_string(index=False))
    print()

    checks = [
        (f"{EXPECTED_ROWS:,} filas exactas", len(df) == EXPECTED_ROWS),
        ("todo codigo es canonico o anomalia", canonicos + anomalias == len(df)),
        ("ningun codigo nulo", nulos == 0),
        ("cero restos de mojibake", mojibake == 0),
    ]

    for etiqueta, ok in checks:
        print(f"  [{'OK' if ok else 'X '}] {etiqueta}")

    fallidos = [e for e, ok in checks if not ok]
    if fallidos:
        print(f"\n[X] {len(fallidos)} criterio(s) sin cumplir.")
        return 1

    print("\n[OK] Todos los criterios de aceptacion se cumplen.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
