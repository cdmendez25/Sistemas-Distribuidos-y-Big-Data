#!/usr/bin/env python3
"""Generador sintetico de 300.000 registros con anomalias severas de calidad.

Escribe 6 CSV particionados en ``$DATA_ROOT/raw/`` para que los 3 workers
reciban dos chunks balanceados cada uno.
"""

from __future__ import annotations

import os
import random

import numpy as np
import pandas as pd

# 300.000 es lo que pide el taller. Se puede subir con la variable NUM_ROWS
# para que el pipeline dure lo suficiente como para matar un worker a mitad de
# ejecucion y ver a Dask reprogramar los chunks (con 300k dura ~2 segundos).
NUM_ROWS = int(os.environ.get("NUM_ROWS", 300_000))
NUM_CHUNKS = 6
SEED = 42

DATA_ROOT = os.environ.get("DATA_ROOT", "shared-data")


# --------------------------------------------------------------------------
# Anomalia 1: codigos de cliente desestructurados
# --------------------------------------------------------------------------
# El mismo id de 5 digitos, envuelto por sistemas legados distintos. Las dos
# ultimas plantillas y el None son ruido puro: no contienen id recuperable.
CODE_TEMPLATES = [
    "  CLI-{id}-A  ",
    "cli_{id}_norm",
    "RAW#{id}-[V2]",
    "CUST-{id}-EXP",
    "  {id}  ",
    "ANOMALOUS_STR",
    "INVALID",
    "   ",
    None,
]
CODE_WEIGHTS = [0.45, 0.20, 0.15, 0.10, 0.04, 0.02, 0.02, 0.01, 0.01]


# --------------------------------------------------------------------------
# Anomalia 2: mojibake
# --------------------------------------------------------------------------
# Se parte de frases CORRECTAS y se corrompen programaticamente. Produce los
# mismos bytes que copiar 'BogotÃ¡' a mano, pero garantiza que la reparacion
# del pipeline es reversible al 100 % y no depende de que copiemos bien.
CLEAN_PHRASES = [
    "Transacción exitosa en Bogotá D.C.",
    "Cliente atendido en Medellín por garantía",
    "Verificación de crédito rechazada en Popayán",
    "Envío exprés hacia Cartagena de Indias",
    "Actualización de dirección en Cali (Valle)",
    "Operación pendiente de conciliación bancaria",
    "Sin observaciones registradas",
    "ñandú importado - paquete especial",
]
PHRASE_WEIGHTS = [0.25, 0.20, 0.15, 0.15, 0.10, 0.08, 0.03, 0.02, 0.01, 0.01]


def corrupt_encoding(phrase: str) -> str:
    """UTF-8 escrito y releido como Latin-1: 'Bogotá' -> 'BogotÃ¡'."""
    return phrase.encode("utf-8").decode("latin-1")


# --------------------------------------------------------------------------
# Anomalia 3: telefonos heterogeneos
# --------------------------------------------------------------------------
PHONE_TEMPLATES = [
    "+57 (310) {p1}-{p2}",
    "310.{p1}.{p2}",
    "TEL: 310{p1}{p2} Ext 402",
    "0057 310 {p1} {p2}",
    "310{p1}{p2}",
    "DESCONOCIDO",
    "N/A",
    "--",
    "",
]
PHONE_WEIGHTS = [0.35, 0.25, 0.15, 0.10, 0.08, 0.03, 0.02, 0.01, 0.01]

CATEGORIES = ["FINANCE", "LOGISTICS", "RETAIL", "HEALTH", "TECH"]


def generate_dirty_dataset(num_rows: int = NUM_ROWS, output_dir: str | None = None):
    output_dir = output_dir or os.path.join(DATA_ROOT, "raw")
    os.makedirs(output_dir, exist_ok=True)

    print(f"[*] Generando {num_rows:,} filas con ruido sintetico...")

    # Semilla fija: el experimento debe ser reproducible entre ejecuciones.
    np.random.seed(SEED)
    random.seed(SEED)

    # --- Columna objetivo de la refactorizacion -----------------------------
    clean_ids = np.random.randint(10000, 99999, size=num_rows)

    # Se elige la plantilla UNA sola vez y luego se decide que hacer con ella.
    # El enunciado llama a random.choices() dos veces, asi que la plantilla que
    # comprueba contra None no es la que formatea -> AttributeError.
    raw_customer_code = []
    for cid in clean_ids:
        template = random.choices(CODE_TEMPLATES, weights=CODE_WEIGHTS)[0]
        raw_customer_code.append(np.nan if template is None else template.format(id=cid))

    # --- Notas con encoding corrupto ---------------------------------------
    mojibake_phrases = [corrupt_encoding(p) for p in CLEAN_PHRASES] + ["   ", None]
    corrupted_notes = random.choices(mojibake_phrases, weights=PHRASE_WEIGHTS, k=num_rows)

    # --- Telefonos caoticos -------------------------------------------------
    raw_phones = [
        random.choices(PHONE_TEMPLATES, weights=PHONE_WEIGHTS)[0].format(
            p1=random.randint(100, 999), p2=random.randint(1000, 9999)
        )
        for _ in range(num_rows)
    ]

    # --- Montos con 3 % de nulos -------------------------------------------
    amounts = np.random.exponential(scale=150.0, size=num_rows)
    amounts[np.random.rand(num_rows) < 0.03] = np.nan

    df = pd.DataFrame(
        {
            "transaction_id": [f"TX-{i:07d}" for i in range(1, num_rows + 1)],
            "raw_customer_code": raw_customer_code,
            "city_notes_corrupted": corrupted_notes,
            "phone_raw": raw_phones,
            "amount_usd": np.round(amounts, 2),
            "business_category": random.choices(CATEGORIES, k=num_rows),
        }
    )

    # --- Particionado en 6 archivos ----------------------------------------
    # Un archivo = una particion de Dask. Con 6 archivos y 3 workers, cada
    # worker recibe exactamente dos chunks.
    chunk_size = num_rows // NUM_CHUNKS
    for idx in range(NUM_CHUNKS):
        chunk = df.iloc[idx * chunk_size : (idx + 1) * chunk_size]
        path = os.path.join(output_dir, f"transactions_dirty_part_{idx + 1}.csv")
        chunk.to_csv(path, index=False, encoding="utf-8")
        print(f"    -> {path} ({len(chunk):,} filas)")

    print(f"[OK] Dataset sintetico generado en {output_dir}/")


if __name__ == "__main__":
    generate_dirty_dataset()
