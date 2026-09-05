"""Funciones de limpieza para el pipeline distribuido.

Deliberadamente NO importan Dask: son funciones puras de Python/Pandas que se
pueden testear en un segundo sin levantar el cluster. Cada tarea `Limpiar
Particion en Worker Dask` las invoca sobre su propio chunk, dentro del worker
que le toco.
"""

from __future__ import annotations

import re

import pandas as pd

# --------------------------------------------------------------------------
# 1. Refactorizacion del codigo de cliente -> CUST-XXXXX
# --------------------------------------------------------------------------

# Los sistemas legados entregan el mismo id de 5 digitos envuelto en ruido
# distinto: "  CLI-98234-A  ", "cli_98234_norm", "RAW#98234-[V2]",
# "CUST-98234-EXP", "  98234  ". El prefijo es opcional; lo que importa es
# capturar el bloque de 5 digitos.
CUSTOMER_CODE_RE = re.compile(r"(?:CLI-|cli_|RAW#|CUST-)?(\d{5})")

# Token de alerta. Nunca se descarta la fila: perder trazabilidad es peor que
# arrastrar un registro marcado como anomalo.
ANOMALY_TOKEN = "CUST-00000-ANOMALY"


def canonical_customer_code(raw) -> str:
    """Normaliza cualquier variante sucia al canonico ``CUST-XXXXX``.

    >>> canonical_customer_code("  CLI-98234-A  ")
    'CUST-98234'
    >>> canonical_customer_code("INVALID")
    'CUST-00000-ANOMALY'
    """
    if raw is None or pd.isna(raw):
        return ANOMALY_TOKEN

    match = CUSTOMER_CODE_RE.search(str(raw))
    if match is None:
        return ANOMALY_TOKEN

    return f"CUST-{match.group(1)}"


# --------------------------------------------------------------------------
# 2. Reparacion de mojibake (UTF-8 leido como Latin-1)
# --------------------------------------------------------------------------

# Marcadores de codificacion cruzada. Todo byte UTF-8 de un caracter latino
# empieza por 0xC3 o 0xC2, que en Latin-1 se leen como 'Ã' y 'Â'.
MOJIBAKE_MARKERS = ("Ã", "Â")


def repair_mojibake(text):
    """Deshace la codificacion cruzada UTF-8 -> Latin-1.

    El texto se guardo como UTF-8 pero se leyo como Latin-1, produciendo
    'BogotÃ¡'. La operacion inversa es volver a bytes con Latin-1 y decodificar
    como UTF-8.

    >>> repair_mojibake("TransacciÃ³n en BogotÃ¡")
    'Transacción en Bogotá'
    """
    if text is None or pd.isna(text):
        return None

    text = str(text)

    # Sin marcadores el texto ya esta sano; aplicarle la conversion lo romperia.
    if not any(marker in text for marker in MOJIBAKE_MARKERS):
        return text

    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        # Corrupcion que no encaja en este patron: mejor el original que nada.
        return text


# --------------------------------------------------------------------------
# 3. Normalizacion de telefonos
# --------------------------------------------------------------------------

# Valores centinela que significan "no hay telefono".
PHONE_SENTINELS = {"DESCONOCIDO", "N/A", "--", "", "NAN", "NONE"}

# La extension se elimina ANTES de extraer digitos. Si no, "TEL: 3101234567
# Ext 402" deja 13 digitos y se descartaria un numero perfectamente valido.
PHONE_EXTENSION_RE = re.compile(r"\s*(?:ext|extension)\.?\s*\d+", re.IGNORECASE)
NON_DIGIT_RE = re.compile(r"\D")


def normalize_phone(raw):
    """Extrae el movil colombiano de 10 digitos, o ``None`` si no lo hay.

    >>> normalize_phone("+57 (310) 123-4567")
    '3101234567'
    >>> normalize_phone("TEL: 3101234567 Ext 402")
    '3101234567'
    >>> normalize_phone("DESCONOCIDO") is None
    True
    """
    if raw is None or pd.isna(raw):
        return None

    text = str(raw).strip()
    if text.upper() in PHONE_SENTINELS:
        return None

    digits = NON_DIGIT_RE.sub("", PHONE_EXTENSION_RE.sub("", text))

    # Prefijo de pais en sus dos formatos: "0057 310..." y "+57 310...".
    if digits.startswith("0057"):
        digits = digits[4:]
    elif len(digits) == 12 and digits.startswith("57"):
        digits = digits[2:]

    # Movil colombiano: 10 digitos que empiezan por 3.
    if len(digits) == 10 and digits.startswith("3"):
        return digits

    return None


# --------------------------------------------------------------------------
# 4. Transformacion a nivel de particion (lo que ejecuta cada worker)
# --------------------------------------------------------------------------

OUTPUT_COLUMNS = [
    "transaction_id",
    "customer_code",
    "city_notes",
    "phone",
    "amount_usd",
    "business_category",
]


def clean_partition(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia un bloque completo de Pandas.

    Las tres transformaciones van juntas en una sola pasada: el worker recorre
    el chunk una vez, no tres. Con 50.000 filas por particion esto entra
    holgadamente en el limite de 1.5 GB del contenedor.
    """
    return pd.DataFrame(
        {
            "transaction_id": df["transaction_id"],
            "customer_code": df["raw_customer_code"].map(canonical_customer_code),
            "city_notes": df["city_notes_corrupted"].map(repair_mojibake),
            "phone": df["phone_raw"].map(normalize_phone),
            "amount_usd": df["amount_usd"],
            "business_category": df["business_category"],
        }
    )
