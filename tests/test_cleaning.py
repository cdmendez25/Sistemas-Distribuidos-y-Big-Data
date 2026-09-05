"""Tests de las funciones de limpieza.

Son funciones puras, asi que corren en menos de un segundo sin levantar el
cluster. Depurar el regex aqui es mucho mas barato que hacerlo sobre 300k filas.
"""

import numpy as np
import pandas as pd
import pytest

from src.cleaning import (
    ANOMALY_TOKEN,
    canonical_customer_code,
    clean_partition,
    normalize_phone,
    repair_mojibake,
)
from src.generate_dirty_data import CLEAN_PHRASES, corrupt_encoding


# --------------------------------------------------------------------------
# Codigo de cliente
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "raw,esperado",
    [
        ("  CLI-98234-A  ", "CUST-98234"),
        ("cli_98234_norm", "CUST-98234"),
        ("RAW#98234-[V2]", "CUST-98234"),
        ("CUST-98234-EXP", "CUST-98234"),
        ("  98234  ", "CUST-98234"),
    ],
)
def test_extrae_el_id_de_cualquier_plantilla(raw, esperado):
    assert canonical_customer_code(raw) == esperado


@pytest.mark.parametrize("basura", ["ANOMALOUS_STR", "INVALID", "   ", "", None, np.nan])
def test_lo_irrecuperable_se_marca_como_anomalia(basura):
    assert canonical_customer_code(basura) == ANOMALY_TOKEN


def test_nunca_devuelve_nulo():
    """La fila se conserva siempre: perder trazabilidad es peor que marcarla."""
    for entrada in ["CLI-12345-A", "INVALID", None, np.nan, ""]:
        assert canonical_customer_code(entrada) is not None


# --------------------------------------------------------------------------
# Mojibake
# --------------------------------------------------------------------------
@pytest.mark.parametrize("frase", CLEAN_PHRASES)
def test_ida_y_vuelta(frase):
    """Corromper y reparar debe devolver exactamente el original."""
    assert repair_mojibake(corrupt_encoding(frase)) == frase


def test_el_texto_sano_no_se_toca():
    """Aplicar la conversion a texto ya correcto lo romperia."""
    sano = "Transacción exitosa en Bogotá D.C."
    assert repair_mojibake(sano) == sano


def test_es_idempotente():
    reparado = repair_mojibake(corrupt_encoding("Medellín"))
    assert repair_mojibake(reparado) == "Medellín"


def test_nulos():
    assert repair_mojibake(None) is None
    assert repair_mojibake(np.nan) is None


# --------------------------------------------------------------------------
# Telefonos
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "raw",
    [
        "+57 (310) 123-4567",
        "310.123.4567",
        "0057 310 123 4567",
        "3101234567",
    ],
)
def test_todos_los_formatos_dan_el_mismo_numero(raw):
    assert normalize_phone(raw) == "3101234567"


def test_la_extension_se_quita_antes_de_contar_digitos():
    """Si no, quedan 13 digitos y se descartaria un numero valido."""
    assert normalize_phone("TEL: 3101234567 Ext 402") == "3101234567"


@pytest.mark.parametrize("centinela", ["DESCONOCIDO", "N/A", "--", "", "   ", None, np.nan])
def test_los_centinelas_son_nulo(centinela):
    assert normalize_phone(centinela) is None


def test_numero_de_longitud_incorrecta_se_descarta():
    assert normalize_phone("310123") is None
    assert normalize_phone("6011234567") is None  # fijo, no movil


# --------------------------------------------------------------------------
# Particion completa
# --------------------------------------------------------------------------
def test_clean_partition_devuelve_el_esquema_esperado():
    entrada = pd.DataFrame(
        {
            "transaction_id": ["TX-0000001", "TX-0000002"],
            "raw_customer_code": ["  CLI-98234-A  ", "INVALID"],
            "city_notes_corrupted": [corrupt_encoding("Bogotá"), None],
            "phone_raw": ["+57 (310) 123-4567", "DESCONOCIDO"],
            "amount_usd": [150.0, np.nan],
            "business_category": ["FINANCE", "TECH"],
        }
    )

    salida = clean_partition(entrada)

    assert list(salida.columns) == [
        "transaction_id",
        "customer_code",
        "city_notes",
        "phone",
        "amount_usd",
        "business_category",
    ]
    assert salida["customer_code"].tolist() == ["CUST-98234", ANOMALY_TOKEN]

    # pandas normaliza None a NaN al construir la Serie; ambos son "nulo".
    assert salida["city_notes"][0] == "Bogotá"
    assert pd.isna(salida["city_notes"][1])
    assert salida["phone"][0] == "3101234567"
    assert pd.isna(salida["phone"][1])
