#!/usr/bin/env python3
"""Pipeline distribuido: Prefect orquesta, Dask ejecuta.

Reparto de responsabilidades:

  Prefect  -> el CICLO DE VIDA: en que orden van las etapas, que se reintenta,
              que estado quedo en cada una, y el historial auditable.
  Dask     -> la EJECUCION DISTRIBUIDA: el DaskTaskRunner reparte las tareas
              entre los 3 workers del cluster.

Cada particion es UNA tarea de Prefect. Por eso el grafo del run muestra el
abanico completo (6 cajas en paralelo) y no una sola caja opaca: se ve que
worker se llevo cada chunk. Ningun nodo carga mas de 50.000 filas a la vez, que
es lo que hace el procesamiento out-of-core.
"""

from __future__ import annotations

import glob
import os
import threading
import time

import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_table_artifact
from prefect_dask import DaskTaskRunner, get_dask_client

from src.cleaning import ANOMALY_TOKEN, clean_partition
from src.observability import execution_breakdown

DASK_SCHEDULER = os.environ.get("DASK_SCHEDULER", "tcp://dask-scheduler:8786")
DATA_ROOT = os.environ.get("DATA_ROOT", "/shared-data")

RAW_GLOB = f"{DATA_ROOT}/raw/*.csv"
PROCESSED_DIR = f"{DATA_ROOT}/processed"

EXPECTED_ROWS = int(os.environ.get("NUM_ROWS", 300_000))
EXPECTED_WORKERS = 3


def donde_estoy() -> tuple[str, int]:
    """Identifica el worker y el hilo que ejecutan esta tarea.

    Sin esto, la UI de Prefect dice que la tarea termino, pero no en que nodo
    del cluster.
    """
    try:
        from distributed import get_worker

        return str(get_worker().name), threading.get_ident()
    except (ImportError, ValueError):
        # ValueError = no estamos dentro de un worker (ejecucion local).
        return "local", threading.get_ident()


# --------------------------------------------------------------------------
@task(
    name="1. Validar Infraestructura del Clúster",
    retries=3,
    retry_delay_seconds=10,
)
def validar_infraestructura() -> dict:
    """Los reintentos no son decorativos: el runner puede arrancar antes de que
    los 3 workers hayan terminado de registrarse."""
    logger = get_run_logger()

    with get_dask_client() as client:
        workers = client.scheduler_info()["workers"]
        hilos = sum(w["nthreads"] for w in workers.values())

    logger.info("Cluster conectado: %d workers, %d hilos", len(workers), hilos)

    if len(workers) < EXPECTED_WORKERS:
        raise RuntimeError(
            f"Solo {len(workers)} workers de {EXPECTED_WORKERS}. Revisa `docker compose ps`."
        )

    return {"workers": len(workers), "hilos": hilos}


# --------------------------------------------------------------------------
@task(name="2. Verificar Archivos Crudos Particionados")
def verificar_archivos_crudos() -> list[str]:
    logger = get_run_logger()
    paths = sorted(glob.glob(RAW_GLOB))

    if not paths:
        raise FileNotFoundError(
            f"No hay CSV en {RAW_GLOB}. Ejecuta primero: "
            "docker compose run --rm pipeline python -m src.generate_dirty_data"
        )

    logger.info("Particiones detectadas: %d", len(paths))
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    return paths


# --------------------------------------------------------------------------
# El corazon del abanico: esta tarea se lanza una vez POR ARCHIVO, y el
# DaskTaskRunner decide en que worker corre cada instancia.
@task(name="Limpiar Partición en Worker Dask", retries=2, retry_delay_seconds=5)
def limpiar_particion(csv_path: str) -> dict:
    logger = get_run_logger()
    worker, hilo = donde_estoy()
    particion = os.path.basename(csv_path).replace(".csv", "")

    logger.info("[%s] procesando %s en el hilo %s", worker, particion, hilo)
    inicio = time.perf_counter()

    # Solo este chunk entra en memoria: 50.000 filas, no 300.000.
    crudo = pd.read_csv(csv_path, dtype=str)
    crudo["amount_usd"] = pd.to_numeric(crudo["amount_usd"], errors="coerce")

    limpio = clean_partition(crudo)

    salida = f"{PROCESSED_DIR}/{particion}.parquet"
    limpio.to_parquet(salida, engine="pyarrow", compression="snappy", index=False)

    duracion = time.perf_counter() - inicio
    codigos = limpio["customer_code"]
    anomalias = int((codigos == ANOMALY_TOKEN).sum())

    reporte = {
        "particion": particion,
        "worker": worker,
        "hilo": hilo,
        "filas": len(limpio),
        "canonicos": len(limpio) - anomalias,
        "anomalias": anomalias,
        "mojibake_reparado": int(crudo["city_notes_corrupted"].str.contains("Ã", na=False).sum()),
        "telefonos_validos": int(limpio["phone"].notna().sum()),
        "segundos": duracion,
        "salida": salida,
    }

    logger.info("[%s] %s: %d filas en %.2f s", worker, particion, len(limpio), duracion)
    return reporte


# --------------------------------------------------------------------------
@task(name="4. Generar Matriz de Observabilidad y Quality Gates")
def generar_matriz_y_validar(reportes: list[dict]) -> dict:
    """Agrega lo que reporto cada worker y aplica las aserciones de calidad.

    Un pipeline que termina "sin error" no es un pipeline que termino bien.
    """
    logger = get_run_logger()
    matriz, detalle = execution_breakdown(reportes)

    create_table_artifact(
        key="dask-cluster-execution-breakdown",
        table=matriz,
        description="Reparto real de la carga entre los workers del clúster Dask.",
    )
    create_table_artifact(
        key="particiones-por-worker",
        table=detalle,
        description="Qué hilo de qué worker procesó cada partición, y qué produjo.",
    )

    for fila in matriz:
        logger.info(
            "%s | %d hilos | %d particiones | %s filas | %.2f s | %.1f %% de la carga",
            fila["worker"],
            fila["hilos usados"],
            fila["particiones"],
            f"{fila['filas procesadas']:,}",
            fila["CPU (s)"],
            fila["% de la carga"],
        )

    filas = sum(r["filas"] for r in reportes)
    anomalias = sum(r["anomalias"] for r in reportes)
    canonicos = sum(r["canonicos"] for r in reportes)

    # Verificacion independiente sobre lo que quedo escrito en disco.
    final = pd.read_parquet(PROCESSED_DIR, engine="pyarrow")
    mojibake = int(final["city_notes"].str.contains("Ã", na=False).sum())
    nulos = int(final["customer_code"].isna().sum())

    assert filas == EXPECTED_ROWS, f"Se esperaban {EXPECTED_ROWS:,} filas, hay {filas:,}"
    assert len(final) == EXPECTED_ROWS, "El Parquet escrito no tiene todas las filas"
    assert canonicos + anomalias == filas, "Hay códigos fuera del formato canónico"
    assert nulos == 0, f"Quedaron {nulos:,} códigos nulos"
    assert mojibake == 0, f"Quedan {mojibake:,} notas con mojibake sin reparar"

    logger.info("Quality gates superados: %s filas limpias.", f"{filas:,}")

    return {
        "filas": filas,
        "canonicos": canonicos,
        "anomalias": anomalias,
        "mojibake_restante": mojibake,
        "particiones": len(reportes),
    }


# --------------------------------------------------------------------------
@flow(
    name="Dask-Prefect-Distributed-Cleaning-Flow",
    task_runner=DaskTaskRunner(address=DASK_SCHEDULER),
    log_prints=True,
)
def pipeline_limpieza_distribuida():
    print("=== INICIANDO PIPELINE DISTRIBUIDO (DASK + PREFECT) ===")

    cluster = validar_infraestructura.submit()
    particiones = verificar_archivos_crudos.submit(wait_for=[cluster])

    # El abanico: una tarea de Prefect por archivo. El DaskTaskRunner las
    # reparte entre los 3 workers y la UI muestra las 6 cajas en paralelo.
    limpiezas = [limpiar_particion.submit(ruta) for ruta in particiones.result()]

    return generar_matriz_y_validar.submit(limpiezas).result()


if __name__ == "__main__":
    pipeline_limpieza_distribuida()
