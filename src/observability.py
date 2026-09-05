"""Matriz de observabilidad del cluster.

Cada tarea `Limpiar Particion en Worker Dask` reporta donde se ejecuto y que
produjo. Este modulo agrega esos reportes en dos tablas que Prefect archiva
junto al run, de modo que meses despues se pueda responder "que worker proceso
la particion 4 y cuanto tardo".
"""

from __future__ import annotations

from collections import defaultdict


def execution_breakdown(resultados: list[dict]):
    """Devuelve (matriz por worker, detalle por particion).

    La matriz responde "como se repartio la carga entre los 3 nodos".
    El detalle responde "quien ejecuto exactamente esta particion".
    """
    # Los ids de hilo del SO son numeros de 15 digitos, ilegibles. Se
    # reetiquetan como hilo-0 / hilo-1 dentro de cada worker.
    hilos_por_worker: dict[str, dict[int, str]] = defaultdict(dict)
    for r in resultados:
        pool = hilos_por_worker[r["worker"]]
        if r["hilo"] not in pool:
            pool[r["hilo"]] = f"hilo-{len(pool)}"

    detalle = [
        {
            "particion": r["particion"],
            "worker": r["worker"],
            "hilo": hilos_por_worker[r["worker"]][r["hilo"]],
            "filas": r["filas"],
            "canonicos": r["canonicos"],
            "anomalias": r["anomalias"],
            "mojibake reparado": r["mojibake_reparado"],
            "telefonos validos": r["telefonos_validos"],
            "segundos": round(r["segundos"], 3),
        }
        for r in sorted(resultados, key=lambda x: x["particion"])
    ]

    acumulado: dict[str, dict] = defaultdict(
        lambda: {"particiones": 0, "filas": 0, "segundos": 0.0, "hilos": set()}
    )
    for r in resultados:
        fila = acumulado[r["worker"]]
        fila["particiones"] += 1
        fila["filas"] += r["filas"]
        fila["segundos"] += r["segundos"]
        fila["hilos"].add(hilos_por_worker[r["worker"]][r["hilo"]])

    total = sum(f["segundos"] for f in acumulado.values()) or 1.0

    matriz = [
        {
            "worker": worker,
            "hilos usados": len(datos["hilos"]),
            "particiones": datos["particiones"],
            "filas procesadas": datos["filas"],
            "CPU (s)": round(datos["segundos"], 2),
            "% de la carga": round(100 * datos["segundos"] / total, 1),
        }
        for worker, datos in sorted(acumulado.items())
    ]

    return matriz, detalle
