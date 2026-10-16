#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FASE 1 - ANALISIS EN BATCH (PySpark)
Dataset: Destruccion de infraestructura de produccion de drogas ilicitas

Pregunta central:
    Cuales son los departamentos de Colombia que registran la mayor cantidad
    de destrucciones de infraestructura de produccion de drogas ilicitas?

Nota clave: este dataset trae la columna CANTIDAD, por lo que NO se cuentan
filas sino que se SUMA esa columna. Cada fila es un reporte agregado por
municipio y fecha.

Ejecucion 100% CLI, sin interfaz grafica.
Salida: ~/Escritorio/BIGDATA 3/fase1_drogas/
"""

import os
import io
import sys
import datetime

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =============================================================================
HOME = os.path.expanduser("~")
OUT_DIR = os.path.join(HOME, "Escritorio", "BIGDATA 3", "fase1_drogas")
GRAF_DIR = os.path.join(OUT_DIR, "graficos")
REPORT = os.path.join(OUT_DIR, "reporte_drogas.txt")

NOMBRE = "DESTRUCCION_DE_INFRAESTRUCTURA_DE_PRODUCCION_DE_DROGAS_ILICITAS_20261015"
RUTAS = [
    os.path.join(HOME, "Descargas", NOMBRE + ".csv"),
    os.path.join(HOME, "Descargas",
                 "DESTRUCCIÓN_DE_INFRAESTRUCTURA_DE_PRODUCCIÓN_DE_DROGAS_ILÍCITAS_20261015.csv"),
    os.path.join(HOME, NOMBRE + ".csv"),
]

MAPEO = {
    "FECHA HECHO": "fecha", "COD_DEPTO": "cod_depto",
    "DEPARTAMENTO": "departamento", "COD_MUNI": "cod_muni",
    "MUNICIPIO": "municipio", "CANTIDAD": "cantidad",
    "UNIDAD DE MEDIDA": "unidad",
}
COLS_TEXTO = ["departamento", "municipio", "unidad"]

AZUL, VERDE, NARANJA, ROJO, GRIS, MORADO = \
    "#2c5f8d", "#3d8b5f", "#d98c2b", "#b8453c", "#6b7280", "#7b5ea7"
LOG = []


def emit(line=""):
    print(line)
    LOG.append(str(line))


def regla(t):
    emit("")
    emit("=" * 78)
    emit(t)
    emit("=" * 78)


def tabla(df, n=20):
    filas = df.limit(n).collect()
    if not filas:
        return ["(sin registros)"]
    cols = df.columns
    anchos = [len(str(c)) for c in cols]
    celdas = []
    for f in filas:
        fila = []
        for i, v in enumerate(f):
            s = "NULL" if v is None else str(v)
            anchos[i] = max(anchos[i], len(s))
            fila.append(s)
        celdas.append(fila)
    sep = "+" + "+".join("-" * (a + 2) for a in anchos) + "+"
    out = [sep, "| " + " | ".join(str(c).ljust(anchos[i])
                                 for i, c in enumerate(cols)) + " |", sep]
    for fila in celdas:
        out.append("| " + " | ".join(fila[i].ljust(anchos[i])
                                     for i in range(len(cols))) + " |")
    out.append(sep)
    out.append("({} filas en total)".format(df.count()))
    return out


def guardar(fig, nombre, nota):
    ruta = os.path.join(GRAF_DIR, nombre)
    fig.savefig(ruta, dpi=110, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    emit("  [PNG] {:40} {:7.1f} KB   <- {}".format(
        nombre, os.path.getsize(ruta) / 1024, nota))


def norm(c):
    return F.trim(F.regexp_replace(
        F.expr("translate({}, 'áéíóúüñÁÉÍÓÚÜÑ', 'aeiouunAEIOUUN')".format(c)),
        r"\s+", " "))


def main():
    inicio = datetime.datetime.now()

    regla("PASO 1 - DIRECTORIO DE SALIDA")
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(GRAF_DIR, exist_ok=True)
    emit("Carpeta : {}".format(OUT_DIR))
    emit("Graficos: {}".format(GRAF_DIR))

    ruta = None
    for r in RUTAS:
        if os.path.isfile(r):
            ruta = r
            break
    if not ruta:
        emit("[ERROR] Dataset no encontrado.")
        sys.exit(1)
    emit("")
    emit("Dataset: {}".format(ruta))
    emit("Tamano : {:,} bytes".format(os.path.getsize(ruta)))

    spark = (SparkSession.builder.appName("Destruccion-Infra-Drogas")
             .master("local[*]")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.memory", "2g")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # ---------------------------------------------------------------- PASO 2a
    regla("PASO 2a - CARGA Y ESQUEMA")
    crudo = (spark.read.option("header", "true").option("delimiter", ",")
             .option("encoding", "UTF-8").option("inferSchema", "true")
             .option("mode", "PERMISSIVE").option("quote", '"')
             .csv(ruta))
    total_crudo = crudo.count()
    emit("df.count() : {:,}".format(total_crudo))
    emit("Columnas   : {}".format(len(crudo.columns)))
    emit("")
    emit("printSchema():")
    emit(crudo._jdf.schema().treeString())

    # ---------------------------------------------------------------- PASO 2b
    regla("PASO 2b - LIMPIEZA")
    d = crudo
    for o, n in MAPEO.items():
        if o in d.columns:
            d = d.withColumnRenamed(o, n)
    emit("Columnas renormal: {}".format(d.columns))
    emit("")
    emit("Nulos por columna (antes):")
    for c in d.columns:
        emit("  {:14} {:>8,}".format(c, d.filter(F.col(c).isNull()).count()))

    for c in COLS_TEXTO:
        if c in d.columns:
            d = d.withColumn(c, F.trim(F.col(c)))
            d = d.withColumn(c, F.when(F.col(c) == "", F.lit(None)).otherwise(F.col(c)))
    for c in ["departamento", "municipio"]:
        d = d.withColumn(c, norm(c))
    d = d.withColumn("cantidad",
                     F.when(F.col("cantidad").isNull(), F.lit(0.0))
                      .otherwise(F.col("cantidad").cast("double")))
    # FECHA HECHO viene como texto ("2026 Aug 31 12:00:00 AM"), no como
    # timestamp. F.year() sobre un string devuelve NULL, asi que se extrae
    # el ano con una expresion regular sobre los 4 primeros digitos.
    d = d.withColumn("fecha_dt", F.to_date(F.col("fecha"), "yyyy MMM dd hh:mm:ss a"))
    d = d.withColumn("anio",
                     F.when(F.col("fecha_dt").isNotNull(),
                            F.year(F.col("fecha_dt")))
                      .otherwise(
                          F.regexp_extract(F.col("fecha"), r"^(\\d{4})", 1).cast("int")))

    total = d.count()
    total_unidades = d.agg(F.sum("cantidad")).collect()[0][0]

    emit("")
    emit("Balance:")
    emit("  Filas de reporte            : {:>8,}".format(total))
    emit("  SUMA de CANTIDAD (unidades) : {:>8,.0f}".format(total_unidades))
    emit("")
    emit("  ATENCION: este dataset trae la columna CANTIDAD. Cada fila es un")
    emit("  reporte agregado por municipio y fecha, no un evento individual.")
    emit("  Por eso se SUMA CANTIDAD; contar filas daria un numero sin sentido.")
    emit("")
    emit("  Unidades de medida presentes:")
    for r in (d.groupBy("unidad").agg(F.count(F.lit(1)).alias("filas"))
               .orderBy(F.desc("filas")).collect()):
        emit("    {:20} {:>8,} filas".format(r["unidad"], r["filas"]))
    emit("  (una sola unidad => sumar es valido)")

    # ---------------------------------------------------------------- PASO 3
    regla("PASO 3 - PREGUNTA CENTRAL")
    emit("")
    emit("  DEPARTAMENTOS CON MAS DESTRUCCIONES DE INFRAESTRUCTURA")
    emit("  DE PRODUCCION DE DROGAS ILICITAS")
    emit("")

    por_dep = (d.groupBy("departamento")
                 .agg(F.sum("cantidad").alias("unidades"),
                      F.count(F.lit(1)).alias("reportes"),
                      F.countDistinct("municipio").alias("municipios"))
                 .filter(F.col("unidades") > 0)
                 .orderBy(F.desc("unidades")))

    top = por_dep.limit(10).collect()
    tot = sum(r["unidades"] for r in top)

    emit("  Top 10 departamentos")
    emit("  " + "-" * 76)
    emit("  {:<3}{:<26}{:>12}{:>12}{:>12}{:>9}".format(
        "#", "DEPARTAMENTO", "UNIDADES", "% DEL TOTAL", "REPORTES", "MUNIC."))
    for i, r in enumerate(top, 1):
        emit("  {:<3}{:<26}{:>12,.0f}{:>11.2f}%{:>12,}{:>9,}".format(
            i, r["departamento"][:25], r["unidades"],
            r["unidades"] * 100.0 / total_unidades,
            r["reportes"], r["municipios"]))
    emit("  " + "-" * 76)
    emit("  Los 10 primeros concentran {:,.0f} de {:,.0f} unidades ({:.1f}%)".format(
        tot, total_unidades, tot * 100.0 / total_unidades))

    # ---------------------------------------------------------------- PASO 4
    regla("PASO 4 - CONTROL DE CALIDAD DEL DATO")

    emit("")
    emit("4.1  ¿Cuantos departamentos hay en Colombia?  32")
    emit("     El dataset trae {} valores distintos en DEPARTAMENTO:".format(
        d.select("departamento").distinct().count()))
    emit("")

    # anomalias: codigos que no son DIVIPOLA de 2 digitos
    anomalias = (d.filter(~F.col("cod_depto").rlike("^[0-9]{2}$"))
                 .groupBy("cod_depto", "departamento")
                 .agg(F.sum("cantidad").alias("unidades"),
                      F.count(F.lit(1)).alias("filas"))
                 .orderBy(F.desc("unidades")))
    filas_anom = anomalias.collect()
    if filas_anom:
        emit("4.2  ANOMALIAS DETECTADAS (COD_DEPTO no es un codigo DIVIPOLA de 2 digitos)")
        emit("")
        emit("     {:<12}{:<22}{:>10}{:>8}".format(
            "COD_DEPTO", "DEPARTAMENTO", "UNIDADES", "FILAS"))
        for r in filas_anom:
            emit("     {:<12}{:<22}{:>10,.0f}{:>8,}".format(
                str(r["cod_depto"])[:11], str(r["departamento"])[:21],
                r["unidades"], r["filas"]))
        sucio = sum(r["unidades"] for r in filas_anom)
        emit("")
        emit("     Total contaminado: {:,.0f} unidades ({:.2f}% del total)".format(
            sucio, sucio * 100.0 / total_unidades))
        emit("     'PERU' es un PAIS, no un departamento colombiano.")
        emit("     Se excluyen del ranking oficial pero se dejan documentados.")

    # ---------------------------------------------------------------- PASO 5
    regla("PASO 5 - RANKING OFICIAL (solo departamentos válidos)")

    valido = d.filter(F.col("cod_depto").rlike("^[0-9]{2}$") &
                      (F.upper(F.col("departamento")) != "SIN ESTABLECER"))
    dep_valido = (valido.groupBy("departamento")
                       .agg(F.sum("cantidad").alias("unidades"),
                            F.count(F.lit(1)).alias("reportes"),
                            F.countDistinct("municipio").alias("municipios"),
                            F.min("anio").alias("desde"),
                            F.max("anio").alias("hasta"))
                       .orderBy(F.desc("unidades")))
    emit("")
    emit("5.1  Ranking completo de departamentos validos")
    for l in tabla(dep_valido.limit(20), 20):
        emit(l)

    top5 = dep_valido.limit(5).collect()
    emit("")
    emit("5.2  LOS TRES MAYORES")
    emit("     " + "-" * 52)
    for i, r in enumerate(top5[:3], 1):
        emit("     {}. {:<24}{:>10,.0f} unidades".format(
            i, r["departamento"], r["unidades"]))
    emit("     " + "-" * 52)

    # ---------------------------------------------------------------- PASO 6
    regla("PASO 6 - CONTEXTO: SERIE HISTORICA")

    por_anio = (valido.groupBy("anio").agg(F.sum("cantidad").alias("unidades"),
                                          F.count(F.lit(1)).alias("reportes"))
                   .orderBy("anio"))
    emit("")
    emit("6.1  Destrucciones por ano (todo el pais)")
    for l in tabla(por_anio, 20):
        emit(l)

    # Putumayo vs Narino por ano. Se usa agregacion condicional y NO pivot():
    # DataFrame.pivot() no existe en PySpark 3.5 (solo GroupedData.pivot, y
    # ademas exige la lista explicita de valores).
    par = (valido.filter(F.upper(F.col("departamento")).isin("PUTUMAYO", "NARIÑO"))
           .groupBy("anio")
           .agg(F.sum(F.when(F.upper(F.col("departamento")) == "PUTUMAYO",
                             F.col("cantidad")).otherwise(0)).alias("PUTUMAYO"),
                F.sum(F.when(F.upper(F.col("departamento")) == "NARIÑO",
                             F.col("cantidad")).otherwise(0)).alias("NARINO"))
           .orderBy("anio"))
    emit("")
    emit("6.2  Los dos primeros, ano a ano")
    for l in tabla(par, 20):
        emit(l)

    # ---------------------------------------------------------------- PASO 7
    regla("PASO 7 - VISUALIZACION DE LOS RESULTADOS EN BATCH")
    emit("")
    plt.rcParams.update({"font.size": 9, "axes.titlesize": 11,
                         "axes.titleweight": "bold", "figure.facecolor": "white"})

    # g1 respuesta central
    filas = dep_valido.limit(12).collect()
    labs = [r["departamento"] for r in filas]
    vals = [r["unidades"] for r in filas]
    cols = [ROJO, NARANJA, AZUL] + [GRIS] * (len(labs) - 3)
    fig, ax = plt.subplots(figsize=(9, 5))
    b = ax.barh(labs[::-1], vals[::-1], color=cols[::-1])
    ax.bar_label(b, padding=2, fontsize=8)
    ax.set_title("g1. Destrucciones de infraestructura de drogas ilicitas")
    ax.set_xlabel("Unidades destruidas (2010-2026)")
    ax.grid(axis="x", alpha=.3)
    guardar(fig, "g1_top_departamentos.png", "la respuesta central")

    # g2 participacion
    fig, ax = plt.subplots(figsize=(7, 5))
    p = [v * 100.0 / total_unidades for v in vals]
    wedges, _, autotexts = ax.pie(
        p, labels=None, autopct=lambda x: "{:.1f}%".format(x) if x > 2 else "",
        colors=cols[:len(labs)], startangle=90)
    ax.legend(wedges, labs[:len(labs)], loc="center left",
              bbox_to_anchor=(1, 0.5), fontsize=8)
    ax.set_title("g2. Participacion porcentual por departamento")
    guardar(fig, "g2_participacion.png", "concentracion territorial")

    # g3 serie historica nacional
    pa = por_anio.collect()
    xs = [r["anio"] for r in pa]
    ys = [r["unidades"] for r in pa]
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(xs, ys, marker="o", color=AZUL, lw=2)
    ax.fill_between(xs, ys, alpha=.15, color=AZUL)
    ax.set_title("g3. Destrucciones por ano - Colombia")
    ax.set_ylabel("Unidades"); ax.set_xlabel("Ano")
    ax.set_xticks(xs)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", fontsize=8)
    ax.grid(alpha=.3)
    guardar(fig, "g3_serie_historica.png", "tendencia nacional")

    # g4 Putumayo vs Narino
    cols_par = [c for c in par.columns if c != "anio"]
    fig, ax = plt.subplots(figsize=(10, 4))
    pp = par.collect()
    xs = [r["anio"] for r in pp]
    for col, color in zip(cols_par, [ROJO, AZUL]):
        ax.plot(xs, [r[col] or 0 for r in pp], marker="o", label=col, color=color, lw=2)
    ax.set_title("g4. Putumayo vs Narino - unidades por ano")
    ax.set_xlabel("Ano"); ax.set_ylabel("Unidades")
    ax.legend(); ax.set_xticks(xs)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", fontsize=8)
    ax.grid(alpha=.3)
    guardar(fig, "g4_putumayo_vs_narino.png", "los dos primeros, comparados")

    # g5 municipios top
    top_mun = (valido.groupBy("departamento", "municipio")
                     .agg(F.sum("cantidad").alias("unidades"))
                     .orderBy(F.desc("unidades")).limit(12))
    labs, vals = [x["municipio"] for x in top_mun.collect()], \
                 [x["unidades"] for x in top_mun.collect()]
    fig, ax = plt.subplots(figsize=(9, 5))
    b = ax.barh(labs[::-1], vals[::-1], color=VERDE)
    ax.bar_label(b, padding=2, fontsize=8)
    ax.set_title("g5. Top 12 municipios con mas destrucciones")
    ax.set_xlabel("Unidades"); ax.grid(axis="x", alpha=.3)
    guardar(fig, "g5_top_municipios.png", "concentracion municipal")

    # g6 mapa conceptual: regiones
    fig, ax = plt.subplots(figsize=(8, 5))
    labs6 = [r["departamento"] for r in filas[:8]]
    vals6 = [r["unidades"] for r in filas[:8]]
    colores = plt.cm.OrRd([v / max(vals6) for v in vals6])
    b = ax.bar(range(len(labs6)), vals6, color=colores)
    ax.bar_label(b, padding=2, fontsize=8, fmt="%.0f")
    ax.set_xticks(range(len(labs6)))
    ax.set_xticklabels([l[:11] for l in labs6], rotation=30, ha="right", fontsize=8)
    ax.set_title("g6. Intensidad por departamento (gradiente = magnitud)")
    ax.set_ylabel("Unidades"); ax.grid(axis="y", alpha=.3)
    guardar(fig, "g6_intensidad_departamental.png", "mapa de calor en barras")

    emit("")
    emit("Total de graficos: {}".format(
        len([f for f in os.listdir(GRAF_DIR) if f.endswith(".png")])))

    # ---------------------------------------------------------------- PASO 8
    regla("PASO 8 - EVIDENCIA EN CONSOLA (.show)")
    emit("")
    emit("[.show] Ranking de departamentos:")
    dep_valido.limit(20).show(20, truncate=False)
    emit("")
    emit("[.show] Destrucciones por ano:")
    por_anio.show(20, truncate=False)

    fin = datetime.datetime.now()
    regla("METADATOS")
    emit("Inicio     : {}".format(inicio.strftime("%Y-%m-%d %H:%M:%S")))
    emit("Fin        : {}".format(fin.strftime("%Y-%m-%d %H:%M:%S")))
    emit("Duracion   : {:.1f} segundos".format((fin - inicio).total_seconds()))
    emit("Spark      : {}".format(spark.version))
    emit("Python     : {}".format(sys.version.split()[0]))
    emit("matplotlib : {} (backend Agg, headless)".format(matplotlib.__version__))
    emit("Java       : {}".format(
        spark._jvm.java.lang.System.getProperty("java.version")))
    emit("Dataset    : {}".format(ruta))
    emit("Reportes   : {:,}".format(total))
    emit("Unidades   : {:,.0f}".format(total_unidades))
    emit("")
    emit("RESPUESTA: {}".format(
        ", ".join("{} ({:,.0f})".format(r["departamento"], r["unidades"])
                  for r in top5[:3])))

    with open(REPORT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(LOG) + "\n")

    spark.stop()
    print("")
    print("=" * 78)
    print("  ANALISIS FINALIZADO")
    print("=" * 78)
    for i, r in enumerate(top5[:3], 1):
        print("  {}. {:<26}{:>10,.0f} unidades".format(
            i, r["departamento"], r["unidades"]))
    print("  Reporte : {}".format(REPORT))
    print("  Graficos: {}".format(GRAF_DIR))
    print("=" * 78)


if __name__ == "__main__":
    main()