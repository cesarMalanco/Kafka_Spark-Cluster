from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, max, min, stddev, explode, split, lower, col
import time
import json
from collections import Counter

MASTER_IP = "spark-master"

spark = SparkSession.builder \
    .appName("Ciberseguridad") \
    .master(f"spark://{MASTER_IP}:7077") \
    .config("spark.driver.host", "192.168.1.101") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.driver.port", "7001") \
    .config("spark.blockManager.port", "7002") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

print("\n>>> INFORMACIÓN DEL CLÚSTER")
print("-" * 60)
print(f"  Master URL         : {sc.master}")
print(f"  Aplicación ID      : {sc.applicationId}")
print(f"  Executors activos  : {len(sc._jsc.sc().statusTracker().getExecutorInfos())}")

print("\n>>> [PROCESAMIENTO JSON] CARGANDO DATOS DE CIBERSEGURIDAD DESDE /opt/spark/data/datos_generados/datos_seguridad.json")
print("-" * 60)

t_inicio_total = time.time()

t0 = time.time()
df = spark.read.option("multiLine", "true").json("/opt/spark/data/datos_generados/datos_seguridad.json")
total = df.count()
t1 = time.time()

print(f"  Registros cargados : {total:,}")
print(f"  Tiempo de carga    : {round(t1 - t0, 3)}s")

print("\n[1] [AGREGACIÓN SIMPLE] CONTEO DE EVENTOS POR TIPO DE ATAQUE")
print("-" * 60)
t0 = time.time()
df.groupBy("tipo_ataque").count().orderBy("count", ascending=False).show(15)
t1 = time.time()
print(f"  Tiempo de ejecución: {round(t1 - t0, 3)}s")

print("\n[2] [ANÁLISIS DE DATOS] DISTRIBUCIÓN DE SEVERIDAD")
print("-" * 60)
t0 = time.time()
df.groupBy("severidad").count().orderBy("count", ascending=False).show()
t1 = time.time()
print(f"  Tiempo de ejecución: {round(t1 - t0, 3)}s")

print("\n[3] [AGREGACIÓN COMPLETA] ESTADÍSTICAS DE PROTOCOLOS")
print("-" * 60)
t0 = time.time()
df.groupBy("protocolo") \
  .agg(
      count("id").alias("total_eventos"),
      avg("bytes_transferidos").alias("bytes_promedio"),
      max("bytes_transferidos").alias("bytes_maximo"),
      min("bytes_transferidos").alias("bytes_minimo")
  ) \
  .orderBy("total_eventos", ascending=False) \
  .show()
t1 = time.time()
print(f"  Tiempo de ejecución: {round(t1 - t0, 3)}s")

print("\n[4] [ANÁLISIS DE DATOS] IPs DE ORIGEN MÁS ACTIVAS (Top 20)")
print("-" * 60)
t0 = time.time()
df.groupBy("ip_origen").count().orderBy("count", ascending=False).show(20)
t1 = time.time()
print(f"  Tiempo de ejecución: {round(t1 - t0, 3)}s")

print("\n[5] [ANÁLISIS DE DATOS] ANÁLISIS POR USUARIO INVOLUCRADO")
print("-" * 60)
t0 = time.time()
df.groupBy("usuario").count().orderBy("count", ascending=False).show(15)
t1 = time.time()
print(f"  Tiempo de ejecución: {round(t1 - t0, 3)}s")

print("\n[6] [AGREGACIÓN] SISTEMAS DESTINO Y ESTADO DE EVENTOS")
print("-" * 60)
t0 = time.time()
df.groupBy("sistema_destino", "estado") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(20)
t1 = time.time()
print(f"  Tiempo de ejecución: {round(t1 - t0, 3)}s")

t_fin_total_agg = time.time()
tiempo_total_agg = round(t_fin_total_agg - t_inicio_total, 3)

print("\n\n" + "=" * 60)
print("  CONTEO DE PALABRAS DISTRIBUIDO")
print("=" * 60)

t_inicio_palabras = time.time()

print("\n[7] [CONTEO DE PALABRAS] TOP 20 PALABRAS MÁS FRECUENTES EN PAYLOADS")
print("-" * 60)
t0 = time.time()
palabras_top = df.select(
    explode(split(lower(col("payload")), r"[^a-z0-9]+")).alias("palabra")
) \
    .filter(col("palabra") != "") \
    .groupBy("palabra") \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(20)

palabras_top.show(20, truncate=False)
t1 = time.time()
print(f"  Tiempo de ejecución: {round(t1 - t0, 3)}s")

print("\n[8] [CONTEO DE PALABRAS] PALABRAS ÚNICAS POR TIPO DE ATAQUE")
print("-" * 60)
t0 = time.time()
df_temp = df.select("tipo_ataque", 
                     explode(split(lower(col("payload")), r"[^a-z0-9]+")).alias("palabra")) \
    .filter(col("palabra") != "")
palabras_por_tipo = df_temp.groupBy("tipo_ataque").count().orderBy("count", ascending=False)
palabras_por_tipo.show(15, truncate=False)
t1 = time.time()
print(f"  Tiempo de ejecución: {round(t1 - t0, 3)}s")

t_fin_palabras = time.time()
tiempo_conteo_palabras = round(t_fin_palabras - t_inicio_palabras, 3)

# ============================================================================
# INFORMACIÓN DE DISTRIBUCIÓN ENTRE WORKERS
# ============================================================================
print("\n\n" + "=" * 60)
print("  DISTRIBUCIÓN DE CARGA ENTRE WORKERS")
print("=" * 60)

executors = sc._jsc.sc().statusTracker().getExecutorInfos()
print(f"\n  Total de Executors: {len(executors)}")
print(f"  Clúster: 1 Master + 2 Workers")
print(f"  Particiones por operación: {df.rdd.getNumPartitions()}")
print(f"  Distribución automática: {df.rdd.getNumPartitions()} particiones / 2 workers = {df.rdd.getNumPartitions() / 2:.1f} particiones por worker")

# ============================================================================
# RESUMEN COMPLETO
# ============================================================================
print("\n\n" + "=" * 80)
print(" " * 15 + "RESUMEN FINAL - ANÁLISIS DISTRIBUIDO vs LOCAL")
print("=" * 80)

print(f"\nPRUEBA 1 - AGREGACIONES (6 análisis)      : {tiempo_total_agg}s (DISTRIBUIDO)")
print(f"PRUEBA 2 - CONTEO DE PALABRAS (4 análisis) : {tiempo_conteo_palabras}s (DISTRIBUIDO)")

tiempo_distribuido_total = tiempo_total_agg + tiempo_conteo_palabras

print(f"\nTIEMPO TOTAL DISTRIBUIDO (2 Pruebas)     : {tiempo_distribuido_total}s")
print(f"DATOS PROCESADOS                         : {total:,} registros")
print(f"STATUS                                    : EJECUCIÓN EXITOSA EN CLÚSTER")
print(f"CONFIGURACIÓN                            : 1 Master + 2 Workers")
print(f"DISTRIBUCIÓN                              : {df.rdd.getNumPartitions()} particiones → 2 workers")

print("\n" + "=" * 80)

spark.stop()