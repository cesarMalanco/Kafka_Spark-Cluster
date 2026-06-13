#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="Node1/docker-compose.yml"
MASTER_URL="spark://192.168.1.101:7077"
DATA_DIR="/opt/spark-data"

printf "Ejecutando Spark en master %s\n" "$MASTER_URL"

docker compose -f "$COMPOSE_FILE" exec -T spark spark-shell --master "$MASTER_URL" <<'EOF'
import org.apache.spark.sql.functions._
import spark.implicits._

spark.sparkContext.setLogLevel("WARN")

println("\n=== EJEMPLO BÁSICO DE SPARK ===")
spark.range(5).show()

println("\n=== EJEMPLO DE DATAFRAME CREADO EN MEMORIA ===")
val datos = Seq(("Ana", 10), ("Pepito", 20), ("Martha", 30))
val df = datos.toDF("Nombre", "Edad")
df.show()
df.printSchema()
df.select("Nombre").show()

println("\n=== LECTURA DE DATOS GENERADOS POR EL PRODUCTOR ===")
val personas = spark.read
  .option("multiLine", "true")
  .json(s"$DATA_DIR/personas.json")
val ventas = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(s"$DATA_DIR/ventas.csv")
val sensores = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(s"$DATA_DIR/sensores.csv")
val transacciones = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(s"$DATA_DIR/transacciones.csv")
val logs = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(s"$DATA_DIR/logs-sistema.csv")

println("\n=== ESQUEMAS DE LOS ARCHIVOS DE PRODUCTOR ===")
personas.printSchema()
ventas.printSchema()
sensores.printSchema()
transacciones.printSchema()
logs.printSchema()

println("\n=== RECUENTO DE REGISTROS POR DATASET ===")
println(s"personas: ${personas.count}")
println(s"ventas: ${ventas.count}")
println(s"sensores: ${sensores.count}")
println(s"transacciones: ${transacciones.count}")
println(s"logs: ${logs.count}")

println("\n=== TOP 15 PERSONAS ACTIVAS POR INGRESO MENSUAL ===")
personas.filter(col("activo") === true)
  .orderBy(desc("ingreso_mensual"))
  .select("id_persona", "nombre", "apellido", "ciudad", "ocupacion", "ingreso_mensual")
  .show(15, false)

println("\n=== CUENTA DE PERSONAS POR CIUDAD ===")
personas.groupBy("ciudad")
  .count()
  .orderBy(desc("count"))
  .show(10, false)

println("\n=== VENTAS POR METODO DE PAGO ===")
ventas.groupBy("metodo_pago")
  .agg(count("*").alias("total_ventas"), sum("total").alias("monto_total"), avg("total").alias("promedio_total"))
  .orderBy(desc("monto_total"))
  .show(10, false)

println("\n=== VENTAS POR CATEGORIA ===")
ventas.groupBy("categoria")
  .agg(count("*").alias("ventas"), sum("total").alias("monto_total"))
  .orderBy(desc("monto_total"))
  .show(10, false)

println("\n=== ESTADO PROMEDIO DE SENSORES ===")
sensores.groupBy("estado")
  .agg(
    avg("valor").alias("avg_valor"),
    avg("temperatura").alias("avg_temperatura"),
    avg("humedad").alias("avg_humedad")
  )
  .orderBy(desc("avg_valor"))
  .show(10, false)

println("\n=== SENSOR POR UBICACION CON VALOR PROMEDIO ALTO ===")
sensores.groupBy("ubicacion")
  .agg(avg("valor").alias("avg_valor"), avg("temperatura").alias("avg_temperatura"))
  .orderBy(desc("avg_valor"))
  .show(10, false)

println("\n=== TRANSACCIONES POR ESTADO ===")
transacciones.groupBy("estado")
  .agg(count("*").alias("total_transacciones"), sum("monto").alias("monto_total"))
  .orderBy(desc("monto_total"))
  .show(10, false)

println("\n=== TRANSACCIONES POR BANCO DESTINO ===")
transacciones.groupBy("banco_destino")
  .agg(count("*").alias("total_transacciones"), sum("monto").alias("monto_total"))
  .orderBy(desc("monto_total"))
  .show(10, false)

println("\n=== LOGS POR NIVEL Y MÓDULO ===")
logs.groupBy("nivel", "modulo")
  .count()
  .orderBy(desc("count"))
  .show(20, false)

println("\n=== CÓDIGOS DE ERROR MÁS FRECUENTES ===")
logs.groupBy("codigo_error")
  .count()
  .orderBy(desc("count"))
  .show(20, false)

println("\n=== JOIN VENTAS + PERSONAS PARA INFORMACIÓN DE CLIENTE ===")
val ventasPersonas = ventas.join(personas, ventas("id_persona") === personas("id_persona"), "inner")
ventasPersonas
  .select(
    ventas("id_venta"),
    personas("nombre"),
    personas("apellido"),
    ventas("producto"),
    ventas("total"),
    ventas("metodo_pago"),
    personas("ciudad")
  )
  .orderBy(desc("total"))
  .show(20, false)

println("\n=== CONSULTA SQL DIRECTA SOBRE PERSONAS ===")
personas.createOrReplaceTempView("personas")
val personasActivas = spark.sql(
  "SELECT ciudad, COUNT(*) AS cantidad, AVG(ingreso_mensual) AS ingreso_promedio " +
  "FROM personas WHERE activo = true GROUP BY ciudad ORDER BY ingreso_promedio DESC"
)
personasActivas.show(10, false)

println("\n=== CONSULTA SQL DIRECTA SOBRE VENTAS ===")
ventas.createOrReplaceTempView("ventas")
val resumenVentas = spark.sql(
  "SELECT metodo_pago, COUNT(*) AS total_ventas, SUM(total) AS monto_total, AVG(total) AS promedio_total " +
  "FROM ventas GROUP BY metodo_pago ORDER BY monto_total DESC"
)
resumenVentas.show(10, false)
EOF

echo "Proceso Spark terminado. Revisa la salida anterior para los resultados."
