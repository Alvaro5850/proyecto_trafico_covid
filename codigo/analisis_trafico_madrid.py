from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, avg, expr,
    to_timestamp, year, month, dayofmonth, hour,
    date_format
)


def main():
    spark = (
        SparkSession.builder
        .appName("Analisis_trafico_Madrid")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")
        .csv("data/raw/*/*.csv")
    )

    print("👉 Filas leídas inicialmente:", df.count())
    print("👉 Columnas:", df.columns)

    df = df.select(
        "id",
        "fecha",
        "tipo_elem",
        "intensidad",
        "ocupacion",
        "carga",
        "vmed",
        "error",
        "periodo_integracion"
    )

    # ❗ IMPORTANTE: formato correcto yyyy-MM-dd HH:mm:ss
    df = df.withColumn("fecha_ts", to_timestamp(col("fecha"), "yyyy-MM-dd HH:mm:ss"))

    print("👉 Ejemplo de fechas parseadas:")
    df.select("fecha", "fecha_ts").show(5, truncate=False)

    df = df.withColumn("intensidad", col("intensidad").cast("double"))
    df = df.filter(col("intensidad").isNotNull())
    print("👉 Filas tras filtrar intensidad no nula:", df.count())

    df = df.withColumn("year", year("fecha_ts"))
    df = df.withColumn("month", month("fecha_ts"))
    df = df.withColumn("day", dayofmonth("fecha_ts"))
    df = df.withColumn("hour", hour("fecha_ts"))

    df = df.filter((col("year") >= 2019) & (col("year") <= 2022))
    print("👉 Filas tras filtrar años 2019–2022:", df.count())

    df = df.withColumn("fecha_ymd", date_format(col("fecha_ts"), "yyyy-MM-dd"))
    df = df.withColumn("fecha_dia", col("fecha_ymd"))

    print("👉 Filas listas para agrupar (2019–2022, intensidad!=null):", df.count())

    df = df.withColumn(
        "periodo_covid",
        when(col("fecha_ymd") < lit("2020-03-15"), "Pre-COVID")
        .when(
            (col("fecha_ymd") >= lit("2020-03-15")) &
            (col("fecha_ymd") <= lit("2020-06-21")),
            "Confinamiento"
        )
        .when(
            (col("fecha_ymd") >= lit("2020-06-22")) &
            (col("fecha_ymd") <= lit("2020-10-25")),
            "Desescalada"
        )
        .when(
            (col("fecha_ymd") >= lit("2020-10-26")) &
            (col("fecha_ymd") <= lit("2021-05-09")),
            "2º estado de alarma"
        )
        .otherwise("Post-restricciones")
    )

    df.cache()
    print("👉 Filas totales (con periodo_covid):", df.count())

    resumen_periodos = (
        df.groupBy("periodo_covid")
        .agg(
            avg("intensidad").alias("intensidad_media"),
            expr("percentile_approx(intensidad, 0.5)").alias("intensidad_mediana"),
        )
        .orderBy("periodo_covid")
    )
    resumen_periodos.write.mode("overwrite").option("header", "true") \
        .csv("output/resumen_periodos")

    intensidad_mensual = (
        df.groupBy("year", "month")
        .agg(avg("intensidad").alias("intensidad_media"))
        .orderBy("year", "month")
    )
    intensidad_mensual.write.mode("overwrite").option("header", "true") \
        .csv("output/intensidad_mensual")

    curva_horaria = (
        df.groupBy("periodo_covid", "hour")
        .agg(avg("intensidad").alias("intensidad_media"))
        .orderBy("periodo_covid", "hour")
    )
    curva_horaria.write.mode("overwrite").option("header", "true") \
        .csv("output/curva_horaria")

    intensidad_diaria = (
        df.groupBy("fecha_dia")
        .agg(avg("intensidad").alias("intensidad_media_dia"))
        .orderBy("fecha_dia")
    )
    intensidad_diaria.write.mode("overwrite").option("header", "true") \
        .csv("output/intensidad_diaria")

    print("✅ Spark terminado. CSV generados en carpeta 'output/'.")
    spark.stop()


if __name__ == "__main__":
    main()
