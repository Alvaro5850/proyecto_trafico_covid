from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, avg, expr, to_timestamp, year, month, hour, date_format


def main():
    SesionSpark = (
        SparkSession.builder
        .appName("Analisis_trafico_Madrid")
        .getOrCreate()
    )
    SesionSpark.sparkContext.setLogLevel("WARN")

    Datos = (
        SesionSpark.read
        .option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")
        .csv("data/raw/*/*.csv")
    )

    ColumnasNecesarias = [
        "id", "fecha", "tipo_elem", "intensidad",
        "ocupacion", "carga", "vmed", "error", "periodo_integracion"
    ]
    Datos = Datos.select(*ColumnasNecesarias)

    Datos = Datos.withColumn("FechaTimestamp", to_timestamp(col("fecha"), "yyyy-MM-dd HH:mm:ss"))

    Datos = Datos.withColumn("Intensidad", col("intensidad").cast("double"))

    Datos = Datos.filter(col("FechaTimestamp").isNotNull())
    Datos = Datos.filter(col("Intensidad").isNotNull())

    if "error" in Datos.columns:
        Datos = Datos.filter(col("error") == lit("N"))

    Datos = Datos.withColumn("Anio", year(col("FechaTimestamp")))
    Datos = Datos.withColumn("Mes", month(col("FechaTimestamp")))
    Datos = Datos.withColumn("Hora", hour(col("FechaTimestamp")))

    Datos = Datos.filter((col("Anio") >= lit(2019)) & (col("Anio") <= lit(2022)))

    Datos = Datos.withColumn("FechaDia", date_format(col("FechaTimestamp"), "yyyy-MM-dd"))

    Datos = Datos.withColumn(
        "PeriodoCovid",
        when(col("FechaDia") < lit("2020-03-15"), lit("Pre-COVID"))
        .when((col("FechaDia") >= lit("2020-03-15")) & (col("FechaDia") <= lit("2020-06-21")), lit("Confinamiento"))
        .when((col("FechaDia") >= lit("2020-06-22")) & (col("FechaDia") <= lit("2020-10-25")), lit("Desescalada"))
        .when((col("FechaDia") >= lit("2020-10-26")) & (col("FechaDia") <= lit("2021-05-09")), lit("2º estado de alarma"))
        .otherwise(lit("Post-restricciones"))
    )

    Datos.cache()

    ResumenPeriodos = (
        Datos.groupBy("PeriodoCovid")
        .agg(
            avg(col("Intensidad")).alias("IntensidadMedia"),
            expr("percentile_approx(Intensidad, 0.5)").alias("IntensidadMediana")
        )
        .orderBy("PeriodoCovid")
    )
    ResumenPeriodos.write.mode("overwrite").option("header", "true").csv("output/resumen_periodos")

    IntensidadMensual = (
        Datos.groupBy("Anio", "Mes")
        .agg(avg(col("Intensidad")).alias("IntensidadMedia"))
        .orderBy("Anio", "Mes")
    )
    IntensidadMensual.write.mode("overwrite").option("header", "true").csv("output/intensidad_mensual")

    CurvaHoraria = (
        Datos.groupBy("PeriodoCovid", "Hora")
        .agg(avg(col("Intensidad")).alias("IntensidadMedia"))
        .orderBy("PeriodoCovid", "Hora")
    )
    CurvaHoraria.write.mode("overwrite").option("header", "true").csv("output/curva_horaria")

    IntensidadDiaria = (
        Datos.groupBy("FechaDia")
        .agg(avg(col("Intensidad")).alias("IntensidadMediaDia"))
        .orderBy("FechaDia")
    )
    IntensidadDiaria.write.mode("overwrite").option("header", "true").csv("output/intensidad_diaria")

    SesionSpark.stop()


if __name__ == "__main__":
    main()
