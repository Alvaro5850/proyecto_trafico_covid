import os
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


#Rutas de salida

CARPETA_SALIDA = Path("output")
CARPETA_GRAFICAS_AVANZADAS = CARPETA_SALIDA / "graficas_avanzadas"
CARPETA_GRAFICAS_AVANZADAS.mkdir(parents=True, exist_ok=True)

plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams["font.size"] = 11



#Lectura de datos


def leer_csv_spark(carpeta):
    """Lee todos los CSV de una carpeta generada por Spark y devuelve un único DataFrame de Pandas con todos los datos concatenados."""
    rutas_csv = []

    for nombre in os.listdir(carpeta):
        if nombre.endswith(".csv"):
            rutas_csv.append(Path(carpeta) / nombre)

    if len(rutas_csv) == 0:
        raise FileNotFoundError("No se han encontrado archivos CSV en " + str(carpeta))

    dataframes = []

    for ruta in rutas_csv:
        df = pd.read_csv(ruta)
        dataframes.append(df)

    df_final = pd.concat(dataframes, ignore_index=True)
    return df_final


def cargar_datos_base():
    """
    Carga las tablas agregadas producidas por Spark.
    """
    resumen_periodos = leer_csv_spark(CARPETA_SALIDA / "resumen_periodos")
    intensidad_mensual = leer_csv_spark(CARPETA_SALIDA / "intensidad_mensual")
    curva_horaria = leer_csv_spark(CARPETA_SALIDA / "curva_horaria")
    intensidad_diaria = leer_csv_spark(CARPETA_SALIDA / "intensidad_diaria")

    return resumen_periodos, intensidad_mensual, curva_horaria, intensidad_diaria



#Funciones de apoyo

def obtener_periodo_covid_por_fecha(fecha):
    """Asigna un periodo COVID según la fecha"""
    fecha_normalizada = fecha.normalize()

    if fecha_normalizada < pd.Timestamp("2020-03-15"):
        return "Pre-COVID"
    if pd.Timestamp("2020-03-15") <= fecha_normalizada <= pd.Timestamp("2020-06-21"):
        return "Confinamiento"
    if pd.Timestamp("2020-06-22") <= fecha_normalizada <= pd.Timestamp("2020-10-25"):
        return "Desescalada"
    if pd.Timestamp("2020-10-26") <= fecha_normalizada <= pd.Timestamp("2021-05-09"):
        return "2º estado de alarma"

    return "Post-restricciones"


def obtener_tipo_dia(dia_semana):
    """Clasifica el día como Laborable, Sábado o Domingo. dayofweek: 0 = lunes, 6 = domingo"""
    if dia_semana < 5:
        return "Laborable"
    if dia_semana == 5:
        return "Sábado"
    return "Domingo"



#Gráficas avanzadas

def grafica_heatmap_mes_vs_anio(intensidad_mensual):
    """Mapa de calor: Mes (filas) vs Año (columnas) con intensidad media."""
    tabla_pivote = intensidad_mensual.pivot(
        index="month",
        columns="year",
        values="intensidad_media"
    ).sort_index()

    figura, ejes = plt.subplots()
    imagen = ejes.imshow(tabla_pivote.values, aspect="auto", cmap="viridis")

    figura.colorbar(imagen, ax=ejes, label="Intensidad media (vehículos/hora)")

    ejes.set_xticks(range(len(tabla_pivote.columns)))
    ejes.set_xticklabels(tabla_pivote.columns)

    ejes.set_yticks(range(len(tabla_pivote.index)))
    ejes.set_yticklabels(tabla_pivote.index)

    ejes.set_xlabel("Año")
    ejes.set_ylabel("Mes")
    ejes.set_title("Mapa de calor: intensidad media por mes y año")

    plt.tight_layout()
    ruta = CARPETA_GRAFICAS_AVANZADAS / "01_heatmap_mes_vs_anio.png"
    plt.savefig(ruta, dpi=220)
    plt.close()


def grafica_heatmap_hora_vs_periodo(curva_horaria):
    """Mapa de calor: Hora (filas) vs Periodo COVID (columnas)."""
    orden_periodos = [
        "Pre-COVID",
        "Confinamiento",
        "Desescalada",
        "2º estado de alarma",
        "Post-restricciones"
    ]

    datos = curva_horaria.copy()
    datos["periodo_covid"] = pd.Categorical(
        datos["periodo_covid"],
        categories=orden_periodos,
        ordered=True
    )
    datos = datos.sort_values(["periodo_covid", "hour"])

    tabla_pivote = datos.pivot(
        index="hour",
        columns="periodo_covid",
        values="intensidad_media"
    )

    figura, ejes = plt.subplots()
    imagen = ejes.imshow(tabla_pivote.values, aspect="auto", cmap="plasma")

    figura.colorbar(imagen, ax=ejes, label="Intensidad media (vehículos/hora)")

    ejes.set_xticks(range(len(tabla_pivote.columns)))
    ejes.set_xticklabels(tabla_pivote.columns, rotation=15)

    ejes.set_yticks(range(len(tabla_pivote.index)))
    ejes.set_yticklabels(tabla_pivote.index)

    ejes.set_xlabel("Periodo COVID")
    ejes.set_ylabel("Hora del día")
    ejes.set_title("Mapa de calor: intensidad por hora y periodo COVID")

    plt.tight_layout()
    ruta = CARPETA_GRAFICAS_AVANZADAS / "02_heatmap_hora_vs_periodo.png"
    plt.savefig(ruta, dpi=220)
    plt.close()


def grafica_boxplot_por_periodo(intensidad_diaria):
    """Boxplot: distribución de la intensidad media diaria por periodo COVID."""
    datos = intensidad_diaria.copy()
    datos["fecha_dia"] = pd.to_datetime(datos["fecha_dia"])
    datos["periodo_covid"] = datos["fecha_dia"].apply(obtener_periodo_covid_por_fecha)

    orden_periodos = [
        "Pre-COVID",
        "Confinamiento",
        "Desescalada",
        "2º estado de alarma",
        "Post-restricciones"
    ]

    listas_por_periodo = []

    for periodo in orden_periodos:
        serie = datos.loc[datos["periodo_covid"] == periodo, "intensidad_media_dia"].dropna()
        listas_por_periodo.append(serie)

    figura, ejes = plt.subplots()
    ejes.boxplot(listas_por_periodo, labels=orden_periodos, showfliers=False)

    ejes.set_ylabel("Intensidad media diaria (vehículos/hora)")
    ejes.set_title("Distribución de intensidad diaria por periodo COVID")
    ejes.grid(axis="y", alpha=0.3)

    plt.xticks(rotation=15)
    plt.tight_layout()

    ruta = CARPETA_GRAFICAS_AVANZADAS / "03_boxplot_por_periodo.png"
    plt.savefig(ruta, dpi=220)
    plt.close()


def grafica_laborables_vs_fin_de_semana(intensidad_diaria):
    """Barras: intensidad media diaria según tipo de día (laborable, sábado, domingo)."""
    datos = intensidad_diaria.copy()
    datos["fecha_dia"] = pd.to_datetime(datos["fecha_dia"])
    datos["dia_semana"] = datos["fecha_dia"].dt.dayofweek
    datos["tipo_dia"] = datos["dia_semana"].apply(obtener_tipo_dia)

    resumen = (
        datos.groupby("tipo_dia", as_index=False)["intensidad_media_dia"]
        .mean()
        .rename(columns={"intensidad_media_dia": "intensidad_media"})
    )

    orden = ["Laborable", "Sábado", "Domingo"]
    resumen["tipo_dia"] = pd.Categorical(resumen["tipo_dia"], categories=orden, ordered=True)
    resumen = resumen.sort_values("tipo_dia")

    figura, ejes = plt.subplots()
    ejes.bar(resumen["tipo_dia"], resumen["intensidad_media"])

    ejes.set_xlabel("Tipo de día")
    ejes.set_ylabel("Intensidad media diaria (vehículos/hora)")
    ejes.set_title("Comparativa de intensidad: laborables vs fin de semana")
    ejes.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    ruta = CARPETA_GRAFICAS_AVANZADAS / "04_laborables_vs_fin_de_semana.png"
    plt.savefig(ruta, dpi=220)
    plt.close()


def grafica_top_10_dias_mas_intensos(intensidad_diaria):
    """Barras horizontales: Top 10 días con mayor intensidad media diaria."""
    datos = intensidad_diaria.copy()
    datos["fecha_dia"] = pd.to_datetime(datos["fecha_dia"])

    datos = datos.sort_values("intensidad_media_dia", ascending=False)
    datos = datos.head(10)
    datos = datos.sort_values("intensidad_media_dia", ascending=True)

    figura, ejes = plt.subplots()
    ejes.barh(datos["fecha_dia"].dt.strftime("%Y-%m-%d"), datos["intensidad_media_dia"])

    ejes.set_xlabel("Intensidad media diaria (vehículos/hora)")
    ejes.set_ylabel("Fecha")
    ejes.set_title("Top 10 días con mayor intensidad media")

    plt.tight_layout()
    ruta = CARPETA_GRAFICAS_AVANZADAS / "05_top_10_dias_mas_intensos.png"
    plt.savefig(ruta, dpi=220)
    plt.close()


def grafica_intensidad_media_anual(intensidad_mensual):
    """Línea: evolución de la intensidad media anual."""
    intensidad_anual = (
        intensidad_mensual
        .groupby("year", as_index=False)["intensidad_media"]
        .mean()
        .rename(columns={"intensidad_media": "intensidad_media_anual"})
    ).sort_values("year")

    figura, ejes = plt.subplots()
    ejes.plot(
        intensidad_anual["year"],
        intensidad_anual["intensidad_media_anual"],
        marker="o",
        linestyle="-"
    )

    for anio, valor in zip(intensidad_anual["year"], intensidad_anual["intensidad_media_anual"]):
        ejes.text(anio, valor, f"{valor:.0f}", ha="center", va="bottom", fontsize=9)

    ejes.set_xlabel("Año")
    ejes.set_ylabel("Intensidad media anual (vehículos/hora)")
    ejes.set_title("Evolución de la intensidad media anual")
    ejes.grid(True, alpha=0.3)

    plt.tight_layout()
    ruta = CARPETA_GRAFICAS_AVANZADAS / "06_intensidad_media_anual.png"
    plt.savefig(ruta, dpi=220)
    plt.close()


#Ejecución principal

def main():
    resumen_periodos, intensidad_mensual, curva_horaria, intensidad_diaria = cargar_datos_base()

    grafica_heatmap_mes_vs_anio(intensidad_mensual)
    grafica_heatmap_hora_vs_periodo(curva_horaria)
    grafica_boxplot_por_periodo(intensidad_diaria)
    grafica_laborables_vs_fin_de_semana(intensidad_diaria)
    grafica_top_10_dias_mas_intensos(intensidad_diaria)
    grafica_intensidad_media_anual(intensidad_mensual)

    print("Proceso completado: gráficas avanzadas generadas en output/graficas_avanzadas.")


if __name__ == "__main__":
    main()
