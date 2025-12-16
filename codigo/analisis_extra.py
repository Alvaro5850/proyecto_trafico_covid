#Asignatura:Grandes Volúmenes de Datos
#Tarea: PL Final Analisis del Trafico en Madrid durante COVID-19
#Integrantes: Álvaro Salvador, Pablo Cuevas, José Luis Blázquez, Jorge Esgueva
import os
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

#Rutas base

BASE_OUTPUT = Path("output")
CARPETA_GRAFICAS_EXTRA = BASE_OUTPUT / "graficas_extra"
EXCEL_SALIDA = BASE_OUTPUT / "resultados_trafico_completo.xlsx"

CARPETA_GRAFICAS_EXTRA.mkdir(parents=True, exist_ok=True)

plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams["font.size"] = 11

#Contenedor de datos
class DatosProyecto:

    def __init__(self):

        self.df_res = None
        self.df_mensual = None
        self.df_horaria = None
        self.df_diaria = None

        self.df_anual = None
        self.df_lab = None
        self.top_mas = None
        self.top_menos = None
        self.df_comp = None



#Lectura y formato
def leer_csv_unico(carpeta: Path, datos: DatosProyecto, destino: str):

    archivos = []

    for archivo in os.listdir(carpeta):
        if archivo.endswith(".csv"):
            archivos.append(archivo)

    if len(archivos) == 0:
        raise FileNotFoundError("No hay CSV en " + str(carpeta))

    dfs = []
    for nombre in archivos:
        ruta = carpeta / nombre
        df_temp = pd.read_csv(ruta)
        dfs.append(df_temp)

    df_final = pd.concat(dfs, ignore_index=True)

    if destino == "df_res":
        datos.df_res = df_final
    elif destino == "df_mensual":
        datos.df_mensual = df_final
    elif destino == "df_horaria":
        datos.df_horaria = df_final
    elif destino == "df_diaria":
        datos.df_diaria = df_final


def redondear_columnas(df: pd.DataFrame, columnas, decimales: int):

    i = 0

    while i < len(columnas):
        col = columnas[i]
        if col in df.columns:
            df[col] = df[col].astype(float)
            df[col] = df[col].round(decimales)
        i+= 1


def cargar_datos_base(datos: DatosProyecto):

    leer_csv_unico(BASE_OUTPUT / "resumen_periodos", datos, "df_res")
    leer_csv_unico(BASE_OUTPUT / "intensidad_mensual", datos, "df_mensual")
    leer_csv_unico(BASE_OUTPUT / "curva_horaria", datos, "df_horaria")
    leer_csv_unico(BASE_OUTPUT / "intensidad_diaria", datos, "df_diaria")

    redondear_columnas(datos.df_res, ["intensidad_media", "intensidad_mediana"], 2)
    redondear_columnas(datos.df_mensual, ["intensidad_media"], 2)
    redondear_columnas(datos.df_horaria, ["intensidad_media"], 2)
    redondear_columnas(datos.df_diaria, ["intensidad_media_dia"], 2)


#Cálculos extra 
def calcular_intensidad_anual(datos: DatosProyecto):

    df_mensual = datos.df_mensual.copy()

    #Media anual
    df_anual = df_mensual.groupby("year", as_index=False)["intensidad_media"].mean()
    df_anual = df_anual.rename(columns={"intensidad_media": "intensidad_media_anual"})

    #Variación porcentual (año vs anterior)
    df_anual = df_anual.sort_values("year").reset_index(drop=True)
    df_anual["variacion_pct_vs_anio_anterior"] = df_anual["intensidad_media_anual"].pct_change() * 100
    df_anual["variacion_pct_vs_anio_anterior"] = df_anual["variacion_pct_vs_anio_anterior"].round(2)

    datos.df_anual = df_anual


def calcular_laboral_vs_findes(datos: DatosProyecto):

    df = datos.df_diaria.copy()
    df["fecha_dia"] = pd.to_datetime(df["fecha_dia"])
    df["dia_semana"] = df["fecha_dia"].dt.dayofweek  # 0 lunes ... 6 domingo

    #Clasificación explícita 
    tipos = []

    for dia in df["dia_semana"]:

        if dia < 5:
            tipos.append("Laborable")
        elif dia == 5:
            tipos.append("Sábado")
        else:
            tipos.append("Domingo")

    df["tipo_dia"] = tipos

    resumen = df.groupby("tipo_dia", as_index=False)["intensidad_media_dia"].mean()
    resumen = resumen.rename(columns={"intensidad_media_dia": "intensidad_media"})

    #Orden explícito
    orden = ["Laborable", "Sábado", "Domingo"]
    resumen["tipo_dia"] = pd.Categorical(resumen["tipo_dia"], categories=orden, ordered=True)
    resumen = resumen.sort_values("tipo_dia").reset_index(drop=True)

    redondear_columnas(resumen, ["intensidad_media"], 2)

    datos.df_lab = resumen


def calcular_top_dias(datos: DatosProyecto, n: int):

    df = datos.df_diaria.copy()
    df["fecha_dia"] = pd.to_datetime(df["fecha_dia"])

    df_ordenado_desc = df.sort_values("intensidad_media_dia", ascending=False)
    df_ordenado_asc = df.sort_values("intensidad_media_dia", ascending=True)

    datos.top_mas = df_ordenado_desc.head(n).copy()
    datos.top_menos = df_ordenado_asc.head(n).copy()


def calcular_comparativa_pre_post(datos: DatosProyecto):
    df = datos.df_res.copy()

    #Buscar Pre-COVID de forma explícita
    encontrado = False
    valor_pre = None

    for _, fila in df.iterrows():
        if str(fila["periodo_covid"]) == "Pre-COVID":
            valor_pre = float(fila["intensidad_media"])
            encontrado = True

    if encontrado:
        variaciones = []
        for _, fila in df.iterrows():
            actual = float(fila["intensidad_media"])
            pct = ((actual - valor_pre) / valor_pre) * 100
            variaciones.append(round(pct, 2))

        df["variacion_vs_PreCOVID_pct"] = variaciones
    else:
        df["variacion_vs_PreCOVID_pct"] = None

    datos.df_comp = df


#Excel completo
def crear_excel_completo(datos: DatosProyecto):

    writer = pd.ExcelWriter(EXCEL_SALIDA, engine="openpyxl")

    datos.df_res.to_excel(writer, sheet_name="Resumen_periodos", index=False)
    datos.df_mensual.to_excel(writer, sheet_name="Intensidad_mensual", index=False)
    datos.df_horaria.to_excel(writer, sheet_name="Curva_horaria", index=False)
    datos.df_diaria.to_excel(writer, sheet_name="Intensidad_diaria", index=False)

    datos.df_anual.to_excel(writer, sheet_name="Intensidad_anual", index=False)
    datos.df_lab.to_excel(writer, sheet_name="Laboral_vs_findes", index=False)
    datos.top_mas.to_excel(writer, sheet_name="Top10_dias_mas", index=False)
    datos.top_menos.to_excel(writer, sheet_name="Top10_dias_menos", index=False)
    datos.df_comp.to_excel(writer, sheet_name="Comparativa_Pre_Post", index=False)

    writer.close()
    print("Excel completo generado en:", EXCEL_SALIDA)



#Gráficas extra

def grafica_intensidad_anual(datos: DatosProyecto):

    df = datos.df_anual

    plt.figure()
    plt.bar(df["year"], df["intensidad_media_anual"])

    #Etiquetas de variación
    for x, y, pct in zip(df["year"], df["intensidad_media_anual"], df["variacion_pct_vs_anio_anterior"]):
        if pd.notna(pct):
            plt.text(x, y, f"{pct:+.1f}%", ha="center", va="bottom")

    plt.xlabel("Año")
    plt.ylabel("Intensidad media anual (vehículos/hora)")
    plt.title("Intensidad media anual y variación vs año anterior")
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()

    ruta = CARPETA_GRAFICAS_EXTRA / "05_intensidad_anual.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("Gráfico extra guardado:", ruta)


def grafica_laboral_vs_findes(datos: DatosProyecto):
    df = datos.df_lab

    plt.figure()
    plt.bar(df["tipo_dia"], df["intensidad_media"])
    plt.xlabel("Tipo de día")
    plt.ylabel("Intensidad media (vehículos/hora)")
    plt.title("Comparativa laborables vs fin de semana")
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()

    ruta = CARPETA_GRAFICAS_EXTRA / "06_laboral_vs_findes.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("Gráfico extra guardado:", ruta)


def grafica_top10_dias(datos: DatosProyecto):
    df = datos.top_mas.copy()
    df["fecha_dia"] = pd.to_datetime(df["fecha_dia"])
    df = df.sort_values("intensidad_media_dia", ascending=True)

    plt.figure()
    plt.barh(df["fecha_dia"].dt.strftime("%Y-%m-%d"), df["intensidad_media_dia"])
    plt.xlabel("Intensidad media diaria (vehículos/hora)")
    plt.ylabel("Fecha")
    plt.title("Top 10 días con mayor intensidad media")
    plt.tight_layout()

    ruta = CARPETA_GRAFICAS_EXTRA / "07_top10_dias_mas_intensos.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("Gráfico extra guardado:", ruta)


def heatmap_mes_vs_ano(datos: DatosProyecto):
    df_mensual = datos.df_mensual.copy()

    df_pivot = df_mensual.pivot(index="month", columns="year", values="intensidad_media")
    df_pivot = df_pivot.sort_index()

    plt.figure()
    plt.imshow(df_pivot.values, aspect="auto")
    plt.colorbar(label="Intensidad media (vehículos/hora)")

    plt.xticks(range(len(df_pivot.columns)), df_pivot.columns, rotation=0)
    plt.yticks(range(0, 12), range(1, 13))
    plt.xlabel("Año")
    plt.ylabel("Mes")
    plt.title("Mapa de calor: intensidad media por mes y año")
    plt.tight_layout()

    ruta = CARPETA_GRAFICAS_EXTRA / "08_heatmap_mes_vs_ano.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("Heatmap guardado:", ruta)


def heatmap_hora_vs_periodo(datos: DatosProyecto):

    df = datos.df_horaria.copy()

    orden = ["Pre-COVID", "Confinamiento", "Desescalada", "2º estado de alarma", "Post-restricciones"]
    df["periodo_covid"] = pd.Categorical(df["periodo_covid"],categories=orden, ordered=True)
    df = df.sort_values(["periodo_covid", "hour"])

    df_pivot = df.pivot(index="hour", columns="periodo_covid", values="intensidad_media")

    plt.figure()
    plt.imshow(df_pivot.values, aspect="auto")
    plt.colorbar(label="Intensidad media (vehículos/hora)")

    plt.xticks(range(len(df_pivot.columns)), df_pivot.columns, rotation=15)
    plt.yticks(range(0, 24), range(0, 24))
    plt.xlabel("Periodo COVID")
    plt.ylabel("Hora del día")
    plt.title("Mapa de calor: intensidad media por hora y periodo COVID")
    plt.tight_layout()

    ruta = CARPETA_GRAFICAS_EXTRA / "09_heatmap_hora_vs_periodo.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("Heatmap guardado:", ruta)

#Ejecución principal
def main():
    datos = DatosProyecto()

    cargar_datos_base(datos)

    calcular_intensidad_anual(datos)
    calcular_laboral_vs_findes(datos)
    calcular_top_dias(datos, 10)
    calcular_comparativa_pre_post(datos)

    crear_excel_completo(datos)

    grafica_intensidad_anual(datos)
    grafica_laboral_vs_findes(datos)
    grafica_top10_dias(datos)
    heatmap_mes_vs_ano(datos)
    heatmap_hora_vs_periodo(datos)

    print("Análisis extra generado(Excel completo + gráficas extra).")


if __name__ == "__main__":
    main()
