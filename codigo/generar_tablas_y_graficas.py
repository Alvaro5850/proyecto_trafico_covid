import os
import pandas as pd
import matplotlib.pyplot as plt

# =====================
#  CONFIGURACIÓN BASE
# =====================

BASE_OUTPUT = "output"
EXCEL_SALIDA = os.path.join(BASE_OUTPUT, "resultados_trafico.xlsx")
CARPETA_GRAFICAS = os.path.join(BASE_OUTPUT, "graficas")

os.makedirs(CARPETA_GRAFICAS, exist_ok=True)

plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams["font.size"] = 11


# =====================
#  UTILIDADES
# =====================

def leer_csv_unico(ruta_carpeta: str) -> pd.DataFrame:
    """
    Lee todos los CSV de una carpeta (formato típico de salida de Spark)
    y los concatena en un único DataFrame.
    """
    archivos = [f for f in os.listdir(ruta_carpeta) if f.endswith(".csv")]
    if not archivos:
        raise FileNotFoundError(f"No hay CSV en {ruta_carpeta}")
    dfs = [pd.read_csv(os.path.join(ruta_carpeta, f)) for f in archivos]
    return pd.concat(dfs, ignore_index=True)


def formatear_numeros(df: pd.DataFrame, columnas, decimales: int = 2) -> pd.DataFrame:
    df = df.copy()
    for c in columnas:
        if c in df.columns:
            df[c] = df[c].astype(float).round(decimales)
    return df


# =====================
#  CREACIÓN DEL EXCEL
# =====================

def crear_excel():
    writer = pd.ExcelWriter(EXCEL_SALIDA, engine="openpyxl")

    # 1) Resumen por periodo
    df_res = leer_csv_unico(os.path.join(BASE_OUTPUT, "resumen_periodos"))
    df_res = formatear_numeros(
        df_res, ["intensidad_media", "intensidad_mediana"], 2
    )
    df_res.to_excel(writer, sheet_name="Resumen_periodos", index=False)

    # 2) Intensidad mensual
    df_mensual = leer_csv_unico(os.path.join(BASE_OUTPUT, "intensidad_mensual"))
    df_mensual = formatear_numeros(df_mensual, ["intensidad_media"], 2)
    df_mensual.to_excel(writer, sheet_name="Intensidad_mensual", index=False)

    # 3) Curva horaria
    df_horaria = leer_csv_unico(os.path.join(BASE_OUTPUT, "curva_horaria"))
    df_horaria = formatear_numeros(df_horaria, ["intensidad_media"], 2)
    df_horaria.to_excel(writer, sheet_name="Curva_horaria", index=False)

    # 4) Intensidad diaria
    df_diaria = leer_csv_unico(os.path.join(BASE_OUTPUT, "intensidad_diaria"))
    df_diaria = formatear_numeros(df_diaria, ["intensidad_media_dia"], 2)
    df_diaria.to_excel(writer, sheet_name="Intensidad_diaria", index=False)

    writer.close()
    print(f"✅ Excel generado en: {EXCEL_SALIDA}")
    return df_res, df_mensual, df_horaria, df_diaria


# =====================
#  GRÁFICAS
# =====================

def grafica_barras_resumen(df_res: pd.DataFrame):
    """
    Barra por periodo COVID.

    - Calcula la intensidad media por periodo.
    - Ordena intentando usar el orden lógico de la pandemia,
      pero solo con los periodos que existan realmente.
    """
    if "periodo_covid" not in df_res.columns:
        print("⚠️ No se encuentra la columna 'periodo_covid' en df_res. Saltando gráfico 01.")
        return

    # Agregamos por si hay varias filas por periodo
    df_agg = (
        df_res
        .groupby("periodo_covid", as_index=False)["intensidad_media"]
        .mean()
    )

    # Orden lógico deseado
    orden_logico = [
        "Pre-COVID",
        "Confinamiento",
        "Desescalada",
        "2º estado de alarma",
        "Post-restricciones",
    ]

    # Filtramos solo los periodos que existan en los datos
    periodos_presentes = [p for p in orden_logico if p in df_agg["periodo_covid"].unique()]

    if periodos_presentes:
        df_plot = (
            df_agg
            .set_index("periodo_covid")
            .loc[periodos_presentes]
            .reset_index()
        )
    else:
        # Si ninguno coincide con el orden lógico, usamos todos los que haya ordenados alfabéticamente
        df_plot = df_agg.sort_values("periodo_covid")

    plt.figure()
    barras = plt.bar(df_plot["periodo_covid"], df_plot["intensidad_media"])
    plt.xticks(rotation=20)
    plt.ylabel("Intensidad media (vehículos/hora)")
    plt.title("Intensidad media por periodo COVID en Madrid")
    plt.grid(axis="y", alpha=0.3)

    # Etiquetas numéricas encima de cada barra
    for bar, valor in zip(barras, df_plot["intensidad_media"]):
        altura = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            altura,
            f"{valor:.0f}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    plt.tight_layout()
    ruta = os.path.join(CARPETA_GRAFICAS, "01_intensidad_por_periodo.png")
    plt.savefig(ruta, dpi=220)
    plt.close()
    print("📊 Gráfico guardado:", ruta)


def grafica_lineas_mensual(df_mensual: pd.DataFrame):
    plt.figure()
    for year, df_y in df_mensual.groupby("year"):
        df_y = df_y.sort_values("month")
        plt.plot(
            df_y["month"],
            df_y["intensidad_media"],
            marker="o",
            label=str(year),
        )

    plt.xlabel("Mes")
    plt.ylabel("Intensidad media (vehículos/hora)")
    plt.title("Intensidad media mensual por año")
    plt.xticks(range(1, 13))
    plt.grid(True, alpha=0.3)
    plt.legend(title="Año")
    plt.tight_layout()
    ruta = os.path.join(CARPETA_GRAFICAS, "02_intensidad_mensual_por_ano.png")
    plt.savefig(ruta, dpi=220)
    plt.close()
    print("📊 Gráfico guardado:", ruta)


def grafica_curva_horaria(df_horaria: pd.DataFrame):
    plt.figure()
    for periodo, df_p in df_horaria.groupby("periodo_covid"):
        df_p = df_p.sort_values("hour")
        plt.plot(
            df_p["hour"],
            df_p["intensidad_media"],
            marker="o",
            label=periodo,
        )

    plt.xlabel("Hora del día")
    plt.ylabel("Intensidad media (vehículos/hora)")
    plt.title("Curva horaria de intensidad según periodo COVID")
    plt.xticks(range(0, 24, 2))
    plt.grid(True, alpha=0.3)
    plt.legend(title="Periodo")
    plt.tight_layout()
    ruta = os.path.join(CARPETA_GRAFICAS, "03_curva_horaria_por_periodo.png")
    plt.savefig(ruta, dpi=220)
    plt.close()
    print("📊 Gráfico guardado:", ruta)


def grafica_intensidad_diaria(df_diaria: pd.DataFrame):
    plt.figure()
    df_diaria_sorted = df_diaria.sort_values("fecha_dia")
    df_diaria_sorted["fecha_dia"] = pd.to_datetime(df_diaria_sorted["fecha_dia"])

    plt.plot(
        df_diaria_sorted["fecha_dia"],
        df_diaria_sorted["intensidad_media_dia"],
    )
    plt.xlabel("Fecha")
    plt.ylabel("Intensidad media diaria (vehículos/hora)")
    plt.title("Serie diaria de intensidad media (2019–2022)")
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=30)
    plt.tight_layout()
    ruta = os.path.join(CARPETA_GRAFICAS, "04_intensidad_diaria.png")
    plt.savefig(ruta, dpi=220)
    plt.close()
    print("📊 Gráfico guardado:", ruta)


# =====================
#  MAIN
# =====================

if __name__ == "__main__":
    # 1) Crear Excel con todas las tablas formateadas
    df_res, df_mensual, df_horaria, df_diaria = crear_excel()

    # 2) Crear gráficas
    grafica_barras_resumen(df_res)
    grafica_lineas_mensual(df_mensual)
    grafica_curva_horaria(df_horaria)
    grafica_intensidad_diaria(df_diaria)

    print("✅ Todo generado en carpeta 'output/' (Excel + gráficas).")
