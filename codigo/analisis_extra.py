import os
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

# =========================
# Rutas base
# =========================

BASE_OUTPUT = Path("output")
CARPETA_GRAFICAS_EXTRA = BASE_OUTPUT / "graficas_extra"
EXCEL_SALIDA = BASE_OUTPUT / "resultados_trafico_completo.xlsx"

CARPETA_GRAFICAS_EXTRA.mkdir(parents=True, exist_ok=True)

# Estilo general de gráficas
plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams["font.size"] = 11


# =========================
# Funciones de apoyo
# =========================

def leer_csv_unico(carpeta: Path) -> pd.DataFrame:
    archivos = [f for f in os.listdir(carpeta) if f.endswith(".csv")]
    if not archivos:
        raise FileNotFoundError(f"No hay CSV en {carpeta}")
    dfs = [pd.read_csv(carpeta / f) for f in archivos]
    return pd.concat(dfs, ignore_index=True)


def formatear_numeros(df: pd.DataFrame, columnas, decimales=2) -> pd.DataFrame:
    for c in columnas:
        if c in df.columns:
            df[c] = df[c].astype(float).round(decimales)
    return df


# =========================
# Carga de datos base
# =========================

def cargar_datos_base():
    df_res = leer_csv_unico(BASE_OUTPUT / "resumen_periodos")
    df_res = formatear_numeros(df_res,
                               ["intensidad_media", "intensidad_mediana"], 2)

    df_mensual = leer_csv_unico(BASE_OUTPUT / "intensidad_mensual")
    df_mensual = formatear_numeros(df_mensual, ["intensidad_media"], 2)

    df_horaria = leer_csv_unico(BASE_OUTPUT / "curva_horaria")
    df_horaria = formatear_numeros(df_horaria, ["intensidad_media"], 2)

    df_diaria = leer_csv_unico(BASE_OUTPUT / "intensidad_diaria")
    df_diaria = formatear_numeros(df_diaria, ["intensidad_media_dia"], 2)

    return df_res, df_mensual, df_horaria, df_diaria


# =========================
# Tablas extra para el Excel
# =========================

def calcular_intensidad_anual(df_mensual: pd.DataFrame) -> pd.DataFrame:
    df_anual = (
        df_mensual
        .groupby("year", as_index=False)["intensidad_media"]
        .mean()
        .rename(columns={"intensidad_media": "intensidad_media_anual"})
    )
    df_anual["variacion_pct_vs_anio_anterior"] = (
        df_anual["intensidad_media_anual"].pct_change() * 100
    ).round(2)
    return df_anual


def calcular_laboral_vs_findes(df_diaria: pd.DataFrame) -> pd.DataFrame:
    df = df_diaria.copy()
    df["fecha_dia"] = pd.to_datetime(df["fecha_dia"])
    df["dia_semana"] = df["fecha_dia"].dt.dayofweek  # 0=Lunes, 6=Domingo

    def clasificar(d):
        if d < 5:
            return "Laborable"
        elif d == 5:
            return "Sábado"
        else:
            return "Domingo"

    df["tipo_dia"] = df["dia_semana"].apply(clasificar)

    resumen = (
        df.groupby("tipo_dia", as_index=False)["intensidad_media_dia"]
        .mean()
        .rename(columns={"intensidad_media_dia": "intensidad_media"})
    )

    orden = ["Laborable", "Sábado", "Domingo"]
    resumen["tipo_dia"] = pd.Categorical(resumen["tipo_dia"],
                                         categories=orden,
                                         ordered=True)
    resumen = resumen.sort_values("tipo_dia").reset_index(drop=True)

    return formatear_numeros(resumen, ["intensidad_media"], 2)


def top_dias(df_diaria: pd.DataFrame, n=10):
    df = df_diaria.copy()
    df["fecha_dia"] = pd.to_datetime(df["fecha_dia"])
    df = df.sort_values("intensidad_media_dia", ascending=False)

    top_mas = df.head(n).copy()
    top_menos = df.sort_values("intensidad_media_dia",
                               ascending=True).head(n).copy()

    return top_mas, top_menos


def comparativa_pre_post(df_res: pd.DataFrame) -> pd.DataFrame:
    df = df_res.copy()
    pre_row = df.loc[df["periodo_covid"] == "Pre-COVID"]
    if pre_row.empty:
        return df

    pre_val = float(pre_row["intensidad_media"].iloc[0])

    df["variacion_vs_PreCOVID_pct"] = (
        (df["intensidad_media"] - pre_val) / pre_val * 100
    ).round(2)
    return df


# =========================
# Gráficas extra
# =========================

def grafica_intensidad_anual(df_anual: pd.DataFrame):
    plt.figure()
    plt.bar(df_anual["year"], df_anual["intensidad_media_anual"])
    for x, y, pct in zip(df_anual["year"],
                         df_anual["intensidad_media_anual"],
                         df_anual["variacion_pct_vs_anio_anterior"]):
        if pd.notna(pct):
            plt.text(x, y, f"{pct:+.1f}%", ha="center", va="bottom")
    plt.xlabel("Año")
    plt.ylabel("Intensidad media anual (vehículos/hora)")
    plt.title("Intensidad media anual y variación respecto al año anterior")
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    ruta = CARPETA_GRAFICAS_EXTRA / "05_intensidad_anual.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("📊 Gráfico extra guardado:", ruta)


def grafica_laboral_vs_findes(df_lab: pd.DataFrame):
    plt.figure()
    plt.bar(df_lab["tipo_dia"], df_lab["intensidad_media"])
    plt.xlabel("Tipo de día")
    plt.ylabel("Intensidad media (vehículos/hora)")
    plt.title("Comparativa laborables vs fin de semana")
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    ruta = CARPETA_GRAFICAS_EXTRA / "06_laboral_vs_findes.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("📊 Gráfico extra guardado:", ruta)


def grafica_top10_dias(top_mas: pd.DataFrame):
    df = top_mas.copy()
    df["fecha_dia"] = pd.to_datetime(df["fecha_dia"])
    df = df.sort_values("intensidad_media_dia", ascending=True)

    plt.figure()
    plt.barh(df["fecha_dia"].dt.strftime("%Y-%m-%d"),
             df["intensidad_media_dia"])
    plt.xlabel("Intensidad media diaria (vehículos/hora)")
    plt.ylabel("Fecha")
    plt.title("Top 10 días con mayor intensidad media")
    plt.tight_layout()
    ruta = CARPETA_GRAFICAS_EXTRA / "07_top10_dias_mas_carga.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("📊 Gráfico extra guardado:", ruta)


def heatmap_mes_vs_ano(df_mensual: pd.DataFrame):
    """Mapa de calor mes (1–12) vs año con intensidad media."""
    df_pivot = df_mensual.pivot(index="month",
                                columns="year",
                                values="intensidad_media")
    df_pivot = df_pivot.sort_index()

    plt.figure()
    plt.imshow(df_pivot.values, aspect="auto")
    plt.colorbar(label="Intensidad media (vehículos/hora)")
    plt.xticks(range(len(df_pivot.columns)),
               df_pivot.columns, rotation=0)
    plt.yticks(range(1, 13), range(1, 13))
    plt.xlabel("Año")
    plt.ylabel("Mes")
    plt.title("Mapa de calor: intensidad media por mes y año")
    plt.tight_layout()
    ruta = CARPETA_GRAFICAS_EXTRA / "08_heatmap_mes_vs_ano.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("📊 Heatmap guardado:", ruta)


def heatmap_hora_vs_periodo(df_horaria: pd.DataFrame):
    """Mapa de calor hora (0–23) vs periodo COVID."""
    df = df_horaria.copy()

    # Asegurar orden de los periodos
    orden = ["Pre-COVID", "Confinamiento",
             "Desescalada", "2º estado de alarma", "Post-restricciones"]
    df["periodo_covid"] = pd.Categorical(df["periodo_covid"],
                                         categories=orden,
                                         ordered=True)
    df = df.sort_values(["periodo_covid", "hour"])

    df_pivot = df.pivot(index="hour",
                        columns="periodo_covid",
                        values="intensidad_media")

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
    print("📊 Heatmap guardado:", ruta)


# =========================
# Escritura del Excel
# =========================

def crear_excel_completo(df_res, df_mensual, df_horaria, df_diaria,
                         df_anual, df_lab, top_mas, top_menos, df_comp):
    with pd.ExcelWriter(EXCEL_SALIDA, engine="openpyxl") as writer:
        df_res.to_excel(writer, sheet_name="Resumen_periodos", index=False)
        df_mensual.to_excel(writer, sheet_name="Intensidad_mensual", index=False)
        df_horaria.to_excel(writer, sheet_name="Curva_horaria", index=False)
        df_diaria.to_excel(writer, sheet_name="Intensidad_diaria", index=False)

        df_anual.to_excel(writer, sheet_name="Intensidad_anual", index=False)
        df_lab.to_excel(writer, sheet_name="Laboral_vs_findes", index=False)
        top_mas.to_excel(writer, sheet_name="Top10_dias_mas_carga", index=False)
        top_menos.to_excel(writer, sheet_name="Top10_dias_menos_carga", index=False)
        df_comp.to_excel(writer, sheet_name="Comparativa_Pre_Post", index=False)

    print(f"✅ Excel completo generado en: {EXCEL_SALIDA}")


# =========================
# Main
# =========================

def main():
    df_res, df_mensual, df_horaria, df_diaria = cargar_datos_base()

    df_anual = calcular_intensidad_anual(df_mensual)
    df_lab = calcular_laboral_vs_findes(df_diaria)
    top_mas, top_menos = top_dias(df_diaria)
    df_comp = comparativa_pre_post(df_res)

    crear_excel_completo(df_res, df_mensual, df_horaria, df_diaria,
                         df_anual, df_lab, top_mas, top_menos, df_comp)

    grafica_intensidad_anual(df_anual)
    grafica_laboral_vs_findes(df_lab)
    grafica_top10_dias(top_mas)
    heatmap_mes_vs_ano(df_mensual)
    heatmap_hora_vs_periodo(df_horaria)

    print("🔥 Análisis extra generado (Excel + gráficas extra).")


if __name__ == "__main__":
    main()
