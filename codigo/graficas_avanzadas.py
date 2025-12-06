import os
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

BASE_OUTPUT = Path("output")
CARPETA_GRAFICAS = BASE_OUTPUT / "graficas_avanzadas"
CARPETA_GRAFICAS.mkdir(parents=True, exist_ok=True)

plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams["font.size"] = 11


def leer_csv_unico(carpeta: Path) -> pd.DataFrame:
    archivos = [f for f in os.listdir(carpeta) if f.endswith(".csv")]
    if not archivos:
        raise FileNotFoundError(f"No hay CSV en {carpeta}")
    dfs = [pd.read_csv(carpeta / f) for f in archivos]
    return pd.concat(dfs, ignore_index=True)


def cargar_datos_base():
    df_res = leer_csv_unico(BASE_OUTPUT / "resumen_periodos")
    df_mensual = leer_csv_unico(BASE_OUTPUT / "intensidad_mensual")
    df_horaria = leer_csv_unico(BASE_OUTPUT / "curva_horaria")
    df_diaria = leer_csv_unico(BASE_OUTPUT / "intensidad_diaria")
    return df_res, df_mensual, df_horaria, df_diaria


# =========================
# GRÁFICA 1: Heatmap mes vs año
# =========================
def heatmap_mes_vs_ano(df_mensual: pd.DataFrame):
    df_pivot = df_mensual.pivot(index="month",
                                columns="year",
                                values="intensidad_media")
    df_pivot = df_pivot.sort_index()

    fig, ax = plt.subplots()
    im = ax.imshow(df_pivot.values, aspect="auto", cmap="viridis")

    cbar = fig.colorbar(im, ax=ax, label="Intensidad media (vehículos/hora)")
    ax.set_xticks(range(len(df_pivot.columns)))
    ax.set_xticklabels(df_pivot.columns)
    ax.set_yticks(range(len(df_pivot.index)))
    ax.set_yticklabels(df_pivot.index)

    ax.set_xlabel("Año")
    ax.set_ylabel("Mes")
    ax.set_title("Mapa de calor: intensidad media por mes y año")

    plt.tight_layout()
    ruta = CARPETA_GRAFICAS / "01_heatmap_mes_vs_ano.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("📊 Heatmap mes vs año guardado:", ruta)


# =========================
# GRÁFICA 2: Heatmap hora vs periodo COVID
# =========================
def heatmap_hora_vs_periodo(df_horaria: pd.DataFrame):
    df = df_horaria.copy()

    orden = ["Pre-COVID", "Confinamiento",
             "Desescalada", "2º estado de alarma", "Post-restricciones"]
    df["periodo_covid"] = pd.Categorical(df["periodo_covid"],
                                         categories=orden,
                                         ordered=True)
    df = df.sort_values(["periodo_covid", "hour"])

    df_pivot = df.pivot(index="hour",
                        columns="periodo_covid",
                        values="intensidad_media")

    fig, ax = plt.subplots()
    im = ax.imshow(df_pivot.values, aspect="auto", cmap="plasma")

    fig.colorbar(im, ax=ax, label="Intensidad media (vehículos/hora)")
    ax.set_xticks(range(len(df_pivot.columns)))
    ax.set_xticklabels(df_pivot.columns, rotation=15)
    ax.set_yticks(range(len(df_pivot.index)))
    ax.set_yticklabels(df_pivot.index)

    ax.set_xlabel("Periodo COVID")
    ax.set_ylabel("Hora del día")
    ax.set_title("Mapa de calor: intensidad por hora y periodo COVID")

    plt.tight_layout()
    ruta = CARPETA_GRAFICAS / "02_heatmap_hora_vs_periodo.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("📊 Heatmap hora vs periodo guardado:", ruta)


# =========================
# Helper: asignar periodo COVID a una fecha (para boxplots)
# =========================
def asignar_periodo_fecha(fecha: pd.Timestamp) -> str:
    f = fecha.normalize()  # quitar hora por si acaso
    if f < pd.Timestamp("2020-03-15"):
        return "Pre-COVID"
    elif pd.Timestamp("2020-03-15") <= f <= pd.Timestamp("2020-06-21"):
        return "Confinamiento"
    elif pd.Timestamp("2020-06-22") <= f <= pd.Timestamp("2020-10-25"):
        return "Desescalada"
    elif pd.Timestamp("2020-10-26") <= f <= pd.Timestamp("2021-05-09"):
        return "2º estado de alarma"
    else:
        return "Post-restricciones"


# =========================
# GRÁFICA 3: Boxplot por periodo COVID (intensidad diaria)
# =========================
def boxplot_por_periodo(df_diaria: pd.DataFrame):
    df = df_diaria.copy()
    df["fecha_dia"] = pd.to_datetime(df["fecha_dia"])
    df["periodo_covid"] = df["fecha_dia"].apply(asignar_periodo_fecha)

    orden = ["Pre-COVID", "Confinamiento",
             "Desescalada", "2º estado de alarma", "Post-restricciones"]
    data = [
        df.loc[df["periodo_covid"] == p, "intensidad_media_dia"].dropna()
        for p in orden
    ]

    fig, ax = plt.subplots()
    ax.boxplot(data, labels=orden, showfliers=False)
    ax.set_ylabel("Intensidad media diaria (vehículos/hora)")
    ax.set_title("Distribución de intensidad diaria por periodo COVID")
    ax.grid(axis="y", alpha=0.3)

    plt.xticks(rotation=15)
    plt.tight_layout()
    ruta = CARPETA_GRAFICAS / "03_boxplot_por_periodo.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("📊 Boxplot por periodo guardado:", ruta)


# =========================
# GRÁFICA 4: Laborables vs sábados vs domingos
# =========================
def grafica_laboral_vs_findes(df_diaria: pd.DataFrame):
    df = df_diaria.copy()
    df["fecha_dia"] = pd.to_datetime(df["fecha_dia"])
    df["dia_semana"] = df["fecha_dia"].dt.dayofweek  # 0 = lunes

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
    resumen = resumen.sort_values("tipo_dia")

    fig, ax = plt.subplots()
    ax.bar(resumen["tipo_dia"], resumen["intensidad_media"])
    ax.set_xlabel("Tipo de día")
    ax.set_ylabel("Intensidad media diaria (vehículos/hora)")
    ax.set_title("Comparativa de intensidad: laborables vs fin de semana")
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    ruta = CARPETA_GRAFICAS / "04_laboral_vs_findes.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("📊 Gráfico laborables vs findes guardado:", ruta)


# =========================
# GRÁFICA 5: Top 10 días de mayor intensidad
# =========================
def grafica_top10_dias(df_diaria: pd.DataFrame):
    df = df_diaria.copy()
    df["fecha_dia"] = pd.to_datetime(df["fecha_dia"])
    df = df.sort_values("intensidad_media_dia", ascending=False).head(10)
    df = df.sort_values("intensidad_media_dia", ascending=True)

    fig, ax = plt.subplots()
    ax.barh(df["fecha_dia"].dt.strftime("%Y-%m-%d"),
            df["intensidad_media_dia"])
    ax.set_xlabel("Intensidad media diaria (vehículos/hora)")
    ax.set_ylabel("Fecha")
    ax.set_title("Top 10 días con mayor intensidad media")

    plt.tight_layout()
    ruta = CARPETA_GRAFICAS / "05_top10_dias_mas_intensos.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("📊 Top 10 días guardado:", ruta)


# =========================
# GRÁFICA 6: Intensidad anual agregada
# =========================
def grafica_intensidad_anual(df_mensual: pd.DataFrame):
    df_anual = (
        df_mensual
        .groupby("year", as_index=False)["intensidad_media"]
        .mean()
        .rename(columns={"intensidad_media": "intensidad_media_anual"})
    )

    fig, ax = plt.subplots()
    ax.plot(df_anual["year"], df_anual["intensidad_media_anual"],
            marker="o", linestyle="-")
    for x, y in zip(df_anual["year"], df_anual["intensidad_media_anual"]):
        ax.text(x, y, f"{y:.0f}", ha="center", va="bottom", fontsize=9)

    ax.set_xlabel("Año")
    ax.set_ylabel("Intensidad media anual (vehículos/hora)")
    ax.set_title("Evolución de la intensidad media anual")
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    ruta = CARPETA_GRAFICAS / "06_intensidad_anual.png"
    plt.savefig(ruta, dpi=200)
    plt.close()
    print("📊 Intensidad anual guardada:", ruta)


def main():
    df_res, df_mensual, df_horaria, df_diaria = cargar_datos_base()

    heatmap_mes_vs_ano(df_mensual)
    heatmap_hora_vs_periodo(df_horaria)
    boxplot_por_periodo(df_diaria)
    grafica_laboral_vs_findes(df_diaria)
    grafica_top10_dias(df_diaria)
    grafica_intensidad_anual(df_mensual)

    print("✅ Gráficas avanzadas generadas en 'output/graficas_avanzadas/'.")


if __name__ == "__main__":
    main()
