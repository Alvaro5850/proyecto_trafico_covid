Proyecto: Análisis del Tráfico en Madrid (2019–2022) y Efecto del COVID-19

Asignatura: Grandes Volúmenes de Datos

 Objetivo del proyecto

Analizar cómo evolucionó el tráfico en la ciudad de Madrid entre 2019 y 2022, y estudiar el impacto de la pandemia de COVID-19 utilizando los datos abiertos del Ayuntamiento.

El análisis se centra en la variable intensidad, que representa el número de vehículos que pasan por un punto de medición en una hora.

 ¿Qué es la intensidad?

La intensidad es el indicador principal en el estudio de movilidad.
Refleja:

Volumen total de tráfico

Congestión

Comportamiento social (ida al trabajo, ocio, etc.)

Efecto de restricciones COVID

Ejemplo:
Si entre las 08:00–09:00 pasan 1200 vehículos por un sensor → intensidad = 1200.

 Datos utilizados

Los datos originales están formados por más de 83 archivos ZIP, uno por cada mes entre 2019 y 2022.
Cada ZIP contiene multitud de registros horarios de sensores de tráfico.

Se procesaron cerca de 900 millones de filas mediante PySpark.

Pipeline del proyecto
1) Ingesta y ordenación

Los ZIP originales se organizan por año:

data/raw/2019
data/raw/2020
data/raw/2021
data/raw/2022

2) Procesamiento con Spark

El script analisis_trafico_madrid.py:

Lee todos los CSV de los 4 años

Filtra valores erróneos

Convierte fechas

Calcula agregaciones:

Periodos COVID

Curva horaria

Intensidad mensual

Intensidad diaria

Genera:

output/resumen_periodos/*.csv
output/intensidad_mensual/*.csv
output/curva_horaria/*.csv
output/intensidad_diaria/*.csv

3) Análisis avanzado y visualizaciones

El script analisis_extra.py produce:

Gráficas avanzadas

Mapas de calor

Top 10 días de más tráfico

Comparativa Pre-COVID vs Post-restricciones

Excel consolidado con todas las tablas

Se genera:

output/resultados_trafico_completo.xlsx
output/graficas_extra/*.png

 Gráficas generadas

Incluye:

Intensidad por periodo COVID

Intensidad mensual por año

Curva horaria por periodo

Serie diaria completa

Intensidad anual

Laborables vs findes

Top 10 días de más tráfico

Heatmap mes-año

Heatmap hora-periodo

 Interpretación de resultados

Las gráficas muestran claramente:

Hundimiento del tráfico durante el confinamiento

Recuperación progresiva en 2021–2022

Patrones de movilidad diarios y estacionales

Diferencias fuertes entre días laborables y fines de semana

Impacto de las restricciones en horas punta

 Cómo ejecutar el proyecto
1) Procesar datos
python codigo/analisis_trafico_madrid.py

2) Generar gráficas y análisis extra
python codigo/analisis_extra.py


Los resultados se guardan en la carpeta output/.