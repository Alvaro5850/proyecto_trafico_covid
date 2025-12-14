Proyecto: Análisis del Tráfico en Madrid (2019–2022)

Impacto del COVID-19 en la movilidad urbana en Madrid

Asignatura: Grandes Volúmenes de Datos

1. Objetivo del proyecto

El objetivo de este proyecto es analizar cómo evolucionó el tráfico en la ciudad de Madrid entre los años 2019 y 2022, y estudiar el impacto que tuvo la pandemia de COVID-19 sobre la movilidad urbana.

Debido a que los datos contienen mediciones frecuentes durante varios años, se ha utilizado Apache Spark para el procesamiento distribuido. Posteriormente, se han empleado Python, Pandas y Matplotlib para el análisis y la visualización de los resultados.

2. ¿Qué es la intensidad de tráfico?

La intensidad es la variable principal del estudio y representa el número de vehículos que pasan por un punto de medición durante una hora.

Este indicador permite analizar:

El volumen de tráfico en la ciudad.

La congestión en diferentes franjas horarias.

Los patrones de movilidad diaria y semanal.

El efecto de las restricciones impuestas durante la pandemia de COVID-19.

Ejemplo:
Si entre las 08:00 y las 09:00 pasan 1.200 vehículos por un sensor, la intensidad es 1200 vehículos/hora.

3. Datos utilizados

Los datos proceden de sensores de tráfico del Ayuntamiento de Madrid.

Estructura de los datos

45 archivos CSV correspondientes al periodo 2019–2022.

Cada archivo contiene registros horarios de sensores de tráfico.

Separador ;.

Columnas principales utilizadas:

id

fecha

tipo_elem

intensidad

ocupacion

carga

vmed

error

periodo_integracion

4. Arquitectura del proyecto (Docker + Spark)

El proyecto se ejecuta dentro de contenedores Docker para garantizar que el entorno sea reproducible.

La arquitectura definida es la siguiente:

Spark Master: coordina el trabajo, planifica las tareas y divide la ejecución en stages.

Spark Worker 1

Spark Worker 2

Cada worker procesa una parte del dataset en paralelo. El periodo de trabajo abarca desde 2019 hasta 2022, que es el rango temporal seleccionado para el estudio.

Docker permite aislar dependencias, controlar versiones de Python, Java y Spark, y ejecutar el proyecto de la misma forma en cualquier ordenador.

5. Pipeline del proyecto
5.1 Procesamiento con Spark

Script principal:

codigo/analisis_trafico_madrid.py


Este script realiza los siguientes pasos:

Creación de la sesión Spark
Se crea una SparkSession, que es el punto de entrada para leer y transformar datos con Spark.

Carga de datos
Se leen todos los archivos CSV desde data/raw/*/*.csv, indicando el separador ; y permitiendo que Spark infiera los tipos de datos.

Selección de columnas
Se seleccionan únicamente las columnas necesarias para reducir el uso de memoria y el coste computacional.

Conversión de fechas
La columna fecha se convierte a un tipo timestamp real para poder extraer componentes temporales como año, mes y hora.

Limpieza de datos

Se eliminan registros con intensidad nula.

Se filtran registros marcados con error.

Se limita el análisis al periodo 2019–2022.

Generación de variables temporales
Se crean las columnas año, mes, hora y fecha sin hora (FechaDia).

Clasificación por periodo COVID
Se crea la columna PeriodoCovid utilizando condiciones when y otherwise, que en Spark equivalen a un if / elif / else.
Cada registro se clasifica como:

Pre-COVID

Confinamiento

Desescalada

Segundo estado de alarma

Post-restricciones

Cacheo del DataFrame
El DataFrame se guarda en memoria porque se reutiliza en varias agregaciones, evitando recalcular las mismas operaciones.

Agregaciones finales
Se generan cuatro conjuntos de resultados:

Resumen por periodo COVID (media y mediana).

Intensidad media mensual.

Curva horaria por periodo COVID.

Intensidad media diaria.

Salidas generadas
output/resumen_periodos/*.csv
output/intensidad_mensual/*.csv
output/curva_horaria/*.csv
output/intensidad_diaria/*.csv

6. Ejecución y stages en Spark

Durante la ejecución aparecen distintos stages.
Un stage es una fase de ejecución que Spark genera automáticamente cuando una operación requiere reorganizar los datos, por ejemplo en agregaciones o ordenaciones.

Cada stage se divide en tareas que se reparten entre los workers, lo que permite ejecutar el análisis en paralelo de forma eficiente.

7. Análisis y visualización

Tras el procesamiento con Spark, se utilizan scripts en Python para generar los entregables finales.

Scripts principales:

generar_tablas_y_graficas.py

analisis_extra.py

graficas_avanzadas.py

Estos scripts utilizan Pandas y Matplotlib para:

Generar un archivo Excel con varias hojas.

Crear gráficas de evolución temporal.

Representar comparativas entre periodos.

Construir mapas de calor.

Mapas de calor

Los mapas de calor representan la intensidad media mediante colores.
Cuanto más intenso es el color, mayor es el tráfico, lo que permite identificar de forma visual patrones temporales y cambios de comportamiento.

8. Ejecución del proyecto

El proyecto se ejecuta completamente con Docker.

Desde la raíz del proyecto:

docker compose up --build


Este comando:

Levanta el clúster Spark.

Ejecuta el procesamiento de datos.

Genera automáticamente los CSV, el Excel y las gráficas.

Los resultados se almacenan en la carpeta:

output/
