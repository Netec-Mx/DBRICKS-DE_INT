---
layout: lab
title: 'Práctica 12: Optimización de pipeline y comparación de métricas'
permalink: /lab12/lab12/
images_base: /labs/lab12/img
duration: 65 minutos
objective:
- Comparar particionado estático y Liquid Clustering con el mismo conjunto de eventos.
- Medir 24 ejecuciones con duraciones reales, sin forzar workers ni permisos adicionales.
- Interpretar metadatos Delta y registrar una recomendación para la Práctica 13.
prerequisites:
- Práctica 11 completada o acceso a dbricks_de_int.
- Cuenta principal con CREATE SCHEMA en dbricks_de_int o un esquema p12_performance_lab preparado y con CREATE TABLE y MODIFY.
- Clúster All-Purpose existente compatible con Unity Catalog y Liqsuid Clustering (Databricks Runtime 13.3 LTS o superior).
- Tiempo y cuota suficientes para almacenar aproximadamente 200,000 registros en tres tablas Delta.
introduction:
- Crear una comparación controlada entre particionado tradicional por país y Liquid Clustering por cliente/fecha; ambas tablas tendrán 200,000 eventos reproducibles.
slug: lab12
lab_number: 12
final_result: Origen y dos diseños físicos con 200,000 filas, 24 mediciones registradas y recomendación basada en resultados reales.
notes:
- Trabaja en el catálogo dbricks_de_int sin modificar objetos de las prácticas anteriores.
- Los tiempos y costos deben provenir de mediciones reales; no se proporcionan valores inventados.
references:
- text: Liquid Clustering (Azure Databricks)
  url: https://learn.microsoft.com/en-us/azure/databricks/tables/clustering
- text: OPTIMIZE Delta
  url: https://learn.microsoft.com/en-us/azure/databricks/tables/operations/optimize
- text: Spark UI
  url: https://learn.microsoft.com/en-us/azure/databricks/compute/spark-ui
prev: /lab11/lab11/
next: /lab13/lab13/
---

> **Continuidad:** esta práctica **no modifica ni necesita consultar** las tablas de la Práctica 11. Se reutilizan el catálogo `dbricks_de_int`, la cuenta principal y el clúster existente. La Práctica 13 sí reutilizará `p12_performance_lab.events_liquid`, `benchmark_metrics` y `benchmark_recommendation`.
{: .lab-note .important .compact}

{% assign results = site.data.task-results[page.slug].results %}

## 🔎 Tarea 1. Preparar notebook, parámetros y tablas de métricas — 10 min

Reutilizar el cómputo autorizado y crear objetos del experimento en un esquema exclusivo.

- {% include step_label.html %} **Cuenta principal → Workspace > Home > batch_1:** crea el notebook `12_benchmark_layout`, con lenguaje **Python** y conecta el clúster existente; no crees otro clúster.

  > **Salida esperada:** Notebook conectado y listo para ejecutar Python.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 1 (Python):** inspecciona versión del runtime y configuración de Spark; no supongas workers fijos ni Photon.

  ```python
  print('spark_version=', spark.version)
  print('AQE=', spark.conf.get('spark.sql.adaptive.enabled'))
  print('shuffle_partitions=', spark.conf.get('spark.sql.shuffle.partitions'))
  ```

  > **Salida esperada:** Se imprimen la versión de Spark y los dos parámetros reales.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 2 (Python):** establece parámetros constantes para todas las pruebas del notebook.

  ```python
  spark.conf.set('spark.sql.adaptive.enabled', 'true')
  spark.conf.set('spark.sql.shuffle.partitions', '200')
  print('AQE=', spark.conf.get('spark.sql.adaptive.enabled'), 'shuffle=', spark.conf.get('spark.sql.shuffle.partitions'))
  ```

  > **Salida esperada:** `AQE=true` y `shuffle_partitions=200` para el experimento.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 3 (SQL):** cambia el selector de lenguaje a **SQL**, crea el esquema de laboratorio. Si el catálogo deniega `CREATE SCHEMA`, solicita al propietario que lo cree sin alterar `batch1`.

  ```sql
  CREATE SCHEMA IF NOT EXISTS dbricks_de_int.p12_performance_lab;
  ```

  > **Salida esperada:** El esquema aislado aparece al repetir `SHOW SCHEMAS`.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 4 (Python):** vuelve a Python y define los nombres que reutilizarán todos los pasos y la Práctica 13.

  ```python
  BASE = 'dbricks_de_int.p12_performance_lab'
  SOURCE=f'{BASE}.events_source'
  STATIC=f'{BASE}.events_partitioned_country'
  LIQUID=f'{BASE}.events_liquid'
  METRICS=f'{BASE}.benchmark_metrics'
  RECOMMEND=f'{BASE}.benchmark_recommendation'
  print('source=',SOURCE,'static=',STATIC,'liquid=',LIQUID,'metrics=',METRICS,'recommend=',RECOMMEND)
  ```

  > **Salida esperada:** Las cinco rutas se muestran dentro de `dbricks_de_int.p12_performance_lab`.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 5 (SQL):** crea la tabla de 24 mediciones y la tabla de recomendaciones en una operación por sentencia.

  ```sql
  CREATE TABLE IF NOT EXISTS dbricks_de_int.p12_performance_lab.benchmark_metrics (
    experiment_id STRING, run_id STRING, recorded_at TIMESTAMP,
    physical_design STRING, table_name STRING, query_name STRING,
    execution_number INT, is_warmup BOOLEAN, duration_seconds DOUBLE,
    result_row_count BIGINT, file_count BIGINT, notes STRING
  ) USING DELTA;
  
  CREATE TABLE IF NOT EXISTS dbricks_de_int.p12_performance_lab.benchmark_recommendation (
    experiment_id STRING, recorded_at TIMESTAMP, recommended_design STRING,
    evidence_summary STRING, limitations STRING, responsible STRING
  ) USING DELTA;
  ```

  > **Salida esperada:** Se crean `benchmark_metrics` y `benchmark_recommendation` sin borrar ensayos anteriores.
  {: .lab-note .output .compact}

{% capture r1 %}{{ results[0] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r1 %}
{% include support-prompt.html task="tarea1" %}

---

## ⚙ Tarea 2. Construir y validar los dos diseños Delta — 18 min

Comparar particionado por país y Liquid Clustering sobre 200,000 eventos idénticos.

- {% include step_label.html %} **Celda 6 (SQL):** genera los 200,000 eventos sintéticos reproducibles en `events_source`; esta tabla es el origen común.

  ```sql
  CREATE OR REPLACE TABLE dbricks_de_int.p12_performance_lab.events_source
  USING DELTA AS
  SELECT id AS event_id,
    concat('C',lpad(CAST(pmod(id,1000) AS STRING),4,'0')) AS customer_id,
    CASE pmod(id,5) WHEN 0 THEN 'MX' WHEN 1 THEN 'US'
      WHEN 2 THEN 'CO' WHEN 3 THEN 'ES' ELSE 'AR' END AS country,
    date_add(DATE'2025-01-01', CAST(pmod(id,90) AS INT)) AS event_date,
    element_at(array('view','search','cart','purchase'),CAST(pmod(id,4) AS INT)+1) AS event_type,
    CAST(10 + pmod(id*7,990)/10.0 AS DECIMAL(12,2)) AS amount
  FROM range(200000);
  ```

  > **Salida esperada:** Se materializan 200,000 filas y 6 columnas.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 7 (SQL):** comprueba filas, clientes, países, fechas y claves únicas del origen.

  ```sql
  SELECT COUNT(*) filas, COUNT(DISTINCT event_id) ids_unicos,
   COUNT(DISTINCT customer_id) clientes, COUNT(DISTINCT country) paises,
   COUNT(DISTINCT event_date) fechas
  FROM dbricks_de_int.p12_performance_lab.events_source;
  ```

  > **Salida esperada:** `filas=200000`, `clientes=1000`, `paises=5`, `fechas=90`, `ids_unicos=200000`.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 8 (SQL):** crea el diseño estático por **country** (5 valores), no por 1,000 clientes; así evitas demasiados directorios pequeños.

  ```sql
  CREATE OR REPLACE TABLE dbricks_de_int.p12_performance_lab.events_partitioned_country
  USING DELTA PARTITIONED BY (country) AS
  SELECT * FROM dbricks_de_int.p12_performance_lab.events_source;
  ```

  > **Salida esperada:** `events_partitioned_country` queda particionada por `country`.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 9 (SQL):** crea el diseño con **Liquid Clustering** en cliente y fecha usando exactamente el mismo origen.

  ```sql
  CREATE OR REPLACE TABLE dbricks_de_int.p12_performance_lab.events_liquid
  CLUSTER BY (customer_id,event_date) AS
  SELECT * FROM dbricks_de_int.p12_performance_lab.events_source;
  ```

  > **Salida esperada:** `events_liquid` queda configurada con `CLUSTER BY (customer_id,event_date)`.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 10 (SQL):** ejecuta `OPTIMIZE` solo sobre Liquid Clustering.

  ```sql
  OPTIMIZE dbricks_de_int.p12_performance_lab.events_liquid;
  ```

  > **Salida esperada:** Se obtiene el resultado real de la operación; puede indicar 0 archivos reescritos si no había trabajo pendiente.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 11 (SQL):** compara filas e importe; ambos diseños deben representar exactamente los mismos eventos.

  ```sql
  SELECT 'static' diseno,COUNT(*) filas,SUM(amount) total_amount
  FROM dbricks_de_int.p12_performance_lab.events_partitioned_country
  UNION ALL
  SELECT 'liquid',COUNT(*),SUM(amount)
  FROM dbricks_de_int.p12_performance_lab.events_liquid;
  ```

  > **Salida esperada:** Dos filas con 200,000 eventos y el mismo `total_amount`.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celdas 12 y 13 (SQL):** ejecuta **por separado** `DESCRIBE DETAIL` sobre cada diseño y registra `numFiles`, `sizeInBytes` y sus claves.

  ```sql
  DESCRIBE DETAIL dbricks_de_int.p12_performance_lab.events_partitioned_country;
  ```

  > **Salida esperada:** La tabla estática presenta `partitionColumns=[country]`; Liquid contiene configuración de clustering sin particionado estático.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 13 (SQL):** ejecuta la inspección equivalente de Liquid.

  ```sql
  DESCRIBE DETAIL dbricks_de_int.p12_performance_lab.events_liquid;
  ```

  > **Salida esperada:** Se muestra el tamaño físico real del diseño Liquid.
  {: .lab-note .output .compact}

{% capture r2 %}{{ results[1] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r2 %}
{% include support-prompt.html task="tarea2" %}

---

## ️ Tarea 3. Ejecutar benchmark y registrar tiempos — 18 min

Las seis combinaciones se ejecutan cuatro veces; las primeras seis son calentamientos.

- {% include step_label.html %} **Celda 14 (Python):** define tres consultas equivalentes que se ejecutarán sobre ambos diseños.

  ```python
  QUERIES={
   'Q1_customer_date': "SELECT customer_id,event_date,COUNT(*) eventos,SUM(amount) total FROM {table} WHERE customer_id BETWEEN 'C0000' AND 'C0099' AND event_date BETWEEN DATE'2025-02-01' AND DATE'2025-02-28' GROUP BY customer_id,event_date",
   'Q2_country_day': "SELECT country,event_date,COUNT(*) eventos,SUM(amount) total FROM {table} WHERE event_date BETWEEN DATE'2025-02-01' AND DATE'2025-03-15' GROUP BY country,event_date",
   'Q3_summary': "SELECT customer_id,COUNT(*) eventos,SUM(amount) total FROM {table} GROUP BY customer_id ORDER BY total DESC LIMIT 100"
  }
  DESIGNS={'static':STATIC,'liquid':LIQUID}
  print('queries=',list(QUERIES),'designs=',DESIGNS)
  ```

  > **Salida esperada:** Quedan definidos 3 patrones (cliente/fecha, país/día y agregado total).
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 15 (Python):** ejecuta cada consulta en ambos diseños para comprobar que entrega **las mismas filas y valores** antes de medir.

  ```python
  for q,template in QUERIES.items():
      a=spark.sql(template.format(table=STATIC)).collect()
      b=spark.sql(template.format(table=LIQUID)).collect()
      assert sorted(map(str,a))==sorted(map(str,b)), q
      print('coinciden=',q,'filas=',len(a))
  ```

  > **Salida esperada:** Se imprimen 3 comprobaciones iguales; cualquier diferencia detiene las mediciones.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 16 (Python):** ejecuta 1 calentamiento y 3 mediciones por combinación; el identificador permite filtrar las 24 filas. **No uses `spark.sparkContext`**, no disponible en algunos modos de acceso.

  ```python
  import time,uuid
  from datetime import datetime,timezone
  from pyspark.sql.types import StructType,StructField,StringType,TimestampType,IntegerType,BooleanType,DoubleType,LongType
  EXPERIMENT_ID='p12_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')
  SCHEMA_METRICS=StructType([
   StructField('experiment_id',StringType()),StructField('run_id',StringType()),
   StructField('recorded_at',TimestampType()),StructField('physical_design',StringType()),
   StructField('table_name',StringType()),StructField('query_name',StringType()),
   StructField('execution_number',IntegerType()),StructField('is_warmup',BooleanType()),
   StructField('duration_seconds',DoubleType()),StructField('result_row_count',LongType()),
   StructField('file_count',LongType()),StructField('notes',StringType())])
  records=[]
  for name,template in QUERIES.items():
      for repeat in range(4):
          for design,table in DESIGNS.items():
              rid=str(uuid.uuid4())
              started=time.perf_counter()
              rows=spark.sql(template.format(table=table)).collect()
              seconds=max(time.perf_counter()-started,0.000001)
              details=spark.sql(f'DESCRIBE DETAIL {table}').first()
              records.append((EXPERIMENT_ID,rid,datetime.now(timezone.utc),design,table,
                  name,repeat,repeat==0,float(seconds),len(rows),int(details['numFiles']),
                  'Tiempo cliente Python; no incluye metadatos DESCRIBE DETAIL'))
              print(f'{name} | {design} | repeat={repeat} | filas={len(rows)} | segundos={seconds:.4f}')
  spark.createDataFrame(records,SCHEMA_METRICS).write.mode('append').saveAsTable(METRICS)
  print('experiment_id=',EXPERIMENT_ID,'ejecuciones=',len(records))
  ```

  > **Salida esperada:** Se imprimen 24 resultados con consulta, diseño, repetición, filas y duración; se guarda el `experiment_id`.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 17 (Python):** verifica que son 24 ejecuciones, 6 calentamientos y 18 mediciones.

  ```python
  from pyspark.sql import functions as F
  r=spark.table(METRICS).filter(F.col('experiment_id')==EXPERIMENT_ID)
  total=r.count(); warm=r.filter(F.col('is_warmup')).count(); measured=total-warm
  assert (total,warm,measured)==(24,6,18),(total,warm,measured)
  print(f'experiment_id={EXPERIMENT_ID}; total={total}; warmup={warm}; medidas={measured}')
  ```

  > **Salida esperada:** `total=24`, `warmup=6`, `medidas=18`.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 18 (Python):** muestra las mediciones del experimento sin incluir las ejecuciones de otros participantes.

  ```python
  display(r.filter(~F.col('is_warmup')).select('query_name','physical_design','execution_number','duration_seconds','run_id').orderBy('query_name','physical_design','execution_number'))
  ```

  > **Salida esperada:** Aparecen 18 duraciones reales identificadas.
  {: .lab-note .output .compact}

{% capture r3 %}{{ results[2] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r3 %}
{% include support-prompt.html task="tarea3" %}

---

## 🚀 Tarea 4. Inspeccionar ejecución y comparar resultados — 11 min

Spark UI aporta evidencia adicional si está accesible; los tiempos principales provienen del notebook.

- {% include step_label.html %} **Interfaz → Compute → selecciona el clúster → Spark UI** (si tienes permiso). Busca las pestañas **SQL/DataFrame**, **Jobs** y **Stages** para una ejecución de cada consulta.

  > **Salida esperada:** Se identifican las ejecuciones observables; si el workspace limita Spark UI, deja constancia de la limitación.
  {: .lab-note .output .compact}

- {% include step_label.html %} **En Spark UI**, revisa `Input`, `Shuffle Read/Write`, duración de stages y `Spill` de las consultas que estén visibles. No traslades valores de otra ejecución.

  > **Salida esperada:** Queda una evidencia por cada patrón; cero solo significa cero cuando la interfaz lo indique explícitamente.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 19 (SQL):** compara medianas y archivos físicos por consulta y diseño, filtrando por el ID actual. Sustituye el ID del experimento en el filtro.

  > **Nota:** Sustituye el marcador por el `experiment_id` impreso en la celda 16; no ejecutes la consulta con el marcador literal.
  {: .lab-note .info .compact}

  ```sql
  SELECT physical_design,query_name,COUNT(*) medidas,
   ROUND(percentile_approx(duration_seconds,0.5),4) mediana_s,
   MAX(file_count) num_files
  FROM dbricks_de_int.p12_performance_lab.benchmark_metrics
  WHERE experiment_id = '<EXPERIMENT_ID_REAL>' AND NOT is_warmup
  GROUP BY physical_design,query_name ORDER BY query_name,physical_design;
  ```

  > **Salida esperada:** Seis filas con tres mediciones por combinación; la mediana es calculada, no estimada.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 20 (Python):** consulta medianas filtradas sin sustituir marcadores manualmente.

  ```python
  display(r.filter(~F.col('is_warmup')).groupBy('query_name','physical_design').agg(F.expr('percentile_approx(duration_seconds,0.5)').alias('mediana_s'),F.count('*').alias('runs')).orderBy('query_name','physical_design'))
  ```

  > **Salida esperada:** Se muestran medianas reales de las seis combinaciones.
  {: .lab-note .output .compact}

{% capture r4 %}{{ results[3] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r4 %}
{% include support-prompt.html task="tarea4" %}

---

## 🧩 Tarea 5. Concluir y conservar recursos para la Práctica 13 — 8 min

La recomendación distingue hallazgos observados de hipótesis sobre costo y escalabilidad.

- {% include step_label.html %} **Celda 21 (Python):** calcula una tabla comparativa de medianas por consulta sin mezclar experimentos.

  ```python
  median=r.filter(~F.col('is_warmup')).groupBy('query_name','physical_design').agg(F.expr('percentile_approx(duration_seconds,0.5)').alias('seconds'))
  a=median.filter(F.col('physical_design')=='static').select('query_name',F.col('seconds').alias('static_s'))
  b=median.filter(F.col('physical_design')=='liquid').select('query_name',F.col('seconds').alias('liquid_s'))
  comparisons=a.join(b,'query_name').withColumn('liquid_mejora_pct',F.round(100*(F.col('static_s')-F.col('liquid_s'))/F.col('static_s'),2))
  display(comparisons.orderBy('query_name'))
  ```

  > **Salida esperada:** Tres comparaciones con `static_s`, `liquid_s` y porcentaje observado.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 22 (Python):** crea una recomendación **condicionada** por los tres resultados y los límites del experimento; no presupongas que Liquid gana.

  ```python
  results=comparisons.orderBy('query_name').collect()
  evidence='; '.join(f"{x.query_name}: static={x.static_s:.4f}s liquid={x.liquid_s:.4f}s cambio={x.liquid_mejora_pct}%" for x in results)
  recommended='evaluar liquid por consulta' if any(x.liquid_mejora_pct>0 for x in results) else 'mantener comparacion sin ganador general'
  owner=spark.sql('SELECT current_user() AS u').first().u
  from pyspark.sql.types import StructType,StructField,StringType,TimestampType
  rec_schema=StructType([StructField('experiment_id',StringType()),StructField('recorded_at',TimestampType()),StructField('recommended_design',StringType()),StructField('evidence_summary',StringType()),StructField('limitations',StringType()),StructField('responsible',StringType())])
  spark.createDataFrame([(EXPERIMENT_ID,datetime.now(timezone.utc),recommended,evidence,'200000 filas; clúster compartido; latencia Python; no implica ahorro de costos',owner)],rec_schema).write.mode('append').saveAsTable(RECOMMEND)
  print('experiment_id=',EXPERIMENT_ID,'recomendacion=',recommended,'evidencia=',evidence)
  ```

  > **Salida esperada:** Se registra una fila ligada al `experiment_id` para usar en la Práctica 13.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 23 (SQL):** comprueba que los objetos para la Práctica 13 siguen disponibles y **no los elimines**.

  ```sql
  SHOW TABLES IN dbricks_de_int.p12_performance_lab;
  ```

  > **Salida esperada:** Se observan `events_liquid`, `benchmark_metrics` y `benchmark_recommendation`.
  {: .lab-note .output .compact}

{% capture r5 %}{{ results[4] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r5 %}
{% include support-prompt.html task="tarea5" %}