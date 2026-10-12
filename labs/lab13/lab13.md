---
layout: lab
title: 'Práctica 13: Ajuste de configuración y análisis de costos'
permalink: /lab13/lab13/
images_base: /labs/lab13/img
duration: 65 minutos
objective:
- Comparar configuraciones de shuffle con AQE activo usando la tabla de Práctica 12.
- Registrar seis mediciones reales y calcular variación porcentual de mediana.
- Separar el tiempo observado de consultas de la facturación real de Azure Databricks.
prerequisites:
- Práctica 12 finalizada con la tabla dbricks_de_int.p12_performance_lab.events_liquid y benchmark_metrics disponibles.
- Cuenta principal con SELECT sobre events_liquid y CREATE TABLE/MODIFY sobre p12_performance_lab.
- Notebook conectado al mismo cómputo utilizado para Práctica 12, sin modificar workers ni modo de acceso.
- Tarifas y acceso a políticas o facturación son opcionales; sin ellos se documenta pendiente.
introduction:
- Realizar un benchmark A/B de dos configuraciones de shuffle sin exigir acceso a SparkContext, sin modificar el clúster y sin suponer costos monetarios.
slug: lab13
lab_number: 13
final_result: Seis ejecuciones medidas, comparaciones de mediana, observaciones sobre AQE y costos pendientes o estimados con fuentes verificadas.
notes:
- Trabaja en el catálogo dbricks_de_int sin modificar objetos de las prácticas anteriores.
- Los tiempos y costos deben provenir de mediciones reales; no se proporcionan valores inventados.
references:
- text: Adaptive Query Execution
  url: https://learn.microsoft.com/en-us/azure/databricks/optimizations/aqe
- text: Spark UI
  url: https://learn.microsoft.com/en-us/azure/databricks/compute/spark-ui
- text: Compute policies
  url: https://learn.microsoft.com/en-us/azure/databricks/admin/clusters/policies
- text: Billing usage system tables
  url: https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/billing
prev: /lab12/lab12/
next: /
---

> **Continuidad:** esta práctica usa `dbricks_de_int.p12_performance_lab.events_liquid` de la Práctica 12. **No crees otro catálogo, esquema o clúster.** Los grupos de la Práctica 10 no son necesarios para medir el rendimiento; trabaja con la cuenta principal.
{: .lab-note .important .compact}

{% assign results = site.data.task-results[page.slug].results %}

## 🔎 Tarea 1. Verificar la continuidad y preparar el experimento — 10 min

Los objetos vienen de la Práctica 12; no se introduce otro conjunto de datos.

- {% include step_label.html %} **Cuenta principal → Workspace > Home > batch_1:** crea el notebook `13_ajuste_costos`, elige **Python** y conecta el **mismo clúster** utilizado en la Práctica 12.

  > **Salida esperada:** El notebook está conectado; no se cambia la cantidad de workers ni Photon.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 1 (Python):** verifica la tabla Liquid y que existan mediciones previas de la Práctica 12.

  ```python
  BASE='dbricks_de_int.p12_performance_lab'
  EVENTS=f'{BASE}.events_liquid'
  METRICS=f'{BASE}.benchmark_metrics'
  TUNING=f'{BASE}.tuning_metrics'
  rows=spark.table(EVENTS).count()
  previous=spark.table(METRICS).count()
  assert rows==200000 and previous>=24,(rows,previous)
  print(f'eventos={rows}; benchmark_p12={previous}; fuente={EVENTS}')
  ```

  > **Salida esperada:** `eventos=200000` y al menos 24 mediciones previas.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 2 (Python):** registra la configuración de la sesión. No uses `spark.sparkContext` ni presupongas DBR 15.4, dos workers o una policy específica.

  ```python
  ORIGINAL_AQE=spark.conf.get('spark.sql.adaptive.enabled')
  ORIGINAL_SHUFFLE=spark.conf.get('spark.sql.shuffle.partitions')
  print('spark=',spark.version,'AQE=',ORIGINAL_AQE,'shuffle=',ORIGINAL_SHUFFLE)
  ```

  > **Salida esperada:** Imprime Spark, AQE y particiones originales.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 3 (SQL):** crea `tuning_metrics` solo si aún no existe, sin borrar métricas anteriores.

  ```sql
  CREATE TABLE IF NOT EXISTS dbricks_de_int.p12_performance_lab.tuning_metrics (
   experiment_id STRING,run_id STRING,recorded_at TIMESTAMP,
   scenario STRING,repetition INT,query_fingerprint STRING,
   aqe_enabled BOOLEAN,shuffle_partitions INT,duration_seconds DOUBLE,
   output_groups BIGINT,notes STRING
  ) USING DELTA;
  ```

  > **Salida esperada:** La tabla de métricas A/B queda preparada.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 4 (Python):** declara una consulta fija y su fingerprint; usa exactamente la misma sentencia para ambas configuraciones.

  ```python
  import hashlib,time,uuid
  from datetime import datetime,timezone
  QUERY=f'''SELECT customer_id,event_date,COUNT(*) eventos,SUM(amount) importe
  FROM {EVENTS} GROUP BY customer_id,event_date ORDER BY eventos DESC'''
  FINGERPRINT=hashlib.sha256(' '.join(QUERY.split()).encode()).hexdigest()
  EXPERIMENT='p13_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')
  print('experiment=',EXPERIMENT,'fingerprint=',FINGERPRINT)
  ```

  > **Salida esperada:** Se muestra el fingerprint SHA-256 y el ID de experimento.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 5 (Python):** confirma el total de eventos de la consulta antes de medir.

  ```python
  preview=spark.sql(QUERY).collect()
  count=sum(int(x['eventos']) for x in preview)
  assert count==200000,count
  print('grupos=',len(preview),'eventos=',count)
  ```

  > **Salida esperada:** La suma de grupos devuelve `200000` eventos.
  {: .lab-note .output .compact}

{% capture r1 %}{{ results[0] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r1 %}
{% include support-prompt.html task="tarea1" %}

---

## ⚙ Tarea 2. Medir tres ejecuciones con AQE y 200 particiones — 13 min

Calentamiento separado y duraciones medidas con la misma consulta.

- {% include step_label.html %} **Celda 6 (Python):** fija `AQE=true` y 200 particiones para la configuración base.

  ```python
  spark.conf.set('spark.sql.adaptive.enabled','true')
  spark.conf.set('spark.sql.shuffle.partitions','200')
  print('AQE=',spark.conf.get('spark.sql.adaptive.enabled'),'shuffle=',spark.conf.get('spark.sql.shuffle.partitions'))
  ```

  > **Salida esperada:** Imprime `true` y `200`.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 7 (Python):** define un medidor de tiempo que **no necesita Job Groups ni SparkContext**. Los IDs son de registro del experimento, no IDs nativos de Spark UI.

  ```python
  records=[]
  def measure(scenario,partitions,repeat_count=3):
      assert spark.conf.get('spark.sql.adaptive.enabled')=='true'
      assert int(spark.conf.get('spark.sql.shuffle.partitions'))==partitions
      for i in range(1,repeat_count+1):
          rid=str(uuid.uuid4())
          t0=time.perf_counter()
          result=spark.sql(QUERY).collect()
          seconds=max(time.perf_counter()-t0,0.000001)
          records.append((EXPERIMENT,rid,datetime.now(timezone.utc),scenario,i,
                          FINGERPRINT,True,partitions,float(seconds),len(result),
                          'Tiempo cliente Python; métricas Spark UI no capturadas'))
          print(f'{scenario} repeticion={i}; grupos={len(result)}; segundos={seconds:.4f}; run_id={rid}')
      print('mediciones_'+scenario+'=',len([x for x in records if x[3]==scenario]))
  print('medidor preparado; registros=',len(records))
  ```

  > **Salida esperada:** `measure` queda definido y `records` está vacío.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 8 (Python):** ejecuta un calentamiento sin registrarlo; conserva el mismo AQE y consulta.

  ```python
  warm=spark.sql(QUERY).collect()
  print('warmup_base_grupos=',len(warm),'mediciones=',len(records))
  ```

  > **Salida esperada:** Se imprime la cantidad real de grupos y `records=0`.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 9 (Python):** mide tres ejecuciones con la configuración base.

  ```python
  measure('baseline',200,3)
  ```

  > **Salida esperada:** `mediciones_baseline=3`; aparecen tres duraciones reales.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 10 (Python):** comprueba el resultado base y prepara la comparación.

  ```python
  base_records=[x for x in records if x[3]=='baseline']
  assert len(base_records)==3 and len({x[5] for x in base_records})==1
  print('baseline=',len(base_records),'fingerprints=',len({x[5] for x in base_records}))
  ```

  > **Salida esperada:** `baseline=3` y un fingerprint.
  {: .lab-note .output .compact}

{% capture r2 %}{{ results[1] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r2 %}
{% include support-prompt.html task="tarea2" %}

---

## ️ Tarea 3. Probar un segundo escenario de particiones — 15 min

Comparar 200 contra 64, sin suponer un resultado ni requerir acceso a métricas que el workspace no expone.

- {% include step_label.html %} **Interfaz → Compute → clúster → Spark UI:** revisa si las ejecuciones SQL de la consulta muestran shuffle; anota el valor **si está disponible**. No inventes valores de bytes.

  > **Salida esperada:** Se identifica el shuffle observado o se registra explícitamente que no fue accesible.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 11 (Python):** utiliza 64 particiones como **segundo escenario didáctico**, no como tamaño óptimo calculado. AQE puede coalescer particiones y reducir la diferencia efectiva.

  > **Nota:** Se reemplaza el requisito previo de ingresar bytes de shuffle y cores que podría bloquear el laboratorio. El experimento compara dos configuraciones controladas y mide el resultado, sin afirmar una configuración óptima.
  {: .lab-note .info .compact}

  ```python
  BASE_PARTITIONS=200
  ADJUSTED_PARTITIONS=64
  assert ADJUSTED_PARTITIONS!=BASE_PARTITIONS
  print('baseline=',BASE_PARTITIONS,'adjusted=',ADJUSTED_PARTITIONS,'motivo=escenario A/B, no optimizacion garantizada')
  ```

  > **Salida esperada:** Imprime `baseline=200`, `adjusted=64` y deja constancia de que es una hipótesis de comparación.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 12 (Python):** cambia exclusivamente las particiones de shuffle a 64; mantén AQE habilitado.

  ```python
  spark.conf.set('spark.sql.shuffle.partitions',str(ADJUSTED_PARTITIONS))
  print('AQE=',spark.conf.get('spark.sql.adaptive.enabled'),'shuffle_partitions=',spark.conf.get('spark.sql.shuffle.partitions'))
  ```

  > **Salida esperada:** `AQE=true`, `shuffle_partitions=64`.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 13 (Python):** ejecuta un calentamiento del escenario ajustado sin añadirlo a las seis mediciones.

  ```python
  adjusted_warm=spark.sql(QUERY).collect()
  assert len(adjusted_warm)==len(preview)
  print('warmup_adjusted_grupos=',len(adjusted_warm))
  ```

  > **Salida esperada:** Se imprime la misma cantidad de grupos lógicos.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 14 (Python):** mide tres ejecuciones del escenario ajustado.

  ```python
  measure('adjusted',ADJUSTED_PARTITIONS,3)
  ```

  > **Salida esperada:** `mediciones_adjusted=3`.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 15 (Python):** verifica cantidad, fingerprint y particiones realmente declaradas en las seis mediciones.

  ```python
  assert len(records)==6 and len({x[5] for x in records})==1
  assert all(x[6] for x in records)
  print('total=',len(records),'fingerprints=',len({x[5] for x in records}),
        'all_aqe=',all(x[6] for x in records),'particiones=',sorted({x[7] for x in records}))
  ```

  > **Salida esperada:** `total=6`, `fingerprints=1`, `aqe=True`, particiones `64` y `200`.
  {: .lab-note .output .compact}

{% capture r3 %}{{ results[2] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r3 %}
{% include support-prompt.html task="tarea3" %}

---

## 🚀 Tarea 4. Guardar y analizar seis mediciones reales — 13 min

Comparar únicamente el experimento actual sin atribuir causalidad más allá del escenario medido.

- {% include step_label.html %} **Celda 16 (Python):** guarda exactamente seis mediciones en `tuning_metrics`, identificadas con el nuevo `experiment_id`.

  ```python
  from pyspark.sql.types import StructType,StructField,StringType,TimestampType,IntegerType,BooleanType,DoubleType,LongType
  SCHEMA_TUNING=StructType([StructField('experiment_id',StringType()),StructField('run_id',StringType()),
   StructField('recorded_at',TimestampType()),StructField('scenario',StringType()),
   StructField('repetition',IntegerType()),StructField('query_fingerprint',StringType()),
   StructField('aqe_enabled',BooleanType()),StructField('shuffle_partitions',IntegerType()),
   StructField('duration_seconds',DoubleType()),StructField('output_groups',LongType()),
   StructField('notes',StringType())])
  assert len({x[1] for x in records})==6
  spark.createDataFrame(records,SCHEMA_TUNING).write.mode('append').saveAsTable(TUNING)
  print('experiment_id=',EXPERIMENT,'guardadas=',len(records))
  ```

  > **Salida esperada:** `guardadas=6` con seis UUID distintos.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 17 (Python):** filtra **solo el experimento actual** y muestra las seis mediciones.

  ```python
  from pyspark.sql import functions as F
  tuned=spark.table(TUNING).filter(F.col('experiment_id')==EXPERIMENT)
  assert tuned.count()==6
  display(tuned.select('scenario','repetition','shuffle_partitions','duration_seconds','run_id').orderBy('scenario','repetition'))
  ```

  > **Salida esperada:** Tres `baseline` y tres `adjusted`, con duraciones reales.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 18 (Python):** calcula medianas por escenario y variación porcentual de `adjusted` respecto a `baseline`.

  ```python
  from statistics import median
  baseline_times=[x[8] for x in records if x[3]=='baseline']
  adjusted_times=[x[8] for x in records if x[3]=='adjusted']
  base_median=median(baseline_times)
  adjusted_median=median(adjusted_times)
  variation=100*(adjusted_median-base_median)/base_median
  print(f'baseline_mediana={base_median:.4f}s; adjusted_mediana={adjusted_median:.4f}s; variacion={variation:.2f}%')
  ```

  > **Salida esperada:** Dos medianas y un porcentaje real; negativo significa menos tiempo medido.
  {: .lab-note .output .compact}

- {% include step_label.html %} **En Spark UI** (si tienes acceso), contrasta shuffle read/write, spill y stages de ambas configuraciones. En ausencia de datos, registra **no disponible**: no lo conviertas en cero.

  > **Salida esperada:** Las observaciones de Spark UI quedan separadas de los tiempos almacenados en Delta.
  {: .lab-note .output .compact}

{% capture r4 %}{{ results[3] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r4 %}
{% include support-prompt.html task="tarea4" %}

---

## 🧩 Tarea 5. Evaluar costos y restaurar parámetros — 14 min

El análisis financiero distingue el tiempo de consulta de consumo DBU y cargos Azure facturados.

- {% include step_label.html %} **Interfaz → Compute → selecciona el clúster → Configuration:** consulta, si tienes acceso, runtime, modo de autoscaling, nodo driver y workers; no cambies la configuración durante esta práctica.

  > **Salida esperada:** Quedan registradas las propiedades que realmente muestra la interfaz.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Si la interfaz lo permite → Compute → Policies:** revisa las políticas disponibles. **No busques obligatoriamente `DE_BATCH3_POLICY`**; podría no existir.

  > **Salida esperada:** Política observada o `No disponible para esta cuenta`, sin bloquear el laboratorio.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 19 (Python):** calcula la duración acumulada de las consultas y las horas equivalentes **sin confundirlas con facturación**.

  ```python
  for name in ('baseline','adjusted'):
      times=[x[8] for x in records if x[3]==name]
      print(f'escenario={name}; consultas={len(times)}; segundos={sum(times):.4f}; horas_consulta={sum(times)/3600:.8f}')
  ```

  > **Salida esperada:** Se imprimen segundos y horas de consulta de ambos escenarios.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 20 (Python):** prepara un estado honesto sobre tarifas monetarias. Si no tienes tarifa de DBU, VM y duración facturada, deja el costo pendiente.

  ```python
  vm_hourly_rate=None
  dbu_hourly_rate=None
  measured_billing_hours=None
  status='PENDIENTE_TARIFAS_Y_USO_FACTURADO' if any(x is None for x in (vm_hourly_rate,dbu_hourly_rate,measured_billing_hours)) else 'TARIFAS_VERIFICADAS'
  print('estado=',status,'nota=las horas de consulta no equivalen a horas de facturacion')
  ```

  > **Salida esperada:** `estado=PENDIENTE_TARIFAS_Y_USO_FACTURADO`; no se muestra un precio inventado.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Celda 21 (Python):** restituye los parámetros previos de esta sesión (`ORIGINAL_AQE`, `ORIGINAL_SHUFFLE`) para no afectar siguientes actividades.

  ```python
  spark.conf.set('spark.sql.adaptive.enabled',ORIGINAL_AQE)
  spark.conf.set('spark.sql.shuffle.partitions',ORIGINAL_SHUFFLE)
  print('AQE_restaurado=',spark.conf.get('spark.sql.adaptive.enabled'),'shuffle_restaurado=',spark.conf.get('spark.sql.shuffle.partitions'))
  ```

  > **Salida esperada:** La configuración final coincide con los valores guardados en la celda 2.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Conserva** `tuning_metrics` y documenta qué escenario tuvo menor mediana, además de las limitaciones: tamaño de datos, AQE, clúster compartido, costo no verificado.

  > **Salida esperada:** Recomendación respaldada por seis tiempos reales y sin montos especulativos.
  {: .lab-note .output .compact}

{% capture r5 %}{{ results[4] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r5 %}
{% include support-prompt.html task="tarea5" %}