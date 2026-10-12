---
layout: lab
title: 'Práctica 6: Creación de workflow multi-tarea'
permalink: /lab6/lab6/
images_base: /labs/lab6/img
duration: 50 minutos
objective:
- Crear un Job de cuatro tareas Notebook con dependencias explícitas.
- Parametrizar ingesta, transformación y validación con datos reproducibles.
- Publicar task values y comprobar la auditoría Delta.
- Configurar Job Compute, timeouts, reintentos, notificación y concurrencia.
prerequisites:
- Workspace Databricks con acceso al catálogo dbricks_de_int y esquema batch1.
- Volumen de Unity Catalog dbricks_de_int.batch1.landing con permisos READ VOLUME y WRITE VOLUME.
- Permiso para crear y ejecutar Jobs y tablas Delta en el esquema de entrenamiento.
- Databricks Runtime 15.4 LTS y Job Compute autorizado por política institucional.
introduction:
- Construirás y ejecutarás cuatro notebooks coordinados en un DAG lineal. Usarás datos baseline con ocho filas para demostrar
  calidad, paso de task values y auditoría reproducible, sin modificar las tablas de las prácticas anteriores.
slug: lab6
lab_number: 6
final_result: El Job job_orders_orchestration_b2 ejecuta cuatro notebooks en orden, procesa ocho filas y registra tres válidas,
  cinco rechazadas y un duplicado excedente en p6_workflow_audit.
notes:
- Los objetos del workflow utilizan prefijo p6_ para evitar conflictos de esquema.
- No se configura programación recurrente; las ejecuciones son manuales.
- Los 50 minutos presuponen cómputo disponible y bloques listos para pegar.
references:
- text: Jobs and tasks
  url: https://docs.databricks.com/aws/en/jobs/
- text: Job parameters
  url: https://docs.databricks.com/aws/en/jobs/job-parameters
- text: Task values
  url: https://docs.databricks.com/aws/en/jobs/task-values
- text: Conditional tasks
  url: https://docs.databricks.com/aws/en/jobs/if-else
- text: Repair job runs
  url: https://docs.databricks.com/aws/en/jobs/repair-job-failures
prev: /lab5/lab5/
next: /lab7/lab7/
---

<!-- Lab con pasos de ejecución y verificación reproducibles. Catálogo dbricks_de_int validado. -->

---

## 🔎 Tarea 1. Preparar entorno y notebooks — 8 min

Verificar **Unity Catalog** y crear los cuatro notebooks que ejecutará el Job. Mantener aislados los objetos del workflow para evitar conflictos con las tablas de prácticas anteriores.

### Tarea 1.1. Comprobar acceso al workspace

Verificar cómputo y permisos de lectura sobre el catálogo antes de configurar las tareas.

- {% include step_label.html %} En Databricks, abre **Workspace** y localiza tu carpeta personal de trabajo.

  > **Salida esperada:** Se muestra tu directorio bajo /**Workspace**/Users/ y puedes crear elementos.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Abre** **Compute** y confirma que puedes utilizar **Job Compute** con acceso **Standard** y **Databricks Runtime 15.4 LTS**.

  > **Nota:** No crear un All-Purpose nuevo; el Job utilizará su propia configuración de cómputo.
  {: .lab-note .info .compact}

  > **Salida esperada:** Existe una política o configuración autorizada para ejecutar un Job con notebooks.
  {: .lab-note .output .compact}

- {% include step_label.html %} En un notebook Python temporal ejecuta esta consulta de permisos y contexto.

  ```python
  CATALOG = 'dbricks_de_int'
  SCHEMA = 'batch1'
  spark.sql(f'USE CATALOG `{CATALOG}`')
  spark.sql(f'USE SCHEMA `{SCHEMA}`')
  spark.sql('SELECT current_catalog(), current_schema()').show()
  assert spark.sql('SELECT current_catalog()').first()[0] == CATALOG
  assert spark.sql('SELECT current_schema()').first()[0] == SCHEMA
  assert spark.sql(f"SHOW SCHEMAS IN `{CATALOG}`").filter("databaseName = 'batch1'").count() == 1
  assert spark.sql(f"SHOW VOLUMES IN `{CATALOG}`.`{SCHEMA}`").filter("volume_name = 'landing'").count() == 1, 'Falta Volume landing'
  display(dbutils.fs.ls(f'/Volumes/{CATALOG}/{SCHEMA}/landing'))
  print('VALIDACIÓN EXITOSA: catálogo, esquema y Volume landing disponibles')
  ```

  > **Salida esperada:** Se muestra dbricks_de_int.batch1; el Volume landing es accesible y aparece VALIDACIÓN EXITOSA.
  {: .lab-note .output .compact}

### Tarea 1.2. Crear cuatro notebooks del workflow

Organizar los notebooks en una carpeta específica y diferenciar sus responsabilidades.

- {% include step_label.html %} En **Workspace** crea la carpeta **workflow_6** dentro de tu directorio personal.

  > **Salida esperada:** La carpeta **workflow_6** aparece en el árbol de **Workspace**.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Crea** el notebook Python **01_ingest_landing** dentro de **workflow_6**.

  > **Salida esperada:** El notebook se muestra con el nombre exacto y lenguaje Python.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Crea** el notebook Python **02_transform_orders** dentro de **workflow_6**.

  > **Salida esperada:** El notebook se muestra con el nombre exacto y lenguaje Python.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Crea** el notebook Python **03_validate_orders** dentro de **workflow_6**.

  > **Salida esperada:** El notebook se muestra con el nombre exacto y lenguaje Python.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Crea** el notebook Python **04_publish_run_metrics** dentro de **workflow_6**.

  > **Salida esperada:** El notebook se muestra con el nombre exacto y lenguaje Python.
  {: .lab-note .output .compact}

### Tarea 1.3. Definir parámetros compartidos

Establecer el contrato que utilizarán las tareas de las Prácticas 6 y 7.

- {% include step_label.html %} **Revisa** los cinco parámetros comunes que configuraremos a nivel de Job.

  ```python
  # Parámetros para el Job (referencia; no es necesario ejecutarlos):
  # batch_id=wf_baseline_001 ; run_date=2026-10-11
  # source_file=wf_baseline_001.csv
  # quality_scenario=baseline ; simulate_publish_failure=false
  ```

  > **Salida esperada:** Parámetros previstos: **batch_id**, **run_date**, **source_file**, **quality_scenario** y **simulate_publish_failure**.
  {: .lab-note .output .compact}

{% assign results = site.data.task-results[page.slug].results %}
{% capture r1 %}{{ results[0] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r1 %}

{% include support-prompt.html task="tarea1" %}

---

## ☁️ Tarea 2. Implementar ingesta y transformación — 12 min

Construir los dos notebooks iniciales: crear un CSV controlado con escenarios reproducibles y guardarlo en una tabla **Delta** exclusiva del workflow.

### Tarea 2.1. Generar un CSV controlado con escenarios

Crear ocho registros **baseline** y tres registros en cada escenario posterior mediante un parámetro.

- {% include step_label.html %} **Abre** **01_ingest_landing** y pega el bloque completo de generación del archivo.

  > **Nota:** El nombre del archivo solo admite caracteres seguros y termina en .csv; el directorio landing debe existir.
  {: .lab-note .info .compact}

  ```python
  import csv, io, re
  from datetime import date
  for key, default in {'batch_id':'wf_baseline_001','run_date':'2026-10-11','source_file':'wf_baseline_001.csv','quality_scenario':'baseline'}.items():
      dbutils.widgets.text(key,default)
  batch_id=dbutils.widgets.get('batch_id').strip()
  run_date=dbutils.widgets.get('run_date').strip()
  source_file=dbutils.widgets.get('source_file').strip()
  scenario=dbutils.widgets.get('quality_scenario').strip()
  assert re.fullmatch(r'[A-Za-z0-9_-]+',batch_id)
  assert re.fullmatch(r'[A-Za-z0-9_-]+\.csv',source_file)
  assert scenario in {'baseline','valid','invalid'}
  date.fromisoformat(run_date)
  rows_by_scenario={
   'baseline':[['O1001','C001','100.00'],['O1002','C002','75.00'],['O1003','C003','20.00'],['O1004','C004','15.00'],['O1004','C004','15.00'],['O1005','C005','-5.00'],['O1006','C006',''],['','C007','12.00']],
   'valid':[['O2001','C011','30.00'],['O2002','C012','60.00'],['O2003','C013','90.00']],
   'invalid':[['O3001','C021','40.00'],['INVALID','C022','20.00'],['O3003','C023','']]
  }
  rows=rows_by_scenario[scenario]
  buf=io.StringIO(); w=csv.writer(buf)
  w.writerow(['order_id','customer_id','amount']);w.writerows(rows)
  path=f'/Volumes/dbricks_de_int/batch1/landing/{source_file}'
  dbutils.fs.put(path,buf.getvalue(),True)
  print(f'Escenario={scenario}; Archivo={path}; Filas={len(rows)}')
  print('VALIDACIÓN EXITOSA: comprobaciones del paso completadas')
  ```

  > **Salida esperada:** La celda queda preparada para los tres escenarios sin modificar el código entre ejecuciones.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Ejecuta** **01_ingest_landing** con los valores predeterminados del escenario **baseline**.

  > **Salida esperada:** Se muestra Escenario=**baseline** y Filas=8.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Verifica** con Python que el CSV de **baseline** contiene exactamente ocho filas de datos.

  ```python
  file_path='/Volumes/dbricks_de_int/batch1/landing/wf_baseline_001.csv'
  check=spark.read.option('header',True).csv(file_path)
  assert check.count()==8
  assert check.columns==['order_id','customer_id','amount']
  print('CSV validado: 8 filas')
  print('VALIDACIÓN EXITOSA: comprobaciones del paso completadas')
  ```

  > **Salida esperada:** La aserción se completa y aparece CSV validado: 8 filas.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Identifica** en el CSV un identificador duplicado, un importe negativo, un importe vacío y un ID vacío.

  ```python
  display(check.orderBy('order_id','amount'))
  ```

  > **Salida esperada:** Los cuatro tipos de observación aparecen en la vista previa.
  {: .lab-note .output .compact}

### Tarea 2.2. Persistir datos de origen en una tabla Delta aislada

Evitar escrituras en bronze_orders o en las tablas de auditoría usadas por otras prácticas.

- {% include step_label.html %} **Abre** **02_transform_orders** y agrega la definición de widgets y el lector CSV.

  ```python
  from pyspark.sql import functions as F
  import re
  for key,default in {'batch_id':'wf_baseline_001','run_date':'2026-10-11','source_file':'wf_baseline_001.csv'}.items():
      dbutils.widgets.text(key,default)
  batch_id=dbutils.widgets.get('batch_id').strip()
  run_date=dbutils.widgets.get('run_date').strip()
  source_file=dbutils.widgets.get('source_file').strip()
  assert re.fullmatch(r'[A-Za-z0-9_-]+',batch_id)
  assert re.fullmatch(r'[A-Za-z0-9_-]+\.csv',source_file)
  source_path=f'/Volumes/dbricks_de_int/batch1/landing/{source_file}'
  raw=spark.read.option('header',True).option('inferSchema',False).csv(source_path)
  assert raw.columns==['order_id','customer_id','amount']
  print('Columnas originales verificadas')
  print('VALIDACIÓN EXITOSA: comprobaciones del paso completadas')
  ```

  > **Salida esperada:** Se leen los parámetros y se prepara el **DataFrame** de origen.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Ejecuta** el bloque para crear **p6_workflow_orders** con contrato **Delta** explícito.

  ```python
  spark.sql('''CREATE TABLE IF NOT EXISTS dbricks_de_int.batch1.p6_workflow_orders (
   batch_id STRING, run_date DATE, source_file STRING, order_id STRING,
   customer_id STRING, amount_raw STRING, ingested_at TIMESTAMP
  ) USING DELTA''')
  assert spark.catalog.tableExists('dbricks_de_int.batch1.p6_workflow_orders')
  print('Tabla Delta preparada')
  print('VALIDACIÓN EXITOSA: comprobaciones del paso completadas')
  ```

  > **Salida esperada:** La tabla existe y DESCRIBE DETAIL indica formato delta.
  {: .lab-note .output .compact}

- {% include step_label.html %} Transforma el **DataFrame** agregando lote, fecha de proceso, origen y marca temporal.

  ```python
  orders=(raw.select(F.lit(batch_id).alias('batch_id'),F.to_date(F.lit(run_date)).alias('run_date'),
   F.lit(source_file).alias('source_file'),F.col('order_id'),F.col('customer_id'),
   F.col('amount').alias('amount_raw'),F.current_timestamp().alias('ingested_at')))
  assert len(orders.columns)==7
  print('Esquema de ingesta validado')
  print('VALIDACIÓN EXITOSA: comprobaciones del paso completadas')
  ```

  > **Salida esperada:** El esquema contiene los siete campos del contrato **Delta**.
  {: .lab-note .output .compact}

- {% include step_label.html %} Escribe solo la partición lógica de **batch_id** mediante reemplazo selectivo.

  > **Nota:** replaceWhere evita duplicados al reejecutar el mismo lote; nunca se sobrescribe toda la tabla.
  {: .lab-note .info .compact}

  ```python
  (orders.write.format('delta').mode('overwrite')
   .option('replaceWhere',f"batch_id = '{batch_id}'")
   .saveAsTable('dbricks_de_int.batch1.p6_workflow_orders'))
  count=spark.table('dbricks_de_int.batch1.p6_workflow_orders').filter(F.col('batch_id')==batch_id).count()
  assert count==raw.count()
  print(f'Filas persistidas: {count}')
  print('VALIDACIÓN EXITOSA: comprobaciones del paso completadas')
  ```

  > **Salida esperada:** La tabla contiene ocho registros **baseline**, sin duplicarse en una repetición.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Consulta** el lote **baseline** y verifica la presencia de los datos crudos.

  ```python
  display(spark.sql("SELECT order_id, customer_id, amount_raw FROM dbricks_de_int.batch1.p6_workflow_orders WHERE batch_id='wf_baseline_001' ORDER BY order_id"))
  ```

  > **Salida esperada:** Se observan 8 registros, incluido el importe negativo y los duplicados.
  {: .lab-note .output .compact}

{% capture r2 %}{{ results[1] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r2 %}

{% include support-prompt.html task="tarea2" %}

---

## 🚀 Tarea 3. Implementar validación y auditoría — 10 min

Evaluar calidad por fila, publicar métricas de tarea y persistir los resultados sin mezclar contratos de otras prácticas.

### Tarea 3.1. Clasificar filas y publicar task values

Tratar los duplicados, los importes incorrectos y los identificadores vacíos como rechazos observables.

- {% include step_label.html %} **Abre** **03_validate_orders** y pega el lector de parámetros y los registros del lote.

  ```python
  from pyspark.sql import functions as F
  from pyspark.sql.window import Window
  for k,v in {'batch_id':'wf_baseline_001'}.items(): dbutils.widgets.text(k,v)
  batch_id=dbutils.widgets.get('batch_id').strip()
  source=spark.table('dbricks_de_int.batch1.p6_workflow_orders').filter(F.col('batch_id')==batch_id)
  assert source.count()>0,'No existen filas para el batch_id recibido'
  print(f'Filas a validar: {source.count()}')
  print('VALIDACIÓN EXITOSA: comprobaciones del paso completadas')
  ```

  > **Salida esperada:** La tarea encuentra los registros provenientes de **02_transform_orders**.
  {: .lab-note .output .compact}

- {% include step_label.html %} Calcula importe decimal, duplicados y una causa de rechazo por registro.

  ```python
  typed=(source.withColumn('amount',F.expr('try_cast(amount_raw AS DECIMAL(18,2))'))
   .withColumn('order_id',F.trim('order_id')))
  counts=Window.partitionBy('batch_id','order_id')
  validated=(typed.withColumn('duplicates',F.count(F.lit(1)).over(counts))
   .withColumn('validation_reason',F.concat_ws('; ',
   F.when(F.col('order_id').isNull() | (F.col('order_id')==''),'invalid_order_id'),
   F.when(~F.col('order_id').rlike('^O[0-9]+$') & (F.col('order_id')!=''),'invalid_order_id'),
   F.when(F.col('customer_id').isNull() | (F.trim('customer_id')==''),'invalid_customer_id'),
   F.when(F.col('amount').isNull(),'invalid_amount'),
   F.when(F.col('amount')<=0,'non_positive_amount'),
   F.when(F.col('duplicates')>1,'duplicate_order_id'))))
  assert 'validation_reason' in validated.columns
  print('Reglas de calidad aplicadas')
  print('VALIDACIÓN EXITOSA: comprobaciones del paso completadas')
  ```

  > **Salida esperada:** Se genera validation_reason en cada fila: vacío solo para registros aprobados.
  {: .lab-note .output .compact}

- {% include step_label.html %} Calcula total, válidos, rechazados y duplicados excedentes.

  ```python
  total_rows=validated.count()
  valid_rows=validated.filter(F.col('validation_reason')=='').count()
  invalid_rows=total_rows-valid_rows
  duplicate_excess=int(validated.groupBy('order_id').count().selectExpr('greatest(count - 1,0) as extra').agg(F.sum('extra')).first()[0] or 0)
  status='APPROVED' if invalid_rows==0 else 'OBSERVED'
  assert total_rows==valid_rows+invalid_rows
  print(f'total={total_rows}; valid={valid_rows}; invalid={invalid_rows}; duplicates={duplicate_excess}; status={status}')
  print('VALIDACIÓN EXITOSA: comprobaciones del paso completadas')
  ```

  > **Salida esperada:** Baseline: total=8, valid=3, invalid=5 y **duplicate_excess**=1.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Guarda** el lote validado en p6_workflow_validated usando reemplazo por **batch_id**.

  ```python
  spark.sql('''CREATE TABLE IF NOT EXISTS dbricks_de_int.batch1.p6_workflow_validated (
   batch_id STRING, run_date DATE, source_file STRING, order_id STRING, customer_id STRING,
   amount_raw STRING, ingested_at TIMESTAMP, amount DECIMAL(18,2), duplicates BIGINT,
   validation_reason STRING
  ) USING DELTA''')
  (validated.write.format('delta').mode('overwrite')
   .option('replaceWhere',f"batch_id = '{batch_id}'")
   .saveAsTable('dbricks_de_int.batch1.p6_workflow_validated'))
  assert spark.table('dbricks_de_int.batch1.p6_workflow_validated').filter(F.col('batch_id')==batch_id).count()==total_rows
  print('Filas validadas persistidas')
  print('VALIDACIÓN EXITOSA: comprobaciones del paso completadas')
  ```

  > **Salida esperada:** La tabla **Delta** conserva la razón de rechazo y permite repetir el lote.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Publica** métricas pequeñas mediante dbutils.jobs.taskValues.set.

  > **Nota:** Los **task values** se recuperan entre tareas dentro de una misma ejecución del Job.
  {: .lab-note .info .compact}

  ```python
  for k,v in {'total_rows':total_rows,'valid_rows':valid_rows,'invalid_rows':invalid_rows,
             'duplicate_excess':duplicate_excess,'quality_status':status}.items():
   dbutils.jobs.taskValues.set(key=k,value=v)
  print('Task values publicados: total_rows, valid_rows, invalid_rows, duplicate_excess, quality_status')
  ```

  > **Salida esperada:** Se crean cinco valores y el log indica el estado de calidad.
  {: .lab-note .output .compact}

### Tarea 3.2. Persistir las métricas de auditoría

Crear una tabla de eventos compatible también con las ramas condicionales de la Práctica 7.

- {% include step_label.html %} **Abre** **04_publish_run_metrics** y agrega el lector de parámetros y **task values**.

  ```python
  from pyspark.sql import functions as F
  for k,v in {'batch_id':'wf_baseline_001'}.items(): dbutils.widgets.text(k,v)
  batch_id=dbutils.widgets.get('batch_id').strip()
  keys=['total_rows','valid_rows','invalid_rows','duplicate_excess','quality_status']
  vals={k:dbutils.jobs.taskValues.get(taskKey='validate_orders',key=k,debugValue=-1) for k in keys}
  assert all(vals[k]!=-1 for k in keys),'Faltan task values; ejecuta el Job completo'
  print(vals)
  print('VALIDACIÓN EXITOSA: comprobaciones del paso completadas')
  ```

  > **Salida esperada:** Los cinco valores proceden de la tarea **validate_orders** del mismo Run.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Crea** la tabla **p6_workflow_audit** antes de registrar el evento de ejecución.

  ```python
  spark.sql('''CREATE TABLE IF NOT EXISTS dbricks_de_int.batch1.p6_workflow_audit (
   batch_id STRING, event_type STRING, quality_status STRING,
   total_rows BIGINT, valid_rows BIGINT, invalid_rows BIGINT, duplicate_excess BIGINT,
   message STRING, event_timestamp TIMESTAMP
  ) USING DELTA''')
  assert spark.catalog.tableExists('dbricks_de_int.batch1.p6_workflow_audit')
  print('Auditoría preparada')
  print('VALIDACIÓN EXITOSA: comprobaciones del paso completadas')
  ```

  > **Salida esperada:** La tabla tiene un contrato uniforme para las Prácticas 6 y 7.
  {: .lab-note .output .compact}

- {% include step_label.html %} Inserta un evento **QUALITY_CHECK** sustituyendo eventos previos del mismo lote y tipo.

  ```python
  table='dbricks_de_int.batch1.p6_workflow_audit'
  spark.sql(f"DELETE FROM {table} WHERE batch_id = '{batch_id}' AND event_type = 'QUALITY_CHECK'")
  records=[(batch_id,'QUALITY_CHECK',str(vals['quality_status']),int(vals['total_rows']),
   int(vals['valid_rows']),int(vals['invalid_rows']),int(vals['duplicate_excess']),'Métricas desde validate_orders')]
  audit=spark.createDataFrame(records,'batch_id string,event_type string,quality_status string,total_rows long,valid_rows long,invalid_rows long,duplicate_excess long,message string').withColumn('event_timestamp',F.current_timestamp())
  audit.write.format('delta').mode('append').saveAsTable(table)
  assert spark.table(table).filter((F.col('batch_id')==batch_id)&(F.col('event_type')=='QUALITY_CHECK')).count()==1
  print('QUALITY_CHECK registrado')
  print('VALIDACIÓN EXITOSA: comprobaciones del paso completadas')
  ```

  > **Salida esperada:** Existe un único evento **QUALITY_CHECK** para el lote actual.
  {: .lab-note .output .compact}

{% capture r3 %}{{ results[2] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r3 %}

{% include support-prompt.html task="tarea3" %}

---

## 🧩 Tarea 4. Crear y configurar Job multi-tarea — 13 min

Configurar **Job Compute**, los cinco parámetros y las dependencias exactas de los cuatro notebooks, incluyendo reintento, timeout y alertas.

### Tarea 4.1. Crear el Job y configurar cómputo

Utilizar un **Job Compute** compartido por las cuatro tareas, sujeto a la política institucional.

- {% include step_label.html %} En Databricks abre Workflows o **Jobs & Pipelines** y selecciona Create job.

  > **Salida esperada:** Se abre el editor de un Job nuevo.
  {: .lab-note .output .compact}

- {% include step_label.html %} Asigna el nombre **job_orders_orchestration_b2** y guarda.

  > **Salida esperada:** El nombre aparece en la cabecera del Job.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Crea** la primera Notebook task **ingest_landing** y selecciona **01_ingest_landing**.

  > **Salida esperada:** La tarea muestra el notebook de ingesta en workflow_6.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Configura** **Job Compute** con Runtime 15.4 LTS, **Standard** y 1 worker si está permitido.

  > **Nota:** Los nombres exactos de las propiedades pueden variar por cloud y política; reutiliza el mismo **Job Compute** en las tareas.
  {: .lab-note .info .compact}

  > **Salida esperada:** La tarea dispone de cómputo de Job válido.
  {: .lab-note .output .compact}

### Tarea 4.2. Agregar las tareas y sus dependencias

Formar un **DAG** lineal sin tareas aisladas.

- {% include step_label.html %} **Crea** **transform_orders** con el notebook **02_transform_orders** y dependencia **ingest_landing**.

  > **Salida esperada:** Se muestra una flecha desde **ingest_landing** hacia **transform_orders**.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Crea** **validate_orders** con el notebook **03_validate_orders** y dependencia **transform_orders**.

  > **Salida esperada:** El tercer nodo aparece conectado únicamente después de **transform_orders**.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Crea** **publish_run_metrics** con **04_publish_run_metrics** y dependencia **validate_orders**.

  > **Salida esperada:** La última tarea depende de **validate_orders** y podrá recuperar **task values**.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Selecciona** el mismo **Job Compute** en las cuatro tareas.

  > **Salida esperada:** Las cuatro tareas utilizan la misma definición de cómputo.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Inspecciona** el grafo y verifica el orden completo de las dependencias.

  > **Salida esperada:** **DAG**: **ingest_landing** → **transform_orders** → **validate_orders** → **publish_run_metrics**.
  {: .lab-note .output .compact}

### Tarea 4.3. Configurar controles de ejecución

Restringir concurrencia y registrar avisos sin crear una programación recurrente.

- {% include step_label.html %} En **Job parameters** crea los cinco parámetros con los valores **baseline** establecidos.

  ```python
  # Valores en la interfaz Job parameters:
  # batch_id = wf_baseline_001
  # run_date = 2026-10-11
  # source_file = wf_baseline_001.csv
  # quality_scenario = baseline
  # simulate_publish_failure = false
  ```

  > **Salida esperada:** Aparecen **batch_id**, **run_date**, **source_file**, **quality_scenario** y **simulate_publish_failure**.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Configura** Timeout = 20 minutos en cada tarea y Max concurrent runs = 1 en el Job.

  > **Salida esperada:** Los límites se muestran guardados en la configuración.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Configura** 1 reintento en **transform_orders** y 0 en las otras tareas; agrega notificación On failure.

  > **Salida esperada:** La configuración permite observar fallos sin reintentar tareas distintas de **transform_orders**.
  {: .lab-note .output .compact}

{% capture r4 %}{{ results[3] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r4 %}

{% include support-prompt.html task="tarea4" %}

---

## ✅ Tarea 5. Ejecutar y verificar el workflow — 7 min

Lanzar un Run manual y comprobar evidencias en el grafo, registros **Delta** y métricas de calidad.

### Tarea 5.1. Ejecutar una instancia del Job

Validar que la orquestación complete las cuatro tareas con el lote **baseline**.

- {% include step_label.html %} **Selecciona** **Run now** en el Job con los cinco parámetros **baseline** y abre la ejecución.

  > **Salida esperada:** Aparece un Run con cuatro tareas en orden; al completar debe indicar **Succeeded**.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Inspecciona** las salidas de **validate_orders** y **publish_run_metrics**.

  > **Salida esperada:** El log de **validate_orders** muestra 8, 3, 5, 1 y **OBSERVED**; la última tarea publica auditoría.
  {: .lab-note .output .compact}

### Tarea 5.2. Confirmar el contrato para la siguiente práctica

Verificar que los resultados persisten sin alterar tablas de los laboratorios anteriores.

- {% include step_label.html %} En un notebook conectado comprueba las métricas del lote **baseline**.

  ```python
  row=spark.sql("SELECT total_rows,valid_rows,invalid_rows,duplicate_excess,quality_status FROM dbricks_de_int.batch1.p6_workflow_audit WHERE batch_id='wf_baseline_001' AND event_type='QUALITY_CHECK' ORDER BY event_timestamp DESC LIMIT 1").first()
  assert row and tuple(row)==(8,3,5,1,'OBSERVED'),row
  print('Auditoría baseline validada')
  print('VALIDACIÓN EXITOSA: comprobaciones del paso completadas')
  ```

  > **Salida esperada:** Se muestra exactamente 8/3/5/1 con estado **OBSERVED**.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Conserva** los cuatro notebooks, el Job y las tablas p6_ para la Práctica 7.

  > **Nota:** No ejecutes DROP TABLE ni borres el Job; la Práctica 7 lo extenderá.
  {: .lab-note .info .compact}

  > **Salida esperada:** Los recursos siguen disponibles y no se ha borrado información de otras prácticas.
  {: .lab-note .output .compact}

{% capture r5 %}{{ results[4] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r5 %}

{% include support-prompt.html task="tarea5" %}

