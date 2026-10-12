---
layout: lab
title: 'Práctica 7: Configuración de tareas condicionales y recuperación ante fallos'
permalink: /lab7/lab7/
images_base: /labs/lab7/img
duration: 50 minutos
objective:
- Ampliar el Job de la Práctica 6 con una condición If/else basada en task values.
- Promover lotes aprobados y colocar los registros rechazados en cuarentena.
- Provocar una falla controlada y recuperar mediante Repair run.
- Verificar resultados y eventos de auditoría en tablas Delta aisladas.
prerequisites:
- Práctica 6 completada con el Job job_orders_orchestration_b2 y los cuatro notebooks en workflow_6.
- Tablas Delta p6_workflow_orders, p6_workflow_validated y p6_workflow_audit disponibles.
- Permisos para editar el Job y escribir tablas en dbricks_de_int.batch1, y WRITE VOLUME en landing.
- Job Compute compatible y acceso al volumen landing de Unity Catalog.
introduction:
- Extenderás el mismo Job de cuatro tareas con If/else, promoción y cuarentena. Tres lotes pequeños permitirán verificar rutas
  True/False y reparar una falla controlada sin repetir tareas completadas.
slug: lab7
lab_number: 7
final_result: Los escenarios wf_valid_001, wf_invalid_001 y wf_repair_001 registran respectivamente promoción, dos rechazos
  y recuperación selectiva mediante Repair run.
notes:
- No modifica la lógica de validación ni los datos de los laboratorios 1 a 5.
- No ejecuta un pipeline DLT; prepara tablas Delta para su consumo posterior.
- La reparación reutiliza los task values del Run; una marca de fallo de una sola vez evita editar parámetros durante Repair run.
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
prev: /lab6/lab6/
next: /lab8/lab8/
---

<!-- Lab con pasos de ejecución y verificación reproducibles. -->

---

## 🔎 Tarea 1. Preparar workflow y parámetros — 7 min

> **Mapa de trabajo:** en esta práctica usarás **tres lugares**: (1) **Jobs & Pipelines > job_orders_orchestration_b2 > Tasks/Runs** para editar el DAG y consultar ejecuciones; (2) **Workspace > tu carpeta personal > workflow_6** para editar notebooks; (3) **Workspace > tu carpeta personal > batch_1 > 07_control_workflow** para ejecutar las validaciones independientes. Este último notebook se crea en la Tarea 1 y sus celdas se agregan **en orden** del 1 al 8. Las celdas de `05_promote_for_dlt` se numeran 1–3 y las de `06_quarantine_invalid_rows` 1–2. **No ejecutes manualmente las celdas de promoción/cuarentena durante los escenarios**; las ejecuta el Job con los parámetros del Run.
{: .lab-note .important .compact}

Reutilizar el Job existente de la Práctica 6 y comprobar sus parámetros para ejecutar escenarios aprobado, observado y falla controlada.

### Tarea 1.1. Revisar el Job multi-tarea

Comprobar el **DAG** y el contrato de auditoría antes de agregar condiciones.

- {% include step_label.html %} En **Jobs & Pipelines > Jobs**, busca `job_orders_orchestration_b2`, haz clic en su nombre y abre **Tasks** para visualizar el DAG. No crees otro Job.

  > **Salida esperada:** Se muestran **ingest_landing**, **transform_orders**, **validate_orders** y **publish_run_metrics**.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Jobs & Pipelines > job_orders_orchestration_b2 > Tasks**, selecciona cada nodo y comprueba el campo **Notebook**: `ingest_landing` → `01_ingest_landing`, `transform_orders` → `02_transform_orders`, `validate_orders` → `03_validate_orders` y `publish_run_metrics` → `04_publish_run_metrics`. Comprueba además las flechas en el DAG.

  > **Salida esperada:** El **DAG** muestra cuatro tareas secuenciales sin enlaces rotos.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Workspace > tu carpeta personal > batch_1**, crea un notebook Python `07_control_workflow` y conéctalo al mismo cluster All-Purpose de las prácticas anteriores. En su **celda 1 (Python)** pega el siguiente bloque para comprobar las tablas p6_; ejecútalo con **Shift+Enter**.

  ```python
  C = 'dbricks_de_int'; S = 'batch1'
  spark.sql(f'USE CATALOG `{C}`')
  spark.sql(f'USE SCHEMA `{S}`')
  required = ['p6_workflow_orders', 'p6_workflow_validated', 'p6_workflow_audit']
  for name in required:
      full_name = f'{C}.{S}.{name}'
      assert spark.catalog.tableExists(full_name), f'Falta {full_name}'
      print(f'VALIDACIÓN EXITOSA: tabla={full_name}; existe=Sí')
  assert spark.sql(f"SHOW VOLUMES IN `{C}`.`{S}`").filter(
      "volume_name = 'landing'").count() == 1, 'No existe Volume landing'
  print(f'VALIDACIÓN EXITOSA: Volume={C}.{S}.landing; existe=Sí; comprobado_por=SHOW VOLUMES')
  ```

  > **Salida esperada:** Tres líneas confirman las tablas p6_workflow_orders, p6_workflow_validated y p6_workflow_audit; otra confirma dbricks_de_int.batch1.landing.
  {: .lab-note .output .compact}

### Tarea 1.2. Verificar parámetros de escenarios

Utilizar valores de lote distintos para evitar mezclas de registros y auditoría.

- {% include step_label.html %} Vuelve a **Jobs & Pipelines > job_orders_orchestration_b2 > Job parameters**. Localiza las cinco filas `batch_id`, `run_date`, `source_file`, `quality_scenario` y `simulate_publish_failure`; todavía no cambies sus valores.

  > **Salida esperada:** Existen **batch_id**, **run_date**, **source_file**, **quality_scenario** y **simulate_publish_failure**.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Workspace > tu carpeta personal > workflow_6**, abre `01_ingest_landing`. En su **primera celda de código**, busca `rows_by_scenario` y verifica que aparecen las claves `baseline`, `valid` e `invalid`. No ejecutes ni modifiques esta celda.

  > **Salida esperada:** El código de **01_ingest_landing** define exactamente esos tres escenarios.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Jobs & Pipelines > job_orders_orchestration_b2 > Tasks**, abre `ingest_landing` y revisa **Compute**; al crear las nuevas tareas selecciona esa misma definición de Job Compute. No crees un cluster All-Purpose nuevo.

  > **Salida esperada:** No se crea un clúster extra y las tareas Notebook comparten cómputo.
  {: .lab-note .output .compact}

{% assign results = site.data.task-results[page.slug].results %}
{% capture r1 %}{{ results[0] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r1 %}

{% include support-prompt.html task="tarea1" %}

---

## ☁️ Tarea 2. Publicar métricas y configurar If/else — 12 min

Aprovechar **quality_status** publicado por **validate_orders** para seleccionar entre promoción y cuarentena sin sustituir la lógica del notebook.

### Tarea 2.1. Validar task values del workflow

Confirmar que el nombre técnico de la tarea y la clave coinciden con la condición.

- {% include step_label.html %} En **Workspace > tu carpeta personal > workflow_6**, abre `03_validate_orders`. Localiza la **celda que contiene** `dbutils.jobs.taskValues.set`, hacia el final del notebook. Confirma las cinco claves publicadas y vuelve al Job; no cambies el código.

  > **Salida esperada:** Existen las claves total_rows, valid_rows, invalid_rows, **duplicate_excess** y **quality_status**.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `07_control_workflow`, crea la **celda 2 (Python)**, inmediatamente debajo de la validación de tablas. Pega la consulta siguiente y ejecútala; comprueba el lote `wf_baseline_001` en `p6_workflow_audit`.

  ```python
  r = spark.sql("SELECT quality_status, invalid_rows FROM dbricks_de_int.batch1.p6_workflow_audit WHERE batch_id='wf_baseline_001' AND event_type='QUALITY_CHECK' ORDER BY event_timestamp DESC LIMIT 1").first()
  assert r is not None, 'No existe auditoría baseline de Práctica 6'
  assert r.quality_status == 'OBSERVED' and r.invalid_rows == 5
  print(f'VALIDACIÓN EXITOSA: lote=wf_baseline_001; quality_status={r.quality_status}; invalid_rows={r.invalid_rows}; rama_esperada=False (cuarentena)')
  ```

  > **Salida esperada:** `lote=wf_baseline_001; quality_status=OBSERVED; invalid_rows=5; rama_esperada=False (cuarentena)`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Jobs & Pipelines > job_orders_orchestration_b2 > Tasks**, abre el nodo `publish_run_metrics`; en **Depends on** comprueba `validate_orders`. Después abre su **último Run exitoso > Output** para verificar el evento `QUALITY_CHECK`.

  > **Salida esperada:** La publicación original permanece activa y sigue consumiendo **task values**.
  {: .lab-note .output .compact}

- {% include step_label.html %} En el editor del Job, observa la salida de `validate_orders` en el DAG. La condición nueva debe evaluar el valor `quality_status` igual a `APPROVED`; `OBSERVED` tomará la rama False. No cambies el notebook.

  > **Salida esperada:** Los escenarios válidos irán a True y los inválidos a False.
  {: .lab-note .output .compact}

- {% include step_label.html %} Permanece en **Jobs & Pipelines > job_orders_orchestration_b2 > Tasks**. Ubica el botón **Add task (+)** próximo a `validate_orders`: allí agregarás la condición antes de conectar las ramas.

  > **Salida esperada:** Se reconoce el nodo previo al nuevo **If/else**.
  {: .lab-note .output .compact}

### Tarea 2.2. Configurar `evaluate_quality_gate` en el editor actual

La interfaz de esta versión de Databricks presenta la condición en **dos renglones dentro de Condition**. Sigue exactamente los controles visibles en la captura. **No ejecutes código en un notebook para esta subtarea**: toda la configuración se realiza en el editor del Job.

- {% include step_label.html %} En **Jobs & Pipelines > job_orders_orchestration_b2 > Tasks**, pulsa **Add task** si aún no existe la compuerta. En el panel que aparece debajo del DAG, escribe `evaluate_quality_gate` en **Task name** y elige **If/else condition** en **Type**. Si ya aparece el nodo `evaluate_quality_gate`, selecciónalo y **edita la tarea existente sin crear otra**.

  > **Salida esperada:** El panel muestra **Task name = evaluate_quality_gate** y **Type = If/else condition**. En el DAG hay una sola tarea con ese nombre.
  {: .lab-note .output .compact}

- {% include step_label.html %} En el mismo panel, localiza **Depends on** debajo de **Condition**. Abre el desplegable, selecciona únicamente `validate_orders` y deja **Run if dependencies = All succeeded**. No selecciones `publish_run_metrics` como dependencia de la compuerta.

  > **Salida esperada:** **Depends on** contiene `validate_orders` y **Run if dependencies** indica `All succeeded`. La condición se evaluará cuando termine correctamente la validación.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la sección **Condition**, usa la **primera fila**: en la caja superior pega la referencia dinámica de abajo. A la derecha de esta caja aparece el selector con icono **`{}`**; deja la referencia como expresión dinámica. En la **segunda fila**, abre el desplegable de la izquierda y selecciona **`==`**.

  ```text
  {% raw %}{{tasks.validate_orders.values.quality_status}}{% endraw %}
  ```

  > **Salida esperada:** La primera fila contiene exactamente `{{tasks.validate_orders.values.quality_status}}` y el operador de la segunda fila es `==`. No escribas las llaves en el nombre de la tarea ni en **Depends on**.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Condition**, en la **segunda fila y a la derecha del operador `==`**, escribe `APPROVED` como valor de comparación. Déjalo como texto literal, no como expresión dinámica. Comprueba que la condición completa sea **`{{tasks.validate_orders.values.quality_status}} == APPROVED`** y que **Depends on** siga indicando `validate_orders`. Si creaste o modificaste algo, pulsa **Save task** al pie del panel.

  ```text
  APPROVED
  ```

  > **Salida esperada:** El DAG muestra la condición `quality_status == APPROVED`; los lotes con estado `APPROVED` tomarán **True**, mientras que `OBSERVED` tomará **False**. El botón **Save task** puede permanecer gris si no hubo modificaciones.
  {: .lab-note .output .compact}

{% capture r2 %}{{ results[1] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r2 %}

{% include support-prompt.html task="tarea2" %}

---

## 🚀 Tarea 3. Implementar las dos ramas condicionales — 11 min

Crear una promoción de datos aprobados y una cuarentena de datos rechazados, ambas con auditoría y escrituras repetibles por lote.

### Tarea 3.1. Preparar la ruta aprobada

Crear la tabla promocionada y un notebook con error inducido antes de escribir, para poder repararlo.

- {% include step_label.html %} En **Workspace > tu carpeta personal > workflow_6**, selecciona **Create > Notebook**, nómbralo `05_promote_for_dlt` y elige **Python**. Déjalo vacío hasta el siguiente paso.

  > **Salida esperada:** El notebook existe y se puede seleccionar desde el Job.
  {: .lab-note .output .compact}

- {% include step_label.html %} Abre `05_promote_for_dlt`; en su **celda 1 (Python)** pega completo el bloque de widgets y falla controlada. Ejecuta inicialmente con `simulate_publish_failure=false`, usando el cluster de edición. La ejecución `true` se hará después desde el Job.

  > **Nota:** La primera ejecución con simulate_publish_failure=true falla antes de publicar; crea una marca por lote en landing/lab7_repair. Repair run puede reutilizar los mismos parámetros y completar la tarea.
  {: .lab-note .info .compact}

  ```python
  from pyspark.sql import functions as F
  import re
  for key, default in {'batch_id':'wf_valid_001', 'simulate_publish_failure':'false'}.items():
      dbutils.widgets.text(key, default)
  batch_id = dbutils.widgets.get('batch_id').strip()
  simulate = dbutils.widgets.get('simulate_publish_failure').strip().lower()
  assert re.fullmatch(r'[A-Za-z0-9_-]+', batch_id), 'batch_id inválido'
  assert simulate in {'true', 'false'}, 'Bandera inválida'
  # La marca es exclusiva del lote y se escribe antes de provocar el error.
  marker_dir = '/Volumes/dbricks_de_int/batch1/landing/lab7_repair'
  marker = f'{marker_dir}/{batch_id}.failed_once'
  dbutils.fs.mkdirs(marker_dir)
  already_failed = any(f.name == f'{batch_id}.failed_once' for f in dbutils.fs.ls(marker_dir))
  if simulate == 'true' and not already_failed:
      dbutils.fs.put(marker, 'FALLA_CONTROLADA_PROMOCION', overwrite=False)
      raise RuntimeError('FALLA_CONTROLADA_PROMOCION: usa Repair run sin cambiar parámetros')
  print(f'VALIDACIÓN EXITOSA: lote={batch_id}; simulate_publish_failure={simulate}; marca_previa={already_failed}; promoción autorizada')
  ```

  > **Salida esperada:** Con `simulate_publish_failure=true` y sin marca previa, aparece `FALLA_CONTROLADA_PROMOCION` (error intencional). Al reparar: `lote=wf_repair_001; simulate_publish_failure=true; marca_previa=True; promoción autorizada`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `05_promote_for_dlt`, pulsa **+ Code** justo **debajo de la celda 1** y pega en la **celda 2 (Python)** la creación de `p6_workflow_promoted` y la publicación de pedidos válidos. Déjala guardada: se ejecutará en el Job tras la celda 1.

  ```python
  table='dbricks_de_int.batch1.p6_workflow_promoted'
  spark.sql('''CREATE TABLE IF NOT EXISTS dbricks_de_int.batch1.p6_workflow_promoted (
   batch_id STRING, order_id STRING, customer_id STRING, amount DECIMAL(18,2)
  ) USING DELTA''')
  approved=(spark.table('dbricks_de_int.batch1.p6_workflow_validated')
   .filter((F.col('batch_id')==batch_id)&(F.col('validation_reason')==''))
   .select('batch_id','order_id','customer_id','amount'))
  assert approved.count()>0,'No hay filas aprobadas'
  (approved.write.format('delta').mode('overwrite')
   .option('replaceWhere',f"batch_id = '{batch_id}'").saveAsTable(table))
  assert spark.table(table).filter(F.col('batch_id')==batch_id).count()==approved.count()
  print(f'VALIDACIÓN EXITOSA: lote={batch_id}; tabla=p6_workflow_promoted; pedidos_aprobados={approved.count()}; registros_persistidos={spark.table(table).filter(F.col("batch_id") == batch_id).count()}')
  ```

  > **Salida esperada:** `lote=wf_valid_001; tabla=p6_workflow_promoted; pedidos_aprobados=3; registros_persistidos=3` (para reparación, lote `wf_repair_001`).
  {: .lab-note .output .compact}

- {% include step_label.html %} En `05_promote_for_dlt`, añade una **celda 3 (Python)** inmediatamente debajo de la celda 2 y pega el bloque de auditoría `DLT_PROMOTION`. Guarda el notebook; no ejecutes esta celda aisladamente antes de preparar el lote.

  > **Nota:** La promoción prepara datos **Delta**; en esta práctica no se ejecuta un pipeline DLT/Lakeflow.
  {: .lab-note .info .compact}

  ```python
  audit='dbricks_de_int.batch1.p6_workflow_audit'
  spark.sql(f"DELETE FROM {audit} WHERE batch_id='{batch_id}' AND event_type='DLT_PROMOTION'")
  metrics=spark.table('dbricks_de_int.batch1.p6_workflow_promoted').filter(F.col('batch_id')==batch_id).count()
  row=[(batch_id,'DLT_PROMOTION','APPROVED',metrics,metrics,0,0,'Datos Delta preparados para consumo posterior')]
  (spark.createDataFrame(row,'batch_id string,event_type string,quality_status string,total_rows long,valid_rows long,invalid_rows long,duplicate_excess long,message string')
   .withColumn('event_timestamp',F.current_timestamp())
   .write.format('delta').mode('append').saveAsTable(audit))
  assert spark.table(audit).filter((F.col('batch_id')==batch_id)&(F.col('event_type')=='DLT_PROMOTION')).count()==1
  print(f'VALIDACIÓN EXITOSA: lote={batch_id}; evento=DLT_PROMOTION; eventos_registrados=1; pedidos_promocionados={metrics}')
  ```

  > **Salida esperada:** `evento=DLT_PROMOTION; eventos_registrados=1; pedidos_promocionados=3`, acompañado del identificador del lote.
  {: .lab-note .output .compact}

### Tarea 3.2. Preparar cuarentena y conectar rutas

Crear la tarea de rechazo y configurar True/False sin sustituir la auditoría original.

- {% include step_label.html %} En **Workspace > tu carpeta personal > workflow_6**, selecciona **Create > Notebook**, nómbralo `06_quarantine_invalid_rows` y fija **Python** como lenguaje.

  > **Salida esperada:** Existe el notebook de cuarentena.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `06_quarantine_invalid_rows`, pega en la **celda 1 (Python)** el bloque de widgets, lectura de `p6_workflow_validated` y escritura de `p6_workflow_quarantine`. Guarda la celda; el Job la ejecutará con `wf_invalid_001`.

  ```python
  from pyspark.sql import functions as F
  dbutils.widgets.text('batch_id','wf_invalid_001')
  batch_id=dbutils.widgets.get('batch_id').strip()
  table='dbricks_de_int.batch1.p6_workflow_quarantine'
  spark.sql('''CREATE TABLE IF NOT EXISTS dbricks_de_int.batch1.p6_workflow_quarantine (
   batch_id STRING, order_id STRING, customer_id STRING, amount_raw STRING,
   validation_reason STRING
  ) USING DELTA''')
  rejected=(spark.table('dbricks_de_int.batch1.p6_workflow_validated')
   .filter((F.col('batch_id')==batch_id)&(F.col('validation_reason')!=''))
   .select('batch_id','order_id','customer_id','amount_raw','validation_reason'))
  assert rejected.count()>0
  (rejected.write.format('delta').mode('overwrite')
   .option('replaceWhere',f"batch_id = '{batch_id}'").saveAsTable(table))
  assert spark.table(table).filter(F.col('batch_id')==batch_id).count()==rejected.count()
  print(f'VALIDACIÓN EXITOSA: lote={batch_id}; tabla=p6_workflow_quarantine; registros_rechazados={rejected.count()}; filas_guardadas={spark.table(table).filter(F.col("batch_id") == batch_id).count()}')
  ```

  > **Salida esperada:** `lote=wf_invalid_001; tabla=p6_workflow_quarantine; registros_rechazados=2; filas_guardadas=2`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `06_quarantine_invalid_rows`, agrega **+ Code** y pega en la **celda 2 (Python)** el bloque que registra `QUALITY_ALERT` en `p6_workflow_audit`. Guarda el notebook.

  ```python
  audit='dbricks_de_int.batch1.p6_workflow_audit'
  spark.sql(f"DELETE FROM {audit} WHERE batch_id='{batch_id}' AND event_type='QUALITY_ALERT'")
  total=spark.table('dbricks_de_int.batch1.p6_workflow_validated').filter(F.col('batch_id')==batch_id).count()
  invalid=spark.table('dbricks_de_int.batch1.p6_workflow_quarantine').filter(F.col('batch_id')==batch_id).count()
  row=[(batch_id,'QUALITY_ALERT','OBSERVED',total,total-invalid,invalid,0,'Registros rechazados por la compuerta')]
  (spark.createDataFrame(row,'batch_id string,event_type string,quality_status string,total_rows long,valid_rows long,invalid_rows long,duplicate_excess long,message string')
   .withColumn('event_timestamp',F.current_timestamp())
   .write.format('delta').mode('append').saveAsTable(audit))
  assert spark.table(audit).filter((F.col('batch_id')==batch_id)&(F.col('event_type')=='QUALITY_ALERT')).count()==1
  print(f'VALIDACIÓN EXITOSA: lote={batch_id}; evento=QUALITY_ALERT; eventos_registrados=1; total={total}; rechazados={invalid}; válidos={total-invalid}')
  ```

  > **Salida esperada:** `lote=wf_invalid_001; evento=QUALITY_ALERT; eventos_registrados=1; total=3; rechazados=2; válidos=1`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Jobs & Pipelines > job_orders_orchestration_b2 > Tasks > Add task**, crea una **Notebook task** con **Task name** `promote_for_dlt`; selecciona el notebook `workflow_6/05_promote_for_dlt`, la misma Job Compute y **Depends on** `evaluate_quality_gate` con **Outcome = True**. Configura **Retries = 0**.

  > **Salida esperada:** La rama True apunta al notebook de promoción con reintentos en cero.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Tasks > Add task**, crea otra **Notebook task**, **Task name** `quarantine_invalid_rows`, notebook `workflow_6/06_quarantine_invalid_rows`, misma Job Compute, **Depends on** `evaluate_quality_gate` con **Outcome = False** y **Retries = 0**. Comprueba ambas ramas en el DAG.

  > **Salida esperada:** La rama False apunta al notebook de cuarentena; ambas tareas reciben **batch_id**.
  {: .lab-note .output .compact}

{% capture r3 %}{{ results[2] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r3 %}

{% include support-prompt.html task="tarea3" %}

---

## 🧩 Tarea 4. Probar las ramas y provocar un fallo — 12 min

Ejecutar dos escenarios contrastantes y generar una falla después de validar para comprobar el comportamiento del **DAG**.

### Tarea 4.1. Ejecutar escenarios aprobado y observado

Mantener los mismos notebooks de ingesta y validación para comprobar que la condición usa datos procesados.

- {% include step_label.html %} En la cabecera de **Jobs & Pipelines > job_orders_orchestration_b2**, pulsa **Run now** (o **Run now with different parameters**). Para esta ejecución sustituye `batch_id=wf_valid_001`, `source_file=wf_valid_001.csv`, `quality_scenario=valid`, `simulate_publish_failure=false`; conserva `run_date`. Abre el Run resultante y comprueba la rama **True**.

  ```text
  # run_date=2026-10-11
  # simulate_publish_failure=false
  ```

  > **Salida esperada:** El Job finaliza; promoción **Succeeded** y cuarentena **Excluded**.
  {: .lab-note .output .compact}

- {% include step_label.html %} Cuando termine el Run válido, vuelve a `07_control_workflow`. Crea la **celda 3 (Python)** debajo de la celda 2, pega la validación siguiente y ejecútala. Confirma tres filas en `dbricks_de_int.batch1.p6_workflow_promoted`.

  ```python
  promoted = spark.sql("SELECT COUNT(*) AS n FROM dbricks_de_int.batch1.p6_workflow_promoted WHERE batch_id='wf_valid_001'").first()['n']
  assert promoted == 3, f'Esperados 3 pedidos; encontrados {promoted}'
  print(f'VALIDACIÓN EXITOSA: lote=wf_valid_001; rama=True; pedidos_promocionados={promoted}; esperado=3')
  ```

  > **Salida esperada:** `lote=wf_valid_001; rama=True; pedidos_promocionados=3; esperado=3`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Vuelve a **Jobs & Pipelines > job_orders_orchestration_b2 > Run now with different parameters**. Define `batch_id=wf_invalid_001`, `source_file=wf_invalid_001.csv`, `quality_scenario=invalid`, `simulate_publish_failure=false` y conserva `run_date`. En el Run revisa **evaluate_quality_gate=False** y `quarantine_invalid_rows=Succeeded`.

  ```text
  # run_date=2026-10-11
  # simulate_publish_failure=false
  ```

  > **Salida esperada:** La tarea de cuarentena termina **Succeeded** y promoción queda **Excluded**.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `07_control_workflow`, crea la **celda 4 (Python)** inmediatamente debajo de la celda 3. Pega el bloque para consultar `p6_workflow_quarantine` y `p6_workflow_audit` y ejecútalo; debe mostrar dos filas rechazadas y un `QUALITY_ALERT`.

  ```python
  q = spark.sql("SELECT order_id, validation_reason FROM dbricks_de_int.batch1.p6_workflow_quarantine WHERE batch_id='wf_invalid_001'")
  a = spark.sql("SELECT event_type, invalid_rows FROM dbricks_de_int.batch1.p6_workflow_audit WHERE batch_id='wf_invalid_001' AND event_type='QUALITY_ALERT'")
  assert q.count() == 2, 'Se esperaban dos rechazados'
  assert a.count() == 1 and a.first()['invalid_rows'] == 2
  display(q)
  display(a)
  print(f'VALIDACIÓN EXITOSA: lote=wf_invalid_001; rama=False; rechazados={q.count()}; eventos_QUALITY_ALERT={a.count()}')
  ```

  > **Salida esperada:** Dos filas de rechazo, un evento `QUALITY_ALERT` y el mensaje `lote=wf_invalid_001; rama=False; rechazados=2; eventos_QUALITY_ALERT=1`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `07_control_workflow`, crea la **celda 5 (SQL)** debajo de la celda 4: pulsa **+ Code**, cambia el selector de lenguaje a **SQL** y pega la consulta siguiente **sin `%sql`**. Ejecuta para comprobar ambos eventos `QUALITY_CHECK`.

  ```sql
  SELECT batch_id,quality_status,total_rows,invalid_rows
  FROM dbricks_de_int.batch1.p6_workflow_audit
  WHERE batch_id IN ('wf_valid_001','wf_invalid_001')
  AND event_type='QUALITY_CHECK' ORDER BY batch_id;
  ```

  > **Salida esperada:** Las dos filas **QUALITY_CHECK** existen en la tabla de auditoría.
  {: .lab-note .output .compact}

### Tarea 4.2. Inducir error después de completar validación

Provocar error únicamente en promoción para no repetir las tareas iniciales.

- {% include step_label.html %} En **Jobs & Pipelines > job_orders_orchestration_b2 > Run now with different parameters**, define `batch_id=wf_repair_001`, `source_file=wf_repair_001.csv`, `quality_scenario=valid` y `simulate_publish_failure=true`; conserva `run_date`. Abre el Run y espera el fallo intencional de `promote_for_dlt`.

  ```text
  # run_date=2026-10-11
  # simulate_publish_failure=true
  ```

  > **Salida esperada:** **validate_orders** y **evaluate_quality_gate** terminan **Succeeded**; **promote_for_dlt** falla.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la página del **Run fallido**, haz clic en el nodo `promote_for_dlt`, abre **Output** o **Logs** y localiza el texto `FALLA_CONTROLADA_PROMOCION`. Confirma que las tareas anteriores aparecieron como **Succeeded**.

  > **Salida esperada:** El registro muestra RuntimeError antes de cualquier inserción en promoción.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `07_control_workflow`, agrega la **celda 6 (Python)** debajo de la celda 5 SQL (cambia el selector a Python). Ejecuta esta consulta para confirmar que `wf_repair_001` todavía tiene cero filas publicadas.

  ```python
  assert spark.catalog.tableExists('dbricks_de_int.batch1.p6_workflow_promoted')
  count = spark.sql("SELECT COUNT(*) AS n FROM dbricks_de_int.batch1.p6_workflow_promoted WHERE batch_id='wf_repair_001'").first()['n']
  assert count == 0, f'No debería haber promoción antes de reparar: {count}'
  print(f'VALIDACIÓN EXITOSA: lote=wf_repair_001; etapa=antes_de_Repair_run; pedidos_promocionados={count}; esperado=0')
  ```

  > **Salida esperada:** `lote=wf_repair_001; etapa=antes_de_Repair_run; pedidos_promocionados=0; esperado=0`.
  {: .lab-note .output .compact}

{% capture r4 %}{{ results[3] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r4 %}

{% include support-prompt.html task="tarea4" %}

---

## ✅ Tarea 5. Ejecutar Repair run y validar recuperación — 8 min

Reprocesar únicamente la tarea descendiente fallida y comprobar que no se repitieron validación ni ingesta.

### Tarea 5.1. Reparar solo la tarea fallida

Reparar el Run existente sin cambiar parámetros ni reejecutar las tareas anteriores.

- {% include step_label.html %} En **Jobs & Pipelines > job_orders_orchestration_b2 > Runs**, abre específicamente el **Run fallido de `wf_repair_001`** (no pulses Run now). En el menú de acciones selecciona **Repair run** y examina las tareas propuestas.

  > **Salida esperada:** La interfaz identifica **promote_for_dlt** como tarea fallida y reparable.
  {: .lab-note .output .compact}

- {% include step_label.html %} En el panel **Repair run**, deja seleccionada solo `promote_for_dlt` y verifica que `batch_id=wf_repair_001` y `simulate_publish_failure=true` permanecen sin cambios; **no** marques `ingest_landing`, `transform_orders`, `validate_orders` ni la condición.

  > **Nota:** No necesitas editar parámetros del Run ni cambiar el notebook: la primera falla dejó una marca persistente y la reparación la detecta.
  {: .lab-note .info .compact}

  > **Salida esperada:** El **Repair run** conserva **batch_id**=wf_repair_001 y **quality_scenario**=valid.
  {: .lab-note .output .compact}

- {% include step_label.html %} En ese panel pulsa **Repair run** para iniciar la reparación. En el historial del mismo Run abre el nuevo intento de `promote_for_dlt` y confirma **Succeeded**; `validate_orders` no debe tener un nuevo intento.

  > **Salida esperada:** La tarea finaliza **Succeeded** y las predecesoras conservan su ejecución original.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `07_control_workflow`, crea la **celda 7 (Python)** debajo de la celda 6. Pega y ejecuta la consulta siguiente: la tabla `p6_workflow_promoted` debe contener tres pedidos para `wf_repair_001`.

  ```python
  count = spark.sql("SELECT COUNT(*) AS n FROM dbricks_de_int.batch1.p6_workflow_promoted WHERE batch_id='wf_repair_001'").first()['n']
  assert count == 3, f'Esperados 3 pedidos después de Repair run; encontrados {count}'
  print(f'VALIDACIÓN EXITOSA: lote=wf_repair_001; etapa=después_de_Repair_run; pedidos_promocionados={count}; esperado=3')
  ```

  > **Salida esperada:** `lote=wf_repair_001; etapa=después_de_Repair_run; pedidos_promocionados=3; esperado=3`.
  {: .lab-note .output .compact}

### Tarea 5.2. Consolidar evidencia y preservar recursos

Validar la auditoría final de los tres escenarios y conservar las tablas para prácticas futuras.

- {% include step_label.html %} En `07_control_workflow`, crea la **celda 8 (SQL)** debajo de la celda 7 (selector **SQL**) y pega la consulta de auditoría siguiente sin `%sql`. Confirma `DLT_PROMOTION` para los lotes valid/repair y `QUALITY_ALERT` para invalid.

  ```sql
  SELECT batch_id,event_type,quality_status,total_rows,valid_rows,invalid_rows
  FROM dbricks_de_int.batch1.p6_workflow_audit
  WHERE batch_id IN ('wf_valid_001','wf_invalid_001','wf_repair_001')
  ORDER BY batch_id,event_type;
  ```

  > **Salida esperada:** La celda SQL muestra `DLT_PROMOTION` para `wf_valid_001` y `wf_repair_001`, y `QUALITY_ALERT` para `wf_invalid_001`; también conserva los tres eventos `QUALITY_CHECK`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Workspace > tu carpeta personal > workflow_6**, comprueba que están guardados los seis notebooks. En **Jobs & Pipelines > job_orders_orchestration_b2 > Runs**, conserva como evidencia los Run de `wf_valid_001`, `wf_invalid_001` y el Run reparado de `wf_repair_001`. No elimines las tablas ni el Job.

  > **Salida esperada:** La evidencia contiene rama True, rama False, error controlado y reparación selectiva.
  {: .lab-note .output .compact}

{% capture r5 %}{{ results[4] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r5 %}

{% include support-prompt.html task="tarea5" %}

