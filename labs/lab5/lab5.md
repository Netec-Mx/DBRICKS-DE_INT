---
layout: lab
title: 'Práctica 5: Auditoría con Time Travel y restauración de versión'
permalink: /lab5/lab5/
images_base: /labs/lab5/img
duration: 40 minutos
objective:
- Consultar historial Delta y seleccionar una versión segura real.
- Comparar VERSION AS OF y TIMESTAMP AS OF sobre una copia aislada.
- Provocar un UPDATE controlado y restaurar con RESTORE TABLE.
- Registrar y validar evidencia de recuperación en Delta.
prerequisites:
- 'Práctica 4 terminada: p3_silver_orders contiene 6 claves únicas.'
- Mismo cluster All-Purpose de las prácticas anteriores, conectado y compatible con Unity Catalog.
- Catálogo dbricks_de_int, schema batch1 y permisos USE CATALOG, USE SCHEMA, SELECT, CREATE TABLE y MODIFY.
- Conocimientos básicos de versiones Delta, SQL UPDATE y agregaciones.
introduction:
- Simularás un incidente en una copia Delta independiente de la tabla Silver para practicar Time Travel, RESTORE
  TABLE y auditoría. La tabla que contiene los resultados incrementales de las prácticas anteriores no se modificará.
slug: lab5
lab_number: 5
final_result: La copia Delta se restaura a su versión segura y recupera el total anterior; el historial conserva
  UPDATE/RESTORE y una tabla independiente registra eventos de auditoría.
notes:
- 'Nunca ejecutar UPDATE o RESTORE sobre p3_silver_orders: solo usar p5_silver_restore_lab.'
- La copia p5_ se recrea para cada ejecución y se genera un recovery_id único para que la auditoría no mezcle sesiones.
- No ejecutar VACUUM para conservar archivos históricos necesarios para Time Travel.
- La consulta TIMESTAMP AS OF depende de que el historial y sus archivos sigan disponibles; los resultados se validan en el workspace.
references:
- text: Delta Lake Time Travel
  url: https://docs.databricks.com/aws/en/delta/history#query-an-older-snapshot-of-a-table-time-travel
- text: Delta Lake table history
  url: https://docs.databricks.com/aws/en/delta/history
- text: Unity Catalog managed tables
  url: https://docs.databricks.com/aws/en/tables/managed
- text: RESTORE TABLE
  url: https://docs.databricks.com/aws/en/sql/language-manual/delta-restore
prev: /lab4/lab4/
next: /lab6/lab6/
---

<!-- 5 tareas; 11 subtareas; 31 pasos; 40 minutos. -->

---

## 🔎 Tarea 1. Inspeccionar Silver y preparar recuperación aislada — 7 min

Comprobar el resultado incremental de Silver y crear una copia Delta de recuperación exclusiva para el laboratorio, sin exponer datos de otros ejercicios a modificaciones.

### Tarea 1.1. Abrir notebook y comprobar la fuente

Identificar la tabla fuente y confirmar las seis claves disponibles después del MERGE.

- {% include step_label.html %} En **Workspace > Home > batch_1**, selecciona **Create > Notebook**; nómbralo `05_time_travel_restore` y elige **Python**.

  > **Salida esperada:** Se crea el notebook.
  {: .lab-note .output .compact}

- {% include step_label.html %} Conecta el mismo cluster de las prácticas anteriores; configura el catálogo, la sesión UTC y un identificador único.

  ```python
  from pyspark.sql import functions as F
  from datetime import datetime, timezone
  C='dbricks_de_int'; S='batch1'; ORIGINAL=f'{C}.{S}.p3_silver_orders'
  R=f'{C}.{S}.p5_silver_restore_lab'; AUD=f'{C}.{S}.p5_delta_recovery_audit'
  from uuid import uuid4
  RECOVERY_ID=f'lab5_{uuid4().hex[:12]}'
  spark.sql('SET TIME ZONE \'UTC\'')
  print('Fuente:',ORIGINAL,'| Copia de ensayo:',R,'| Sesión:',RECOVERY_ID)
  ```

  > **Salida esperada:** Aparecen dbricks_de_int.batch1.p3_silver_orders, la copia p5_ y un ID único.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que la tabla Silver fuente contiene seis pedidos únicos.

  ```python
  assert spark.catalog.tableExists(ORIGINAL), 'Falta Silver: completa la Práctica 3 y 4'
  original_df=spark.table(ORIGINAL)
  assert original_df.count()==6, 'Se requieren 6 pedidos al finalizar la Práctica 4'
  assert original_df.select('order_id').distinct().count()==6
  assert {'O1001','O1002','O1004','O1005'}.issubset(
      set(r.order_id for r in original_df.select('order_id').collect()))
  source_snapshot=sorted((r.order_id, str(r.amount), r.status) for r in original_df.collect())
  print('VALIDACIÓN EXITOSA: Fuente Silver: 6 claves únicas; estado inicial capturado')
  ```

  > **Salida esperada:** La tabla fuente contiene seis claves únicas. Se muestra `VALIDACIÓN EXITOSA: Fuente Silver: 6 claves únicas; estado inicial capturado`.
  {: .lab-note .output .compact}

### Tarea 1.2. Crear copia Delta y auditoría

Crear una copia de los datos actuales en otra tabla Delta para ejecutar UPDATE y RESTORE sin riesgos externos.

- {% include step_label.html %} En una celda Python nueva, crea únicamente la tabla de ensayo p5_ a partir de Silver; ejecuta con **Shift+Enter**.

  > **Importante:** CREATE OR REPLACE se utiliza solo con la copia p5_, nunca con la fuente p3_.
  {: .lab-note .important .compact}

  ```python
  assert R!=ORIGINAL and R.startswith(f'{C}.{S}.p5_')
  spark.sql(f'CREATE OR REPLACE TABLE {R} USING DELTA AS SELECT * FROM {ORIGINAL}')
  assert spark.table(R).count()==6
  assert spark.sql(f'DESCRIBE DETAIL {R}').first()['format']=='delta'
  print('VALIDACIÓN EXITOSA: Copia Delta aislada: 6 registros; fuente original protegida')
  ```

  > **Salida esperada:** La tabla p5_silver_restore_lab contiene seis filas. Se muestra `VALIDACIÓN EXITOSA: Copia Delta aislada: 6 registros; fuente original protegida`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Crea la tabla Delta de auditoría de recuperación si aún no existe.

  ```python
  spark.sql(f'''CREATE TABLE IF NOT EXISTS {AUD} (
   recovery_id STRING, event_type STRING, occurred_at TIMESTAMP, safe_version BIGINT,
   incident_key STRING, row_count BIGINT, total_amount DECIMAL(20,2), notes STRING) USING DELTA''')
  ```

  > **Salida esperada:** Se crea p5_delta_recovery_audit.
  {: .lab-note .output .compact}

- {% include step_label.html %} Consulta el historial de la copia Delta y comprueba que tiene al menos una versión.

  ```python
  display(spark.sql(f'DESCRIBE HISTORY {R} LIMIT 5'))
  ```

  > **Salida esperada:** El historial de la copia contiene una operación inicial WRITE o CREATE TABLE AS SELECT.
  {: .lab-note .output .compact}

{% assign results = site.data.task-results[page.slug].results %}

{% capture r1 %}{{ results[0] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r1 %}

{% include support-prompt.html task="tarea1" %}

---

## 🟤 Tarea 2. Identificar y consultar una versión segura — 8 min

Guardar una línea base histórica con número de versión real, contrastar VERSION AS OF con TIMESTAMP AS OF y preparar la auditoría antes de modificar datos.

### Tarea 2.1. Seleccionar la versión segura

Registrar las métricas de la versión más reciente existente antes del incidente.

- {% include step_label.html %} Captura automáticamente la versión segura y sus métricas desde DESCRIBE HISTORY.

  ```python
  history=spark.sql(f'DESCRIBE HISTORY {R}')
  safe_version=int(history.orderBy(F.col('version').desc()).first()['version'])
  base=spark.sql(f'SELECT COUNT(*) AS n, CAST(SUM(amount) AS DECIMAL(20,2)) AS total FROM {R} VERSION AS OF {safe_version}').first()
  print('Versión:',safe_version,'| Filas:',base['n'],'| Ventas:',base['total'])
  ```

  > **Salida esperada:** safe_version y las métricas se almacenan en variables.
  {: .lab-note .output .compact}

- {% include step_label.html %} Verifica que la consulta VERSION AS OF devuelve las seis filas originales.

  ```python
  assert base['n']==6
  display(spark.sql(f'SELECT order_id,amount,status FROM {R} VERSION AS OF {safe_version} ORDER BY order_id'))
  print('VALIDACIÓN EXITOSA: Time Travel VERSION AS OF: 6 pedidos recuperados')
  ```

  > **Salida esperada:** Se muestran seis pedidos históricos. Se muestra `VALIDACIÓN EXITOSA: Time Travel VERSION AS OF: 6 pedidos recuperados`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Define la función de auditoría y guarda el evento BASELINE_CAPTURED.

  ```python
  from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,DecimalType
  schema=StructType([StructField('recovery_id',StringType()),StructField('event_type',StringType()),
   StructField('occurred_at',TimestampType()),StructField('safe_version',LongType()),
   StructField('incident_key',StringType()),StructField('row_count',LongType()),
   StructField('total_amount',DecimalType(20,2)),StructField('notes',StringType())])
  def log_event(event,notes):
      s=spark.sql(f'SELECT COUNT(*) AS n, CAST(SUM(amount) AS DECIMAL(20,2)) AS total FROM {R}').first()
      row=[(RECOVERY_ID,event,datetime.now(timezone.utc).replace(tzinfo=None),safe_version,
            globals().get('incident_id',''),int(s['n']),s['total'],notes)]
      spark.createDataFrame(row,schema).write.format('delta').mode('append').saveAsTable(AUD)
  
  log_event('BASELINE_CAPTURED','Estado anterior al incidente')
  ```

  > **Salida esperada:** La auditoría incluye BASELINE_CAPTURED.
  {: .lab-note .output .compact}

### Tarea 2.2. Comparar VERSION y TIMESTAMP AS OF

Verificar que la marca temporal de una versión disponible permite consultar su contenido histórico.

- {% include step_label.html %} Obtén la marca temporal del historial correspondiente a safe_version.

  ```python
  hist=spark.sql(f'DESCRIBE HISTORY {R}')
  safe_ts=hist.filter(F.col('version')==safe_version).first()['timestamp']
  assert safe_ts is not None
  safe_ts_sql=safe_ts.strftime('%Y-%m-%d %H:%M:%S.%f')
  print('Timestamp histórico UTC:',safe_ts_sql)
  print('VALIDACIÓN EXITOSA: Condiciones comprobadas sin errores')
  ```

  > **Salida esperada:** Se muestra la marca temporal de la versión segura. Se muestra `VALIDACIÓN EXITOSA: Condiciones comprobadas sin errores`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Consulta la copia histórica utilizando TIMESTAMP AS OF con el instante registrado.

  > **Importante:** No redondees el instante al segundo: hacerlo puede seleccionar una versión anterior.
  {: .lab-note .important .compact}

  ```python
  display(spark.sql(f"SELECT order_id,amount FROM {R} TIMESTAMP AS OF '{safe_ts_sql}' ORDER BY order_id"))
  ```

  > **Salida esperada:** Se muestran los mismos pedidos de la versión segura.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que los conteos históricos por versión y por timestamp son iguales.

  ```python
  via_ts=spark.sql(f"SELECT COUNT(*) AS n FROM {R} TIMESTAMP AS OF '{safe_ts_sql}'").first()['n']
  assert via_ts==base['n']
  via_ts_total=spark.sql(f"SELECT CAST(SUM(amount) AS DECIMAL(20,2)) AS total FROM {R} TIMESTAMP AS OF '{safe_ts_sql}'").first()['total']
  assert via_ts_total==base['total']
  print('VALIDACIÓN EXITOSA: Time Travel TIMESTAMP AS OF: 6 pedidos y total coincidente')
  ```

  > **Salida esperada:** Ambas consultas históricas devuelven seis filas. Se muestra `VALIDACIÓN EXITOSA: Time Travel TIMESTAMP AS OF: 6 pedidos y total coincidente`.
  {: .lab-note .output .compact}

{% capture r2 %}{{ results[1] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r2 %}

{% include support-prompt.html task="tarea2" %}

---

## 🧹 Tarea 3. Simular incidente y restaurar la versión segura — 12 min

Modificar deliberadamente un único pedido en la tabla aislada, medir una diferencia de 9999.99 y recuperar exactamente el contenido anterior mediante RESTORE TABLE.

### Tarea 3.1. Seleccionar y modificar una fila

Producir un incidente controlado sobre una clave única sin afectar el origen del curso.

- {% include step_label.html %} Selecciona un pedido único, registra su importe y realiza el UPDATE de simulación.

  > **Advertencia:** Nunca ejecutes UPDATE en la tabla p3_silver_orders de las prácticas anteriores.
  {: .lab-note .warning .compact}

  ```python
  pick=spark.table(R).filter(F.col('amount').isNotNull()).orderBy('order_id').select('order_id','amount').first()
  assert pick is not None, 'No hay pedido apto para simular el incidente'
  incident_id=pick['order_id']; original_amount=pick['amount']
  assert spark.table(R).filter(F.col('order_id')==incident_id).count()==1
  spark.sql(f"UPDATE {R} SET amount=CAST(amount + 9999.99 AS DECIMAL(14,2)) WHERE order_id='{incident_id}'")
  print('Pedido afectado:',incident_id,'Importe anterior:',original_amount)
  print('VALIDACIÓN EXITOSA: Incidente controlado: pedido único seleccionado; UPDATE ejecutado en la copia')
  ```

  > **Salida esperada:** Se cambia el importe de un único pedido en la copia p5_. Se muestra `VALIDACIÓN EXITOSA: Incidente controlado: pedido único seleccionado; UPDATE ejecutado en la copia`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Consulta la fila modificada y comprueba que el cambio quedó persistido.

  ```python
  display(spark.table(R).filter(F.col('order_id')==incident_id).select('order_id','amount'))
  ```

  > **Salida esperada:** El importe aumentó en 9999.99.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que el historial generó una operación UPDATE.

  ```python
  assert 'UPDATE' in [r['operation'] for r in spark.sql(f'DESCRIBE HISTORY {R}').collect()]
  print('VALIDACIÓN EXITOSA: Historial: operación UPDATE encontrada')
  ```

  > **Salida esperada:** Se encuentra UPDATE en el historial. Se muestra `VALIDACIÓN EXITOSA: Historial: operación UPDATE encontrada`.
  {: .lab-note .output .compact}

### Tarea 3.2. Cuantificar diferencias del incidente

Comparar las métricas actuales con las históricas antes de ejecutar RESTORE.

- {% include step_label.html %} Calcula las diferencias de conteo e importe contra la versión segura.

  ```python
  old=spark.sql(f'SELECT COUNT(*) AS n, CAST(SUM(amount) AS DECIMAL(20,2)) AS total FROM {R} VERSION AS OF {safe_version}').first()
  now=spark.sql(f'SELECT COUNT(*) AS n, CAST(SUM(amount) AS DECIMAL(20,2)) AS total FROM {R}').first()
  print('Diferencia de filas:',now['n']-old['n'],'| Diferencia de importe:',now['total']-old['total'])
  ```

  > **Salida esperada:** La diferencia de filas es 0 y la diferencia de importes es 9999.99.
  {: .lab-note .output .compact}

- {% include step_label.html %} Verifica automáticamente que un solo registro fue modificado.

  ```python
  from decimal import Decimal
  assert now['n']==old['n']
  assert now['total']-old['total']==Decimal('9999.99')
  print('VALIDACIÓN EXITOSA: Incidente: 0 filas de diferencia; importe alterado en 9999.99')
  ```

  > **Salida esperada:** Se validan diferencia 0 filas y 9999.99 de importe. Se muestra `VALIDACIÓN EXITOSA: Incidente: 0 filas de diferencia; importe alterado en 9999.99`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Registra la evidencia INCIDENT_DETECTED antes de restaurar.

  ```python
  log_event('INCIDENT_DETECTED','Importe alterado en copia aislada')
  ```

  > **Salida esperada:** La tabla de auditoría contiene INCIDENT_DETECTED.
  {: .lab-note .output .compact}

### Tarea 3.3. Ejecutar recuperación Delta

Restaurar usando una versión verificada y comprobar el historial que queda después del RESTORE.

- {% include step_label.html %} Ejecuta RESTORE TABLE sobre la copia p5_ a la versión segura.

  ```python
  spark.sql(f'RESTORE TABLE {R} TO VERSION AS OF {safe_version}').show(truncate=False)
  ```

  > **Salida esperada:** La operación RESTORE finaliza sin errores.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que el importe del pedido afectado volvió a su valor original.

  ```python
  assert spark.table(R).filter(F.col('order_id')==incident_id).first()['amount']==original_amount
  print('VALIDACIÓN EXITOSA: RESTORE: el importe original del pedido fue recuperado')
  ```

  > **Salida esperada:** El pedido recupera su importe anterior. Se muestra `VALIDACIÓN EXITOSA: RESTORE: el importe original del pedido fue recuperado`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que el historial contiene RESTORE posterior a UPDATE.

  ```python
  ops=[r['operation'] for r in spark.sql(f'DESCRIBE HISTORY {R}').orderBy(F.col('version').desc()).collect()]
  assert ops[0]=='RESTORE' and 'UPDATE' in ops
  print('VALIDACIÓN EXITOSA: Historial: RESTORE es posterior a UPDATE')
  ```

  > **Salida esperada:** La operación más reciente es RESTORE y UPDATE continúa en el historial. Se muestra `VALIDACIÓN EXITOSA: Historial: RESTORE es posterior a UPDATE`.
  {: .lab-note .output .compact}

{% capture r3 %}{{ results[2] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r3 %}

{% include support-prompt.html task="tarea3" %}

---

## 🔁 Tarea 4. Auditar y verificar la recuperación — 8 min

Reconciliar la copia restaurada contra la versión segura, registrar el evento RESTORE_COMPLETED y comprobar que la tabla fuente no fue afectada.

### Tarea 4.1. Verificar métricas de recuperación

Comprobar que los importes y conteos son idénticos antes del incidente y después de RESTORE.

- {% include step_label.html %} Compara el estado restaurado con el estado histórico original.

  ```python
  old=spark.sql(f'SELECT COUNT(*) AS n, CAST(SUM(amount) AS DECIMAL(20,2)) AS total FROM {R} VERSION AS OF {safe_version}').first()
  now=spark.sql(f'SELECT COUNT(*) AS n, CAST(SUM(amount) AS DECIMAL(20,2)) AS total FROM {R}').first()
  print('Diferencia de filas:',now['n']-old['n'],'| Diferencia de importe:',now['total']-old['total'])
  ```

  > **Salida esperada:** Las diferencias de filas e importe son 0 y 0.00.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba automáticamente que las métricas históricas coinciden.

  ```python
  assert now['n']==old['n'] and now['total']==old['total']
  print('VALIDACIÓN EXITOSA: Recuperación: 0 filas de diferencia; 0.00 de diferencia en importe')
  ```

  > **Salida esperada:** La tabla restaurada coincide con la línea base. Se muestra `VALIDACIÓN EXITOSA: Recuperación: 0 filas de diferencia; 0.00 de diferencia en importe`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Verifica que la tabla fuente original conserva sus seis registros.

  ```python
  assert spark.table(ORIGINAL).count()==6
  source_after=sorted((r.order_id, str(r.amount), r.status) for r in spark.table(ORIGINAL).collect())
  assert source_after==source_snapshot, 'La fuente Silver cambió durante el ejercicio'
  print('VALIDACIÓN EXITOSA: Fuente Silver original: 6 pedidos sin alteraciones')
  ```

  > **Salida esperada:** Los datos de la Práctica 4 siguen intactos. Se muestra `VALIDACIÓN EXITOSA: Fuente Silver original: 6 pedidos sin alteraciones`.
  {: .lab-note .output .compact}

### Tarea 4.2. Registrar la recuperación y revisar eventos

Guardar evidencia temporal y validar la secuencia de auditoría de la práctica.

- {% include step_label.html %} Registra el evento RESTORE_COMPLETED en la auditoría Delta.

  ```python
  log_event('RESTORE_COMPLETED','Recuperación verificada con métricas históricas')
  ```

  > **Salida esperada:** Se escribe RESTORE_COMPLETED.
  {: .lab-note .output .compact}

- {% include step_label.html %} Consulta cronológicamente los tres eventos de auditoría.

  ```python
  display(spark.table(AUD).filter(F.col('recovery_id')==RECOVERY_ID).orderBy('occurred_at'))
  ```

  > **Salida esperada:** Aparecen BASELINE_CAPTURED, INCIDENT_DETECTED y RESTORE_COMPLETED.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que existe exactamente un evento BASELINE_CAPTURED y uno RESTORE_COMPLETED.

  ```python
  events=[r['event_type'] for r in spark.table(AUD).filter(F.col('recovery_id')==RECOVERY_ID).collect()]
  assert events.count('BASELINE_CAPTURED')==1
  assert events.count('INCIDENT_DETECTED')==1
  assert events.count('RESTORE_COMPLETED')==1
  print('VALIDACIÓN EXITOSA: Auditoría: los 3 eventos aparecen exactamente una vez')
  ```

  > **Salida esperada:** Los dos eventos obligatorios aparecen exactamente una vez. Se muestra `VALIDACIÓN EXITOSA: Auditoría: los 3 eventos aparecen exactamente una vez`.
  {: .lab-note .output .compact}

{% capture r4 %}{{ results[3] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r4 %}

{% include support-prompt.html task="tarea4" %}

---

## ✅ Tarea 5. Probar límites y conservar las evidencias — 5 min

Demostrar el manejo de versiones inexistentes y dejar intactas tanto la fuente del curso como la copia restaurada y la auditoría.

### Tarea 5.1. Validar un número de versión inexistente

Confirmar que una consulta histórica inválida devuelve error sin provocar modificaciones.

- {% include step_label.html %} Consulta intencionalmente una versión futura inexistente y captura el error.

  ```python
  future=int(spark.sql(f'DESCRIBE HISTORY {R} LIMIT 1').first()['version'])+100000
  failed=False
  try:
      spark.sql(f'SELECT COUNT(*) FROM {R} VERSION AS OF {future}').collect()
  except Exception as err:
      failed=True; print('Error esperado:',str(err)[:240])
  assert failed
  print('VALIDACIÓN EXITOSA: Versión inexistente: error esperado capturado sin cambios')
  ```

  > **Salida esperada:** Se captura un error por versión inexistente; no se modifica la tabla. Se muestra `VALIDACIÓN EXITOSA: Versión inexistente: error esperado capturado sin cambios`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Confirma que los importes siguen coincidiendo después de la prueba de error.

  ```python
  current=spark.sql(f'SELECT COUNT(*) AS n, CAST(SUM(amount) AS DECIMAL(20,2)) AS total FROM {R}').first()
  historical=spark.sql(f'SELECT COUNT(*) AS n, CAST(SUM(amount) AS DECIMAL(20,2)) AS total FROM {R} VERSION AS OF {safe_version}').first()
  assert current['n']==historical['n']
  assert current['total']==historical['total']
  print('Copia intacta tras consulta inválida:',current['n'],current['total'])
  print('VALIDACIÓN EXITOSA: Consulta inválida: la copia mantiene 6 pedidos y el importe restaurado')
  ```

  > **Salida esperada:** La consulta de versión inexistente no alteró las seis filas ni el importe restaurado. Se muestra `VALIDACIÓN EXITOSA: Consulta inválida: la copia mantiene 6 pedidos y el importe restaurado`.
  {: .lab-note .output .compact}

### Tarea 5.2. Preservar resultados y finalizar

Conservar historial, auditoría y copia restaurada sin ejecutar operaciones destructivas.

- {% include step_label.html %} Guarda la evidencia del historial con UPDATE y RESTORE en el notebook.

  ```python
  display(spark.sql(f'DESCRIBE HISTORY {R} LIMIT 6').select('version','timestamp','operation'))
  ```

  > **Salida esperada:** El historial muestra UPDATE y RESTORE.
  {: .lab-note .output .compact}

- {% include step_label.html %} Guarda el notebook y conserva la copia, el historial y la tabla de auditoría.

  > **Importante:** No ejecutes VACUUM ni DROP sobre la copia o sobre la fuente; se necesita el historial.
  {: .lab-note .important .compact}

  > **Salida esperada:** Los datos quedan disponibles para revisión del instructor.
  {: .lab-note .output .compact}

{% capture r5 %}{{ results[4] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r5 %}

{% include support-prompt.html task="tarea5" %}

