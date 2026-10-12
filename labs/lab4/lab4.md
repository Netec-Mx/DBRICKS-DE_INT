---
layout: lab
title: 'Práctica 4: Implementación de MERGE incremental optimizado'
permalink: /lab4/lab4/
images_base: /labs/lab4/img
duration: 60 minutos
objective:
- Ingerir 6 filas incrementales y aplicar evolución de esquema en Bronze.
- Deduplicar las entradas por order_id priorizando source_updated_at y secuencia.
- Actualizar 2 pedidos e insertar 2 mediante MERGE en Silver.
- Reconstruir Gold por fecha/canal y ejecutar OPTIMIZE ZORDER.
prerequisites:
- Práctica 3 corregida completada; deben existir las tablas p3_ y las claves semilla O1001 y O1002.
- Databricks Runtime 15.4 LTS compatible con Unity Catalog, clúster iniciado.
- Privilegios USE CATALOG, USE SCHEMA, SELECT y MODIFY; permisos READ VOLUME y WRITE VOLUME sobre landing. El esquema y volumen ya se prepararon en la Práctica 3.
introduction:
- Aplicarás un MERGE incremental sobre tablas Delta aisladas de las prácticas anteriores, usando un archivo con
  seis filas, duplicados, cambios de estado y canales. El staging retiene cuatro claves únicas y permite dos actualizaciones
  y dos inserciones verificables.
slug: lab4
lab_number: 4
final_result: Bronze contiene 16 filas (6 de batch_003), staging 4 claves, Silver 6 pedidos (2 actualizados y 2 insertados),
  Gold 460.75 en ventas no canceladas y auditoría Delta con MERGE y OPTIMIZE.
notes:
- Este laboratorio usa las tablas p3_ y depende de la semilla O1001/O1002 creada en la Práctica 3.
- La evolución de esquema se aplica solo a Bronze; Silver ya contiene las columnas de auditoría y channel.
- ZORDER se ejecuta de forma demostrativa en una tabla Delta sin liquid clustering; con pocos registros no se esperan mejoras medibles.
- No ejecutes otra vez la Práctica 3 después de completar esta: restauraría las tablas p3_ a su estado inicial y perderías el avance de MERGE.
references:
- text: Delta Lake MERGE
  url: https://docs.databricks.com/aws/en/delta/merge
- text: Delta Lake table history
  url: https://docs.databricks.com/aws/en/delta/history
- text: Unity Catalog managed tables
  url: https://docs.databricks.com/aws/en/tables/managed
- text: Databricks OPTIMIZE
  url: https://docs.databricks.com/aws/en/sql/language-manual/delta-optimize
prev: /lab3/lab3/
next: /lab5/lab5/
---

<!-- 5 tareas; 12 subtareas; 46 pasos; 60 minutos. -->

---

## 🔎 Tarea 1. Preparar los datos base del MERGE — 8 min

Verificar que Silver contiene los dos pedidos semilla y que los contratos de Bronze, Silver y auditoría son compatibles con el proceso incremental.

### Tarea 1.1. Abrir notebook y verificar objetos

Confirmar que la ejecución anterior dejó los datos necesarios.

- {% include step_label.html %} Crea el notebook Python **04_merge_incremental_optimizado** en **batch_1**.

  > **Salida esperada:** El notebook se crea en Workspace.
  {: .lab-note .output .compact}

- {% include step_label.html %} Conecta el clúster compatible con Unity Catalog que utiliza el curso.

  > **Salida esperada:** La conexión del notebook figura activa.
  {: .lab-note .output .compact}

- {% include step_label.html %} Ejecuta la configuración de nombres de tablas y ruta de entrada.

  ```python
  from pyspark.sql import functions as F
  from pyspark.sql.window import Window
  from datetime import datetime, timezone
  from decimal import Decimal
  from pyspark.sql import types as T
  
  C = 'dbricks_de_int'
  S = 'batch1'
  PREFIX = f'{C}.{S}.p3_'
  B = PREFIX + 'bronze_orders'
  V = PREFIX + 'silver_orders'
  G = PREFIX + 'gold_daily_sales'
  Q = PREFIX + 'quarantine_orders'
  A = PREFIX + 'batch_ingestion_audit'
  LANDING = f'/Volumes/{C}/{S}/landing'
  LAB_DIR = f'{LANDING}/lab4'
  P = f'{LAB_DIR}/orders_batch_003_p4.csv'
  BATCH = 'batch_003'
  spark.sql(f'USE CATALOG `{C}`')
  spark.sql(f'USE SCHEMA `{S}`')
  print('Catálogo y esquema:', C, S)
  ```

  > **Salida esperada:** Se definen B, V, G, A y P.
  {: .lab-note .output .compact}

- {% include step_label.html %} Verifica que las tablas del laboratorio batch existen antes de continuar.

  ```python
  required_tables = [B, V, G, Q, A]

  missing = [
      name for name in required_tables
      if not spark.catalog.tableExists(name)
  ]

  assert not missing, f'Faltan tablas de la Práctica 3: {missing}'

  assert spark.sql(
      f'DESCRIBE DETAIL {V}'
  ).first()['format'].lower() == 'delta'

  # Validar directamente el Volume con su ruta completa.
  volumes = spark.sql(
      f"SHOW VOLUMES IN `{C}`.`{S}`"
  )

  assert volumes.filter(
      F.col('volume_name') == 'landing'
  ).count() == 1, 'Falta el Volume landing'

  # Confirmar acceso a la ruta completa.
  display(dbutils.fs.ls(LANDING))

  # Crear la carpeta exclusiva de la Práctica 4.
  dbutils.fs.mkdirs(LAB_DIR)

  print('VALIDACIÓN EXITOSA: Las cinco tablas están disponibles')
  print('VALIDACIÓN EXITOSA: Silver utiliza formato Delta')
  print('VALIDACIÓN EXITOSA: Volume landing accesible')
  print('VALIDACIÓN EXITOSA: Carpeta preparada:', LAB_DIR)
  ```

  > **Salida esperada:** Las cuatro tablas están disponibles. Mensaje: `VALIDACIÓN EXITOSA: Dependencias: cinco tablas Delta disponibles y Volume landing accesible`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Consulta los pedidos O1001 y O1002 que el MERGE actualizará.

  ```python
  seed = {r['order_id']: r for r in spark.table(V)
          .filter(F.col('order_id').isin('O1001', 'O1002'))
          .select('order_id', 'amount', 'status', 'batch_id').collect()}
  assert set(seed) == {'O1001', 'O1002'}, 'Faltan las semillas de Práctica 3'
  assert seed['O1001']['amount'] == Decimal('100.00')
  assert seed['O1002']['amount'] == Decimal('70.00')
  assert all(seed[k]['batch_id'] == 'seed_p3' for k in seed)
  display(spark.table(V).filter(F.col('order_id').isin('O1001', 'O1002')))
  print("VALIDACIÓN EXITOSA: Semillas O1001=100.00 y O1002=70.00 verificadas")
  ```

  > **Salida esperada:** Se observan O1001=100.00 y O1002=70.00. Mensaje: `VALIDACIÓN EXITOSA: Semillas O1001=100.00 y O1002=70.00 verificadas`.
  {: .lab-note .output .compact}

### Tarea 1.2. Revisar esquema y estado del lote

Registrar claves actuales y comprobar que el lote batch_003 no está previamente insertado.

- {% include step_label.html %} Comprueba que Silver tiene las columnas de negocio y auditoría exigidas.

  ```python
  required = {'order_id', 'customer_id', 'order_date', 'quantity', 'unit_price',
              'amount', 'status', 'channel', 'batch_id', 'source_file', 'ingested_at',
              'source_updated_at', 'created_at', 'updated_at'}
  missing_cols = required - set(spark.table(V).columns)
  assert not missing_cols, f'Columnas Silver faltantes: {missing_cols}'
  assert spark.table(B).filter(F.col('batch_id') == 'batch_002').count() == 10
  assert spark.table(V).count() == 4
  print('Silver inicial: 4 pedidos; Bronze batch_002: 10 filas')
  print("VALIDACIÓN EXITOSA: Silver inicial: 4 pedidos, contrato completo; Bronze anterior: 10 filas")
  ```

  > **Salida esperada:** La validación de columnas finaliza sin errores. Mensaje: `VALIDACIÓN EXITOSA: Silver inicial: 4 pedidos, contrato completo; Bronze anterior: 10 filas`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que el lote batch_003 aún no está presente en Silver.

  > **Importante:** Si el lote ya existe, detén la ejecución. Este laboratorio es de una sola aplicación; repetirlo exige restablecer el estado inicial bajo autorización.
  {: .lab-note .important .compact}

  ```python
  assert spark.table(B).filter(F.col('batch_id') == BATCH).count() == 0, 'batch_003 ya existe en Bronze'
  assert spark.table(V).filter(F.col('batch_id') == BATCH).count() == 0, 'batch_003 ya fue aplicado en Silver'
  assert spark.table(A).filter(F.col('batch_id') == BATCH).count() == 0, 'batch_003 ya está auditado'
  started = datetime.now(timezone.utc).replace(tzinfo=None)
  print('Sin ejecuciones previas; inicio UTC:', started)
  print("VALIDACIÓN EXITOSA: Lote batch_003 no procesado: Bronze, Silver y auditoría libres")
  ```

  > **Salida esperada:** batch_003 no existe en Bronze, Silver ni auditoría; se registra la hora de inicio. Mensaje: `VALIDACIÓN EXITOSA: Lote batch_003 no procesado: Bronze, Silver y auditoría libres`.
  {: .lab-note .output .compact}

{% assign results = site.data.task-results[page.slug].results %}

{% capture r1 %}{{ results[0] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r1 %}

{% include support-prompt.html task="tarea1" %}

---

## 🟤 Tarea 2. Crear e ingerir el lote incremental — 10 min

Construir seis filas con dos claves repetidas, incorporar columnas nuevas mediante evolución explícita de Bronze y validar la trazabilidad de batch_003.

### Tarea 2.1. Crear el archivo incremental

Preparar versiones antiguas y recientes de pedidos junto con dos inserciones nuevas.

- {% include step_label.html %} Genera el archivo CSV incremental con seis filas controladas.

  ```python
  csv = """order_id,customer_id,order_date,amount,status,source_updated_at,channel,update_sequence
  O1001,C101,2025-01-10,125.50,SHIPPED,2025-02-10 09:00:00,web,1
  O1001,C101,2025-01-10,125.50,DELIVERED,2025-02-10 10:30:00,mobile,2
  O1002,C102,2025-01-11,80.00,CANCELLED,2025-02-10 11:00:00,store,1
  O1004,C104,2025-02-10,210.75,NEW,2025-02-10 12:00:00,web,1
  O1004,C104,2025-02-10,215.00,CONFIRMED,2025-02-10 12:15:00,partner,2
  O1005,C105,2025-02-10,45.25,NEW,2025-02-10 13:00:00,mobile,1
  """
  dbutils.fs.put(P, csv, overwrite=True)
  print('Archivo preparado:', P)
  ```

  > **Salida esperada:** Se crea orders_batch_003_p4.csv.
  {: .lab-note .output .compact}

- {% include step_label.html %} Muestra su contenido y comprueba que contiene dos filas para O1001 y O1004.

  ```python
  print(dbutils.fs.head(P,3000))
  ```

  > **Salida esperada:** Aparecen O1001 y O1004 repetidos.
  {: .lab-note .output .compact}

- {% include step_label.html %} Lee el lote sin inferir tipos y verifica su estructura.

  ```python
  preview = spark.read.option('header', True).option('inferSchema', False).csv(P)
  expected = {'order_id','customer_id','order_date','amount','status',
              'source_updated_at','channel','update_sequence'}
  assert set(preview.columns) == expected, 'Esquema de CSV inesperado'
  assert preview.count() == 6, 'Se esperaban seis filas'
  assert preview.filter(F.col('order_id').isin('O1001','O1004')).count() == 4
  print('CSV correcto: 6 filas, 8 columnas')
  print("VALIDACIÓN EXITOSA: CSV validado: 6 filas, 8 columnas y duplicados controlados")
  ```

  > **Salida esperada:** Se leen seis filas del lote. Mensaje: `VALIDACIÓN EXITOSA: CSV validado: 6 filas, 8 columnas y duplicados controlados`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que la columna channel tiene valores de los cuatro canales admitidos.

  ```python
  display(preview.select('channel').distinct())
  ```

  > **Salida esperada:** Los canales son web, mobile, store y partner.
  {: .lab-note .output .compact}

### Tarea 2.2. Evolucionar Bronze de forma controlada

Añadir campos de actualización que no existían durante la carga batch de la práctica anterior.

- {% include step_label.html %} Convierte las columnas CSV al contrato crudo Bronze y añade batch_id.

  ```python
  source = spark.read.option('header', True).option('inferSchema', False).csv(P)
  inc = (source.select(
      'order_id', 'customer_id', F.col('order_date').alias('order_date_raw'),
      F.lit('1').alias('quantity_raw'), F.col('amount').alias('unit_price_raw'),
      'status', F.col('source_updated_at').alias('source_updated_at_raw'),
      'channel', F.col('update_sequence').alias('update_sequence_raw'))
      .withColumn('batch_id', F.lit(BATCH))
      .withColumn('source_file', F.lit(P))
      .withColumn('ingested_at', F.current_timestamp()))
  assert inc.count() == 6
  print("VALIDACIÓN EXITOSA: DataFrame incremental: 6 registros preparados")
  ```

  > **Salida esperada:** Se crea inc conservando las columnas como texto. Mensaje: `VALIDACIÓN EXITOSA: DataFrame incremental: 6 registros preparados`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Desactiva la evolución automática de esquema en la sesión.

  ```python
  spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled', 'false')
  assert spark.conf.get('spark.databricks.delta.schema.autoMerge.enabled') == 'false'
  print('autoMerge global deshabilitado')
  print("VALIDACIÓN EXITOSA: Evolución global de esquema deshabilitada")
  ```

  > **Salida esperada:** La evolución global queda deshabilitada. Mensaje: `VALIDACIÓN EXITOSA: Evolución global de esquema deshabilitada`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Anexa el lote incremental a Bronze utilizando mergeSchema solo en esta escritura.

  ```python
  assert spark.table(B).filter(F.col('batch_id') == BATCH).count() == 0
  (inc.write.format('delta').mode('append')
   .option('mergeSchema', 'true').saveAsTable(B))
  print('Anexadas 6 filas a Bronze con evolución explícita de esquema')
  print("VALIDACIÓN EXITOSA: Bronze: escritura incremental autorizada y realizada")
  ```

  > **Salida esperada:** Bronze recibe seis filas y las nuevas columnas channel, source_updated_at_raw y update_sequence_raw. Mensaje: `VALIDACIÓN EXITOSA: Bronze: escritura incremental autorizada y realizada`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Valida el conteo de filas y claves únicas del lote ingresado.

  ```python
  z = spark.table(B).filter(F.col('batch_id') == BATCH)
  assert z.count() == 6 and z.select('order_id').distinct().count() == 4
  assert {'channel','source_updated_at_raw','update_sequence_raw'}.issubset(set(z.columns))
  assert spark.table(B).count() == 16
  print('Bronze total: 16; batch_003: 6; claves únicas: 4')
  print("VALIDACIÓN EXITOSA: Bronze: 16 filas totales; batch_003=6; 4 claves únicas y nuevas columnas")
  ```

  > **Salida esperada:** El resultado es 6 filas físicas y 4 claves de negocio. Mensaje: `VALIDACIÓN EXITOSA: Bronze: 16 filas totales; batch_003=6; 4 claves únicas y nuevas columnas`.
  {: .lab-note .output .compact}

{% capture r2 %}{{ results[1] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r2 %}

{% include support-prompt.html task="tarea2" %}

---

## 🧹 Tarea 3. Construir staging y ejecutar MERGE — 18 min

Deduplicar por marca de actualización y secuencia, ejecutar un MERGE condicional y diferenciar actualizaciones frente a inserciones reales.

### Tarea 3.1. Preparar staging deduplicado

Conservar únicamente la versión más reciente por pedido antes de combinarla con Silver.

- {% include step_label.html %} Prepara el DataFrame tipado desde Bronze y la vista p4_incremental_staging.

  ```python
  bronze3 = spark.table(B).filter(F.col('batch_id') == BATCH)
  stage = bronze3.select(
      'order_id', 'customer_id',
      F.expr('try_cast(order_date_raw AS DATE)').alias('order_date'),
      F.expr('try_cast(quantity_raw AS INT)').alias('quantity'),
      F.expr('try_cast(unit_price_raw AS DECIMAL(12,2))').alias('unit_price'),
      F.expr('try_cast(unit_price_raw AS DECIMAL(14,2))').alias('amount'),
      'status',
      F.expr('try_cast(source_updated_at_raw AS TIMESTAMP)').alias('source_updated_at'),
      'channel', F.expr('try_cast(update_sequence_raw AS INT)').alias('seq'),
      'batch_id', 'source_file', 'ingested_at')
  assert stage.filter(F.col('source_updated_at').isNull() | F.col('amount').isNull()
                      | F.col('seq').isNull()).count() == 0
  w = Window.partitionBy('order_id').orderBy(F.col('source_updated_at').desc(), F.col('seq').desc())
  latest = stage.withColumn('rn', F.row_number().over(w)).filter(F.col('rn') == 1).drop('rn','seq')
  latest.createOrReplaceTempView('p4_incremental_staging')
  assert latest.count() == 4
  print("VALIDACIÓN EXITOSA: Staging tipado: conversiones válidas y 4 claves deduplicadas")
  ```

  > **Salida esperada:** La vista temporal contiene cuatro claves únicas. Mensaje: `VALIDACIÓN EXITOSA: Staging tipado: conversiones válidas y 4 claves deduplicadas`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Consulta el staging ordenado por order_id.

  ```python
  display(spark.table('p4_incremental_staging').orderBy('order_id'))
  ```

  > **Salida esperada:** Se muestran cuatro pedidos.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que se conservaron los estados más recientes de O1001 y O1004.

  ```python
  r={x['order_id']:x['status'] for x in spark.table('p4_incremental_staging').collect()}
  assert r['O1001']=='DELIVERED' and r['O1004']=='CONFIRMED'
  print("VALIDACIÓN EXITOSA: Staging: O1001=DELIVERED y O1004=CONFIRMED")
  ```

  > **Salida esperada:** Los estados son DELIVERED y CONFIRMED. Mensaje: `VALIDACIÓN EXITOSA: Staging: O1001=DELIVERED y O1004=CONFIRMED`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Verifica que no existen identificadores repetidos en el staging.

  ```python
  s=spark.table('p4_incremental_staging')
  assert s.count()==s.select('order_id').distinct().count()==4
  print("VALIDACIÓN EXITOSA: Staging: 4 registros y 4 identificadores únicos")
  ```

  > **Salida esperada:** Cuatro registros y cuatro claves únicas. Mensaje: `VALIDACIÓN EXITOSA: Staging: 4 registros y 4 identificadores únicos`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Guarda el estado anterior de los pedidos para contrastarlo después del MERGE.

  ```python
  before_merge = {r['order_id']: r['amount'] for r in spark.table(V)
                  .filter(F.col('order_id').isin('O1001','O1002'))
                  .select('order_id','amount').collect()}
  assert before_merge == {'O1001': Decimal('100.00'), 'O1002': Decimal('70.00')}
  print('Importes previos:', before_merge)
  print("VALIDACIÓN EXITOSA: Estado previo del MERGE: O1001=100.00, O1002=70.00")
  ```

  > **Salida esperada:** Se guardan importes anteriores de las dos actualizaciones. Mensaje: `VALIDACIÓN EXITOSA: Estado previo del MERGE: O1001=100.00, O1002=70.00`.
  {: .lab-note .output .compact}

### Tarea 3.2. Ejecutar actualización e inserción Delta

Aplicar MERGE conservando created_at y actualizando updated_at solo cuando la fuente no sea antigua.

- {% include step_label.html %} Ejecuta MERGE INTO desde la vista deduplicada.

  > **Importante:** No uses directamente Bronze: contiene duplicados que hacen ambiguas las coincidencias.
  {: .lab-note .important .compact}

  ```python
  spark.sql(f'''MERGE INTO {V} target
  USING p4_incremental_staging source ON target.order_id=source.order_id
  WHEN MATCHED AND source.source_updated_at >= target.source_updated_at THEN UPDATE SET
   target.customer_id=source.customer_id, target.order_date=source.order_date,
   target.quantity=source.quantity, target.unit_price=source.unit_price,
   target.amount=source.amount, target.status=source.status,
   target.channel=source.channel, target.source_updated_at=source.source_updated_at,
   target.batch_id=source.batch_id, target.source_file=source.source_file,
   target.ingested_at=source.ingested_at, target.updated_at=current_timestamp()
  WHEN NOT MATCHED THEN INSERT
   (order_id,customer_id,order_date,quantity,unit_price,status,batch_id,source_file,
    ingested_at,amount,source_updated_at,channel,created_at,updated_at)
  VALUES (source.order_id,source.customer_id,source.order_date,source.quantity,source.unit_price,
   source.status,source.batch_id,source.source_file,source.ingested_at,source.amount,
   source.source_updated_at,source.channel,current_timestamp(),current_timestamp())''')
  ```

  > **Salida esperada:** Delta MERGE finaliza correctamente.
  {: .lab-note .output .compact}

- {% include step_label.html %} Consulta los cuatro pedidos afectados para observar status, amount y channel.

  ```python
  display(spark.table(V).filter(F.col('order_id').isin('O1001','O1002','O1004','O1005')).select('order_id','status','amount','channel'))
  ```

  > **Salida esperada:** Dos filas actualizadas y dos nuevas.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que O1001 y O1002 se actualizaron con los valores nuevos.

  ```python
  d = {r['order_id']: r for r in spark.table(V)
       .filter(F.col('order_id').isin('O1001','O1002')).collect()}
  assert d['O1001']['status'] == 'DELIVERED' and d['O1001']['amount'] == Decimal('125.50')
  assert d['O1002']['status'] == 'CANCELLED' and d['O1002']['amount'] == Decimal('80.00')
  assert d['O1001']['created_at'] is not None and d['O1002']['created_at'] is not None
  print('Dos actualizaciones verificadas')
  print("VALIDACIÓN EXITOSA: MERGE: O1001=125.50 DELIVERED y O1002=80.00 CANCELLED")
  ```

  > **Salida esperada:** Las actualizaciones presentan DELIVERED y CANCELLED. Mensaje: `VALIDACIÓN EXITOSA: MERGE: O1001=125.50 DELIVERED y O1002=80.00 CANCELLED`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que O1004 y O1005 fueron insertados y aparecen una sola vez.

  ```python
  new = {r['order_id']: r['amount'] for r in spark.table(V)
         .filter(F.col('order_id').isin('O1004','O1005'))
         .select('order_id','amount').collect()}
  assert new == {'O1004': Decimal('215.00'), 'O1005': Decimal('45.25')}
  print('Dos inserciones verificadas')
  print("VALIDACIÓN EXITOSA: MERGE: O1004=215.00 y O1005=45.25 insertados")
  ```

  > **Salida esperada:** Las dos claves nuevas se encuentran en Silver. Mensaje: `VALIDACIÓN EXITOSA: MERGE: O1004=215.00 y O1005=45.25 insertados`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que no se produjeron claves duplicadas en la tabla Silver.

  ```python
  assert spark.table(V).count() == 6
  assert spark.table(V).select('order_id').distinct().count() == 6
  print('Silver: 6 claves distintas')
  print("VALIDACIÓN EXITOSA: Silver contiene 6 pedidos sin identificadores duplicados")
  ```

  > **Salida esperada:** Cada order_id es único. Mensaje: `VALIDACIÓN EXITOSA: Silver contiene 6 pedidos sin identificadores duplicados`.
  {: .lab-note .output .compact}

### Tarea 3.3. Verificar trazabilidad del MERGE

Comprobar que los cuatro pedidos procesados conservan el identificador de lote y fechas técnicas.

- {% include step_label.html %} Consulta el lote batch_003 después del MERGE y verifica exactamente cuatro registros.

  ```python
  assert spark.table(V).filter(F.col('batch_id')==BATCH).count()==4
  print("VALIDACIÓN EXITOSA: Trazabilidad: 4 pedidos asociados al lote batch_003")
  ```

  > **Salida esperada:** El lote incremental tiene cuatro pedidos únicos. Mensaje: `VALIDACIÓN EXITOSA: Trazabilidad: 4 pedidos asociados al lote batch_003`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que las columnas created_at y updated_at están pobladas.

  ```python
  assert spark.table(V).filter(F.col('batch_id')==BATCH).filter(F.col('created_at').isNull() | F.col('updated_at').isNull()).count()==0
  print("VALIDACIÓN EXITOSA: Trazabilidad: created_at y updated_at no tienen valores nulos")
  ```

  > **Salida esperada:** Todas las filas procesadas conservan marcas temporales. Mensaje: `VALIDACIÓN EXITOSA: Trazabilidad: created_at y updated_at no tienen valores nulos`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Visualiza la operación MERGE en el historial Delta de Silver.

  ```python
  display(spark.sql(f'DESCRIBE HISTORY {V} LIMIT 5'))
  ```

  > **Salida esperada:** Aparece una operación MERGE reciente.
  {: .lab-note .output .compact}

{% capture r3 %}{{ results[2] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r3 %}

{% include support-prompt.html task="tarea3" %}

---

## 🔁 Tarea 4. Actualizar Gold y optimizar Silver — 15 min

Reconstruir Gold con datos incrementales y comprobar operaciones físicas Delta; distinguir la optimización del resultado de negocio.

### Tarea 4.1. Reconstruir las ventas diarias

Actualizar los agregados por fecha y canal excluyendo pedidos cancelados.

- {% include step_label.html %} Reconstruye Gold con la tabla Silver actualizada.

  ```python
  spark.sql(f'''CREATE OR REPLACE TABLE {G} USING DELTA AS
   SELECT order_date, COALESCE(channel,'unknown') AS channel,
   COUNT(*) AS total_orders, CAST(SUM(amount) AS DECIMAL(16,2)) AS total_sales
   FROM {V} WHERE status <> 'CANCELLED' GROUP BY order_date, COALESCE(channel,'unknown')''')
  ```

  > **Salida esperada:** Gold agrupa por fecha/canal y suma 460.75, excluyendo O1002 (CANCELLED).
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que el pedido O1002 cancelado no contribuye al agregado Gold.

  ```python
  g = spark.table(G)
  assert g.filter(F.col('order_date') == F.lit('2025-01-11').cast('date')).count() == 0
  assert g.agg(F.sum('total_sales')).first()[0] == Decimal('460.75')
  print('Gold excluye CANCELLED; ventas 460.75')
  print("VALIDACIÓN EXITOSA: Gold: pedido cancelado excluido; ventas no canceladas=460.75")
  ```

  > **Salida esperada:** La fecha y canal del pedido CANCELLED no aparecen. Mensaje: `VALIDACIÓN EXITOSA: Gold: pedido cancelado excluido; ventas no canceladas=460.75`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba el aporte de O1004 y O1005 en la fecha 2025-02-10.

  ```python
  day = (spark.table(G).filter(F.col('order_date') == F.lit('2025-02-10').cast('date'))
         .select('channel', 'total_sales'))
  amounts = {r['channel']: r['total_sales'] for r in day.collect()}
  assert amounts == {'unknown': Decimal('30.00'), 'partner': Decimal('215.00'),
                     'mobile': Decimal('45.25')}
  display(day.orderBy('channel'))
  print("VALIDACIÓN EXITOSA: Gold del 10 de febrero: unknown=30.00, partner=215.00 y mobile=45.25")
  ```

  > **Salida esperada:** partner=215.00 y mobile=45.25; unknown puede incluir 30.00 de batch_002. Mensaje: `VALIDACIÓN EXITOSA: Gold del 10 de febrero: unknown=30.00, partner=215.00 y mobile=45.25`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Consulta el total Gold y el total Silver no cancelado para reconciliar importes.

  ```python
  a=spark.table(G).agg(F.sum('total_sales')).first()[0]
  b=spark.table(V).filter(F.col('status')!='CANCELLED').agg(F.sum('amount')).first()[0]
  assert a==b
  print("VALIDACIÓN EXITOSA: Conciliación: importes elegibles de Silver y Gold coinciden")
  ```

  > **Salida esperada:** La reconciliación de ventas es exacta. Mensaje: `VALIDACIÓN EXITOSA: Conciliación: importes elegibles de Silver y Gold coinciden`.
  {: .lab-note .output .compact}

### Tarea 4.2. Optimizar físicamente Silver

Ejecutar OPTIMIZE y ZORDER sobre claves consultadas y documentar que los beneficios son limitados con datos pequeños.

- {% include step_label.html %} Registra la cantidad de archivos previa a OPTIMIZE.

  ```python
  n_before=spark.sql(f'DESCRIBE DETAIL {V}').first()['numFiles']; print(n_before)
  ```

  > **Salida esperada:** Se obtiene el número inicial de archivos.
  {: .lab-note .output .compact}

- {% include step_label.html %} Ejecuta OPTIMIZE con ZORDER BY sobre order_id y customer_id.

  > **Nota:** Este dataset es pequeño; la operación demuestra sintaxis e historial, no una mejora medible de rendimiento.
  {: .lab-note .info .compact}

  ```python
  spark.sql(f'OPTIMIZE {V} ZORDER BY (order_id, customer_id)').show(truncate=False)
  ```

  > **Salida esperada:** Se muestra el resultado de OPTIMIZE.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba el estado físico después de la optimización.

  ```python
  print('Archivos después:',spark.sql(f'DESCRIBE DETAIL {V}').first()['numFiles'])
  ```

  > **Salida esperada:** Se muestra el conteo posterior de archivos.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que el resultado lógico no cambió tras OPTIMIZE.

  ```python
  assert spark.table(V).count() == 6
  assert spark.table(V).select('order_id').distinct().count() == 6
  assert spark.table(V).filter(F.col('batch_id') == BATCH).count() == 4
  print('OPTIMIZE no modificó el contenido lógico')
  print("VALIDACIÓN EXITOSA: OPTIMIZE: Silver conserva 6 pedidos y 4 del lote batch_003")
  ```

  > **Salida esperada:** Silver conserva seis claves únicas: dos de batch_002, dos semillas actualizadas y dos nuevas. Mensaje: `VALIDACIÓN EXITOSA: OPTIMIZE: Silver conserva 6 pedidos y 4 del lote batch_003`.
  {: .lab-note .output .compact}

### Tarea 4.3. Revisar historial y métricas

Examinar operaciones transaccionales y contrastar MERGE y OPTIMIZE mediante las entradas del Delta Log.

- {% include step_label.html %} Muestra únicamente las operaciones MERGE y OPTIMIZE usando el historial como DataFrame.

  ```python
  h=spark.sql(f'DESCRIBE HISTORY {V}')
  display(h.filter(F.col('operation').isin('MERGE','OPTIMIZE')).select('version','operation','operationMetrics'))
  ```

  > **Salida esperada:** Se muestran MERGE y OPTIMIZE con métricas cuando están disponibles.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que el historial contiene ambas operaciones.

  ```python
  ops={r['operation'] for r in h.select('operation').collect()}
  assert {'MERGE','OPTIMIZE'}.issubset(ops)
  print("VALIDACIÓN EXITOSA: Historial Delta contiene MERGE y OPTIMIZE")
  ```

  > **Salida esperada:** Se validan operaciones MERGE y OPTIMIZE. Mensaje: `VALIDACIÓN EXITOSA: Historial Delta contiene MERGE y OPTIMIZE`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Consulta el historial de Bronze para ubicar la escritura con evolución de esquema.

  ```python
  display(spark.sql(f'DESCRIBE HISTORY {B} LIMIT 5'))
  ```

  > **Salida esperada:** El historial muestra la escritura APPEND con evolución de columnas.
  {: .lab-note .output .compact}

{% capture r4 %}{{ results[3] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r4 %}

{% include support-prompt.html task="tarea4" %}

---

## ✅ Tarea 5. Auditar y validar el lote incremental — 9 min

Registrar los conteos de batch_003 y obtener evidencia de que Silver y Gold reflejan las dos inserciones y las dos actualizaciones esperadas.

### Tarea 5.1. Validar reglas y cifras del MERGE

Ejecutar aserciones reproducibles sobre las cuatro claves procesadas y sobre la agregación Gold.

- {% include step_label.html %} Valida que Bronze tiene seis registros del lote y Silver cuatro registros afectados.

  ```python
  assert spark.table(B).filter(F.col('batch_id')==BATCH).count()==6
  assert spark.table(V).filter(F.col('batch_id')==BATCH).count()==4
  print("VALIDACIÓN EXITOSA: Conteos finales: Bronze batch_003=6 y Silver batch_003=4")
  ```

  > **Salida esperada:** Bronze=6 y Silver batch_003=4. Mensaje: `VALIDACIÓN EXITOSA: Conteos finales: Bronze batch_003=6 y Silver batch_003=4`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba los canales finales de O1001, O1002, O1004 y O1005.

  ```python
  channels={r['order_id']:r['channel'] for r in spark.table(V).filter(F.col('batch_id')==BATCH).collect()}
  assert channels=={'O1001':'mobile','O1002':'store','O1004':'partner','O1005':'mobile'}
  print("VALIDACIÓN EXITOSA: Canales finales: mobile, store, partner y mobile por pedido")
  ```

  > **Salida esperada:** Los cuatro canales coinciden con el lote incremental. Mensaje: `VALIDACIÓN EXITOSA: Canales finales: mobile, store, partner y mobile por pedido`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba la ausencia de duplicados después del MERGE.

  ```python
  assert spark.table(V).count()==spark.table(V).select('order_id').distinct().count()
  print("VALIDACIÓN EXITOSA: Integridad: no existen order_id duplicados en Silver")
  ```

  > **Salida esperada:** La tabla Silver conserva claves de negocio únicas. Mensaje: `VALIDACIÓN EXITOSA: Integridad: no existen order_id duplicados en Silver`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Valida que la tabla Gold existe y usa formato Delta.

  ```python
  assert spark.sql(f'DESCRIBE DETAIL {G}').first()['format']=='delta'
  print("VALIDACIÓN EXITOSA: Tabla Gold validada con formato Delta")
  ```

  > **Salida esperada:** Gold es Delta y sus resultados son consultables. Mensaje: `VALIDACIÓN EXITOSA: Tabla Gold validada con formato Delta`.
  {: .lab-note .output .compact}

### Tarea 5.2. Registrar auditoría y conservar estado

Guardar un registro compatible con el esquema original de la auditoría de la Práctica 3.

- {% include step_label.html %} Registra batch_003 en la tabla de auditoría con los campos que creó la Práctica 3.

  ```python
  completed = datetime.now(timezone.utc).replace(tzinfo=None)
  audit_schema = T.StructType([
      T.StructField('batch_id', T.StringType()),
      T.StructField('source_file', T.StringType()),
      T.StructField('started_at', T.TimestampType()),
      T.StructField('completed_at', T.TimestampType()),
      T.StructField('bronze_records', T.LongType()),
      T.StructField('valid_records', T.LongType()),
      T.StructField('rejected_records', T.LongType()),
      T.StructField('schema_enforcement_status', T.StringType()),
      T.StructField('detail', T.StringType())
  ])
  assert spark.table(A).filter(F.col('batch_id') == BATCH).count() == 0
  row = [(BATCH, P, started, completed, 6, 4, 0,
          'NOT_TESTED', 'MERGE: 2 actualizaciones, 2 inserciones; OPTIMIZE ejecutado')]
  spark.createDataFrame(row, audit_schema).write.format('delta').mode('append').saveAsTable(A)
  assert spark.table(A).filter(F.col('batch_id') == BATCH).count() == 1
  print("VALIDACIÓN EXITOSA: Auditoría: batch_003 registrado una sola vez")
  ```

  > **Salida esperada:** Auditoría registra 6 filas Bronze y 4 claves incrementales. Mensaje: `VALIDACIÓN EXITOSA: Auditoría: batch_003 registrado una sola vez`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Consulta la auditoría y el historial Silver como evidencia del laboratorio.

  ```python
  display(spark.table(A).filter(F.col('batch_id')==BATCH));display(spark.sql(f'DESCRIBE HISTORY {V} LIMIT 5'))
  ```

  > **Salida esperada:** Aparece una fila batch_003 y las operaciones Delta recientes.
  {: .lab-note .output .compact}

- {% include step_label.html %} Guarda el notebook sin eliminar las tablas que usará la Práctica 5.

  > **Importante:** Evita DROP y VACUUM: los datos deben conservar su historial para Time Travel.
  {: .lab-note .important .compact}

  > **Salida esperada:** El conjunto incremental queda disponible para realizar auditoría y recuperación.
  {: .lab-note .output .compact}

{% capture r5 %}{{ results[4] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r5 %}

{% include support-prompt.html task="tarea5" %}

