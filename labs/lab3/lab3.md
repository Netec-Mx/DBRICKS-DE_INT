---
layout: lab
title: 'Práctica 3: Ingesta batch completa con validaciones'
permalink: /lab3/lab3/
images_base: /labs/lab3/img
duration: 70 minutos
objective:
- Ingerir un lote de 10 filas en Bronze con trazabilidad.
- Aplicar validaciones de calidad y cuarentena sobre ocho registros rechazados.
- Publicar dos pedidos válidos y sembrar dos claves base para probar MERGE en la Práctica 4.
- Construir Gold y auditar schema enforcement sin alterar Silver.
prerequisites:
- Unity Catalog habilitado; permisos USE CATALOG, USE SCHEMA, CREATE TABLE, MODIFY, SELECT y CREATE VOLUME/READ VOLUME/WRITE VOLUME para preparar los recursos cuando corresponda.
- Reutilizar el mismo clúster All-Purpose de las Prácticas 1 y 2, con Databricks Runtime 15.4 LTS compatible con Unity Catalog; debe estar Running.
- Conocimientos básicos de PySpark, DataFrames, tablas Delta y SQL.
- Catálogo dbricks_de_int y esquema batch1 del curso; usar un esquema autorizado exclusivo por participante si el entorno es compartido.
introduction:
- Procesarás un archivo CSV controlado con diez pedidos, reglas de calidad y cuarentena. El ejercicio utiliza tablas
  p3_ aisladas para evitar incompatibilidades con el esquema de la Práctica 2 y deja preparados los datos base del
  siguiente MERGE.
slug: lab3
lab_number: 3
final_result: Se registran 10 filas Bronze, 2 válidas y 8 rechazadas; Silver contiene 4 pedidos incluyendo las 2 semillas. Gold (4 fechas, 245.00) y auditoría
  Delta quedan disponibles para continuar con las Prácticas 4 y 5.
notes:
- Se usan nombres p3_ en las tablas para preservar intactos los ejercicios anteriores.
- El lote batch_002 contiene 10 filas; se siembran por separado O1001 y O1002 para el MERGE posterior.
- La preparación crea esquema y volumen si faltan, siempre que haya permisos; no requiere otro clúster. Si CREATE VOLUME es denegado, solicita permisos al administrador antes de continuar.
- La ejecución repetida reemplaza únicamente las cinco tablas p3_ de esta práctica y el CSV exclusivo p3; no elimina las tablas de lab2.
references:
- text: Delta Lake MERGE
  url: https://docs.databricks.com/aws/en/delta/merge
- text: Delta Lake table history
  url: https://docs.databricks.com/aws/en/delta/history
- text: Unity Catalog managed tables
  url: https://docs.databricks.com/aws/en/tables/managed
- text: Databricks OPTIMIZE
  url: https://docs.databricks.com/aws/en/sql/language-manual/delta-optimize
prev: /lab2/lab2/
next: /lab4/lab4/
---

<!-- 5 tareas; 12 subtareas; 52 pasos; 70 minutos. -->

---

## 🔎 Tarea 1. Preparar el entorno y las tablas auxiliares — 12 min

Configurar un laboratorio aislado en el esquema autorizado, comprobar los permisos y crear estructuras de cuarentena y auditoría sin modificar las tablas de la Práctica 2.

### Tarea 1.1. Abrir notebook y confirmar el cómputo

Ubicar el notebook y verificar que hay un clúster compatible con Unity Catalog.

- {% include step_label.html %} En Workspace, crea el notebook Python **03_ingesta_batch_validaciones** dentro de **batch_1**.

  > **Salida esperada:** Notebook creado con lenguaje Python.
  {: .lab-note .output .compact}

- {% include step_label.html %} Asocia el notebook al mismo clúster de Databricks disponible para las prácticas.

  > **Salida esperada:** El selector de cómputo indica Connected o Running.
  {: .lab-note .output .compact}

- {% include step_label.html %} Ejecuta la celda de constantes para definir las cinco tablas p3_ y el archivo del lote.

  ```python
  from pyspark.sql import functions as F
  from pyspark.sql.window import Window
  from datetime import datetime, timezone
  from pyspark.sql import types as T
  C = 'dbricks_de_int'
  S = 'batch1'
  PREFIX = f'{C}.{S}.p3_'
  B=PREFIX+'bronze_orders'; V=PREFIX+'silver_orders'; G=PREFIX+'gold_daily_sales'
  Q=PREFIX+'quarantine_orders'; A=PREFIX+'batch_ingestion_audit'
  LANDING = f'/Volumes/{C}/{S}/landing'
  LAB_DIR = f'{LANDING}/lab3'
  P = f'{LAB_DIR}/orders_batch_002_p3.csv'
  BATCH='batch_002'
  print('Dataset aislado:', PREFIX)
  ```

  > **Salida esperada:** Se imprime dbricks_de_int.batch1.p3_ como prefijo; se conserva el clúster existente.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que el esquema y el volumen de aterrizaje son accesibles.

  ```python
  spark.sql(f'USE CATALOG `{C}`')
  spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{C}`.`{S}`')
  spark.sql(f'CREATE VOLUME IF NOT EXISTS `{C}`.`{S}`.`landing`')
  dbutils.fs.mkdirs(LAB_DIR)
  display(dbutils.fs.ls(LANDING))
  ```

  > **Salida esperada:** El esquema y el volumen landing existen, se crea la carpeta lab3 y se lista el contenido sin UC_VOLUME_NOT_FOUND. Si faltan privilegios, solicita acceso; no cambies de catálogo automáticamente.
  {: .lab-note .output .compact}

### Tarea 1.2. Crear las tablas de cuarentena y auditoría

Preparar el almacenamiento Delta necesario para controlar rechazos y métricas de cada lote.

- {% include step_label.html %} Confirma que trabajarás con tablas p3_ y no con las tablas creadas por la Práctica 2.

  > **Nota:** Así se evita sobrescribir accidentalmente el laboratorio anterior.
  {: .lab-note .info .compact}

  ```python
  print('Tablas exclusivas de la Práctica 3:')
  for name in [B, V, G, Q, A]:
      assert name.startswith(PREFIX)
      print(name)
  print('VALIDACIÓN EXITOSA: las cinco tablas pertenecen al prefijo p3_')
  ```

  > **Salida esperada:** La celda confirma explícitamente que las cinco tablas usan el prefijo p3_.
  {: .lab-note .output .compact}

- {% include step_label.html %} Crea las tablas Delta para cuarentena y auditoría utilizando los esquemas definidos.

  ```python
  spark.sql(f'''CREATE TABLE IF NOT EXISTS {Q} (
   order_id STRING, customer_id STRING, order_date_raw STRING, quantity_raw STRING,
   unit_price_raw STRING, status STRING, batch_id STRING, source_file STRING,
   ingested_at TIMESTAMP, rejection_reason STRING) USING DELTA''')
  spark.sql(f'''CREATE TABLE IF NOT EXISTS {A} (
   batch_id STRING, source_file STRING, started_at TIMESTAMP, completed_at TIMESTAMP,
   bronze_records BIGINT, valid_records BIGINT, rejected_records BIGINT,
   schema_enforcement_status STRING, detail STRING) USING DELTA''')
  ```

  > **Salida esperada:** Se crean las tablas p3_quarantine_orders y p3_batch_ingestion_audit.
  {: .lab-note .output .compact}

- {% include step_label.html %} Verifica las columnas y tipos de ambas tablas auxiliares.

  ```python
  display(spark.sql(f'DESCRIBE {Q}')); display(spark.sql(f'DESCRIBE {A}'))
  ```

  > **Salida esperada:** La auditoría usa conteos BIGINT y marca temporal TIMESTAMP.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que las dos tablas auxiliares utilizan el formato Delta.

  ```python
  assert all(spark.sql(f'DESCRIBE DETAIL {t}').first()['format']=='delta' for t in [Q,A])
  print('VALIDACIÓN EXITOSA: cuarentena y auditoría utilizan formato Delta')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA: cuarentena y auditoría utilizan formato Delta.
  {: .lab-note .output .compact}

### Tarea 1.3. Preparar ejecución reproducible

Determinar el estado inicial y evitar residuos de una repetición anterior.

- {% include step_label.html %} Registra el instante de inicio en una variable Python para usarlo en la auditoría.

  ```python
  started=datetime.now(timezone.utc).replace(tzinfo=None)
  ```

  > **Salida esperada:** La variable started contiene un datetime.
  {: .lab-note .output .compact}

- {% include step_label.html %} Consulta los objetos p3_ existentes antes de crear el lote de pruebas.

  ```python
  display(spark.sql(f'SHOW TABLES IN `{C}`.`{S}`').filter(F.col('tableName').startswith('p3_')))
  ```

  > **Salida esperada:** Se muestran los nombres p3_ existentes.
  {: .lab-note .output .compact}

- {% include step_label.html %} Revisa el comportamiento de una repetición: las escrituras reemplazan las tablas de datos p3_; la auditoría también se reemplazará para evitar filas duplicadas.

  > **Advertencia:** No borres tablas de la Práctica 2 ni ejecutes DROP: las tablas p3_ se actualizarán de forma controlada.
  {: .lab-note .warning .compact}

  ```python
  # No se requiere DROP ni limpieza manual.
  # Bronze, Silver, Gold, cuarentena y auditoría se reescriben de forma controlada.
  print('Reejecución permitida sobre tablas p3_ exclusivamente')
  ```

  > **Salida esperada:** No se elimina ninguna tabla y no hace falta intervenir manualmente antes de repetir.
  {: .lab-note .output .compact}

{% assign results = site.data.task-results[page.slug].results %}

{% capture r1 %}{{ results[0] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r1 %}

{% include support-prompt.html task="tarea1" %}

---

## 🟤 Tarea 2. Generar e ingerir un lote batch — 10 min

Crear un archivo de diez pedidos que ejercite todas las reglas de calidad y cargarlo íntegro en Bronze con metadatos técnicos verificables.

### Tarea 2.1. Crear el CSV controlado

Proporcionar dos pedidos válidos y ocho filas rechazables, con duplicados y conversiones incorrectas.

- {% include step_label.html %} Ejecuta la celda que construye las diez filas del CSV controlado para batch_002.

  ```python
  csv='''order_id,customer_id,order_date,quantity,unit_price,status
  1001,C001,2025-02-10,2,15.50,NEW
  1002,C002,2025-02-10,1,30.00,NEW
  ,C003,2025-02-11,3,10.00,NEW
  1001,C004,2025-02-11,1,25.00,NEW
  1004,C005,2025-02-12,0,12.50,NEW
  1005,C006,2025-02-12,2,-5.00,NEW
  1006,C007,2025-02-13,abc,18.00,NEW
  1007,C008,fecha_invalida,2,20.00,NEW
  1008,,2025-02-14,4,8.75,NEW
  1009,C009,2025-02-14,5,9.00,NEW
  '''
  dbutils.fs.put(P, csv, overwrite=True)
  assert any(item.name == 'orders_batch_002_p3.csv' for item in dbutils.fs.ls(LAB_DIR))
  print('VALIDACIÓN EXITOSA: archivo CSV del laboratorio 3 disponible en el Volume')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA: archivo CSV del laboratorio 3 disponible en el Volume.
  {: .lab-note .output .compact}

- {% include step_label.html %} Confirma que el archivo puede abrirse desde el volumen administrado.

  ```python
  print(dbutils.fs.head(P,3000))
  ```

  > **Salida esperada:** Se observan 10 registros de datos y una cabecera.
  {: .lab-note .output .compact}

- {% include step_label.html %} Lee el CSV sin inferencia de tipos y comprueba que conserva los datos inválidos como texto.

  ```python
  preview=spark.read.option('header',True).option('inferSchema',False).csv(P)
  display(preview)
  ```

  > **Salida esperada:** El DataFrame tiene diez filas; aparecen abc y fecha_invalida.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba los nombres de las columnas y el número exacto de filas de origen.

  ```python
  assert preview.count()==10
  assert set(preview.columns)=={'order_id','customer_id','order_date','quantity','unit_price','status'}
  print('VALIDACIÓN EXITOSA: CSV con 10 filas y las 6 columnas requeridas')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA: CSV con 10 filas y las 6 columnas requeridas.
  {: .lab-note .output .compact}

### Tarea 2.2. Persistir los datos originales en Bronze

Conservar todos los registros en una tabla Delta sin filtrar los datos de mala calidad.

- {% include step_label.html %} Prepara las columnas Bronze sin conversiones de tipos de negocio.

  ```python
  raw=spark.read.option('header',True).option('inferSchema',False).csv(P)
  bronze=(raw.select('order_id','customer_id',F.col('order_date').alias('order_date_raw'),
                     F.col('quantity').alias('quantity_raw'),F.col('unit_price').alias('unit_price_raw'),'status')
          .withColumn('batch_id',F.lit(BATCH))
          .withColumn('source_file',F.lit(P))
          .withColumn('ingested_at',F.current_timestamp()))
  ```

  > **Salida esperada:** bronze conserva el contenido del CSV como texto.
  {: .lab-note .output .compact}

- {% include step_label.html %} Escribe los datos originales en una tabla Delta p3_bronze_orders aislada.

  ```python
  bronze.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(B)
  ```

  > **Salida esperada:** Bronze almacena diez filas de batch_002.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba el conteo y el identificador de lote en Bronze.

  ```python
  assert spark.table(B).count()==10
  assert {row.batch_id for row in spark.table(B).select('batch_id').distinct().collect()} == {BATCH}
  print('VALIDACIÓN EXITOSA: Bronze contiene 10 filas exclusivamente del lote batch_002')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA: Bronze contiene 10 filas exclusivamente del lote batch_002.
  {: .lab-note .output .compact}

- {% include step_label.html %} Visualiza los metadatos y los dos registros repetidos con order_id 1001.

  ```python
  display(spark.table(B).filter(F.col('order_id')=='1001'))
  ```

  > **Salida esperada:** Dos filas con order_id 1001; cada una incluye source_file e ingested_at.
  {: .lab-note .output .compact}

{% capture r2 %}{{ results[1] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r2 %}

{% include support-prompt.html task="tarea2" %}

---

## 🧹 Tarea 3. Aplicar validaciones y publicar Silver — 23 min

Tipar los registros, validar ocho condiciones, conservar motivos de rechazo y persistir únicamente los pedidos correctos en Silver. Crear además dos pedidos base controlados para el MERGE posterior.

### Tarea 3.1. Convertir datos y validar campos

Aplicar try_cast para que las conversiones fallidas no detengan el proceso de carga.

- {% include step_label.html %} Lee solo el lote actual desde Bronze y realiza conversiones tolerantes con try_cast.

  ```python
  x=(spark.table(B).filter(F.col('batch_id')==BATCH)
     .withColumn('order_date',F.expr('try_cast(order_date_raw AS DATE)'))
     .withColumn('quantity',F.expr('try_cast(quantity_raw AS INT)'))
     .withColumn('unit_price',F.expr('try_cast(unit_price_raw AS DECIMAL(12,2))'))
     .withColumn('occurrences',F.count('*').over(Window.partitionBy('batch_id','order_id'))))
  ```

  > **Salida esperada:** Se crea x con columnas order_date, quantity, unit_price y occurrences.
  {: .lab-note .output .compact}

- {% include step_label.html %} Muestra las conversiones inválidas para identificar las filas que requieren cuarentena.

  ```python
  display(x.filter(F.col('quantity').isNull() | F.col('order_date').isNull()))
  ```

  > **Salida esperada:** Aparecen la cantidad abc y la fecha fecha_invalida.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que order_id 1001 tiene dos ocurrencias en el lote.

  ```python
  display(x.filter(F.col('order_id')=='1001').select('order_id','occurrences'))
  ```

  > **Salida esperada:** Las dos filas muestran occurrences = 2.
  {: .lab-note .output .compact}

- {% include step_label.html %} Construye la columna rejection_reason con ocho condiciones de calidad (identificador, cliente, fecha, cantidad, precio y duplicados).

  ```python
  x=x.withColumn('rejection_reason',F.concat_ws('; ',
   F.when(F.col('order_id').isNull() | (F.trim(F.col('order_id'))==''),'order_id inválido'),
   F.when(F.col('customer_id').isNull() | (F.trim(F.col('customer_id'))==''),'customer_id inválido'),
   F.when(F.col('order_date').isNull(),'fecha inválida'),
   F.when(F.col('quantity').isNull(),'cantidad no convertible'),
   F.when(F.col('quantity')<=0,'cantidad no positiva'),
   F.when(F.col('unit_price').isNull(),'precio no convertible'),
   F.when(F.col('unit_price')<=0,'precio no positivo'),
   F.when(F.col('occurrences')>1,'order_id duplicado en lote')))
  assert x.filter(F.col('rejection_reason') == '').count() == 2
  print('VALIDACIÓN EXITOSA: se detectaron exactamente 2 filas válidas')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA: se detectaron exactamente 2 filas válidas.
  {: .lab-note .output .compact}

- {% include step_label.html %} Muestra las causas de rechazo antes de guardar los datos.

  ```python
  display(x.select('order_id','rejection_reason'))
  ```

  > **Salida esperada:** Se observan 8 filas con algún motivo y 2 sin rechazo.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que se identificaron dos registros válidos y ocho rechazados.

  ```python
  assert x.filter(F.col('rejection_reason')=='').count()==2
  assert x.filter(F.col('rejection_reason')!='').count()==8
  print('VALIDACIÓN EXITOSA: 2 pedidos válidos y 8 registros rechazados')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA: 2 pedidos válidos y 8 registros rechazados.
  {: .lab-note .output .compact}

### Tarea 3.2. Separar y persistir los registros

Preparar tablas Silver y cuarentena con el contrato que se reutilizará para MERGE y Time Travel.

- {% include step_label.html %} Construye valid y rejected; calcula amount y conserva metadatos.

  ```python
  valid=(x.filter(F.col('rejection_reason')=='').select(
   'order_id','customer_id','order_date','quantity','unit_price','status','batch_id','source_file','ingested_at')
   .withColumn('amount',(F.col('quantity')*F.col('unit_price')).cast('decimal(14,2)'))
   .withColumn('source_updated_at',F.to_timestamp(F.concat(F.col('order_date').cast('string'),F.lit(' 09:00:00'))))
   .withColumn('channel',F.lit('unknown'))
   .withColumn('created_at',F.current_timestamp()).withColumn('updated_at',F.current_timestamp()))
  rejected=x.filter(F.col('rejection_reason')!='').select('order_id','customer_id','order_date_raw',
   'quantity_raw','unit_price_raw','status','batch_id','source_file','ingested_at','rejection_reason')
  ```

  > **Salida esperada:** valid contiene amount DECIMAL y rejected conserva rejection_reason.
  {: .lab-note .output .compact}

- {% include step_label.html %} Persiste los ocho registros rechazados en la tabla de cuarentena.

  ```python
  rejected.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(Q)
  ```

  > **Salida esperada:** Cuarentena contiene 8 registros.
  {: .lab-note .output .compact}

- {% include step_label.html %} Escribe los dos pedidos correctos en la tabla Delta Silver p3_silver_orders.

  ```python
  valid.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(V)
  assert set(r.order_id for r in spark.table(V).select('order_id').collect()) == {'1002', '1009'}
  print('VALIDACIÓN EXITOSA: Silver contiene los pedidos 1002 y 1009')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA: Silver contiene los pedidos 1002 y 1009.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que los pedidos válidos no están repetidos.

  ```python
  assert spark.table(V).count()==spark.table(V).select('order_id').distinct().count()
  print('VALIDACIÓN EXITOSA: no hay identificadores duplicados en Silver')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA: no hay identificadores duplicados en Silver.
  {: .lab-note .output .compact}

- {% include step_label.html %} Muestra las filas rechazadas y sus causas desde la tabla Delta.

  ```python
  display(spark.table(Q).select('order_id','rejection_reason'))
  ```

  > **Salida esperada:** La consulta muestra 8 filas con sus razones específicas.
  {: .lab-note .output .compact}

### Tarea 3.3. Preparar datos base del MERGE posterior

Sembrar dos claves reales que permitirán demostrar actualizaciones y nuevas inserciones en la Práctica 4.

- {% include step_label.html %} Crea dos pedidos iniciales O1001 y O1002 con el esquema de Silver.

  ```python
  seed=spark.createDataFrame([
   ('O1001','C101','2025-01-10',1,'100.00','NEW'),
   ('O1002','C102','2025-01-11',1,'70.00','NEW')],
   'order_id string, customer_id string, order_date string, quantity long, unit_price string, status string')
  seed=(seed.withColumn('order_date',F.to_date('order_date'))
   .withColumn('quantity',F.col('quantity').cast('int'))
   .withColumn('unit_price',F.col('unit_price').cast('decimal(12,2)'))
   .withColumn('amount',(F.col('quantity') * F.col('unit_price')).cast('decimal(14,2)'))
   .withColumn('batch_id',F.lit('seed_p3'))
   .withColumn('source_file',F.lit('lab3_seed'))
   .withColumn('ingested_at',F.current_timestamp())
   .withColumn('source_updated_at',F.to_timestamp(F.concat(F.col('order_date').cast('string'),F.lit(' 08:00:00'))))
   .withColumn('channel',F.lit('unknown'))
   .withColumn('created_at',F.current_timestamp()).withColumn('updated_at',F.current_timestamp()))
  # Seleccionar columnas en el orden de Silver y evitar errores de esquema al insertar.
  seed = seed.select(*spark.table(V).columns)
  seed.write.format('delta').mode('append').saveAsTable(V)
  ```

  > **Salida esperada:** Silver conserva también O1001 y O1002 como datos base.
  {: .lab-note .output .compact}

- {% include step_label.html %} Valida las cuatro claves de negocio que permanecen disponibles en Silver.

  ```python
  assert set(r[0] for r in spark.table(V).select('order_id').collect())=={'1002','1009','O1001','O1002'}
  print('VALIDACIÓN EXITOSA: Silver contiene 1002, 1009, O1001 y O1002')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA: Silver contiene 1002, 1009, O1001 y O1002.
  {: .lab-note .output .compact}

- {% include step_label.html %} Consulta los importes iniciales de los pedidos que actualizará el MERGE.

  ```python
  display(spark.table(V).filter(F.col('order_id').isin('O1001','O1002')).select('order_id','amount','status'))
  ```

  > **Salida esperada:** O1001=100.00 y O1002=70.00; ambos con status NEW.
  {: .lab-note .output .compact}

- {% include step_label.html %} Confirma que source_updated_at y created_at existen y tienen tipo timestamp.

  ```python
  display(spark.sql(f'DESCRIBE {V}'))
  ```

  > **Salida esperada:** Aparecen source_updated_at, created_at y updated_at como timestamp.
  {: .lab-note .output .compact}

- {% include step_label.html %} Guarda una consulta de control con el total Silver antes de la carga incremental.

  ```python
  display(spark.table(V).groupBy('batch_id').agg(F.count('*').alias('rows'),F.sum('amount').alias('sales')))
  ```

  > **Salida esperada:** seed_p3=2 y batch_002=2; los importes permiten rastrear los lotes.
  {: .lab-note .output .compact}

{% capture r3 %}{{ results[2] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r3 %}

{% include support-prompt.html task="tarea3" %}

---

## 🔁 Tarea 4. Actualizar Gold y auditar la carga — 15 min

Reconstruir ventas por fecha y canal, comprobar schema enforcement sin alterar Silver y registrar tiempos y conteos del lote en una tabla de auditoría.

### Tarea 4.1. Construir la tabla Gold del laboratorio

Materializar el agregado diario sin mezclarlo con objetos Gold de otros laboratorios.

- {% include step_label.html %} Reconstruye Gold desde Silver con una fila por día y canal.

  ```python
  spark.sql(f'''CREATE OR REPLACE TABLE {G} USING DELTA AS
   SELECT order_date, COALESCE(channel,'unknown') AS channel,
   COUNT(*) AS total_orders, CAST(SUM(amount) AS DECIMAL(16,2)) AS total_sales
   FROM {V} WHERE status <> 'CANCELLED' GROUP BY order_date, COALESCE(channel,'unknown')''')
  ```

  > **Salida esperada:** Se crea p3_gold_daily_sales con columnas order_date, channel y total_sales.
  {: .lab-note .output .compact}

- {% include step_label.html %} Consulta los importes Gold de las fechas 10 y 14 de febrero.

  ```python
  display(spark.table(G).filter(F.col('order_date').isin('2025-02-10','2025-02-14')).orderBy('order_date'))
  ```

  > **Salida esperada:** Las ventas del lote batch_002 son 30.00 y 45.00.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que la suma de Gold coincide con la suma completa de Silver.

  ```python
  gold_total = spark.table(G).agg(F.sum('total_sales')).first()[0]
  silver_total = spark.table(V).filter(F.col('status') != 'CANCELLED').agg(F.sum('amount')).first()[0]
  assert gold_total == silver_total == 245
  print(f'VALIDACIÓN EXITOSA: ventas conciliadas en Silver y Gold = {gold_total:.2f}')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA y las ventas conciliadas en Silver y Gold = 245.00.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que la tabla Gold está persistida como Delta.

  ```python
  assert spark.sql(f'DESCRIBE DETAIL {G}').first()['format']=='delta'
  print('VALIDACIÓN EXITOSA: Gold está almacenada en formato Delta')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA: Gold está almacenada en formato Delta.
  {: .lab-note .output .compact}

- {% include step_label.html %} Verifica que la tabla Gold conserva los pedidos base para el siguiente laboratorio.

  ```python
  display(spark.table(G).orderBy('order_date'))
  ```

  > **Salida esperada:** Aparecen las fechas de enero de los pedidos semilla y las de febrero.
  {: .lab-note .output .compact}

### Tarea 4.2. Probar schema enforcement y registrar auditoría

Comprobar que Delta rechaza un DataFrame incompatible y registrar el resultado sin modificar las filas Silver.

- {% include step_label.html %} Guarda el conteo Silver anterior a la prueba de incompatibilidad.

  ```python
  before=spark.table(V).count()
  print('Silver antes:',before)
  ```

  > **Salida esperada:** Se registra before = 4.
  {: .lab-note .output .compact}

- {% include step_label.html %} Intenta añadir una columna inesperada a Silver y captura el error.

  > **Advertencia:** No habilites mergeSchema: una inserción exitosa indicaría que la prueba no cumplió su propósito.
  {: .lab-note .warning .compact}

  ```python
  bad = spark.createDataFrame([('fake', 'incompatible')], ['order_id', 'unexpected_column'])
  status = 'UNEXPECTED_SUCCESS'
  message = ''
  try:
      bad.write.format('delta').mode('append').option('mergeSchema','false').saveAsTable(V)
  except Exception as err:
      status='EXPECTED_ERROR'; message=str(err)[:300]
  print(status, message)
  if status != 'EXPECTED_ERROR':
      raise AssertionError('La prueba no rechazó el esquema incompatible; revisar Silver')
  ```

  > **Salida esperada:** La operación debe devolver EXPECTED_ERROR.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba que Silver no recibió la fila de prueba.

  ```python
  assert status=='EXPECTED_ERROR'
  assert spark.table(V).count()==before
  assert spark.table(V).filter(F.col('order_id')=='fake').count()==0
  print('VALIDACIÓN EXITOSA: Delta rechazó el esquema incompatible')
  print(f'VALIDACIÓN EXITOSA: Silver conserva sus {before} registros')
  print("VALIDACIÓN EXITOSA: no se insertó el pedido ficticio 'fake'")
  ```

  > **Salida esperada:** Aparecen tres confirmaciones: Delta rechazó el esquema, Silver conserva 4 registros y no existe el pedido fake.
  {: .lab-note .output .compact}

- {% include step_label.html %} Registra los conteos calculados y el control de schema enforcement; reemplaza el único evento del lote en reejecuciones.

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
  audit = spark.createDataFrame(
      [(BATCH, P, started, completed, spark.table(B).count(),
        spark.table(V).filter(F.col('batch_id') == BATCH).count(),
        spark.table(Q).count(), status, 'Carga batch controlada')],
      audit_schema
  )
  audit.write.format('delta').mode('overwrite').saveAsTable(A)
  ```

  > **Salida esperada:** La auditoría registra 10 Bronze, 2 válidos, 8 rechazados y EXPECTED_ERROR.
  {: .lab-note .output .compact}

- {% include step_label.html %} Consulta los valores que se almacenaron en la tabla de auditoría.

  ```python
  display(spark.table(A).filter(F.col('batch_id')==BATCH))
  ```

  > **Salida esperada:** La fila contiene los cuatro indicadores esperados.
  {: .lab-note .output .compact}

{% capture r4 %}{{ results[3] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r4 %}

{% include support-prompt.html task="tarea4" %}

---

## ✅ Tarea 5. Verificar integridad y guardar evidencia — 10 min

Ejecutar controles globales sobre las capas y preservar el historial técnico, dejando los datos base listos para que la Práctica 4 pueda realizar un MERGE reproducible. Cada validación con `assert` incorpora una confirmación visible; si falla, Databricks detiene la celda y muestra el error.

### Tarea 5.1. Verificar conteos y resultados

Confirmar los conteos del lote y la preparación de los registros de ejemplo de la siguiente práctica.

- {% include step_label.html %} Ejecuta las aserciones finales de conteos y motivos de rechazo.

  ```python
  assert spark.table(B).count()==10
  assert spark.table(Q).count()==8
  assert spark.table(V).filter(F.col('batch_id')==BATCH).count()==2
  assert spark.table(V).filter(F.col('batch_id')=='seed_p3').count()==2
  print('VALIDACIÓN EXITOSA: Bronze=10, cuarentena=8, Silver batch_002=2 y semillas=2')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA: Bronze=10, cuarentena=8, Silver batch_002=2 y semillas=2.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba los importes de los dos pedidos válidos del lote batch_002.

  ```python
  display(spark.table(V).filter(F.col('batch_id')==BATCH).select('order_id','amount'))
  ```

  > **Salida esperada:** 1002=30.00 y 1009=45.00.
  {: .lab-note .output .compact}

- {% include step_label.html %} Confirma que se registró exactamente un evento de auditoría para batch_002.

  ```python
  assert spark.table(A).filter(F.col('batch_id')==BATCH).count()==1
  print('VALIDACIÓN EXITOSA: existe exactamente un registro de auditoría para batch_002')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA: existe exactamente un registro de auditoría para batch_002.
  {: .lab-note .output .compact}

- {% include step_label.html %} Verifica que el archivo de entrada continúa disponible para su trazabilidad.

  ```python
  assert any(item.name == 'orders_batch_002_p3.csv' for item in dbutils.fs.ls(LAB_DIR))
  print('VALIDACIÓN EXITOSA: el archivo de origen sigue disponible en el Volume')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA: el archivo de origen sigue disponible en el Volume.
  {: .lab-note .output .compact}

### Tarea 5.2. Consultar historial y conservar recursos

Preservar las transacciones Delta y el estado que utilizará el ejercicio incremental siguiente.

- {% include step_label.html %} Muestra las últimas operaciones Delta de Bronze, Silver y cuarentena.

  ```python
  for name in [B,V,Q]:
   print(name); display(spark.sql(f'DESCRIBE HISTORY {name} LIMIT 3'))
  ```

  > **Salida esperada:** Se observan operaciones WRITE de las tablas.
  {: .lab-note .output .compact}

- {% include step_label.html %} Verifica que el cómputo y las tablas p3_ continúan accesibles.

  ```python
  for name in [B,V,G,Q,A]:
   assert spark.catalog.tableExists(name)
  print('Recursos listos para Práctica 4')
  print('VALIDACIÓN EXITOSA: las cinco tablas p3_ siguen disponibles para la Práctica 4')
  ```

  > **Salida esperada:** Aparece VALIDACIÓN EXITOSA: las cinco tablas p3_ siguen disponibles para la Práctica 4.
  {: .lab-note .output .compact}

- {% include step_label.html %} Guarda el notebook y evita ejecutar limpieza sobre las tablas p3_.

  > **Importante:** No ejecutes VACUUM ni DROP: conserva la evidencia Delta y las dos semillas O1001/O1002 para la Práctica 4.
  {: .lab-note .important .compact}

  > **Salida esperada:** El notebook queda guardado; los datos pueden reutilizarse en la Práctica 4.
  {: .lab-note .output .compact}

{% capture r5 %}{{ results[4] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r5 %}

{% include support-prompt.html task="tarea5" %}

