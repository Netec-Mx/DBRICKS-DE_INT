---
layout: lab
title: "Práctica 2: Implementación inicial de arquitectura Bronze/Silver"
permalink: /lab2/lab2/
images_base: /labs/lab2/img
duration: "40 minutos"
objective:
  - Implementar una tabla Delta Bronze con datos originales y metadatos de trazabilidad.
  - Construir una tabla Silver tipada, con identificadores válidos y pedidos deduplicados.
  - Calcular importes por pedido y producir una agregación Gold básica de ventas diarias.
  - Validar conteos, tipos, trazabilidad e historial Delta en Unity Catalog.
prerequisites:
  - Workspace de Databricks con Unity Catalog habilitado y Databricks Runtime 15.4 LTS o equivalente compatible.
  - Cluster All-Purpose Single node creado en la Práctica 1 (lab1-etl-<usuario>), Runtime 15.4 LTS y modo Standard; permiso para iniciarlo si está detenido.
  - Catálogo dbricks_de_int existente y accesible; esquema batch1 creado por el notebook si no existe.
  - Permisos USE CATALOG, CREATE SCHEMA (si hace falta), USE SCHEMA, CREATE VOLUME (si hace falta), CREATE TABLE, READ VOLUME y WRITE VOLUME.
  - Conocimientos básicos de notebooks, SQL, PySpark DataFrames, tipos de datos y claves de negocio.
introduction:
  - En esta práctica independiente crearás una arquitectura inicial Medallion. Prepararás un lote CSV controlado, conservarás su contenido original y metadatos en Bronze, convertirás y deduplicarás pedidos en Silver y calcularás un agregado diario Gold. Comprobarás los resultados con SQL y el historial Delta en Unity Catalog.
slug: lab2
lab_number: 2
final_result: >
  Se obtienen las tablas administradas dbricks_de_int.batch1.bronze_orders (7 filas de entrada),
  dbricks_de_int.batch1.silver_orders (5 pedidos válidos y únicos) y
  dbricks_de_int.batch1.gold_daily_sales (2 fechas, con ventas totales de 330.00),
  con trazabilidad del lote batch_001 e historial Delta consultable.
notes:
  - "Se reutiliza el cluster de la Práctica 1; no crees otro. Si todavía no lo tienes, solicita acceso al cluster autorizado del curso."
  - "Se crea o actualiza únicamente el CSV de ejemplo en la subcarpeta landing/lab2_bronze_silver; no se tocan otros archivos."
  - "Usa un esquema exclusivo del participante autorizado por el instructor: overwrite reemplaza las tres tablas. Cambia SCHEMA en la primera celda si aplica; las celdas SQL también deben reflejarlo."
  - "Los 40 minutos suponen el cluster Running y permisos de Unity Catalog. El notebook crea esquema y volumen si faltan; no elimina recursos."
references:
  - text: Databricks Runtime 15.4 LTS
    url: https://docs.databricks.com/en/release-notes/runtime/15.4lts.html
  - text: Tablas administradas de Unity Catalog
    url: https://docs.databricks.com/en/tables/managed.html
  - text: Arquitectura Medallion
    url: https://docs.databricks.com/en/lakehouse/medallion.html
  - text: Delta Lake en Databricks
    url: https://docs.databricks.com/en/delta/index.html
prev: /lab1/lab1/
next: /lab3/lab3/
---

<!-- Estructura Jekyll: cuatro tareas dentro de 40 minutos. Un step_label por acción verificable. -->


---

## 🔎 Tarea 1. Verificar el entorno y los datos de origen — 7 min

Reutilizar el cluster de la Práctica 1, crear un notebook y comprobar el catálogo, esquema, volumen y CSV antes de crear tablas. El conjunto de datos controlado permite validar los resultados de esta práctica sin depender de otros laboratorios.


### Tarea 1.1. Preparar el notebook y el contexto de Unity Catalog

Abre tu carpeta existente batch_1, crea el notebook 02 y conecta el mismo cluster usado en lab1. Después, establece una sola vez las rutas y nombres de las tablas.


- {% include step_label.html %} En la barra lateral izquierda, entra a **Workspace > Home** o **Workspace > Users > tu usuario** y abre **batch_1** (creada en la Práctica 1).

  > **Nota:** Si **batch_1** no existe, en tu carpeta personal selecciona **Create > Folder**, escribe `batch_1` y confirma **Create**. No crees otra carpeta si ya existe.
  {: .lab-note .info .compact}

  > **Salida esperada:** La carpeta batch_1 aparece bajo Workspace > Users > tu usuario.
  {: .lab-note .output .compact}


- {% include step_label.html %} Dentro de **batch_1**, haz clic en **Create > Notebook**; escribe `02_bronze_silver_orders`, selecciona **Python** y confirma **Create**.

  > **Salida esperada:** El notebook se muestra en la carpeta y su lenguaje predeterminado es Python.
  {: .lab-note .output .compact}


- {% include step_label.html %} En la parte superior del notebook, abre el selector **Connect** o **Compute** y selecciona `lab1-etl-<usuario>` (el mismo de lab1).

  > **Nota:** No selecciones **Create compute**. Si el cluster está **Terminated**, ábrelo desde **Compute > All-purpose compute**, pulsa **Start**, espera **Running** y vuelve a conectarlo. Si aparece el aviso de cuota CPU, usa el cluster autorizado del instructor.
  {: .lab-note .info .compact}

  > **Salida esperada:** El notebook indica que está conectado al clúster disponible.
  {: .lab-note .output .compact}


- {% include step_label.html %} En la primera celda **Python**, pega y ejecuta el bloque completo. Verifica catálogo, crea esquema y volumen antes de acceder a la ruta.

  ```python
  from pyspark.sql import functions as F
  from pyspark.sql.window import Window
    
  CATALOG = "dbricks_de_int"
  SCHEMA = "batch1"

  # Utiliza el catálogo real del workspace de la Práctica 1.
  catalogs = {row[0] for row in spark.sql("SHOW CATALOGS").collect()}
  if CATALOG not in catalogs:
      raise ValueError(
          f"El catálogo {CATALOG} no está disponible. "
          "Confirma su nombre en Catalog Explorer y solicita acceso."
      )

  BATCH_ID = "batch_001"
  LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
  LAB2_PATH = f"{LANDING_PATH}/lab2_bronze_silver"
  SOURCE_FILE = f"{LAB2_PATH}/orders_{BATCH_ID}.csv"
  BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"
  SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_orders"
  GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_daily_sales"
  spark.sql(f"USE CATALOG `{CATALOG}`")
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
  spark.sql(f"USE SCHEMA `{SCHEMA}`")
  spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`landing`")
  print(f"Contexto: {CATALOG}.{SCHEMA}")
  ```

  > **Salida esperada:** Se imprime `Contexto: dbricks_de_int.batch1`; el volumen `landing` ya existe o se acaba de crear. Si faltan permisos, solicita `CREATE VOLUME` al instructor antes de continuar.
  {: .lab-note .output .compact}


### Tarea 1.2. Preparar y comprobar el archivo de origen

Comprobar el volumen creado en el paso anterior, generar un lote exclusivo de esta práctica y validar su estructura antes de transformar.


- {% include step_label.html %} Debajo de la celda de constantes, pulsa **+ Code**, pega el comando y presiona **Shift+Enter** para listar el volumen `landing`.

  ```python
  display(dbutils.fs.ls(LANDING_PATH))
  ```

  > **Salida esperada:** La lista del volumen aparece sin error `UC_VOLUME_NOT_FOUND`. Puede estar vacía.
  {: .lab-note .output .compact}


- {% include step_label.html %} Agrega una celda **Python**; crea la subcarpeta exclusiva y escribe el CSV de muestra completo para una ejecución reproducible.

  > **Importante:** Solo se reemplaza `landing/lab2_bronze_silver/orders_batch_001.csv`, archivo propiedad de esta práctica. No uses esa ruta para datos reales.
  {: .lab-note .important .compact}

  ```python
  csv_content = """order_id,order_ts,quantity,unit_price
  1001,2025-01-15 09:00:00,1,100.00
  1001,2025-01-15 11:00:00,2,60.00
  1002,2025-01-15 12:00:00,1,80.00
  1003,2025-01-16 08:00:00,3,15.00
  1004,2025-01-16 10:00:00,2,30.00
  ,2025-01-16 11:00:00,1,999.00
  1005,2025-01-16 13:00:00,1,25.00
  """
  dbutils.fs.mkdirs(LAB2_PATH)
  dbutils.fs.put(SOURCE_FILE, csv_content, overwrite=True)
  print("CSV controlado listo:", SOURCE_FILE)
  ```

  > **Salida esperada:** `Wrote ... bytes` y `CSV controlado listo` en la subcarpeta exclusiva de lab2.
  {: .lab-note .output .compact}


- {% include step_label.html %} Debajo, agrega una celda Python, pega `spark.read...csv(SOURCE_FILE)` y ejecútala; usa `display()` para revisar el contenido.

  ```python
  raw_df = spark.read.option("header", True).option("inferSchema", False).csv(SOURCE_FILE)
  display(raw_df.limit(10))
  ```

  > **Salida esperada:** Se muestran 7 filas y las columnas `order_id`, `order_ts`, `quantity` y `unit_price`.
  {: .lab-note .output .compact}


- {% include step_label.html %} En otra celda Python, ejecuta la validación de columnas, `printSchema()` y `count()`; detente si falla el `assert`.

  > **Advertencia:** Si la validación falla, revisa que hayas ejecutado todas las celdas en orden y que `SOURCE_FILE` apunte a la subcarpeta de lab2.
  {: .lab-note .warning .compact}

  ```python
  required = {"order_id", "order_ts", "quantity", "unit_price"}
  assert set(raw_df.columns) == required, f"Columnas inesperadas: {raw_df.columns}"
  assert raw_df.count() == 7, "El CSV de práctica debe contener 7 filas"
  raw_df.printSchema()
  print("Filas en origen:", raw_df.count())
  ```

  > **Salida esperada:** El esquema muestra cuatro campos string. Para el CSV controlado, el conteo es 7.
  {: .lab-note .output .compact}


{% assign results = site.data.task-results[page.slug].results %}


{% capture r1 %}{{ results[0] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r1 %}

{% include support-prompt.html task="tarea1" %}


---

## 🟤 Tarea 2. Implementar la capa Bronze — 10 min

Ingerir el CSV como texto y agregar metadatos técnicos de origen, momento de carga e identificador de lote. Guardar el resultado en una tabla Delta administrada y comprobar que se preservaron los datos originales sin aplicar limpieza de negocio.


### Tarea 2.1. Leer el archivo y agregar metadatos técnicos

Construir un DataFrame Bronze que conserve las columnas de origen y añada tres columnas técnicas necesarias para trazabilidad y auditoría básica.


- {% include step_label.html %} En una celda Python debajo de la validación, pega la lectura de Bronze y ejecútala sin `inferSchema`.

  ```python
  bronze_source_df = (
      spark.read.option("header", True)
      .option("inferSchema", False)
      .csv(SOURCE_FILE)
  )
  ```

  > **Salida esperada:** bronze_source_df queda definido sin convertir valores de negocio.
  {: .lab-note .output .compact}


- {% include step_label.html %} En la siguiente celda Python, incorpora la ruta del archivo como `source_file`, definida por `SOURCE_FILE` (esta lectura usa un solo archivo).

  ```python
  bronze_with_source_df = bronze_source_df.withColumn("source_file", F.lit(SOURCE_FILE))
  ```

  > **Salida esperada:** El DataFrame incorpora la columna string source_file con la ruta de entrada.
  {: .lab-note .output .compact}


- {% include step_label.html %} En una nueva celda Python, agrega `ingest_ts` para identificar el instante de ingesta; ejecútala.

  ```python
  bronze_with_time_df = bronze_with_source_df.withColumn("ingest_ts", F.current_timestamp())
  ```

  > **Salida esperada:** El DataFrame añade una columna timestamp llamada ingest_ts.
  {: .lab-note .output .compact}


- {% include step_label.html %} En la celda siguiente, agrega `batch_id` usando la constante `BATCH_ID` y ejecútala.

  ```python
  bronze_df = bronze_with_time_df.withColumn("batch_id", F.lit(BATCH_ID))
  ```

  > **Salida esperada:** La columna batch_id contiene batch_001 en todas las filas.
  {: .lab-note .output .compact}


- {% include step_label.html %} En otra celda Python, ejecuta `printSchema()` y `display()`; comprueba las cuatro columnas originales y las tres técnicas.

  ```python
  bronze_df.printSchema()
  display(bronze_df.limit(10))
  ```

  > **Salida esperada:** Las cuatro columnas originales permanecen como string y aparecen source_file, ingest_ts y batch_id.
  {: .lab-note .output .compact}


### Tarea 2.2. Crear y verificar la tabla Delta Bronze

Persistir Bronze como tabla administrada de Unity Catalog, consultar una muestra y comprobar el número de registros y la identidad del lote.


- {% include step_label.html %} En una celda Python nueva, ejecuta el bloque `bronze_df.write` para crear `bronze_orders` como tabla Delta administrada.

  > **Advertencia:** mode("overwrite") reemplaza el contenido de la tabla indicada; usa un esquema exclusivo si el workspace es compartido.
  {: .lab-note .warning .compact}

  ```python
  (
      bronze_df.write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(BRONZE_TABLE)
  )
  ```

  > **Salida esperada:** La escritura termina sin errores y se crea dbricks_de_int.batch1.bronze_orders.
  {: .lab-note .output .compact}


- {% include step_label.html %} Agrega una celda debajo, escribe `%sql` en la primera línea, pega el `SELECT` y ejecútalo con **Shift+Enter**.

  ```sql
  %sql
  SELECT order_id, order_ts, quantity, unit_price, source_file, ingest_ts, batch_id
  FROM dbricks_de_int.batch1.bronze_orders
  LIMIT 10;
  ```

  > **Salida esperada:** Se observan las columnas originales y las tres columnas técnicas en la tabla.
  {: .lab-note .output .compact}


- {% include step_label.html %} Agrega otra celda con `%sql` y ejecuta la consulta de conteos de archivos, lotes y filas.

  ```sql
  %sql
  SELECT COUNT(*) AS total_filas,
         COUNT(DISTINCT source_file) AS archivos,
         COUNT(DISTINCT batch_id) AS lotes
  FROM dbricks_de_int.batch1.bronze_orders;
  ```

  > **Salida esperada:** `total_filas=7`, `archivos=1`, `lotes=1`.
  {: .lab-note .output .compact}


- {% include step_label.html %} En otra celda `%sql`, consulta `batch_id` y `source_file`; comprueba el sufijo `orders_batch_001.csv`.

  ```sql
  %sql
  SELECT DISTINCT batch_id, source_file
  FROM dbricks_de_int.batch1.bronze_orders;
  ```

  > **Salida esperada:** Se muestra un único lote batch_001 y la ruta del CSV utilizado.
  {: .lab-note .output .compact}


{% capture r2 %}{{ results[1] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r2 %}

{% include support-prompt.html task="tarea2" %}


---

## 🔧 Tarea 3. Implementar la capa Silver — 15 min

Estandarizar los campos del lote, eliminar pedidos sin identificador y elegir una fila por order_id mediante una ventana ordenada por fecha. Calcular el importe de cada pedido y persistir una tabla Silver con tipos explícitos y metadatos de origen.


### Tarea 3.1. Estandarizar datos y convertir tipos

Leer Bronze, limpiar espacios y convertir fechas y valores numéricos para que Silver pueda utilizarse en consultas y cálculos de negocio.


- {% include step_label.html %} Vuelve a **+ Code** y crea una celda Python debajo de las consultas; ejecuta `spark.table(BRONZE_TABLE)`.

  ```python
  bronze_input_df = spark.table(BRONZE_TABLE)
  ```

  > **Salida esperada:** bronze_input_df apunta a la tabla Bronze persistida.
  {: .lab-note .output .compact}


- {% include step_label.html %} En otra celda Python, ejecuta `trim()` sobre `order_id` y guarda el resultado en `order_id_clean`.

  ```python
  silver_work_df = bronze_input_df.withColumn("order_id_clean", F.trim(F.col("order_id")))
  ```

  > **Salida esperada:** Los identificadores con espacios se normalizan sin alterar Bronze.
  {: .lab-note .output .compact}


- {% include step_label.html %} En la siguiente celda Python, ejecuta `to_timestamp()` con el patrón `yyyy-MM-dd HH:mm:ss`.

  ```python
  silver_work_df = silver_work_df.withColumn(
      "order_ts_parsed", F.to_timestamp(F.trim(F.col("order_ts")), "yyyy-MM-dd HH:mm:ss")
  )
  ```

  > **Salida esperada:** La nueva columna order_ts_parsed tiene tipo timestamp.
  {: .lab-note .output .compact}


- {% include step_label.html %} Agrega una celda Python y convierte `quantity` a `int` y `unit_price` a `decimal(12,2)`; ejecútala.

  ```python
  silver_work_df = (
      silver_work_df
      .withColumn("quantity_parsed", F.trim(F.col("quantity")).cast("int"))
      .withColumn("unit_price_parsed", F.trim(F.col("unit_price")).cast("decimal(12,2)"))
  )
  ```

  > **Salida esperada:** Los campos derivados tienen tipos integer y decimal(12,2).
  {: .lab-note .output .compact}


- {% include step_label.html %} En otra celda Python, ejecuta `printSchema()` sobre las cuatro columnas derivadas y revisa sus tipos.

  ```python
  silver_work_df.select(
      "order_id_clean", "order_ts_parsed", "quantity_parsed", "unit_price_parsed"
  ).printSchema()
  ```

  > **Salida esperada:** Se muestran los tipos string, timestamp, integer y decimal(12,2), respectivamente.
  {: .lab-note .output .compact}


### Tarea 3.2. Excluir identificadores inválidos y deduplicar

Conservar los pedidos con identificadores válidos y resolver claves repetidas mediante fecha descendente y un criterio de desempate estable.


- {% include step_label.html %} En la siguiente celda Python, elimina los pedidos cuyo identificador normalizado sea nulo o vacío.

  ```python
  silver_valid_df = silver_work_df.filter(
      F.col("order_id_clean").isNotNull() & (F.length("order_id_clean") > 0)
  )
  ```

  > **Salida esperada:** Para el CSV controlado quedan 6 filas: se excluye la fila de identificador vacío.
  {: .lab-note .output .compact}


- {% include step_label.html %} En otra celda Python, calcula `dedupe_hash` como criterio técnico de desempate reproducible.

  > **Nota:** El hash es un criterio reproducible de desempate, no sustituye una regla de negocio de actualización.
  {: .lab-note .info .compact}

  ```python
  silver_valid_df = silver_valid_df.withColumn(
      "dedupe_hash", F.sha2(F.concat_ws("||",
          F.coalesce(F.col("order_id"), F.lit("")),
          F.coalesce(F.col("order_ts"), F.lit("")),
          F.coalesce(F.col("quantity"), F.lit("")),
          F.coalesce(F.col("unit_price"), F.lit("")),
          F.coalesce(F.col("source_file"), F.lit("")),
          F.col("batch_id")
      ), 256)
  )
  ```

  > **Salida esperada:** Cada fila válida contiene un valor dedupe_hash.
  {: .lab-note .output .compact}


- {% include step_label.html %} En una nueva celda Python, define `Window.partitionBy("order_id_clean")` y ordena por fecha descendente y hash.

  ```python
  dedupe_window = Window.partitionBy("order_id_clean").orderBy(
      F.col("order_ts_parsed").desc_nulls_last(),
      F.col("dedupe_hash").desc()
  )
  ```

  > **Salida esperada:** dedupe_window define un orden reproducible para la selección de pedidos.
  {: .lab-note .output .compact}


- {% include step_label.html %} Debajo, agrega una celda Python, calcula `row_number()` y filtra `row_num == 1`; ejecútala.

  ```python
  silver_unique_df = (
      silver_valid_df
      .withColumn("row_num", F.row_number().over(dedupe_window))
      .filter(F.col("row_num") == 1)
  )
  ```

  > **Salida esperada:** El lote controlado queda con 5 identificadores únicos; 1001 conserva su registro más reciente.
  {: .lab-note .output .compact}


- {% include step_label.html %} En otra celda Python, ejecuta `count()` y `display()`; confirma que el pedido 1001 usa el registro de las 11:00.

  ```python
  assert silver_unique_df.count() == 5, "Se esperaban 5 pedidos únicos"
  print("Pedidos únicos:", silver_unique_df.count())
  display(silver_unique_df.select("order_id_clean", "order_ts_parsed"))
  ```

  > **Salida esperada:** Se muestran 5 pedidos únicos en el CSV controlado.
  {: .lab-note .output .compact}


### Tarea 3.3. Calcular importes y guardar la tabla Silver

Conservar los campos tipados, calcular quantity por unit_price y crear una tabla administrada con un registro por pedido válido.


- {% include step_label.html %} Agrega una celda Python para seleccionar las columnas finales; calcula `total_amount` multiplicando cantidad por precio.

  ```python
  silver_df = silver_unique_df.select(
      F.col("order_id_clean").alias("order_id"),
      F.col("order_ts_parsed").alias("order_ts"),
      F.col("quantity_parsed").alias("quantity"),
      F.col("unit_price_parsed").alias("unit_price"),
      (F.col("quantity_parsed") * F.col("unit_price_parsed"))
          .cast("decimal(14,2)").alias("total_amount"),
      "source_file", "ingest_ts", "batch_id"
  )
  ```

  > **Salida esperada:** silver_df incluye total_amount decimal(14,2) y conserva los metadatos técnicos.
  {: .lab-note .output .compact}


- {% include step_label.html %} En otra celda Python, ejecuta el bloque `silver_df.write` para persistir `silver_orders` en Delta.

  > **Importante:** La sobrescritura es apropiada para este lote controlado; las cargas incrementales y MERGE se verán posteriormente.
  {: .lab-note .important .compact}

  ```python
  (
      silver_df.write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(SILVER_TABLE)
  )
  ```

  > **Salida esperada:** Se crea dbricks_de_int.batch1.silver_orders sin errores.
  {: .lab-note .output .compact}


- {% include step_label.html %} Agrega una celda con `%sql`, pega el `SELECT` de Silver y ejecútalo; compara los cinco importes.

  ```sql
  %sql
  SELECT order_id, order_ts, quantity, unit_price, total_amount
  FROM dbricks_de_int.batch1.silver_orders
  ORDER BY order_id;
  ```

  > **Salida esperada:** Para el CSV controlado, aparecen 5 pedidos y los importes 120.00, 80.00, 45.00, 60.00 y 25.00.
  {: .lab-note .output .compact}


- {% include step_label.html %} En otra celda `%sql`, ejecuta los indicadores de calidad y comprueba que no haya IDs inválidos ni importes nulos.

  ```sql
  %sql
  SELECT COUNT(*) AS filas,
         COUNT(DISTINCT order_id) AS pedidos_unicos,
         SUM(CASE WHEN order_id IS NULL OR trim(order_id) = '' THEN 1 ELSE 0 END) AS ids_invalidos,
         SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) AS importes_nulos
  FROM dbricks_de_int.batch1.silver_orders;
  ```

  > **Salida esperada:** Con el CSV controlado: filas=5, pedidos_unicos=5, ids_invalidos=0 e importes_nulos=0.
  {: .lab-note .output .compact}


{% capture r3 %}{{ results[2] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r3 %}

{% include support-prompt.html task="tarea3" %}


---

## ✅ Tarea 4. Generar Gold y validar la arquitectura — 8 min

Crear una agregación diaria Gold a partir de Silver y comprobar las propiedades más relevantes de la solución inicial: importes coherentes, tablas Delta gobernadas, metadatos técnicos e historial de operaciones para posteriores laboratorios.


### Tarea 4.1. Generar ventas diarias en Gold

Agrupar los pedidos por fecha, calcular cantidades y ventas, y guardar un resultado analítico básico para verificar la transformación.


- {% include step_label.html %} Agrega una celda Python debajo de la validación de Silver y ejecuta el `groupBy` para calcular ventas diarias.

  ```python
  gold_df = (
      spark.table(SILVER_TABLE)
      .filter(F.col("order_ts").isNotNull())
      .withColumn("order_date", F.to_date("order_ts"))
      .groupBy("order_date")
      .agg(
          F.countDistinct("order_id").alias("total_orders"),
          F.sum("quantity").cast("bigint").alias("total_units"),
          F.sum("total_amount").cast("decimal(16,2)").alias("daily_sales_amount")
      )
  )
  ```

  > **Salida esperada:** gold_df contiene una fila por fecha válida de pedido.
  {: .lab-note .output .compact}


- {% include step_label.html %} En la celda Python siguiente, ejecuta `gold_df.write` para crear `gold_daily_sales` como tabla Delta.

  ```python
  (
      gold_df.write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(GOLD_TABLE)
  )
  ```

  > **Salida esperada:** Se crea la tabla dbricks_de_int.batch1.gold_daily_sales.
  {: .lab-note .output .compact}


- {% include step_label.html %} Agrega una celda `%sql`, ejecuta la consulta de Gold y compara pedidos, unidades y ventas por día.

  ```sql
  %sql
  SELECT order_date, total_orders, total_units, daily_sales_amount
  FROM dbricks_de_int.batch1.gold_daily_sales
  ORDER BY order_date;
  ```

  > **Salida esperada:** 2025-01-15: 2 pedidos, 3 unidades, 200.00; 2025-01-16: 3 pedidos, 6 unidades, 130.00.
  {: .lab-note .output .compact}


### Tarea 4.2. Verificar integridad de las capas

Comprobar los conteos y la conciliación de importes entre Silver y Gold, además de la presencia de metadatos de procedencia.


- {% include step_label.html %} Agrega una nueva celda `%sql` y ejecuta el `UNION ALL` para comparar los conteos de las tres capas.

  ```sql
  %sql
  SELECT 'Bronze' AS capa, COUNT(*) AS filas FROM dbricks_de_int.batch1.bronze_orders
  UNION ALL
  SELECT 'Silver', COUNT(*) FROM dbricks_de_int.batch1.silver_orders
  UNION ALL
  SELECT 'Gold', COUNT(*) FROM dbricks_de_int.batch1.gold_daily_sales;
  ```

  > **Salida esperada:** Con el CSV controlado: Bronze=7, Silver=5 y Gold=2.
  {: .lab-note .output .compact}


- {% include step_label.html %} Agrega una celda `%sql` y ejecuta la conciliación: las ventas totales de Silver y Gold deben coincidir.

  ```sql
  %sql
  SELECT
   (SELECT SUM(total_amount) FROM dbricks_de_int.batch1.silver_orders) AS ventas_silver,
   (SELECT SUM(daily_sales_amount) FROM dbricks_de_int.batch1.gold_daily_sales) AS ventas_gold;
  ```

  > **Salida esperada:** Los dos valores son 330.00 para el CSV controlado.
  {: .lab-note .output .compact}


- {% include step_label.html %} En otra celda `%sql`, agrupa por `batch_id` y confirma que las cinco filas Silver provienen del mismo CSV.

  ```sql
  %sql
  SELECT batch_id, COUNT(DISTINCT source_file) AS archivos, COUNT(*) AS pedidos
  FROM dbricks_de_int.batch1.silver_orders
  GROUP BY batch_id;
  ```

  > **Salida esperada:** Se muestra batch_001, un archivo y 5 pedidos.
  {: .lab-note .output .compact}


### Tarea 4.3. Comprobar historial Delta y finalizar

Verificar el formato e historial transaccional de las tablas y conservar los objetos creados para las siguientes actividades del curso.


- {% include step_label.html %} En una celda `%sql`, ejecuta `DESCRIBE DETAIL` sobre Bronze; repite en dos celdas nuevas con Silver y Gold.

  ```sql
  %sql
  DESCRIBE DETAIL dbricks_de_int.batch1.bronze_orders;
  ```

  > **Salida esperada:** La columna format muestra delta. Ejecuta el mismo comando con silver_orders y gold_daily_sales para verificar ambas.
  {: .lab-note .output .compact}


- {% include step_label.html %} En una celda `%sql`, ejecuta `DESCRIBE HISTORY` de Bronze. Verifica que el notebook se guardó automáticamente en `batch_1`.

  > **Nota:** No elimines los datos del volumen ni las tablas; se reutilizarán al estudiar cargas posteriores. Para Silver y Gold puedes repetir DESCRIBE HISTORY.
  {: .lab-note .info .compact}

  ```sql
  %sql
  DESCRIBE HISTORY dbricks_de_int.batch1.bronze_orders;
  ```

  > **Salida esperada:** Aparece al menos una versión de Bronze. El notebook queda guardado y las tres tablas continúan disponibles.
  {: .lab-note .output .compact}


{% capture r4 %}{{ results[3] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r4 %}

{% include support-prompt.html task="tarea4" %}