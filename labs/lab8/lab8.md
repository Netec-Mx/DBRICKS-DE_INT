---
layout: lab
title: 'Práctica 8: Creación de pipeline Bronze → Silver con DLT'
permalink: /lab8/lab8/
images_base: /labs/lab8/img
duration: 60 minutos
objective:
- Crear una streaming table Bronze y una materialized view Silver con código SQL Lakeflow.
- Verificar ocho filas Bronze, seis pedidos Silver y un total de 220.00.
- Interpretar dependencias, actualización y event log.
prerequisites:
- Acceso a Databricks Workflows/Lakeflow y Unity Catalog.
- Permisos READ VOLUME y WRITE VOLUME en dbricks_de_int.batch1.landing; USE CATALOG, USE SCHEMA y CREATE TABLE en batch1.
- Cluster All-Purpose existente para preparar el CSV; cómputo Lakeflow autorizado para ejecutar el pipeline.
introduction:
- Prepararás un CSV en un Volume administrado y crearás desde cero un ETL pipeline en modo Triggered. Bronze conservará ocho filas y Silver publicará seis pedidos únicos. En cada paso se indica el editor, archivo o celda donde trabajar.
slug: lab8
lab_number: 8
final_result: Pipeline pipeline_orders_bronze_silver_b2 ejecutado; p8_orders_bronze con ocho filas y p8_orders_silver con
  seis pedidos e importe 220.00.
notes:
- 'Los bloques Python de preparación se ejecutan en 08_preparar_origen; el SQL declarativo se edita en transformations/bronze_silver_orders.sql desde Edit pipeline, no en SQL Warehouse.'
- Los nombres de tablas p8_ son exclusivos y no alteran Prácticas 1–7; no sobrescribas archivos ni tablas de otras prácticas.
- 'No eliminar el pipeline: la Práctica 9 reutiliza el resultado de la Práctica 8.'
- 'El pipeline se ejecuta con identidad Run as y permisos propios; el cluster All-Purpose solo prepara/verifica el origen.'
references:
- text: Lakeflow SQL
  url: https://docs.databricks.com/aws/en/ldp/developer/sql-dev
- text: Expectations
  url: https://docs.databricks.com/aws/en/ldp/expectations
- text: Pipeline event log
  url: https://docs.databricks.com/aws/en/ldp/monitor-event-logs
- text: read_files Auto Loader
  url: https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files
prev: /lab7/lab7/
next: /lab9/lab9/
---

<!-- 5 tareas, 38 pasos, 60 minutos. Creación desde cero del ETL pipeline. -->

---

## 🔎 Tarea 1. Preparar el origen y comprobar permisos — 10 min

Preparar un conjunto CSV reproducible de ocho registros en un directorio exclusivo y comprobar el acceso al catálogo y al volumen administrado.

### Tarea 1.1. Verificar recursos de Unity Catalog

Usar recursos existentes sin modificar tablas de prácticas anteriores.

- {% include step_label.html %} **Abre** Workspace y localiza tu carpeta personal; comprueba que puedes crear una subcarpeta.

  > **Salida esperada:** Aparece tu directorio /Workspace/Users/<usuario> y está habilitada la creación de archivos.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Workspace > Home > batch_1**, selecciona **Create > Notebook**, nómbralo `08_preparar_origen`, elige **Python** y conéctalo al **mismo cluster All-Purpose** de las Prácticas 1–5 (no crees otro).

  > **Nota:** El cómputo para preparar el CSV es independiente del cómputo administrado por Lakeflow.
  {: .lab-note .info .compact}

  > **Salida esperada:** La sesión permite ejecutar una celda Python sin error de conexión.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `08_preparar_origen`, **celda 1 (Python)**, ejecuta este bloque para comprobar catálogo, esquema, Volume `landing` y la ruta completa antes de escribir archivos.

  ```python
  v=spark.sql('SHOW VOLUMES IN dbricks_de_int.batch1')
  display(v)
  assert v.filter("volume_name = 'landing'").count()==1
  print('Volumen encontrado: dbricks_de_int.batch1.landing')
  ```

  > **Salida esperada:** Se imprime la validación del catálogo, esquema y Volume landing; un error de permisos detiene la ejecución.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `08_preparar_origen`, **celda 2 (Python)**, debajo de la celda 1, define la ruta exclusiva del pipeline y confirma que comienza en el Volume `landing`.

  ```python
  INPUT='/Volumes/dbricks_de_int/batch1/landing/p8_approved'
  print('Directorio de entrada:',INPUT)
  assert INPUT.endswith('/p8_approved')
  ```

  > **Salida esperada:** Se imprime `VALIDACIÓN: directorio de P8=/Volumes/dbricks_de_int/batch1/landing/p8_approved`.
  {: .lab-note .output .compact}

### Tarea 1.2. Crear y verificar el CSV controlado

Crear ocho filas con seis claves únicas y versiones reproducibles para dos pedidos.

- {% include step_label.html %} En `08_preparar_origen`, **celda 3 (Python)**, pega las ocho filas de ejemplo y ejecútalas; identifica `P8002` y `P8004` como IDs con dos versiones.

  ```python
  import csv,io
  rows=[['P8001','C801','2026-09-01','COMPLETED','2','10.00','2026-09-01 08:00:00','1'],
  ['P8002','C802','2026-09-01','PENDING','1','20.00','2026-09-01 09:00:00','1'],
  ['P8002','C802','2026-09-01','COMPLETED','2','20.00','2026-09-01 10:00:00','2'],
  ['P8003','C803','2026-09-02','COMPLETED','3','15.00','2026-09-02 08:00:00','1'],
  ['P8004','C804','2026-09-02','PENDING','1','30.00','2026-09-02 09:00:00','1'],
  ['P8004','C804','2026-09-02','COMPLETED','2','30.00','2026-09-02 10:00:00','2'],
  ['P8005','C805','2026-09-03','CANCELLED','1','5.00','2026-09-03 08:00:00','1'],
  ['P8006','C806','2026-09-03','COMPLETED','4','12.50','2026-09-03 09:00:00','1']]
  assert len(rows)==8 and len({r[0] for r in rows})==6
  print('VALIDACIÓN: filas_origen=8; pedidos_unicos=6; claves_repetidas=P8002,P8004')
  ```

  > **Salida esperada:** Las claves P8002 y P8004 aparecen dos veces; hay seis claves distintas.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `08_preparar_origen`, **celda 4 (Python)**, justo debajo de las filas, crea el CSV en memoria con sus ocho columnas y verifica el encabezado.

  ```python
  headers=['order_id','customer_id','order_date','status','quantity','unit_price','source_updated_at','update_sequence']
  buf=io.StringIO(); writer=csv.writer(buf); writer.writerow(headers);writer.writerows(rows)
  assert len(buf.getvalue().splitlines())==9
  print('VALIDACIÓN: csv_columnas=8; cabeceras=1; registros=8')
  ```

  > **Salida esperada:** El encabezado incluye order_id, status, quantity, unit_price, source_updated_at y update_sequence.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `08_preparar_origen`, **celda 5 (Python)**, crea `landing/p8_approved` y escribe `orders_p8_baseline.csv`; verifica la ruta antes de continuar.

  ```python
  dbutils.fs.mkdirs(INPUT)
  BASEFILE=f'{INPUT}/orders_p8_baseline.csv'
  dbutils.fs.put(BASEFILE,buf.getvalue(),overwrite=True)
  assert any(f.name == 'orders_p8_baseline.csv' for f in dbutils.fs.ls(INPUT))
  print('VALIDACIÓN: archivo_escrito=', BASEFILE)
  ```

  > **Salida esperada:** Se crea orders_p8_baseline.csv con nueve líneas físicas.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `08_preparar_origen`, **celda 6 (Python)**, lee `BASEFILE` con Spark y comprueba ocho filas, ocho columnas y las cuatro filas de `P8002`/`P8004`.

  ```python
  test=spark.read.option('header',True).option('inferSchema',False).csv(BASEFILE)
  assert test.count()==8 and len(test.columns)==8
  assert test.filter("order_id IN ('P8002','P8004')").count()==4
  print('Archivo verificado: 8 filas, 8 columnas, 4 filas de claves duplicadas')
  ```

  > **Salida esperada:** La lectura devuelve exactamente 8 filas y 8 columnas; P8002 y P8004 aparecen dos veces.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `08_preparar_origen`, **celda 7 (Python)**, calcula el importe con la última secuencia de cada pedido y verifica seis pedidos y un total de 220.00.

  ```python
  from decimal import Decimal
  latest={}
  for r in rows:
      k=r[0]
      if k not in latest or int(r[7])>int(latest[k][7]): latest[k]=r
  sales=sum(Decimal(r[4])*Decimal(r[5]) for r in latest.values())
  assert sales==Decimal('220.00')
  print(f'VALIDACIÓN: pedidos_silver_esperados={len(latest)}; ventas_esperadas={sales}')
  ```

  > **Salida esperada:** Importe esperado Silver: 220.00 en 6 pedidos; P8002=40.00 y P8004=60.00.
  {: .lab-note .output .compact}

{% assign results = site.data.task-results[page.slug].results %}
{% capture r1 %}{{ results[0] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r1 %}

{% include support-prompt.html task="tarea1" %}

---

## ☁️ Tarea 2. Crear el ETL pipeline y desarrollar Bronze/Silver — 22 min

Crear el pipeline por primera vez en **Jobs & Pipelines**, configurar `transformations/` y escribir las dos definiciones declarativas sin utilizar un SQL Warehouse.

### Tarea 2.1. Crear el ETL pipeline desde cero y abrir su código fuente

Crear el recurso antes de intentar editar su SQL y establecer su destino de Unity Catalog.

- {% include step_label.html %} En la barra izquierda entra a **Jobs & Pipelines**. Selecciona **Create** o **Create pipeline** y elige **ETL pipeline** (Lakeflow Spark Declarative Pipelines); no selecciones **Job**.

  > **Salida esperada:** Se abre la vista de un nuevo **ETL pipeline** con botones **Edit pipeline**, **Settings** y **Run pipeline**.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la vista del pipeline nuevo pulsa **Settings**. En **Pipeline settings > Pipeline name**, selecciona el icono de lápiz; escribe `pipeline_orders_bronze_silver_b2` y confirma.

  > **Salida esperada:** **Pipeline name** muestra `pipeline_orders_bronze_silver_b2`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Sin salir de **Settings**, en **Default location for data assets** pulsa **Edit catalog and schema**. Establece **Default catalog**=`dbricks_de_int` y **Default schema**=`batch1`; confirma los cambios.

  > **Salida esperada:** Se muestran `dbricks_de_int` y `batch1` como destino del pipeline; no permanece `default` como esquema.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Settings > Pipeline mode**, selecciona el lápiz y establece **Triggered**. En **Compute**, utiliza **Serverless** si está autorizado. Conserva la identidad de **Run as** de tu cuenta o la asignada por el curso.

  > **Salida esperada:** **Pipeline mode**=`Triggered` y **Compute**=`Serverless` (o la opción permitida por la política institucional).
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Settings > Code assets**, comprueba que **Root folder** apunta al directorio generado al crear el pipeline y **Source code** señala `transformations/`. Si falta, usa **Configure paths** para vincular esa carpeta; no crees archivos en otro directorio.

  > **Salida esperada:** **Source code** incluye `transformations/` dentro de **Root folder**.
  {: .lab-note .output .compact}

- {% include step_label.html %} Cierra **Settings** con la **X**. En la vista principal abre **Edit pipeline**, y en el explorador del editor expande la carpeta `transformations/`.

  > **Nota:** Todavía no ejecutes **Run pipeline**. Si el editor contiene un archivo SQL de ejemplo, puede sustituirse; evita mantener definiciones de ejemplo junto a las del curso.
  {: .lab-note .info .compact}

  > **Salida esperada:** Se abre el editor de código del pipeline y puedes ver `transformations/`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Dentro de `transformations/`, usa **Add > File** o **Create file** para crear `bronze_silver_orders.sql` de tipo SQL. Si existe un SQL de ejemplo, elimina sus definiciones o excluye ese archivo para evitar tablas adicionales.

  > **Salida esperada:** En `transformations/` hay un archivo `bronze_silver_orders.sql` listo para editar, sin definiciones duplicadas de Bronze o Silver.
  {: .lab-note .output .compact}

### Tarea 2.2. Escribir la definición Bronze

Utilizar `read_files` para alimentar una streaming table con los datos crudos del CSV.

- {% include step_label.html %} En `transformations/bronze_silver_orders.sql`, **al inicio del archivo**, pega el bloque completo de Bronze. No lo ejecutes desde **SQL Editor**: se procesará al iniciar el pipeline.

  ```sql
  CREATE OR REFRESH STREAMING TABLE p8_orders_bronze
  COMMENT 'P8 Bronze: conserva CSV y metadatos de origen'
  AS SELECT *, _metadata.file_path AS source_file,
            current_timestamp() AS ingestion_timestamp
  FROM STREAM read_files(
    '/Volumes/dbricks_de_int/batch1/landing/p8_approved/',
    format => 'csv', header => true, inferColumnTypes => false
  );
  ```

  > **Salida esperada:** La primera sentencia crea `p8_orders_bronze` como **STREAMING TABLE** y apunta al directorio de entrada exclusivo.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la primera sentencia SQL, localiza `FROM STREAM read_files(...)` y comprueba que la ruta contiene catálogo `dbricks_de_int`, esquema `batch1`, volumen `landing` y carpeta `p8_approved`.

  > **Salida esperada:** La ruta es `/Volumes/dbricks_de_int/batch1/landing/p8_approved/`; no apunta a la raíz del volumen.
  {: .lab-note .output .compact}

- {% include step_label.html %} En el `SELECT` de Bronze verifica que figuran `*`, `_metadata.file_path AS source_file` y `current_timestamp() AS ingestion_timestamp`, sin filtros ni deduplicación.

  > **Salida esperada:** Bronze preservará las ocho filas originales y añadirá `source_file` e `ingestion_timestamp`.
  {: .lab-note .output .compact}

### Tarea 2.3. Escribir Silver y guardar el SQL

Añadir la vista materializada tipada y deduplicada debajo de la sentencia Bronze.

- {% include step_label.html %} En el mismo archivo SQL, **debajo del `;` que termina Bronze**, agrega la segunda sentencia completa para Silver.

  ```sql
  CREATE OR REFRESH MATERIALIZED VIEW p8_orders_silver
  COMMENT 'P8 Silver: tipado y última versión por pedido'
  AS WITH typed AS (
    SELECT nullif(trim(order_id),'') AS order_id,
           trim(customer_id) AS customer_id,
           try_cast(order_date AS DATE) AS order_date,
           upper(trim(status)) AS order_status,
           try_cast(quantity AS INT) AS quantity,
           try_cast(unit_price AS DECIMAL(12,2)) AS unit_price,
           try_cast(source_updated_at AS TIMESTAMP) AS source_updated_at,
           try_cast(update_sequence AS INT) AS update_sequence,
           source_file, ingestion_timestamp
    FROM p8_orders_bronze
  ), ranked AS (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY source_updated_at DESC NULLS LAST,
               update_sequence DESC NULLS LAST,
               source_file DESC
    ) AS rn FROM typed
  )
  SELECT order_id, customer_id, order_date, order_status, quantity, unit_price,
         CAST(quantity * unit_price AS DECIMAL(14,2)) AS order_amount,
         source_updated_at, update_sequence, source_file, ingestion_timestamp
  FROM ranked WHERE rn = 1;
  ```

  > **Salida esperada:** La segunda sentencia declara `p8_orders_silver` como **MATERIALIZED VIEW** dependiente de Bronze.
  {: .lab-note .output .compact}

- {% include step_label.html %} Dentro de `p8_orders_silver`, ubica **`WITH typed AS`** y comprueba que usa `try_cast` para `order_date`, `quantity`, `unit_price`, `source_updated_at` y `update_sequence`.

  > **Salida esperada:** Las conversiones tolerantes están en el CTE `typed` y conservan el nombre `order_status` para las expectativas de la Práctica 9.
  {: .lab-note .output .compact}

- {% include step_label.html %} En el CTE **`ranked`** confirma `PARTITION BY order_id` y las columnas `source_updated_at` y `update_sequence` con orden `DESC`.

  > **Salida esperada:** La regla conservará la versión 2 de `P8002` y `P8004` al aplicar `rn = 1`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En el **SELECT final** de Silver, identifica `CAST(quantity * unit_price AS DECIMAL(14,2)) AS order_amount` y `FROM ranked WHERE rn = 1`.

  > **Salida esperada:** Silver calculará `order_amount` y tendrá una fila por identificador de pedido.
  {: .lab-note .output .compact}

{% capture r2 %}{{ results[1] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r2 %}

{% include support-prompt.html task="tarea2" %}

---

## 🚀 Tarea 3. Validar el código del pipeline antes de ejecutarlo — 8 min

El pipeline ya fue creado y configurado en la Tarea 2. En esta tarea **no vuelvas a crearlo, editar sus propiedades ni iniciar la carga**: utiliza **Dry run** para revisar las definiciones SQL antes de procesar los archivos.

### Tarea 3.1. Realizar la validación previa con Dry run

Comprobar la definición de Bronze y Silver sin iniciar la primera actualización de datos.

- {% include step_label.html %} En **Jobs & Pipelines**, abre `pipeline_orders_bronze_silver_b2`. En la vista principal del pipeline, selecciona **Dry run** (debajo del área del gráfico central). Espera a que concluya la validación.

  > **Nota:** **Dry run** comprueba las definiciones del pipeline; **no sustituye** a **Run pipeline** y no garantiza que la carga de datos vaya a finalizar correctamente.
  {: .lab-note .info .compact}

  > **Salida esperada:** El resultado de Dry run no presenta errores de análisis de las definiciones `p8_orders_bronze` y `p8_orders_silver`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Revisa el resultado del **Dry run**. Si aparece un error, abre su detalle e identifica el **archivo y la línea SQL**; no continúes con la carga hasta corregirlo.

  > **Salida esperada:** La validación previa termina sin errores pendientes. Si hubo errores, queda identificado el mensaje técnico que debió corregirse.
  {: .lab-note .output .compact}

### Tarea 3.2. Corregir el SQL únicamente si es necesario

Mantener el archivo fuente único y volver a validar solo cuando haya errores.

- {% include step_label.html %} **Solo si Dry run reportó errores**, selecciona **Edit pipeline**, abre `transformations/bronze_silver_orders.sql`, corrige la línea señalada y guarda con **Ctrl+S**. Regresa al pipeline y repite **Dry run** hasta que no haya errores. Si pasó a la primera, omite la edición.

  > **Salida esperada:** El archivo conserva las dos sentencias `CREATE OR REFRESH` y el último Dry run no muestra errores de análisis.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la vista principal del pipeline, comprueba que **aún no se ha realizado la primera actualización de datos**: el gráfico puede seguir vacío. Continúa a la **Tarea 4**, donde ejecutarás **Run pipeline** una sola vez.

  > **Salida esperada:** Pipeline listo para su primera actualización, sin carga duplicada ni tablas creadas manualmente.
  {: .lab-note .output .compact}

{% capture r3 %}{{ results[2] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r3 %}

{% include support-prompt.html task="tarea3" %}

---

## 🧩 Tarea 4. Ejecutar y comprobar las tablas del pipeline — 12 min

Lanzar una actualización triggered y confirmar el DAG, las tablas Delta y los resultados concretos de deduplicación.

### Tarea 4.1. Ejecutar la primera actualización

Usar el grafo como evidencia de que la dependencia es declarativa.

- {% include step_label.html %} En **Jobs & Pipelines**, abre `pipeline_orders_bronze_silver_b2` y pulsa **Run pipeline** (botón superior derecho). Esta es la **única ejecución de carga** de la práctica. Espera a que finalice y consulta su estado en la misma vista.

  > **Nota:** El tiempo de inicio de cómputo puede variar; no inventes métricas de duración.
  {: .lab-note .info .compact}

  > **Salida esperada:** La actualización termina Succeeded; las dos tablas aparecen en el DAG.
  {: .lab-note .output .compact}

- {% include step_label.html %} Cuando el estado sea **Succeeded**, revisa el **gráfico central** generado por la ejecución: identifica `p8_orders_bronze` y sigue la dependencia hacia `p8_orders_silver`.

  > **Salida esperada:** Se visualiza una flecha entre la streaming table y la materialized view.
  {: .lab-note .output .compact}

- {% include step_label.html %} En los detalles de la **actualización que acabas de ejecutar**, registra su **Update ID** y el estado final; consulta la duración únicamente si la interfaz la muestra.

  > **Salida esperada:** La actualización presenta un identificador y una duración observada, no estimada.
  {: .lab-note .output .compact}

### Tarea 4.2. Verificar resultados reales de las dos capas

Demostrar con consultas la conservación de Bronze y el resultado tipado Silver.

- {% include step_label.html %} Regresa a `08_preparar_origen` y crea la **celda 8 (Python)** debajo de la celda 7. Ejecuta la consulta SQL mediante `spark.sql` y muestra el conteo Bronze.

   ```python
   bronze_result = spark.sql("SELECT COUNT(*) AS bronze_rows FROM dbricks_de_int.batch1.p8_orders_bronze").first()
   assert bronze_result.bronze_rows == 8
   print(f"VALIDACIÓN: Bronze=p8_orders_bronze; filas={bronze_result.bronze_rows}; esperado=8")
   ```

  > **Salida esperada:** Se imprime `VALIDACIÓN: Bronze=p8_orders_bronze; filas=8; esperado=8`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `08_preparar_origen`, crea la **celda 9 (Python)** debajo de la celda 8. Ejecuta la agregación SQL mediante `spark.sql` y comprueba 6 filas, 6 IDs y 220.00.

   ```python
   from decimal import Decimal
   silver_result = spark.sql("""SELECT COUNT(*) AS silver_rows,
    COUNT(DISTINCT order_id) AS ids_distintos,
    CAST(SUM(order_amount) AS DECIMAL(14,2)) AS ventas
    FROM dbricks_de_int.batch1.p8_orders_silver""").first()
   assert (silver_result.silver_rows, silver_result.ids_distintos, silver_result.ventas) == (6, 6, Decimal('220.00'))
   print(f"VALIDACIÓN: Silver=6 filas; ids_unicos=6; ventas={silver_result.ventas}")
   ```

  > **Salida esperada:** Se imprime `VALIDACIÓN: Silver=6 filas; ids_unicos=6; ventas=220.00`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `08_preparar_origen`, crea la **celda 10 (Python)** debajo de la celda 9. Consulta `P8002` y `P8004` y confirma las secuencias finales.

   ```python
   orders = spark.sql("""SELECT order_id,order_status,update_sequence,order_amount
   FROM dbricks_de_int.batch1.p8_orders_silver
   WHERE order_id IN ('P8002','P8004') ORDER BY order_id""")
   data = {r.order_id: r for r in orders.collect()}
   assert set(data) == {'P8002', 'P8004'}
   assert data['P8002'].update_sequence == 2 and data['P8002'].order_amount == Decimal('40.00')
   assert data['P8004'].update_sequence == 2 and data['P8004'].order_amount == Decimal('60.00')
   display(orders)
   print('VALIDACIÓN: P8002=40.00 (secuencia=2); P8004=60.00 (secuencia=2)')
   ```

  > **Salida esperada:** Se muestran P8002=40.00 y P8004=60.00, ambos con secuencia 2; aparece el mensaje con los mismos valores.
  {: .lab-note .output .compact}

{% capture r4 %}{{ results[3] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r4 %}

{% include support-prompt.html task="tarea4" %}

---

## ✅ Tarea 5. Comprobar integridad y event log — 8 min

Revisar la calidad estructural de Silver y la evidencia operativa de la actualización en Lakeflow.

### Tarea 5.1. Validar tipos y ausencia de duplicados

Dejar comprobaciones reproducibles que puedan repetirse después de la Práctica 9.

- {% include step_label.html %} En `08_preparar_origen`, crea la **celda 11 (Python)** debajo de la celda 10. Ejecuta la consulta de duplicados e IDs nulos; ambos conteos deben ser cero.

   ```python
   quality = spark.sql("""SELECT
    (SELECT COUNT(*) FROM (SELECT order_id FROM dbricks_de_int.batch1.p8_orders_silver
    GROUP BY order_id HAVING COUNT(*) > 1) AS duplicate_ids) AS duplicate_groups,
    (SELECT COUNT(*) FROM dbricks_de_int.batch1.p8_orders_silver WHERE order_id IS NULL) AS ids_nulos""").first()
   assert quality.duplicate_groups == 0 and quality.ids_nulos == 0
   print(f'VALIDACIÓN: Silver grupos_duplicados={quality.duplicate_groups}; ids_nulos={quality.ids_nulos}')
   ```

  > **Salida esperada:** Se imprime `VALIDACIÓN: Silver grupos_duplicados=0; ids_nulos=0`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `08_preparar_origen`, crea la **celda 12 (Python)** debajo de la celda 11. Ejecuta `DESCRIBE TABLE` mediante `spark.sql` y muestra los tipos DATE, INT y DECIMAL.

   ```python
   schema_rows = spark.sql('DESCRIBE TABLE dbricks_de_int.batch1.p8_orders_silver')
   types = {r.col_name: r.data_type.lower() for r in schema_rows.collect()}
   assert types['order_date'] == 'date'
   assert types['quantity'] in ('int', 'integer')
   assert types['unit_price'].startswith('decimal(') and types['order_amount'].startswith('decimal(')
   display(schema_rows)
   print('VALIDACIÓN: order_date=DATE; quantity=INT; unit_price/order_amount=DECIMAL')
   ```

  > **Salida esperada:** Se muestra `DESCRIBE TABLE` y se imprime `VALIDACIÓN: order_date=DATE; quantity=INT; unit_price/order_amount=DECIMAL`.
  {: .lab-note .output .compact}

### Tarea 5.2. Revisar actualización y eventos de ejecución

Identificar eventos reales sin confundir el event log con el historial Delta.

- {% include step_label.html %} En **Jobs & Pipelines > pipeline_orders_bronze_silver_b2 > Updates**, selecciona el último Update ID y comprueba **Succeeded** y ambos datasets.

  > **Salida esperada:** El estado observado es Succeeded y aparecen Bronze y Silver como datasets procesados.
  {: .lab-note .output .compact}

- {% include step_label.html %} En los detalles del pipeline, abre **Event log / Events** (según la vista), filtra por el Update ID y revisa eventos de progreso, finalización y errores.

  > **Nota:** El event log depende del pipeline, no de DESCRIBE HISTORY de una tabla Delta.
  {: .lab-note .info .compact}

  > **Salida esperada:** Se muestran eventos asociados al pipeline y su update ID; no hay error de ejecución.
  {: .lab-note .output .compact}

- {% include step_label.html %} En `08_preparar_origen` conserva las **celdas 8–12** con sus resultados; en Lakeflow conserva el pipeline y su **Update ID** para la Práctica 9. No ejecutes DROP ni elimines el archivo CSV.

  > **Salida esperada:** Evidencia final: 8 filas Bronze; 6 pedidos Silver; 220.00 en importe total.
  {: .lab-note .output .compact}

{% capture r5 %}{{ results[4] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r5 %}

{% include support-prompt.html task="tarea5" %}

