---
layout: lab
title: 'Práctica 9: Implementación de validaciones y manejo de errores'
permalink: /lab9/lab9/
images_base: /labs/lab9/img
duration: 40 minutos
objective:
- Definir expectativas WARN, DROP ROW y FAIL UPDATE sin reemplazar el tipo de dataset Silver.
- Ejecutar pruebas reproducibles y verificar métricas/eventos.
- Recuperar una actualización fallida con Full refresh controlado en Development.
prerequisites:
- 'Práctica 8 ejecutada: pipeline_orders_bronze_silver_b2 disponible.'
- Permiso para editar `transformations/bronze_silver_orders.sql` dentro del pipeline existente y ejecutar actualizaciones.
- Permisos para crear y retirar archivos del directorio p8_approved.
introduction:
- Continuar la Práctica 8 sin crear otro ETL pipeline. Modificar Silver con tres expectativas y probar por separado WARN, DROP ROW y FAIL UPDATE; reconstruir al final con un único Full refresh.
slug: lab9
lab_number: 9
final_result: 'Tras la recuperación: Bronze con 11 filas, Silver con siete; WARN retiene Q9001, DROP rechaza Q9002/Q9003 y
  FAIL UPDATE queda documentado.'
notes:
- 'Cada paso identifica el lugar de ejecución, la celda o sección y el resultado concreto; los mensajes de validación muestran los valores comprobados.'
- Los nombres de tablas P8 son exclusivos y no alteran Prácticas 1–7.
- 'No eliminar el pipeline: la Práctica 10 puede reutilizar sus tablas y su configuración.'
- 'Antes de comenzar, confirma que la Práctica 8 revisada terminó correctamente con Bronze=8 y Silver=6.'
- 'Los CSV de P9 se agregan únicamente a p8_approved; no crees un segundo pipeline ni sobrescribas el CSV baseline.'
references:
- text: Pipeline updates y Full refresh
  url: https://docs.databricks.com/aws/en/ldp/updates
- text: Lakeflow SQL
  url: https://docs.databricks.com/aws/en/ldp/developer/sql-dev
- text: Expectations
  url: https://docs.databricks.com/aws/en/ldp/expectations
- text: Pipeline event log
  url: https://docs.databricks.com/aws/en/ldp/monitor-event-logs
- text: read_files Auto Loader
  url: https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files
prev: /lab8/lab8/
next: /lab10/lab10/
---

<!-- Lab con pasos de ejecución y verificación reproducibles. -->

---

> **Nota de continuidad:** El pipeline de P8 debe usar el catálogo `dbricks_de_int`, el esquema `batch1` y el origen `/Volumes/dbricks_de_int/batch1/landing/p8_approved/`. El notebook `09_control_expectations` es solo para preparar datos y verificar resultados; las sentencias `CREATE OR REFRESH` se editan únicamente en el archivo SQL del pipeline.
{: .lab-note .important .compact}

## 🔎 Tarea 1. Verificar el pipeline y su estado inicial — 6 min

Comprobar el resultado de la única ejecución de la Práctica 8. El código Lakeflow se modifica desde **Edit pipeline > transformations/**. Para comprobaciones, crear `09_control_expectations` dentro de `batch_1` y ejecutar únicamente las celdas indicadas.

### Tarea 1.1. Abrir el pipeline y el archivo SQL de la Práctica 8

Trabajar sobre el mismo pipeline y los mismos objetos de Unity Catalog.

- {% include step_label.html %} En **Jobs & Pipelines**, abre `pipeline_orders_bronze_silver_b2` (creado en P8). Revisa la última actualización en el historial de ejecuciones: debe figurar **Succeeded**; en el DAG localiza `p8_orders_bronze` → `p8_orders_silver`.

  > **Salida esperada:** El pipeline existente tiene p8_orders_bronze y p8_orders_silver sin errores activos.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la página del mismo pipeline pulsa **Edit pipeline**. En el explorador del editor abre **transformations > bronze_silver_orders.sql**; localiza las dos sentencias `CREATE OR REFRESH`. No crees otro archivo SQL.

  > **Salida esperada:** Las definiciones CREATE OR REFRESH de Bronze y Silver están presentes.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **transformations/bronze_silver_orders.sql**, ubica `FROM STREAM read_files(...)` dentro de `p8_orders_bronze` y comprueba que apunta exactamente a `/Volumes/dbricks_de_int/batch1/landing/p8_approved/`. No edites esa sentencia.

  > **Salida esperada:** Los archivos que se crearán en P9 serán leídos por el mismo pipeline, no por otras prácticas.
  {: .lab-note .output .compact}

### Tarea 1.2. Registrar línea base observable

Comparar posteriores pruebas contra datos iniciales medibles.

- {% include step_label.html %} En `Workspace > batch_1`, crea el notebook Python **09_control_expectations**, conéctalo al cluster existente y ejecuta la **celda 1 (Python)**.

  ```python
  from decimal import Decimal
  CATALOG = 'dbricks_de_int'; SCHEMA = 'batch1'
  SILVER = f'{CATALOG}.{SCHEMA}.p8_orders_silver'
  BRONZE = f'{CATALOG}.{SCHEMA}.p8_orders_bronze'
  baseline = spark.sql(f"SELECT COUNT(*) AS pedidos, CAST(SUM(order_amount) AS DECIMAL(14,2)) AS ventas FROM {SILVER}").first()
  assert baseline.pedidos == 6 and baseline.ventas == Decimal('220.00'), baseline
  print(f'VALIDACIÓN EXITOSA: Silver inicial: pedidos={baseline.pedidos}; ventas={baseline.ventas}')
  ```

  > **Salida esperada:** El resultado inicial es 6 pedidos e importe total 220.00.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **09_control_expectations**, agrega la **celda 2 (Python)** debajo de la anterior; comprueba que los IDs de P9 aún no estén publicados.

  ```python
  pending = spark.sql(f"SELECT COUNT(*) AS n FROM {SILVER} WHERE order_id IN ('Q9001','Q9002','Q9003','Q9004')").first().n
  assert pending == 0, f'P9 ya tiene {pending} registros en Silver'
  print(f'VALIDACIÓN EXITOSA: IDs Q9001 a Q9004 previamente publicados={pending}')
  ```

  > **Salida esperada:** El conteo para Q9001, Q9002, Q9003 y Q9004 es cero.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **09_control_expectations**, agrega la **celda 3 (Python)** para inspeccionar los tipos y columnas de Silver.

  ```python
  silver_cols = set(spark.table(SILVER).columns)
  required = {'order_id', 'order_status', 'order_amount', 'quantity', 'unit_price'}
  assert required.issubset(silver_cols), f'Faltan columnas: {required - silver_cols}'
  print('VALIDACIÓN EXITOSA: Silver incluye order_id, order_status, order_amount, quantity y unit_price')
  display(spark.sql(f'DESCRIBE TABLE {SILVER}'))
  ```

  > **Salida esperada:** Existen order_id, order_status y order_amount; no se utilizarán status ni amount inexistentes.
  {: .lab-note .output .compact}

{% assign results = site.data.task-results[page.slug].results %}
{% capture r1 %}{{ results[0] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r1 %}

{% include support-prompt.html task="tarea1" %}

---

## ☁️ Tarea 2. Incorporar expectativas sin reemplazar el tipo de Silver — 8 min

Modificar exclusivamente la sentencia `CREATE OR REFRESH MATERIALIZED VIEW p8_orders_silver` en el archivo del pipeline; conservar la sentencia Bronze que la precede.

### Tarea 2.1. Añadir las tres acciones de calidad

Configurar WARN implícito, DROP ROW y FAIL UPDATE con expresiones sobre columnas Silver.

- {% include step_label.html %} En **Edit pipeline > transformations > bronze_silver_orders.sql**, identifica la segunda sentencia, que inicia `CREATE OR REFRESH MATERIALIZED VIEW p8_orders_silver`. Sustituye **solo esa sentencia**, desde `CREATE` hasta su `;` final, por el bloque siguiente; conserva Bronze intacta.

  > **Nota:** Sustituye solo la sentencia Silver anterior; conserva intacta la sentencia Bronze. En SQL Lakeflow WARN se expresa con EXPECT sin ON VIOLATION WARN.
  {: .lab-note .info .compact}

  ```sql
  CREATE OR REFRESH MATERIALIZED VIEW p8_orders_silver (
   CONSTRAINT valid_status EXPECT (
     order_status IN ('PENDING','COMPLETED','CANCELLED')
   ),
   CONSTRAINT valid_amount EXPECT (
     order_amount IS NOT NULL AND order_amount > 0
   ) ON VIOLATION DROP ROW,
   CONSTRAINT valid_order_id EXPECT (
     order_id IS NOT NULL
   ) ON VIOLATION FAIL UPDATE
  )
  COMMENT 'P8/P9 Silver: reglas de calidad y deduplicación'
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
    PARTITION BY order_id ORDER BY source_updated_at DESC NULLS LAST,
    update_sequence DESC NULLS LAST, source_file DESC) AS rn
   FROM typed
  )
  SELECT order_id,customer_id,order_date,order_status,quantity,unit_price,
   CAST(quantity*unit_price AS DECIMAL(14,2)) AS order_amount,
   source_updated_at,update_sequence,source_file,ingestion_timestamp
  FROM ranked WHERE rn=1;
  ```

  > **Salida esperada:** Aparecen las expectativas valid_status, valid_amount y valid_order_id en Silver.
  {: .lab-note .output .compact}

- {% include step_label.html %} En ese mismo archivo SQL, dentro de la sentencia Silver, localiza `CONSTRAINT valid_status`: debe contener `EXPECT` y **no** incluir `ON VIOLATION`. Es la regla WARN (conserva Q9001).

  > **Salida esperada:** WARN se aplica por defecto; las únicas cláusulas ON VIOLATION son DROP ROW y FAIL UPDATE.
  {: .lab-note .output .compact}

- {% include step_label.html %} Debajo de `valid_status`, revisa `CONSTRAINT valid_amount`: debe evaluar `order_amount > 0` y finalizar en `ON VIOLATION DROP ROW`. Será la regla de Q9002/Q9003.

  > **Salida esperada:** Las filas con importe nulo o no positivo se descartan de Silver.
  {: .lab-note .output .compact}

- {% include step_label.html %} Debajo de `valid_amount`, revisa `CONSTRAINT valid_order_id`: debe comprobar `order_id IS NOT NULL` y usar `ON VIOLATION FAIL UPDATE`. Será la prueba controlada de la Tarea 4.

  > **Salida esperada:** El campo order_id vacío se convierte en NULL mediante nullif(trim(order_id),'').
  {: .lab-note .output .compact}

### Tarea 2.2. Guardar y activar la nueva definición

Reutilizar la materialized view de P8 sin redefinirla como streaming table.

- {% include step_label.html %} En el editor **Edit pipeline**, guarda `transformations/bronze_silver_orders.sql` con **Ctrl+S**. Comprueba que el archivo contiene exactamente dos sentencias: Bronze `STREAMING TABLE` y Silver `MATERIALIZED VIEW` con tres `CONSTRAINT`.

  > **Salida esperada:** La única modificación funcional es el conjunto de expectativas sobre Silver.
  {: .lab-note .output .compact}

- {% include step_label.html %} Regresa a la página de `pipeline_orders_bronze_silver_b2` y pulsa **Run pipeline** una vez para activar la nueva definición **antes de crear los CSV de P9**. Abre la ejecución y espera el estado **Succeeded**. No selecciones **Full refresh** aquí.

  > **Salida esperada:** La actualización termina Succeeded y Silver mantiene 6 registros e importe 220.00 (antes de añadir CSV P9).
  {: .lab-note .output .compact}

- {% include step_label.html %} En **09_control_expectations**, crea la **celda 4 (Python)** y consulta Silver para comprobar los seis pedidos y el total 220.00.

  ```python
  initial = spark.sql(f"SELECT COUNT(*) AS pedidos, CAST(SUM(order_amount) AS DECIMAL(14,2)) AS ventas FROM {SILVER}").first()
  assert initial.pedidos == 6 and initial.ventas == Decimal('220.00'), initial
  print(f'VALIDACIÓN EXITOSA: expectations activadas; Silver={initial.pedidos} filas; ventas={initial.ventas}')
  ```

  > **Salida esperada:** Se observan exactamente 6 filas y 220.00; ninguna fila base es descartada.
  {: .lab-note .output .compact}

{% capture r2 %}{{ results[1] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r2 %}

{% include support-prompt.html task="tarea2" %}

---

## 🚀 Tarea 3. Comprobar WARN y DROP ROW con archivos independientes — 10 min

Usar el notebook `09_control_expectations` para crear archivos CSV; después ejecutar actualizaciones manuales en el pipeline y verificar las filas mediante consultas en celdas Python.

### Tarea 3.1. Probar WARN conservando la fila Q9001

Inyectar un estado fuera de dominio con importe válido.

- {% include step_label.html %} En **Workspace > batch_1 > 09_control_expectations**, crea la **celda 5 (Python)** debajo de la celda 4. Define `ROOT`, `HEAD` y la función `put_case`; se reutilizará en las celdas 6, 8 y 11.

  ```python
  import csv,io
  ROOT=f'/Volumes/{CATALOG}/{SCHEMA}/landing/p8_approved'
  assert any(x.name == 'orders_p8_baseline.csv' for x in dbutils.fs.ls(ROOT)), 'Falta el CSV baseline de P8'
  HEAD=['order_id','customer_id','order_date','status','quantity','unit_price','source_updated_at','update_sequence']
  def put_case(name,values):
   b=io.StringIO();w=csv.writer(b);w.writerow(HEAD);w.writerows(values)
   if any(item.name == name for item in dbutils.fs.ls(ROOT)):
    raise RuntimeError(f'{name} ya existe; no se sobrescriben archivos procesados')
   dbutils.fs.put(f'{ROOT}/{name}',b.getvalue(),overwrite=False)
   print(f'CSV creado: {name}; filas={len(values)}; directorio={ROOT}')
  assert len(HEAD)==8
  existing_p9 = sorted(x.name for x in dbutils.fs.ls(ROOT) if x.name.startswith('p9_'))
  assert not existing_p9, f'Existen archivos de una ejecución P9 anterior: {existing_p9}'
  print(f'VALIDACIÓN EXITOSA: directorio={ROOT}; columnas CSV={len(HEAD)}; baseline disponible; archivos P9 previos=0')
  ```

  > **Salida esperada:** Se muestra la ruta completa de `p8_approved`, ocho columnas CSV y `archivos P9 previos=0`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **09_control_expectations**, crea la **celda 6 (Python)** y genera `p9_warn.csv` con Q9001.

  ```python
  put_case('p9_warn.csv', [['Q9001','C901','2026-09-04','OUT_OF_DOMAIN','2','6.00','2026-09-04 10:00:00','1']])
  assert spark.read.option('header', True).csv(f'{ROOT}/p9_warn.csv').count() == 1
  print('VALIDACIÓN EXITOSA: p9_warn.csv; filas=1; ID=Q9001; importe esperado=12.00')
  ```

  > **Salida esperada:** El archivo p9_warn.csv contiene exactamente una fila de negocio.
  {: .lab-note .output .compact}

- {% include step_label.html %} Después de crear `p9_warn.csv`, vuelve a **Jobs & Pipelines > pipeline_orders_bronze_silver_b2** y pulsa **Run pipeline** **una sola vez**. Abre el detalle de esta actualización y espera **Succeeded**; en el DAG inspecciona `p8_orders_silver`.

  > **Salida esperada:** La actualización termina Succeeded; la infracción WARN no detiene el pipeline.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **09_control_expectations**, crea la **celda 7 (Python)**; consulta Q9001 en Silver y revisa la infracción `valid_status` en los detalles del último Update.

  ```python
  warn = spark.sql(f"SELECT order_id, order_status, order_amount FROM {SILVER} WHERE order_id='Q9001'").collect()
  assert len(warn) == 1 and warn[0].order_status == 'OUT_OF_DOMAIN' and warn[0].order_amount == Decimal('12.00'), warn
  print(f'VALIDACIÓN EXITOSA: WARN; order_id={warn[0].order_id}; status={warn[0].order_status}; amount={warn[0].order_amount}; conservado=1')
  display(spark.table(SILVER).filter("order_id = 'Q9001'"))
  ```

  > **Salida esperada:** La celda muestra Q9001/OUT_OF_DOMAIN/12.00. En los detalles de la actualización, consulta métricas de `valid_status` si están disponibles.
  {: .lab-note .output .compact}

### Tarea 3.2. Probar DROP ROW con dos tipos de importe inválido

Agregar casos de importe cero y unit_price vacío para aislar la expectativa valid_amount.

- {% include step_label.html %} En **09_control_expectations**, crea la **celda 8 (Python)** para generar `p9_drop.csv` con Q9002 y Q9003.

  ```python
  put_case('p9_drop.csv', [['Q9002','C902','2026-09-04','COMPLETED','1','0.00','2026-09-04 11:00:00','1'],['Q9003','C903','2026-09-04','PENDING','1','','2026-09-04 12:00:00','1']])
  assert spark.read.option('header', True).csv(f'{ROOT}/p9_drop.csv').count() == 2
  print('VALIDACIÓN EXITOSA: p9_drop.csv; filas=2; Q9002=0.00; Q9003=precio vacío')
  ```

  > **Salida esperada:** El archivo contiene dos registros; ningún otro campo es inválido.
  {: .lab-note .output .compact}

- {% include step_label.html %} Después de escribir `p9_drop.csv`, vuelve a la página del mismo pipeline y pulsa **Run pipeline** **una vez**. Espera **Succeeded** y revisa el detalle de la actualización de `p8_orders_silver`; no uses **Full refresh** todavía.

  > **Salida esperada:** La actualización termina Succeeded y Bronze incorpora las dos filas de p9_drop.csv.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **09_control_expectations**, crea la **celda 9 (Python)** para verificar la ausencia de Q9002/Q9003 en Silver.

  ```python
  dropped = spark.sql(f"SELECT COUNT(*) AS n FROM {SILVER} WHERE order_id IN ('Q9002','Q9003')").first().n
  assert dropped == 0, f'Silver contiene {dropped} filas que debieron descartarse'
  print(f'VALIDACIÓN EXITOSA: DROP ROW; Q9002/Q9003 publicados={dropped}; esperado=0')
  ```

  > **Salida esperada:** El resultado pedidos_descartados=0; Q9001 permanece publicado.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **09_control_expectations**, crea la **celda 10 (Python)** y consulta Bronze para comprobar los dos registros rechazados antes de Silver.

  ```python
  bronze_drop = spark.sql(f"SELECT order_id, quantity, unit_price FROM {BRONZE} WHERE order_id IN ('Q9002','Q9003') ORDER BY order_id").collect()
  assert [r.order_id for r in bronze_drop] == ['Q9002','Q9003'], bronze_drop
  print(f'VALIDACIÓN EXITOSA: Bronze conserva ambos casos DROP; registros={len(bronze_drop)}')
  display(spark.sql(f"SELECT order_id, quantity, unit_price FROM {BRONZE} WHERE order_id IN ('Q9002','Q9003') ORDER BY order_id"))
  ```

  > **Salida esperada:** Bronze muestra Q9002 y Q9003. En las métricas de `valid_amount` busca los dos registros rechazados por DROP ROW.
  {: .lab-note .output .compact}

{% capture r3 %}{{ results[2] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r3 %}

{% include support-prompt.html task="tarea3" %}

---

## 🧩 Tarea 4. Provocar FAIL UPDATE y registrar el incidente — 9 min

Simular una clave ausente que impide publicar Silver sin eliminar las versiones previas exitosas.

### Tarea 4.1. Introducir una clave nula controlada

Aislar el error de clave; conservar el resto de columnas válidas.

- {% include step_label.html %} En **09_control_expectations**, crea la **celda 11 (Python)** y escribe `p9_fail.csv` con `order_id` vacío.

  ```python
  put_case('p9_fail.csv', [['','C904','2026-09-04','COMPLETED','1','49.90','2026-09-04 13:00:00','1']])
  assert spark.read.option('header', True).csv(f'{ROOT}/p9_fail.csv').count() == 1
  print('VALIDACIÓN EXITOSA: p9_fail.csv; filas=1; order_id vacío; importe calculado=49.90')
  ```

  > **Salida esperada:** El archivo contiene una fila con order_id vacío e importe calculable de 49.90.
  {: .lab-note .output .compact}

- {% include step_label.html %} Después de crear `p9_fail.csv`, desde la página del pipeline pulsa **Run pipeline** **una vez**. Mantén `valid_order_id` habilitada; abre la nueva ejecución y comprueba el estado **Failed** en la actualización de Silver.

  > **Salida esperada:** La actualización de Silver falla por `valid_order_id`; el estado global puede mostrarse Failed aunque otros flujos hayan completado.
  {: .lab-note .output .compact}

- {% include step_label.html %} Dentro del detalle de la actualización fallida, selecciona el nodo **p8_orders_silver** del DAG y abre **Errors** o el panel de errores disponible. Busca `valid_order_id`, `EXPECTATION_VIOLATION` o `FAIL UPDATE`.

  > **Salida esperada:** El mensaje señala la regla FAIL UPDATE o la expresión order_id IS NOT NULL.
  {: .lab-note .output .compact}

### Tarea 4.2. Consultar evidencias de error y continuidad

Distinguir estado del flujo Silver y datos ya materializados.

- {% include step_label.html %} En la vista de la actualización fallida, abre los detalles de eventos (por ejemplo **Event log**, si está disponible) y localiza el error de `p8_orders_silver`; anota el identificador de esa actualización, sin ejecutar otro Run.

  > **Salida esperada:** El evento correspondiente identifica p8_orders_silver y la expectativa valid_order_id.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **09_control_expectations**, agrega la **celda 12 (Python)** y comprueba que Silver conserva Q9001 pero no Q9002/Q9003.

  > **Nota:** El FAIL UPDATE impide publicar la actualización fallida de Silver; no implica que ningún otro flujo paralelo haya avanzado.
  {: .lab-note .info .compact}

  ```python
  kept = spark.sql(f"SELECT order_id,order_amount FROM {SILVER} WHERE order_id IN ('Q9001','Q9002','Q9003') ORDER BY order_id").collect()
  assert len(kept) == 1 and kept[0].order_id == 'Q9001' and kept[0].order_amount == Decimal('12.00'), kept
  print(f'VALIDACIÓN EXITOSA: después del fallo; Silver conserva Q9001={kept[0].order_amount}; Q9002/Q9003 ausentes')
  ```

  > **Salida esperada:** Q9001 conserva 12.00; Q9002/Q9003 siguen ausentes.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **09_control_expectations**, crea la **celda 13 (Python)** para consultar `DESCRIBE HISTORY` de Silver, si está habilitado.

  ```python
  try:
      display(spark.sql(f'DESCRIBE HISTORY {SILVER} LIMIT 5'))
      print('Historial Delta consultado para Silver; compara versión y operación con el último Update exitoso')
  except Exception as err:
      print('DESCRIBE HISTORY no disponible para esta materialized view; revisa Updates > Event log.')
      print('Detalle:', str(err)[:180])
  ```

  > **Salida esperada:** Si DESCRIBE HISTORY está permitido, se listan versiones. Si no, aparece la instrucción de revisar los detalles de actualizaciones del pipeline.
  {: .lab-note .output .compact}

{% capture r4 %}{{ results[3] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r4 %}

{% include support-prompt.html task="tarea4" %}

---

## ✅ Tarea 5. Recuperar el pipeline y verificar las reglas — 7 min

Retirar solo `p9_fail.csv` con una celda Python del notebook de control; en `Jobs & Pipelines > pipeline_orders_bronze_silver_b2`, iniciar una actualización con Full refresh en Development.

### Tarea 5.1. Retirar el CSV fallido y ejecutar Full refresh

Evitar un nuevo pipeline y evitar borrar tablas manualmente.

- {% include step_label.html %} En **09_control_expectations**, crea la **celda 14 (Python)** y elimina solamente `p9_fail.csv`; confirma su ausencia.

  ```python
  bad=f'{ROOT}/p9_fail.csv'
  dbutils.fs.rm(bad,False)
  assert all(x.name!='p9_fail.csv' for x in dbutils.fs.ls(ROOT))
  remaining = sorted(x.name for x in dbutils.fs.ls(ROOT) if x.name.endswith('.csv'))
  assert remaining == ['orders_p8_baseline.csv', 'p9_drop.csv', 'p9_warn.csv'], remaining
  print(f'VALIDACIÓN EXITOSA: eliminado=p9_fail.csv; CSV restantes={remaining}')
  ```

  > **Salida esperada:** p9_fail.csv no aparece en el directorio p8_approved.
  {: .lab-note .output .compact}

- {% include step_label.html %} Después de eliminar `p9_fail.csv`, regresa al pipeline y abre la **flecha desplegable** junto a **Run pipeline**. Selecciona **Full refresh** (o la opción equivalente para actualizar todas las tablas); confirma el aviso y espera **Succeeded**. Es una recuperación de laboratorio: **no pulses Run pipeline normal antes de este Full refresh**.

  > **Nota:** Full refresh reinicializa los checkpoints de las streaming tables y reprocesa los CSV presentes. Ejecuta esta operación únicamente en el pipeline de práctica, con autorización; no depende de que aparezca un interruptor Development en Settings.
  {: .lab-note .info .compact}

  > **Salida esperada:** La actualización termina Succeeded y vuelve a procesar solo los archivos que permanecen en p8_approved.
  {: .lab-note .output .compact}

- {% include step_label.html %} Cuando el Update termine en `Succeeded`, abre **09_control_expectations** y crea la **celda 15 (Python)** para validar Silver.

  ```python
  recovered = spark.sql(f"""SELECT COUNT(*) AS silver_rows,
    SUM(CASE WHEN order_id='Q9001' THEN 1 ELSE 0 END) AS warn_present,
    SUM(CASE WHEN order_id IN ('Q9002','Q9003') THEN 1 ELSE 0 END) AS dropped_present,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_ids,
    CAST(SUM(order_amount) AS DECIMAL(14,2)) AS ventas
    FROM {SILVER}""").first()
  assert (recovered.silver_rows,recovered.warn_present,recovered.dropped_present,recovered.null_ids,recovered.ventas) == (7,1,0,0,Decimal('232.00')), recovered
  print(f'VALIDACIÓN EXITOSA: Silver recuperada; filas={recovered.silver_rows}; WARN={recovered.warn_present}; DROP={recovered.dropped_present}; IDs nulos={recovered.null_ids}; ventas={recovered.ventas}')
  ```

  > **Salida esperada:** `Silver recuperada; filas=7; WARN=1; DROP=0; IDs nulos=0; ventas=232.00`.
  {: .lab-note .output .compact}

### Tarea 5.2. Conservar evidencias de calidad y recuperación

Cerrar el laboratorio con resultados operativos comprobables.

- {% include step_label.html %} En **09_control_expectations**, agrega la **celda 16 (Python)** y consulta el total de Bronze después de Full refresh.

  ```python
  bronze_count = spark.sql(f'SELECT COUNT(*) AS n FROM {BRONZE}').first().n
  assert bronze_count == 11, f'Bronze={bronze_count}; esperado=11'
  print(f'VALIDACIÓN EXITOSA: Bronze recuperada; filas={bronze_count}; 8 base + 1 WARN + 2 DROP')
  ```

  > **Salida esperada:** `Bronze recuperada; filas=11; 8 base + 1 WARN + 2 DROP`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En el historial de actualizaciones del pipeline, abre primero el Run **Failed** (regla `valid_order_id`) y luego el **Succeeded** de **Full refresh**; registra ambos identificadores y los conteos comprobados en las celdas 15–16.

  > **Salida esperada:** Evidencia: tres reglas con resultados diferentes y un Full refresh final Succeeded.
  {: .lab-note .output .compact}

{% capture r5 %}{{ results[4] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r5 %}

{% include support-prompt.html task="tarea5" %}

