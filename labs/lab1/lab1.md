---
layout: lab
title: "Práctica 1: Configuración de cluster y ejecución de notebook ETL"
permalink: /lab1/lab1/
images_base: /labs/lab1/img
duration: "40 minutos"
objective:
  - "Configurar o validar cómputo All-Purpose Single node con Databricks Runtime 15.4 LTS y modo de acceso Standard."
  - "Crear un notebook Python, conectarlo al cómputo y comprobar la ejecución de PySpark."
  - "Ejecutar un ETL básico sobre cinco pedidos, transformar tipos y publicar un resultado consultable mediante una vista temporal."
  - "Validar cantidades e importes y conservar el notebook para reutilización."
prerequisites:
  - "Workspace de Databricks y permiso para crear o utilizar cómputo All-Purpose."
  - "Databricks Runtime 15.4 LTS disponible, con política institucional compatible con Standard."
  - "Acceso de escritura en la carpeta personal del Workspace."
  - "Conocimientos básicos de notebooks, Python, DataFrames y consultas SQL."
introduction:
  - "Prepararás el cómputo interactivo de Databricks y ejecutarás un notebook ETL autónomo sobre cinco pedidos controlados. Cargarás los datos en un DataFrame, aplicarás conversiones y agregaciones, y publicarás una vista temporal para consultar los resultados sin crear tablas Bronze, Silver o Gold."
slug: lab1
lab_number: 1
final_result: >
  Un cluster All-Purpose Single node operativo y un notebook Python guardado con un ETL reproducible: cinco pedidos de entrada,
  dos fechas agregadas y ventas totales de 470.50. La salida se publica como vista temporal de la sesión;
  no se crean tablas persistentes de Unity Catalog ni recursos de la arquitectura Medallion.
notes:
  - "Tiempo máximo: 40 minutos. Si la cuota de CPU es insuficiente o el aprovisionamiento tarda demasiado, utiliza un cluster compatible ya iniciado y autorizado por el curso."
  - "Este laboratorio es independiente de la Práctica 2: los cinco registros se crean en memoria, sin CSV, volumen o esquema previos."
  - "Conserva el notebook; la vista temporal dura lo que su SparkSession y debe regenerarse al iniciar una nueva sesión."
  - "No ejecutes consultas ni limpiezas sobre tablas del catálogo compartido."
references:
  - text: "Databricks Runtime 15.4 LTS"
    url: https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/15.4lts
  - text: "Configuración de cómputo Databricks"
    url: https://learn.microsoft.com/en-us/azure/databricks/compute/configure
  - text: "Vista temporal con createOrReplaceTempView"
    url: https://spark.apache.org/docs/3.5.0/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.createOrReplaceTempView.html
prev: /
next: /lab2/lab2/
---

---

<!-- Paso = acción ejecutable o verificable; notas y salidas no cuentan como pasos. -->

## 🔎 Tarea 1. Configurar el cluster All-Purpose — 12 min

Configurarás un cluster **Single node** para esta práctica de cinco pedidos, siguiendo el orden visible en **Create new compute** de Azure Databricks. No se necesitan dos workers para este ejercicio.

### Tarea 1.1. Abrir el formulario de creación

- {% include step_label.html %} Inicia sesión en el workspace de **Azure Databricks** con las credenciales del curso.

- {% include step_label.html %} En el menú izquierdo, selecciona **Compute** para abrir tus recursos de cómputo.

  > **Salida esperada:** Se muestra la lista de recursos que puedes utilizar o administrar.
  {: .lab-note .output .compact}

- {% include step_label.html %} Si ya tienes un cluster autorizado y compatible, selecciónalo; de lo contrario, pulsa **Create compute**.

  > **Importante:** No crees recursos duplicados si el instructor ya proporcionó un cluster funcional.
  {: .lab-note .important .compact}

- {% include step_label.html %} En **Create new compute**, confirma que el encabezado indique **Simple form: ON**.

  > **Salida esperada:** El formulario muestra las secciones **General**, **Performance**, **Tags** y **Advanced**.
  {: .lab-note .output .compact}

### Tarea 1.2. Completar General y Performance

Configura las propiedades **en este orden**, igual que aparecen en tu interfaz.

| Orden | Propiedad visible | Valor mínimo para el laboratorio |
| --- | --- | --- |
| 1 | **Compute name** | `lab1-etl-<usuario>` |
| 2 | **Policy** | `Unrestricted`, si está autorizado; de lo contrario, la política del curso |
| 3 | **Machine learning** | Sin seleccionar |
| 4 | **Databricks runtime** | `15.4 LTS` (sin ML) |
| 5 | **Photon acceleration** | Sin seleccionar |
| 6 | **Preferred node type** | El nodo más pequeño autorizado **con cuota disponible** |
| 7 | **Single node** | Activado |
| 8 | **Terminate after** | Activar y escribir `20` minutos |
| 9 | **Tags** | Dejar vacíos salvo que la institución los exija |
| 10 | **Advanced > Access mode** | `Manual` → `Standard (formerly: Shared)` |

- {% include step_label.html %} En **General > Compute name**, escribe `lab1-etl-<usuario>`; sustituye `<usuario>` por tu identificador.

  > **Salida esperada:** El nombre identifica exclusivamente tu cluster del laboratorio.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Policy**, deja **Unrestricted** únicamente si tienes autorización; en otro caso, selecciona la política del curso.

  > **Nota:** Una política institucional puede bloquear algunas propiedades; conserva los valores obligatorios.
  {: .lab-note .info .compact}

- {% include step_label.html %} En **Performance**, deja **Machine learning** desmarcado y selecciona **Databricks runtime: 15.4 LTS**.

  > **Salida esperada:** El selector muestra `15.4 LTS`, con Spark 3.5.0.
  {: .lab-note .output .compact}

- {% include step_label.html %} Deja desmarcada la casilla **Photon acceleration**; no es necesaria para este ejemplo pequeño.

- {% include step_label.html %} En **Preferred node type**, selecciona el nodo autorizado más pequeño con cuota suficiente.

  > **Advertencia:** La captura muestra `Standard_D4a_v4` (4 cores) con **Estimated available: 0, requested: 4**. Ese nodo no tiene cuota suficiente en la imagen. No presiones Create esperando que se solucione: busca un tamaño disponible o solicita al instructor/administrador liberar o aumentar la cuota regional correspondiente.
  {: .lab-note .warning .compact}

- {% include step_label.html %} Mantén marcada la casilla **Single node** para ejecutar driver y procesamiento en un solo nodo.

  > **Importante:** No configures 2 workers. **Single node** es suficiente para los cinco registros de este laboratorio y reduce el uso de recursos frente a un cluster multinodo equivalente.
  {: .lab-note .important .compact}

- {% include step_label.html %} Marca **Terminate after** e introduce `20` en el campo de minutos de inactividad.

  > **Salida esperada:** Se muestra **Terminate after 20 minutes of inactivity**.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Tags**, deja **Key** y **Value** vacíos si el curso no exige etiquetas personalizadas.

### Tarea 1.3. Configurar Advanced, crear y verificar

- {% include step_label.html %} Despliega **Advanced** y entra en **Access mode**, como se muestra al final del formulario.

- {% include step_label.html %} Selecciona **Manual** y, en el desplegable contiguo, **Standard (formerly: Shared)**.

  > **Salida esperada:** El formulario muestra `Manual` y `Standard (formerly: Shared)`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Comprueba nombre, Runtime 15.4 LTS, Single node, terminación 20 minutos y Access mode Standard.

- {% include step_label.html %} Solo si **no aparece una advertencia de cuota o bloqueo**, pulsa **Create** en la parte inferior del formulario.

  > **Advertencia:** Si la cuota sigue en `0`, no podrás garantizar el aprovisionamiento. Solicita un cluster del curso ya iniciado o la corrección de cuota antes de continuar con la Tarea 2.
  {: .lab-note .warning .compact}

- {% include step_label.html %} En la ficha del cluster, espera el estado **Running**; si aparece **Terminated**, pulsa **Start**.

  > **Salida esperada:** El cluster está **Running** y disponible para conectarlo a un notebook.
  {: .lab-note .output .compact}


{% assign results = site.data.task-results[page.slug].results %}


{% capture r1 %}{{ results[0] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r1 %}

{% include support-prompt.html task="tarea1" %}


---


## ☁️ Tarea 2. Preparar el notebook de trabajo — 8 min

Crearás un notebook de Python dentro de tu espacio personal, lo asociarás al cluster y verificarás la versión de Spark. Organizarás las celdas para ejecutar una secuencia ETL sencilla y comprensible.


### Tarea 2.1. Crear y conectar el notebook

Prepara el documento ejecutable donde se desarrollarán y validarán las transformaciones.


- {% include step_label.html %} En la barra lateral, abre **Workspace > Home** y localiza tu carpeta de usuario en **Users**.

  > **Salida esperada:** Se muestra tu directorio de trabajo con permiso para crear archivos.
  {: .lab-note .output .compact}


- {% include step_label.html %} En tu carpeta personal, pulsa **Create > Folder**, escribe `batch_1` y entra a la carpeta.

  > **Nota:** La ruta definitiva será `/Workspace/Users/<usuario>/batch_1/`.
  {: .lab-note .info .compact}

  > **Salida esperada:** Aparece la carpeta de esta práctica.
  {: .lab-note .output .compact}


- {% include step_label.html %} Dentro de `batch_1`, pulsa **Create > Notebook**, escribe `01_etl_basico` y confirma **Create**.

  > **Salida esperada:** Se abre un notebook vacío con el nombre indicado.
  {: .lab-note .output .compact}


- {% include step_label.html %} Elige **Python** como lenguaje; arriba a la derecha, pulsa el **boton verde ó Connect** y selecciona el cluster Running recien creado.

  > **Salida esperada:** El notebook está conectado a un cluster Running y listo para ejecutar celdas.
  {: .lab-note .output .compact}


### Tarea 2.2. Verificar Spark y organizar las celdas

Confirma la sesión de Spark y agrega una introducción breve al propósito del notebook.


- {% include step_label.html %} En la primera celda **Python**, pega el código de versiones y pulsa **Shift+Enter** para ejecutarlo.

  ```python
  import sys
  print("Python:", sys.version.split()[0])
  print("Spark:", spark.version)
  ```

  > **Salida esperada:** La salida muestra la versión de Spark incluida en Runtime 15.4 LTS (Spark 3.5.0) y Python compatible.
  {: .lab-note .output .compact}


- {% include step_label.html %} Debajo de la primera celda, agrega otra, cambia su tipo a **Markdown** y pega el texto indicado.

  ```markdown
  **Objetivo:** procesar cinco pedidos controlados sin archivos externos.
  **Flujo:** Extract (DataFrame) → Transform (tipos y métricas) → Load (vista temporal).
  ```

  > **Salida esperada:** El texto se visualiza como Markdown y describe el pipeline.
  {: .lab-note .output .compact}


- {% include step_label.html %} Debajo del Markdown, agrega una celda **Python**, pega las importaciones y ejecútala con **Shift+Enter**.

  ```python
  from pyspark.sql import functions as F
  from pyspark.sql.types import DecimalType
  ```

  > **Salida esperada:** La celda ejecuta sin errores de importación.
  {: .lab-note .output .compact}


{% capture r2 %}{{ results[1] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r2 %}

{% include support-prompt.html task="tarea2" %}


---


## 🚀 Tarea 3. Ejecutar el proceso ETL básico — 15 min

Construirás un DataFrame con cinco pedidos de ejemplo, aplicarás conversiones explícitas y calcularás ventas diarias. Publicarás la salida como vista temporal para consultarla con SQL, sin crear tablas Delta.


### Tarea 3.1. Cargar los datos de ejemplo

Extrae cinco registros definidos en el notebook para mantener independencia frente a archivos o permisos de almacenamiento.


- {% include step_label.html %} Debajo de las importaciones, crea una celda **Python**, pega la lista `pedidos` y ejecútala.

  ```python
  pedidos = [
      ("1001", "2025-01-15", "Norte", "120.50"),
      ("1002", "2025-01-15", "Sur", "80.00"),
      ("1003", "2025-01-16", "Norte", "50.25"),
      ("1004", "2025-01-16", "Centro", "200.00"),
      ("1005", "2025-01-16", "Sur", "19.75"),
  ]
  ```

  > **Salida esperada:** La variable `pedidos` contiene cinco tuplas con identificador, fecha, región e importe.
  {: .lab-note .output .compact}


- {% include step_label.html %} Agrega una celda **Python** debajo de `pedidos`, pega `spark.createDataFrame(...)` y ejecútala.

  ```python
  raw_df = spark.createDataFrame(
      pedidos, ["order_id", "order_date", "region", "amount"]
  )
  ```

  > **Salida esperada:** `raw_df` queda disponible en la sesión de Spark.
  {: .lab-note .output .compact}


- {% include step_label.html %} En una celda **Python** nueva, ejecuta `raw_df.printSchema()` y revisa los cuatro tipos.

  ```python
  raw_df.printSchema()
  ```

  > **Salida esperada:** Las cuatro columnas iniciales muestran tipo `string`.
  {: .lab-note .output .compact}


- {% include step_label.html %} En la siguiente celda **Python**, ejecuta `display(raw_df.orderBy("order_id"))` y cuenta las filas.

  ```python
  display(raw_df.orderBy("order_id"))
  ```

  > **Salida esperada:** Aparecen los pedidos 1001 a 1005 con sus importes como texto.
  {: .lab-note .output .compact}


### Tarea 3.2. Transformar datos mediante PySpark

Convierte fechas e importes y prepara datos tipados para producir un resumen diario confiable.


- {% include step_label.html %} Debajo de la carga, crea una celda **Python**, pega la transformación `typed_df` y ejecútala.

  ```python
  typed_df = (
      raw_df
      .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
      .withColumn("amount", F.col("amount").cast(DecimalType(12, 2)))
  )
  ```

  > **Salida esperada:** `order_date` y `amount` ya no son cadenas de texto.
  {: .lab-note .output .compact}


- {% include step_label.html %} En una celda **Python** nueva, ejecuta `typed_df.printSchema()` y revisa fecha e importe.

  ```python
  typed_df.printSchema()
  ```

  > **Salida esperada:** Aparecen `order_date: date` y `amount: decimal(12,2)`.
  {: .lab-note .output .compact}


- {% include step_label.html %} En la siguiente celda **Python**, pega el filtro que define `valid_df` y ejecútalo.

  ```python
  valid_df = typed_df.filter(
      F.col("order_id").isNotNull()
      & F.col("order_date").isNotNull()
      & F.col("amount").isNotNull()
      & (F.col("amount") >= 0)
  )
  ```

  > **Salida esperada:** El DataFrame conserva los cinco pedidos válidos del ejemplo.
  {: .lab-note .output .compact}


- {% include step_label.html %} Agrega una celda **Python** con `valid_df.count()` y ejecútala para comprobar los cinco pedidos.

  ```python
  total_validos = valid_df.count()
  print("Pedidos válidos:", total_validos)
  ```

  > **Salida esperada:** La consola muestra `Pedidos válidos: 5`.
  {: .lab-note .output .compact}


- {% include step_label.html %} Debajo del filtro, agrega una celda **Python**, pega el agregado `daily_df` y ejecútalo.

  ```python
  daily_df = (
      valid_df.groupBy("order_date")
      .agg(
          F.countDistinct("order_id").alias("order_count"),
          F.sum("amount").alias("total_sales"),
      )
      .orderBy("order_date")
  )
  ```

  > **Salida esperada:** `daily_df` queda definido con una fila por fecha y dos métricas.
  {: .lab-note .output .compact}


### Tarea 3.3. Generar y consultar el resultado del ETL

Publica el resultado como vista temporal de sesión y consúltalo desde SQL para completar la etapa Load.


- {% include step_label.html %} En una celda **Python** nueva, ejecuta `display(daily_df)` y verifica las dos fechas.

  ```python
  display(daily_df)
  ```

  > **Salida esperada:** 2025-01-15: 2 pedidos y 200.50; 2025-01-16: 3 pedidos y 270.00.
  {: .lab-note .output .compact}


- {% include step_label.html %} En la siguiente celda **Python**, ejecuta `daily_df.createOrReplaceTempView("etl_ventas_diarias")`.

  > **Importante:** La vista no es una tabla Delta: solo existe mientras esté activa la misma SparkSession.
  {: .lab-note .important .compact}

  ```python
  daily_df.createOrReplaceTempView("etl_ventas_diarias")
  ```

  > **Salida esperada:** La operación finaliza sin errores y registra la vista temporal en la sesión.
  {: .lab-note .output .compact}


- {% include step_label.html %} Agrega una celda nueva, cambia su lenguaje a **SQL** o usa `%sql`, y ejecuta la consulta.

  ```sql
  SELECT order_date, order_count, total_sales
  FROM etl_ventas_diarias
  ORDER BY order_date;
  ```

  > **Nota:** Si la celda sigue en Python, escribe `%sql` en su primera línea antes de `SELECT`.
  {: .lab-note .info .compact}

  > **Salida esperada:** La consulta devuelve dos filas con ventas de 200.50 y 270.00.
  {: .lab-note .output .compact}


- {% include step_label.html %} En otra celda **SQL** del mismo notebook y cluster, pega la consulta de totales y ejecútala.

  ```sql
  SELECT SUM(order_count) AS pedidos, SUM(total_sales) AS ventas
  FROM etl_ventas_diarias;
  ```

  > **Salida esperada:** La salida muestra `pedidos = 5` y `ventas = 470.50`.
  {: .lab-note .output .compact}


{% capture r3 %}{{ results[2] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r3 %}

{% include support-prompt.html task="tarea3" %}


---


## ✅ Tarea 4. Validar la ejecución y conservar el notebook — 5 min

Verificarás con aserciones que el ETL produce los registros y totales previstos. Repetirás el tramo de transformación y carga para comprobar reproducibilidad y guardarás el notebook sin eliminar recursos del curso.


### Tarea 4.1. Comprobar resultados y repetir la ejecución

Confirma la consistencia del procesamiento con controles automáticos y una segunda ejecución funcional.


- {% include step_label.html %} Agrega una celda **Python** debajo de las consultas SQL, pega las aserciones y ejecútalas.

  ```python
  from decimal import Decimal
  resumen = spark.sql("""
    SELECT COUNT(*) AS dias, SUM(order_count) AS pedidos,
           SUM(total_sales) AS ventas
    FROM etl_ventas_diarias
  """).first()
  assert resumen.dias == 2
  assert resumen.pedidos == 5
  assert resumen.ventas == Decimal("470.50")
  print("Validación ETL: OK")
  ```

  > **Salida esperada:** Se imprime `Validación ETL: OK` y no aparece ningún `AssertionError`.
  {: .lab-note .output .compact}


- {% include step_label.html %} Vuelve a la celda `daily_df`, ejecútala y después reejecuta la celda `createOrReplaceTempView`.

  > **Nota:** No es necesario volver a iniciar el cluster ni volver a crear el notebook.
  {: .lab-note .info .compact}

  > **Salida esperada:** Las dos celdas terminan sin errores y reemplazan la vista temporal.
  {: .lab-note .output .compact}


- {% include step_label.html %} Regresa a la última consulta **SQL**, ejecútala nuevamente y compara pedidos y ventas.

  ```sql
  SELECT SUM(order_count) AS pedidos, SUM(total_sales) AS ventas
  FROM etl_ventas_diarias;
  ```

  > **Salida esperada:** El resultado continúa siendo 5 pedidos y 470.50 en ventas.
  {: .lab-note .output .compact}


### Tarea 4.2. Guardar el notebook y comprobar el estado final

Conserva la implementación y confirma el estado del recurso para que pueda utilizarse en otra sesión.


- {% include step_label.html %} En **Workspace > Home > batch_1**, comprueba que existe `01_etl_basico` y vuelve a abrirlo.

  > **Nota:** Databricks guarda automáticamente los cambios del notebook; verifica que el nombre y la ubicación sean correctos.
  {: .lab-note .info .compact}

  > **Salida esperada:** El notebook aparece en Workspace y mantiene sus celdas y resultados.
  {: .lab-note .output .compact}


- {% include step_label.html %} Abre **Compute** y revisa el estado; pulsa **Terminate** solo si el instructor autoriza detenerlo.

  > **Advertencia:** No detengas un cluster compartido mientras otros participantes lo estén utilizando.
  {: .lab-note .warning .compact}

  > **Salida esperada:** El cómputo permanece Running para continuar o pasa a Terminated si se autorizó detenerlo.
  {: .lab-note .output .compact}


{% capture r4 %}{{ results[3] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r4 %}

{% include support-prompt.html task="tarea4" %}