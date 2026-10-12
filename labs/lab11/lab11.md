---
layout: lab
title: "Práctica 11: Migración y validación de accesos"
permalink: /lab11/lab11/
images_base: /labs/lab11/img
duration: "40 minutos"
objective:
  - "Preparar un origen Delta administrado en Unity Catalog y copiarlo mediante DEEP CLONE."
  - "Comparar esquema, claves, filas, importes y ubicaciones entre origen y destino."
  - "Comprobar independencia física entre el origen y el clon sin alterar datos sensibles."
  - "Proteger email y phone mediante una vista dinámica y validar accesos con los usuarios de la Práctica 10."
prerequisites:
  - "Práctica 10 completada: existen data_engineers_b2 y data_analysts_b2, con una cuenta de prueba por grupo."
  - "Cuenta principal autorizada sobre dbricks_de_int: USE CATALOG y CREATE SCHEMA, y capacidad para crear tablas y vistas en el nuevo esquema."
  - "La cuenta principal es propietaria de los objetos creados, o dispone de MANAGE para conceder permisos."
  - "Acceso al SQL Warehouse/cómputo autorizado para las tres identidades y almacenamiento administrado disponible."
introduction:
  - "La práctica simula una migración entre tablas administradas en Unity Catalog: crea un origen Delta de 20 transacciones sintéticas, realiza DEEP CLONE, valida integridad e implementa una vista con enmascaramiento por grupo. No usa Hive Metastore, SYNC TABLE, External Location ni Storage Credential."
slug: lab11
lab_number: 11
final_result: >
  El esquema dbricks_de_int.p11_migration_lab conserva 20 transacciones en origen
  y en el clon, con 20 claves únicas, importe 2100.00 y diferencias cero.
  La vista p11_vw_sales_masked muestra PII completa a Ingeniería y enmascarada
  a Analítica, que no puede consultar directamente la tabla clonada.
notes:
  - "DEEP CLONE copia datos y metadatos; NO representa una migración sin copia de archivos, a diferencia de SYNC TABLE."
  - "No modificar dbricks_de_int.batch1 ni dbricks_de_int.p10_security_lab."
  - "No se crean grupos ni usuarios: reutiliza los que se comprobaron en la Práctica 10."
  - "Al ejecutar SHOW GRANTS, usa una celda SQL independiente por objeto y comprueba explícitamente los dos grupos sobre la vista."
  - "Si hay permisos heredados que conceden SELECT sobre la tabla clonada a Analítica, detén la prueba de denegación sin revocar recursos institucionales."
references:
  - text: "CREATE TABLE CLONE"
    url: "https://docs.databricks.com/azure/en/sql/language-manual/delta-clone"
  - text: "Clonar tablas Delta"
    url: "https://docs.databricks.com/azure/en/delta/clone"
  - text: "Vistas dinámicas"
    url: "https://docs.databricks.com/azure/en/views/dynamic"
  - text: "Permisos de Unity Catalog"
    url: "https://docs.databricks.com/azure/en/data-governance/unity-catalog/manage-privileges"
prev: /lab10/lab10/
next: /lab12/lab12/
---

> **Continuidad con la Práctica 10:** usa la cuenta principal para las **Tareas 1–4** y el cierre de la **Tarea 5**; usa `lab10.engineer_00` y el usuario real de Analítica proporcionado por el administrador únicamente para las pruebas de la Tarea 5. Los correos exactos se confirman en **Settings → Identity and access → Users / Groups → Manage**. Los grupos son `data_engineers_b2` y `data_analysts_b2`.
{: .lab-note .important .compact}

> **Antes de comenzar:** confirma `USE CATALOG` y `CREATE SCHEMA` en `dbricks_de_int`, acceso a un SQL Warehouse/cómputo autorizado, almacenamiento administrado en el catálogo y facultad de gestionar los `GRANT` sobre los objetos creados. Para evitar colisiones, no reutilices un esquema `p11_migration_lab` ajeno.
{: .lab-note .warning .compact}

{% assign results = site.data.task-results[page.slug].results %}

---

## 🔎 Tarea 1. Verificar entorno y crear la fuente Delta — 9 min

Crear un origen reproducible completamente dentro de Unity Catalog, sin dependencia de Hive Metastore.

### Tarea 1.1. Confirmar recursos y preparar notebook

- {% include step_label.html %} **Con la cuenta principal**, entra en **Workspace → Home → tu carpeta de trabajo → batch_1**. Crea el notebook SQL **`11_migracion_accesos`**, selecciona el cómputo autorizado y ejecuta la **celda SQL 1**.

  ```sql
  SELECT current_user() AS usuario,
         current_catalog() AS catalogo_actual,
         current_schema() AS esquema_actual;
  ```

  > **Salida esperada:** Aparece el usuario principal real; conserva esta sesión para crear las tablas y conceder permisos.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **`11_migracion_accesos`**, agrega la **celda SQL 2**; comprueba que el catálogo está disponible y que aún no tienes un esquema de P11 perteneciente a otro participante.

  ```sql
  SHOW SCHEMAS IN dbricks_de_int LIKE 'p11_migration_lab';
  ```

  > **Salida esperada:** Si aparece una fila, verifica que el esquema sea tuyo antes de continuar; no elimines datos ajenos.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 3**, revisa permisos sobre el catálogo para detectar privilegios amplios heredables. No ejecutes `REVOKE` institucional.

  ```sql
  SHOW GRANTS ON CATALOG dbricks_de_int;
  ```

  > **Salida esperada:** Se muestran los principals y sus privilegios; identifica si Analítica recibe lectura amplia por otros grupos.
  {: .lab-note .output .compact}

### Tarea 1.2. Crear tabla fuente sintética

- {% include step_label.html %} En la **celda SQL 4**, crea el esquema **`p11_migration_lab`** con la cuenta principal. Necesitas `CREATE SCHEMA` sobre el catálogo.

  ```sql
  CREATE SCHEMA IF NOT EXISTS dbricks_de_int.p11_migration_lab;
  ```

  > **Salida esperada:** El esquema aparece en `dbricks_de_int`; `batch1` y `p10_security_lab` quedan intactos.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 5**, crea la tabla administrada de origen **`p11_sales_source`** y carga 20 transacciones sintéticas; pega ambas sentencias en esa celda y ejecútalas en orden.

  ```sql
  CREATE TABLE IF NOT EXISTS dbricks_de_int.p11_migration_lab.p11_sales_source (
    transaction_id STRING,
    customer_id STRING,
    email STRING,
    phone STRING,
    region STRING,
    amount DECIMAL(12,2),
    status STRING
  ) USING DELTA;
  
  INSERT OVERWRITE dbricks_de_int.p11_migration_lab.p11_sales_source
  SELECT format_string('TX11-%03d', id + 1),
         format_string('C11-%03d', id + 1),
         format_string('cliente%02d@ejemplo.test', id + 1),
         format_string('555-01%02d', id + 1),
         CASE WHEN id % 2 = 0 THEN 'Norte' ELSE 'Sur' END,
         CAST((id + 1) * 10.00 AS DECIMAL(12,2)),
         'COMPLETED'
  FROM range(20);
  ```

  > **Salida esperada:** `p11_sales_source` contiene 20 transacciones `TX11-001` a `TX11-020`, con datos ficticios.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 6**, comprueba la línea base antes de clonar.

  ```sql
  SELECT COUNT(*) AS filas,
         COUNT(DISTINCT transaction_id) AS claves,
         CAST(SUM(amount) AS DECIMAL(14,2)) AS importe,
         MIN(transaction_id) AS primera,
         MAX(transaction_id) AS ultima
  FROM dbricks_de_int.p11_migration_lab.p11_sales_source;
  ```

  > **Salida esperada:** `filas=20`, `claves=20`, `importe=2100.00`, `primera=TX11-001` y `ultima=TX11-020`.
  {: .lab-note .output .compact}

{% capture r1 %}{{ results[0] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r1 %}

{% include support-prompt.html task="tarea1" %}

---

## ⚙️ Tarea 2. Ejecutar DEEP CLONE y confirmar independencia — 8 min

Crear una copia física Delta administrada y verificar dónde quedaron los archivos.

### Tarea 2.1. Clonar una tabla de Unity Catalog

- {% include step_label.html %} En **`11_migracion_accesos`**, crea la **celda SQL 7** y ejecuta **`DEEP CLONE`** desde `p11_sales_source` hacia `p11_sales_migrated`. No ejecutes `SYNC TABLE`.

  > **Nota:** Si ya existe `p11_sales_migrated`, `IF NOT EXISTS` no actualiza sus datos. Solo úsalo como primera ejecución; si pertenece a otro participante, no continúes.
  {: .lab-note .info .compact}

  ```sql
  CREATE TABLE IF NOT EXISTS dbricks_de_int.p11_migration_lab.p11_sales_migrated
  DEEP CLONE dbricks_de_int.p11_migration_lab.p11_sales_source;
  ```

  > **Salida esperada:** La tabla destino se registra como Delta; el resultado de CLONE muestra métricas de la operación que pueden variar por ejecución.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 8**, muestra las dos tablas del esquema.

  ```sql
  SHOW TABLES IN dbricks_de_int.p11_migration_lab;
  ```

  > **Salida esperada:** Aparecen `p11_sales_source` y `p11_sales_migrated` (más otros objetos propios, si existen).
  {: .lab-note .output .compact}

### Tarea 2.2. Confirmar Delta y ubicación distinta

- {% include step_label.html %} En la **celda SQL 9**, inspecciona los metadatos del origen.

  ```sql
  DESCRIBE DETAIL dbricks_de_int.p11_migration_lab.p11_sales_source;
  ```

  > **Salida esperada:** `format=delta`; anota el valor exacto de `location` del origen.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 10**, inspecciona los metadatos del clon.

  ```sql
  DESCRIBE DETAIL dbricks_de_int.p11_migration_lab.p11_sales_migrated;
  ```

  > **Salida esperada:** `format=delta` y `location` **distinta** de la fuente, porque `DEEP CLONE` crea archivos independientes.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 11**, inspecciona las operaciones recientes del clon.

  ```sql
  DESCRIBE HISTORY dbricks_de_int.p11_migration_lab.p11_sales_migrated LIMIT 5;
  ```

  > **Salida esperada:** Se observa historial propio del destino y una operación de clonación; no se presupone que herede el historial completo de la fuente.
  {: .lab-note .output .compact}

{% capture r2 %}{{ results[1] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r2 %}

{% include support-prompt.html task="tarea2" %}

---

## 🛡️ Tarea 3. Validar la integridad y consistencia del clon — 9 min

Comprobar igualdad de datos y esquema con consultas que muestren diferencias numéricas concretas.

### Tarea 3.1. Verificar estructura y cantidades

- {% include step_label.html %} En **`11_migracion_accesos`**, ejecuta la **celda SQL 12** para inspeccionar columnas y tipos del origen.

  ```sql
  DESCRIBE TABLE dbricks_de_int.p11_migration_lab.p11_sales_source;
  ```

  > **Salida esperada:** Se identifican siete columnas: `transaction_id`, `customer_id`, `email`, `phone`, `region`, `amount` y `status`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 13**, revisa las mismas columnas en el clon.

  ```sql
  DESCRIBE TABLE dbricks_de_int.p11_migration_lab.p11_sales_migrated;
  ```

  > **Salida esperada:** Los nombres, tipos y orden de las siete columnas coinciden con la celda 12.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 14**, compara filas, claves e importe mediante una única consulta de diferencias.

  ```sql
  WITH origen AS (
    SELECT COUNT(*) AS filas, COUNT(DISTINCT transaction_id) AS claves,
           CAST(SUM(amount) AS DECIMAL(14,2)) AS importe
    FROM dbricks_de_int.p11_migration_lab.p11_sales_source
  ), destino AS (
    SELECT COUNT(*) AS filas, COUNT(DISTINCT transaction_id) AS claves,
           CAST(SUM(amount) AS DECIMAL(14,2)) AS importe
    FROM dbricks_de_int.p11_migration_lab.p11_sales_migrated
  )
  SELECT origen.filas AS filas_origen, destino.filas AS filas_destino,
         destino.filas - origen.filas AS diferencia_filas,
         origen.claves AS claves_origen, destino.claves AS claves_destino,
         destino.claves - origen.claves AS diferencia_claves,
         origen.importe AS importe_origen, destino.importe AS importe_destino,
         destino.importe - origen.importe AS diferencia_importe
  FROM origen CROSS JOIN destino;
  ```

  > **Salida esperada:** `filas_origen=20`, `filas_destino=20`, `diferencia_filas=0`; `claves_origen=20`, `claves_destino=20`, `diferencia_claves=0`; ambos importes `2100.00` y diferencia `0.00`.
  {: .lab-note .output .compact}

### Tarea 3.2. Comparar filas, no solo agregados

- {% include step_label.html %} En la **celda SQL 15**, compara todas las columnas del origen contra el clon con `EXCEPT`.

  ```sql
  SELECT * FROM dbricks_de_int.p11_migration_lab.p11_sales_source
  EXCEPT
  SELECT * FROM dbricks_de_int.p11_migration_lab.p11_sales_migrated;
  ```

  > **Salida esperada:** Cero filas: no hay valores de origen ausentes en el clon.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 16**, invierte la comparación.

  ```sql
  SELECT * FROM dbricks_de_int.p11_migration_lab.p11_sales_migrated
  EXCEPT
  SELECT * FROM dbricks_de_int.p11_migration_lab.p11_sales_source;
  ```

  > **Salida esperada:** Cero filas: el clon no contiene valores adicionales. Como cada `transaction_id` es único, la comparación cubre las 20 transacciones.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 17**, confirma el contrato de PII antes de crear la vista.

  ```sql
  SELECT COUNT(*) AS filas,
         SUM(CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END) AS emails,
         SUM(CASE WHEN phone IS NOT NULL THEN 1 ELSE 0 END) AS telefonos
  FROM dbricks_de_int.p11_migration_lab.p11_sales_migrated;
  ```

  > **Salida esperada:** `filas=20`, `emails=20` y `telefonos=20` (datos exclusivamente ficticios).
  {: .lab-note .output .compact}

{% capture r3 %}{{ results[2] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r3 %}

{% include support-prompt.html task="tarea3" %}

---

## 📊 Tarea 4. Crear vista protegida y configurar permisos — 8 min

Conceder lectura de la vista a los dos grupos, pero lectura directa del clon solamente a Ingeniería.

### Tarea 4.1. Conceder uso y crear vista dinámica

- {% include step_label.html %} Con la **cuenta principal**, en **`11_migracion_accesos`**, ejecuta la **celda SQL 18**. El propietario del catálogo o quien tenga `MANAGE` debe conceder `USE CATALOG` a ambos grupos **solo si faltaba**; si ya se hizo en P10, verifica sin repetir cambios.

  ```sql
  SHOW GRANTS ON CATALOG dbricks_de_int;
  ```

  > **Salida esperada:** Aparecen `USE CATALOG` de `data_engineers_b2` y `data_analysts_b2` (concedidos en la Práctica 10). Si falta uno, solicita al propietario que lo configure antes de seguir.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 19**, concede `USE SCHEMA` únicamente en el nuevo esquema, sin permisos globales sobre otras tablas.

  ```sql
  GRANT USE SCHEMA ON SCHEMA dbricks_de_int.p11_migration_lab TO `data_engineers_b2`;
  GRANT USE SCHEMA ON SCHEMA dbricks_de_int.p11_migration_lab TO `data_analysts_b2`;
  ```

  > **Salida esperada:** Ambos grupos pueden resolver los objetos de `p11_migration_lab`; esto no da acceso directo a las tablas.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 20**, crea `p11_vw_sales_masked`. Su lógica muestra correo y teléfono completos únicamente al grupo Ingeniería.

  ```sql
  CREATE OR REPLACE VIEW dbricks_de_int.p11_migration_lab.p11_vw_sales_masked AS
  SELECT transaction_id, customer_id, region, amount, status,
         CASE WHEN is_account_group_member('data_engineers_b2') THEN email
              WHEN email IS NULL THEN NULL ELSE '***MASKED_EMAIL***' END AS email,
         CASE WHEN is_account_group_member('data_engineers_b2') THEN phone
              WHEN phone IS NULL THEN NULL ELSE '***MASKED_PHONE***' END AS phone
  FROM dbricks_de_int.p11_migration_lab.p11_sales_migrated;
  ```

  > **Salida esperada:** Se crea una vista de 20 filas con `email` y `phone` dinámicamente enmascarados según la identidad.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 21**, concede `SELECT` directo del clon **solo** a Ingeniería y `SELECT` de la vista **a ambos grupos**.

  ```sql
  GRANT SELECT ON TABLE dbricks_de_int.p11_migration_lab.p11_sales_migrated TO `data_engineers_b2`;
  GRANT SELECT ON VIEW dbricks_de_int.p11_migration_lab.p11_vw_sales_masked TO `data_engineers_b2`;
  GRANT SELECT ON VIEW dbricks_de_int.p11_migration_lab.p11_vw_sales_masked TO `data_analysts_b2`;
  ```

  > **Salida esperada:** Ingeniería tiene lectura de tabla y vista; Analítica tiene lectura de la vista, no de las tablas de origen o clon.
  {: .lab-note .output .compact}

### Tarea 4.2. Validación de permisos antes del cambio de sesión

- {% include step_label.html %} En la **celda SQL 22**, ejecuta **solo** `SHOW GRANTS` de la tabla clonada y confirma el `SELECT` de Ingeniería.

  ```sql
  SHOW GRANTS ON TABLE dbricks_de_int.p11_migration_lab.p11_sales_migrated;
  ```

  > **Salida esperada:** `data_engineers_b2` aparece con `SELECT`; `data_analysts_b2` **no** debe tener `SELECT` directo ni por herencia.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 23**, consulta únicamente los privilegios de la vista. No combines esta sentencia con el `SHOW GRANTS` anterior.

  ```sql
  SHOW GRANTS ON VIEW dbricks_de_int.p11_migration_lab.p11_vw_sales_masked;
  ```

  > **Salida esperada:** Aparecen **dos filas `SELECT`**, una para `data_engineers_b2` y otra para `data_analysts_b2`. Si falta alguna, vuelve a la celda 21 y concede exclusivamente el `SELECT` faltante.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 24**, verifica que ambos grupos tengan `USE SCHEMA` y que no haya concesiones amplias inesperadas.

  ```sql
  SHOW GRANTS ON SCHEMA dbricks_de_int.p11_migration_lab;
  ```

  > **Salida esperada:** Ambos grupos figuran con `USE SCHEMA`. Si existe `SELECT` heredado sobre las tablas para Analítica, suspende las pruebas de denegación y solicita aislamiento.
  {: .lab-note .output .compact}

{% capture r4 %}{{ results[3] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r4 %}

{% include support-prompt.html task="tarea4" %}

---

## ✅ Tarea 5. Probar identidades y cerrar la migración — 6 min

Inicia sesiones independientes para comprobar los permisos y el enmascaramiento, luego conserva el resultado.

### Tarea 5.1. Probar Ingeniería y Analítica en SQL Editor

- {% include step_label.html %} **Cierra la sesión principal** o usa una sesión privada. Inicia Databricks con la **cuenta de Ingeniería** de la Práctica 10. Abre **SQL Editor → New query**, selecciona el SQL Warehouse autorizado y ejecuta **consulta 1**.

  ```sql
  SELECT current_user() AS usuario,
         is_account_group_member('data_engineers_b2') AS ingenieria,
         is_account_group_member('data_analysts_b2') AS analitica;
  ```

  > **Salida esperada:** `ingenieria=true` y `analitica=false`. Si aparece `false` inesperadamente aun estando el usuario en Groups → Members, cierra sesión, vuelve a entrar y repite la consulta antes de cambiar permisos.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **misma sesión de Ingeniería**, ejecuta la **consulta 2** sobre la vista para demostrar que no hay máscaras.

  ```sql
  SELECT transaction_id, email, phone
  FROM dbricks_de_int.p11_migration_lab.p11_vw_sales_masked
  ORDER BY transaction_id LIMIT 2;
  ```

  > **Salida esperada:** `TX11-001` y `TX11-002` muestran correos `cliente01@ejemplo.test`, `cliente02@ejemplo.test` y teléfonos de prueba completos.
  {: .lab-note .output .compact}

- {% include step_label.html %} Cierra Ingeniería e inicia con el **usuario de Analítica** existente. En **SQL Editor → New query**, ejecuta **consulta 3** y comprueba su membresía.

  ```sql
  SELECT current_user() AS usuario,
         is_account_group_member('data_engineers_b2') AS ingenieria,
         is_account_group_member('data_analysts_b2') AS analitica;
  ```

  > **Salida esperada:** `ingenieria=false` y `analitica=true`. Si la membresía acaba de asignarse y no aparece, inicia sesión nuevamente.
  {: .lab-note .output .compact}

- {% include step_label.html %} Con Analítica, ejecuta **consulta 4** sobre la vista enmascarada y registra el resultado permitido.

  ```sql
  SELECT transaction_id, email, phone
  FROM dbricks_de_int.p11_migration_lab.p11_vw_sales_masked
  ORDER BY transaction_id LIMIT 2;
  ```

  > **Salida esperada:** `TX11-001` y `TX11-002` devuelven `***MASKED_EMAIL***` y `***MASKED_PHONE***` sin exponer PII.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **otra consulta separada**, todavía con Analítica, ejecuta la **prueba negativa** sobre el clon. No realices `UPDATE` ni cambios a datos.

  ```sql
  SELECT email FROM dbricks_de_int.p11_migration_lab.p11_sales_migrated LIMIT 1;
  ```

  > **Salida esperada:** `INSUFFICIENT_PERMISSIONS`, `PERMISSION_DENIED` o equivalente. Si devuelve correos, registra la concesión heredada inesperada y detén la prueba; no revoques permisos institucionales.
  {: .lab-note .output .compact}

### Tarea 5.2. Comprobación final con la cuenta principal

- {% include step_label.html %} Regresa a la **cuenta principal** y al notebook **`11_migracion_accesos`**. Ejecuta la **celda SQL 25** y conserva los totales finales del clon y la vista.

  ```sql
  SELECT 'clon' AS objeto, COUNT(*) AS filas,
         CAST(SUM(amount) AS DECIMAL(14,2)) AS importe
  FROM dbricks_de_int.p11_migration_lab.p11_sales_migrated
  UNION ALL
  SELECT 'vista' AS objeto, COUNT(*) AS filas,
         CAST(SUM(amount) AS DECIMAL(14,2)) AS importe
  FROM dbricks_de_int.p11_migration_lab.p11_vw_sales_masked;
  ```

  > **Salida esperada:** Las dos filas indican `filas=20` e `importe=2100.00`. Conserva el origen, el clon y la vista para prácticas posteriores.
  {: .lab-note .output .compact}

{% capture r5 %}{{ results[4] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r5 %}

{% include support-prompt.html task="tarea5" %}
