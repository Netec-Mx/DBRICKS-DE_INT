---
layout: lab
title: "Práctica 10: Configuración completa de permisos en Unity Catalog"
permalink: /lab10/lab10/
images_base: /labs/lab10/img
duration: "40 minutos"
objective:
  - "Verificar dos grupos y dos usuarios de prueba preparados previamente, sin crearlos durante el laboratorio."
  - "Crear un esquema aislado, dos tablas Delta, una vista dinámica y un Volume exclusivo."
  - "Conceder privilegios mínimos y comprobar acceso completo, enmascarado y denegado."
  - "Identificar privilegios heredados que invaliden las pruebas de acceso denegado."
prerequisites:
  - "Cuenta principal utilizada en las prácticas 1–9, con USE CATALOG y CREATE SCHEMA sobre dbricks_de_int."
  - "Grupos de cuenta data_engineers_b2 y data_analysts_b2 creados previamente por un administrador."
  - "Dos cuentas de prueba reales, una por grupo, sin privilegios administrativos ni pertenencia cruzada; sus correos se comunicarán antes de iniciar."
  - "Cuenta principal autorizada para gestionar GRANT sobre el catálogo (o apoyo del propietario con MANAGE), y propiedad del esquema y objetos del ejercicio."
  - "Acceso a un SQL Warehouse autorizado para cada identidad y almacenamiento administrado habilitado para CREATE VOLUME."
introduction:
  - "Con la cuenta principal se verifican identidades, permisos heredados y recursos; después se prueban la vista dinámica y las denegaciones con dos usuarios independientes. No se crean grupos ni usuarios en esta práctica."
slug: lab10
lab_number: 10
final_result: >
  Dos grupos existentes verificados y dos usuarios de prueba utilizados; esquema exclusivo
  dbricks_de_int.p10_security_lab con 12 pedidos (1365.00), tabla de auditoría,
  vista de correos enmascarados y volumen propio. Se documentan lecturas permitidas
  y denegadas sin modificar los permisos de batch1.
notes:
  - "Los correos de ejemplo son marcadores: el participante utiliza los usuarios reales comunicados por el administrador."
  - "No crear grupos, usuarios, nuevas credenciales ni usar cuentas administradoras como identidades de prueba."
  - "No ejecutar REVOKE en dbricks_de_int ni batch1: la existencia de permisos heredados es una condición de bloqueo para pruebas negativas."
  - "Si la identidad de prueba tiene acceso heredado a los objetos, reportar el caso y solicitar un catálogo aislado; no declarar una denegación que no ocurrió."
  - "Antes de cambiar de usuario, SHOW GRANTS ON VIEW debe confirmar SELECT para ambos grupos; cada SHOW GRANTS va en una celda separada."
  - "Si la membresía es visible en Groups > Members, pero SQL devuelve false, cerrar sesión y volver a entrar antes de modificar identidades o privilegios."
references:
  - text: "Databricks: gestionar grupos"
    url: "https://docs.databricks.com/aws/en/admin/users-groups/manage-groups"
  - text: "Databricks: privilegios Unity Catalog"
    url: "https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges"
  - text: "Databricks: dynamic views"
    url: "https://docs.databricks.com/aws/en/views/dynamic"
prev: /lab9/lab9/
next: /lab11/lab11/
---

> **Preparación previa a cargo del administrador:** deben existir el grupo `data_engineers_b2` y el grupo `data_analysts_b2`, además de **dos usuarios diferentes**, uno por grupo. La cuenta principal del curso se utilizará para configurar los recursos, no para simular a ingeniería ni a analítica. Cada usuario de prueba debe poder iniciar sesión en este workspace y usar el SQL Warehouse autorizado. Los nombres de usuario reales se proporcionarán al inicio.
{: .lab-note .important .compact}

| Identidad | Usuario que se emplea | Uso dentro del laboratorio |
|---|---|---|
| Cuenta principal | Cuenta actual de las prácticas anteriores | Tareas 1, 2, 3 y 5: inventario, creación, GRANT y validaciones finales |
| Ingeniería | Usuario real del grupo `data_engineers_b2` (ejemplo: `lab10.engineer@tu-dominio.com`) | Tarea 4: verificar datos completos y permisos de modificación |
| Analítica | Usuario real del grupo `data_analysts_b2` (ejemplo: `lab10.analyst@tu-dominio.com`) | Tarea 4: verificar vista enmascarada y acceso directo denegado |

> **Privilegios requeridos de la cuenta principal:** `USE CATALOG` y `CREATE SCHEMA` en `dbricks_de_int`; propiedad del nuevo esquema o privilegios `CREATE TABLE`, `CREATE VIEW` y `CREATE VOLUME` para crear sus objetos; propiedad o `MANAGE` de cada objeto para conceder privilegios. La concesión de `USE CATALOG` sobre `dbricks_de_int` requiere al propietario del catálogo o un principal con `MANAGE`. Si falta alguno, debe prepararlo un administrador antes de continuar. **No se requiere ser Account Admin para ejecutar el resto del laboratorio.**
{: .lab-note .info .compact}

> **Aislamiento y herencia:** utilizaremos únicamente `dbricks_de_int.p10_security_lab`. Esto evita alterar objetos de `batch1`, pero **no elimina concesiones heredadas desde el catálogo**. Si alguno de los usuarios de prueba tiene `SELECT` o `MODIFY` por `ALL PRIVILEGES`, otro grupo, propiedad o privilegios administrativos, una prueba de denegación no es válida. No revoques privilegios compartidos; solicita un catálogo de laboratorio aislado si fuera necesario.
{: .lab-note .warning .compact}

---

{% assign results = site.data.task-results[page.slug].results %}

## 🔎 Tarea 1. Verificar grupos, usuarios y privilegios previos — 8 min

### Tarea 1.1. Confirmar las identidades existentes

- {% include step_label.html %} **Con la cuenta principal**, abre el menú de usuario de Databricks y entra en **Settings → Identity and access**. En **Management and permissions > Groups**, pulsa **Manage**. Busca `data_engineers_b2` y `data_analysts_b2`; abre cada grupo para comprobar su lista **Members**.

  > **Salida esperada:** Se muestran ambos grupos existentes. El usuario de ingeniería pertenece a `data_engineers_b2` y el de analítica a `data_analysts_b2`. Si no aparecen, solicita al administrador que los prepare: **no los crees en esta práctica**.
  {: .lab-note .output .compact}

- {% include step_label.html %} Sin salir de **Settings → Identity and access**, en **Users** pulsa **Manage**. Busca los **dos correos reales** facilitados por el administrador y revisa que estén activos, tengan acceso al workspace y no sean administradores.

  > **Salida esperada:** Existen dos usuarios distintos y utilizables. Ninguno es Workspace Admin, Account Admin ni propietario de los recursos del laboratorio. Si la pantalla no permite consultar membresías o roles, solicita confirmación al administrador.
  {: .lab-note .output .compact}

- {% include step_label.html %} En **Workspace → Home**, abre tu carpeta de trabajo (por ejemplo `batch_1`). Crea el notebook **`10_unity_catalog`**, con lenguaje **SQL**, conecta el cómputo autorizado y ejecuta su **celda SQL 1**.

  ```sql
  SELECT current_user() AS usuario_principal,
         current_catalog() AS catalogo_actual,
         current_schema() AS esquema_actual;
  ```

  > **Salida esperada:** Se muestra el usuario principal real. Mantendrás esta sesión para crear y configurar los objetos.
  {: .lab-note .output .compact}

### Tarea 1.2. Identificar concesiones heredadas antes de operar

- {% include step_label.html %} En `10_unity_catalog`, agrega la **celda SQL 2** debajo de la anterior. Inspecciona el catálogo compartido antes de crear el esquema.

  ```sql
  SHOW GRANTS ON CATALOG dbricks_de_int;
  ```

  > **Salida esperada:** Puedes identificar concesiones existentes. Revisa si `account users`, grupos amplios o alguno de los dos usuarios de prueba recibe privilegios heredables como `ALL PRIVILEGES`, `SELECT` o `MODIFY`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 3**, verifica que puedes usar el catálogo y que el nombre del esquema exclusivo no corresponde a un esquema institucional.

  ```sql
  SHOW SCHEMAS IN dbricks_de_int LIKE 'p10_security_lab';
  ```

  > **Salida esperada:** El esquema todavía no aparece si es la primera ejecución. Si ya existe, comprueba con el instructor su propiedad antes de reutilizarlo; no sobrescribas recursos de otro participante.
  {: .lab-note .output .compact}

{% capture r1 %}{{ results[0] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r1 %}
{% include support-prompt.html task="tarea1" %}

---

## ⚙️ Tarea 2. Crear recursos aislados de Unity Catalog — 9 min

### Tarea 2.1. Crear esquema y tabla de pedidos

- {% include step_label.html %} **Con la cuenta principal**, en `10_unity_catalog` ejecuta la **celda SQL 4** para crear el esquema exclusivo. Requiere `USE CATALOG` y `CREATE SCHEMA`.

  ```sql
  CREATE SCHEMA IF NOT EXISTS dbricks_de_int.p10_security_lab;
  ```

  > **Salida esperada:** Existe `dbricks_de_int.p10_security_lab`, separado de `batch1`. Si obtienes `PERMISSION_DENIED`, detente y solicita los privilegios indicados en los requisitos.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 5**, revisa las concesiones del esquema recién creado, incluyendo las que Databricks muestre como heredadas.

  ```sql
  SHOW GRANTS ON SCHEMA dbricks_de_int.p10_security_lab;
  ```

  > **Salida esperada:** El ejecutor puede administrar el esquema. Si alguno de los usuarios de prueba ya tiene `SELECT` o `MODIFY` efectivos por herencia, suspende las pruebas negativas y solicita un catálogo aislado; no ejecutes `REVOKE` sobre el catálogo compartido.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 6**, crea `p10_orders_sensitive` y carga **únicamente datos sintéticos**. Ejecuta ambas sentencias en orden; requieren creación de tablas y capacidad de insertar datos.

  ```sql
  CREATE TABLE IF NOT EXISTS dbricks_de_int.p10_security_lab.p10_orders_sensitive (
    order_id STRING, customer_email STRING, region STRING,
    amount DECIMAL(12,2), status STRING
  ) USING DELTA;

  INSERT OVERWRITE dbricks_de_int.p10_security_lab.p10_orders_sensitive
  SELECT format_string('P10-%03d', id + 1),
         format_string('cliente%02d@ejemplo.test', id + 1),
         CASE WHEN id % 3 = 0 THEN 'Norte'
              WHEN id % 3 = 1 THEN 'Centro' ELSE 'Sur' END,
         CAST((id + 1) * 17.50 AS DECIMAL(12,2)),
         'COMPLETED'
  FROM range(12);
  ```

  > **Salida esperada:** La tabla contiene `P10-001` a `P10-012`, todos con correos ficticios `@ejemplo.test`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 7**, comprueba los datos generados antes de conceder permisos a los grupos.

  ```sql
  SELECT COUNT(*) AS pedidos,
         COUNT(DISTINCT order_id) AS ids_unicos,
         CAST(SUM(amount) AS DECIMAL(12,2)) AS importe_total
  FROM dbricks_de_int.p10_security_lab.p10_orders_sensitive;
  ```

  > **Salida esperada:** `pedidos=12`, `ids_unicos=12` e `importe_total=1365.00`.
  {: .lab-note .output .compact}

### Tarea 2.2. Crear auditoría y volumen propios

- {% include step_label.html %} En la **celda SQL 8**, crea la tabla de auditoría y registra su único evento inicial.

  ```sql
  CREATE TABLE IF NOT EXISTS dbricks_de_int.p10_security_lab.p10_access_audit (
    event_id STRING, event_type STRING, event_timestamp TIMESTAMP,
    description STRING
  ) USING DELTA;

  INSERT OVERWRITE dbricks_de_int.p10_security_lab.p10_access_audit
  SELECT 'EVT-001', 'BASELINE', current_timestamp(),
         'Configuración inicial de permisos P10';
  ```

  > **Salida esperada:** La tabla contiene el evento `EVT-001` de tipo `BASELINE`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 9**, crea un **Managed Volume** del laboratorio, sin tocar `dbricks_de_int.batch1.landing`.

  ```sql
  CREATE VOLUME IF NOT EXISTS dbricks_de_int.p10_security_lab.p10_files;
  SHOW VOLUMES IN dbricks_de_int.p10_security_lab;
  ```

  > **Salida esperada:** Se encuentra el volumen `p10_files`. Si falta almacenamiento administrado o `CREATE VOLUME`, pide apoyo al administrador; no sustituyas este recurso por `landing`.
  {: .lab-note .output .compact}

{% capture r2 %}{{ results[1] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r2 %}
{% include support-prompt.html task="tarea2" %}

---

## 🛡️ Tarea 3. Configurar GRANT y la vista dinámica — 10 min

### Tarea 3.1. Conceder acceso mínimo por grupo

- {% include step_label.html %} **Con la cuenta principal**, en `10_unity_catalog` ejecuta la **celda SQL 10**. El propietario del catálogo, o quien tenga `MANAGE`, debe conceder `USE CATALOG` a ambos grupos; si no eres ese principal, solicita que ejecute únicamente estas dos sentencias.

  ```sql
  GRANT USE CATALOG ON CATALOG dbricks_de_int TO `data_engineers_b2`;
  GRANT USE CATALOG ON CATALOG dbricks_de_int TO `data_analysts_b2`;
  ```

  > **Salida esperada:** Ambos grupos tienen `USE CATALOG`. No se concede `SELECT` ni `ALL PRIVILEGES` en todo `dbricks_de_int`.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 11**, como propietario del esquema, concede `USE SCHEMA` a ambos grupos sin otorgar acceso a todas las tablas.

  ```sql
  GRANT USE SCHEMA ON SCHEMA dbricks_de_int.p10_security_lab TO `data_engineers_b2`;
  GRANT USE SCHEMA ON SCHEMA dbricks_de_int.p10_security_lab TO `data_analysts_b2`;
  ```

  > **Salida esperada:** Los grupos pueden resolver los nombres del esquema, pero `USE SCHEMA` por sí solo no les concede `SELECT` sobre pedidos.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 12**, otorga a Ingeniería `SELECT` y `MODIFY` sobre pedidos, lectura de auditoría y permisos de lectura/escritura **solo** sobre `p10_files`.

  ```sql
  GRANT SELECT, MODIFY ON TABLE dbricks_de_int.p10_security_lab.p10_orders_sensitive TO `data_engineers_b2`;
  GRANT SELECT ON TABLE dbricks_de_int.p10_security_lab.p10_access_audit TO `data_engineers_b2`;
  GRANT READ VOLUME, WRITE VOLUME ON VOLUME dbricks_de_int.p10_security_lab.p10_files TO `data_engineers_b2`;
  ```

  > **Salida esperada:** Ingeniería recibe estos privilegios sobre objetos específicos, sin capacidad de administrarlos ni permiso nuevo sobre `batch1`.
  {: .lab-note .output .compact}

### Tarea 3.2. Crear la vista y validar los permisos antes de las pruebas

- {% include step_label.html %} En la **celda SQL 13**, crea la vista dinámica. La condición evalúa **la identidad del usuario que consulta**, no la del propietario que crea la vista.

  ```sql
  CREATE OR REPLACE VIEW dbricks_de_int.p10_security_lab.p10_vw_orders_analytics AS
  SELECT order_id, region, amount, status,
         CASE WHEN is_account_group_member('data_engineers_b2') THEN customer_email
              WHEN customer_email IS NULL THEN NULL
              ELSE regexp_replace(customer_email, '^(.).*(@[^@]+)$', '$1***$2')
         END AS customer_email
  FROM dbricks_de_int.p10_security_lab.p10_orders_sensitive;
  ```

  > **Salida esperada:** La vista preserva los 12 pedidos y muestra correo completo a Ingeniería, pero `c***@ejemplo.test` a Analítica.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 14**, concede `SELECT` sobre la vista a ambos grupos. Analítica **no** debe recibir `SELECT` sobre la tabla sensible.

  ```sql
  GRANT SELECT ON VIEW dbricks_de_int.p10_security_lab.p10_vw_orders_analytics TO `data_engineers_b2`;
  GRANT SELECT ON VIEW dbricks_de_int.p10_security_lab.p10_vw_orders_analytics TO `data_analysts_b2`;
  ```

  > **Salida esperada:** Los dos grupos tienen `SELECT` sobre la vista; solamente Ingeniería tiene `SELECT` directo sobre `p10_orders_sensitive`.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Con la cuenta principal**, en `10_unity_catalog`, crea la **celda SQL 15** para revisar exclusivamente los permisos de la **tabla sensible**. Ejecuta esta consulta por separado.

  ```sql
  SHOW GRANTS ON TABLE dbricks_de_int.p10_security_lab.p10_orders_sensitive;
  ```

  > **Salida esperada:** `data_engineers_b2` tiene `SELECT` y `MODIFY`. No debe aparecer una concesión directa de `SELECT` para `data_analysts_b2`. Revisa también los privilegios heredados antes de realizar pruebas negativas.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 16**, revisa **solo los permisos de la vista dinámica**. Este es un control **obligatorio** antes de comenzar la Tarea 4.

  ```sql
  SHOW GRANTS ON VIEW dbricks_de_int.p10_security_lab.p10_vw_orders_analytics;
  ```

  > **Salida esperada:** Deben aparecer **dos filas `SELECT`**, una con `Principal=data_engineers_b2` y otra con `Principal=data_analysts_b2`. Si solo aparece Ingeniería, **no cambies de usuario todavía**: aplica la corrección del siguiente paso.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Solo si falta `SELECT` para Analítica** en la celda 16, ejecuta la **celda SQL 17** con la cuenta propietaria; después **vuelve a ejecutar la celda 16** para verificar que el permiso quedó registrado. Si ya existe, omite el `GRANT`.

  ```sql
  GRANT SELECT ON VIEW dbricks_de_int.p10_security_lab.p10_vw_orders_analytics
  TO `data_analysts_b2`;
  ```

  > **Salida esperada:** La siguiente ejecución de `SHOW GRANTS ON VIEW` muestra `SELECT` tanto para Ingeniería como para Analítica. **No concedas `SELECT` sobre `p10_orders_sensitive` a Analítica.**
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 18**, comprueba por separado los privilegios de **auditoría**; no combines varios `SHOW GRANTS` en una sola celda, porque podrías ver únicamente la última salida.

  ```sql
  SHOW GRANTS ON TABLE dbricks_de_int.p10_security_lab.p10_access_audit;
  ```

  > **Salida esperada:** Ingeniería tiene `SELECT` sobre `p10_access_audit`; Analítica no recibe un `SELECT` directo en esta tabla.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 19**, comprueba los privilegios del **Volume exclusivo**.

  ```sql
  SHOW GRANTS ON VOLUME dbricks_de_int.p10_security_lab.p10_files;
  ```

  > **Salida esperada:** `data_engineers_b2` muestra `READ VOLUME` y `WRITE VOLUME`. Que aparezcan solo dos filas aquí es normal: esta consulta verifica únicamente el Volume.
  {: .lab-note .output .compact}

- {% include step_label.html %} **Antes de pasar a la Tarea 4**, confirma en las **celdas 15 y 16** que Ingeniería tiene `SELECT` en tabla y vista, y Analítica **solo en la vista**. Confirma también las concesiones `USE CATALOG` y `USE SCHEMA` para ambos grupos; si faltan, solicita a la cuenta propietaria que las conceda. Si existen permisos amplios heredados, detén las pruebas de denegación.

  > **Salida esperada:** Ambos grupos pueden resolver `dbricks_de_int.p10_security_lab`; solo Ingeniería tiene lectura directa de los pedidos; **ambos** tienen `SELECT` en `p10_vw_orders_analytics`. No inicies la prueba de Analítica sin cumplir estas condiciones.
  {: .lab-note .output .compact}

{% capture r3 %}{{ results[2] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r3 %}
{% include support-prompt.html task="tarea3" %}

---

## 📊 Tarea 4. Probar con el usuario de Ingeniería y el de Analítica — 8 min

### Tarea 4.1. Sesión exclusiva de Ingeniería

- {% include step_label.html %} **Cierra sesión de la cuenta principal** o abre una sesión privada y entra en Databricks con el **usuario de Ingeniería** proporcionado previamente. Abre **SQL Editor → New query** y selecciona el SQL Warehouse autorizado. Ejecuta la **consulta 1**.

  ```sql
  SELECT current_user() AS usuario,
         is_account_group_member('data_engineers_b2') AS es_ingeniero,
         is_account_group_member('data_analysts_b2') AS es_analista;
  ```

  > **Salida esperada:** `es_ingeniero=true` y `es_analista=false`. Si `false` aparece aunque el usuario ya figura en **Settings → Identity and access → Groups → Members**, **cierra por completo la sesión de prueba y vuelve a iniciar sesión**; repite la consulta. Puede existir un retraso de propagación o contexto de sesión anterior. Si persiste, solicita al administrador que valide el grupo y no continúes.
  {: .lab-note .output .compact}

- {% include step_label.html %} **En la misma sesión de Ingeniería**, agrega la **consulta 2** en SQL Editor; primero consulta la tabla original y después la vista (ejecuciones separadas).

  ```sql
  SELECT order_id, customer_email
  FROM dbricks_de_int.p10_security_lab.p10_orders_sensitive
  ORDER BY order_id LIMIT 2;
  ```

  ```sql
  SELECT order_id, customer_email
  FROM dbricks_de_int.p10_security_lab.p10_vw_orders_analytics
  ORDER BY order_id LIMIT 2;
  ```

  > **Salida esperada:** En ambas consultas aparecen `cliente01@ejemplo.test` y `cliente02@ejemplo.test`, sin máscara. Registra el usuario de la sesión y ambos resultados.
  {: .lab-note .output .compact}

### Tarea 4.2. Sesión exclusiva de Analítica

- {% include step_label.html %} Cierra la sesión de Ingeniería e inicia otra con el **usuario de Analítica**. En **SQL Editor → New query**, selecciona el SQL Warehouse y ejecuta la **consulta 3**.

  ```sql
  SELECT current_user() AS usuario,
         is_account_group_member('data_engineers_b2') AS es_ingeniero,
         is_account_group_member('data_analysts_b2') AS es_analista;
  ```

  > **Salida esperada:** `es_ingeniero=false` y `es_analista=true` con el correo real del usuario analítico. Si `es_analista=false` pese a aparecer en **Groups → Members**, cierra sesión, vuelve a ingresar y repite la consulta antes de avanzar.
  {: .lab-note .output .compact}

- {% include step_label.html %} **En la misma sesión de Analítica**, ejecuta la **consulta 4** sobre la vista dinámica; no cambies de identidad durante su ejecución.

  ```sql
  SELECT order_id, customer_email
  FROM dbricks_de_int.p10_security_lab.p10_vw_orders_analytics
  ORDER BY order_id LIMIT 2;
  ```

  > **Salida esperada:** Aparecen `P10-001 | c***@ejemplo.test` y `P10-002 | c***@ejemplo.test`. Si aparece `INSUFFICIENT_PERMISSIONS` para **la vista**, regresa a la cuenta principal y verifica `SHOW GRANTS ON VIEW` (Tarea 3, celda 16): deben figurar ambos grupos con `SELECT`. Concede el permiso faltante solo sobre la vista, confirma la concesión y vuelve a iniciar sesión con Analítica antes de repetir esta consulta.
  {: .lab-note .output .compact}

- {% include step_label.html %} En otra consulta de **SQL Editor**, todavía como Analítica, intenta leer **directamente** la tabla sensible. Esta es una prueba negativa deliberada: no requiere modificar datos.

  ```sql
  SELECT customer_email
  FROM dbricks_de_int.p10_security_lab.p10_orders_sensitive
  LIMIT 1;
  ```

  > **Salida esperada:** `PERMISSION_DENIED` o mensaje equivalente. **Si devuelve un correo**, no declares éxito: inspecciona privilegios efectivos y herencia, detén las pruebas negativas y solicita un entorno aislado.
  {: .lab-note .output .compact}

{% capture r4 %}{{ results[3] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r4 %}
{% include support-prompt.html task="tarea4" %}

---

## ✅ Tarea 5. Confirmar integridad y documentar los permisos — 5 min

### Tarea 5.1. Comprobar resultados con la cuenta principal

- {% include step_label.html %} **Cierra la sesión de Analítica** y vuelve a iniciar con la **cuenta principal**. Abre `Workspace → batch_1 → 10_unity_catalog`, conecta el cómputo y ejecuta la **celda SQL 20** para revisar el total de pedidos tras las pruebas.

  ```sql
  SELECT COUNT(*) AS pedidos,
         CAST(SUM(amount) AS DECIMAL(12,2)) AS importe_total
  FROM dbricks_de_int.p10_security_lab.p10_orders_sensitive;
  ```

  > **Salida esperada:** `pedidos=12`, `importe_total=1365.00`. Las pruebas de acceso no modifican pedidos.
  {: .lab-note .output .compact}

- {% include step_label.html %} En la **celda SQL 21**, confirma que la tabla de auditoría conserva su registro inicial.

  ```sql
  SELECT event_id, event_type
  FROM dbricks_de_int.p10_security_lab.p10_access_audit;
  ```

  > **Salida esperada:** Una fila `EVT-001 | BASELINE`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Registra una matriz de permisos en tus notas, con las **dos cuentas reales** y los resultados observados. No intentes provocar denegaciones ejecutando `UPDATE` ni modifiques concesiones institucionales.

  | Cuenta de prueba | Tabla sensible | Vista dinámica | Auditoría | Volumen propio |
  |---|---|---|---|---|
  | Ingeniería | `SELECT`, `MODIFY` concedidos | `SELECT`, correo completo | `SELECT` | `READ VOLUME`, `WRITE VOLUME` |
  | Analítica | `SELECT` denegado si no existe herencia | `SELECT`, correo enmascarado | Sin `SELECT` concedido | Sin permisos concedidos |

  > **Salida esperada:** Se documentan dos usuarios reales, resultados positivos y negativos, 12 pedidos, 1365.00 y los recursos exclusivos del Schema `p10_security_lab`.
  {: .lab-note .output .compact}

- {% include step_label.html %} Conserva el notebook `10_unity_catalog`, la vista `p10_vw_orders_analytics`, ambas tablas y el Volume `p10_files`. No elimines los grupos ni cambies permisos de `batch1`.

  > **Salida esperada:** Los recursos de la Práctica 10 permanecen en `dbricks_de_int.p10_security_lab` para revisión posterior.
  {: .lab-note .output .compact}

{% capture r5 %}{{ results[4] }}{% endcapture %}
{% include task-result.html title="Tarea finalizada" content=r5 %}
{% include support-prompt.html task="tarea5" %}
