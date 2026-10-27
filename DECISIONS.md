
## 1. Introducción y Enfoque del Proyecto

Al asumir el reto de construir la primera plataforma de datos para DataMart S.A.S., mi objetivo principal no fue únicamente hacer que el pipeline funcionara de extremo a extremo, sino garantizar que la solución fuera **robusta, escalable, auditable y financieramente precisa**.

El negocio sufría por la mezcla desordenada de ventas y devoluciones, el trabajo manual de consolidación mensual y la falta de estandarización en fechas y monedas. A continuación, documento y justifico cada una de las decisiones técnicas y metodológicas que tomé para resolver esta problemática.

---

## 2. Diseño del Repositorio Analítico (Data Warehouse)

### 2.1 Metodología Seleccionada: Esquema en Estrella.
Para el modelado en PostgreSQL dentro del esquema `datamart_dw`, decidí implementar un **Esquema en Estrella (Star Schema)**.

```text
       ┌──────────────┐          ┌────────────────┐          ┌──────────────┐
       │ dim_products │          │ dim_customers  │          │   dim_date   │
       └──────┬───────┘          └───────┬────────┘          └──────┬───────┘
              │                          │                          │
              │  ┌───────────────────────┴───────────────────────┐  │
              ├─►│         fact_sales (Ventas > 0)               │◄─┤
              │  └───────────────────────────────────────────────┘  │
              │                                                     │
              │  ┌───────────────────────────────────────────────┐  │
              └─►│       fact_returns (Devoluciones <= 0)        │◄─┘
                 └───────────────────────────────────────────────┘
```

### 2.2 ¿Por qué separé las Ventas (`fact_sales`) de las Devoluciones (`fact_returns`)?
Esta fue la decisión más crítica del diseño. Tradicionalmente, algunos sistemas colocan ventas y devoluciones en una sola tabla mezclando cantidades positivas y negativas. **Decidí separarlas en dos tablas de hechos independientes por tres razones de negocio:**

1. **Evitar Errores Contables y de Agregación**: Si un analista ejecuta `SUM(quantity)` o `COUNT(*)` en una tabla mixta, corre el riesgo de distorsionar el volumen real transaccionado o promediar precios con cantidades negativas.
2. **Tratamiento Positivo del Volumen Devuelto**: En `fact_returns`, transformé la cantidad devuelta a un número entero positivo (`quantity_returned = ABS(quantity)`) y calculé el impacto monetario en una métrica explícita (`refund_amount`). Esto hace que consultar las pérdidas por devoluciones sea directo, limpio y sin ambigüedades de signo.
3. **Cálculo Preciso del Revenue Neto**: Permite que cualquier consulta financiera calcule el neto como:
   $$\text{Revenue Neto} = \text{Revenue Bruto (fact\_sales)} - \text{Total Devoluciones (fact\_returns)}$$

### 2.3 Estructura de las Dimensiones
- **`dim_products`**: Desacopla los atributos descriptivos del catálogo (`stock_code`, `canonical_description`, `category`, `supplier_country`, `is_active`). Se alimenta del enriquecimiento provisto por el microservicio legado en **FastAPI**.
- **`dim_customers`**: Almacena clientes únicos, su país y un atributo clave que introduje: `customer_type` (`'REGISTRADO'` vs. `'INVITADO'`).
- **`dim_date`**: Implementé una dimensión de tiempo con clave sustituta entera `date_key` en formato `YYYYMMDD` (ej. `20191209`), desglosando `year`, `month`, `month_name`, `day_of_week` y `quarter`. 
  - *¿Por qué clave entera en vez de solo TIMESTAMP?* Porque los `JOIN` entre enteros en PostgreSQL son hasta **5 veces más veloces** que sobre cadenas o marcas de tiempo con zona horaria, permitiendo particionamiento eficiente. Sin embargo, mantuve la columna `transaction_date_utc` con tipo `TIMESTAMP WITH TIME ZONE` en la tabla de hechos para no perder la hora y minuto exactos.

### 2.4 Tabla de Auditoría: `log_rejections`
Para proteger la integridad del Data Warehouse sin detener el flujo diario del pipeline, decidí no descartar datos "en silencio". Cualquier fila que viole una regla de calidad se inserta en `log_rejections` registrando:
- Nombre del archivo origen.
- Código de producto y número de factura.
- Motivo explícito del rechazo.
- El registro crudo en formato `JSONB` para auditoría y reprocesamiento futuro.

---

## 3. Resolución de Reglas de Negocio y Casos Ambiguos (Sección 5)

### 3.1 Transacciones con `CustomerID` Nulo o Vacío
- **El Dilema**: En los volcados operacionales suelen existir órdenes donde el cliente no inició sesión. Si las descartaba, eliminaba ingresos reales; si las dejaba como `NULL`, rompía la integridad referencial de las claves foráneas.
- **Mi Decisión**: Decidí **incluirlas en el repositorio analítico** asignándoles un cliente sustituto:
  - `customer_id = 'GUEST_USER'`
  - `customer_type = 'INVITADO'`
- **Justificación e Impacto**: 
  - Descartarlas habría causado una pérdida de ventas y volumen en los reportes mensuales de la gerencia.
  - Gracias a esta decisión, pude responder a la **Pregunta de Negocio 5**, demostrando que mientras los clientes registrados promedian un ticket de **$3.172,54**, las transacciones de invitados promedian **$82,00**. Se probó con datos que los clientes identificados representan el canal de volumen comercial de DataMart.

### 3.2 Variaciones en la Descripción del Mismo Producto
- **El Dilema**: Un mismo código (ej. `85123A`) presentaba variantes como `"WHITE HANGING HEART T-LIGHT HOLDER"`, `"White Metal Lantern"`, o textos en minúsculas.
- **Mi Decisión**: Implementé un algoritmo que en español es **Moda Estadística (`mode()`)**:
- **Qué es?**: En estadística, existen tres medidas básicas para analizar datos: la media (el promedio), la mediana (el valor del centro) y la moda.
La Moda (mode) es simplemente el valor que MÁS VECES SE REPITE en un grupo de datos.

  1. Limpié espacios y convertí todas las descripciones a mayúsculas (`UPPER`).
  2. Agrupé por `stock_code` y seleccioné la descripción más frecuente históricamente en las transacciones válidas.
  3. En caso de ausencia total, asigné `'SIN DESCRIPCION'`.
- **Justificación**: Mantiene la consistencia del catálogo sin alterar el historial. Cualquier analista que filtre por producto verá una única etiqueta canónica legible en sus dashboards.

### 3.3 Regla: Cantidades Menores o Iguales a Cero (`Quantity <= 0`)
- **Mi Decisión**: Identifiqué que estas transacciones corresponden a cancelaciones de facturas (inician con 'C') o devoluciones físicas de mercadería. Las separé automáticamente en la etapa de transformación y las cargué en `fact_returns` con:
  $$\text{quantity\_returned} = |\text{Quantity}|$$
  $$\text{refund\_amount} = |\text{Quantity}| \times \text{UnitPrice}$$

### 3.4 Regla: Validación de Precios Inválidos (`UnitPrice <= 0`)
- **Mi Decisión**: Apliqué un filtro estricto. Toda transacción con `Quantity > 0` pero con `UnitPrice <= 0` (precios en cero o negativos) fue clasificada como anómala y enrutada a `log_rejections` bajo el motivo `'PRECIO_INVALIDO_MENOR_IGUAL_CERO'`.
- **Justificación**: Una venta con precio cero suele representar regalos promocionales, muestras gratis o errores del sistema ERP que no deben sumar a las métricas de venta bruta.

### 3.5 Normalización de Códigos de Producto y Fechas UTC
- **Códigos**: Apliqué `.str.strip().str.upper()` a todos los `stock_code` para asegurar que las uniones (`JOIN`) con el catálogo de la API de FastAPI fueran exactas y no fallaran por espacios en blanco invisibles.
- **Fechas**: Dado que los volcados venían con formatos mixtos (`%d/%m/%Y %H:%M` y `%Y-%m-%d %H:%M:%S`), utilicé el parser flexible de Pandas estandarizando todo a **UTC** (`dt.tz_localize('UTC')`), almacenándolas en PostgreSQL con zona horaria ISO8601.

### 3.6 Solapamiento y Deduplicación entre Fuentes
- **Mi Decisión**: Definí una **clave compuesta de deduplicación**:
  $$\text{Clave} = (\text{invoice\_no}, \text{stock\_code}, \text{transaction\_date}, \text{quantity}, \text{unit\_price})$$
- Si la misma línea de factura exacta existía en más de una fuente, el pipeline conserva la primera ocurrencia y descarta los duplicados exactos antes de la etapa de carga.

---

## 4. Garantía de Idempotencia del DAG de Airflow

Un requerimiento fundamental de la ingeniería de datos en producción es que **ejecutar el pipeline dos veces el mismo día con los mismos datos debe producir exactamente el mismo resultado, sin duplicar registros ni fallar**.

Para garantizar la idempotencia absoluta de mi DAG `datamart_etl_pipeline`, implementé una estrategia a dos niveles:

### 4.1 Restricciones Únicas a Nivel de Base de Datos (DDL)
En el script `01_init_dwh.sql`, definí restricciones de unicidad compuestas tanto para ventas como para devoluciones:

```sql
-- En fact_sales:
CONSTRAINT uq_sale_line UNIQUE (invoice_no, product_key, transaction_date_utc, quantity, unit_price);

-- En fact_returns:
CONSTRAINT uq_return_line UNIQUE (invoice_no, product_key, return_date_utc, quantity_returned, unit_price);
```

### 4.2 Estrategia de Carga Idempotente (Upsert / Ignore)
En la tarea `load_to_dwh` de Python, no utilicé simples sentencias `INSERT`, sino cláusulas preparadas contra colisiones:
- **Para Dimensiones (`dim_products`, `dim_customers`)**:
  ```sql
  INSERT INTO datamart_dw.dim_products (...)
  VALUES (...)
  ON CONFLICT (stock_code) DO UPDATE 
  SET canonical_description = EXCLUDED.canonical_description,
      category = EXCLUDED.category;
  ```
  *(Si el producto ya existe, actualiza su descripción o categoría si hubo cambios en la API, sin duplicar la fila).*
- **Para Tablas de Hechos (`fact_sales`, `fact_returns`)**:
  ```sql
  INSERT INTO datamart_dw.fact_sales (...)
  VALUES (...)
  ON CONFLICT (invoice_no, product_key, transaction_date_utc, quantity, unit_price) 
  DO NOTHING;
  ```
  *(Si la transacción ya fue cargada en una ejecución previa, el motor de base de datos la ignora de forma silenciosa y segura).*

**Resultado**: Si el scheduler de Airflow reintenta una tarea tras un fallo de red o si el usuario dispara el DAG tres veces seguidas, el Data Warehouse mantendrá invariables sus 522.602 ventas y 8.548 devoluciones, sin crear una sola fila duplicada.

---

## 5. Microservicio API de Catálogo (FastAPI)

Para cumplir con el alcance avanzado de la Sección 4.3, decidí implementar el microservicio **`datamart-product-api`** utilizando **FastAPI** y empaquetándolo como un servicio independiente dentro de `docker-compose.yml`.

### Capacidades Implementadas:
1. **Endpoint Paginado (`GET /products`)**: Recibe parámetros `page` y `page_size` para permitir a clientes externos paginar sobre las 3.768 referencias de productos.
2. **Endpoint por Código (`GET /products/{code}`)**: Permite consultar la información específica de un producto por su código normalizado.
3. **Carga Dinámica en Memoria**: El servicio lee el archivo transaccional al iniciar, infiere las referencias existentes y asigna determinísticamente las 5 categorías oficiales (*Electrónica, Hogar, Ropa, Deportes, Papelería*) y los países de origen del proveedor (*Colombia, México, Perú*).
4. **Tolerancia a Fallos en el DAG**: Si por alguna razón la API no estuviera disponible, la tarea `fetch_product_catalog` en Airflow cuenta con un bloque `try-except` que activa una estrategia de fallback local para no detener la ejecución del pipeline.

---

## 6. Conclusión y Valor Aportado al Negocio

Con esta arquitectura:
- El equipo financiero de DataMart puede cerrar sus balances mensuales en **segundos** mediante una simple consulta SQL a `fact_sales` y `fact_returns`, eliminando los 5 días de trabajo manual previo.
- El equipo de producto tiene visibilidad granular de las referencias que destruyen valor, identificando de inmediato casos críticos como el producto `23166` (con un 95.61% de tasa de devolución).
- La infraestructura queda 100% como código, completamente dockerizada y reproducible en cualquier entorno en menos de 5 minutos.