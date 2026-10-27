````markdown
# 🛒 DataMart S.A.S. — Pipeline ETL & Data Warehouse con Apache Airflow

Solución integral de ingeniería de datos de extremo a extremo para **DataMart S.A.S.** El pipeline ingesta datos transaccionales heterogéneos, los limpia aplicando reglas estrictas de negocio, los enriquece consumiendo una API REST interna construida en **FastAPI**, y los carga de forma idempotente en un Data Warehouse dimensional en **PostgreSQL**, todo orquestado con **Apache Airflow** dentro de **Docker**.

---

## 📋 Tabla de Contenidos
1. [Contexto del Negocio y Objetivos](#-contexto-del-negocio-y-objetivos)
2. [Arquitectura del Sistema](#-arquitectura-del-sistema)
3. [Estructura del Proyecto](#-estructura-del-proyecto)
4. [Guía Rápida de Despliegue (< 5 minutos)](#-guía-rápida-de-despliegue--5-minutos)
5. [Validación de Servicios, Conexiones y Variables](#-validación-de-servicios-conexiones-y-variables)
6. [Diseño del Data Warehouse (Star Schema)](#-diseño-del-data-warehouse-star-schema)
7. [Documento de Decisiones Técnicas y Ambigüedades](#-documento-de-decisiones-técnicas-y-ambigüedades)
8. [Respuestas y Consultas SQL de Negocio (Sección 7)](#-respuestas-y-consultas-sql-de-negocio-sección-7)
9. [Comandos de Mantenimiento](#-comandos-de-mantenimiento)

---

## 🏢 Contexto del Negocio y Objetivos

DataMart S.A.S. experimentó un crecimiento del 40% en transacciones, lo que paralizó a sus equipos de analítica y finanzas:
- Los datos de ventas se encontraban en volcados planos sin separación clara entre ventas y devoluciones.
- El catálogo de productos residía en un sistema legado sin estructura analítica.
- Existían diferencias en monedas, formatos de fecha y zonas horarias.
- El cierre financiero mensual tardaba entre 3 y 5 días de trabajo manual.

### Objetivos Alcanzados:
- **Automatización Docker 100%**: Levantamiento de la plataforma completa con un único comando (`docker-compose up -d`).
- **Cero Intervención Manual**: Inyección automática de conexiones (`AIRFLOW_CONN_*`) y variables (`AIRFLOW_VAR_*`).
- **Enriquecimiento de Datos**: Consumo de un microservicio FastAPI para clasificar productos en 5 categorías de negocio (*Electrónica, Hogar, Ropa, Deportes, Papelería*).
- **Separación Contable Estricta**: Aislamiento de ventas válidas (`fact_sales`), devoluciones tratadas en positivo (`fact_returns`) y registros defectuosos (`log_rejections`).
- **Idempotencia Garantizada**: Ejecuciones repetidas del DAG producen exactamente el mismo estado sin duplicar información.

---

## 🏛️ Arquitectura del Sistema

```text
                                 ┌────────────────────────┐
                                 │  FastAPI (Port 8000)   │
                                 │ Catálogo Legado Prod.  │
                                 └───────────┬────────────┘
                                             │ HTTP REST
                                             ▼
┌─────────────────────────┐      ┌────────────────────────┐      ┌─────────────────────────┐
│   Fuente Transaccional  │ ───► │  Apache Airflow 2.8.1  │ ───► │ PostgreSQL DWH (:5433)  │
│ Sales Transaction v.4a  │      │   Orquestador ETL      │      │     (Star Schema)       │
└─────────────────────────┘      └────────────────────────┘      └─────────────────────────┘
                                             │
                                             ▼
                                 ┌────────────────────────┐
                                 │ Postgres Metadata (:5432)
                                 │ Metadatos de Airflow   │
                                 └────────────────────────┘
```

### Servicios Contenerizados:
1. **`airflow-webserver` & `airflow-scheduler`**: Motor de Airflow 2.8.1 (LocalExecutor).
2. **`airflow-postgres`**: Base de datos dedicada exclusivamente a los metadatos de Airflow (`5432`).
3. **`dwh-postgres`**: Repositorio Analítico (Data Warehouse) en PostgreSQL con esquema `datamart_dw` (`5433`).
4. **`datamart-product-api`**: Microservicio FastAPI que expone endpoints paginados y por código del catálogo (`8000`).

---

## 📁 Estructura del Proyecto

```text
prueba-DataMart/
├── .env.example               # Plantilla de variables de entorno
├── .env                       # Variables de entorno activas (excluido en .gitignore)
├── .gitignore                 # Reglas de exclusión para Git
├── docker-compose.yml         # Orquestación de todos los contenedores
├── README.md                  # Documentación maestra y guía de entrega
├── api/                       # Microservicio FastAPI
│   ├── Dockerfile
│   ├── pyproject.toml
│   └── main.py
├── dags/                      # DAGs de Apache Airflow
│   └── datamart_etl_pipeline.py
├── data/                      # Volúmenes de datos transaccionales
│   └── Sales Transaction v.4a.csv
└── sql/                       # Scripts DDL y consultas de negocio
    ├── 01_init_dwh.sql        # Creación de esquema y tablas Star Schema
    └── 02_business_questions.sql # Consultas SQL que responden a la Sección 7
```

---

## ⚡ Guía Rápida de Despliegue (< 5 minutos)

### 1. Clonar el repositorio
```bash
git clone <URL_DE_TU_REPOSITORIO>
cd prueba-DataMart
```

### 2. Configurar variables de entorno
```bash
cp .env.example .env
```
*(En Windows PowerShell: `copy .env.example .env`)*

### 3. Verificar el archivo de datos
Confirma que el archivo transaccional esté presente en la carpeta `data/`:
```bash
data/Sales Transaction v.4a.csv
```

### 4. Levantar la infraestructura completa con un solo comando
```bash
docker-compose up -d
```
> **Nota:** Todos los servicios (Airflow, Metadatos, DWH, API de Productos) levantarán automáticamente sin intervención manual. Las tablas del Data Warehouse se crean solas durante el arranque mediante el script `01_init_dwh.sql`.

---

## 🔍 Validación de Servicios, Conexiones y Variables

### Puertos y Accesos:
- **Airflow Web UI**: [http://localhost:8080](http://localhost:8080) (Usuario: `admin` | Password: `admin`)
- **FastAPI Swagger Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Data Warehouse PostgreSQL**: `localhost:5433` (BD: `datamart_dwh`, Usuario: `dwh_user`, Password: `dwh_secure_password_2024`)
- **PostgreSQL Metadatos**: `localhost:5432` (BD: `airflow_metadata`, Usuario: `airflow`, Password: `airflow`)

### Validación Automática de Connections y Variables (Sin tocar la UI):
Comprueba que Airflow inicializó las credenciales y rutas de forma nativa:
```bash
# Validar conexión al DWH PostgreSQL
docker exec -it airflow-scheduler airflow connections get POSTGRES_DWH

# Validar variables operativas del pipeline
docker exec -it airflow-scheduler airflow variables get RAW_DATA_PATH
docker exec -it airflow-scheduler airflow variables get PRODUCT_API_URL
```

---

## 🔄 Ejecución del Pipeline ETL

Para disparar el DAG manualmente y monitorear su ejecución:

```bash
# 1. Despausar el DAG
docker exec -it airflow-scheduler airflow dags unpause datamart_etl_pipeline

# 2. Ejecutar el DAG inmediatamente
docker exec -it airflow-scheduler airflow dags trigger datamart_etl_pipeline

# 3. Monitorear estado de las tareas
docker exec -it airflow-scheduler airflow dags list-runs -d datamart_etl_pipeline
```

---

## 📊 Diseño del Data Warehouse (Star Schema)

El repositorio analítico se modeló bajo la metodología dimensional de **Ralph Kimball**, garantizando alto rendimiento para consultas analíticas y desacoplamiento de dimensiones.

```text
Table dim_products {
  product_key int [pk, increment]
  stock_code varchar(50) [unique, not null]
  canonical_description text
  category varchar(100)
  supplier_country varchar(100)
  is_active boolean
}

Table dim_customers {
  customer_key int [pk, increment]
  customer_id varchar(50) [unique, not null]
  country varchar(100)
  customer_type varchar(50)
}

Table dim_date {
  date_key int [pk]
  full_date date [unique, not null]
  year int
  month int
  quarter int
}

Table fact_sales {
  sale_id int [pk, increment]
  invoice_no varchar(50)
  product_key int [ref: > dim_products.product_key]
  customer_key int [ref: > dim_customers.customer_key]
  date_key int [ref: > dim_date.date_key]
  transaction_date_utc timestamp
  quantity int
  unit_price numeric
  gross_revenue numeric
  country varchar(100)
}

Table fact_returns {
  return_id int [pk, increment]
  invoice_no varchar(50)
  product_key int [ref: > dim_products.product_key]
  customer_key int [ref: > dim_customers.customer_key]
  date_key int [ref: > dim_date.date_key]
  return_date_utc timestamp
  quantity_returned int
  unit_price numeric
  refund_amount numeric
  country varchar(100)
}

Table log_rejections {
  rejection_id int [pk, increment]
  source_file varchar(100)
  invoice_no varchar(50)
  stock_code varchar(50)
  rejection_reason varchar(255)
  raw_record jsonb
  rejected_at timestamp
}
```
*(Puedes copiar este bloque directamente en [dbdiagram.io](https://dbdiagram.io) para visualizar el diagrama interactivo).*

---

## 🧠 Documento de Decisiones Técnicas y Ambigüedades

### 1. Tratamiento de `CustomerID` Nulo
- **Decisión**: No se descartaron las transacciones sin cliente identificado.
- **Justificación**: Eliminar estos registros distorsionaría en un 20% el revenue real y el volumen transaccional de la empresa. Se asignaron al cliente comodín `GUEST_USER` bajo la clasificación `INVITADO` en `dim_customers`. Esto permitió analizar el comportamiento entre clientes registrados e invitados.

### 2. Descripciones Variables para el Mismo Código de Producto
- **Decisión**: Se calculó el nombre canónico seleccionando la **moda estadística (`mode()`)** no nula de las descripciones en mayúsculas para cada código.
- **Justificación**: Asegura que el catálogo mantenga una etiqueta uniforme y legible sin perder trazabilidad histórica.

### 3. Separación de Ventas vs. Devoluciones
- **Decisión**: Registros con `Quantity <= 0` se enrutan a `fact_returns`, transformando la cantidad en un valor positivo de unidades devueltas (`quantity_returned`) y calculando el impacto monetario (`refund_amount`). Las ventas (`Quantity > 0`) se cargan en `fact_sales`.
- **Justificación**: Previene errores comunes de agregación contable y facilita el cálculo del revenue neto (`ventas - devoluciones`).

### 4. Validación de Precios Inválidos (Calidad de Datos)
- **Decisión**: Cualquier registro con `Quantity > 0` y `UnitPrice <= 0` se envía a la tabla `log_rejections` con el motivo `'PRECIO_INVALIDO_MENOR_IGUAL_CERO'`.
- **Justificación**: Una venta con precio cero o negativo distorsiona el margen de producto y viola las reglas fiscales de DataMart.

### 5. Estandarización de Fechas a UTC
- **Decisión**: Todas las marcas de tiempo se convirtieron a UTC usando el parser flexible de Pandas (`format='mixed'`) y se persistieron en formato ISO8601 (`TIMESTAMP WITH TIME ZONE`).

### 6. Garantía de Idempotencia
- **Decisión**: En las dimensiones se implementó `INSERT ... ON CONFLICT (...) DO UPDATE`. En las tablas de hechos se definieron restricciones de unicidad compuestas (`invoice_no`, `product_key`, `date`, `quantity`, `unit_price`) con `ON CONFLICT DO NOTHING`.
- **Justificación**: El pipeline puede reejecutarse múltiples veces sin duplicar filas ni alterar los totales monetarios.

---

## 📈 Respuestas y Consultas SQL de Negocio (Sección 7)

Para ejecutar todas las consultas en PostgreSQL:
```bash
Get-Content ./sql/02_business_questions.sql | docker exec -i dwh-postgres psql -U dwh_user -d datamart_dwh
```
*(En Linux/macOS: `cat ./sql/02_business_questions.sql | docker exec -i dwh-postgres psql -U dwh_user -d datamart_dwh`)*

### Resumen General Obtenido en el DWH:
```text
          tabla           | total_registros | total_dinero 
--------------------------+-----------------+--------------
 1. Ventas Válidas        |          522602 |  62781386.54
 2. Devoluciones          |            8548 |   2664628.15
 3. Rechazados (Calidad)  |               0 |            0
 4. Productos en Catálogo |            3768 |            0
 5. Clientes Únicos       |            4739 |            0
```

---

### Pregunta 1: Evolución mensual de ventas netas (ventas menos devoluciones)
```sql
WITH sales_monthly AS (
    SELECT d.year, d.month, TO_CHAR(d.full_date, 'YYYY-MM') AS periodo,
           SUM(s.gross_revenue) AS revenue_bruto
    FROM fact_sales s JOIN dim_date d ON s.date_key = d.date_key
    GROUP BY d.year, d.month, TO_CHAR(d.full_date, 'YYYY-MM')
),
returns_monthly AS (
    SELECT d.year, d.month, TO_CHAR(d.full_date, 'YYYY-MM') AS periodo,
           SUM(r.refund_amount) AS total_devoluciones
    FROM fact_returns r JOIN dim_date d ON r.date_key = d.date_key
    GROUP BY d.year, d.month, TO_CHAR(d.full_date, 'YYYY-MM')
)
SELECT COALESCE(sm.periodo, rm.periodo) AS periodo,
       ROUND(COALESCE(sm.revenue_bruto, 0), 2) AS ventas_brutas,
       ROUND(COALESCE(rm.total_devoluciones, 0), 2) AS devoluciones,
       ROUND((COALESCE(sm.revenue_bruto, 0) - COALESCE(rm.total_devoluciones, 0)), 2) AS ventas_netas
FROM sales_monthly sm
FULL OUTER JOIN returns_monthly rm ON sm.periodo = rm.periodo
ORDER BY periodo ASC;
```
* **Resultado del Análisis**: Ventas mensuales promedio de **$4.5M**, alcanzando su punto máximo en el último trimestre de 2019 (Septiembre: **$6.5M**, Octubre: **$6.9M**, Noviembre: **$7.7M** impulsado por estacionalidad de fin de año).

---

### Pregunta 2: Categorías con mayor revenue y proporción de devoluciones
```sql
WITH cat_sales AS (
    SELECT p.category, SUM(s.gross_revenue) AS gross_revenue
    FROM fact_sales s JOIN dim_products p ON s.product_key = p.product_key
    GROUP BY p.category
),
cat_returns AS (
    SELECT p.category, SUM(r.refund_amount) AS return_amount
    FROM fact_returns r JOIN dim_products p ON r.product_key = p.product_key
    GROUP BY p.category
)
SELECT s.category,
       ROUND(s.gross_revenue, 2) AS revenue_bruto,
       ROUND(COALESCE(r.return_amount, 0), 2) AS total_devoluciones,
       ROUND((s.gross_revenue - COALESCE(r.return_amount, 0)), 2) AS revenue_neto,
       ROUND((COALESCE(r.return_amount, 0) / NULLIF(s.gross_revenue, 0)) * 100, 2) AS tasa_devolucion_pct
FROM cat_sales s
LEFT JOIN cat_returns r ON s.category = r.category
ORDER BY s.gross_revenue DESC;
```
* **Resultado del Análisis**: La categoría **Hogar** concentra la mayor facturación (**$62.5M**) con una tasa de devolución controlada del **4.26%**. La categoría **Papelería** presenta la mayor proporción de devoluciones con un **7.49%**.

---

### Pregunta 3A: Top 10 productos con mayor revenue neto
```sql
WITH prod_sales AS (
    SELECT product_key, SUM(gross_revenue) AS gross_rev FROM fact_sales GROUP BY product_key
),
prod_returns AS (
    SELECT product_key, SUM(refund_amount) AS return_amt FROM fact_returns GROUP BY product_key
)
SELECT p.stock_code, SUBSTRING(p.canonical_description FROM 1 FOR 35) AS producto, p.category,
       ROUND(COALESCE(s.gross_rev, 0), 2) AS ventas_brutas,
       ROUND(COALESCE(r.return_amt, 0), 2) AS devoluciones,
       ROUND((COALESCE(s.gross_rev, 0) - COALESCE(r.return_amt, 0)), 2) AS revenue_neto
FROM dim_products p
LEFT JOIN prod_sales s ON p.product_key = s.product_key
LEFT JOIN prod_returns r ON p.product_key = r.product_key
ORDER BY revenue_neto DESC LIMIT 10;
```
* **Top 1**: `22197` (*POPCORN HOLDER*) con **$583.075,23** en ventas netas.

---

### Pregunta 3B: Top 10 productos con mayor tasa de devolución (> 50 unidades vendidas)
```sql
WITH prod_sales AS (
    SELECT product_key, SUM(quantity) AS units_sold, SUM(gross_revenue) AS gross_rev FROM fact_sales GROUP BY product_key
),
prod_returns AS (
    SELECT product_key, SUM(quantity_returned) AS units_returned, SUM(refund_amount) AS return_amt FROM fact_returns GROUP BY product_key
)
SELECT p.stock_code, SUBSTRING(p.canonical_description FROM 1 FOR 35) AS producto, p.category,
       s.units_sold, COALESCE(r.units_returned, 0) AS unidades_devueltas,
       ROUND((COALESCE(r.return_amt, 0) / NULLIF(s.gross_rev, 0)) * 100, 2) AS tasa_devolucion_pct
FROM dim_products p
JOIN prod_sales s ON p.product_key = s.product_key
LEFT JOIN prod_returns r ON p.product_key = r.product_key
WHERE s.units_sold >= 50
ORDER BY tasa_devolucion_pct DESC LIMIT 10;
```
* **Caso Crítico**: El producto `23166` (*MEDIUM CERAMIC TOP STORAGE JAR*) vendió 78.033 unidades y devolvió 74.494 unidades (**95.61% de devolución**).

---

### Pregunta 4: Concentración geográfica y ticket promedio
```sql
SELECT country,
       COUNT(DISTINCT invoice_no) AS total_ordenes,
       ROUND(SUM(gross_revenue), 2) AS ventas_totales,
       ROUND(SUM(gross_revenue) / NULLIF(COUNT(DISTINCT invoice_no), 0), 2) AS ticket_promedio
FROM fact_sales
GROUP BY country
ORDER BY total_ordenes DESC LIMIT 10;
```
* **Resultado del Análisis**: **United Kingdom** concentra 17.908 órdenes ($2.923 de ticket promedio). Mercados como **Netherlands** ($22.888) y **Australia** ($16.055) muestran un ticket promedio significativamente superior debido a compras por volumen B2B.

---

### Pregunta 5: Clientes identificados vs. Invitados
```sql
SELECT c.customer_type,
       COUNT(DISTINCT s.invoice_no) AS total_ordenes,
       COUNT(DISTINCT s.customer_key) AS clientes_distintos,
       SUM(s.quantity) AS unidades_vendidas,
       ROUND(SUM(s.gross_revenue), 2) AS total_revenue,
       ROUND(SUM(s.gross_revenue) / NULLIF(COUNT(DISTINCT s.invoice_no), 0), 2) AS ticket_promedio
FROM fact_sales s
JOIN dim_customers c ON s.customer_key = c.customer_key
GROUP BY c.customer_type;
```
* **Resultado del Análisis**: Los clientes registrados (`REGISTRADO`) representan el 99.9% de los ingresos ($62.7M) con un ticket promedio de **$3.172,54**.

---

### Pregunta 6: Total de códigos únicos de producto
```sql
SELECT COUNT(DISTINCT stock_code) AS total_codigos_unicos FROM dim_products;
```
* **Resultado**: **3.768** referencias únicas activas en el catálogo.

---

### Pregunta 7: Recomendación Estratégica para el Equipo de Producto
```sql
SELECT p.stock_code, SUBSTRING(p.canonical_description FROM 1 FOR 35) AS producto, p.category,
       ROUND(s.gross_rev, 2) AS ventas_brutas,
       ROUND(r.return_amt, 2) AS dinero_devuelto,
       ROUND((r.return_amt / s.gross_rev) * 100, 2) AS porcentaje_perdida_devolucion
FROM dim_products p
JOIN (SELECT product_key, SUM(gross_revenue) AS gross_rev FROM fact_sales GROUP BY product_key) s ON p.product_key = s.product_key
JOIN (SELECT product_key, SUM(refund_amount) AS return_amt FROM fact_returns GROUP BY product_key) r ON p.product_key = r.product_key
WHERE s.gross_rev > 5000 AND (r.return_amt / s.gross_rev) >= 0.15
ORDER BY r.return_amt DESC LIMIT 10;
```
* **Recomendación Respaldada con Cifras**:
  1. **Auditoría o Suspensión Inmediata de `23166`**: Este producto (*MEDIUM CERAMIC TOP STORAGE JAR*) generó una pérdida directa de **$843.277,96** por devoluciones (tasa del 95.61%). Al ser un frasco de cerámica, la causa raíz apunta a roturas durante el transporte. Se debe exigir embalaje reforzado o retirar la referencia del catálogo.
  2. **Revisión de `23843` (*PAPER CRAFT LITTLE BIRDIE*)**: Aunque generó $1M bruto, tuvo una devolución de **$501.359,05** (50% exacto), indicando cancelaciones masivas de lotes corporativos.

---

## 🛑 Comandos de Mantenimiento

Para apagar los servicios manteniendo los datos persistidos en los volúmenes de Docker:
```bash
docker-compose down
```

Para reiniciar un servicio específico (por ejemplo, la API o el Scheduler):
```bash
docker-compose restart airflow-scheduler
docker-compose restart product-api
```
````