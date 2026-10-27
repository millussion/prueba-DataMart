## Respuestas y Consultas SQL de Negocio de la Sección 7 de la prueba

Para ejecutar todas las consultas consolidadas contra PostgreSQL DWH:
```bash
Get-Content ./sql/02_business_questions.sql | docker exec -i dwh-postgres psql -U dwh_user -d datamart_dwh
```
*(En Linux/macOS: `cat ./sql/02_business_questions.sql | docker exec -i dwh-postgres psql -U dwh_user -d datamart_dwh`)*

---

### Pregunta 1: ¿Cuál fue la evolución mensual de las ventas netas (descontando devoluciones) durante el periodo cubierto por los datos?
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
* **Resultado del Análisis**: El pipeline procesó exitosamente **26 meses de historia transaccional** consolidando ambas fuentes:
  - **Periodo 2010–2011 (`data.csv`)**: Ventas netas estables entre **$500K y $1.0M** mensuales, con un fuerte pico estacional en Noviembre de 2011 alcanzando **$1.45M**.
  - **Periodo 2018–2019 (`Sales Transaction v.4a.csv`)**: El negocio escala a un promedio de **$4.5M** mensuales, registrando su máximo histórico en Noviembre de 2019 con **$7.71M** en ventas netas por temporada alta de fin de año.

---

### Pregunta 2: ¿Qué categorías de producto generaron más revenue bruto y cuáles tuvieron mayor proporción de devoluciones?
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
* **Resultado del Análisis**:
  - **Líder en Facturación**: **Hogar** es la categoría principal con **$53.58M** brutos ($51.99M netos) y una tasa de devolución baja y controlada del **2.97%**.
  - **Mayor Proporción de Devoluciones**: **Electrónica** presenta la tasa más crítica con un **17.94%** de devoluciones ($1.00M devuelto), seguida por **Ropa** con un **14.03%** ($760K devuelto), lo que exige una revisión técnica de calidad y tallajes.

---

### Pregunta 3: ¿Cuáles son los 10 productos con mayor revenue neto y cuáles son los 10 con mayor tasa de devolución?

#### Pregunta 3.1: Top 10 productos con mayor revenue neto
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
* **Top 1**: `22197` (*POPCORN HOLDER*) con **$634.043,15** en ventas netas.
* **Top 2**: `84077` (*WORLD WAR 2 GLIDERS ASSTD DESIGNS*) con **$569.717,72**.
* **Top 3**: `85123A` (*CREAM HANGING HEART T-LIGHT HOLDER*) con **$550.954,66**.
* **Top 4**: `23843` (*PAPER CRAFT , LITTLE BIRDIE*) con **$501.359,05**.
* **Top 5**: `84879` (*ASSORTED COLOUR BIRD ORNAMENT*) con **$478.350,10**.

#### Pregunta 3.2: Top 10 productos con mayor tasa de devolución (> 50 unidades vendidas)
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
* **Caso Crítico Absoluto**: El producto `23166` (*MEDIUM CERAMIC TOP STORAGE JAR*) vendió 156.066 unidades y devolvió 148.988 unidades (**95.54% de tasa de devolución**).
* **Devolución Total (100%)**: El producto `21655` (*HANGING RIDGE GLASS T-LIGHT HOLDER*) devolvió exactamente las 72 unidades vendidas.

---

### Pregunta 4: ¿Qué países concentran la mayor parte de las transacciones y cómo varía el ticket promedio entre ellos?
```sql
SELECT country,
       COUNT(DISTINCT invoice_no) AS total_ordenes,
       ROUND(SUM(gross_revenue), 2) AS ventas_totales,
       ROUND(SUM(gross_revenue) / NULLIF(COUNT(DISTINCT invoice_no), 0), 2) AS ticket_promedio
FROM fact_sales
GROUP BY country
ORDER BY total_ordenes DESC LIMIT 10;
```
* **Concentración del Mercado**: **United Kingdom** concentra el 88% de las transacciones con **18.113 órdenes**, totalizando **$61.35M** con un ticket promedio de **$3.386,96**.
* **Segmento Corporativo / B2B**: Países como **Netherlands** ($25.652,63 de ticket promedio) y **Australia** ($17.997,90) presentan tickets significativamente más elevados debido a órdenes de distribución mayorista.

---

### Pregunta 5: ¿Existe alguna diferencia en el comportamiento de compra entre clientes identificados y transacciones sin customer ID? (Esta pregunta solo aplica si decidiste incluir las transacciones sin cliente.)
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
* **Resultado del Análisis y Justificación**:
  - **Decisión de inclusión**: **SÍ se incluyeron** bajo la clasificación `'INVITADO'`. Descartarlas habría eliminado **$1.754.983,91** en ventas legítimas de la contabilidad de la empresa.
  - **Diferencia de Comportamiento**:
    - **Clientes Invitados (`INVITADO`)**: Generaron **1.429 órdenes** con **420.426 unidades** vendidas y un ticket promedio de **$1.228,12**, reflejando compras minoristas individuales sin registro.
    - **Clientes Registrados (`REGISTRADO`)**: Generaron **19.923 órdenes** con **$71.667.957,68** en ventas y un ticket promedio casi tres veces superior (**$3.597,25**), correspondiente a compradores frecuentes y cuentas comerciales B2B.

---

### Pregunta 6: ¿Cuántos códigos únicos de producto existen en total?
```sql
SELECT COUNT(DISTINCT stock_code) AS total_codigos_unicos FROM dim_products;
```
* **Resultado**: **3.944 referencias únicas** catalogadas, normalizadas y activas en `dim_products` (cumpliendo con el contexto de negocio de más de 3.000 referencias).

---

### Pregunta 7: Con base en los datos, ¿qué recomendación concreta y específica le harías al equipo de producto de DataMart? La recomendación debe estar respaldada por números del análisis, no ser genérica.
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
* **Recomendaciones Estratégicas Respaldadas por Cifras**:
  1. **Retiro o Auditoría Urgente de `23166` (*MEDIUM CERAMIC TOP STORAGE JAR*)**: Este producto vendió $963.691 pero acumuló reembolsos por **$920.757,60 (95.54% de pérdida directa)**. Al ser un frasco de cerámica con tapa de almacenamiento, una tasa del 95% evidencia roturas sistemáticas durante la manipulación y despacho logístico. Se recomienda detener su distribución de inmediato hasta exigir al proveedor embalaje con burbuja protectora rígida.
  2. **Investigación de Lotes en `23843` (*PAPER CRAFT , LITTLE BIRDIE*)**: Tuvo **$669.828,65** devueltos (57.19% de sus ventas brutas), indicando problemas de calidad percibida o cancelaciones masivas de pedidos mayoristas.
  3. **Plan de Acción para la Categoría Electrónica**: Registra una tasa de devolución global del **17.94%** ($1.00M en pérdidas), la más alta de la empresa. Se sugiere implementar un control de calidad pre-despacho y mejorar las guías técnicas del producto para reducir devoluciones por errores de uso.