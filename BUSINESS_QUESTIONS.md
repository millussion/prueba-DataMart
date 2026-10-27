## Respuestas y Consultas SQL de Negocio de la Sección 7 de la prueba.

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
* **Resultado del Análisis**: Ventas mensuales promedio de **$4.5M**, alcanzando su punto máximo en el último trimestre de 2019 (Septiembre: **$6.5M**, Octubre: **$6.9M**, Noviembre: **$7.7M** impulsado por estacionalidad de fin de año).

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
* **Resultado del Análisis**: La categoría **Hogar** concentra la mayor facturación (**$62.5M**) con una tasa de devolución controlada del **4.26%**. La categoría **Papelería** presenta la mayor proporción de devoluciones con un **7.49%**.

---
### Pregunta 3: ¿Cuáles son los 10 productos con mayor revenue neto y cuáles son los 10 con mayor tasa de devolución?
### Pregunta 3.1: Top 10 productos con mayor revenue neto
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

### Pregunta 3.2: Top 10 productos con mayor tasa de devolución (> 50 unidades vendidas)
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
* **Resultado del Análisis**: **United Kingdom** concentra 17.908 órdenes ($2.923 de ticket promedio). Mercados como **Netherlands** ($22.888) y **Australia** ($16.055) muestran un ticket promedio significativamente superior debido a compras por volumen B2B.

---

### Pregunta 5:  ¿Existe alguna diferencia en el comportamiento de compra entre clientes identificados y transacciones sin customer ID? (Esta pregunta solo aplica si decidiste incluir las transacciones sin cliente.)
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

### Pregunta 6: ¿Cuántos códigos únicos de producto existen en total?
```sql
SELECT COUNT(DISTINCT stock_code) AS total_codigos_unicos FROM dim_products;
```
* **Resultado**: **3.768** referencias únicas activas en el catálogo.

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
* **Recomendación Respaldada por números**:
  1. **Suspensión Inmediata de `23166`**: Este producto (*MEDIUM CERAMIC TOP STORAGE JAR*) generó una pérdida directa de **$843.277,96** por devoluciones (tasa del 95.61%). Al ser un frasco de cerámica, la causa raíz apunta a roturas durante el transporte. Se debe exigir embalaje reforzado o retirar la referencia del catálogo.
  2. **Revisión de `23843` (*PAPER CRAFT LITTLE BIRDIE*)**: Aunque generó $1M bruto, tuvo una devolución de **$501.359,05** (50% exacto), indicando cancelaciones masivas de lotes corporativos.

---
