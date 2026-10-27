-- sql/02_business_queries.sql
SET search_path TO datamart_dw, public;

-- =============================================================================
-- PREGUNTA 1: Evolución mensual de ventas netas (ventas menos devoluciones)
-- =============================================================================
WITH sales_monthly AS (
    SELECT 
        d.year,
        d.month,
        TO_CHAR(d.full_date, 'YYYY-MM') AS periodo,
        SUM(s.gross_revenue) AS revenue_bruto
    FROM fact_sales s
    JOIN dim_date d ON s.date_key = d.date_key
    GROUP BY d.year, d.month, TO_CHAR(d.full_date, 'YYYY-MM')
),
returns_monthly AS (
    SELECT 
        d.year,
        d.month,
        TO_CHAR(d.full_date, 'YYYY-MM') AS periodo,
        SUM(r.refund_amount) AS total_devoluciones
    FROM fact_returns r
    JOIN dim_date d ON r.date_key = d.date_key
    GROUP BY d.year, d.month, TO_CHAR(d.full_date, 'YYYY-MM')
)
SELECT 
    COALESCE(sm.periodo, rm.periodo) AS periodo,
    ROUND(COALESCE(sm.revenue_bruto, 0), 2) AS ventas_brutas,
    ROUND(COALESCE(rm.total_devoluciones, 0), 2) AS devoluciones,
    ROUND((COALESCE(sm.revenue_bruto, 0) - COALESCE(rm.total_devoluciones, 0)), 2) AS ventas_netas
FROM sales_monthly sm
FULL OUTER JOIN returns_monthly rm ON sm.periodo = rm.periodo
ORDER BY periodo ASC;

-- =============================================================================
-- PREGUNTA 2: Categorías con más revenue y mayor proporción de devoluciones
-- =============================================================================
WITH cat_sales AS (
    SELECT p.category, SUM(s.gross_revenue) AS gross_revenue
    FROM fact_sales s
    JOIN dim_products p ON s.product_key = p.product_key
    GROUP BY p.category
),
cat_returns AS (
    SELECT p.category, SUM(r.refund_amount) AS return_amount
    FROM fact_returns r
    JOIN dim_products p ON r.product_key = p.product_key
    GROUP BY p.category
)
SELECT 
    s.category,
    ROUND(s.gross_revenue, 2) AS revenue_bruto,
    ROUND(COALESCE(r.return_amount, 0), 2) AS total_devoluciones,
    ROUND((s.gross_revenue - COALESCE(r.return_amount, 0)), 2) AS revenue_neto,
    ROUND((COALESCE(r.return_amount, 0) / NULLIF(s.gross_revenue, 0)) * 100, 2) AS tasa_devolucion_pct
FROM cat_sales s
LEFT JOIN cat_returns r ON s.category = r.category
ORDER BY s.gross_revenue DESC;

-- =============================================================================
-- PREGUNTA 3A: Top 10 productos con mayor revenue neto
-- =============================================================================
WITH prod_sales AS (
    SELECT product_key, SUM(gross_revenue) AS gross_rev
    FROM fact_sales GROUP BY product_key
),
prod_returns AS (
    SELECT product_key, SUM(refund_amount) AS return_amt
    FROM fact_returns GROUP BY product_key
)
SELECT 
    p.stock_code,
    SUBSTRING(p.canonical_description FROM 1 FOR 35) AS producto,
    p.category,
    ROUND(COALESCE(s.gross_rev, 0), 2) AS ventas_brutas,
    ROUND(COALESCE(r.return_amt, 0), 2) AS devoluciones,
    ROUND((COALESCE(s.gross_rev, 0) - COALESCE(r.return_amt, 0)), 2) AS revenue_neto
FROM dim_products p
LEFT JOIN prod_sales s ON p.product_key = s.product_key
LEFT JOIN prod_returns r ON p.product_key = r.product_key
ORDER BY revenue_neto DESC
LIMIT 10;

-- =============================================================================
-- PREGUNTA 3B: Top 10 productos con mayor tasa de devolución (> 50 unidades vendidas)
-- =============================================================================
WITH prod_sales AS (
    SELECT product_key, SUM(quantity) AS units_sold, SUM(gross_revenue) AS gross_rev
    FROM fact_sales GROUP BY product_key
),
prod_returns AS (
    SELECT product_key, SUM(quantity_returned) AS units_returned, SUM(refund_amount) AS return_amt
    FROM fact_returns GROUP BY product_key
)
SELECT 
    p.stock_code,
    SUBSTRING(p.canonical_description FROM 1 FOR 35) AS producto,
    p.category,
    s.units_sold,
    COALESCE(r.units_returned, 0) AS unidades_devueltas,
    ROUND((COALESCE(r.return_amt, 0) / NULLIF(s.gross_rev, 0)) * 100, 2) AS tasa_devolucion_pct
FROM dim_products p
JOIN prod_sales s ON p.product_key = s.product_key
LEFT JOIN prod_returns r ON p.product_key = r.product_key
WHERE s.units_sold >= 50
ORDER BY tasa_devolucion_pct DESC
LIMIT 10;

-- =============================================================================
-- PREGUNTA 4: Países con más transacciones y variación de ticket promedio
-- =============================================================================
SELECT 
    country,
    COUNT(DISTINCT invoice_no) AS total_ordenes,
    ROUND(SUM(gross_revenue), 2) AS ventas_totales,
    ROUND(SUM(gross_revenue) / NULLIF(COUNT(DISTINCT invoice_no), 0), 2) AS ticket_promedio
FROM fact_sales
GROUP BY country
ORDER BY total_ordenes DESC
LIMIT 10;

-- =============================================================================
-- PREGUNTA 5: Clientes identificados vs transacciones sin cliente (INVITADOS)
-- =============================================================================
SELECT 
    c.customer_type,
    COUNT(DISTINCT s.invoice_no) AS total_ordenes,
    COUNT(DISTINCT s.customer_key) AS clientes_distintos,
    SUM(s.quantity) AS unidades_vendidas,
    ROUND(SUM(s.gross_revenue), 2) AS total_revenue,
    ROUND(SUM(s.gross_revenue) / NULLIF(COUNT(DISTINCT s.invoice_no), 0), 2) AS ticket_promedio
FROM fact_sales s
JOIN dim_customers c ON s.customer_key = c.customer_key
GROUP BY c.customer_type;

-- =============================================================================
-- PREGUNTA 6: Total de códigos únicos de producto
-- =============================================================================
SELECT COUNT(DISTINCT stock_code) AS total_codigos_unicos FROM dim_products;

-- =============================================================================
-- PREGUNTA 7: Recomendación basada en datos para el equipo de Producto
-- (Productos con ventas significativas pero donde las devoluciones superan el 15%)
-- =============================================================================
SELECT 
    p.stock_code,
    SUBSTRING(p.canonical_description FROM 1 FOR 35) AS producto,
    p.category,
    ROUND(s.gross_rev, 2) AS ventas_brutas,
    ROUND(r.return_amt, 2) AS dinero_devuelto,
    ROUND((r.return_amt / s.gross_rev) * 100, 2) AS porcentaje_perdida_devolucion
FROM dim_products p
JOIN (SELECT product_key, SUM(gross_revenue) AS gross_rev FROM fact_sales GROUP BY product_key) s ON p.product_key = s.product_key
JOIN (SELECT product_key, SUM(refund_amount) AS return_amt FROM fact_returns GROUP BY product_key) r ON p.product_key = r.product_key
WHERE s.gross_rev > 5000 AND (r.return_amt / s.gross_rev) >= 0.15
ORDER BY r.return_amt DESC
LIMIT 10;