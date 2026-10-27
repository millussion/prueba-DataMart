```markdown
# Documento de Decisiones Técnicas — DataMart S.A.S.
## 1. Diseño del Modelo de Datos (Star Schema)
Se diseñó un esquema en estrella normalizado en el esquema `datamart_dw`:
- **Separación entre `fact_sales` y `fact_returns`**: Fundamental para evitar distorsiones contables. Las cantidades en `fact_returns` se almacenan en positivo como volumen devuelto, y el impacto monetario en `refund_amount`. Esto simplifica enormemente el cálculo del revenue neto (`ventas - devoluciones`).
- **Dimensiones (`dim_products`, `dim_customers`, `dim_date`)**: Permiten análisis multidimensional rápido por categoría, país, temporalidad y cliente.
- **Log de Rechazos (`log_rejections`)**: Los registros con precios menores o iguales a cero en ventas se aíslan con su JSON original y motivo para auditoría.
## 2. Resolución de Casos Ambiguos
- **Clientes nulos (`CustomerID` vacío)**: No se descartaron para no perder el ~20% del volumen transaccional. Se agruparon bajo el identificador `'GUEST_USER'` con categoría `'INVITADO'`. Esto permitió responder la pregunta 5 demostrando que los clientes identificados tienen un ticket promedio diferente.
- **Descripciones variables para el mismo producto**: Se normalizó el nombre canónico calculando la moda estadística (`mode()`) no nula de las descripciones en mayúsculas por cada código de producto.
- **Zonas horarias y fechas**: Se estandarizaron todas las fechas a UTC mediante parseo flexible (`format='mixed'`), almacenadas en formato universal ISO8601.
## 3. Garantía de Idempotencia
- Se definieron restricciones de unicidad compuestas en las tablas de hechos (`invoice_no`, `product_key`, `date`, `quantity`, `unit_price`).
- La carga utiliza sentencias `INSERT ... ON CONFLICT (...) DO NOTHING` y `DO UPDATE` en dimensiones. Ejecutar el DAG múltiples veces el mismo día produce exactamente el mismo estado sin duplicados.