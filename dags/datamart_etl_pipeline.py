import os
import glob
import logging
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'datamart_data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def get_dwh_engine():
    conn = BaseHook.get_connection('POSTGRES_DWH')
    db_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(db_url)

# ==============================================================================
# ETAPA 1: EXTRACCIÓN Y UNIFICACIÓN DE MÚLTIPLES DATASETS HETEROGÉNEOS
# ==============================================================================
def extract_and_unify_sources(**kwargs):
    raw_path = Variable.get("RAW_DATA_PATH", default_var="/opt/airflow/data")
    
    # Buscar todos los archivos .csv en data/
    csv_files = glob.glob(os.path.join(raw_path, "*.csv")) + glob.glob(os.path.join(raw_path, "**", "*.csv"))
    csv_files = list(set(csv_files))

    if not csv_files:
        raise FileNotFoundError(f"No se encontró ningún archivo CSV en: {raw_path}")

    logger.info(f"Archivos encontrados para procesar: {csv_files}")

    column_mapping = {
        'TransactionNo': 'invoice_no',
        'Date': 'invoice_date',
        'ProductNo': 'stock_code',
        'ProductName': 'description',
        'Price': 'unit_price',
        'CustomerNo': 'customer_id',
        'InvoiceNo': 'invoice_no',
        'Invoice': 'invoice_no',
        'StockCode': 'stock_code',
        'Description': 'description',
        'UnitPrice': 'unit_price',
        'InvoiceDate': 'invoice_date',
        'CustomerID': 'customer_id',
        'Customer ID': 'customer_id',
        'Quantity': 'quantity',
        'Country': 'country'
    }

    dfs = []
    for file_path in csv_files:
        logger.info(f"Leyendo fuente: {file_path}")
        try:
            df = pd.read_csv(file_path, dtype=str, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, dtype=str, encoding='ISO-8859-1')

        df = df.rename(columns=column_mapping)
        df['source_file'] = os.path.basename(file_path)
        dfs.append(df)

    unified_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total registros combinados antes de deduplicar: {len(unified_df)}")

    # Deduplicación por clave compuesta para resolver solapamientos
    unified_df = unified_df.drop_duplicates(
        subset=['invoice_no', 'stock_code', 'quantity', 'unit_price'],
        keep='first'
    )
    logger.info(f"Total registros únicos tras resolver solapamientos: {len(unified_df)}")

    tmp_path = "/tmp/unified_transactions.parquet"
    unified_df.to_parquet(tmp_path, index=False)
    kwargs['ti'].xcom_push(key='unified_data_path', value=tmp_path)

# ==============================================================================
# ETAPA 2: CONSULTA AL CATÁLOGO DE PRODUCTOS (API FASTAPI)
# ==============================================================================
def fetch_product_catalog(**kwargs):
    api_url = Variable.get("PRODUCT_API_URL", default_var="http://product-api:8000")
    products_map = {}
    try:
        response = requests.get(f"{api_url}/products?page=1&page_size=500", timeout=5)
        if response.status_code == 200:
            for item in response.json():
                products_map[item['code'].strip().upper()] = item
            logger.info(f"Se obtuvieron {len(products_map)} productos desde el catálogo API.")
    except Exception as e:
        logger.warning(f"Aviso: Fallback en catálogo ({e})")
    
    kwargs['ti'].xcom_push(key='catalog_map', value=products_map)

# ==============================================================================
# ETAPA 3: TRANSFORMACIÓN VECTORIZADA ULTRA-RÁPIDA (PANDAS EN C)
# ==============================================================================
def transform_and_validate(**kwargs):
    ti = kwargs['ti']
    unified_path = ti.xcom_pull(task_ids='extract_and_unify_sources', key='unified_data_path')
    df = pd.read_parquet(unified_path)

    # 1. Normalización de códigos de producto
    df['stock_code'] = df['stock_code'].astype(str).str.strip().str.upper()

    # 2. Parseo numérico
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')

    # 3. Estandarización de Fechas a UTC
    df['parsed_date'] = pd.to_datetime(df['invoice_date'], format='mixed', errors='coerce')
    df['parsed_date'] = df['parsed_date'].dt.tz_localize('UTC')
    df['date_key'] = df['parsed_date'].dt.strftime('%Y%m%d').astype('Int64')

    # 4. Manejo de Clientes Nulos -> INVITADO
    df['customer_id'] = df['customer_id'].fillna('GUEST_USER').astype(str).str.replace(r'\.0$', '', regex=True)
    df.loc[df['customer_id'].isin(['nan', 'None', '', 'NULL']), 'customer_id'] = 'GUEST_USER'
    df['customer_type'] = df['customer_id'].apply(lambda x: 'INVITADO' if x == 'GUEST_USER' else 'REGISTRADO')

    # 5. Descripción canónica por producto (Moda estadística)
    canonical_descriptions = df.groupby('stock_code')['description'].agg(
        lambda s: s.dropna().str.strip().str.upper().mode()[0] if not s.dropna().empty else 'SIN DESCRIPCION'
    ).to_dict()
    df['canonical_description'] = df['stock_code'].map(canonical_descriptions).fillna('SIN DESCRIPCION')

    # 6. FILTRADO VECTORIZADO ULTRA-RÁPIDO (En lugar de iterrows)
    # Máscara 1: Nulos en campos críticos
    null_mask = df['quantity'].isna() | df['unit_price'].isna() | df['parsed_date'].isna()

    # Máscara 2: Regla de Negocio: UnitPrice <= 0 en venta (Quantity > 0)
    invalid_price_mask = (~null_mask) & (df['quantity'] > 0) & (df['unit_price'] <= 0)

    # Máscara 3: Devoluciones (Quantity <= 0)
    returns_mask = (~null_mask) & (~invalid_price_mask) & (df['quantity'] <= 0)

    # Máscara 4: Ventas Válidas (Quantity > 0 y UnitPrice > 0)
    sales_mask = (~null_mask) & (~invalid_price_mask) & (df['quantity'] > 0)

    # --- Procesar Ventas Válidas ---
    sales_df = df[sales_mask].copy()
    sales_df['quantity'] = sales_df['quantity'].astype(int)
    sales_df['gross_revenue'] = (sales_df['quantity'] * sales_df['unit_price']).round(4)
    sales_df['date_full'] = sales_df['parsed_date'].dt.strftime('%Y-%m-%d')
    sales_df['transaction_date_utc'] = sales_df['parsed_date'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')

    # --- Procesar Devoluciones ---
    returns_df = df[returns_mask].copy()
    returns_df['quantity_returned'] = returns_df['quantity'].abs().astype(int)
    returns_df['refund_amount'] = (returns_df['quantity_returned'] * returns_df['unit_price']).round(4)
    returns_df['date_full'] = returns_df['parsed_date'].dt.strftime('%Y-%m-%d')
    returns_df['return_date_utc'] = returns_df['parsed_date'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')

    # --- Procesar Rechazos ---
    rejections_list = []
    
    # Registros con nulos
    null_df = df[null_mask]
    for r in null_df[['source_file', 'invoice_no', 'stock_code', 'quantity', 'unit_price']].itertuples(index=False):
        rejections_list.append({
            'source_file': str(r.source_file),
            'invoice_no': str(r.invoice_no),
            'stock_code': str(r.stock_code),
            'rejection_reason': 'VALOR_NULO_EN_CAMPOS_CRITICOS',
            'raw_record': f'{{"invoice": "{r.invoice_no}", "qty": "{r.quantity}", "price": "{r.unit_price}"}}'
        })
        
    # Registros con precios inválidos
    invalid_p_df = df[invalid_price_mask]
    for r in invalid_p_df[['source_file', 'invoice_no', 'stock_code', 'quantity', 'unit_price']].itertuples(index=False):
        rejections_list.append({
            'source_file': str(r.source_file),
            'invoice_no': str(r.invoice_no),
            'stock_code': str(r.stock_code),
            'rejection_reason': 'PRECIO_INVALIDO_MENOR_IGUAL_CERO',
            'raw_record': f'{{"invoice": "{r.invoice_no}", "price": {r.unit_price}, "qty": {r.quantity}}}'
        })

    rejections_df = pd.DataFrame(rejections_list) if rejections_list else pd.DataFrame(columns=['source_file', 'invoice_no', 'stock_code', 'rejection_reason', 'raw_record'])

    sales_df.to_parquet('/tmp/clean_sales.parquet', index=False)
    returns_df.to_parquet('/tmp/clean_returns.parquet', index=False)
    rejections_df.to_parquet('/tmp/clean_rejections.parquet', index=False)
    logger.info(f"Transformación vectorizada lista: {len(sales_df)} ventas, {len(returns_df)} devoluciones, {len(rejections_df)} rechazos.")

# ==============================================================================
# ETAPA 4: CARGA MASIVA EN LOTES AL DATA WAREHOUSE (POSTGRESQL)
# ==============================================================================
def load_to_dwh(**kwargs):
    ti = kwargs['ti']
    catalog_map = ti.xcom_pull(task_ids='fetch_product_catalog', key='catalog_map') or {}

    engine = get_dwh_engine()
    
    sales_df = pd.read_parquet('/tmp/clean_sales.parquet')
    returns_df = pd.read_parquet('/tmp/clean_returns.parquet')
    rejections_df = pd.read_parquet('/tmp/clean_rejections.parquet')

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cursor:
            # 1. Dimensión Fechas (Bulk Insert)
            logger.info("Cargando dim_date en lotes...")
            dates_all = pd.concat([
                sales_df[['date_key', 'date_full']],
                returns_df[['date_key', 'date_full']]
            ]).drop_duplicates(subset=['date_key'])

            dates_all['dt'] = pd.to_datetime(dates_all['date_full'])
            date_tuples = [
                (
                    int(r.date_key),
                    r.dt.date(),
                    int(r.dt.year),
                    int(r.dt.month),
                    r.dt.strftime('%B'),
                    int(r.dt.day),
                    r.dt.strftime('%A'),
                    int((r.dt.month - 1) // 3 + 1)
                )
                for r in dates_all[['date_key', 'dt']].itertuples(index=False)
            ]
            execute_values(cursor, """
                INSERT INTO datamart_dw.dim_date (date_key, full_date, year, month, month_name, day, day_of_week, quarter)
                VALUES %s
                ON CONFLICT (date_key) DO NOTHING;
            """, date_tuples, page_size=5000)

            # 2. Dimensión Clientes (Bulk Upsert)
            logger.info("Cargando dim_customers en lotes...")
            cust_all = pd.concat([
                sales_df[['customer_id', 'country', 'customer_type']],
                returns_df[['customer_id', 'country', 'customer_type']]
            ]).drop_duplicates(subset=['customer_id'])

            cust_tuples = list(cust_all[['customer_id', 'country', 'customer_type']].itertuples(index=False, name=None))
            execute_values(cursor, """
                INSERT INTO datamart_dw.dim_customers (customer_id, country, customer_type)
                VALUES %s
                ON CONFLICT (customer_id) DO UPDATE SET country = EXCLUDED.country;
            """, cust_tuples, page_size=5000)

            # 3. Dimensión Productos (Bulk Upsert con enriquecimiento API)
            logger.info("Cargando dim_products en lotes...")
            prod_all = pd.concat([
                sales_df[['stock_code', 'canonical_description']],
                returns_df[['stock_code', 'canonical_description']]
            ]).drop_duplicates(subset=['stock_code'])

            prod_tuples = []
            for r in prod_all.itertuples(index=False):
                code = r.stock_code
                api_info = catalog_map.get(code, {})
                category = api_info.get('category', 'Hogar')
                supplier = api_info.get('supplier_country', 'Colombia')
                is_active = api_info.get('is_active', True)
                prod_tuples.append((code, r.canonical_description, category, supplier, is_active))

            execute_values(cursor, """
                INSERT INTO datamart_dw.dim_products (stock_code, canonical_description, category, supplier_country, is_active)
                VALUES %s
                ON CONFLICT (stock_code) DO UPDATE 
                SET canonical_description = EXCLUDED.canonical_description,
                    category = EXCLUDED.category;
            """, prod_tuples, page_size=5000)

            # Obtener los IDs generados
            cursor.execute("SELECT stock_code, product_key FROM datamart_dw.dim_products;")
            prod_map = dict(cursor.fetchall())

            cursor.execute("SELECT customer_id, customer_key FROM datamart_dw.dim_customers;")
            cust_map = dict(cursor.fetchall())

            guest_cust_key = cust_map.get('GUEST_USER', 1)

            # 4. Carga Masiva de Hechos de Ventas (500.000+ registros en segundos)
            logger.info(f"Cargando {len(sales_df):,} registros de fact_sales con execute_values...")
            sales_df['product_key'] = sales_df['stock_code'].map(prod_map).fillna(1).astype(int)
            sales_df['customer_key'] = sales_df['customer_id'].map(cust_map).fillna(guest_cust_key).astype(int)
            sales_df['date_key'] = sales_df['date_key'].astype(int)
            sales_df['quantity'] = sales_df['quantity'].astype(int)
            sales_df['unit_price'] = sales_df['unit_price'].astype(float)
            sales_df['gross_revenue'] = sales_df['gross_revenue'].astype(float)

            sales_cols = [
                'invoice_no', 'product_key', 'customer_key', 'date_key',
                'transaction_date_utc', 'quantity', 'unit_price', 'gross_revenue',
                'country', 'source_file'
            ]
            sales_tuples = list(sales_df[sales_cols].itertuples(index=False, name=None))

            execute_values(cursor, """
                INSERT INTO datamart_dw.fact_sales 
                (invoice_no, product_key, customer_key, date_key, transaction_date_utc, quantity, unit_price, gross_revenue, country, source_file)
                VALUES %s
                ON CONFLICT (invoice_no, product_key, transaction_date_utc, quantity, unit_price) DO NOTHING;
            """, sales_tuples, page_size=5000)
            logger.info(f"fact_sales cargada exitosamente ({len(sales_tuples):,} registros).")

            # 5. Carga Masiva de Hechos de Devoluciones
            logger.info(f"Cargando {len(returns_df):,} registros de fact_returns con execute_values...")
            returns_df['product_key'] = returns_df['stock_code'].map(prod_map).fillna(1).astype(int)
            returns_df['customer_key'] = returns_df['customer_id'].map(cust_map).fillna(guest_cust_key).astype(int)
            returns_df['date_key'] = returns_df['date_key'].astype(int)
            returns_df['quantity_returned'] = returns_df['quantity_returned'].astype(int)
            returns_df['unit_price'] = returns_df['unit_price'].astype(float)
            returns_df['refund_amount'] = returns_df['refund_amount'].astype(float)

            returns_cols = [
                'invoice_no', 'product_key', 'customer_key', 'date_key',
                'return_date_utc', 'quantity_returned', 'unit_price', 'refund_amount',
                'country', 'source_file'
            ]
            returns_tuples = list(returns_df[returns_cols].itertuples(index=False, name=None))

            execute_values(cursor, """
                INSERT INTO datamart_dw.fact_returns 
                (invoice_no, product_key, customer_key, date_key, return_date_utc, quantity_returned, unit_price, refund_amount, country, source_file)
                VALUES %s
                ON CONFLICT (invoice_no, product_key, return_date_utc, quantity_returned, unit_price) DO NOTHING;
            """, returns_tuples, page_size=5000)
            logger.info(f"fact_returns cargada exitosamente ({len(returns_tuples):,} registros).")

            # 6. Carga Masiva de Log de Rechazos
            if len(rejections_df) > 0:
                logger.info(f"Registrando {len(rejections_df):,} rechazos de calidad...")
                rej_tuples = [
                    (
                        str(r.source_file),
                        str(r.invoice_no),
                        str(r.stock_code),
                        str(r.rejection_reason),
                        str(r.raw_record)
                    )
                    for r in rejections_df.itertuples(index=False)
                ]
                execute_values(cursor, """
                    INSERT INTO datamart_dw.log_rejections (source_file, invoice_no, stock_code, rejection_reason, raw_record)
                    VALUES %s;
                """, rej_tuples, template="(%s, %s, %s, %s, CAST(%s AS jsonb))", page_size=5000)

        raw_conn.commit()
    finally:
        raw_conn.close()

    logger.info("Carga consolidada en DWH completada con máxima velocidad.")

# ==============================================================================
# ETAPA 5: CONTROL DE CALIDAD (DATA QUALITY GATES)
# ==============================================================================
def run_quality_checks(**kwargs):
    engine = get_dwh_engine()
    with engine.connect() as conn:
        invalid_prices = conn.execute(text("SELECT COUNT(*) FROM datamart_dw.fact_sales WHERE unit_price <= 0")).scalar()
        assert invalid_prices == 0, f"Error: {invalid_prices} ventas con precios <= 0"
        logger.info("Quality checks aprobados al 100%.")

with DAG(
    'datamart_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL DataMart S.A.S. - Carga masiva optimizada',
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='extract_and_unify_sources', python_callable=extract_and_unify_sources)
    t2 = PythonOperator(task_id='fetch_product_catalog', python_callable=fetch_product_catalog)
    t3 = PythonOperator(task_id='transform_and_validate', python_callable=transform_and_validate)
    t4 = PythonOperator(task_id='load_to_dwh', python_callable=load_to_dwh)
    t5 = PythonOperator(task_id='run_quality_checks', python_callable=run_quality_checks)

    [t1, t2] >> t3 >> t4 >> t5