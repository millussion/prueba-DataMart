import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine, text

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

CSV_FILE_NAME = "Sales Transaction v.4a.csv"

def get_dwh_engine():
    conn = BaseHook.get_connection('POSTGRES_DWH')
    db_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(db_url)

# ==============================================================================
# ETAPA 1: EXTRACCIÓN EXCLUSIVA DE 'Sales Transaction v.4a.csv'
# ==============================================================================
def extract_single_source(**kwargs):
    raw_path = Variable.get("RAW_DATA_PATH", default_var="/opt/airflow/data")
    file_path = os.path.join(raw_path, CSV_FILE_NAME)

    # Si por alguna razón está dentro de una subcarpeta o con mayúsculas/minúsculas
    if not os.path.exists(file_path):
        import glob
        matches = glob.glob(os.path.join(raw_path, "**", "*Sales Transaction*.csv"), recursive=True)
        if matches:
            file_path = matches[0]
        else:
            raise FileNotFoundError(f"No se encontró el archivo {CSV_FILE_NAME} en {raw_path}")

    logger.info(f"Cargando archivo: {file_path}")

    # Soporte para ambas codificaciones habituales
    try:
        df = pd.read_csv(file_path, dtype=str, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, dtype=str, encoding='ISO-8859-1')

    # Mapeo universal de columnas según la estructura de Sales Transaction v.4a
    column_mapping = {
        'TransactionNo': 'invoice_no',
        'InvoiceNo': 'invoice_no',
        'Invoice': 'invoice_no',
        
        'ProductNo': 'stock_code',
        'StockCode': 'stock_code',
        
        'ProductName': 'description',
        'Description': 'description',
        
        'Price': 'unit_price',
        'UnitPrice': 'unit_price',
        
        'Quantity': 'quantity',
        
        'Date': 'invoice_date',
        'InvoiceDate': 'invoice_date',
        
        'CustomerNo': 'customer_id',
        'CustomerID': 'customer_id',
        'Customer ID': 'customer_id',
        
        'Country': 'country'
    }
    df = df.rename(columns=column_mapping)
    df['source_file'] = CSV_FILE_NAME

    # Deduplicación inicial de filas exactamente repetidas
    df = df.drop_duplicates()

    tmp_path = "/tmp/raw_transactions.parquet"
    df.to_parquet(tmp_path, index=False)
    kwargs['ti'].xcom_push(key='raw_data_path', value=tmp_path)
    logger.info(f"Registros extraídos correctamente: {len(df)}")

# ==============================================================================
# ETAPA 2: CONSULTA AL CATÁLOGO DE PRODUCTOS (API FASTAPI)
# ==============================================================================
# Reemplazar la función fetch_product_catalog en dags/datamart_etl_pipeline.py:
def fetch_product_catalog(**kwargs):
    logger.info("Cargando catálogo estático interno de DataMart (Estrategia Sección 4.3)...")
    
    # Catálogo precargado con las categorías oficiales de DataMart
    catalog_map = {
        "85123A": {"category": "Hogar", "supplier_country": "Colombia"},
        "71053":  {"category": "Hogar", "supplier_country": "Colombia"},
        "84406B": {"category": "Hogar", "supplier_country": "México"},
        "84029G": {"category": "Ropa", "supplier_country": "Perú"},
        "84029E": {"category": "Ropa", "supplier_country": "Perú"},
        "22752":  {"category": "Papelería", "supplier_country": "Colombia"},
        "21730":  {"category": "Hogar", "supplier_country": "México"},
        "22633":  {"category": "Electrónica", "supplier_country": "Colombia"},
        "22632":  {"category": "Deportes", "supplier_country": "Perú"},
    }
    kwargs['ti'].xcom_push(key='catalog_map', value=catalog_map)

# ==============================================================================
# ETAPA 3: TRANSFORMACIÓN Y APLICACIÓN DE REGLAS DE NEGOCIO
# ==============================================================================
def transform_and_validate(**kwargs):
    ti = kwargs['ti']
    raw_path = ti.xcom_pull(task_ids='extract_single_source', key='raw_data_path')
    df = pd.read_parquet(raw_path)

    # 1. Normalización de códigos de producto (mayúsculas y sin espacios)
    df['stock_code'] = df['stock_code'].astype(str).str.strip().str.upper()

    # 2. Parseo numérico
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')

    # 3. Estandarización de Fechas a UTC
    df['parsed_date'] = pd.to_datetime(df['invoice_date'], format='mixed', errors='coerce')
    df['parsed_date'] = df['parsed_date'].dt.tz_localize('UTC')
    df['date_key'] = df['parsed_date'].dt.strftime('%Y%m%d').astype(int)

    # 4. Manejo de Clientes Nulos -> INVITADO
    df['customer_id'] = df['customer_id'].fillna('GUEST_USER').astype(str).str.replace(r'\.0$', '', regex=True)
    df['customer_type'] = df['customer_id'].apply(lambda x: 'INVITADO' if x in ('GUEST_USER', 'nan', '') else 'REGISTRADO')

    # 5. Descripción canónica por producto (Moda estadística)
    canonical_descriptions = df.groupby('stock_code')['description'].agg(
        lambda s: s.dropna().str.strip().str.upper().mode()[0] if not s.dropna().empty else 'SIN DESCRIPCION'
    ).to_dict()
    df['canonical_description'] = df['stock_code'].map(canonical_descriptions)

    rejections, sales, returns = [], [], []

    for _, row in df.iterrows():
        qty = row['quantity']
        price = row['unit_price']

        # Validación de campos nulos
        if pd.isna(qty) or pd.isna(price) or pd.isna(row['parsed_date']):
            rejections.append({
                'source_file': row['source_file'],
                'invoice_no': str(row['invoice_no']),
                'stock_code': row['stock_code'],
                'rejection_reason': 'VALOR_NULO_EN_CAMPOS_CRITICOS',
                'raw_record': f'{{"invoice": "{row["invoice_no"]}", "qty": "{qty}", "price": "{price}"}}'
            })
            continue

        # Regla de Negocio: Precios <= 0 en ventas van a rechazos
        if qty > 0 and price <= 0:
            rejections.append({
                'source_file': row['source_file'],
                'invoice_no': str(row['invoice_no']),
                'stock_code': row['stock_code'],
                'rejection_reason': 'PRECIO_INVALIDO_MENOR_IGUAL_CERO',
                'raw_record': f'{{"invoice": "{row["invoice_no"]}", "price": {price}, "qty": {qty}}}'
            })
            continue

        # Regla de Negocio: Cantidad <= 0 es Devolución
        if qty <= 0:
            abs_qty = abs(int(qty))
            returns.append({
                'invoice_no': str(row['invoice_no']),
                'stock_code': row['stock_code'],
                'customer_id': row['customer_id'],
                'customer_type': row['customer_type'],
                'canonical_description': row['canonical_description'],
                'date_key': row['date_key'],
                'date_full': row['parsed_date'].strftime('%Y-%m-%d'),
                'return_date_utc': row['parsed_date'].isoformat(),
                'quantity_returned': abs_qty,
                'unit_price': float(price),
                'refund_amount': round(abs_qty * float(price), 4),
                'country': str(row['country']),
                'source_file': row['source_file']
            })
        else: # Venta normal
            sales.append({
                'invoice_no': str(row['invoice_no']),
                'stock_code': row['stock_code'],
                'customer_id': row['customer_id'],
                'customer_type': row['customer_type'],
                'canonical_description': row['canonical_description'],
                'date_key': row['date_key'],
                'date_full': row['parsed_date'].strftime('%Y-%m-%d'),
                'transaction_date_utc': row['parsed_date'].isoformat(),
                'quantity': int(qty),
                'unit_price': float(price),
                'gross_revenue': round(int(qty) * float(price), 4),
                'country': str(row['country']),
                'source_file': row['source_file']
            })

    pd.DataFrame(sales).to_parquet('/tmp/clean_sales.parquet', index=False)
    pd.DataFrame(returns).to_parquet('/tmp/clean_returns.parquet', index=False)
    pd.DataFrame(rejections).to_parquet('/tmp/clean_rejections.parquet', index=False)
    logger.info(f"Transformación lista: {len(sales)} ventas, {len(returns)} devoluciones, {len(rejections)} rechazos.")

# ==============================================================================
# ETAPA 4: CARGA IDEMPOTENTE AL DATA WAREHOUSE (POSTGRESQL)
# ==============================================================================
def load_to_dwh(**kwargs):
    ti = kwargs['ti']
    catalog_map = ti.xcom_pull(task_ids='fetch_product_catalog', key='catalog_map') or {}

    engine = get_dwh_engine()
    
    sales_df = pd.read_parquet('/tmp/clean_sales.parquet')
    returns_df = pd.read_parquet('/tmp/clean_returns.parquet')
    rejections_df = pd.read_parquet('/tmp/clean_rejections.parquet')

    with engine.begin() as conn:
        # 1. Dimensión Fechas
        dates_all = pd.concat([
            sales_df[['date_key', 'date_full']],
            returns_df[['date_key', 'date_full']]
        ]).drop_duplicates(subset=['date_key'])

        for _, d in dates_all.iterrows():
            dt = datetime.strptime(d['date_full'], '%Y-%m-%d')
            conn.execute(text("""
                INSERT INTO datamart_dw.dim_date (date_key, full_date, year, month, month_name, day, day_of_week, quarter)
                VALUES (:k, :fd, :y, :m, :mn, :d, :dow, :q)
                ON CONFLICT (date_key) DO NOTHING;
            """), {
                'k': int(d['date_key']),
                'fd': dt.date(),
                'y': dt.year,
                'm': dt.month,
                'mn': dt.strftime('%B'),
                'd': dt.day,
                'dow': dt.strftime('%A'),
                'q': (dt.month - 1) // 3 + 1
            })

        # 2. Dimensión Clientes
        cust_all = pd.concat([
            sales_df[['customer_id', 'country', 'customer_type']],
            returns_df[['customer_id', 'country', 'customer_type']]
        ]).drop_duplicates(subset=['customer_id'])

        for _, c in cust_all.iterrows():
            conn.execute(text("""
                INSERT INTO datamart_dw.dim_customers (customer_id, country, customer_type)
                VALUES (:cid, :co, :ct)
                ON CONFLICT (customer_id) DO UPDATE SET country = EXCLUDED.country;
            """), {'cid': c['customer_id'], 'co': c['country'], 'ct': c['customer_type']})

        # 3. Dimensión Productos
        prod_all = pd.concat([
            sales_df[['stock_code', 'canonical_description']],
            returns_df[['stock_code', 'canonical_description']]
        ]).drop_duplicates(subset=['stock_code'])

        for _, p in prod_all.iterrows():
            code = p['stock_code']
            api_info = catalog_map.get(code, {})
            category = api_info.get('category', 'Hogar')
            supplier = api_info.get('supplier_country', 'Colombia')
            is_active = api_info.get('is_active', True)

            conn.execute(text("""
                INSERT INTO datamart_dw.dim_products (stock_code, canonical_description, category, supplier_country, is_active)
                VALUES (:code, :desc, :cat, :sup, :act)
                ON CONFLICT (stock_code) DO UPDATE 
                SET canonical_description = EXCLUDED.canonical_description,
                    category = EXCLUDED.category;
            """), {
                'code': code,
                'desc': p['canonical_description'],
                'cat': category,
                'sup': supplier,
                'act': is_active
            })

        prod_map = dict(conn.execute(text("SELECT stock_code, product_key FROM datamart_dw.dim_products")).fetchall())
        cust_map = dict(conn.execute(text("SELECT customer_id, customer_key FROM datamart_dw.dim_customers")).fetchall())

        # 4. Hechos de Ventas (Idempotente)
        for _, s in sales_df.iterrows():
            conn.execute(text("""
                INSERT INTO datamart_dw.fact_sales 
                (invoice_no, product_key, customer_key, date_key, transaction_date_utc, quantity, unit_price, gross_revenue, country, source_file)
                VALUES (:inv, :pk, :ck, :dk, :dt, :qty, :up, :gr, :cou, :src)
                ON CONFLICT (invoice_no, product_key, transaction_date_utc, quantity, unit_price) DO NOTHING;
            """), {
                'inv': s['invoice_no'],
                'pk': prod_map[s['stock_code']],
                'ck': cust_map[s['customer_id']],
                'dk': int(s['date_key']),
                'dt': s['transaction_date_utc'],
                'qty': int(s['quantity']),
                'up': float(s['unit_price']),
                'gr': float(s['gross_revenue']),
                'cou': s['country'],
                'src': s['source_file']
            })

        # 5. Hechos de Devoluciones (Idempotente)
        for _, r in returns_df.iterrows():
            conn.execute(text("""
                INSERT INTO datamart_dw.fact_returns 
                (invoice_no, product_key, customer_key, date_key, return_date_utc, quantity_returned, unit_price, refund_amount, country, source_file)
                VALUES (:inv, :pk, :ck, :dk, :dt, :qty, :up, :ref, :cou, :src)
                ON CONFLICT (invoice_no, product_key, return_date_utc, quantity_returned, unit_price) DO NOTHING;
            """), {
                'inv': r['invoice_no'],
                'pk': prod_map[r['stock_code']],
                'ck': cust_map[r['customer_id']],
                'dk': int(r['date_key']),
                'dt': r['return_date_utc'],
                'qty': int(r['quantity_returned']),
                'up': float(r['unit_price']),
                'ref': float(r['refund_amount']),
                'cou': r['country'],
                'src': r['source_file']
            })

        # 6. Log de Rechazos
        for _, rej in rejections_df.iterrows():
            conn.execute(text("""
                INSERT INTO datamart_dw.log_rejections (source_file, invoice_no, stock_code, rejection_reason, raw_record)
                VALUES (:src, :inv, :code, :reason, :raw::jsonb)
            """), {
                'src': rej['source_file'],
                'inv': rej['invoice_no'],
                'code': rej['stock_code'],
                'reason': rej['rejection_reason'],
                'raw': rej['raw_record']
            })

    logger.info("Carga en DWH completada exitosamente.")

# ==============================================================================
# ETAPA 5: CONTROL DE CALIDAD (DATA QUALITY GATES)
# ==============================================================================
def run_quality_checks(**kwargs):
    engine = get_dwh_engine()
    with engine.connect() as conn:
        invalid_prices = conn.execute(text("SELECT COUNT(*) FROM datamart_dw.fact_sales WHERE unit_price <= 0")).scalar()
        assert invalid_prices == 0, f"Error: {invalid_prices} ventas con precios <= 0 en fact_sales"
        logger.info("Quality checks aprobados al 100%.")

with DAG(
    'datamart_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL DataMart S.A.S. - Fuente: Sales Transaction v.4a.csv',
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='extract_single_source', python_callable=extract_single_source)
    t2 = PythonOperator(task_id='fetch_product_catalog', python_callable=fetch_product_catalog)
    t3 = PythonOperator(task_id='transform_and_validate', python_callable=transform_and_validate)
    t4 = PythonOperator(task_id='load_to_dwh', python_callable=load_to_dwh)
    t5 = PythonOperator(task_id='run_quality_checks', python_callable=run_quality_checks)

    [t1, t2] >> t3 >> t4 >> t5