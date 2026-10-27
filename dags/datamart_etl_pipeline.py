import os
import glob
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
        # Formato 1: Sales Transaction v.4a
        'TransactionNo': 'invoice_no',
        'Date': 'invoice_date',
        'ProductNo': 'stock_code',
        'ProductName': 'description',
        'Price': 'unit_price',
        'CustomerNo': 'customer_id',
        
        # Formato 2: data.csv (Kaggle diario)
        'InvoiceNo': 'invoice_no',
        'Invoice': 'invoice_no',
        'StockCode': 'stock_code',
        'Description': 'description',
        'UnitPrice': 'unit_price',
        'InvoiceDate': 'invoice_date',
        'CustomerID': 'customer_id',
        'Customer ID': 'customer_id',
        
        # Comunes
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

        # Renombrar columnas según el diccionario
        df = df.rename(columns=column_mapping)
        df['source_file'] = os.path.basename(file_path)
        dfs.append(df)

    # 1. Unificar ambas fuentes
    unified_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total registros combinados antes de deduplicar: {len(unified_df)}")

    # 2. Manejo de Ambigüedad: Deduplicación por clave compuesta (Sección 5 del PDF)
    # Si la misma factura, producto, cantidad y precio existen en ambos archivos, se conserva una sola ocurrencia
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
# ETAPA 3: TRANSFORMACIÓN Y APLICACIÓN DE REGLAS DE NEGOCIO
# ==============================================================================
def transform_and_validate(**kwargs):
    ti = kwargs['ti']
    unified_path = ti.xcom_pull(task_ids='extract_and_unify_sources', key='unified_data_path')
    df = pd.read_parquet(unified_path)

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

        # Regla: Precios <= 0 en ventas van a rechazos
        if qty > 0 and price <= 0:
            rejections.append({
                'source_file': row['source_file'],
                'invoice_no': str(row['invoice_no']),
                'stock_code': row['stock_code'],
                'rejection_reason': 'PRECIO_INVALIDO_MENOR_IGUAL_CERO',
                'raw_record': f'{{"invoice": "{row["invoice_no"]}", "price": {price}, "qty": {qty}}}'
            })
            continue

        # Regla: Cantidad <= 0 es Devolución
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
                VALUES (:src, :inv, :code, :reason, CAST(:raw AS jsonb))
            """), {
                'src': rej['source_file'],
                'inv': rej['invoice_no'],
                'code': rej['stock_code'],
                'reason': rej['rejection_reason'],
                'raw': str(rej['raw_record'])
            })

    logger.info("Carga consolidada en DWH completada con éxito.")

# ==============================================================================
# ETAPA 5: CONTROL DE CALIDAD
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
    description='Pipeline ETL DataMart S.A.S. - Múltiples fuentes heterogéneas',
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='extract_and_unify_sources', python_callable=extract_and_unify_sources)
    t2 = PythonOperator(task_id='fetch_product_catalog', python_callable=fetch_product_catalog)
    t3 = PythonOperator(task_id='transform_and_validate', python_callable=transform_and_validate)
    t4 = PythonOperator(task_id='load_to_dwh', python_callable=load_to_dwh)
    t5 = PythonOperator(task_id='run_quality_checks', python_callable=run_quality_checks)

    [t1, t2] >> t3 >> t4 >> t5