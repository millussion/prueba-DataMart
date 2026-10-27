CREATE SCHEMA IF NOT EXISTS datamart_dw;
SET search_path TO datamart_dw, public;

CREATE TABLE IF NOT EXISTS dim_products (
    product_key SERIAL PRIMARY KEY,
    stock_code VARCHAR(50) UNIQUE NOT NULL,
    canonical_description TEXT NOT NULL,
    category VARCHAR(100) NOT NULL,
    supplier_country VARCHAR(100) DEFAULT 'Colombia',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    country VARCHAR(100) NOT NULL,
    customer_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INT NOT NULL,
    day_of_week VARCHAR(20) NOT NULL,
    quarter INT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id SERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    product_key INT NOT NULL REFERENCES dim_products(product_key),
    customer_key INT NOT NULL REFERENCES dim_customers(customer_key),
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    transaction_date_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 4) NOT NULL CHECK (unit_price > 0),
    gross_revenue NUMERIC(14, 4) NOT NULL,
    country VARCHAR(100) NOT NULL,
    source_file VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_sale_line UNIQUE (invoice_no, product_key, transaction_date_utc, quantity, unit_price)
);

CREATE TABLE IF NOT EXISTS fact_returns (
    return_id SERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    product_key INT NOT NULL REFERENCES dim_products(product_key),
    customer_key INT NOT NULL REFERENCES dim_customers(customer_key),
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    return_date_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    quantity_returned INT NOT NULL CHECK (quantity_returned > 0),
    unit_price NUMERIC(12, 4) NOT NULL,
    refund_amount NUMERIC(14, 4) NOT NULL,
    country VARCHAR(100) NOT NULL,
    source_file VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_return_line UNIQUE (invoice_no, product_key, return_date_utc, quantity_returned, unit_price)
);

CREATE TABLE IF NOT EXISTS log_rejections (
    rejection_id SERIAL PRIMARY KEY,
    source_file VARCHAR(100) NOT NULL,
    invoice_no VARCHAR(50),
    stock_code VARCHAR(50),
    rejection_reason VARCHAR(255) NOT NULL,
    raw_record JSONB NOT NULL,
    rejected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);