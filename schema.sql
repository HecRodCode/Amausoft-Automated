-- Regions Dimension
CREATE TABLE IF NOT EXISTS dim_regions (
    region_id VARCHAR(100) PRIMARY KEY,
    region VARCHAR(100),
    subregion VARCHAR(100),
    country_code VARCHAR(10)
);

-- Product Dimension
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(100)
);

-- Clients Dimension
CREATE TABLE IF NOT EXISTS dim_clients (
    client_id SERIAL PRIMARY KEY,
    name VARCHAR(150),
    city VARCHAR(100),
    country VARCHAR(100),
    email VARCHAR(150),
    fetch_timestamp TIMESTAMP
);

-- Fact Sales
CREATE TABLE IF NOT EXISTS fact_sales (
    order_id SERIAL PRIMARY KEY,
    order_number INT,
    quantity_ordered INT,
    price_each DECIMAL(10,2),
    sales_amount DECIMAL(10,2),
    order_date DATE,

    region_id INTEGER
        REFERENCES dim_regions(region_id)

    product_id INTEGER
        REFERENCES dim_products(product_id),

    client_id INTEGER
        REFERENCES dim_clients(client_id)
);