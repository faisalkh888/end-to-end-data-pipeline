CREATE ROLE extractor LOGIN PASSWORD :'extractor_password';
CREATE ROLE loader LOGIN PASSWORD :'loader_password';
CREATE ROLE airflow LOGIN PASSWORD :'airflow_password';
CREATE DATABASE airflow OWNER airflow;
GRANT CONNECT ON DATABASE commerce TO extractor, loader;
CREATE SCHEMA IF NOT EXISTS source;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE source.customers (
  customer_id INTEGER PRIMARY KEY, full_name TEXT NOT NULL, email TEXT,
  country TEXT NOT NULL, segment TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE source.products (
  product_id INTEGER PRIMARY KEY, product_name TEXT NOT NULL, category TEXT NOT NULL,
  unit_price NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO source.customers VALUES
 (1,'Avery Chen','avery@example.test','US','standard','2025-01-01'),
 (2,'Morgan Singh','morgan@example.test','GB','premium','2025-01-01'),
 (3,'Taylor Garcia','taylor@example.test','CA','standard','2025-01-01')
ON CONFLICT DO NOTHING;
INSERT INTO source.products VALUES
 (101,'Trail Bottle','outdoors',24.00,'2025-01-01'),
 (102,'Canvas Tote','accessories',18.50,'2025-01-01'),
 (103,'Desk Lamp','home',42.00,'2025-01-01')
ON CONFLICT DO NOTHING;
GRANT USAGE ON SCHEMA source TO extractor;
GRANT SELECT ON ALL TABLES IN SCHEMA source TO extractor;
ALTER DEFAULT PRIVILEGES IN SCHEMA source GRANT SELECT ON TABLES TO extractor;
GRANT USAGE ON SCHEMA warehouse, ops TO loader;
GRANT CREATE ON SCHEMA warehouse, ops TO loader;
