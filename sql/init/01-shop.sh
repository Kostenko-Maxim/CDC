#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=cdc_password="$CDC_PASSWORD" <<'SQL'
CREATE ROLE cdc LOGIN REPLICATION PASSWORD :'cdc_password';
GRANT CREATE ON DATABASE shop TO cdc;
CREATE SCHEMA shop AUTHORIZATION cdc;
SET ROLE cdc;

CREATE TABLE shop.customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE
);
CREATE TABLE shop.products (
    product_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(12, 2) NOT NULL CHECK (price >= 0)
);
CREATE TABLE shop.orders (
    order_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES shop.customers,
    product_id INTEGER NOT NULL REFERENCES shop.products,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    amount NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    status TEXT NOT NULL DEFAULT 'new',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE shop.orders REPLICA IDENTITY FULL;
CREATE TABLE shop.dbz_heartbeat (
    id INTEGER PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL
);
INSERT INTO shop.dbz_heartbeat VALUES (1, now());
INSERT INTO shop.customers
SELECT g, 'Customer ' || g, 'customer-' || g || '@example.test'
FROM generate_series(1, 1000) g;
INSERT INTO shop.products
SELECT g, 'Product ' || g, (10 + g * 0.99)::NUMERIC(12,2)
FROM generate_series(1, 100) g;
INSERT INTO shop.orders (customer_id, product_id, quantity, amount, status)
SELECT 1 + (g % 1000), 1 + (g % 100), 1 + (g % 5),
       ((10 + (1 + g % 100) * 0.99) * (1 + g % 5))::NUMERIC(12,2),
       CASE WHEN g % 3 = 0 THEN 'paid' ELSE 'new' END
FROM generate_series(1, 20000) g;
SQL
