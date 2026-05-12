-- Initial schema + data + publication + replication slot для Flink CDC.
-- Запустить: docker compose exec postgres psql -U cdc -d cdcdb -f /init/init.sql

-- 1. Tables
DROP TABLE IF EXISTS public.orders CASCADE;
CREATE TABLE public.orders (
    id           BIGSERIAL PRIMARY KEY,
    customer_id  BIGINT NOT NULL,
    amount       NUMERIC(10, 2) NOT NULL,
    status       VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMP NOT NULL DEFAULT now()
);

DROP TABLE IF EXISTS public.customers CASCADE;
CREATE TABLE public.customers (
    id       BIGSERIAL PRIMARY KEY,
    name     VARCHAR(100) NOT NULL,
    email    VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- 2. Replica identity FULL — нужно для CDC чтобы DELETE/UPDATE содержали prev row image
ALTER TABLE public.orders REPLICA IDENTITY FULL;
ALTER TABLE public.customers REPLICA IDENTITY FULL;

-- 3. Seed data
INSERT INTO public.customers (name, email)
SELECT 'customer_' || gs, 'c' || gs || '@example.com'
FROM generate_series(1, 200) gs;

INSERT INTO public.orders (customer_id, amount, status)
SELECT
    1 + (gs % 200),
    (random() * 500)::numeric(10, 2),
    (ARRAY['pending', 'paid', 'shipped', 'cancelled'])[1 + (gs % 4)]
FROM generate_series(1, 1000) gs;

-- 4. Publication для logical replication
DROP PUBLICATION IF EXISTS cdc_pub;
CREATE PUBLICATION cdc_pub FOR TABLE public.orders, public.customers;

-- 5. Replication slot (pgoutput)
SELECT pg_drop_replication_slot('flink_slot')
FROM pg_replication_slots
WHERE slot_name = 'flink_slot';

SELECT pg_create_logical_replication_slot('flink_slot', 'pgoutput');

-- 6. Grants для CDC user (sufficient permissions to read tables и replication)
GRANT SELECT ON public.orders, public.customers TO cdc;
ALTER USER cdc WITH REPLICATION;
