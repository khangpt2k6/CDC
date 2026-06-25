-- Demo OLTP schema for the CDC pipeline.
--
-- Runs once on first Postgres init (mounted into docker-entrypoint-initdb.d).
-- A small e-commerce slice gives Debezium realistic inserts, updates, and
-- deletes to capture, and gives the ClickHouse side something worth charting.

-- Customers placing orders.
CREATE TABLE IF NOT EXISTS public.customers (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text        NOT NULL,
    email      text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Orders belonging to customers.
CREATE TABLE IF NOT EXISTS public.orders (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  bigint      NOT NULL REFERENCES public.customers (id),
    status       text        NOT NULL DEFAULT 'pending',
    amount_cents bigint      NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- Full before-images for UPDATE/DELETE so downstream sees old row state
-- regardless of the default replica identity (primary key only).
ALTER TABLE public.customers REPLICA IDENTITY FULL;
ALTER TABLE public.orders    REPLICA IDENTITY FULL;

-- Publication consumed by the Debezium pgoutput connector.
CREATE PUBLICATION cdc_pub FOR TABLE public.customers, public.orders;

-- Seed rows so the initial snapshot is non-empty.
INSERT INTO public.customers (name, email) VALUES
    ('Ada Lovelace',   'ada@example.com'),
    ('Alan Turing',    'alan@example.com'),
    ('Grace Hopper',   'grace@example.com');

INSERT INTO public.orders (customer_id, status, amount_cents) VALUES
    (1, 'paid',    1299),
    (1, 'pending',  499),
    (2, 'paid',    8400),
    (3, 'shipped', 2500);
