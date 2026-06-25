-- Seed objects for local CDC development.
--
-- Runs once on first Postgres init (mounted into docker-entrypoint-initdb.d).
-- Statements run top to bottom, so order matters: schema, then publication,
-- then seed data. This runs ONLY when the data volume is empty; to re-apply
-- after edits, recreate the volume: `docker compose down -v && docker compose up -d`.
--
-- The customers/orders tables are a small e-commerce slice acting as the SOURCE
-- application this CDC pipeline captures from. The business columns are demo
-- props; the pipeline itself is Debezium -> Kafka -> Go -> ClickHouse.

-- ============================================================
-- 1. Schema (the captured source tables)
-- ============================================================

-- Customers placing orders. REPLICA IDENTITY FULL so UPDATE/DELETE events carry
-- the full old row image, not just the primary key.
CREATE TABLE public.customers (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email       text        NOT NULL UNIQUE,
    full_name   text        NOT NULL,
    country     text        NOT NULL DEFAULT 'US',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.customers REPLICA IDENTITY FULL;

-- Orders belonging to a customer. status is text + CHECK rather than a native
-- enum (enums stream awkwardly through Debezium); total_amount is numeric, not
-- float, for exact currency. Both choices map cleanly to ClickHouse later.
CREATE TABLE public.orders (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  bigint        NOT NULL REFERENCES public.customers (id),
    status       text          NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled')),
    total_amount numeric(12, 2) NOT NULL CHECK (total_amount >= 0),
    currency     char(3)       NOT NULL DEFAULT 'USD',
    placed_at    timestamptz   NOT NULL DEFAULT now(),
    updated_at   timestamptz   NOT NULL DEFAULT now()
);
ALTER TABLE public.orders REPLICA IDENTITY FULL;
CREATE INDEX ON public.orders (customer_id);

-- Heartbeat table: a periodic UPDATE to its single row generates WAL on an idle
-- database so the replication slot's confirmed_flush_lsn keeps advancing. Not a
-- demo table - it is pipeline infrastructure (backs the OP_HEARTBEAT envelope).
CREATE TABLE IF NOT EXISTS public.cdc_heartbeat (
    id int PRIMARY KEY,
    ts timestamptz NOT NULL DEFAULT now()
);
INSERT INTO public.cdc_heartbeat (id, ts)
VALUES (1, now())
ON CONFLICT (id) DO NOTHING;

-- ============================================================
-- 2. Publication consumed by the pgoutput logical slot (Phase 1)
-- ============================================================

CREATE PUBLICATION cdc_pub
    FOR TABLE public.customers, public.orders, public.cdc_heartbeat;

-- ============================================================
-- 3. Seed data (a believable starting story)
-- ============================================================

-- Customers first: orders reference them. id is GENERATED ALWAYS, so we never
-- insert it explicitly; on a fresh volume the sequence yields ids 1..5.
INSERT INTO public.customers (email, full_name, country) VALUES
    ('ava@example.com',   'Ava Nguyen',   'US'),
    ('ben@example.com',   'Ben Carter',   'US'),
    ('chloe@example.com', 'Chloe Martin', 'FR'),
    ('dev@example.com',   'Dev Patel',    'IN'),
    ('emma@example.com',  'Emma Schmidt', 'DE');

-- Orders across those customers, mixed statuses and currencies so the demo can
-- narrate inserts, status updates, and a delete. Ava (id 1) has two orders.
INSERT INTO public.orders (customer_id, status, total_amount, currency) VALUES
    (1, 'delivered', 129.99, 'USD'),
    (1, 'paid',       42.50, 'USD'),
    (2, 'shipped',   310.00, 'USD'),
    (3, 'pending',    89.00, 'EUR'),
    (3, 'cancelled',  15.75, 'EUR'),
    (4, 'paid',      750.00, 'USD'),
    (5, 'delivered',  62.00, 'EUR');
