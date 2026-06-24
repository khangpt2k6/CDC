-- Seed objects for local CDC development.
--
-- Runs once on first Postgres init (mounted into docker-entrypoint-initdb.d).
-- Provides a sample table and a publication so Phase 1 has something to capture.

-- Sample table the pipeline captures changes from.
CREATE TABLE IF NOT EXISTS public.demo_events (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payload    text        NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Full before-images for UPDATE/DELETE so local testing sees old row state
-- regardless of the default replica identity (primary key only).
ALTER TABLE public.demo_events REPLICA IDENTITY FULL;

-- Heartbeat table (Phase 1, Issue 1.5): a timer updates one row to generate
-- WAL on idle databases so the slot's confirmed_flush_lsn keeps advancing.
CREATE TABLE IF NOT EXISTS public.cdc_heartbeat (
    id int PRIMARY KEY,
    ts timestamptz NOT NULL DEFAULT now()
);
INSERT INTO public.cdc_heartbeat (id, ts)
VALUES (1, now())
ON CONFLICT (id) DO NOTHING;

-- Publication consumed by the pgoutput logical slot in Phase 1.
CREATE PUBLICATION cdc_pub FOR TABLE public.demo_events, public.cdc_heartbeat;

-- A couple of seed rows so the sample table is non-empty.
INSERT INTO public.demo_events (payload) VALUES ('hello'), ('world');
