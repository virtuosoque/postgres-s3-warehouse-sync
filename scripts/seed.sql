-- Local dev seed: a small 'events' table for end-to-end smoke tests.
CREATE TABLE IF NOT EXISTS public.events (
    id           BIGSERIAL PRIMARY KEY,
    user_id      BIGINT NOT NULL,
    event_type   TEXT   NOT NULL,
    payload      JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS events_updated_at_id_idx
  ON public.events (updated_at, id);

INSERT INTO public.events (user_id, event_type, payload)
SELECT
    (random() * 10000)::bigint,
    (ARRAY['click','view','purchase','signup'])[1 + (random() * 3)::int],
    jsonb_build_object('ref', md5(random()::text))
FROM generate_series(1, 100000);

CREATE TABLE IF NOT EXISTS public.users (
    id          BIGSERIAL PRIMARY KEY,
    email       TEXT UNIQUE NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO public.users (email)
SELECT md5(random()::text) || '@example.com'
FROM generate_series(1, 10000)
ON CONFLICT DO NOTHING;
