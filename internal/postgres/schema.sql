-- Wingpost schema. Every statement is safe to run again.

CREATE TABLE IF NOT EXISTS apps (
	id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	name       text NOT NULL,
	key_hash   text NOT NULL UNIQUE,
	created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS endpoints (
	id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	app_id     uuid NOT NULL REFERENCES apps (id),
	url        text NOT NULL,
	secret     text NOT NULL,
	patterns   text[] NOT NULL,
	enabled    boolean NOT NULL DEFAULT true,
	created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS endpoints_app ON endpoints (app_id);

CREATE TABLE IF NOT EXISTS events (
	id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	app_id          uuid NOT NULL REFERENCES apps (id),
	type            text NOT NULL,
	payload         jsonb NOT NULL,
	idempotency_key text,
	created_at      timestamptz NOT NULL
);

-- NULL keys are distinct, so events without a key never conflict.
CREATE UNIQUE INDEX IF NOT EXISTS events_idempotency ON events (app_id, idempotency_key);

CREATE TABLE IF NOT EXISTS deliveries (
	id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	event_id        uuid NOT NULL REFERENCES events (id),
	endpoint_id     uuid NOT NULL REFERENCES endpoints (id),
	status          text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'delivered', 'dead')),
	attempts        integer NOT NULL DEFAULT 0,
	next_attempt_at timestamptz NOT NULL,
	last_error      text NOT NULL DEFAULT '',
	updated_at      timestamptz NOT NULL
);

-- The worker reads only pending rows, ordered by their due time.
CREATE INDEX IF NOT EXISTS deliveries_due ON deliveries (next_attempt_at) WHERE status = 'pending';
