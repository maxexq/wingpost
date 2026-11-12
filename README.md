# Wingpost

Wingpost is a self-hosted event delivery service. Your projects send it an event with one HTTP call. Wingpost queues the event, signs it, delivers it to every endpoint that subscribed to that event type, and retries until the endpoint answers.

It exists so that each project does not write its own retry, deduplication, and signing code.

## How it works

1. A project sends `POST /v1/events` with an event type and a JSON payload.
2. The API saves the event and one pending delivery for each matching endpoint in one database transaction, then answers `202`.
3. A worker claims the deliveries that are due, sends each one as a signed HTTP `POST`, and records the result.
4. If the endpoint does not answer with a `2xx` status, the worker schedules a retry. After six failed attempts the delivery is marked `dead`.

Delivery is at-least-once. A worker that dies in the middle of an attempt does not lose the delivery, so an endpoint can receive the same delivery twice. Endpoints must deduplicate on the `X-Wingpost-Delivery-Id` header.

The queue is the `deliveries` table in PostgreSQL. Workers claim rows with `FOR UPDATE SKIP LOCKED`, so you can run several workers against one database.

## Run it

You need Docker with the Compose plugin.

```sh
docker compose up --build
```

This starts PostgreSQL, creates the tables, and runs the API on port 8080 and one worker.

Create an app. The command prints the app id and the API key.

```sh
docker compose run --rm api app-create -name smart-home
```

Add an endpoint for that app. The command prints the signing secret.

```sh
docker compose run --rm api endpoint-add \
	-app <app_id> \
	-url https://example.com/hooks/wingpost \
	-events 'home.*'
```

## Send an event

```sh
curl -X POST http://localhost:8080/v1/events \
	-H "Authorization: Bearer <api_key>" \
	-H "Idempotency-Key: door-2026-11-12T09:00:00Z" \
	-d '{"type":"home.door.opened","payload":{"room":"bathroom"}}'
```

The API answers `202` with the event id:

```json
{"id":"d4e9e8a3-610c-4b22-a1fd-6831777786ed","duplicate":false}
```

If you send the same `Idempotency-Key` again for the same app, the API returns the first event's id with `"duplicate":true` and creates no new delivery. The header is optional. Without it, every request creates an event.

An event type is lowercase segments joined by dots, for example `judge.submission.passed`. A segment can contain `a-z`, `0-9`, `_`, and `-`.

An endpoint pattern is one of these forms:

| Pattern | Matches |
| --- | --- |
| `*` | every event type |
| `home.*` | every type that starts with `home.` |
| `home.door.opened` | that type only |

## Receive a delivery

Wingpost sends the event payload as the request body, with these headers:

| Header | Value |
| --- | --- |
| `X-Wingpost-Event` | the event type |
| `X-Wingpost-Event-Id` | the event id |
| `X-Wingpost-Delivery-Id` | the delivery id, the same on every retry |
| `X-Wingpost-Attempt` | the attempt number, starting at 1 |
| `X-Wingpost-Timestamp` | the Unix time of the attempt |
| `X-Wingpost-Signature` | `v1=` and the HMAC-SHA256 signature in hex |

The signed text is the timestamp, a `.`, and the raw request body. The key is the endpoint's signing secret. Verify the signature before you trust the request:

```go
func verify(secret string, r *http.Request, body []byte) bool {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(r.Header.Get("X-Wingpost-Timestamp") + "."))
	mac.Write(body)
	want := "v1=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(want), []byte(r.Header.Get("X-Wingpost-Signature")))
}
```

Answer with a `2xx` status within 10 seconds. Any other answer counts as a failed attempt.

Wingpost stores the payload as `jsonb`, so the delivered body can differ from the body you sent in key order and whitespace. The signature covers the delivered body.

## Retry schedule

| After failed attempt | Wait before the next attempt |
| --- | --- |
| 1 | 10 seconds |
| 2 | 1 minute |
| 3 | 5 minutes |
| 4 | 30 minutes |
| 5 | 2 hours |
| 6 | none, the delivery is marked `dead` |

## Configuration

| Variable | Used by | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | every command | PostgreSQL connection URL. Required. |
| `ADDR` | `api` | Listen address. Default `:8080`. |

The worker takes flags: `-interval` (default `1s`), `-batch` (default `20`), and `-timeout` (default `10s`). Run `wingpost help` for the list of commands.

## Develop

You need Go 1.24 or later. The tests for `internal/postgres` need a database and skip without one.

```sh
docker compose up -d postgres
export WINGPOST_TEST_DATABASE_URL='postgres://wingpost:wingpost@localhost:5432/wingpost?sslmode=disable'
make check
```

The tests for `internal/postgres` delete every row in `deliveries`. Do not point `WINGPOST_TEST_DATABASE_URL` at a database you want to keep.

`make check` runs the format check, `go vet`, and the test suite four times: three runs with different shuffle seeds and the race detector, and one run in serial.

### Layout

| Path | Contents |
| --- | --- |
| `internal/core` | Types, event pattern matching, the retry schedule, and signing. No I/O. |
| `internal/ingest` | The HTTP API. Declares the `Store` interface that it needs. |
| `internal/dispatch` | The worker and the HTTP sender. Declares the `Queue` and `Sender` interfaces that it needs. |
| `internal/postgres` | The PostgreSQL implementation of `Store` and `Queue`, and the schema. |
| `internal/memstore` | An in-memory implementation with the same behaviour. The tests use it as a fake. |
| `cmd/wingpost` | The binary: `migrate`, `api`, `worker`, `app-create`, `endpoint-add`. |

## Not built yet

- OpenTelemetry traces and metrics, with trace context passed from the sender to the endpoint
- Connectors that format a message for Discord, Telegram, and LINE
- An API to list deliveries and replay dead ones
- Rate limits for each app and a circuit breaker for each endpoint
- Jitter on the retry schedule, and concurrent sends inside one batch
- A Helm chart, with worker autoscaling on queue depth
- Kafka between the API and the worker
