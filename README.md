# Concurrent File Processing Pipeline

Implementation of `Concurrent File Processing Pipeline — Implementation Doc.md`: browser → presigned S3 upload,
Celery extraction (prefork), fair dispatcher + 20-slot Redis semaphore + circuit breaker → mock vendor (gevent),
Postgres outbox, task ids / events / SSE progress, DLQ + replay, completion notifications.

## Prerequisites
- Python 3.12+ and Node 22+ for local development and tests.
- Docker Engine with Compose v2 (`docker compose`) or v1 (`docker-compose`) for the full stack.

```sh
make install    # creates .venv with dev deps and installs web/node_modules (re-runs only when deps change)
make help       # lists every target
```

## Run

### With Docker (full stack)
```sh
make dev        # build and start everything
make logs       # follow logs from all services
make down       # stop the stack
```
The Makefile detects either Compose command and checks the Engine is running; set `COMPOSE` to override.

| Service | URL |
|---|---|
| Web app | http://localhost:5173 |
| API | http://localhost:8000 |
| Mock vendor | http://localhost:4000 |
| MailHog (email notifications) | http://localhost:8025 |
| Flower (Celery) | http://localhost:5555 |

### Without Docker
```sh
make dev-local                                          # terminal 1: API, workers, dispatcher, mock vendor
cd web && VITE_API_URL=http://127.0.0.1:8000 npm run dev  # terminal 2: web app on :5173
```
fakeredis, moto and SQLite stand in for Redis, MinIO and Postgres; logs go to `.run/logs/`. Workers use the
thread pool instead of prefork/gevent (see the note in `scripts/dev_nodocker.sh`). Vendor replies are sped up
10×; `MOCK_LATENCY_SCALE=1 make dev-local` gives the real 10–20 s. Ctrl-C stops every process.

### Configuration
Every setting in `pipeline/config.py` can be overridden by environment variables, a `.env` file
(see `.env.example`) or `docker-compose.yml`: vendor slots and timeouts, mock vendor latency and fault rates.

## Using the web app
1. **Pick a user** in the header (a stub bearer token; press Enter to switch). Each user sees only their batches.
2. **Choose files**: drop or select PDF, CSV, XLSX or JSON, up to 100 MB each and 50 per batch. Selecting again
   adds to the list; unsupported, empty or oversized files are flagged and must be removed before uploading.
3. **Choose notifications**: in app, email (shown in MailHog) or webhook, optionally on the first failure too.
4. **Upload**: files are hashed, a batch is created and each file goes straight to S3. Cancel stops the remaining
   uploads; selecting the same files again resumes an interrupted or cancelled upload.
5. **Follow progress**: the batch view updates live over SSE (falling back to polling). Filter by in progress,
   failed or done, and click a file to see its stages, history and error cause. Failed extraction or delivery can
   be retried per file or for the whole batch. The selected batch is kept in the URL, so refresh keeps the view.

The header shows vendor slots in use, circuit breaker state, queue depth and throughput.

## Tests
```sh
make test               # unit + in-process integration (SQLite, fakeredis, moto, mock vendor) + web typecheck
make test-unit          # or test-integration / test-web on their own
make test-unit PYTEST_ARGS="-k extractors"   # pass extra pytest arguments
make test-e2e           # acceptance run against a live stack (start it with make dev or make dev-local first)
make lint               # ruff
make typecheck          # mypy
make build-web          # production build of the web app
make clean              # remove caches and build output
```
`make test` needs no Docker. `make test-e2e` uploads 50 files per user with vendor faults on and checks that every
file reaches a terminal state, the vendor never sees more than 20 concurrent calls or a duplicate record, and a
retried `POST /batches` is idempotent. Point it elsewhere with `API_URL`, `VENDOR_URL` and `E2E_USERS`.
`make lint` and `make typecheck` currently report existing issues and are not part of `make test`.

## Layout
| Path | Contents |
|---|---|
| `pipeline/` | API, Celery tasks, dispatcher, resilience (semaphore, breaker, fair queue), errors, extractors, DB |
| `mock_vendor/` | Rate-limited vendor with configurable latency and faults |
| `alembic/` | Database migrations |
| `web/` | React + TypeScript client |
| `tests/` | `unit/`, `integration/` (in-process) and `e2e/` (live stack) |
| `scripts/` | `dev_nodocker.sh` Docker-free launcher, `fake_infra.py` Redis/S3 stand-ins |
