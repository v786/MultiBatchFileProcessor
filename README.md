# Concurrent File Processing Pipeline

Implementation of `Concurrent File Processing Pipeline — Implementation Doc.md`: browser → presigned S3 upload,
Celery extraction (prefork), fair dispatcher + 20-slot Redis semaphore + circuit breaker → mock vendor (gevent),
Postgres outbox, task ids / events / SSE progress, DLQ + replay, completion notifications.

## Run
```sh
make dev      # start the full development stack
make down     # stop the stack
make logs     # follow logs from all services
```

Docker Compose v2 (`docker compose`) or v1 (`docker-compose`) must be installed.
The Docker Engine must also be running. The Makefile detects either Compose
command and checks the Engine before starting; set `COMPOSE` to override the
Compose command.

Open the web app at http://localhost:5173. The API is available at :8000,
MailHog at :8025, Flower at :5555, and the mock vendor at :4000.

`make dev-local` runs the same stack without Docker (fakeredis + moto + SQLite).

## Tests
```sh
make install            # .venv with dev deps + web node_modules (re-runs only when deps change)
make test               # unit + in-process integration (SQLite, fakeredis, moto, mock vendor) + web typecheck
make test-unit          # or test-integration / test-web individually; PYTEST_ARGS="-k name" to filter
make test-e2e           # 50-file acceptance run against a live stack (start it with make dev first)
make lint typecheck     # ruff / mypy
```
`make help` lists every target.

## Layout
`pipeline/` (api, tasks, dispatcher, resilience, errors, extract, db), `mock_vendor/`, `alembic/`, `web/` (React + TS),
`tests/`, `scripts/` (`dev_nodocker.sh` is an experimental Docker-free launcher; see caveat in it).
