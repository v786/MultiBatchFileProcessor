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

Run the 50-file acceptance test against the live stack:
```sh
python tests/e2e/acceptance.py
```

## Tests (no Docker needed)
    python -m venv .venv && .venv/bin/pip install -e ".[dev]"
    .venv/bin/python -m pytest       # unit + in-process integration (SQLite, fakeredis, moto, mock vendor)

## Layout
`pipeline/` (api, tasks, dispatcher, resilience, errors, extract, db), `mock_vendor/`, `alembic/`, `web/` (React + TS),
`tests/`, `scripts/` (`dev_nodocker.sh` is an experimental Docker-free launcher; see caveat in it).
