#!/usr/bin/env bash
# Run the whole pipeline as separate local processes without Docker:
# fakeredis (TCP) + moto S3 + SQLite stand in for Redis, MinIO and Postgres.
#   scripts/dev_nodocker.sh            # start everything; Ctrl-C stops it
#   MOCK_LATENCY_SCALE=1 scripts/dev_nodocker.sh   # real 10-20 s vendor replies
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}
RUN_DIR=${RUN_DIR:-.run}
mkdir -p "$RUN_DIR/logs"
RUN_DIR=$(cd "$RUN_DIR" && pwd)
rm -f "$RUN_DIR/pipeline.db"

export DATABASE_URL="sqlite:///$RUN_DIR/pipeline.db"
export REDIS_URL="redis://127.0.0.1:6390/0"
export S3_ENDPOINT_URL="http://127.0.0.1:9010" S3_PUBLIC_ENDPOINT_URL="http://127.0.0.1:9010"
export S3_ACCESS_KEY=test S3_SECRET_KEY=test
export VENDOR_URL="http://127.0.0.1:4000"
export MOCK_LATENCY_SCALE=${MOCK_LATENCY_SCALE:-0.1}
export CORS_ORIGINS='["http://localhost:5173","http://127.0.0.1:5173"]'

pids=()
start() { local name=$1; shift; "$@" >"$RUN_DIR/logs/$name.log" 2>&1 & pids+=($!); echo "started $name (pid $!)"; }
trap 'kill "${pids[@]}" 2>/dev/null; wait 2>/dev/null' EXIT INT TERM

start infra "$PY" scripts/fake_infra.py --redis-port 6390 --s3-port 9010
sleep 2
"$PY" -m alembic upgrade head >"$RUN_DIR/logs/migrate.log" 2>&1
"$PY" -c "from pipeline import storage; storage.ensure_bucket()"
start vendor "$PY" -m uvicorn mock_vendor.app:app --port 4000
start api "$PY" -m uvicorn pipeline.api.app:app --port 8000
# threads, not prefork/gevent: billiard prefork breaks under macOS/Python 3.14's spawn start method.
# docker-compose runs the real pools (prefork for extract/notify, gevent for deliver) on Linux.
start extract "$PY" -m celery -A pipeline.celery_app worker -Q extract -P threads -c 4 -n extract@local
start deliver "$PY" -m celery -A pipeline.celery_app worker -Q deliver -P threads -c 50 -n deliver@local
start notify "$PY" -m celery -A pipeline.celery_app worker -Q notify,housekeeping -P threads -c 2 -n notify@local
start beat "$PY" -m celery -A pipeline.celery_app beat --schedule "$RUN_DIR/celerybeat-schedule"
start dispatcher "$PY" -m pipeline.dispatcher
echo "API http://127.0.0.1:8000  vendor http://127.0.0.1:4000  logs in $RUN_DIR/logs"
echo "Web client: cd web && VITE_API_URL=http://127.0.0.1:8000 npm run dev"
wait
