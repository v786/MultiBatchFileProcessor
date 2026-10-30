import os

if os.environ.get("PIPELINE_GEVENT") == "1":  # deliver worker: make psycopg2 cooperative under gevent
    from psycogreen.gevent import patch_psycopg

    patch_psycopg()

from celery import Celery  # noqa: E402

from pipeline.config import settings  # noqa: E402
from pipeline.log import configure_logging  # noqa: E402

configure_logging()

app = Celery("pipeline", broker=settings.redis_url, include=["pipeline.tasks"])
app.conf.update(
    task_acks_late=True,                  # ack after the task finishes, not on receipt
    task_reject_on_worker_lost=True,      # crashed worker -> message returns to the queue
    worker_prefetch_multiplier=1,         # a worker holds only what it is running
    broker_transport_options={"visibility_timeout": 3600},  # longer than any task
    task_soft_time_limit=90,              # SoftTimeLimitExceeded inside the task
    task_time_limit=120,                  # hard kill
    worker_max_memory_per_child=512_000,  # KB; recycle a bloated prefork child
    task_ignore_result=True,              # state lives in Postgres, not in Celery results
    worker_hijack_root_logger=False,
    task_routes={
        "pipeline.tasks.extract_file": {"queue": "extract"},
        "pipeline.tasks.deliver_file": {"queue": "deliver"},
        "pipeline.tasks.send_notification": {"queue": "notify"},
        "pipeline.tasks.relay_outbox": {"queue": "housekeeping"},
        "pipeline.tasks.sweep": {"queue": "housekeeping"},
        "pipeline.tasks.noop": {"queue": "housekeeping"},
    },
    beat_schedule={
        "relay-outbox": {"task": "pipeline.tasks.relay_outbox", "schedule": 1.0, "options": {"expires": 5}},
        "sweep": {"task": "pipeline.tasks.sweep", "schedule": 60.0, "options": {"expires": 50}},
    },
)
