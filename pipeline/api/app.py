from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pipeline.api import batches, dlq, events, files, s3_events, system, tasks
from pipeline.config import settings
from pipeline.log import configure_logging


def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(title="File Pipeline API", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["Retry-After"],
    )
    for module in (batches, files, tasks, events, dlq, s3_events, system):
        app.include_router(module.router)
    return app


app = create_app()
