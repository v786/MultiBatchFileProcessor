FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY pyproject.toml ./
COPY pipeline ./pipeline
COPY mock_vendor ./mock_vendor
RUN pip install ".[workers]"

COPY alembic.ini ./
COPY alembic ./alembic
COPY scripts ./scripts

RUN useradd --create-home app
USER app

CMD ["uvicorn", "pipeline.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
