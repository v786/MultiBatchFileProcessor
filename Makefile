COMPOSE ?= $(shell if command -v docker-compose >/dev/null 2>&1; then echo docker-compose; else echo docker compose; fi)
PYTHON ?= $(shell command -v python3.13 || command -v python3.12 || command -v python3)
VENV ?= .venv
PY := $(VENV)/bin/python
NPM ?= npm
API_URL ?= http://localhost:8000
VENDOR_URL ?= http://localhost:4000
E2E_USERS ?= 3
PYTEST_ARGS ?=

.DEFAULT_GOAL := help

.PHONY: help check-compose check-docker dev dev-local down logs \
	install install-py install-web \
	test test-unit test-integration test-e2e test-web lint typecheck \
	build-web clean

help:
	@echo "Stack"
	@echo "  make dev               start the full stack with Docker Compose"
	@echo "  make dev-local         start the stack without Docker (fakeredis + moto + SQLite)"
	@echo "  make down | logs       stop the stack / follow its logs"
	@echo "Setup"
	@echo "  make install           create $(VENV), install Python dev deps and web deps"
	@echo "Tests (no Docker needed unless noted)"
	@echo "  make test              unit + integration tests, then web typecheck"
	@echo "  make test-unit         Python unit tests"
	@echo "  make test-integration  in-process pipeline tests (SQLite, fakeredis, moto, mock vendor)"
	@echo "  make test-web          TypeScript typecheck of the web app"
	@echo "  make test-e2e          50-file acceptance run against a live stack (make dev first)"
	@echo "  make lint | typecheck  ruff / mypy (report existing issues; not part of make test)"

# --- stack ---------------------------------------------------------------------------------------------------

check-compose:
	@$(COMPOSE) version >/dev/null 2>&1 || { \
		echo "Docker Compose is required. Install Docker Compose v2 (docker compose) or v1 (docker-compose)."; \
		exit 1; \
	}

check-docker:
	@docker info >/dev/null 2>&1 || { \
		echo "Docker Engine is not running. Start Docker Desktop or your Docker runtime, then retry."; \
		exit 1; \
	}

dev: check-compose check-docker
	$(COMPOSE) up --build

dev-local: $(VENV)/.installed
	PY=$(PY) scripts/dev_nodocker.sh

down: check-compose check-docker
	$(COMPOSE) down

logs: check-compose check-docker
	$(COMPOSE) logs --follow

# --- setup ---------------------------------------------------------------------------------------------------

install: install-py install-web

install-py: $(VENV)/.installed

# Re-installs only when pyproject.toml changes.
$(VENV)/.installed: pyproject.toml
	@test -x $(PY) || $(PYTHON) -m venv $(VENV)
	$(PY) -m pip install --quiet --upgrade pip
	$(PY) -m pip install --quiet -e ".[dev]"
	@touch $@

install-web: web/node_modules/.installed

web/node_modules/.installed: web/package.json web/package-lock.json
	cd web && $(NPM) ci --no-audit --no-fund
	@touch $@

# --- tests ---------------------------------------------------------------------------------------------------

test: test-unit test-integration test-web

test-unit: install-py
	$(PY) -m pytest tests/unit $(PYTEST_ARGS)

test-integration: install-py
	$(PY) -m pytest tests/integration $(PYTEST_ARGS)

test-web: install-web
	cd web && $(NPM) run typecheck

test-e2e: install-py
	@$(PY) -c "import httpx, sys; httpx.get('$(API_URL)/system/status', headers={'Authorization': 'Bearer e2e'}, timeout=5)" 2>/dev/null || { \
		echo "API not reachable at $(API_URL). Start the stack with 'make dev' (or 'make dev-local') first."; \
		exit 1; \
	}
	$(PY) tests/e2e/acceptance.py --api $(API_URL) --vendor $(VENDOR_URL) --users $(E2E_USERS)

lint: install-py
	$(PY) -m ruff check pipeline mock_vendor tests scripts

typecheck: install-py
	$(PY) -m mypy pipeline mock_vendor

build-web: install-web
	cd web && $(NPM) run build

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache web/dist web/tsconfig.tsbuildinfo
	find . -name __pycache__ -type d -not -path './$(VENV)/*' -not -path './web/node_modules/*' -prune -exec rm -rf {} +
