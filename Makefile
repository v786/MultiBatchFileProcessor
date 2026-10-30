COMPOSE ?= $(shell if command -v docker-compose >/dev/null 2>&1; then echo docker-compose; else echo docker compose; fi)

.PHONY: check-compose check-docker dev down logs

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

down: check-compose check-docker
	$(COMPOSE) down

logs: check-compose check-docker
	$(COMPOSE) logs --follow
