# =============================================================================
# COMEXT Pipeline — Makefile
# Wraps common Docker Compose and uv commands for convenience.
# =============================================================================

.DEFAULT_GOAL := help
COMPOSE_PROD := docker compose -f docker-compose.yml

# ── Help ───────────────────────────────────────────────────────────────────────
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ── Dev environment ────────────────────────────────────────────────────────────
.PHONY: dev
dev: ## Build and start dev environment (foreground, auto-merges override)
	docker compose up --build

.PHONY: dev-d
dev-d: ## Build and start dev environment in background
	docker compose up --build -d

.PHONY: start
start: ## Start existing dev containers without rebuilding
	docker compose start

.PHONY: stop
stop: ## Stop dev containers without removing them
	docker compose stop

.PHONY: restart
restart: ## Restart dev containers without rebuilding
	docker compose restart

.PHONY: down
down: ## Stop and remove dev containers
	docker compose down

.PHONY: logs
logs: ## Tail dev logs
	docker compose logs -f dagster

.PHONY: ps
ps: ## Show status of all containers
	docker compose ps

# ── Production environment ─────────────────────────────────────────────────────
.PHONY: prod
prod: ## Build and start prod environment (target: prod, no override)
	$(COMPOSE_PROD) up --build -d

.PHONY: prod-down
prod-down: ## Stop and remove prod containers
	$(COMPOSE_PROD) down

.PHONY: prod-logs
prod-logs: ## Tail prod logs
	$(COMPOSE_PROD) logs -f dagster

# ── Testing ────────────────────────────────────────────────────────────────────
.PHONY: test
test: ## Run tests inside the dev container
	docker compose run --rm dagster pytest

.PHONY: test-cov
test-cov: ## Run tests with coverage report
	docker compose run --rm dagster pytest --cov=comext_pipeline --cov-report=term-missing

# ── Linting / type-checking ────────────────────────────────────────────────────
.PHONY: lint
lint: ## Run ruff linter
	docker compose run --rm dagster ruff check comext_pipeline

.PHONY: fmt
fmt: ## Run ruff formatter
	docker compose run --rm dagster ruff format comext_pipeline

.PHONY: typecheck
typecheck: ## Run mypy type checker
	docker compose run --rm dagster mypy comext_pipeline

# ── Shell access ───────────────────────────────────────────────────────────────
.PHONY: shell
shell: ## Open a shell in the dev container
	docker compose run --rm dagster bash

# ── Backfill helpers ───────────────────────────────────────────────────────────
.PHONY: backfill-all
backfill-all: ## Run a full historical backfill (dev container)
	docker compose run --rm dagster python scripts/backfill.py --all

# ── Dependency management ──────────────────────────────────────────────────────
.PHONY: lock
lock: ## Regenerate uv.lock from pyproject.toml
	uv lock

.PHONY: lock-upgrade
lock-upgrade: ## Upgrade all dependencies and regenerate lockfile
	uv lock --upgrade

# ── Housekeeping ───────────────────────────────────────────────────────────────
.PHONY: clean
clean: ## Remove stopped containers and dangling images
	docker compose down --remove-orphans
	docker image prune -f

.PHONY: clean-volumes
clean-volumes: ## !! Deletes all data volumes (irreversible)
	docker compose down -v
	$(COMPOSE_PROD) down -v