# Compose command: prefer the v2 plugin ("docker compose"), fall back to the
# standalone binary ("docker-compose"). Override with: make dev-up DC="docker compose"
DC ?= $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")

.PHONY: help install lint test dev-up dev-down dev-logs localaws-up localaws-down localaws-logs localaws-clean migrate build push

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Install Python deps in editable mode
	pip install -e ".[dev,gateway]"

lint: ## Lint with ruff
	ruff check src tests

test: ## Run unit tests (skips integration tests without Postgres)
	pytest -q

dev-up: ## Start local stack (Postgres x2 + LocalStack + Dagster + Gateway)
	$(DC) -f docker-compose.dev.yml up --build -d

dev-down: ## Stop local stack
	$(DC) -f docker-compose.dev.yml down -v

dev-logs: ## Tail local stack logs
	$(DC) -f docker-compose.dev.yml logs -f --tail=100

localaws-up: ## Run locally against REAL AWS RDS + S3 (needs .env.localaws)
	$(DC) -f docker-compose.localaws.yml up --build -d

localaws-down: ## Stop the real-AWS stack (KEEPS metadata: catalog + watermarks)
	$(DC) -f docker-compose.localaws.yml down

localaws-logs: ## Tail the real-AWS stack logs
	$(DC) -f docker-compose.localaws.yml logs -f --tail=100

localaws-clean: ## Stop the real-AWS stack AND wipe local metadata (forces re-bootstrap)
	$(DC) -f docker-compose.localaws.yml down -v

migrate: ## Apply metadata DB migrations (idempotent)
	viamedia-migrate

build-extractor: ## Build extractor image
	docker build -f docker/Dockerfile.extractor -t viamedia-extractor:latest .

build-gateway: ## Build gateway image
	docker build -f docker/Dockerfile.gateway -t viamedia-gateway:latest .

build-dagster: ## Build dagster webserver image
	docker build -f docker/Dockerfile.dagster-webserver -t viamedia-dagster:latest .

push: build-extractor build-gateway build-dagster ## Tag + push all images to ECR
	@test -n "$$REGISTRY" || (echo "Set REGISTRY=<account>.dkr.ecr.<region>.amazonaws.com" && exit 1)
	@test -n "$$ENV"      || (echo "Set ENV=dev|stg|prod" && exit 1)
	docker tag viamedia-extractor:latest $$REGISTRY/viamedia-extractor:$$ENV
	docker tag viamedia-gateway:latest   $$REGISTRY/viamedia-gateway:$$ENV
	docker tag viamedia-dagster:latest   $$REGISTRY/viamedia-dagster:$$ENV
	docker push $$REGISTRY/viamedia-extractor:$$ENV
	docker push $$REGISTRY/viamedia-gateway:$$ENV
	docker push $$REGISTRY/viamedia-dagster:$$ENV

# Manual EC2 deploy (no Terraform) -- see DEPLOY.md.
