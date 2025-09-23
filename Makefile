.PHONY: help install test test-unit test-integration test-e2e lint format typecheck clean start-localstack stop-localstack

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install dependencies
	uv pip install -e ".[dev]"

test: test-unit test-integration test-e2e ## Run all tests

test-unit: ## Run unit tests only
	uv run pytest tests/unit -v

test-integration: ## Run integration tests only
	uv run pytest tests/integration -v

test-e2e: start-localstack ## Run e2e tests (starts LocalStack)
	@echo "Running E2E tests..."
	@export AWS_ACCESS_KEY_ID=test && \
	export AWS_SECRET_ACCESS_KEY=test && \
	export AWS_DEFAULT_REGION=us-east-1 && \
	export AWS_ENDPOINT_URL=http://localhost:4566 && \
	uv run pytest tests/e2e -v --tb=short; \
	exit_code=$$?; \
	$(MAKE) stop-localstack; \
	exit $$exit_code

start-localstack: ## Start LocalStack for e2e testing
	@echo "Starting LocalStack..."
	@docker run -d \
		--name deltaglider-localstack \
		-p 4566:4566 \
		-e SERVICES=s3 \
		-e DEBUG=0 \
		-e DATA_DIR=/tmp/localstack/data \
		localstack/localstack:latest || true
	@echo "Waiting for LocalStack to be ready..."
	@max_attempts=30; \
	attempt=0; \
	while [ $$attempt -lt $$max_attempts ]; do \
		if curl -s -f http://localhost:4566/_localstack/health > /dev/null 2>&1; then \
			echo "LocalStack is ready!"; \
			break; \
		fi; \
		echo "Waiting... (attempt $$((attempt + 1))/$$max_attempts)"; \
		sleep 2; \
		attempt=$$((attempt + 1)); \
	done; \
	if [ $$attempt -eq $$max_attempts ]; then \
		echo "LocalStack failed to start"; \
		docker logs deltaglider-localstack; \
		docker rm -f deltaglider-localstack; \
		exit 1; \
	fi

stop-localstack: ## Stop LocalStack
	@echo "Stopping LocalStack..."
	@docker rm -f deltaglider-localstack || true

lint: ## Run linting
	uv run ruff check src tests

format: ## Format code
	uv run ruff format src tests

typecheck: ## Run type checking
	uv run mypy src

clean: ## Clean up
	rm -rf .pytest_cache
	rm -rf __pycache__
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -delete