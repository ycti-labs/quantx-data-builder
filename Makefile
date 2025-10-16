.PHONY: help install install-dev test test-cov lint format type-check clean docker-build docker-run func-start

# Default target
help: ## Show this help message
	@echo "QuantX Data Builder - Development Commands"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

# Installation targets
install: ## Install production dependencies
	pip install -r requirements.txt
	cd azure_functions && pip install -r requirements.txt
	cd container && pip install -r requirements.txt

install-dev: ## Install development dependencies
	pip install -r requirements.txt
	pip install pytest pytest-cov pytest-mock mypy black flake8 isort ipython

setup: ## Setup project directories
	mkdir -p data/us data/hk data/jp data/eu
	mkdir -p meta
	mkdir -p logs
	@if [ ! -f .env ]; then cp .env.example .env; echo ".env file created from template"; fi

# Testing targets
test: ## Run all tests
	pytest tests/ -v

test-cov: ## Run tests with coverage report
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html
	@echo "Coverage report generated in htmlcov/index.html"

test-watch: ## Run tests in watch mode (requires pytest-watch)
	ptw -- tests/ -v

test-specific: ## Run specific test file (usage: make test-specific FILE=test_hkex_universe.py)
	pytest tests/$(FILE) -v

# Code quality targets
lint: ## Run linting checks
	@echo "Running flake8..."
	flake8 src/ azure_functions/ container/ tests/ --max-line-length=100 --ignore=E203,W503
	@echo "✓ Linting passed"

format: ## Format code with black and isort
	@echo "Running black..."
	black src/ azure_functions/ container/ tests/ --line-length=100
	@echo "Running isort..."
	isort src/ azure_functions/ container/ tests/ --profile black
	@echo "✓ Code formatted"

format-check: ## Check code formatting without modifying files
	@echo "Checking black formatting..."
	black src/ azure_functions/ container/ tests/ --check --line-length=100
	@echo "Checking isort formatting..."
	isort src/ azure_functions/ container/ tests/ --check-only --profile black

type-check: ## Run type checking with mypy
	@echo "Running mypy..."
	mypy src/ --ignore-missing-imports --follow-imports=silent
	@echo "✓ Type checking passed"

check-all: format-check lint type-check ## Run all code quality checks

# Azure Functions targets
func-start: ## Start Azure Functions locally
	cd azure_functions && func start

func-install: ## Install Azure Functions Core Tools dependencies
	cd azure_functions && pip install -r requirements.txt

# Container targets
docker-build: ## Build Docker container image
	docker build -f container/Dockerfile -t quantx-data-builder:latest .

docker-run: ## Run Docker container locally (test)
	docker run --rm quantx-data-builder:latest python --version

docker-shell: ## Open shell in Docker container
	docker run --rm -it quantx-data-builder:latest /bin/bash

docker-clean: ## Remove Docker images
	docker rmi quantx-data-builder:latest || true

# CLI targets
cli-help: ## Show CLI help
	cd container && python cli.py --help

cli-backfill: ## Run backfill command (example)
	cd container && python cli.py backfill --help

cli-update: ## Run daily update command (example)
	cd container && python cli.py update-daily --help

# Azure deployment targets
deploy-functions: ## Deploy Azure Functions
	cd azure_functions && func azure functionapp publish $(FUNCTION_APP_NAME)

deploy-container: ## Deploy container to Azure Container Apps
	./deploy/deploy_hybrid.sh

# Cleanup targets
clean: ## Clean generated files and caches
	@echo "Cleaning Python cache files..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf htmlcov/ .coverage 2>/dev/null || true
	@echo "✓ Cleanup complete"

clean-data: ## Clean data directories (use with caution!)
	@echo "WARNING: This will delete all data in data/ and meta/"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		rm -rf data/* meta/*.csv meta/*.parquet logs/*; \
		echo "✓ Data cleaned"; \
	else \
		echo "Cancelled"; \
	fi

# Development workflow
dev-setup: install-dev setup ## Complete development setup
	@echo "✓ Development environment ready!"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Configure .env file with your Azure credentials"
	@echo "  2. Run: make test"
	@echo "  3. Run: make func-start (to test Azure Functions)"

dev-check: clean format test lint type-check ## Run full development check (format, test, lint, type-check)
	@echo "✓ All checks passed! Ready to commit."

# CI/CD simulation
ci: clean format-check lint type-check test ## Simulate CI pipeline
	@echo "✓ CI checks passed!"

# Version info
version: ## Show version information
	@echo "Python: $$(python --version)"
	@echo "pip: $$(pip --version)"
	@echo "pytest: $$(pytest --version)"
	@echo "black: $$(black --version)"
	@echo "mypy: $$(mypy --version)"
	@echo "Azure CLI: $$(az --version | head -n 1)"
	@echo "Azure Functions Core Tools: $$(func --version)"
