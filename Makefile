.PHONY: help install install-dev lint format test test-cov clean \
        docker-up docker-down docker-logs docker-restart \
        airflow-logs grafana-logs deploy

# Default target
help:
	@echo "Bidata Velib ETL Pipeline - Available commands:"
	@echo ""
	@echo "Development:"
	@echo "  make install       Install production dependencies"
	@echo "  make install-dev   Install development dependencies"
	@echo "  make lint          Run linter (ruff)"
	@echo "  make format        Format code with black"
	@echo "  make test          Run tests"
	@echo "  make test-cov      Run tests with coverage report"
	@echo "  make clean         Remove build artifacts"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-up     Start all services"
	@echo "  make docker-down   Stop all services"
	@echo "  make docker-restart Restart all services"
	@echo "  make docker-logs   View all service logs"
	@echo "  make airflow-logs  View Airflow logs"
	@echo "  make grafana-logs  View Grafana logs"
	@echo ""
	@echo "Deployment:"
	@echo "  make deploy        Deploy to target environment"
	@echo ""

# ============================================
# Development
# ============================================

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements-dev.txt
	pre-commit install || true

lint:
	ruff check src/ airflow/dags/ tests/
	black --check src/ airflow/dags/ tests/

format:
	ruff check --fix src/ airflow/dags/ tests/
	black src/ airflow/dags/ tests/

test:
	PYTHONPATH=src:airflow/dags pytest tests/ -v

test-cov:
	PYTHONPATH=src:airflow/dags pytest tests/ \
		--cov=src \
		--cov=airflow/dags \
		--cov-report=html \
		--cov-report=term-missing

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "coverage.xml" -delete
	find . -type f -name ".coverage" -delete

# ============================================
# Docker
# ============================================

docker-up:
	docker compose up -d
	@echo ""
	@echo "Services starting..."
	@echo "  Airflow UI:  http://localhost:8080 (airflow/airflow)"
	@echo "  Grafana:     http://localhost:3000 (admin/admin)"
	@echo ""

docker-down:
	docker compose down

docker-restart:
	docker compose down
	docker compose up -d

docker-logs:
	docker compose logs -f

airflow-logs:
	docker compose logs -f airflow-webserver airflow-scheduler airflow-worker

grafana-logs:
	docker compose logs -f grafana loki promtail

docker-status:
	docker compose ps

docker-clean:
	docker compose down -v --remove-orphans
	docker system prune -f

# ============================================
# Database
# ============================================

db-shell-postgres:
	docker compose exec postgres psql -U airflow -d airflow

db-shell-mongo:
	docker compose exec mongodb mongosh -u mongo -p mongo

# ============================================
# Airflow
# ============================================

airflow-shell:
	docker compose exec airflow-webserver bash

dag-test:
	docker compose exec airflow-webserver airflow dags test velib_etl

dag-list:
	docker compose exec airflow-webserver airflow dags list

# ============================================
# Deployment
# ============================================

deploy:
	@echo "Running deployment..."
	./deploy/deploy.sh
