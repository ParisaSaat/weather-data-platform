# Weather Data Platform: developer entry points. Run `make help`.
UV      ?= uv
WEATHER := $(UV) run weather

.DEFAULT_GOAL := help
.PHONY: help install ingest transform narrate narrate-offline validate report run run-offline \
        test test-unit lint format typecheck check docs airflow-up clean

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Create the virtualenv and install locked dependencies (+ dbt packages)
	$(UV) sync --locked
	cd dbt && $(UV) run dbt deps --profiles-dir .

ingest: ## Download NOAA metadata + configured stations into raw.*
	$(WEATHER) ingest

transform: ## dbt build: staging -> intermediate -> marts, with all tests
	$(WEATHER) transform

narrate: ## Generate narratives with Gemini (needs GEMINI_API_KEY in .env)
	$(WEATHER) narrate

narrate-offline: ## Generate deterministic template narratives (no API key needed)
	$(WEATHER) narrate --provider fake

validate: ## Validate narratives against source observations
	$(WEATHER) validate

report: ## Print data-quality + narrative summary
	$(WEATHER) report

run: ## Whole pipeline end to end with Gemini
	$(WEATHER) run

run-offline: ## Whole pipeline end to end without an API key
	$(WEATHER) run --provider fake

test: ## All tests (unit + offline end-to-end)
	$(UV) run pytest

test-unit: ## Fast unit tests only
	$(UV) run pytest -m "not integration"

lint: ## Ruff lint + format check
	$(UV) run ruff check src tests orchestration
	$(UV) run ruff format --check src tests orchestration

format: ## Auto-format and fix lint
	$(UV) run ruff format src tests orchestration
	$(UV) run ruff check --fix src tests orchestration

typecheck: ## mypy --strict on src/
	$(UV) run mypy

check: lint typecheck test ## Everything CI runs

docs: ## Generate and serve dbt docs (lineage graph) on http://localhost:8081
	cd dbt && WEATHER_DUCKDB_PATH=$(CURDIR)/data/warehouse/weather.duckdb $(UV) run dbt docs generate --profiles-dir . && \
	WEATHER_DUCKDB_PATH=$(CURDIR)/data/warehouse/weather.duckdb $(UV) run dbt docs serve --profiles-dir . --port 8081

airflow-up: ## Run the DAG in local Airflow (Docker)
	cd orchestration/airflow && docker compose up

clean: ## Remove the warehouse, landed files and dbt artifacts
	rm -rf data dbt/target dbt/logs logs
