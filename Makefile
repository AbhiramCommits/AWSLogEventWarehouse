SHELL := /bin/bash
PYTHON ?= python3.11
VENV := .venv
BIN := $(VENV)/bin
export PATH := /Applications/Docker.app/Contents/Resources/bin:$(HOME)/.local/bin:$(PATH)

# Spark needs a JDK; on macOS pick up the default from java_home when present.
JAVA_HOME ?= $(shell /usr/libexec/java_home 2>/dev/null)
export JAVA_HOME

export AWS_ENDPOINT_URL ?= http://localhost:4566
export AWS_REGION ?= us-east-1
export AWS_DEFAULT_REGION ?= us-east-1
export AWS_ACCESS_KEY_ID ?= test
export AWS_SECRET_ACCESS_KEY ?= test
export RAW_BUCKET ?= logwarehouse-raw
export CURATED_BUCKET ?= logwarehouse-curated
export STREAM_NAME ?= log-events-stream
export FIREHOSE_NAME ?= log-events-firehose
export TF_ENV := AWS_ENDPOINT_URL=$(AWS_ENDPOINT_URL)

.PHONY: help venv install up down wait tf-init tf-apply tf-destroy tf-fmt lint fmt test \
        produce transform catalog warehouse bench monitor dashboard pipeline reset clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

$(BIN)/python:
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip wheel

venv: $(BIN)/python ## Create virtualenv

install: venv ## Install the package with dev extras
	$(BIN)/pip install -e ".[dev]"

up: ## Start LocalStack via docker compose
	docker compose up -d
	$(MAKE) wait

wait: ## Wait for LocalStack health
	$(BIN)/python scripts/wait_for_localstack.py

down: ## Stop LocalStack (keeps the volume)
	docker compose down

tf-init: ## terraform init in infra/terraform
	cd infra/terraform && $(TF_ENV) terraform init -input=false

tf-apply: ## terraform apply against LocalStack
	cd infra/terraform && $(TF_ENV) terraform apply -auto-approve -input=false

tf-destroy: ## terraform destroy against LocalStack
	cd infra/terraform && $(TF_ENV) terraform destroy -auto-approve -input=false

tf-fmt: ## Check terraform formatting
	cd infra/terraform && terraform fmt -check -recursive

lint: ## Run ruff
	$(BIN)/ruff check src tests scripts

fmt: ## Auto-fix ruff issues
	$(BIN)/ruff check --fix src tests scripts

test: ## Run the test suite with coverage
	$(BIN)/pytest --cov=logwarehouse --cov=producers --cov=glue --cov=warehouse \
		--cov=monitoring --cov-report=term-missing --cov-report=xml:artifacts/coverage.xml

produce: ## Generate and publish events, deliver to the raw zone
	$(BIN)/python -m producers.publish --source app_log
	$(BIN)/python -m producers.publish --source clickstream
	$(BIN)/python -m producers.consumer --deliver-all

transform: ## Run the PySpark Glue transform for both sources
	$(BIN)/python -m glue.run --all

catalog: ## Register curated tables/partitions in the Glue Catalog
	$(BIN)/python -m glue.catalog

warehouse: ## Build the DuckDB warehouse from curated Parquet
	$(BIN)/python -m warehouse.load

bench: ## Run the before/after SQL tuning benchmark
	$(BIN)/python -m warehouse.benchmark

monitor: ## Publish metrics, create alarms, evaluate the data-quality gate
	$(BIN)/python -m monitoring.metrics
	$(BIN)/python -m monitoring.alarms
	$(BIN)/python -m monitoring.alarms --check

incident: ## Simulate a degraded window to prove the alarms fire
	$(BIN)/python -m monitoring.simulate_incident

restore: ## Clear the simulated incident window
	$(BIN)/python -m monitoring.simulate_incident --restore

dashboard: ## Serve the Streamlit dashboard
	$(BIN)/streamlit run src/dashboard/app.py --server.port 8501 --server.headless true

pipeline: ## Run the full end-to-end pipeline
	$(BIN)/python scripts/run_pipeline.py

reset: ## Wipe local S3/Kinesis/artifacts for a clean run
	$(BIN)/python scripts/reset_state.py

clean: ## Remove caches and build output
	rm -rf .pytest_cache .ruff_cache **/__pycache__ artifacts/coverage.xml
