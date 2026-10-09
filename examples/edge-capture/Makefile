# Thadam edge-capture demo (PR15).
#
#   make up            broker + toxiproxy + simulator (level 1 support cast)
#   make run-native    thadam runs natively on thadam/pipeline.local.yaml
#   make up-s3         ...plus SeaweedFS + bucket init (level 2)
#   make run-native CONFIG=thadam/pipeline.s3.local.yaml
#   make query         DuckDB over the local Parquet segments
#   make down / clean  stop / stop + wipe volumes and ./data
#
# Chaos scenarios (F1-F10), the oracle verifier, and benchmarks arrive in
# PR18; their targets are intentional placeholders.

COMPOSE := docker compose -f compose.yaml
WORKSPACE := $(abspath ../..)
CONFIG ?= thadam/pipeline.local.yaml
ADMIN_BIND ?= 127.0.0.1:9464
# Pinned-by-digest DuckDB CLI image for `make query` on hosts without duckdb
# installed (Docker Hub only; digest recorded when pinned).
DUCKDB_IMAGE := duckdb/duckdb@sha256:69128eff5f937dcd451dd3342d0d2d2e24edcab5ad71e0042b1cc6602257be73

.DEFAULT_GOAL := help

.PHONY: help up up-s3 up-observe build run-native validate status query oracle-stats logs down clean verify chaos-%

help: ## List targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/' | column -ts "$$(printf '\t')"

up: ## Start broker, Toxiproxy, and the simulator (builds the simulator image on first run)
	$(COMPOSE) up -d mosquitto toxiproxy simulator

up-s3: secrets/admin_token secrets/s3_access_key secrets/s3_secret_key ## ...plus SeaweedFS and the bucket init (level 2)
	$(COMPOSE) --profile s3 up -d

up-observe: up-s3 ## ...plus Prometheus and the provisioned Grafana dashboard
	$(COMPOSE) --profile s3 --profile observe up -d

build: ## (Re)build the simulator image
	@mkdir -p secrets
	@test -f secrets/ca.pem || echo "hint: on TLS-intercepting networks, run: make secrets/ca.pem"
	$(COMPOSE) build simulator

secrets/admin_token secrets/s3_access_key secrets/s3_secret_key:
	@mkdir -p secrets
	@if [ ! -f $@ ]; then openssl rand -hex 24 > $@; echo "generated $@"; fi

secrets/ca.pem: ## Optional: CA bundle for TLS-intercepting proxies; used by the simulator build stage only
	@mkdir -p secrets
	@if [ -f /etc/ssl/cert.pem ]; then cp /etc/ssl/cert.pem $@ && echo "copied host CA bundle to $@"; else echo "no /etc/ssl/cert.pem; place your proxy CA chain at $@"; fi

run-native: secrets/admin_token secrets/s3_access_key secrets/s3_secret_key ## Run thadam natively on CONFIG (default thadam/pipeline.local.yaml)
	@export THADAM_DEMO_S3_AK="$$(cat secrets/s3_access_key)" THADAM_DEMO_S3_SK="$$(cat secrets/s3_secret_key)"; \
	cargo run --manifest-path "$(WORKSPACE)/Cargo.toml" -p thadam-cli -- \
	  run "./$(CONFIG)" --log-format pretty --admin-bind "$(ADMIN_BIND)"

validate: ## thadam validate both native configs (level 2 with --allow-unresolved)
	@cargo run --quiet --manifest-path "$(WORKSPACE)/Cargo.toml" -p thadam-cli -- \
	  validate "$(CURDIR)/thadam/pipeline.local.yaml"
	@cargo run --quiet --manifest-path "$(WORKSPACE)/Cargo.toml" -p thadam-cli -- \
	  validate --allow-unresolved "$(CURDIR)/thadam/pipeline.s3.local.yaml"

status: ## readyz and /status from the natively running thadam
	@curl -sf "http://$(ADMIN_BIND)/readyz" && echo " (readyz)"
	@curl -sf "http://$(ADMIN_BIND)/status" | jq .

query: ## sql/explore.sql with DuckDB (local CLI, else the pinned container)
	@if command -v duckdb >/dev/null 2>&1; then \
	  duckdb -c ".read sql/explore.sql"; \
	else \
	  echo "duckdb CLI not found (brew install duckdb to skip the container); using $(DUCKDB_IMAGE)"; \
	  docker run --rm --entrypoint duckdb -v "$(CURDIR):/work" -w /work $(DUCKDB_IMAGE) -c ".read sql/explore.sql"; \
	fi

oracle-stats: ## Aggregates of the simulator oracle (run inside a simulator container)
	$(COMPOSE) run --rm --no-deps simulator oracle-stats /oracle/oracle.ndjson

logs: ## Tail service logs: make logs S=simulator
	$(COMPOSE) logs -f --tail=100 $(S)

down: ## Stop everything (all profiles)
	$(COMPOSE) --profile s3 --profile observe down

clean: ## down -v plus ./data (a full demo reset)
	$(COMPOSE) --profile s3 --profile observe down -v
	rm -rf ./data

verify: ## Placeholder: the oracle verifier ships in PR18
	@echo "verify: added in PR18"

chaos-%: ## Placeholder: chaos scenarios F1-F10 ship in PR18
	@echo "chaos $*: added in PR18"
