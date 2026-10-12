# Spike S3 step 1: Jazzy + rmw_zenoh stack helpers.
#
#   make build        build the s3-ros2-zenoh image
#   make up / down    start / stop router + sensor-pub
#   make logs         tail container logs
#   make clean        down -v (no named volumes today, but symmetric)
#   make secrets/ca.pem  opt-in: host CA bundle for TLS-intercepting proxies

COMPOSE := docker compose -f compose.yaml

.PHONY: build up down logs clean versions

build: ## Build the Jazzy+rmw_zenoh image
	@test -f secrets/ca.pem || echo "hint: on TLS-intercepting networks, run: make secrets/ca.pem"
	$(COMPOSE) build

up: ## Start router + sensor-pub
	$(COMPOSE) up -d

down: ## Stop everything
	$(COMPOSE) down

logs: ## Tail logs: make logs (or S=sensor-pub)
	$(COMPOSE) logs -f --tail=100 $(S)

clean: ## down -v
	$(COMPOSE) down -v

versions: ## Show the rmw_zenoh package version baked into the image
	@docker run --rm --entrypoint cat s3-ros2-zenoh:jazzy-1 /usr/local/lib/s3/versions.txt

secrets/ca.pem: ## Optional: CA bundle so apt in the build survives TLS interception
	@mkdir -p secrets
	@if [ -f /etc/ssl/cert.pem ]; then cp /etc/ssl/cert.pem $@ && echo "copied host CA bundle to $@"; else echo "no /etc/ssl/cert.pem; place your proxy CA chain at $@"; fi
