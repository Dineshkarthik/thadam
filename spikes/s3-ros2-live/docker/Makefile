# Spike S3: Jazzy live-transport stack helpers (compose flavor).
# docker/run.sh is the sandbox-friendly plain-docker equivalent.
#
#   make build         build the s3-ros2-zenoh image
#   make up / down     step 1: rmw_zenoh (router + sensor-pub)
#   make up-dds        step 2: zenoh-bridge-ros2dds + Fast DDS publishers
#   make down          stop everything (all profiles)
#   make logs          tail container logs (S=<service>)
#   make versions      RMW package versions baked into the image
#   make secrets/ca.pem  opt-in: host CA bundle for TLS-intercepting proxies

COMPOSE := docker compose -f compose.yaml

.PHONY: build up up-dds down logs clean versions

build: ## Build the Jazzy image (rmw_zenoh + Fast DDS + publishers)
	@test -f secrets/ca.pem || echo "hint: on TLS-intercepting networks, run: make secrets/ca.pem"
	$(COMPOSE) build

up: ## Start step 1: rmw_zenohd + rmw_zenoh publishers
	$(COMPOSE) up -d

up-dds: ## Start step 2: zenoh-bridge-ros2dds + Fast DDS publishers
	$(COMPOSE) --profile fastdds up -d bridge sensor-pub-dds

down: ## Stop everything (both profiles)
	$(COMPOSE) --profile fastdds down

logs: ## Tail logs: make logs (or S=sensor-pub-dds)
	$(COMPOSE) logs -f --tail=100 $(S)

clean: ## down -v
	$(COMPOSE) --profile fastdds down -v

versions: ## Show the RMW package versions baked into the image
	@docker run --rm --entrypoint cat s3-ros2-zenoh:jazzy-1 /usr/local/lib/s3/versions.txt

secrets/ca.pem: ## Optional: CA bundle so apt in the build survives TLS interception
	@mkdir -p secrets
	@if [ -f /etc/ssl/cert.pem ]; then cp /etc/ssl/cert.pem $@ && echo "copied host CA bundle to $@"; else echo "no /etc/ssl/cert.pem; place your proxy CA chain at $@"; fi
