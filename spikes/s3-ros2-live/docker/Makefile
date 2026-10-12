# Spike S3: live-transport stack helpers (compose flavor).
# docker/run.sh is the sandbox-friendly plain-docker equivalent and covers
# every setup (including the step-3 cross-distro checks compose cannot).
#
#   make build           build the Jazzy image (step 1/2)
#   make build-lyrical   build the Lyrical image (step 3)
#   make build-cyclone   build the Jazzy+Cyclone image (step 3 best effort)
#   make build-humble    build the Humble image (step 3 best effort)
#   make up / down       step 1: Jazzy rmw_zenoh (router + sensor-pub)
#   make up-dds          step 2: bridge + Jazzy Fast DDS publishers
#   make up-lyrical      step 3: Lyrical rmw_zenoh (router + sensor-pub)
#   make up-lyrical-dds  step 3: bridge + Lyrical Fast DDS publishers
#   make up-cyclone      step 3: bridge + Jazzy Cyclone DDS publishers
#   make up-humble       step 3: bridge + Humble Fast DDS publishers
#   make down            stop everything (all profiles)
#   make logs            tail container logs (S=<service>)
#   make versions        RMW package versions baked into the images
#   make secrets/ca.pem  opt-in: host CA bundle for TLS-intercepting proxies

COMPOSE := docker compose -f compose.yaml

.PHONY: build build-lyrical build-cyclone build-humble up up-dds up-lyrical \
        up-lyrical-dds up-cyclone up-humble down logs clean versions

build: ## Build the Jazzy image (rmw_zenoh + Fast DDS + publishers)
	@test -f secrets/ca.pem || echo "hint: on TLS-intercepting networks, run: make secrets/ca.pem"
	$(COMPOSE) build

build-lyrical: ## Build the Lyrical image (rmw_zenoh + Fast DDS 3 + publishers)
	docker build -f Dockerfile.lyrical -t s3-ros2-zenoh:lyrical-1 ..

build-cyclone: ## Build the Jazzy + Cyclone DDS image (best effort)
	docker build -f Dockerfile.cyclone -t s3-ros2-zenoh:jazzy-cyclone-1 ..

build-humble: ## Build the Humble image (best effort)
	docker build -f Dockerfile.humble -t s3-ros2-zenoh:humble-1 ..

up: ## Start step 1: rmw_zenohd + rmw_zenoh publishers
	$(COMPOSE) up -d

up-dds: ## Start step 2: zenoh-bridge-ros2dds + Fast DDS publishers
	$(COMPOSE) --profile fastdds up -d bridge sensor-pub-dds

up-lyrical: ## Start step 3: Lyrical rmw_zenohd + rmw_zenoh publishers
	$(COMPOSE) --profile lyrical up -d router-lyrical sensor-pub-lyrical

up-lyrical-dds: ## Start step 3: bridge + Lyrical Fast DDS publishers
	$(COMPOSE) --profile lyrical-fastdds up -d bridge-lyrical sensor-pub-lyrical-dds

up-cyclone: ## Start step 3: bridge + Jazzy Cyclone DDS publishers
	$(COMPOSE) --profile jazzy-cyclone up -d bridge-cyclone sensor-pub-cyclone-dds

up-humble: ## Start step 3: bridge + Humble Fast DDS publishers
	$(COMPOSE) --profile humble-fastdds up -d bridge-humble sensor-pub-humble-dds

down: ## Stop everything (all profiles)
	$(COMPOSE) --profile fastdds --profile lyrical --profile lyrical-fastdds --profile jazzy-cyclone --profile humble-fastdds down

logs: ## Tail logs: make logs (or S=sensor-pub-dds)
	$(COMPOSE) logs -f --tail=100 $(S)

clean: ## down -v
	$(COMPOSE) --profile fastdds --profile lyrical --profile lyrical-fastdds --profile jazzy-cyclone --profile humble-fastdds down -v

versions: ## Show the RMW package versions baked into the images
	@echo "== jazzy =="; docker run --rm --entrypoint cat s3-ros2-zenoh:jazzy-1 /usr/local/lib/s3/versions.txt
	@echo "== lyrical =="; docker run --rm --entrypoint cat s3-ros2-zenoh:lyrical-1 /usr/local/lib/s3/versions.txt
	@echo "== humble =="; docker run --rm --entrypoint cat s3-ros2-zenoh:humble-1 /usr/local/lib/s3/versions.txt
	@echo "== jazzy+cyclone =="; docker run --rm --entrypoint cat s3-ros2-zenoh:jazzy-cyclone-1 /usr/local/lib/s3/versions.txt

secrets/ca.pem: ## Optional: CA bundle so apt in the build survives TLS interception
	@mkdir -p secrets
	@if [ -f /etc/ssl/cert.pem ]; then cp /etc/ssl/cert.pem $@ && echo "copied host CA bundle to $@"; else echo "no /etc/ssl/cert.pem; place your proxy CA chain at $@"; fi
