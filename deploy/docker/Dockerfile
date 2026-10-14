# Single-binary thadam image. Build from the repository root:
#
#   docker build -f deploy/docker/Dockerfile -t thadam:dev .
#   docker build -f deploy/docker/Dockerfile --build-arg FEATURES=opcua -t thadam:opcua .
#
# Images: public Docker Hub only, pinned by tag. rust-toolchain.toml is not
# copied: the builder already ships the pinned Rust 1.99.0, and rustup would
# otherwise download a second toolchain.

FROM rust:1.99.0-bookworm AS build
ARG FEATURES=""
WORKDIR /src

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY apps ./apps
COPY tools ./tools
COPY sdk ./sdk

# Opt-in for TLS-intercepting networks: place the proxy's CA bundle at
# deploy/docker/secrets/ca.pem (gitignored; the host's /etc/ssl/cert.pem may not
# include it). It is appended to this stage's trust store only; the runtime
# stage copies just the binary, so the CA never reaches the image.
COPY deploy/docker/secrets/ /tmp/build-secrets/
# shortcut: no dependency layer cache; every build compiles the full tree.
RUN set -e; \
    if [ -f /tmp/build-secrets/ca.pem ]; then \
        cat /tmp/build-secrets/ca.pem >> /etc/ssl/certs/ca-certificates.crt; \
        export CARGO_HTTP_CAINFO=/etc/ssl/certs/ca-certificates.crt; \
        export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt; \
    fi; \
    cargo build --release --locked -p thadam-cli --features "$FEATURES"; \
    strip target/release/thadam

FROM debian:bookworm-20261005-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --no-create-home --home-dir /var/lib/thadam --shell /usr/sbin/nologin thadam \
    && install -d -m 0700 -o thadam -g thadam /var/lib/thadam \
    && install -d /etc/thadam

COPY --from=build /src/target/release/thadam /usr/local/bin/thadam

USER thadam
WORKDIR /var/lib/thadam
VOLUME ["/var/lib/thadam"]
EXPOSE 9464

# SIGTERM starts the graceful shutdown; a second signal forces the stop (exit 130).
STOPSIGNAL SIGTERM

# shortcut: the probe assumes port 9464 from CMD; a different port needs a matching HEALTHCHECK.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS -o /dev/null http://127.0.0.1:9464/healthz

ENTRYPOINT ["thadam"]
# The admin API binds all interfaces so `-p 127.0.0.1:9464:9464` can reach it. A
# non-loopback bind needs the token file, mounted at run time and never baked in.
# `thadam run` reads `observability.admin`; these flags stay because the image's
# contract (publishable port on 0.0.0.0, token at /etc/thadam/admin-token) must
# hold for any mounted pipeline.yaml, whatever its config says.
CMD ["run", "--admin-bind", "0.0.0.0:9464", "--admin-token-file", "/etc/thadam/admin-token", "/etc/thadam/pipeline.yaml"]
