# Builds the thadam-simulator demo image. Context is the repository root:
#
#   docker build -f tools/thadam-simulator/Dockerfile -t thadam-simulator:local .
#
# (docker compose in examples/edge-capture passes that context itself.)
#
# Images: public Docker Hub only, pinned by tag (ADR-0020 family of rules).
FROM rust:1.89-bookworm AS build
WORKDIR /src

# Manifest files first so dependency crates stay cached until sources
# change. The image's Rust (1.89) matches the workspace MSRV, so
# rust-toolchain.toml is deliberately not copied (rustup would download a
# second toolchain otherwise).
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY apps ./apps
COPY tools ./tools

# Opt-in for TLS-intercepting proxies: place the CA chain at
# examples/edge-capture/secrets/ca.pem (gitignored; `make secrets/ca.pem` copies
# the host bundle). It is appended to the BUILD stage's trust store only and
# never lands in the final image (the runtime stage copies just the binary).
COPY examples/edge-capture/secrets/ /tmp/demo-secrets/
RUN set -e; \
    if [ -f /tmp/demo-secrets/ca.pem ]; then \
        cat /tmp/demo-secrets/ca.pem >> /etc/ssl/certs/ca-certificates.crt; \
        export CARGO_HTTP_CAINFO=/etc/ssl/certs/ca-certificates.crt; \
        export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt; \
    fi; \
    cargo build --release -p thadam-simulator; \
    strip target/release/thadam-simulator

FROM debian:bookworm-slim
RUN useradd --uid 10001 --create-home thadam
COPY --from=build /src/target/release/thadam-simulator /usr/local/bin/thadam-simulator
# Docker initializes a named volume's contents and ownership from the image's
# mount point on first mount; the demo compose stack mounts the oracle here.
RUN mkdir -p /oracle && chown 10001:10001 /oracle
USER thadam
ENTRYPOINT ["/usr/local/bin/thadam-simulator"]
