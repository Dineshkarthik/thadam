# Linux environment for spike S1. Data directories live on a Docker volume or tmpfs inside the
# Docker Desktop VM, never on a bind mount from the host.
#
# Builds offline (crates and LazyFS are fetched on the host and passed as named build contexts):
#   cargo vendor --locked /tmp/s1wal-vendor
#   git clone --depth 1 https://github.com/dsrhaslab/lazyfs.git /tmp/s1wal-lazyfs
#   git clone --depth 1 --branch v3.7.1 https://github.com/ToruNiina/toml11 /tmp/s1wal-lazyfs/_deps/toml
#   curl -sSL https://github.com/gabime/spdlog/archive/v1.10.0.tar.gz | tar xz -C /tmp/s1wal-lazyfs/_deps \
#     && mv /tmp/s1wal-lazyfs/_deps/spdlog-1.10.0 /tmp/s1wal-lazyfs/_deps/spdlog
#   docker build -f docker/Dockerfile --build-context vendor=/tmp/s1wal-vendor \
#     --build-context lazyfs=/tmp/s1wal-lazyfs -t s1wal-linux:latest .
FROM rust:1-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
      fuse3 libfuse3-dev cmake g++ pkg-config python3 && rm -rf /var/lib/apt/lists/*
COPY --from=vendor . /vendor
RUN printf '[source.crates-io]\nreplace-with = "vendored"\n[source.vendored]\ndirectory = "/vendor"\n' > "$CARGO_HOME/config.toml"
COPY . /spike
WORKDIR /spike
RUN cargo build --release --offline && gcc -shared -fPIC -O2 -o /spike/target/failsync.so fault/failsync.c -ldl
COPY --from=lazyfs . /opt/lazyfs
# LazyFS is best effort: record failure instead of failing the image.
ENV FETCHCONTENT_FLAGS="-DFETCHCONTENT_SOURCE_DIR_TOML=/opt/lazyfs/_deps/toml -DFETCHCONTENT_SOURCE_DIR_SPDLOG=/opt/lazyfs/_deps/spdlog"
RUN (cd /opt/lazyfs/libs/libpcache && mkdir -p build && cd build && cmake $FETCHCONTENT_FLAGS .. && cmake --build . -j8 \
     && ln -sfn /opt/lazyfs/_deps/spdlog /opt/lazyfs/libs/libpcache/build/_deps/spdlog-src \
     && cd /opt/lazyfs/lazyfs && mkdir -p build && cd build && cmake -DLAZYFS_BUILD_TESTS=OFF $FETCHCONTENT_FLAGS .. && cmake --build . -j8) \
    > /opt/lazyfs-build.log 2>&1 \
    || echo failed > /opt/lazyfs-failed
