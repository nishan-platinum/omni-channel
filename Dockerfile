# syntax=docker/dockerfile:1
# ---- build stage -----------------------------------------------------------------------------
FROM rust:1-bookworm AS build
WORKDIR /build
# Cache dependencies: build a dummy crate with the real manifest first.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src target/release/deps/omni_m01* target/release/deps/fake_meta* target/release/deps/hub_load* \
              target/release/omni-m01* target/release/.fingerprint/omni-m01*
# Templates (Askama) and migrations (SQLx) are compiled into the binary.
COPY src ./src
COPY templates ./templates
COPY migrations ./migrations
RUN cargo build --release --locked

# ---- runtime stage ---------------------------------------------------------------------------
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /app omni
WORKDIR /app
COPY --from=build /build/target/release/omni-m01 /usr/local/bin/omni-m01
# fake-meta (WhatsApp Cloud API imitation for development/load tests) and the hub load driver.
COPY --from=build /build/target/release/fake_meta /usr/local/bin/fake_meta
COPY --from=build /build/target/release/hub_load /usr/local/bin/hub_load
COPY static ./static
COPY config ./config
RUN mkdir -p /app/data && chown -R omni:omni /app
USER omni
# A fixed glibc mmap threshold returns large transient buffers (Argon2id uses 19 MiB per hash)
# to the OS instead of keeping them in per-thread arenas.
ENV APP_BIND_ADDR=0.0.0.0:3000 DATA_DIR=/app/data LOG_FORMAT=json MALLOC_MMAP_THRESHOLD_=131072 MALLOC_ARENA_MAX=4
EXPOSE 3000
CMD ["omni-m01"]
