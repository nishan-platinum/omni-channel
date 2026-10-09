# Gateway-only runtime image (ADR-0014): the `gateway` binary and its fixture, nothing from the CRM.
# Built from the main build image so the binary is identical to the one measured:
#   docker compose build app && docker build -f docker/gateway/Dockerfile -t omni-gateway:local .
ARG BUILD_IMAGE=omni-m01-app:local
FROM ${BUILD_IMAGE} AS bin

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* && useradd --system --uid 10001 --home /app gateway
WORKDIR /app
COPY --from=bin /usr/local/bin/gateway /usr/local/bin/gateway
COPY config/gateway-fixture.json /app/config/gateway-fixture.json
USER gateway
ENV GATEWAY_BIND=0.0.0.0:4000 GATEWAY_FIXTURE=/app/config/gateway-fixture.json LOG_FORMAT=json
EXPOSE 4000
CMD ["gateway"]
