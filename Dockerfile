# ─────────────────────────────────────────────────────────────────────────────
# Str:::lab Studio — Dockerfile
# Builds a minimal nginx image that serves the IDE and proxies API calls.
#
# Build:  docker build -t codedstreams/strlabstudio:latest .
# Run:    docker run -p 3030:80 \
#           -e FLINK_GATEWAY_HOST=flink-gateway \
#           -e FLINK_GATEWAY_PORT=8083 \
#           -e JOBMANAGER_HOST=flink-jobmanager \
#           -e JOBMANAGER_PORT=8081 \
#           codedstreams/strlabstudio:latest
# ─────────────────────────────────────────────────────────────────────────────

FROM nginx:1.25-alpine

LABEL org.opencontainers.image.title="Str:::lab Studio"
LABEL org.opencontainers.image.description="SQL Studio for real-time Flink streaming pipelines"
LABEL org.opencontainers.image.authors="Nestor A. A <nestorabiawuh@gmail.com>"
LABEL org.opencontainers.image.source="https://github.com/coded-streams/strlabstudio"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# envsubst for runtime config substitution, wget for healthcheck
RUN apk add --no-cache gettext wget

# ── Static assets ─────────────────────────────────────────────────────────────
COPY studio/ /usr/share/nginx/html/

# ── nginx config
COPY nginx/studio.conf /etc/nginx/templates/default.conf.template
RUN  rm -f /etc/nginx/conf.d/default.conf

# ── Runtime environment variables
ENV FLINK_GATEWAY_HOST=localhost \
    FLINK_GATEWAY_PORT=8083 \
    JOBMANAGER_HOST=localhost \
    JOBMANAGER_PORT=8081

# ── Entrypoint: envsubst → start nginx
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN  chmod +x /docker-entrypoint.sh

EXPOSE 80

HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost/healthz || exit 1

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD        ["nginx", "-g", "daemon off;"]